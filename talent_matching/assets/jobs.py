"""Job pipeline and matching assets.

This module defines the asset graph for processing jobs and generating matches:
1. airtable_jobs: Job records fetched from Airtable (partitioned per record)
2. raw_jobs: Resolved job description text (Notion fetch + Airtable mapping), stored in PostgreSQL
3. normalized_jobs: LLM-normalized job requirements + narratives
4. job_vectors: Semantic embeddings (experience, domain, personality, impact, technical, role_description)
5. matches: Computed candidate-job matches (vector raw 0.4 role + 0.35 domain + 0.25 culture; skill fit 80% rating, 20% semantic when a skill matches; top 30)
6. llm_refined_shortlist: LLM scores each of 30 candidates (1-10, pros, cons), selects final max 15 who fulfill all must-haves
7. upload_matches_to_ats: Write matched candidates as linked chips to ATS and set Job Status to Matchmaking Done
"""

import asyncio
import json
from datetime import UTC
from typing import Any

import numpy as np
from dagster import (
    AllPartitionMapping,
    AssetExecutionContext,
    AssetIn,
    DataVersion,
    DynamicPartitionsDefinition,
    Failure,
    Output,
    asset,
)
from sqlalchemy import select

from talent_matching.db import get_session
from talent_matching.llm import (
    CV_PROMPT_VERSION,
    EMBED_PROMPT_VERSION,
    JOB_PROMPT_VERSION,
)
from talent_matching.llm.operations.embed_text import embed_text
from talent_matching.llm.operations.normalize_job import (
    PROMPT_VERSION as NORMALIZE_JOB_PROMPT_VERSION,
)
from talent_matching.llm.operations.normalize_job import (
    normalize_job,
)
from talent_matching.llm.operations.score_candidate_job_fit import score_candidate_job_fit
from talent_matching.llm.operations.select_final_shortlist import select_final_shortlist
from talent_matching.matchmaking.location_filter import (
    MIN_POOL_SIZE,
    candidate_matches_country,
    candidate_matches_region,
    candidate_passes_location_or_timezone,
    job_locations_to_countries,
    job_locations_to_regions,
    parse_job_preferred_locations,
)
from talent_matching.matchmaking.scoring import (
    candidate_seniority_scale,
    compensation_fit,
    cosine_similarity_batch,
    job_is_high_stakes,
    job_required_seniority_scale,
    location_score,
    seniority_level_penalty,
    seniority_penalty_and_experience_score,
    seniority_scale_fit,
    skill_coverage_score,
    skill_semantic_score,
    tenure_instability_penalty,
)
from talent_matching.models.raw import RawJob
from talent_matching.skills.resolver import load_alias_map, resolve_skill_name, skill_vector_key
from talent_matching.utils.airtable_mapper import normalized_job_to_airtable_fields
from talent_matching.utils.dagster_async import run_with_interrupt_check
from talent_matching.utils.llm_text_validation import require_meaningful_text_fields

# Dynamic partition definition for jobs (one partition per Airtable job record ID)
job_partitions = DynamicPartitionsDefinition(name="jobs")

MIN_RAW_JOB_DESCRIPTION_LEN = 100
MIN_NORMALIZED_JOB_DESCRIPTION_LEN = 50


def resolve_job_ids_for_required_skills(
    normalized_jobs: list[dict[str, Any]],
    record_id: str | None,
    get_job_id_by_airtable_record_id: Any,
) -> list[str]:
    """Build list of normalized job UUIDs for fetching job_required_skills.

    When normalized_jobs and matches run in the same run, the upstream returns the
    asset payload (no DB "id"). We must resolve job id from DB by partition so
    required skills are always loaded. This helper encapsulates that logic for
    testability.
    """
    job_ids: list[str] = []
    for j in normalized_jobs:
        jid = j.get("id")
        if not jid and record_id:
            jid = get_job_id_by_airtable_record_id(record_id)
        if jid:
            job_ids.append(str(jid))
    return job_ids


@asset(
    partitions_def=job_partitions,
    description="Single job record fetched from Airtable ATS table",
    group_name="jobs",
    required_resource_keys={"airtable_jobs"},
    op_tags={"dagster/concurrency_key": "airtable_api"},
    metadata={"source": "airtable_ats"},
)
def airtable_jobs(context: AssetExecutionContext) -> Output[dict[str, Any]]:
    """Fetch a single job row from Airtable ATS table by partition key (Airtable record ID)."""
    record_id = context.partition_key
    context.log.info(f"[airtable_jobs] record_id={record_id} Fetching from Airtable ATS")

    airtable = context.resources.airtable_jobs
    job_record = airtable.fetch_record_by_id_canonical(record_id)

    data_version = job_record.pop("_data_version", None)
    company = job_record.get("company_name", "Unknown")
    title = job_record.get("job_title_raw", "N/A")
    context.log.info(
        f"[airtable_jobs] record_id={record_id} Fetched: {company} / {title} (version={data_version})"
    )

    return Output(
        value=job_record,
        data_version=DataVersion(data_version) if data_version else None,
    )


@asset(
    partitions_def=job_partitions,
    ins={"airtable_jobs": AssetIn()},
    description="Raw job data with resolved job description (Notion fetch or text)",
    group_name="jobs",
    io_manager_key="postgres_io",
    required_resource_keys={"notion"},
    metadata={"table": "raw_jobs"},
)
def raw_jobs(
    context: AssetExecutionContext,
    airtable_jobs: dict[str, Any],
) -> dict[str, Any]:
    """Resolve job description from Airtable row (Notion URL or text) and store as RawJob.

    Prefers existing RawJob from Postgres when present (e.g. from ATS sensor ingestion).
    This ensures ATS-triggered runs get full job_description, non_negotiables, nice_to_have,
    and location_raw even when airtable_jobs fetches from ATS (e.g. sensor wrote RawJob first).
    """
    record_id = context.partition_key
    notion = context.resources.notion

    session = get_session()
    existing_raw = session.execute(
        select(RawJob).where(RawJob.airtable_record_id == record_id)
    ).scalar_one_or_none()
    session.close()

    # Prefer Postgres RawJob only when it has enough description (avoid stale short placeholders)
    existing_desc = (existing_raw.job_description or "").strip() if existing_raw else ""
    if existing_raw and len(existing_desc) >= MIN_RAW_JOB_DESCRIPTION_LEN:
        base = {
            "airtable_record_id": record_id,
            "source": existing_raw.source,
            "source_id": existing_raw.source_id,
            "source_url": existing_raw.source_url,
            "job_title": existing_raw.job_title,
            "company_name": existing_raw.company_name,
            "job_description": (existing_raw.job_description or "").strip(),
            "company_website_url": existing_raw.company_website_url,
            "experience_level_raw": existing_raw.experience_level_raw,
            "location_raw": existing_raw.location_raw,
            "work_setup_raw": existing_raw.work_setup_raw,
            "status_raw": existing_raw.status_raw,
            "job_category_raw": existing_raw.job_category_raw,
            "x_url": existing_raw.x_url,
            "non_negotiables": existing_raw.non_negotiables,
            "nice_to_have": existing_raw.nice_to_have,
            "projected_salary": existing_raw.projected_salary,
        }
        link = existing_raw.source_url
        job_description = base["job_description"]
        context.log.info(
            f"[raw_jobs] record_id={record_id} Using existing RawJob (source={existing_raw.source}, "
            f"desc={len(job_description)} chars)"
        )
    else:
        link = airtable_jobs.get("job_description_link")
        job_description = airtable_jobs.get("job_description_text") or ""
        base = {
            "airtable_record_id": record_id,
            "source": "airtable",
            "source_id": record_id,
            "source_url": link or None,
            "job_title": airtable_jobs.get("job_title_raw"),
            "company_name": airtable_jobs.get("company_name"),
            "job_description": job_description or "(No description provided)",
            "company_website_url": airtable_jobs.get("company_website_url"),
            "experience_level_raw": airtable_jobs.get("experience_level_raw"),
            "location_raw": airtable_jobs.get("location_raw"),
            "work_setup_raw": airtable_jobs.get("work_setup_raw"),
            "status_raw": None,
            "job_category_raw": airtable_jobs.get("job_category_raw")
            or airtable_jobs.get("job_title_raw"),
            "x_url": airtable_jobs.get("x_url"),
            "non_negotiables": airtable_jobs.get("non_negotiables"),
            "nice_to_have": airtable_jobs.get("nice_to_have"),
            "projected_salary": airtable_jobs.get("projected_salary"),
        }

    if (
        link
        and _is_notion_url(link)
        and (not (job_description or "").strip() or job_description == "(No description provided)")
    ):
        context.log.info(f"[raw_jobs] record_id={record_id} Fetching Notion page: {link[:60]}...")
        job_description = (
            notion.fetch_page_content(link) or job_description or "(No content from Notion)"
        )
        base["job_description"] = job_description

    if (
        not (base.get("location_raw") or "").strip()
        and existing_raw
        and ((existing_raw.location_raw or "").strip())
    ):
        base["location_raw"] = existing_raw.location_raw
        context.log.info(
            f"[raw_jobs] record_id={record_id} Using existing location_raw: {(base['location_raw'] or '')[:60]}..."
        )

    desc_len = len(base["job_description"])
    has_non_negotiables = bool((base.get("non_negotiables") or "").strip())
    has_location = bool((base.get("location_raw") or "").strip())
    has_nice_to_have = bool((base.get("nice_to_have") or "").strip())
    context.add_output_metadata(
        {
            "job_desc_len": desc_len,
            "has_non_negotiables": has_non_negotiables,
            "has_location": has_location,
            "has_nice_to_have": has_nice_to_have,
        }
    )
    context.log.info(
        f"[raw_jobs] record_id={record_id} Ready: desc={desc_len} chars, "
        f"non_negotiables={has_non_negotiables}, "
        f"location={has_location}"
    )
    if desc_len < MIN_RAW_JOB_DESCRIPTION_LEN:
        raise Failure(
            description=(
                f"Job description too short for record_id={record_id} "
                f"({desc_len} chars, source={base.get('source') or 'unknown'}). "
                f"Minimum {MIN_RAW_JOB_DESCRIPTION_LEN} chars required before matchmaking. "
                "Check Airtable Job Description Text/Link or run "
                "`scripts/refresh_job_description_from_notion.py`."
            ),
            metadata={"record_id": record_id},
            allow_retries=False,
        )
    return base


def _is_notion_url(url: str) -> bool:
    if not url or not isinstance(url, str):
        return False
    return "notion.site" in url or "notion.so" in url


def _build_job_description_for_scoring(
    raw_job: dict[str, Any],
    normalized_job: dict[str, Any],
) -> tuple[str, str]:
    """Build job description for LLM scoring: prefer raw text, fallback to normalized content.

    When raw_job.job_description is empty (e.g. loaded from DB without it, or different
    ingestion path), we try normalized_job.job_description (stored at normalization time),
    then synthesize from role_summary, narratives, requirements, and responsibilities.

    Returns:
        (job_description, source) where source is "raw" | "normalized" | "synthesized" | "empty"
    """
    raw_desc = (raw_job.get("job_description") or "").strip()
    if raw_desc and len(raw_desc) >= 50:
        return raw_desc, "raw"

    norm_desc = (normalized_job.get("job_description") or "").strip()
    if norm_desc and len(norm_desc) >= 50:
        return norm_desc, "normalized"

    parts: list[str] = []
    role_summary = (normalized_job.get("role_summary") or "").strip()
    if role_summary:
        parts.append(f"Role Summary: {role_summary}")

    narratives = normalized_job.get("narratives") or {}
    nj = normalized_job.get("normalized_json") or normalized_job
    narratives = narratives or nj.get("narratives") or {}
    for key, label in [
        ("role", "Role Description"),
        ("experience", "Ideal Experience"),
        ("domain", "Domain Expertise"),
        ("technical", "Technical Requirements"),
        ("personality", "Work Style"),
        ("impact", "Impact & Scope"),
    ]:
        val = narratives.get(key) or normalized_job.get(f"narrative_{key}")
        if val and isinstance(val, str) and val.strip():
            parts.append(f"{label}:\n{val.strip()}")

    reqs = nj.get("requirements") or {}
    must = reqs.get("must_have_skills") or []
    nice = reqs.get("nice_to_have_skills") or []
    if must:
        names = [
            s.get("name", "") if isinstance(s, dict) else str(s)
            for s in must
            if (s.get("name") if isinstance(s, dict) else s)
        ]
        if names:
            parts.append(f"Must-have skills: {', '.join(names)}")
    if nice:
        names = [
            s.get("name", "") if isinstance(s, dict) else str(s)
            for s in nice
            if (s.get("name") if isinstance(s, dict) else s)
        ]
        if names:
            parts.append(f"Nice-to-have skills: {', '.join(names)}")

    resp = normalized_job.get("responsibilities") or nj.get("responsibilities") or []
    if resp and isinstance(resp, list):
        bullets = [f"  - {r}" for r in resp if r and isinstance(r, str)]
        if bullets:
            parts.append("Responsibilities:\n" + "\n".join(bullets))

    seniority = normalized_job.get("seniority_level") or nj.get("seniority_level")
    if seniority:
        parts.append(f"Seniority: {seniority}")

    min_years = reqs.get("years_of_experience_min") or normalized_job.get("min_years_experience")
    if min_years is not None:
        parts.append(f"Minimum years of experience: {min_years}")

    domain = reqs.get("domain_experience") or normalized_job.get("domain_experience")
    if domain:
        domain_str = ", ".join(domain) if isinstance(domain, list) else str(domain)
        parts.append(f"Domain experience: {domain_str}")

    tech = nj.get("tech_stack") or normalized_job.get("tech_stack")
    if tech and isinstance(tech, list):
        parts.append(f"Tech stack: {', '.join(str(t) for t in tech)}")

    result = "\n\n".join(parts) if parts else raw_desc or "(No job description available)"
    return result, "synthesized" if parts else "empty"


@asset(
    partitions_def=job_partitions,
    ins={"raw_jobs": AssetIn()},
    description="LLM-normalized job requirements with structured fields and narratives",
    group_name="jobs",
    io_manager_key="postgres_io",
    required_resource_keys={"openrouter", "matchmaking"},
    code_version="2.5.0",  # v2.5.0: job_category from candidate taxonomy for matchmaking alignment
    metadata={
        "table": "normalized_jobs",
        "llm_operation": "normalize_job",
    },
    op_tags={"dagster/concurrency_key": "openrouter_api"},
)
def normalized_jobs(
    context: AssetExecutionContext,
    raw_jobs: dict[str, Any],
) -> dict[str, Any]:
    """Normalize raw job description for this partition via LLM; persist to normalized_jobs."""
    record_id = context.partition_key
    job_description = (raw_jobs.get("job_description") or "").strip()
    if not job_description or len(job_description) < MIN_NORMALIZED_JOB_DESCRIPTION_LEN:
        raise Failure(
            description=(
                f"Job description too short for normalization for record_id={record_id} "
                f"({len(job_description or '')} chars). Minimum "
                f"{MIN_NORMALIZED_JOB_DESCRIPTION_LEN} chars required. Fix raw_jobs first."
            ),
            metadata={"record_id": record_id},
            allow_retries=False,
        )
    non_negotiables = (raw_jobs.get("non_negotiables") or "").strip() or None
    nice_to_have = (raw_jobs.get("nice_to_have") or "").strip() or None
    location_raw = (raw_jobs.get("location_raw") or "").strip() or None
    projected_salary = (raw_jobs.get("projected_salary") or "").strip() or None
    job_category_raw = (raw_jobs.get("job_category_raw") or "").strip() or None
    experience_level_raw = (raw_jobs.get("experience_level_raw") or "").strip() or None

    matchmaking = context.resources.matchmaking
    allowed_job_categories = matchmaking.get_allowed_job_categories()
    context.log.info(
        f"[normalized_jobs] record_id={record_id} Using {len(allowed_job_categories)} allowed job categories for job_category alignment"
    )

    openrouter = context.resources.openrouter
    result = asyncio.run(
        run_with_interrupt_check(
            context,
            normalize_job(
                openrouter,
                job_description,
                non_negotiables=non_negotiables,
                nice_to_have=nice_to_have,
                location_raw=location_raw,
                projected_salary=projected_salary,
                job_category_raw=job_category_raw,
                experience_level_raw=experience_level_raw,
                allowed_job_categories=allowed_job_categories or None,
            ),
        )
    )
    data = result.data
    requirements = data.get("requirements") or {}
    must_have_count = len(requirements.get("must_have_skills") or [])
    context.add_output_metadata(
        {
            "llm_cost_usd": result.cost_usd,
            "llm_tokens_input": result.input_tokens,
            "llm_tokens_output": result.output_tokens,
            "llm_model": result.model,
            "must_have_count": must_have_count,
            "has_job_category_raw": bool(job_category_raw),
            "has_experience_level_raw": bool(experience_level_raw),
        }
    )
    context.log.info(
        f"[normalized_jobs] record_id={record_id} Normalized: model={result.model}, "
        f"cost=${result.cost_usd:.6f}, tokens_in={result.input_tokens}, tokens_out={result.output_tokens}"
    )
    payload = {
        "airtable_record_id": record_id,
        **data,
        "job_description": job_description,
        "normalized_json": data,
        "prompt_version": NORMALIZE_JOB_PROMPT_VERSION,
        "model_version": result.model,
    }
    return payload


# Vector types for job narratives (aligned with candidate vector types for matching)
JOB_NARRATIVE_VECTOR_TYPES = [
    "experience",
    "domain",
    "personality",
    "impact",
    "technical",
    "role_description",
]


@asset(
    partitions_def=job_partitions,
    ins={"normalized_jobs": AssetIn()},
    description="Semantic embeddings for job narratives (experience, domain, personality, impact, technical, role_description)",
    group_name="jobs",
    required_resource_keys={"openrouter"},
    io_manager_key="pgvector_io",
    code_version="2.6.0",  # v2.6.0: code changed; bump for staleness detection
    op_tags={"dagster/concurrency_key": "openrouter_api"},
    metadata={
        "table": "job_vectors",
        "vector_types": JOB_NARRATIVE_VECTOR_TYPES + ["skill_*"],
    },
)
def job_vectors(
    context: AssetExecutionContext,
    normalized_jobs: dict[str, Any],
) -> list[dict[str, Any]]:
    """Generate semantic embeddings from normalized job narratives and per-skill expected capability.

    Builds six narrative vectors plus one vector per required skill that has expected_capability.
    Uses same embedding model and skill_* key convention as candidate_vectors for like-for-like similarity.
    """
    record_id = context.partition_key
    openrouter = context.resources.openrouter

    if not normalized_jobs.get("normalized_json"):
        raise Failure(
            description=(
                f"Normalized job has no normalized_json for record_id={record_id}. "
                "Cannot generate meaningful job vectors."
            ),
            metadata={"record_id": record_id},
            allow_retries=False,
        )

    narratives = normalized_jobs.get("narratives") or {}
    validated_narratives = require_meaningful_text_fields(
        {
            "experience narrative": normalized_jobs.get("narrative_experience")
            or narratives.get("experience"),
            "domain narrative": normalized_jobs.get("narrative_domain") or narratives.get("domain"),
            "personality narrative": normalized_jobs.get("narrative_personality")
            or narratives.get("personality"),
            "impact narrative": normalized_jobs.get("narrative_impact") or narratives.get("impact"),
            "technical narrative": normalized_jobs.get("narrative_technical")
            or narratives.get("technical"),
            "role narrative": normalized_jobs.get("narrative_role") or narratives.get("role"),
        },
        context=f"job_vectors record_id={record_id}",
    )

    texts_to_embed = [
        validated_narratives["experience narrative"],
        validated_narratives["domain narrative"],
        validated_narratives["personality narrative"],
        validated_narratives["impact narrative"],
        validated_narratives["technical narrative"],
        validated_narratives["role narrative"],
    ]
    vector_types = list(JOB_NARRATIVE_VECTOR_TYPES)

    # Per-skill expected_capability vectors (same key convention as candidate skill_*)
    requirements = (normalized_jobs.get("normalized_json") or normalized_jobs).get(
        "requirements"
    ) or {}
    must_have = requirements.get("must_have_skills") or []
    nice_to_have = requirements.get("nice_to_have_skills") or []

    session = get_session()
    alias_map = load_alias_map(session)
    session.close()

    for entry in must_have + nice_to_have:
        if isinstance(entry, dict):
            name = (entry.get("name") or "").strip()
            cap = entry.get("expected_capability")
            if name and isinstance(cap, str) and cap.strip():
                canonical_name = resolve_skill_name(name, alias_map)
                texts_to_embed.append(f"{canonical_name}: {cap.strip()}")
                vector_types.append(skill_vector_key(canonical_name))
        # Legacy string-only entries: no expected_capability to embed

    result = asyncio.run(embed_text(openrouter, texts_to_embed))
    skill_count = len(vector_types) - len(JOB_NARRATIVE_VECTOR_TYPES)
    context.add_output_metadata(
        {
            "embedding_cost_usd": result.cost_usd,
            "embedding_tokens": result.input_tokens,
            "embedding_dimensions": result.dimensions,
            "embedding_model": result.model,
            "vectors_generated": len(result.embeddings),
            "skill_vectors": skill_count,
            "actual_narrative_count": len(validated_narratives),
        }
    )
    context.log.info(
        f"[job_vectors] record_id={record_id} Generated {len(result.embeddings)} vectors "
        f"(6 narrative + {skill_count} skill), cost=${result.cost_usd:.6f}"
    )

    vectors = []
    for i, vt in enumerate(vector_types):
        vectors.append(
            {
                "airtable_record_id": record_id,
                "vector_type": vt,
                "vector": result.embeddings[i],
                "model_version": result.model,
            }
        )
    return vectors


@asset(
    partitions_def=job_partitions,
    ins={"normalized_jobs": AssetIn()},
    description="Write all normalized job (N)-prefixed fields back to the Airtable row",
    group_name="jobs",
    required_resource_keys={"airtable_jobs", "matchmaking"},
    op_tags={"dagster/concurrency_key": "airtable_api"},
)
def airtable_job_sync(
    context: AssetExecutionContext,
    normalized_jobs: dict[str, Any],
) -> dict[str, Any]:
    """Write all normalized job fields back to the Airtable row under (N)-prefixed columns.

    Loads full NormalizedJob from Postgres by airtable_record_id and PATCHes the record.
    """
    record_id = context.partition_key
    matchmaking = context.resources.matchmaking
    job = matchmaking.get_normalized_job_by_airtable_record_id(record_id)
    if not job:
        context.log.warning(
            f"[airtable_job_sync] record_id={record_id} No normalized_jobs row; skipping sync"
        )
        return {"airtable_record_id": record_id, "synced": False, "skipped": True, "fields": {}}

    fields = normalized_job_to_airtable_fields(job)

    if not fields:
        context.log.info(f"[airtable_job_sync] record_id={record_id} No fields to sync")
        return {"airtable_record_id": record_id, "synced": False, "fields": {}}

    airtable = context.resources.airtable_jobs
    airtable.update_record(record_id, fields)
    context.log.info(f"[airtable_job_sync] record_id={record_id} Synced {len(fields)} (N) columns")
    return {"airtable_record_id": record_id, "synced": True, "fields": fields}


# ═══════════════════════════════════════════════════════════════════════════
# LOCATION PRE-FILTER: reduce candidate pool before scoring
# ═══════════════════════════════════════════════════════════════════════════

TALENT_JOB_STATUS_FRAUD = "Fraud"  # Exclude manually flagged fraudulent candidates


@asset(
    partitions_def=job_partitions,
    ins={
        "raw_jobs": AssetIn(),
        "normalized_candidates": AssetIn(
            key=["normalized_candidates"],
            partition_mapping=AllPartitionMapping(),
        ),
        "normalized_jobs": AssetIn(),
    },
    description="Candidates that pass the job's Preferred Location filter (or all if Global/No hard requirements)",
    group_name="matching",
)
def location_prefiltered_candidates(
    context: AssetExecutionContext,
    raw_jobs: dict[str, Any],
    normalized_candidates: Any,
    normalized_jobs: Any,
) -> list[dict[str, Any]]:
    """Filter candidates by Job Status (exclude Fraud) and job Preferred Location before scoring.

    First excludes candidates with Job Status = Fraud. Then applies location filter if set.
    """

    def _to_candidate_list(x: Any) -> list[dict[str, Any]]:
        if x is None:
            return []
        if isinstance(x, dict):
            out = []
            for v in x.values():
                if isinstance(v, dict) and v:
                    out.append(v)
                elif isinstance(v, list):
                    out.extend(i for i in v if isinstance(i, dict))
            return out
        if isinstance(x, list):
            return [item for item in x if isinstance(item, dict)]
        return [x] if isinstance(x, dict) else []

    candidates = _to_candidate_list(normalized_candidates)

    # Exclude Fraud (Job Status from Talent Airtable)
    eligible = [
        c for c in candidates if (c.get("job_status") or "").strip() != TALENT_JOB_STATUS_FRAUD
    ]
    fraud_excluded = len(candidates) - len(eligible)
    record_id = context.partition_key
    if fraud_excluded:
        context.log.info(
            f"[location_prefiltered_candidates] record_id={record_id} Job Status filter: "
            f"{fraud_excluded} Fraud excluded, {len(eligible)} eligible"
        )

    location_raw = (raw_jobs.get("location_raw") or "").strip() or None
    job_locations = parse_job_preferred_locations(location_raw)
    if job_locations is None:
        context.log.info(
            f"[location_prefiltered_candidates] record_id={record_id} No location filter; "
            f"passing all {len(eligible)} eligible candidates"
        )
        return eligible

    job_timezone = (
        (normalized_jobs or {}).get("timezone_requirements")
        if isinstance(normalized_jobs, dict)
        else None
    )
    loc_preview = (location_raw or "")[:50] + ("..." if len(location_raw or "") > 50 else "")

    # Step 1: strict (exact location or same/adjacent timezone)
    filtered = [
        c for c in eligible if candidate_passes_location_or_timezone(c, job_locations, job_timezone)
    ]
    if len(filtered) >= MIN_POOL_SIZE:
        context.log.info(
            f"[location_prefiltered_candidates] record_id={record_id} Location filter "
            f"'{loc_preview}': strict pass, {len(filtered)}/{len(eligible)} candidates"
        )
        return filtered

    # Step 2: country expansion
    allowed_countries = job_locations_to_countries(job_locations)
    if allowed_countries:
        filtered = [c for c in eligible if candidate_matches_country(c, allowed_countries)]
        if len(filtered) >= MIN_POOL_SIZE:
            context.log.info(
                f"[location_prefiltered_candidates] record_id={record_id} Location filter "
                f"'{loc_preview}': country expansion, {len(filtered)}/{len(eligible)} candidates"
            )
            return filtered

    # Step 3: region expansion (filtered still has step-2 result; only refine if we have region mapping)
    allowed_regions = job_locations_to_regions(job_locations)
    if allowed_regions:
        filtered = [c for c in eligible if candidate_matches_region(c, allowed_regions)]

    context.log.info(
        f"[location_prefiltered_candidates] record_id={record_id} Location filter "
        f"'{loc_preview}': region expansion (or final), {len(filtered)}/{len(eligible)} candidates"
    )
    return filtered


TOP_N_PER_JOB = 30
ALGORITHM_VERSION = "notion_v3"
SKILL_MIN_THRESHOLD = 0.30


@asset(
    partitions_def=job_partitions,
    ins={
        "location_prefiltered_candidates": AssetIn(),
        "normalized_jobs": AssetIn(),
        "job_vectors": AssetIn(),
    },
    description="Computed matches between jobs and candidates with scores (one partition per job)",
    group_name="matching",
    code_version="2.13.0",  # v2.13.0: scoring weights per job_category via get_weights_for_job_category
    io_manager_key="postgres_io",
    required_resource_keys={"matchmaking"},
    op_tags={
        "dagster/concurrency_key": "matchmaking_vectors",
        "dagster/concurrency_limit": "matchmaking",
    },
    metadata={
        "table": "matches",
        "scoring_weights": "per job_category via get_weights_for_job_category",
    },
)
def matches(
    context: AssetExecutionContext,
    location_prefiltered_candidates: list[dict[str, Any]],
    normalized_jobs: Any,
    job_vectors: Any,
) -> list[dict[str, Any]]:
    """Compute matches: vector_score (role/domain/culture) + skill penalty, top 30 per job.

    Optional (deferred): pre-filter to candidates with candidate_role_fitness.fitness_score >= 60
    for a role matching the job; see plan matchmaking_scoring_and_shortlist.
    """
    record_id = context.partition_key
    context.log.info(
        f"[matches] record_id={record_id} Computing matches (vector+skill, top {TOP_N_PER_JOB} per job)"
    )

    normalized_candidates = location_prefiltered_candidates or []
    if not normalized_jobs or not normalized_candidates:
        context.log.info(
            f"[matches] record_id={record_id} Skipping: jobs={bool(normalized_jobs)}, "
            f"candidates={len(normalized_candidates)}"
        )
        return []

    # Normalize job/vector inputs (single partition: dict or list)
    if isinstance(normalized_jobs, dict):
        normalized_jobs = [normalized_jobs]
    if isinstance(job_vectors, dict):
        job_vectors = [job_vectors]
    flat_jv = []
    for x in job_vectors or []:
        flat_jv.extend(x if isinstance(x, list) else [x])
    job_vectors = flat_jv

    # Build index: raw_job_id -> {role_description, domain, personality} vectors
    job_vecs_by_raw: dict[str, dict[str, list[float]]] = {}
    for rec in job_vectors:
        raw_id = str(rec.get("job_id", ""))
        if not raw_id:
            continue
        if raw_id not in job_vecs_by_raw:
            job_vecs_by_raw[raw_id] = {}
        vt = rec.get("vector_type") or ""
        vec = rec.get("vector")
        if vec is not None and vt:
            job_vecs_by_raw[raw_id][vt] = vec

    # Load candidate vectors only for pre-filtered candidates (not all 7k+)
    raw_cand_ids = list(
        {
            str(c.get("raw_candidate_id", ""))
            for c in normalized_candidates
            if c.get("raw_candidate_id")
        }
    )
    cand_vecs_by_raw = context.resources.matchmaking.get_candidate_vectors(raw_cand_ids)
    context.log.info(
        f"[matches] record_id={record_id} Loaded vectors for {len(cand_vecs_by_raw)}/{len(raw_cand_ids)} candidates"
    )

    # Normalized id lookup. When normalized_jobs and matches run in the same run, the upstream
    # returns the asset payload (no DB "id"). Resolve job id from DB by partition so required
    # skills are always loaded (see resolve_job_ids_for_required_skills and unit test).
    matchmaking = context.resources.matchmaking

    def _get_job_id(rid: str) -> str | None:
        job = matchmaking.get_normalized_job_by_airtable_record_id(rid)
        return str(job["id"]) if job and job.get("id") else None

    job_ids = resolve_job_ids_for_required_skills(normalized_jobs, record_id, _get_job_id)
    cand_ids = [str(c.get("id", "")) for c in normalized_candidates if c.get("id")]
    job_required_skills = matchmaking.get_job_required_skills(job_ids)
    candidate_skills_map = matchmaking.get_candidate_skills(cand_ids)

    match_results: list[dict[str, Any]] = []
    low_info_matchmaking = False
    zero_must_haves = False
    no_job_category = False
    for job in normalized_jobs:
        job_id_norm = job.get("id")
        if not job_id_norm and record_id:
            fallback = matchmaking.get_normalized_job_by_airtable_record_id(record_id)
            job_id_norm = fallback.get("id") if fallback else None
        raw_job_id = str(job.get("raw_job_id", ""))
        if not job_id_norm or not raw_job_id:
            continue
        job_id_norm = str(job_id_norm)
        jvecs = job_vecs_by_raw.get(raw_job_id, {})
        job_role_vec = jvecs.get("role_description")
        job_domain_vec = jvecs.get("domain")
        job_personality_vec = jvecs.get("personality")
        job_impact_vec = jvecs.get("impact")
        job_technical_vec = jvecs.get("technical")
        req_skills = job_required_skills.get(str(job_id_norm), [])
        must_have = [
            s["skill_name"] for s in req_skills if s.get("requirement_type") == "must_have"
        ]
        nice_to_have = [
            s["skill_name"] for s in req_skills if s.get("requirement_type") == "nice_to_have"
        ]
        req_skills_with_min_years = [
            (s["skill_name"], int(s["min_years"]), s.get("requirement_type") or "must_have")
            for s in req_skills
            if s.get("min_years") is not None
        ]
        job_min_years = job.get("min_years_experience")
        if job_min_years is not None and not isinstance(job_min_years, int):
            job_min_years = int(job_min_years) if job_min_years else None
        job_salary_min = job.get("salary_min")
        job_salary_max = job.get("salary_max")
        if job_salary_min is not None and not isinstance(job_salary_min, int | float):
            job_salary_min = float(job_salary_min) if job_salary_min else None
        if job_salary_max is not None and not isinstance(job_salary_max, int | float):
            job_salary_max = float(job_salary_max) if job_salary_max else None
        job_location_type = job.get("location_type")
        job_timezone = job.get("timezone_requirements")
        job_category = (job.get("job_category") or "").strip()
        weights = matchmaking.get_or_create_weights_for_job_category(job.get("job_category"))
        if not must_have:
            zero_must_haves = True
            low_info_matchmaking = True
            context.log.warning(
                f"[matches] record_id={record_id} Job has zero must-have skills; "
                "match quality may be unreliable"
            )
        if not job_category:
            no_job_category = True
            low_info_matchmaking = True
            context.log.warning(
                f"[matches] record_id={record_id} Job has no job_category; desired role "
                "filter will be skipped"
            )

        # Per (job, candidate) raw scores; then we rescale vector_score per job
        # Batch vector similarity: filter candidates first, then compute role/domain/culture in one pass
        filtered_candidates: list[tuple[dict[str, Any], str, str]] = []
        for candidate in normalized_candidates:
            cand_id_norm = candidate.get("id")
            raw_cand_id = str(candidate.get("raw_candidate_id", ""))
            if not cand_id_norm or not raw_cand_id:
                continue
            if job_category:
                desired = candidate.get("desired_job_categories") or []
                desired_normalized = {
                    (c or "").strip().lower() for c in desired if (c or "").strip()
                }
                if not desired_normalized or job_category.lower() not in desired_normalized:
                    continue
            filtered_candidates.append((candidate, raw_cand_id, str(cand_id_norm)))

        role_sims: np.ndarray
        domain_sims: np.ndarray
        culture_sims: np.ndarray
        if filtered_candidates:
            cand_vecs_list = [cand_vecs_by_raw.get(rcid, {}) for _, rcid, _ in filtered_candidates]
            dim = 1536

            # role: max over position_* or experience per candidate
            role_vecs_flat: list[list[float]] = []
            role_cand_ends: list[int] = []
            for cvecs in cand_vecs_list:
                position_keys = [k for k in cvecs if k.startswith("position_")]
                if position_keys:
                    role_vecs_flat.extend(cvecs[k] for k in position_keys)
                elif cvecs.get("experience") is not None:
                    role_vecs_flat.append(cvecs["experience"])
                role_cand_ends.append(len(role_vecs_flat))

            if role_vecs_flat and job_role_vec:
                role_matrix = np.array(role_vecs_flat, dtype=np.float64)
                role_sims_flat = cosine_similarity_batch(
                    np.array(job_role_vec, dtype=np.float64), role_matrix
                )
                role_sims = np.zeros(len(filtered_candidates), dtype=np.float64)
                start = 0
                for i, end in enumerate(role_cand_ends):
                    role_sims[i] = float(np.max(role_sims_flat[start:end])) if end > start else 0.0
                    start = end
            else:
                role_sims = np.zeros(len(filtered_candidates), dtype=np.float64)

            # domain: one vector per candidate
            domain_vecs = [cvecs.get("domain") for cvecs in cand_vecs_list]
            domain_vecs_arr = np.array(
                [v if v is not None else np.zeros(dim, dtype=np.float32) for v in domain_vecs],
                dtype=np.float64,
            )
            domain_sims = (
                cosine_similarity_batch(np.array(job_domain_vec, dtype=np.float64), domain_vecs_arr)
                if job_domain_vec and domain_vecs_arr.size
                else np.zeros(len(filtered_candidates), dtype=np.float64)
            )

            # culture: one vector per candidate
            culture_vecs = [cvecs.get("personality") for cvecs in cand_vecs_list]
            culture_vecs_arr = np.array(
                [v if v is not None else np.zeros(dim, dtype=np.float32) for v in culture_vecs],
                dtype=np.float64,
            )
            culture_sims = (
                cosine_similarity_batch(
                    np.array(job_personality_vec, dtype=np.float64), culture_vecs_arr
                )
                if job_personality_vec and culture_vecs_arr.size
                else np.zeros(len(filtered_candidates), dtype=np.float64)
            )

            # impact: one vector per candidate
            impact_vecs = [cvecs.get("impact") for cvecs in cand_vecs_list]
            impact_vecs_arr = np.array(
                [v if v is not None else np.zeros(dim, dtype=np.float32) for v in impact_vecs],
                dtype=np.float64,
            )
            impact_sims = (
                cosine_similarity_batch(np.array(job_impact_vec, dtype=np.float64), impact_vecs_arr)
                if job_impact_vec and impact_vecs_arr.size
                else np.zeros(len(filtered_candidates), dtype=np.float64)
            )

            # technical: one vector per candidate
            technical_vecs = [cvecs.get("technical") for cvecs in cand_vecs_list]
            technical_vecs_arr = np.array(
                [v if v is not None else np.zeros(dim, dtype=np.float32) for v in technical_vecs],
                dtype=np.float64,
            )
            technical_sims = (
                cosine_similarity_batch(
                    np.array(job_technical_vec, dtype=np.float64), technical_vecs_arr
                )
                if job_technical_vec and technical_vecs_arr.size
                else np.zeros(len(filtered_candidates), dtype=np.float64)
            )
        else:
            role_sims = np.array([], dtype=np.float64)
            domain_sims = np.array([], dtype=np.float64)
            culture_sims = np.array([], dtype=np.float64)
            impact_sims = np.array([], dtype=np.float64)
            technical_sims = np.array([], dtype=np.float64)

        rows: list[
            tuple[
                float,
                float,
                float,
                float,
                float,
                float,
                float,
                float,
                float,
                float,
                float,
                float,
                list[str],
                list[str],
                dict[str, str],
            ]
        ] = []
        job_req_scale = job_required_seniority_scale(job)
        high_stakes = job_is_high_stakes(job)

        for idx, (candidate, raw_cand_id, cand_id_norm) in enumerate(filtered_candidates):
            cvecs = cand_vecs_by_raw.get(raw_cand_id, {})
            role_sim = float(role_sims[idx]) if idx < len(role_sims) else 0.0
            domain_sim = float(domain_sims[idx]) if idx < len(domain_sims) else 0.0
            culture_sim = float(culture_sims[idx]) if idx < len(culture_sims) else 0.0
            impact_sim = float(impact_sims[idx]) if idx < len(impact_sims) else 0.0
            technical_sim = float(technical_sims[idx]) if idx < len(technical_sims) else 0.0

            vector_score = (
                weights.role_weight * role_sim
                + weights.domain_weight * domain_sim
                + weights.culture_weight * culture_sim
                + weights.impact_weight * impact_sim
                + weights.technical_weight * technical_sim
            )

            cand_scale = candidate_seniority_scale(candidate)
            scale_fit = seniority_scale_fit(cand_scale, job_req_scale)
            level_deduction = seniority_level_penalty(
                job.get("seniority_level"),
                candidate.get("seniority_level"),
                weights.seniority_level_max_deduction,
            )
            tenure_deduction_raw = tenure_instability_penalty(candidate, high_stakes)
            tenure_deduction = min(
                weights.tenure_instability_max_deduction,
                tenure_deduction_raw,
            )

            cand_skills_list = candidate_skills_map.get(cand_id_norm, [])
            cand_skills_map_for_cand: dict[str, tuple[float, int | None]] = {}
            for cs in cand_skills_list:
                name = (cs.get("skill_name") or "").strip()
                if name:
                    cand_skills_map_for_cand[name] = (
                        (cs.get("rating") or 5) / 10.0,
                        cs.get("years_experience"),
                    )

            candidate_skill_names = set(cand_skills_map_for_cand.keys())
            missing_must = [s for s in must_have if s not in candidate_skill_names]
            missing_nice = [s for s in nice_to_have if s not in candidate_skill_names]
            matching = [s for s in must_have + nice_to_have if s in candidate_skill_names]

            skill_coverage = skill_coverage_score(
                req_skills, cand_skills_map_for_cand, jvecs, cvecs
            )
            skill_semantic = skill_semantic_score(
                job_role_vec, cvecs, req_skills=req_skills, job_skill_vecs=jvecs
            )
            # Semantic only when at least one skill matches; then rating vs semantic (tie-breaker)
            if matching:
                skill_fit_score = (
                    weights.skill_rating_weight * skill_coverage
                    + weights.skill_semantic_weight * skill_semantic
                )
            else:
                skill_fit_score = skill_coverage

            cand_years = candidate.get("years_of_experience")
            if cand_years is not None and not isinstance(cand_years, int):
                cand_years = int(cand_years) if cand_years else None
            seniority_penalty, experience_match_score = seniority_penalty_and_experience_score(
                job_min_years,
                job.get("max_years_experience"),
                cand_years,
                req_skills_with_min_years,
                cand_skills_map_for_cand,
            )

            comp_min = candidate.get("compensation_min")
            comp_max = candidate.get("compensation_max")
            if comp_min is not None and not isinstance(comp_min, int | float):
                comp_min = float(comp_min) if comp_min else None
            if comp_max is not None and not isinstance(comp_max, int | float):
                comp_max = float(comp_max) if comp_max else None
            compensation_match_score = compensation_fit(
                job_salary_min,
                job_salary_max,
                comp_min,
                comp_max,
            )

            cand_timezone = candidate.get("timezone")
            location_match_score = location_score(cand_timezone, job_timezone, job_location_type)

            if skill_fit_score < SKILL_MIN_THRESHOLD:
                continue

            rows.append(
                (
                    vector_score,
                    role_sim,
                    domain_sim,
                    culture_sim,
                    skill_fit_score,
                    seniority_penalty,
                    compensation_match_score,
                    experience_match_score,
                    location_match_score,
                    scale_fit,
                    level_deduction,
                    tenure_deduction,
                    matching,
                    missing_must + missing_nice,
                    {
                        "candidate_id": str(cand_id_norm),
                        "job_id": str(job_id_norm),
                        "full_name": candidate.get("full_name") or "",
                    },
                )
            )

        scored: list[
            tuple[
                float,
                float,
                float,
                float,
                float,
                float,
                float,
                float,
                list[str],
                list[str],
                dict[str, str],
            ]
        ] = []
        for r in rows:
            (
                v_raw,
                role_sim,
                domain_sim,
                culture_sim,
                skill_fit,
                sen_pen,
                comp_score,
                exp_score,
                loc_score,
                scale_fit,
                level_ded,
                tenure_ded,
                matching,
                missing,
                ids,
            ) = r
            # Use raw vector score (no per-job min-max rescaling)
            base = (
                weights.vector_weight * v_raw
                + weights.skill_fit_weight * skill_fit
                + weights.compensation_weight * comp_score
                + weights.location_weight * loc_score
                + weights.seniority_scale_weight * scale_fit
            )
            years_deduction = min(weights.seniority_max_deduction, sen_pen / 100.0)
            seniority_deduction = years_deduction + level_ded + tenure_ded
            combined_01 = max(0.0, min(1.0, base - seniority_deduction))
            scored.append(
                (
                    combined_01,
                    role_sim,
                    domain_sim,
                    culture_sim,
                    skill_fit,
                    comp_score,
                    exp_score,
                    loc_score,
                    matching,
                    missing,
                    ids,
                )
            )

        scored.sort(key=lambda t: t[0], reverse=True)

        seen_names: set[str] = set()
        deduped: list[tuple] = []
        for entry in scored:
            name = entry[-1].get("full_name", "").strip().lower()
            if name and name in seen_names:
                continue
            if name:
                seen_names.add(name)
            deduped.append(entry)
        scored = deduped

        for rank, (
            combined_01,
            role_sim,
            domain_sim,
            culture_sim,
            skill_fit,
            comp_score,
            exp_score,
            loc_score,
            matching,
            missing,
            ids,
        ) in enumerate(scored[:TOP_N_PER_JOB], start=1):
            match_results.append(
                {
                    "job_id": ids["job_id"],
                    "candidate_id": ids["candidate_id"],
                    "match_score": round(combined_01, 6),
                    "role_similarity_score": round(role_sim, 6),
                    "domain_similarity_score": round(domain_sim, 6),
                    "culture_similarity_score": round(culture_sim, 6),
                    "skills_match_score": round(skill_fit, 6),
                    "compensation_match_score": round(comp_score, 6),
                    "experience_match_score": round(exp_score, 6),
                    "location_match_score": round(loc_score, 6),
                    "matching_skills": matching or None,
                    "missing_skills": missing if missing else None,
                    "rank": rank,
                    "algorithm_version": ALGORITHM_VERSION,
                }
            )

    if match_results:
        avg_score = sum(m["match_score"] for m in match_results) / len(match_results)
        top_score = max(m["match_score"] for m in match_results)
        context.log.info(
            f"[matches] record_id={record_id} Computed {len(match_results)} matches: "
            f"avg_score={avg_score:.3f}, top_score={top_score:.3f}, "
            f"candidate_pool={len(normalized_candidates)}"
        )
    else:
        context.log.info(
            f"[matches] record_id={record_id} No matches (candidate_pool={len(normalized_candidates)}, "
            f"skill_min_threshold={SKILL_MIN_THRESHOLD})"
        )
    context.add_output_metadata(
        {
            "low_info_matchmaking": low_info_matchmaking,
            "zero_must_haves": zero_must_haves,
            "no_job_category": no_job_category,
        }
    )
    return match_results


def _to_candidate_list_for_shortlist(x: Any) -> list[dict[str, Any]]:
    """Flatten normalized_candidates from AllPartitionMapping to list of dicts."""
    if x is None:
        return []
    if isinstance(x, dict):
        out = []
        for v in x.values():
            if isinstance(v, dict) and v:
                out.append(v)
            elif isinstance(v, list):
                out.extend(i for i in v if isinstance(i, dict))
        return out
    if isinstance(x, list):
        return [item for item in x if isinstance(item, dict)]
    return [x] if isinstance(x, dict) else []


@asset(
    partitions_def=job_partitions,
    ins={
        "matches": AssetIn(),
        "normalized_candidates": AssetIn(
            key=["normalized_candidates"],
            partition_mapping=AllPartitionMapping(),
        ),
        "normalized_jobs": AssetIn(),
        "raw_jobs": AssetIn(),
    },
    description="LLM-refined shortlist: score 30 candidates (1-10, pros, cons), select max 15 who fulfill all must-haves",
    group_name="matching",
    io_manager_key="postgres_io",
    required_resource_keys={"openrouter", "matchmaking"},
    code_version="1.5.0",  # v1.5.0: when LLM selects 0, return empty shortlist (do not upload fallback to Airtable)
    metadata={"table": "matches"},
    op_tags={"dagster/concurrency_key": "openrouter_api"},
)
def llm_refined_shortlist(
    context: AssetExecutionContext,
    matches: list[dict[str, Any]],
    normalized_candidates: Any,
    normalized_jobs: Any,
    raw_jobs: Any,
) -> list[dict[str, Any]]:
    """Score each of 30 matches with advanced AI, select final max 15 who fulfill all must-haves."""
    record_id = context.partition_key
    if not matches:
        context.log.info(
            f"[llm_refined_shortlist] record_id={record_id} No matches to refine; skipping"
        )
        return []

    candidates_list = _to_candidate_list_for_shortlist(normalized_candidates)
    cand_by_id: dict[str, dict[str, Any]] = {
        str(c["id"]): c for c in candidates_list if c.get("id")
    }

    job = normalized_jobs if isinstance(normalized_jobs, dict) else (normalized_jobs or [{}])[0]
    raw_job = raw_jobs if isinstance(raw_jobs, dict) else (raw_jobs or [{}])[0]
    job_id_norm = str(job.get("id", ""))
    job_title = job.get("job_title") or job.get("job_category") or "Unknown"

    # Debug: log input availability before building job description
    raw_desc_len = len((raw_job.get("job_description") or "").strip())
    norm_desc_len = len((job.get("job_description") or "").strip())
    context.log.info(
        f"[llm_refined_shortlist] record_id={record_id} Job description inputs: "
        f"raw_job.job_description={raw_desc_len} chars, "
        f"normalized_job.job_description={norm_desc_len} chars, "
        f"raw_job keys={list(raw_job.keys())[:12]}"
    )

    job_description, desc_source = _build_job_description_for_scoring(raw_job, job)
    preview = job_description[:200] + ("..." if len(job_description) > 200 else "")
    context.log.info(
        f"[llm_refined_shortlist] record_id={record_id} Job description for scoring: "
        f"source={desc_source}, len={len(job_description)} chars, "
        f"preview={repr(preview)}"
    )
    if desc_source == "empty":
        raise Failure(
            description=(
                f"Job description is empty for record_id={record_id}. "
                f"raw_job.job_description={raw_desc_len} chars, "
                f"normalized_job.job_description={norm_desc_len} chars. "
                f"Check raw_jobs and normalized_jobs tables in DB for this partition."
            ),
            metadata={"record_id": record_id},
            allow_retries=False,
        )
    if desc_source == "synthesized" and len(job_description) < 100:
        raise Failure(
            description=(
                f"Job description is too thin for record_id={record_id} "
                f"({len(job_description)} chars, source=synthesized). "
                f"LLM cannot produce meaningful scoring results. "
                f"Check raw_jobs.job_description in DB for this partition."
            ),
            metadata={"record_id": record_id},
            allow_retries=False,
        )

    req_skills = context.resources.matchmaking.get_job_required_skills([job_id_norm])
    all_reqs = req_skills.get(job_id_norm, [])
    must_haves = [r for r in all_reqs if r.get("requirement_type") == "must_have"]
    context.add_output_metadata(
        {
            "zero_must_haves": not must_haves,
            "must_have_count": len(must_haves),
        }
    )
    if not must_haves:
        raise Failure(
            description=(
                f"Job has zero must-have skills for record_id={record_id} ({job_title}). "
                "LLM refinement cannot reliably filter candidates. Add clearer requirements "
                "to the job description or normalization inputs."
            ),
            metadata={"record_id": record_id},
            allow_retries=False,
        )

    openrouter = context.resources.openrouter
    openrouter.set_context(
        run_id=context.run_id,
        asset_key="llm_refined_shortlist",
        partition_key=context.partition_key,
        code_version="1.4.0",
    )

    non_negotiables = (raw_job.get("non_negotiables") or "").strip() or None
    nice_to_have = (raw_job.get("nice_to_have") or "").strip() or None
    location_raw = (raw_job.get("location_raw") or "").strip() or None

    # Build list of (match, candidate) for valid candidates; skip missing ones
    scored_items: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for m in matches:
        cand_id = str(m.get("candidate_id", ""))
        candidate = cand_by_id.get(cand_id)
        if not candidate:
            context.log.warning(
                f"[llm_refined_shortlist] record_id={record_id} Candidate {cand_id} not in normalized_candidates; skipping"
            )
            continue
        scored_items.append((m, candidate))

    # Max concurrent LLM calls; matches Dagster openrouter_api pool limit (50) and OpenRouter rate limits
    max_concurrent_llm = 30

    job_category = job.get("job_category")

    async def _score_one(m: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
        return await score_candidate_job_fit(
            openrouter,
            candidate,
            job_description,
            job,
            must_haves,
            non_negotiables=non_negotiables,
            nice_to_have=nice_to_have,
            location_raw=location_raw,
            job_category=job_category,
        )

    async def _score_all() -> list[dict[str, Any]]:
        sem = asyncio.Semaphore(max_concurrent_llm)
        skipped: list[str] = []

        async def limited(
            m: dict[str, Any], c: dict[str, Any]
        ) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]] | None:
            async with sem:
                cand_id = str(m.get("candidate_id", ""))
                cand_name = c.get("full_name") or "Unknown"
                cand_json_len = len(json.dumps(c, default=str))
                context.log.info(
                    f"[llm_refined_shortlist] record_id={record_id} Scoring {cand_name} "
                    f"(id={cand_id}, profile={cand_json_len} chars)"
                )
                try:
                    result = await _score_one(m, c)
                except ValueError as exc:
                    context.log.warning(
                        f"[llm_refined_shortlist] record_id={record_id} Skipping candidate "
                        f"{cand_name} ({cand_id}): {exc}"
                    )
                    skipped.append(cand_id)
                    return None
                return (m, c, result)

        tasks = [limited(m, c) for m, c in scored_items]
        raw_results = await asyncio.gather(*tasks)
        if skipped:
            context.log.warning(
                f"[llm_refined_shortlist] record_id={record_id} Skipped {len(skipped)} "
                f"candidates due to thin profiles: {skipped}"
            )

        return [
            {
                "candidate_id": str(item[0].get("candidate_id", "")),
                "candidate_name": item[1].get("full_name") or "Unknown",
                "fit_score": item[2].get("fit_score", 0),
                "pros": item[2].get("pros") or [],
                "cons": item[2].get("cons") or [],
                "fulfills_all_must_haves": item[2].get("fulfills_all_must_haves", False),
                "_match": item[0],
                "_result": item[2],
            }
            for item in raw_results
            if item is not None
        ]

    context.log.info(
        f"[llm_refined_shortlist] record_id={record_id} Scoring {len(scored_items)} candidates in parallel (max {max_concurrent_llm} concurrent)"
    )
    scored = asyncio.run(run_with_interrupt_check(context, _score_all()))
    fulfills = sum(1 for s in scored if s.get("fulfills_all_must_haves"))
    context.log.info(
        f"[llm_refined_shortlist] record_id={record_id} Scored {len(scored)}/{len(matches)}, "
        f"{fulfills} fulfill must-haves"
    )

    async def _select() -> dict[str, Any]:
        return await select_final_shortlist(
            openrouter,
            [
                {
                    "candidate_id": s["candidate_id"],
                    "candidate_name": s["candidate_name"],
                    "fit_score": s["fit_score"],
                    "pros": s["pros"],
                    "cons": s["cons"],
                    "fulfills_all_must_haves": s["fulfills_all_must_haves"],
                }
                for s in scored
            ],
            job_title,
        )

    selection = asyncio.run(run_with_interrupt_check(context, _select()))
    selected_ids = selection.get("selected_candidate_ids", [])

    scored_by_id = {str(s["candidate_id"]): s for s in scored}
    final: list[dict[str, Any]] = []

    if selected_ids:
        for rank, cand_id in enumerate(selected_ids, start=1):
            s = scored_by_id.get(str(cand_id))
            if not s:
                continue
            m = s["_match"]
            r = s["_result"]
            final.append(
                {
                    **m,
                    "rank": rank,
                    "llm_fit_score": r.get("fit_score"),
                    "strengths": r.get("pros") or None,
                    "red_flags": r.get("cons") or None,
                    "used_fallback": False,
                }
            )
    else:
        context.log.info(
            f"[llm_refined_shortlist] record_id={record_id} LLM selected 0; "
            f"not uploading any candidates (no one passed refinement)"
        )

    fulfills_count = sum(1 for s in scored if s.get("fulfills_all_must_haves"))
    context.log.info(
        f"[llm_refined_shortlist] record_id={record_id} Done: {len(final)} final (from {len(matches)} scored, "
        f"{len(selected_ids)} selected by LLM, {fulfills_count} fulfilled must-haves)"
    )
    return final


# ═══════════════════════════════════════════════════════════════════════════════
# ATS UPLOAD: push match results back to Airtable ATS table
# ═══════════════════════════════════════════════════════════════════════════════

ATS_AI_PROPOSED_FIELD = "AI PROPOSTED CANDIDATES"
ATS_JOB_STATUS_FIELD = "Job Status"
ATS_MATCHMAKING_DONE_STATUS = "Matchmaking Done"
ATS_MATCHMAKING_RESULT_FIELD = "Matchmaking Result"
ATS_MATCHMAKING_LAST_RUN_FIELD = "Matchmaking Last Run"


@asset(
    partitions_def=job_partitions,
    ins={"llm_refined_shortlist": AssetIn()},
    description="Upload top match results to ATS table as linked candidate chips and set Job Status to Matchmaking Done",
    group_name="matching",
    required_resource_keys={"airtable_ats"},
    code_version="1.3.2",  # v1.3.2: clear existing matches (AI PROPOSED + Matches table) before upload
)
def upload_matches_to_ats(
    context: AssetExecutionContext,
    llm_refined_shortlist: list[dict[str, Any]],
) -> None:
    """Write matched candidates as linked records on the ATS row and flip status."""
    from talent_matching.models.candidates import NormalizedCandidate

    matches = llm_refined_shortlist
    record_id = context.partition_key
    ats = context.resources.airtable_ats
    context.log.info(
        f"[upload_matches_to_ats] record_id={record_id} Uploading {len(matches)} matches to ATS"
    )

    current_record = ats.fetch_record_by_id(record_id)
    current_status = current_record.get("fields", {}).get(ATS_JOB_STATUS_FIELD)
    should_flip_status = current_status == "Matchmaking Ready"

    if not should_flip_status:
        context.log.info(
            f"[upload_matches_to_ats] record_id={record_id} Job Status='{current_status}' "
            f"(not Matchmaking Ready); uploading matches but skipping status change"
        )

    from datetime import datetime

    run_timestamp = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z")

    # Clean slate: remove all existing matches for this job (ATS linked field + Matches table),
    # same as we replace AI PROPOSED CANDIDATES with the new list.
    ats.update_record(record_id, {ATS_AI_PROPOSED_FIELD: []})
    if ats.matches_table_id:
        ats.replace_matches_for_job(record_id, [])
    context.log.info(
        f"[upload_matches_to_ats] record_id={record_id} Cleared existing matches (AI PROPOSED CANDIDATES + Matches table)"
    )

    if not matches:
        context.log.warning(
            f"[upload_matches_to_ats] record_id={record_id} No suitable candidates found"
        )
        no_match_fields: dict[str, Any] = {
            ATS_AI_PROPOSED_FIELD: [],
            ATS_MATCHMAKING_RESULT_FIELD: "No suitable candidates found",
            ATS_MATCHMAKING_LAST_RUN_FIELD: run_timestamp,
        }
        if should_flip_status:
            no_match_fields[ATS_JOB_STATUS_FIELD] = ATS_MATCHMAKING_DONE_STATUS
        if ats.matches_view_url:
            base = ats.matches_view_url.rstrip("?")
            sep = "&" if "?" in base else "?"
            no_match_fields["View Matches"] = f"{base}{sep}filterHasAnyOf_Job={record_id}"
        ats.update_record(record_id, no_match_fields)
        return

    candidate_norm_ids = [m["candidate_id"] for m in matches]

    session = get_session()
    rows = (
        session.query(
            NormalizedCandidate.id,
            NormalizedCandidate.airtable_record_id,
            NormalizedCandidate.full_name,
        )
        .filter(NormalizedCandidate.id.in_(candidate_norm_ids))
        .all()
    )
    session.close()

    norm_to_airtable: dict[str, str] = {
        str(row.id): row.airtable_record_id for row in rows if row.airtable_record_id
    }
    norm_to_name: dict[str, str] = {str(row.id): row.full_name or "Unknown" for row in rows}

    sorted_matches = sorted(matches, key=lambda m: m.get("rank", 999))
    linked_record_ids: list[str] = []
    matches_for_table: list[dict[str, Any]] = []

    for m in sorted_matches:
        cand_key = str(m["candidate_id"])
        at_id = norm_to_airtable.get(cand_key)
        if not at_id:
            continue
        linked_record_ids.append(at_id)
        if ats.matches_table_id:
            matches_for_table.append(
                {
                    "name": norm_to_name.get(cand_key, "Unknown"),
                    "candidate_airtable_id": at_id,
                    "score": m.get("llm_fit_score"),
                    "pros": m.get("strengths"),
                    "cons": m.get("red_flags"),
                    "rank": m.get("rank"),
                    "combined_score": round(float(m["match_score"]) * 100, 2)
                    if m.get("match_score") is not None
                    else None,
                    "role_similarity": m.get("role_similarity_score"),
                    "domain_similarity": m.get("domain_similarity_score"),
                    "culture_similarity": m.get("culture_similarity_score"),
                    "skills_fit": m.get("skills_match_score"),
                    "compensation_fit": m.get("compensation_match_score"),
                    "experience_fit": m.get("experience_match_score"),
                    "location_fit": m.get("location_match_score"),
                    "matching_skills": m.get("matching_skills"),
                    "missing_skills": m.get("missing_skills"),
                    "matchmaking_version": m.get("algorithm_version") or ALGORITHM_VERSION,
                    "cv_normalization_version": CV_PROMPT_VERSION,
                    "job_normalization_version": JOB_PROMPT_VERSION,
                    "vectorization_version": EMBED_PROMPT_VERSION,
                }
            )

    context.log.info(
        f"[upload_matches_to_ats] record_id={record_id} Mapped {len(linked_record_ids)}/{len(matches)} "
        f"candidates to Airtable record IDs"
    )

    used_fallback = bool(matches and matches[0].get("used_fallback"))
    result_text = (
        "No must-have matches; showing best candidates"
        if used_fallback
        else f"{len(linked_record_ids)} candidates proposed"
    )
    fields: dict[str, Any] = {
        ATS_MATCHMAKING_RESULT_FIELD: result_text,
        ATS_MATCHMAKING_LAST_RUN_FIELD: run_timestamp,
    }
    if should_flip_status:
        fields[ATS_JOB_STATUS_FIELD] = ATS_MATCHMAKING_DONE_STATUS
    if linked_record_ids:
        fields[ATS_AI_PROPOSED_FIELD] = linked_record_ids
    if ats.matches_view_url:
        base = ats.matches_view_url.rstrip("?")
        sep = "&" if "?" in base else "?"
        fields["View Matches"] = f"{base}{sep}filterHasAnyOf_Job={record_id}"

    if fields:
        ats.update_record(record_id, fields)

    if ats.matches_table_id and matches_for_table:
        ats.replace_matches_for_job(record_id, matches_for_table, run_timestamp=run_timestamp)
        context.log.info(
            f"[upload_matches_to_ats] record_id={record_id} Wrote {len(matches_for_table)} records to Matches table"
        )

    status_msg = (
        f"set Job Status to '{ATS_MATCHMAKING_DONE_STATUS}'"
        if should_flip_status
        else "Job Status unchanged"
    )
    context.log.info(
        f"[upload_matches_to_ats] record_id={record_id} Done: {len(linked_record_ids)} chips to "
        f"'{ATS_AI_PROPOSED_FIELD}', {status_msg}"
    )
