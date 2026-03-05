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
from typing import Any

from dagster import (
    AllPartitionMapping,
    AssetExecutionContext,
    AssetIn,
    DataVersion,
    DynamicPartitionsDefinition,
    Output,
    asset,
)
from sqlalchemy import select

from talent_matching.db import get_session
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
    candidate_matches_location,
    parse_job_preferred_locations,
)
from talent_matching.matchmaking.scoring import (
    SENIORITY_MAX_DEDUCTION,
    compensation_fit,
    cosine_similarity,
    location_score,
    seniority_penalty_and_experience_score,
    skill_coverage_score,
    skill_semantic_score,
)
from talent_matching.models.raw import RawJob
from talent_matching.skills.resolver import load_alias_map, resolve_skill_name, skill_vector_key
from talent_matching.utils.airtable_mapper import normalized_job_to_airtable_fields
from talent_matching.utils.dagster_async import run_with_interrupt_check

# Dynamic partition definition for jobs (one partition per Airtable job record ID)
job_partitions = DynamicPartitionsDefinition(name="jobs")


@asset(
    partitions_def=job_partitions,
    description="Single job record fetched from Airtable (jobs table)",
    group_name="jobs",
    required_resource_keys={"airtable_jobs"},
    op_tags={"dagster/concurrency_key": "airtable_api"},
    metadata={"source": "airtable"},
)
def airtable_jobs(context: AssetExecutionContext) -> Output[dict[str, Any]]:
    """Fetch a single job row from Airtable by partition key (Airtable record ID)."""
    record_id = context.partition_key
    context.log.info(f"[airtable_jobs] record_id={record_id} Fetching from Airtable")

    airtable = context.resources.airtable_jobs
    job_record = airtable.fetch_record_by_id(record_id)

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
    and location_raw even when airtable_jobs fetches from a table with different column names.
    """
    record_id = context.partition_key
    notion = context.resources.notion

    session = get_session()
    existing_raw = session.execute(
        select(RawJob).where(RawJob.airtable_record_id == record_id)
    ).scalar_one_or_none()
    session.close()

    # Prefer Postgres RawJob when it has richer data (e.g. from ATS sensor)
    if existing_raw and (existing_raw.job_description or "").strip():
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
            "experience_level_raw": None,
            "location_raw": airtable_jobs.get("location_raw"),
            "work_setup_raw": None,
            "status_raw": None,
            "job_category_raw": airtable_jobs.get("job_title_raw"),
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
    context.log.info(
        f"[raw_jobs] record_id={record_id} Ready: desc={desc_len} chars, "
        f"non_negotiables={bool((base.get('non_negotiables') or '').strip())}, "
        f"location={bool((base.get('location_raw') or '').strip())}"
    )
    return base


def _is_notion_url(url: str) -> bool:
    if not url or not isinstance(url, str):
        return False
    return "notion.site" in url or "notion.so" in url


def _build_job_description_for_scoring(
    raw_job: dict[str, Any],
    normalized_job: dict[str, Any],
) -> str:
    """Build job description for LLM scoring: prefer raw text, fallback to normalized content.

    When raw_job.job_description is empty (e.g. loaded from DB without it, or different
    ingestion path), we try normalized_job.job_description (stored at normalization time),
    then synthesize from role_summary, narratives, requirements, and responsibilities.
    """
    raw_desc = (raw_job.get("job_description") or "").strip()
    if raw_desc and len(raw_desc) >= 50:
        return raw_desc

    norm_desc = (normalized_job.get("job_description") or "").strip()
    if norm_desc and len(norm_desc) >= 50:
        return norm_desc

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

    return "\n\n".join(parts) if parts else raw_desc or "(No job description available)"


@asset(
    partitions_def=job_partitions,
    ins={"raw_jobs": AssetIn()},
    description="LLM-normalized job requirements with structured fields and narratives",
    group_name="jobs",
    io_manager_key="postgres_io",
    required_resource_keys={"openrouter"},
    code_version="2.3.2",  # v2.3.2: store job_description in payload for downstream scoring
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
    if not job_description or len(job_description) < 50:
        context.log.warning(
            f"[normalized_jobs] record_id={record_id} Skipping LLM: desc too short ({len(job_description or '')} chars)"
        )
        return {
            "airtable_record_id": record_id,
            "title": raw_jobs.get("job_title") or "Unknown",
            "company_name": raw_jobs.get("company_name") or "Unknown",
            "job_description": job_description or "(No description)",
            "normalized_json": None,
            "prompt_version": None,
            "model_version": None,
            "narratives": {},
        }
    non_negotiables = (raw_jobs.get("non_negotiables") or "").strip() or None
    nice_to_have = (raw_jobs.get("nice_to_have") or "").strip() or None
    location_raw = (raw_jobs.get("location_raw") or "").strip() or None
    projected_salary = (raw_jobs.get("projected_salary") or "").strip() or None

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
            ),
        )
    )
    data = result.data
    context.add_output_metadata(
        {
            "llm_cost_usd": result.cost_usd,
            "llm_tokens_input": result.input_tokens,
            "llm_tokens_output": result.output_tokens,
            "llm_model": result.model,
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
    code_version="2.2.0",  # v2.2.0: Canonicalize skill vector keys via alias resolver
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

    narratives = normalized_jobs.get("narratives") or {}

    # Fallback to top-level narrative_* if present (e.g. from DB load)
    def _text(key: str, narrative_key: str) -> str:
        if key == "role_description":
            return (
                normalized_jobs.get("narrative_role")
                or narratives.get("role")
                or "No role description."
            )
        return (
            normalized_jobs.get(f"narrative_{key}") or narratives.get(key) or f"No {key} narrative."
        )

    texts_to_embed = [
        _text("experience", "experience"),
        _text("domain", "domain"),
        _text("personality", "personality"),
        _text("impact", "impact"),
        _text("technical", "technical"),
        _text("role_description", "role"),
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

    filtered = [c for c in eligible if candidate_matches_location(c, job_locations)]
    loc_preview = (location_raw or "")[:50] + ("..." if len(location_raw or "") > 50 else "")
    context.log.info(
        f"[location_prefiltered_candidates] record_id={record_id} Location filter "
        f"'{loc_preview}': {len(filtered)}/{len(eligible)} candidates pass"
    )
    return filtered


# Notion formula weights: role 40%, domain 35%, culture 25%
ROLE_WEIGHT = 0.4
DOMAIN_WEIGHT = 0.35
CULTURE_WEIGHT = 0.25
TOP_N_PER_JOB = 30
ALGORITHM_VERSION = "notion_v3"
SKILL_MIN_THRESHOLD = 0.30

# Combined score = weighted blend (35% vector, 40% skill fit, 10% comp, 15% location) − seniority deduction
VECTOR_WEIGHT = 0.35
SKILL_FIT_WEIGHT = 0.40
COMPENSATION_WEIGHT = 0.10
LOCATION_WEIGHT = 0.15
# When at least one required skill matches: 80% from rating-based coverage, 20% semantic (tie-breaker)
SKILL_RATING_WEIGHT = 0.8
SKILL_SEMANTIC_WEIGHT = 0.2  # only applied when there is at least one matching skill
SENIORITY_PENALTY_PER_YEAR = 2  # soft penalty points per year short (overall)


@asset(
    partitions_def=job_partitions,
    ins={
        "location_prefiltered_candidates": AssetIn(),
        "candidate_vectors": AssetIn(
            key=["candidate_vectors"],
            partition_mapping=AllPartitionMapping(),
        ),
        "normalized_jobs": AssetIn(),
        "job_vectors": AssetIn(),
    },
    description="Computed matches between jobs and candidates with scores (one partition per job)",
    group_name="matching",
    code_version="2.7.0",  # location pre-filter: use location_prefiltered_candidates
    io_manager_key="postgres_io",
    required_resource_keys={"matchmaking"},
    metadata={
        "table": "matches",
        "scoring_weights": {
            "role": ROLE_WEIGHT,
            "domain": DOMAIN_WEIGHT,
            "culture": CULTURE_WEIGHT,
        },
    },
)
def matches(
    context: AssetExecutionContext,
    location_prefiltered_candidates: list[dict[str, Any]],
    candidate_vectors: Any,
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

    # AllPartitionMapping yields dict[partition_key, value]; value per partition is what IO manager returned (dict or list)
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

    def _to_vector_list(x: Any) -> list[dict[str, Any]]:
        if x is None:
            return []
        if isinstance(x, dict):
            flat = []
            for v in x.values():
                if isinstance(v, list):
                    flat.extend(v for item in v if isinstance(item, dict))
                elif isinstance(v, dict):
                    flat.append(v)
            return flat
        if isinstance(x, list):
            flat = []
            for item in x:
                if isinstance(item, list):
                    flat.extend(i for i in item if isinstance(i, dict))
                elif isinstance(item, dict):
                    flat.append(item)
            return flat
        return [x] if isinstance(x, dict) else []

    normalized_candidates = location_prefiltered_candidates or []
    candidate_vectors = _to_vector_list(candidate_vectors)
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

    # Build index: raw_candidate_id -> {position_0, position_1, ..., domain, personality}
    cand_vecs_by_raw: dict[str, dict[str, list[float]]] = {}
    for rec in candidate_vectors:
        raw_id = str(rec.get("candidate_id", ""))
        if not raw_id:
            continue
        if raw_id not in cand_vecs_by_raw:
            cand_vecs_by_raw[raw_id] = {}
        vt = rec.get("vector_type") or ""
        vec = rec.get("vector")
        if vec is not None and vt:
            cand_vecs_by_raw[raw_id][vt] = vec

    # Normalized id lookup (postgres load uses column name "id")
    job_ids = [str(j.get("id", "")) for j in normalized_jobs if j.get("id")]
    cand_ids = [str(c.get("id", "")) for c in normalized_candidates if c.get("id")]
    job_required_skills = context.resources.matchmaking.get_job_required_skills(job_ids)
    candidate_skills_map = context.resources.matchmaking.get_candidate_skills(cand_ids)

    match_results: list[dict[str, Any]] = []
    for job in normalized_jobs:
        job_id_norm = job.get("id")
        raw_job_id = str(job.get("raw_job_id", ""))
        if not job_id_norm or not raw_job_id:
            continue
        jvecs = job_vecs_by_raw.get(raw_job_id, {})
        job_role_vec = jvecs.get("role_description")
        job_domain_vec = jvecs.get("domain")
        job_personality_vec = jvecs.get("personality")
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

        # Per (job, candidate) raw scores; then we rescale vector_score per job
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
                list[str],
                list[str],
                dict[str, str],
            ]
        ] = []
        for candidate in normalized_candidates:
            cand_id_norm = candidate.get("id")
            raw_cand_id = str(candidate.get("raw_candidate_id", ""))
            if not cand_id_norm or not raw_cand_id:
                continue
            # Strict filter: job category must match one of the candidate's desired job categories
            if job_category:
                desired = candidate.get("desired_job_categories") or []
                desired_normalized = {
                    (c or "").strip().lower() for c in desired if (c or "").strip()
                }
                if not desired_normalized or job_category.lower() not in desired_normalized:
                    continue
            cvecs = cand_vecs_by_raw.get(raw_cand_id, {})

            # role_sim: job role_description vs best of candidate position_*
            position_keys = [k for k in cvecs if k.startswith("position_")]
            if job_role_vec and position_keys:
                role_sim = max(cosine_similarity(job_role_vec, cvecs[k]) for k in position_keys)
            elif job_role_vec and cvecs.get("experience"):
                role_sim = cosine_similarity(job_role_vec, cvecs["experience"])
            else:
                role_sim = 0.0

            domain_sim = (
                cosine_similarity(job_domain_vec, cvecs["domain"])
                if job_domain_vec and cvecs.get("domain")
                else 0.0
            )
            culture_sim = (
                cosine_similarity(job_personality_vec, cvecs["personality"])
                if job_personality_vec and cvecs.get("personality")
                else 0.0
            )

            vector_score = (
                ROLE_WEIGHT * role_sim + DOMAIN_WEIGHT * domain_sim + CULTURE_WEIGHT * culture_sim
            )

            cand_skills_list = candidate_skills_map.get(str(cand_id_norm), [])
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
            # Semantic only when at least one skill matches; then 80% rating, 20% semantic (tie-breaker)
            if matching:
                skill_fit_score = (
                    SKILL_RATING_WEIGHT * skill_coverage + SKILL_SEMANTIC_WEIGHT * skill_semantic
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
                matching,
                missing,
                ids,
            ) = r
            # Use raw vector score (no per-job min-max rescaling)
            base = (
                VECTOR_WEIGHT * v_raw
                + SKILL_FIT_WEIGHT * skill_fit
                + COMPENSATION_WEIGHT * comp_score
                + LOCATION_WEIGHT * loc_score
            )
            seniority_deduction = min(SENIORITY_MAX_DEDUCTION, sen_pen / 100.0)
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
    code_version="1.3.0",  # v1.3.0: fallback job description from normalized content when raw empty
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
    job_description = _build_job_description_for_scoring(raw_job, job)

    req_skills = context.resources.matchmaking.get_job_required_skills([job_id_norm])
    all_reqs = req_skills.get(job_id_norm, [])
    must_haves = [r for r in all_reqs if r.get("requirement_type") == "must_have"]

    openrouter = context.resources.openrouter
    openrouter.set_context(
        run_id=context.run_id,
        asset_key="llm_refined_shortlist",
        partition_key=context.partition_key,
        code_version="1.3.0",
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
        )

    async def _score_all() -> list[dict[str, Any]]:
        sem = asyncio.Semaphore(max_concurrent_llm)

        async def limited(
            m: dict[str, Any], c: dict[str, Any]
        ) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
            async with sem:
                result = await _score_one(m, c)
                return (m, c, result)

        tasks = [limited(m, c) for m, c in scored_items]
        results = await asyncio.gather(*tasks)

        return [
            {
                "candidate_id": str(m.get("candidate_id", "")),
                "candidate_name": c.get("full_name") or "Unknown",
                "fit_score": r.get("fit_score", 0),
                "pros": r.get("pros") or [],
                "cons": r.get("cons") or [],
                "fulfills_all_must_haves": r.get("fulfills_all_must_haves", False),
                "_match": m,
                "_result": r,
            }
            for m, c, r in results
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
    selected_ids = {str(s) for s in selection.get("selected_candidate_ids", [])}

    scored_by_id = {s["candidate_id"]: s for s in scored}
    final: list[dict[str, Any]] = []
    for rank, cand_id in enumerate(selection.get("selected_candidate_ids", []), start=1):
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
            }
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
ATS_MATCHMAKING_DONE_STATUS = "Matchmaking Done "  # trailing space matches Airtable choice


@asset(
    partitions_def=job_partitions,
    ins={"llm_refined_shortlist": AssetIn()},
    description="Upload top match results to ATS table as linked candidate chips and set Job Status to Matchmaking Done",
    group_name="matching",
    required_resource_keys={"airtable_ats"},
    code_version="1.3.0",
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

    if not matches:
        if should_flip_status:
            context.log.warning(
                f"[upload_matches_to_ats] record_id={record_id} No matches; setting status to Matchmaking Done anyway"
            )
            ats.update_record(record_id, {ATS_JOB_STATUS_FIELD: ATS_MATCHMAKING_DONE_STATUS})
        else:
            context.log.warning(
                f"[upload_matches_to_ats] record_id={record_id} No matches and status not flippable; skipping"
            )
        return

    candidate_norm_ids = [m["candidate_id"] for m in matches]

    session = get_session()
    rows = (
        session.query(NormalizedCandidate.id, NormalizedCandidate.airtable_record_id)
        .filter(NormalizedCandidate.id.in_(candidate_norm_ids))
        .all()
    )
    session.close()

    norm_to_airtable: dict[str, str] = {
        str(row.id): row.airtable_record_id for row in rows if row.airtable_record_id
    }

    sorted_matches = sorted(matches, key=lambda m: m.get("rank", 999))
    linked_record_ids = []
    for m in sorted_matches:
        at_id = norm_to_airtable.get(m["candidate_id"])
        if at_id:
            linked_record_ids.append(at_id)

    context.log.info(
        f"[upload_matches_to_ats] record_id={record_id} Mapped {len(linked_record_ids)}/{len(matches)} "
        f"candidates to Airtable record IDs"
    )

    fields: dict[str, Any] = {}
    if should_flip_status:
        fields[ATS_JOB_STATUS_FIELD] = ATS_MATCHMAKING_DONE_STATUS
    if linked_record_ids:
        fields[ATS_AI_PROPOSED_FIELD] = linked_record_ids
    if ats.matches_view_url:
        base = ats.matches_view_url.rstrip("?")
        sep = "&" if "?" in base else "?"
        # filterHasAnyOf_Job for linked record; value = ATS record ID
        fields["View Matches"] = f"{base}{sep}filterHasAnyOf_Job={record_id}"

    if fields:
        ats.update_record(record_id, fields)

    if ats.matches_table_id:
        matches_for_table = [
            {
                "candidate_airtable_id": norm_to_airtable.get(m["candidate_id"]),
                "score": m.get("llm_fit_score"),
                "pros": m.get("strengths"),
                "cons": m.get("red_flags"),
                "rank": m.get("rank"),
            }
            for m in sorted_matches
            if norm_to_airtable.get(m["candidate_id"])
        ]
        ats.replace_matches_for_job(record_id, matches_for_table)
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
