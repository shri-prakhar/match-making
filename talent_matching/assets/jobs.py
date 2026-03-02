"""Job pipeline and matching assets.

This module defines the asset graph for processing jobs and generating matches:
1. airtable_jobs: Job records fetched from Airtable (partitioned per record)
2. raw_jobs: Resolved job description text (Notion fetch + Airtable mapping), stored in PostgreSQL
3. normalized_jobs: LLM-normalized job requirements + narratives
4. job_vectors: Semantic embeddings (experience, domain, personality, impact, technical, role_description)
5. matches: Computed candidate-job matches (vector raw 0.4 role + 0.35 domain + 0.25 culture; skill fit 80% rating, 20% semantic when a skill matches; top 15)
6. upload_matches_to_ats: Write matched candidates as linked chips to ATS and set Job Status to Matchmaking Done
"""

import asyncio
import math
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

from talent_matching.db import get_session
from talent_matching.llm.operations.embed_text import embed_text
from talent_matching.llm.operations.normalize_job import (
    PROMPT_VERSION as NORMALIZE_JOB_PROMPT_VERSION,
)
from talent_matching.llm.operations.normalize_job import (
    normalize_job,
)
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
    context.log.info(f"Fetching job record: {record_id}")

    airtable = context.resources.airtable_jobs
    job_record = airtable.fetch_record_by_id(record_id)

    data_version = job_record.pop("_data_version", None)
    context.log.info(
        f"Fetched job: {job_record.get('company_name', 'Unknown')} / {job_record.get('job_title_raw', 'N/A')}"
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
    """Resolve job description from Airtable row (Notion URL or text) and store as RawJob."""
    record_id = context.partition_key
    notion = context.resources.notion
    link = airtable_jobs.get("job_description_link")
    job_description = airtable_jobs.get("job_description_text") or ""

    if link and _is_notion_url(link):
        context.log.info(f"Fetching Notion page for job: {link[:60]}...")
        job_description = (
            notion.fetch_page_content(link) or job_description or "(No content from Notion)"
        )

    payload = {
        "airtable_record_id": record_id,
        "source": "airtable",
        "source_id": record_id,
        "source_url": link or None,
        "job_title": airtable_jobs.get("job_title_raw"),
        "company_name": airtable_jobs.get("company_name"),
        "job_description": job_description or "(No description provided)",
        "company_website_url": airtable_jobs.get("company_website_url"),
        "experience_level_raw": None,
        "location_raw": None,
        "work_setup_raw": None,
        "status_raw": None,
        "job_category_raw": airtable_jobs.get("job_title_raw"),
        "x_url": airtable_jobs.get("x_url"),
    }
    context.log.info(f"Prepared raw job {record_id} (description length: {len(job_description)})")
    return payload


def _is_notion_url(url: str) -> bool:
    if not url or not isinstance(url, str):
        return False
    return "notion.site" in url or "notion.so" in url


@asset(
    partitions_def=job_partitions,
    ins={"raw_jobs": AssetIn()},
    description="LLM-normalized job requirements with structured fields and narratives",
    group_name="jobs",
    io_manager_key="postgres_io",
    required_resource_keys={"openrouter"},
    code_version="2.3.1",  # bump after file format; no logic change
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
        context.log.warning(f"No meaningful job description for {record_id}; skipping LLM")
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
    payload = {
        "airtable_record_id": record_id,
        **data,
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
    context.add_output_metadata(
        {
            "embedding_cost_usd": result.cost_usd,
            "embedding_tokens": result.input_tokens,
            "embedding_dimensions": result.dimensions,
            "embedding_model": result.model,
            "vectors_generated": len(result.embeddings),
            "skill_vectors": len(vector_types) - len(JOB_NARRATIVE_VECTOR_TYPES),
        }
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
        context.log.warning(f"No normalized_jobs row for airtable_record_id={record_id}; skip sync")
        return {"airtable_record_id": record_id, "synced": False, "skipped": True, "fields": {}}

    fields = normalized_job_to_airtable_fields(job)

    if not fields:
        context.log.info(f"No fields to sync for job {record_id}")
        return {"airtable_record_id": record_id, "synced": False, "fields": {}}

    airtable = context.resources.airtable_jobs
    airtable.update_record(record_id, fields)
    context.log.info(f"Synced {record_id}: {len(fields)} (N) columns")
    return {"airtable_record_id": record_id, "synced": True, "fields": fields}


# Notion formula weights: role 40%, domain 35%, culture 25%
ROLE_WEIGHT = 0.4
DOMAIN_WEIGHT = 0.35
CULTURE_WEIGHT = 0.25
TOP_N_PER_JOB = 15
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
SENIORITY_PENALTY_PER_SKILL_YEAR = 1  # points per skill when candidate years < job min_years
SENIORITY_PENALTY_CAP = 10  # cap overall years penalty
SENIORITY_MAX_DEDUCTION = 0.2  # cap deduction from combined (penalty_points/100, max 20%)
SEMANTIC_PARTIAL_CREDIT_CAP = (
    0.5  # max effective rating for a near-miss skill via embedding similarity
)


def _cosine_similarity(a: list[float], b: list[float]) -> float:
    """Cosine similarity in [0, 1]. Returns 0 if either vector is zero."""
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(x * x for x in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    sim = dot / (norm_a * norm_b)
    return max(0.0, min(1.0, sim))


def _compensation_fit(
    job_salary_min: float | None,
    job_salary_max: float | None,
    cand_comp_min: float | None,
    cand_comp_max: float | None,
) -> float:
    """Overlap of job pay band with candidate expectations; 0-1. Neutral 0.5 if missing."""
    if (
        job_salary_min is None
        or job_salary_max is None
        or cand_comp_min is None
        or cand_comp_max is None
    ):
        return 0.5
    if job_salary_max <= job_salary_min:
        return 0.5
    overlap_start = max(job_salary_min, cand_comp_min)
    overlap_end = min(job_salary_max, cand_comp_max)
    if overlap_end <= overlap_start:
        return 0.0
    job_range = job_salary_max - job_salary_min
    overlap = (overlap_end - overlap_start) / job_range
    return min(1.0, overlap)


def _location_score(
    candidate_timezone: str | None,
    job_timezone_requirements: str | None,
    job_location_type: str | None,
) -> float:
    """Timezone overlap 0-1; weighted by location_type (remote=1, hybrid=0.7, onsite=0.5). Neutral 0.5 if missing."""
    weight = 1.0
    if job_location_type:
        lt = (job_location_type or "").strip().lower()
        if lt == "remote":
            weight = 1.0
        elif lt == "hybrid":
            weight = 0.7
        else:
            weight = 0.5
    if not candidate_timezone and not job_timezone_requirements:
        return 0.5
    if not candidate_timezone or not job_timezone_requirements:
        return 0.5 * weight

    def parse_tz(s: str) -> float | None:
        """Parse a timezone string to a UTC offset in hours.

        Handles UTC offset format ("UTC-5", "UTC+05:30", "GMT+1") and
        IANA timezone names ("America/New_York", "Asia/Kolkata").
        """
        stripped = s.strip()
        if not stripped or stripped.lower() == "null":
            return None
        upper = stripped.upper().replace(" ", "")
        if upper.startswith(("UTC", "GMT")):
            prefix_len = 3
            rest = upper[prefix_len:].strip()
            if not rest or rest == "0":
                return 0.0
            sign = 1 if rest.startswith("+") else -1
            rest = rest.lstrip("+-")
            if ":" in rest:
                parts = rest.split(":")
                if parts[0].isdigit() and parts[1].isdigit():
                    return sign * (int(parts[0]) + int(parts[1]) / 60)
            num = rest.split("/")[0].split("-")[0].split("+")[0]
            if num.isdigit():
                return sign * float(num)
            return None
        if "/" not in stripped:
            return None
        from datetime import datetime
        from zoneinfo import ZoneInfo

        zi = ZoneInfo(stripped)
        offset = datetime.now(zi).utcoffset()
        if offset is not None:
            return offset.total_seconds() / 3600
        return None

    c_tz = parse_tz(candidate_timezone)
    j_tz_str = (job_timezone_requirements or "").strip()
    if " to " in j_tz_str:
        parts = j_tz_str.split(" to ")
        j_lo = parse_tz(parts[0]) if parts else None
        j_hi = parse_tz(parts[1]) if len(parts) > 1 else None
    else:
        single = parse_tz(j_tz_str)
        j_lo = single
        j_hi = single
    if c_tz is None or (j_lo is None and j_hi is None):
        return 0.5 * weight
    lo: float = j_lo if j_lo is not None else (j_hi or 0.0)
    hi: float = j_hi if j_hi is not None else (j_lo or 0.0)
    if lo > hi:
        lo, hi = hi, lo
    if lo <= c_tz <= hi:
        return 1.0 * weight
    diff = min(abs(c_tz - lo), abs(c_tz - hi))
    overlap = max(0.0, 1.0 - diff / 12.0)
    return overlap * weight


def _skill_coverage_score(
    req_skills: list[dict[str, Any]],
    cand_skills_map: dict[str, tuple[float, int | None]],
    job_skill_vecs: dict[str, list[float]] | None = None,
    cand_skill_vecs: dict[str, list[float]] | None = None,
) -> float:
    """0-1: how well candidate skills cover job required skills (name + proficiency).

    Must-have skills have 3x weight of nice-to-have. Missing skill contributes 0
    for that component (no separate flat penalty).

    When a job specifies min_level for a skill, candidates below that threshold
    receive a proportional penalty (rating / required_level). Candidates at or
    above the threshold get full credit.

    When a required skill has no exact canonical match in the candidate's profile
    but both job and candidate skill vectors are available, the most similar
    candidate skill vector is found and partial credit is granted, capped by
    SEMANTIC_PARTIAL_CREDIT_CAP.
    """
    if not req_skills:
        return 1.0
    total_weight = 0.0
    scored = 0.0

    cand_vec_keys = (
        [k for k in cand_skill_vecs if k.startswith("skill_")] if cand_skill_vecs else []
    )

    for s in req_skills:
        name = (s.get("skill_name") or "").strip()
        if not name:
            continue
        req_type = s.get("requirement_type") or "must_have"
        w = 3.0 if req_type == "must_have" else 1.0
        total_weight += w
        rating, _years = cand_skills_map.get(name, (0.0, None))

        if rating == 0.0 and job_skill_vecs and cand_skill_vecs and cand_vec_keys:
            job_vec = job_skill_vecs.get(skill_vector_key(name))
            if job_vec:
                max_sim = max(
                    _cosine_similarity(job_vec, cand_skill_vecs[k]) for k in cand_vec_keys
                )
                rating = max_sim * SEMANTIC_PARTIAL_CREDIT_CAP

        level_factor = 1.0
        min_level = s.get("min_level")
        if min_level is not None and rating > 0:
            min_level_norm = min_level / 10.0
            if rating < min_level_norm:
                level_factor = rating / min_level_norm

        scored += rating * w * level_factor
    if total_weight == 0:
        return 1.0
    return min(1.0, scored / total_weight)


def _skill_semantic_score(
    job_role_vec: list[float] | None,
    cand_skill_vecs: dict[str, list[float]],
    req_skills: list[dict[str, Any]] | None = None,
    job_skill_vecs: dict[str, list[float]] | None = None,
) -> float:
    """0-1: per-skill job expected_capability vs candidate skill_* when both exist; else role vs max cand skill."""
    if req_skills and job_skill_vecs:
        total_weight = 0.0
        weighted_sim = 0.0
        for s in req_skills:
            name = (s.get("skill_name") or "").strip()
            if not name:
                continue
            key = skill_vector_key(name)
            job_vec = job_skill_vecs.get(key)
            cand_vec = cand_skill_vecs.get(key)
            if job_vec and cand_vec:
                w = 3.0 if (s.get("requirement_type") or "must_have") == "must_have" else 1.0
                total_weight += w
                weighted_sim += _cosine_similarity(job_vec, cand_vec) * w
        if total_weight > 0:
            return weighted_sim / total_weight
    # Fallback: job role_description vs max similarity to candidate skill_* vectors
    if not job_role_vec:
        return 0.5
    skill_keys = [k for k in cand_skill_vecs if k.startswith("skill_")]
    if not skill_keys:
        return 0.0
    sims = [_cosine_similarity(job_role_vec, cand_skill_vecs[k]) for k in skill_keys]
    return max(sims) if sims else 0.0


def _seniority_penalty_and_experience_score(
    job_min_years: int | None,
    job_max_years: int | None,
    cand_years: int | None,
    req_skills_with_min_years: list[tuple[str, int, str]],
    cand_skills_map: dict[str, tuple[float, int | None]],
) -> tuple[float, float]:
    """Returns (penalty_points, experience_match_score 0-1)."""
    penalty = 0.0
    if job_min_years is not None and cand_years is not None and cand_years < job_min_years:
        short = job_min_years - cand_years
        penalty += min(SENIORITY_PENALTY_CAP, short * SENIORITY_PENALTY_PER_YEAR)
    for skill_name, min_years, req_type in req_skills_with_min_years:
        _, cand_y = cand_skills_map.get(skill_name, (0.0, None))
        if cand_y is not None and min_years is not None and cand_y < min_years:
            penalty += (min_years - cand_y) * SENIORITY_PENALTY_PER_SKILL_YEAR
    max_penalty = SENIORITY_PENALTY_CAP + 5 * SENIORITY_PENALTY_PER_SKILL_YEAR
    experience_match_score = max(0.0, 1.0 - penalty / max_penalty) if max_penalty else 1.0
    return penalty, experience_match_score


@asset(
    partitions_def=job_partitions,
    ins={
        "normalized_candidates": AssetIn(
            key=["normalized_candidates"],
            partition_mapping=AllPartitionMapping(),
        ),
        "candidate_vectors": AssetIn(
            key=["candidate_vectors"],
            partition_mapping=AllPartitionMapping(),
        ),
        "normalized_jobs": AssetIn(),
        "job_vectors": AssetIn(),
    },
    description="Computed matches between jobs and candidates with scores (one partition per job)",
    group_name="matching",
    code_version="2.6.0",  # strict desired job category filter (candidate must list job's category)
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
    normalized_candidates: Any,
    candidate_vectors: Any,
    normalized_jobs: Any,
    job_vectors: Any,
) -> list[dict[str, Any]]:
    """Compute matches: vector_score (role/domain/culture) + skill penalty, top 20 per job.

    Optional (deferred): pre-filter to candidates with candidate_role_fitness.fitness_score >= 60
    for a role matching the job; see plan matchmaking_scoring_and_shortlist.
    """
    context.log.info(
        f"Computing matches for job partition {context.partition_key} (vector + skill penalty, top 20 per job)"
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

    normalized_candidates = _to_candidate_list(normalized_candidates)
    candidate_vectors = _to_vector_list(candidate_vectors)
    if not normalized_jobs or not normalized_candidates:
        context.log.info("No jobs or candidates; skipping")
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
                role_sim = max(_cosine_similarity(job_role_vec, cvecs[k]) for k in position_keys)
            elif job_role_vec and cvecs.get("experience"):
                role_sim = _cosine_similarity(job_role_vec, cvecs["experience"])
            else:
                role_sim = 0.0

            domain_sim = (
                _cosine_similarity(job_domain_vec, cvecs["domain"])
                if job_domain_vec and cvecs.get("domain")
                else 0.0
            )
            culture_sim = (
                _cosine_similarity(job_personality_vec, cvecs["personality"])
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

            skill_coverage = _skill_coverage_score(
                req_skills, cand_skills_map_for_cand, jvecs, cvecs
            )
            skill_semantic = _skill_semantic_score(
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
            seniority_penalty, experience_match_score = _seniority_penalty_and_experience_score(
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
            compensation_match_score = _compensation_fit(
                job_salary_min,
                job_salary_max,
                comp_min,
                comp_max,
            )

            cand_timezone = candidate.get("timezone")
            location_match_score = _location_score(cand_timezone, job_timezone, job_location_type)

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

    context.log.info(f"Computed {len(match_results)} matches ({TOP_N_PER_JOB} per job)")
    return match_results


# ═══════════════════════════════════════════════════════════════════════════════
# ATS UPLOAD: push match results back to Airtable ATS table
# ═══════════════════════════════════════════════════════════════════════════════

ATS_AI_PROPOSED_FIELD = "AI PROPOSTED CANDIDATES"
ATS_JOB_STATUS_FIELD = "Job Status"
ATS_MATCHMAKING_DONE_STATUS = "Matchmaking Done "  # trailing space matches Airtable choice


@asset(
    partitions_def=job_partitions,
    ins={"matches": AssetIn()},
    description="Upload top match results to ATS table as linked candidate chips and set Job Status to Matchmaking Done",
    group_name="matching",
    required_resource_keys={"airtable_ats"},
    code_version="1.3.0",
)
def upload_matches_to_ats(
    context: AssetExecutionContext,
    matches: list[dict[str, Any]],
) -> None:
    """Write matched candidates as linked records on the ATS row and flip status."""
    from talent_matching.models.candidates import NormalizedCandidate

    record_id = context.partition_key
    ats = context.resources.airtable_ats
    context.log.info(f"Uploading {len(matches)} matches to ATS for job {record_id}")

    current_record = ats.fetch_record_by_id(record_id)
    current_status = current_record.get("fields", {}).get(ATS_JOB_STATUS_FIELD)
    should_flip_status = current_status == "Matchmaking Ready"

    if not should_flip_status:
        context.log.info(
            f"Job Status is '{current_status}', not 'Matchmaking Ready' "
            f"— will upload matches but skip status change"
        )

    if not matches:
        if should_flip_status:
            context.log.warning(
                f"No matches for {record_id} — setting status to Matchmaking Done anyway"
            )
            ats.update_record(record_id, {ATS_JOB_STATUS_FIELD: ATS_MATCHMAKING_DONE_STATUS})
        else:
            context.log.warning(f"No matches for {record_id} and status not flippable — skipping")
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
        f"Mapped {len(linked_record_ids)}/{len(matches)} candidates to Airtable record IDs"
    )

    fields: dict[str, Any] = {}
    if should_flip_status:
        fields[ATS_JOB_STATUS_FIELD] = ATS_MATCHMAKING_DONE_STATUS
    if linked_record_ids:
        fields[ATS_AI_PROPOSED_FIELD] = linked_record_ids

    if fields:
        ats.update_record(record_id, fields)

    status_msg = (
        f"set Job Status to '{ATS_MATCHMAKING_DONE_STATUS}'"
        if should_flip_status
        else "Job Status unchanged"
    )
    context.log.info(
        f"Uploaded {len(linked_record_ids)} candidate chips to '{ATS_AI_PROPOSED_FIELD}' "
        f"({status_msg}) for {record_id}"
    )
