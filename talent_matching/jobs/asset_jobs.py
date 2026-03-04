"""Asset job definitions (partitioned pipelines)."""

from dagster import Backoff, Jitter, RetryPolicy, define_asset_job

from talent_matching.assets.candidates import (
    airtable_candidate_sync,
    airtable_candidates,
    candidate_github_commit_history,
    candidate_partitions,
    candidate_role_fitness,
    candidate_skill_verification,
    candidate_vectors,
    normalized_candidates,
    raw_candidates,
)
from talent_matching.assets.jobs import (
    airtable_job_sync,
    airtable_jobs,
    job_partitions,
    job_vectors,
    llm_refined_shortlist,
    matches,
    normalized_jobs,
    raw_jobs,
    upload_matches_to_ats,
)

# Retry policy for API calls (rate limits, transient errors)
openrouter_retry_policy = RetryPolicy(
    max_retries=3,
    delay=1,
    backoff=Backoff.EXPONENTIAL,
    jitter=Jitter.PLUS_MINUS,
)

candidate_pipeline_job = define_asset_job(
    name="candidate_pipeline",
    description=(
        "Process candidates through the full pipeline: "
        "fetch → store → normalize (LLM) → vectorize → role fitness → Airtable write-back. "
        "Use Backfill to select partitions."
    ),
    selection=[
        airtable_candidates,
        raw_candidates,
        normalized_candidates,
        candidate_vectors,
        candidate_role_fitness,
        candidate_github_commit_history,
        candidate_skill_verification,
        airtable_candidate_sync,
    ],
    partitions_def=candidate_partitions,
    op_retry_policy=openrouter_retry_policy,
)

candidate_vectors_job = define_asset_job(
    name="candidate_vectors_backfill",
    description=(
        "Re-vectorize candidate profiles only (no LLM normalization). "
        "Uses already-materialized normalized_candidates as input. "
        "Use this instead of asset backfills to avoid the first-tick submission storm "
        "that blocks completion tracking in the asset backfill daemon."
    ),
    selection=[candidate_vectors],
    partitions_def=candidate_partitions,
    op_retry_policy=openrouter_retry_policy,
)

candidate_ingest_job = define_asset_job(
    name="candidate_ingest",
    description=(
        "Fetch and store raw candidate data from Airtable (no LLM normalization). "
        "Use this for initial data loading before running normalization."
    ),
    selection=[
        airtable_candidates,
        raw_candidates,
    ],
    partitions_def=candidate_partitions,
)

upload_normalized_to_airtable_job = define_asset_job(
    name="upload_normalized_to_airtable",
    description=(
        "Upload normalized candidate data to Airtable (N)-prefixed columns only. "
        "Uses already-materialized normalized_candidates; does not re-run normalization. "
        "Use Backfill to select which candidate partitions to sync."
    ),
    selection=[airtable_candidate_sync],
    partitions_def=candidate_partitions,
)

job_pipeline_job = define_asset_job(
    name="job_pipeline",
    description=("Process jobs: fetch → raw → normalize (LLM) → vectorize → Airtable write-back."),
    selection=[
        airtable_jobs,
        raw_jobs,
        normalized_jobs,
        job_vectors,
        airtable_job_sync,
    ],
    partitions_def=job_partitions,
    op_retry_policy=openrouter_retry_policy,
)

job_ingest_job = define_asset_job(
    name="job_ingest",
    description=(
        "Fetch and store raw job data from Airtable (Notion fetch, no LLM). "
        "Use for initial load before running normalization."
    ),
    selection=[
        airtable_jobs,
        raw_jobs,
    ],
    partitions_def=job_partitions,
)

upload_normalized_jobs_to_airtable_job = define_asset_job(
    name="upload_normalized_jobs_to_airtable",
    description=(
        "Upload normalized job data to Airtable (N)-prefixed columns. "
        "Uses already-materialized normalized_jobs; does not re-run normalization. "
        "Use Backfill to select which job partitions to sync."
    ),
    selection=[airtable_job_sync],
    partitions_def=job_partitions,
)

ats_matchmaking_pipeline_job = define_asset_job(
    name="ats_matchmaking_pipeline",
    description=(
        "Normalize a job, compute vectors, score matches, LLM-refine shortlist, and upload to ATS. "
        "Triggered by ats_matchmaking_sensor after it ingests the raw job to Postgres. "
        "Writes top 15 candidates as linked chips to 'AI PROPOSTED CANDIDATES' and "
        "sets Job Status to 'Matchmaking Done'."
    ),
    selection=[normalized_jobs, job_vectors, matches, llm_refined_shortlist, upload_matches_to_ats],
    partitions_def=job_partitions,
    op_retry_policy=openrouter_retry_policy,
    tags={"dagster/concurrency_limit": "matchmaking"},
)
