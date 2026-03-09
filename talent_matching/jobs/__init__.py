"""Dagster jobs for the talent matching pipeline.

Jobs available in the Dagster dashboard:

ASSET JOBS (for materializing assets):
- candidate_pipeline: Process candidates through full pipeline (partitioned)
- candidate_ingest: Fetch and store raw candidates only (no LLM)
- upload_normalized_to_airtable_job: Upload normalized candidate data to Airtable (N) columns only (partitioned)

OPS JOBS (for operational tasks):
- sync_airtable_candidates_job: Register Airtable candidate records as dynamic partitions
- sync_airtable_jobs_job: Register Airtable job records as dynamic partitions
- sample_candidates_job: Fetch 20 candidates and log data quality stats

USAGE:
1. Run sync_airtable_candidates_job to register candidate partitions from Airtable
2. Go to Jobs → candidate_pipeline → Backfill
3. Select partitions (all, or first N for testing) and launch
"""

from talent_matching.jobs.asset_jobs import (
    ats_matchmaking_pipeline_job,
    candidate_ingest_job,
    candidate_pipeline_job,
    candidate_vectors_job,
    job_ingest_job,
    job_pipeline_job,
    matchmaking_backfill_job,
    upload_normalized_jobs_to_airtable_job,
    upload_normalized_to_airtable_job,
)
from talent_matching.jobs.skill_normalization_job import (
    skill_normalization_job,
    skill_normalization_schedule,
)
from talent_matching.jobs.sync_and_sample_jobs import (
    sample_candidates_job,
    sync_airtable_candidates_job,
    sync_airtable_jobs_job,
)
from talent_matching.jobs.timezone_lookup_job import (
    timezone_lookup_job,
    timezone_lookup_schedule,
)

__all__ = [
    "ats_matchmaking_pipeline_job",
    "candidate_ingest_job",
    "candidate_pipeline_job",
    "candidate_vectors_job",
    "job_ingest_job",
    "job_pipeline_job",
    "matchmaking_backfill_job",
    "sample_candidates_job",
    "skill_normalization_job",
    "skill_normalization_schedule",
    "sync_airtable_candidates_job",
    "sync_airtable_jobs_job",
    "timezone_lookup_job",
    "timezone_lookup_schedule",
    "upload_normalized_jobs_to_airtable_job",
    "upload_normalized_to_airtable_job",
]
