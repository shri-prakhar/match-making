"""Smoke tests: definitions and jobs load without error after refactor."""

from talent_matching.definitions import (
    all_jobs,
    all_schedules,
    all_sensors,
    defs,
    get_environment,
)
from talent_matching.jobs import (
    ats_matchmaking_pipeline_job,
    candidate_ingest_job,
    candidate_pipeline_job,
    candidate_vectors_job,
    job_ingest_job,
    job_pipeline_job,
    matchmaking_backfill_job,
    sample_candidates_job,
    skill_normalization_job,
    skill_normalization_schedule,
    sync_airtable_candidates_job,
    sync_airtable_jobs_job,
    timezone_lookup_job,
    timezone_lookup_schedule,
    upload_normalized_jobs_to_airtable_job,
    upload_normalized_to_airtable_job,
)


class TestDefinitionsLoad:
    """Definitions and job exports load correctly."""

    def test_defs_has_assets(self):
        assert defs.assets is not None
        assert len(defs.assets) > 0

    def test_defs_has_jobs(self):
        assert defs.jobs is not None
        assert len(defs.jobs) == len(all_jobs)

    def test_defs_has_schedules(self):
        assert defs.schedules is not None
        assert len(defs.schedules) == len(all_schedules)

    def test_defs_has_sensors(self):
        assert defs.sensors is not None
        assert len(defs.sensors) == len(all_sensors)

    def test_get_environment_returns_string(self):
        env = get_environment()
        assert isinstance(env, str)
        assert env in ("development", "staging", "production")


class TestJobsExports:
    """All expected jobs and schedules are exported from talent_matching.jobs."""

    def test_asset_jobs_exported(self):
        assert candidate_pipeline_job.name == "candidate_pipeline"
        assert candidate_vectors_job.name == "candidate_vectors_backfill"
        assert candidate_ingest_job.name == "candidate_ingest"
        assert upload_normalized_to_airtable_job.name == "upload_normalized_to_airtable"
        assert job_pipeline_job.name == "job_pipeline"
        assert job_ingest_job.name == "job_ingest"
        assert upload_normalized_jobs_to_airtable_job.name == "upload_normalized_jobs_to_airtable"
        assert ats_matchmaking_pipeline_job.name == "ats_matchmaking_pipeline"
        assert matchmaking_backfill_job.name == "matchmaking_backfill"

    def test_ops_jobs_exported(self):
        assert sync_airtable_candidates_job.name == "sync_airtable_candidates_job"
        assert sync_airtable_jobs_job.name == "sync_airtable_jobs_job"
        assert sample_candidates_job.name == "sample_candidates_job"
        assert skill_normalization_job.name == "skill_normalization_job"
        assert timezone_lookup_job.name == "timezone_lookup_job"

    def test_schedules_exported(self):
        assert skill_normalization_schedule.name == "skill_normalization_daily"
        assert timezone_lookup_schedule.name == "timezone_lookup_daily"

    def test_all_jobs_count(self):
        assert len(all_jobs) == 14

    def test_all_schedules_count(self):
        assert len(all_schedules) == 2
