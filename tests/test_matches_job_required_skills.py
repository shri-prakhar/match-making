"""Unit tests to prevent matchmaking ignoring job required skills.

When normalized_jobs and matches run in the same Dagster run, the matches asset
receives the upstream return value (LLM payload) which has no DB-assigned "id".
If we only used job.get("id"), job_ids would be empty and get_job_required_skills([])
would return {}, so required/nice-to-have skills would never be considered.

We resolve job id from the DB by partition (airtable_record_id) so that
job_required_skills is always loaded. This module tests that resolution logic.
"""

from talent_matching.assets.jobs import resolve_job_ids_for_required_skills


class TestResolveJobIdsForRequiredSkills:
    """Tests for resolve_job_ids_for_required_skills.

    These tests would fail before the fix (job_required_skills was empty when
    normalized_jobs and matches ran in the same run).
    """

    def test_resolves_job_id_when_payload_has_no_id(self):
        """When normalized_jobs item has no 'id', we must resolve from partition so required skills are loaded."""
        normalized_jobs = [
            {
                "raw_job_id": "raw-123",
                "airtable_record_id": "recJobXYZ",
                "job_title": "Growth Analyst",
            },
        ]
        record_id = "recJobXYZ"
        resolved_uuid = "f48a1980-4b04-4a12-b2b4-1545c9399e58"

        def get_job_id(airtable_record_id: str):
            assert airtable_record_id == record_id
            return resolved_uuid

        job_ids = resolve_job_ids_for_required_skills(normalized_jobs, record_id, get_job_id)

        assert job_ids == [resolved_uuid], (
            "job_ids must be non-empty so get_job_required_skills loads skills; "
            "without resolution (when payload has no id) job_ids would be [] and skills ignored."
        )

    def test_uses_payload_id_when_present(self):
        """When payload already has id (e.g. loaded from IOManager), use it and do not call resolver."""
        payload_id = "existing-uuid-from-db"
        normalized_jobs = [{"id": payload_id, "raw_job_id": "raw-1"}]
        record_id = "recX"
        resolver_called = []

        def get_job_id(airtable_record_id: str):
            resolver_called.append(airtable_record_id)
            return "other-uuid"

        job_ids = resolve_job_ids_for_required_skills(normalized_jobs, record_id, get_job_id)

        assert job_ids == [payload_id]
        assert resolver_called == [], "Resolver must not be called when payload has id"

    def test_empty_job_ids_when_no_id_and_no_record_id(self):
        """When job has no id and record_id is None, we cannot resolve; job_ids is empty."""
        normalized_jobs = [{"raw_job_id": "raw-1"}]
        record_id = None

        job_ids = resolve_job_ids_for_required_skills(normalized_jobs, record_id, lambda _: "uuid")

        assert job_ids == []

    def test_empty_job_ids_when_no_id_and_resolver_returns_none(self):
        """When payload has no id and resolver returns None (job not in DB yet), job_ids is empty."""
        normalized_jobs = [{"raw_job_id": "raw-1"}]
        record_id = "recNew"

        job_ids = resolve_job_ids_for_required_skills(normalized_jobs, record_id, lambda _: None)

        assert job_ids == []

    def test_multiple_jobs_each_resolved_when_no_id(self):
        """With multiple jobs without id, each uses resolver for the same partition (single-partition run)."""
        normalized_jobs = [
            {"raw_job_id": "r1"},
            {"raw_job_id": "r2"},
        ]
        record_id = "recPartition"
        resolved = "job-uuid-123"

        job_ids = resolve_job_ids_for_required_skills(
            normalized_jobs, record_id, lambda aid: resolved if aid == record_id else None
        )

        assert job_ids == [resolved, resolved]
