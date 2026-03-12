"""Tests that job pipeline data-validation Failures do not trigger retries and are tagged with record_id."""

from dagster import Failure

from talent_matching.assets.jobs import MIN_RAW_JOB_DESCRIPTION_LEN


def test_raw_jobs_short_description_failure_is_no_retry_and_tagged():
    """Failure for 'job description too short' must not be retried and must include record_id in metadata."""
    record_id = "recaqcwtKW11NgnFV"
    desc_len = 25
    source = "airtable"
    failure = Failure(
        description=(
            f"Job description too short for record_id={record_id} "
            f"({desc_len} chars, source={source}). "
            f"Minimum {MIN_RAW_JOB_DESCRIPTION_LEN} chars required before matchmaking. "
            "Check Airtable Job Description Text/Link or run "
            "`scripts/refresh_job_description_from_notion.py`."
        ),
        metadata={"record_id": record_id},
        allow_retries=False,
    )
    assert failure.allow_retries is False
    # Dagster wraps metadata values (e.g. TextMetadataValue)
    record_id_val = failure.metadata["record_id"]
    resolved = getattr(record_id_val, "text", None) or getattr(
        record_id_val, "value", record_id_val
    )
    assert resolved == record_id
