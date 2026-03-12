"""Run failure sensor that tags failed runs with classified failure reasons.

Known failures are tagged with a specific category (e.g. PDF_EXTRACTION_FAILED).
Unknown failures are tagged with UNKNOWN_FAILURE so they surface for investigation.

When candidate_pipeline fails at normalization (MISSING_DESIRED_JOB_CATEGORY or
INSUFFICIENT_CV_DATA), clears that partition's DB data so no stale normalized/
vector/match data remains.
"""

import dagster as dg

from talent_matching.db import get_session
from talent_matching.utils.clear_candidate_data import clear_candidate_partition_data

FAILURE_TAG = "failure_type"

# Failure types that indicate normalization failed; we clear partition DB data.
NORMALIZATION_FAILURE_TAGS = {"MISSING_DESIRED_JOB_CATEGORY", "INSUFFICIENT_CV_DATA"}
CANDIDATE_PIPELINE_JOB = "candidate_pipeline"

KNOWN_FAILURES: list[tuple[str, list[str]]] = [
    (
        "PDF_INVALID",
        ["Invalid PDF", "FileDataError", "Failed to open stream", "FzErrorFormat"],
    ),
    (
        "PDF_DOWNLOAD_FAILED",
        ["PDF download failed", "ConnectError", "TimeoutException"],
    ),
    (
        "OPENROUTER_API_ERROR",
        ["openrouter.ai", "HTTPStatusError", "422 Unprocessable Entity"],
    ),
    (
        "INVALID_ENUM_VALUE",
        ["InvalidTextRepresentation", "invalid input value for enum"],
    ),
    (
        "STRING_TRUNCATION",
        ["StringDataRightTruncation", "value too long for type"],
    ),
    (
        "LLM_JSON_PARSE_ERROR",
        ["JSONDecodeError", "Expecting value"],
    ),
    (
        "AIRTABLE_API_ERROR",
        ["airtable.com", "AUTHENTICATION_REQUIRED", "TABLE_NOT_FOUND"],
    ),
    (
        "RATE_LIMIT",
        ["429", "Too Many Requests", "rate limit"],
    ),
    (
        "CONCURRENCY_SLOTS_ERROR",
        ["concurrency_limits", "concurrency_slots", "NotNullViolation"],
    ),
    (
        "INSUFFICIENT_NARRATIVE_DATA",
        ["InsufficientNarrativeDataError", "experience is empty.; domain is empty"],
    ),
    (
        "MISSING_DESIRED_JOB_CATEGORY",
        ["MissingDesiredJobCategoryError", "no desired job category"],
    ),
    (
        "INSUFFICIENT_CV_DATA",
        ["InsufficientCvDataError", "No CV data", "total_cv_length"],
    ),
]


def _classify_failure(error_str: str) -> list[str]:
    """Return all matching failure tags for the given error string."""
    tags = []
    for tag, patterns in KNOWN_FAILURES:
        if any(p.lower() in error_str.lower() for p in patterns):
            tags.append(tag)
    return tags


@dg.run_failure_sensor(
    name="run_failure_tagger",
    description=(
        "Tags failed runs with classified failure reasons. "
        "Known failures get a specific tag; unknown failures get UNKNOWN_FAILURE. "
        "Sends Telegram alert for critical/unknown failures."
    ),
    default_status=dg.DefaultSensorStatus.RUNNING,
)
def run_failure_tagger(context: dg.RunFailureSensorContext) -> None:
    run_id = context.dagster_run.run_id
    job_name = context.dagster_run.job_name

    all_tags: set[str] = set()

    for event in context.get_step_failure_events():
        failure_data = event.step_failure_data
        if failure_data is None:
            continue
        error = failure_data.error
        if error is None:
            continue

        all_tags.update(_classify_failure(error.to_string()))

    if not all_tags:
        job_failure = context.failure_event.job_failure_data
        if job_failure is not None and job_failure.error is not None:
            all_tags.update(_classify_failure(job_failure.error.to_string()))

    if not all_tags:
        all_tags.add("UNKNOWN_FAILURE")

    tag_value = ", ".join(sorted(all_tags))
    context.instance.add_run_tags(run_id, {FAILURE_TAG: tag_value})

    context.log.info(f"Tagged failed run {run_id} ({job_name}) with {FAILURE_TAG}={tag_value}")

    # Clear partition DB data when candidate_pipeline fails at normalization
    if job_name == CANDIDATE_PIPELINE_JOB and (all_tags & NORMALIZATION_FAILURE_TAGS):
        partition_key = getattr(context, "partition_key", None)
        if partition_key:
            session = get_session()
            if clear_candidate_partition_data(session, partition_key):
                session.commit()
                context.log.info(
                    f"Cleared DB data for partition {partition_key} (normalization failure)"
                )
            session.close()

    # Telegram alert for critical failures: UNKNOWN_FAILURE or multiple known types
    should_alert = "UNKNOWN_FAILURE" in all_tags or len(all_tags) > 1
    if should_alert and hasattr(context.resources, "telegram"):
        telegram = context.resources.telegram
        if telegram.enabled and telegram.bot_token and telegram.chat_id:
            telegram.send_alert(
                f"Run Failed: {job_name}",
                f"Run {run_id}\nFailure types: {tag_value}",
            )
