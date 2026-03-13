"""ATS matchmaking sensor: triggers job normalization + matchmaking when Job Status
changes to "Matchmaking Ready" on the ATS table.

Flow:
1. Poll ATS for records with Job Status = "Matchmaking Ready"
2. For each record: ingest raw job data to Postgres, create partition
3. Track triggered records in sensor cursor (prevents re-triggering)
4. Trigger ats_matchmaking_pipeline_job (normalize → vectors → matches)

After the pipeline succeeds, a separate mechanism (or manual step) sets
Job Status to "Matchmaking Done" on the ATS record.
"""

import json
from collections.abc import Generator
from datetime import UTC, datetime
from uuid import uuid4

from dagster import (
    RunRequest,
    SensorEvaluationContext,
    SkipReason,
    sensor,
)
from sqlalchemy.dialects.postgresql import insert

from talent_matching.assets.jobs import job_partitions
from talent_matching.db import get_session
from talent_matching.jobs import ats_matchmaking_pipeline_job
from talent_matching.models.enums import ProcessingStatusEnum
from talent_matching.models.raw import RawJob
from talent_matching.utils.airtable_mapper import (
    ATS_JOB_CATEGORY_FIELD_NAMES,
    ATS_LOCATION_FIELD_NAMES,
    ATS_REQUIRED_FIELD_NAMES,
    require_airtable_field,
    require_airtable_field_one_of,
    require_airtable_record_fields,
)


def _resolve_notion_description(
    job_description: str, link: str | None, notion_resource: object | None
) -> str:
    """If the job description is empty but a Notion link is provided, fetch it."""
    if (
        job_description
        and job_description.strip()
        and job_description != "(No description provided)"
    ):
        return job_description
    if not link or not isinstance(link, str):
        return job_description or "(No description provided)"
    if "notion.site" not in link and "notion.so" not in link:
        return job_description or "(No description provided)"
    if notion_resource is None:
        return job_description or "(No description provided)"
    content = notion_resource.fetch_page_content(link)
    return content or job_description or "(No content from Notion)"


def _map_ats_record_to_raw_job(record: dict) -> dict:
    """Map an ATS record to RawJob-compatible fields (standalone, no resource needed).

    Raises AirtableFieldMissingError if any required Airtable field is missing.
    """
    fields = require_airtable_record_fields(record, ATS_REQUIRED_FIELD_NAMES, table_hint="ATS")
    record_id = str(record["id"])

    company_links = require_airtable_field(fields, "Company", record_id=record_id, table_hint="ATS")
    company_name = None
    if isinstance(company_links, list) and company_links:
        company_name = company_links[0] if isinstance(company_links[0], str) else None

    location_values = require_airtable_field_one_of(
        fields, ATS_LOCATION_FIELD_NAMES, record_id=record_id, table_hint="ATS"
    )
    location_raw = ", ".join(location_values) if isinstance(location_values, list) else None

    level_values = require_airtable_field(fields, "Level", record_id=record_id, table_hint="ATS")
    level_raw = ", ".join(level_values) if isinstance(level_values, list) else None

    category_values = require_airtable_field_one_of(
        fields, ATS_JOB_CATEGORY_FIELD_NAMES, record_id=record_id, table_hint="ATS"
    )
    category_raw = ", ".join(category_values) if isinstance(category_values, list) else None

    work_setup = require_airtable_field(
        fields, "Work Set Up Preference", record_id=record_id, table_hint="ATS"
    )
    work_setup_raw = ", ".join(work_setup) if isinstance(work_setup, list) else None

    return {
        "airtable_record_id": record_id,
        "source": "airtable_ats",
        "source_id": record_id,
        "source_url": require_airtable_field(
            fields, "Job Description Link", record_id=record_id, table_hint="ATS"
        ),
        "job_title": require_airtable_field(
            fields, "Open Position (Job Title)", record_id=record_id, table_hint="ATS"
        ),
        "company_name": company_name,
        "job_description": require_airtable_field(
            fields, "Job Description Text", record_id=record_id, table_hint="ATS"
        )
        or "",
        "company_website_url": None,
        "experience_level_raw": level_raw,
        "location_raw": location_raw,
        "work_setup_raw": work_setup_raw,
        "status_raw": require_airtable_field(
            fields, "Job Status", record_id=record_id, table_hint="ATS"
        ),
        "job_category_raw": category_raw,
        "x_url": None,
        "non_negotiables": require_airtable_field(
            fields, "Non Negotiables", record_id=record_id, table_hint="ATS"
        ),
        "nice_to_have": require_airtable_field(
            fields, "Nice-to-have", record_id=record_id, table_hint="ATS"
        ),
        "projected_salary": require_airtable_field(
            fields, "Projected Salary", record_id=record_id, table_hint="ATS"
        ),
    }


def _ingest_raw_job(
    record: dict, notion_resource: object | None, log: object
) -> tuple[str, dict] | None:
    """Write a RawJob to Postgres from an ATS record.

    Returns (record_id, mapped) with mapped containing the final job_description
    (from Notion if link present and Airtable description empty). Returns None on failure.
    """
    mapped = _map_ats_record_to_raw_job(record)
    record_id = mapped["airtable_record_id"]
    if not record_id:
        return None

    raw_description = mapped.get("job_description") or ""
    link = mapped.get("source_url")
    mapped["job_description"] = _resolve_notion_description(raw_description, link, notion_resource)

    if not mapped["job_description"] or mapped["job_description"] == "(No description provided)":
        log.warning(f"[ats_matchmaking_sensor] record_id={record_id} No job description; skipping")
        return None

    session = get_session()
    stmt = insert(RawJob).values(
        id=uuid4(),
        airtable_record_id=record_id,
        source=mapped["source"],
        source_id=mapped["source_id"],
        source_url=mapped.get("source_url"),
        job_title=mapped.get("job_title"),
        company_name=mapped.get("company_name"),
        job_description=mapped["job_description"],
        company_website_url=mapped.get("company_website_url"),
        experience_level_raw=mapped.get("experience_level_raw"),
        location_raw=mapped.get("location_raw"),
        work_setup_raw=mapped.get("work_setup_raw"),
        status_raw=mapped.get("status_raw"),
        job_category_raw=mapped.get("job_category_raw"),
        x_url=mapped.get("x_url"),
        non_negotiables=mapped.get("non_negotiables"),
        nice_to_have=mapped.get("nice_to_have"),
        projected_salary=mapped.get("projected_salary"),
        processing_status=ProcessingStatusEnum.PENDING,
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=["airtable_record_id"],
        set_={
            "source": mapped["source"],
            "job_title": mapped.get("job_title"),
            "company_name": mapped.get("company_name"),
            "job_description": mapped["job_description"],
            "experience_level_raw": mapped.get("experience_level_raw"),
            "location_raw": mapped.get("location_raw"),
            "work_setup_raw": mapped.get("work_setup_raw"),
            "status_raw": mapped.get("status_raw"),
            "job_category_raw": mapped.get("job_category_raw"),
            "non_negotiables": mapped.get("non_negotiables"),
            "nice_to_have": mapped.get("nice_to_have"),
            "projected_salary": mapped.get("projected_salary"),
            "processing_status": ProcessingStatusEnum.PENDING,
        },
    )
    session.execute(stmt)
    session.commit()
    session.close()

    log.info(
        f"[ats_matchmaking_sensor] Ingested raw job {record_id}: "
        f"{mapped.get('job_title', 'No title')} (desc={len(mapped['job_description'])} chars)"
    )
    return (record_id, mapped)


@sensor(
    job=ats_matchmaking_pipeline_job,
    minimum_interval_seconds=120,
    description=(
        "Polls ATS table for records with Job Status = 'Matchmaking Ready'. "
        "Ingests raw job data to Postgres, creates partition, and triggers "
        "normalization + matchmaking."
    ),
    required_resource_keys={"airtable_ats", "notion"},
)
def ats_matchmaking_sensor(
    context: SensorEvaluationContext,
) -> Generator[RunRequest, None, SkipReason | None]:
    """Detect ATS jobs ready for matchmaking and trigger the full pipeline.

    Uses a cursor to track which records have already been triggered so we
    don't re-trigger on every poll while the status is still "Matchmaking Ready".
    The cursor stores a JSON dict: {"triggered": {"recXXX": "2026-02-25T...", ...}}.
    A record is re-triggered if its status goes back to "Matchmaking Ready"
    after the sensor has already processed it (e.g., recruiter re-requests).
    """
    ats = context.resources.airtable_ats
    notion = context.resources.notion
    partitions_name = job_partitions.name or "jobs"

    cursor_data: dict[str, object] = {"triggered": {}}
    if context.cursor:
        cursor_data = json.loads(context.cursor)

    raw_triggered = cursor_data.get("triggered") or {}
    triggered_map: dict[str, str] = (
        {k: v for k, v in raw_triggered.items() if isinstance(k, str) and isinstance(v, str)}
        if isinstance(raw_triggered, dict)
        else {}
    )

    records = ats.fetch_records_by_status("Matchmaking Ready")
    if not records:
        return SkipReason("No ATS jobs with status 'Matchmaking Ready'")

    # Filter out records we've already triggered (still showing "Matchmaking Ready"
    # because the pipeline hasn't set it to "Matchmaking Done" yet)
    new_records = [r for r in records if r.get("id") not in triggered_map]
    if not new_records:
        return SkipReason(
            f"{len(records)} ATS jobs at 'Matchmaking Ready' but all already triggered"
        )

    context.log.info(
        f"[ats_matchmaking_sensor] Found {len(new_records)} new jobs ready "
        f"(total at Matchmaking Ready: {len(records)}, already triggered: {len(triggered_map)})"
    )

    existing_partitions = set(
        context.instance.get_dynamic_partitions(partitions_def_name=partitions_name)
    )
    now_iso = datetime.now(UTC).isoformat()

    for record in new_records:
        record_id = record.get("id")
        if not record_id:
            continue

        title = (record.get("fields", {}).get("Open Position (Job Title)") or "Untitled")[:80]
        context.log.info(f"[ats_matchmaking_sensor] Processing job: {record_id} — {title}")

        result = _ingest_raw_job(record, notion, context.log)
        if not result:
            continue

        if record_id not in existing_partitions:
            context.instance.add_dynamic_partitions(
                partitions_def_name=partitions_name,
                partition_keys=[record_id],
            )
            context.log.info(f"[ats_matchmaking_sensor] Created partition for {record_id}")

        triggered_map[record_id] = now_iso

        yield RunRequest(
            run_key=f"ats-matchmaking-{record_id}-{now_iso}",
            partition_key=record_id,
            tags={"dagster/priority": "10"},
        )

    # Prune triggered entries whose status is no longer "Matchmaking Ready"
    # (they completed or were moved to another status)
    current_ready_ids = {r.get("id") for r in records}
    triggered_map = {k: v for k, v in triggered_map.items() if k in current_ready_ids}

    context.update_cursor(json.dumps({"triggered": triggered_map}))
    return None
