"""Airtable sensors for detecting changes in candidate records.

Sensors:
- airtable_candidate_sensor: Cursor-based incremental sync for candidates.
"""

import json
from collections.abc import Generator
from datetime import UTC, datetime

from dagster import (
    RunRequest,
    SensorEvaluationContext,
    SkipReason,
    sensor,
)

from talent_matching.assets.candidates import candidate_partitions
from talent_matching.jobs import candidate_pipeline_job


@sensor(
    job=candidate_pipeline_job,
    minimum_interval_seconds=900,  # Check every 15 minutes (Airtable API is slow ~20s per full poll)
    description="Polls Airtable for new or updated candidate records using incremental sync",
    required_resource_keys={"airtable"},
)
def airtable_candidate_sensor(
    context: SensorEvaluationContext,
) -> Generator[RunRequest, None, SkipReason | None]:
    """Detect new and updated candidates in Airtable using cursor-based sync.

    Cursor format (JSON):
    {
        "last_sync": "2024-01-15T10:30:00.000Z",  # ISO timestamp
        "initialized": true                        # Whether initial sync completed
    }

    Behavior:
    - First run (no cursor): Full sync of all record IDs
    - Subsequent runs: Only fetch records modified since last_sync
    - For existing partitions, triggers re-run (Dagster's data versioning handles skip)
    """
    airtable = context.resources.airtable
    partitions_name = candidate_partitions.name or "candidates"

    # Parse cursor (if exists)
    cursor_data = {"initialized": False, "last_sync": None}
    if context.cursor:
        cursor_data = json.loads(context.cursor)

    # Track current sync time (before we start fetching)
    current_sync_time = datetime.now(UTC).isoformat()

    # Get existing partition keys
    existing_partitions = set(
        context.instance.get_dynamic_partitions(partitions_def_name=partitions_name)
    )

    if not cursor_data.get("initialized"):
        # First run: Full sync to establish baseline
        context.log.info("First sync - fetching all record IDs from Airtable...")

        all_record_ids = airtable.get_all_record_ids()
        context.log.info(f"Found {len(all_record_ids)} total records in Airtable")

        # Find new records not yet in partitions
        new_record_ids = [rid for rid in all_record_ids if rid not in existing_partitions]

        if new_record_ids:
            context.log.info(f"Adding {len(new_record_ids)} new partitions...")
            context.instance.add_dynamic_partitions(
                partitions_def_name=partitions_name,
                partition_keys=new_record_ids,
            )

        # Update cursor
        new_cursor = json.dumps(
            {
                "initialized": True,
                "last_sync": current_sync_time,
            }
        )
        context.update_cursor(new_cursor)

        # Yield run requests for new records
        for record_id in new_record_ids:
            yield RunRequest(
                run_key=f"candidate-init-{record_id}",
                partition_key=record_id,
            )

        if not new_record_ids:
            return SkipReason("Initial sync complete. No new records to process.")

    else:
        # Incremental sync: Only fetch modified records
        last_sync = cursor_data.get("last_sync")
        context.log.info(f"Incremental sync - checking for changes since {last_sync}")

        modified_records = airtable.fetch_records_modified_since(last_sync)
        context.log.info(f"Found {len(modified_records)} modified records")

        if not modified_records:
            # Update cursor even if no changes (to move the window forward)
            new_cursor = json.dumps(
                {
                    "initialized": True,
                    "last_sync": current_sync_time,
                }
            )
            context.update_cursor(new_cursor)
            return SkipReason(f"No changes since {last_sync}")

        # Separate new vs updated records
        new_records = []
        updated_records = []

        for record in modified_records:
            record_id = record.get("airtable_record_id")
            if record_id in existing_partitions:
                updated_records.append(record_id)
            else:
                new_records.append(record_id)

        context.log.info(
            f"New records: {len(new_records)}, Updated records: {len(updated_records)}"
        )

        # Add new partitions
        if new_records:
            context.instance.add_dynamic_partitions(
                partitions_def_name=partitions_name,
                partition_keys=new_records,
            )

        # Update cursor
        new_cursor = json.dumps(
            {
                "initialized": True,
                "last_sync": current_sync_time,
            }
        )
        context.update_cursor(new_cursor)

        # Yield run requests for all modified records (new + updated)
        for record_id in new_records:
            context.log.info(f"Triggering pipeline for NEW candidate: {record_id}")
            yield RunRequest(
                run_key=f"candidate-new-{record_id}-{current_sync_time}",
                partition_key=record_id,
            )

        for record_id in updated_records:
            context.log.info(f"Triggering pipeline for UPDATED candidate: {record_id}")
            yield RunRequest(
                run_key=f"candidate-update-{record_id}-{current_sync_time}",
                partition_key=record_id,
            )
        return None
