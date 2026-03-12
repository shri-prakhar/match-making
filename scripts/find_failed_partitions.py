"""Find failed partitions from a backfill and print their error messages.

Queries the Dagster event_logs table for STEP_FAILURE events tied to
airtable_candidates and normalized_candidates steps.

Usage:
    poetry run with-remote-db python scripts/find_failed_partitions.py
    poetry run with-local-db python scripts/find_failed_partitions.py
    On server: poetry run python scripts/find_failed_partitions.py --local
"""

import json
import sys

from talent_matching.script_env import apply_local_db  # noqa: E402

apply_local_db()

from sqlalchemy import text  # noqa: E402

from talent_matching.db import get_session  # noqa: E402

BACKFILL_ID = sys.argv[1] if len(sys.argv) > 1 else None

session = get_session()

# Find failed runs for the backfill (or recent candidate_pipeline failures)
if BACKFILL_ID:
    print(f"Looking for failures in backfill: {BACKFILL_ID}\n")
    failed_runs_query = text("""
        SELECT r.run_id, r.partition, r.status, r.create_timestamp
        FROM runs r
        JOIN run_tags rt ON r.run_id = rt.run_id
        WHERE rt.key = 'dagster/backfill'
          AND rt.value = :backfill_id
          AND r.status = 'FAILURE'
        ORDER BY r.create_timestamp
    """)
    failed_runs = session.execute(failed_runs_query, {"backfill_id": BACKFILL_ID}).fetchall()
else:
    print("No backfill ID provided. Showing recent candidate_pipeline failures.\n")
    failed_runs_query = text("""
        SELECT r.run_id, r.partition, r.status, r.create_timestamp
        FROM runs r
        JOIN run_tags rt ON r.run_id = rt.run_id
        WHERE rt.key = 'dagster/job_name'
          AND rt.value = 'candidate_pipeline'
          AND r.status = 'FAILURE'
        ORDER BY r.create_timestamp DESC
        LIMIT 50
    """)
    failed_runs = session.execute(failed_runs_query).fetchall()

print(f"Found {len(failed_runs)} failed runs\n")

# For each failed run, find STEP_FAILURE events
for run in failed_runs:
    run_id = run[0]
    partition = run[1]
    created = run[3]

    step_failures = session.execute(
        text("""
            SELECT step_key, event_body
            FROM event_logs
            WHERE run_id = :run_id
              AND dagster_event_type = 'STEP_FAILURE'
            ORDER BY timestamp
        """),
        {"run_id": run_id},
    ).fetchall()

    if not step_failures:
        print(
            f"  [{partition}] run={run_id[:8]}... created={created} — no STEP_FAILURE events (run-level failure?)"
        )
        continue

    for step_key, event_body_raw in step_failures:
        body = json.loads(event_body_raw) if isinstance(event_body_raw, str) else event_body_raw
        error_info = body.get("__event_specific_data__", {}).get("error", {})
        error_cls = error_info.get("cls_name", "?")
        error_message = error_info.get("message", "")

        # Truncate long messages
        if len(error_message) > 500:
            error_message = error_message[:500] + "..."

        print(f"  [{partition}] step={step_key} error={error_cls}: {error_message}")

print(f"\n--- Summary: {len(failed_runs)} failed runs ---")

# Count failures by step
step_counts: dict[str, int] = {}
for run in failed_runs:
    run_id = run[0]
    steps = session.execute(
        text("""
            SELECT step_key FROM event_logs
            WHERE run_id = :run_id AND dagster_event_type = 'STEP_FAILURE'
        """),
        {"run_id": run_id},
    ).fetchall()
    for (step_key,) in steps:
        step_counts[step_key] = step_counts.get(step_key, 0) + 1

for step, count in sorted(step_counts.items(), key=lambda x: -x[1]):
    print(f"  {step}: {count} failures")

session.close()
