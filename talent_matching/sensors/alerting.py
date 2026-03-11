"""System health sensor: polls Dagster state and sends Telegram alerts.

Checks:
- High failure rate (>threshold in last N runs)
- Backfill completed (with stats)
- Stuck runs (STARTED for >threshold minutes)

Each check is a standalone function. Cursor state tracks cooldowns and backfill tracking.
"""

from __future__ import annotations

import json
from collections import Counter
from datetime import UTC, datetime, timedelta
from typing import Any

from dagster import (
    DagsterRunStatus,
    DefaultSensorStatus,
    RunsFilter,
    SensorEvaluationContext,
    SkipReason,
    sensor,
)
from dagster._core.execution.backfill import BulkActionStatus

FAILURE_TAG = "failure_type"

# In-progress backfill statuses (we track these; when they complete we alert)
BACKFILL_IN_PROGRESS = {
    BulkActionStatus.REQUESTED,
    BulkActionStatus.CANCELING,
    BulkActionStatus.FAILING,
}

# Terminal backfill statuses (completion)
BACKFILL_TERMINAL = {
    BulkActionStatus.COMPLETED,
    BulkActionStatus.FAILED,
    BulkActionStatus.CANCELED,
    BulkActionStatus.COMPLETED_SUCCESS,
    BulkActionStatus.COMPLETED_FAILED,
}


def _parse_cursor(cursor: str | None) -> dict[str, Any]:
    """Parse sensor cursor JSON."""
    if not cursor:
        return {
            "last_failure_rate_alert": None,
            "last_stuck_runs_alert": None,
            "tracked_backfills": {},
        }
    data = json.loads(cursor)
    return {
        "last_failure_rate_alert": data.get("last_failure_rate_alert"),
        "last_stuck_runs_alert": data.get("last_stuck_runs_alert"),
        "tracked_backfills": data.get("tracked_backfills", {}),
    }


def _check_failure_rate(
    context: SensorEvaluationContext,
    instance: Any,
    threshold: float,
    window_minutes: int,
    min_runs: int,
    cooldown_minutes: int,
    cursor_data: dict[str, Any],
) -> tuple[str | None, dict[str, Any]]:
    """Check if failure rate exceeds threshold. Returns (alert_body, updated_cursor)."""
    since = datetime.now(UTC) - timedelta(minutes=window_minutes)
    runs = instance.get_runs(
        limit=500,
        filters=RunsFilter(
            statuses=[DagsterRunStatus.SUCCESS, DagsterRunStatus.FAILURE],
            created_after=since,
        ),
    )
    total = len(runs)
    if total < min_runs:
        return None, cursor_data

    failures = [r for r in runs if r.status == DagsterRunStatus.FAILURE]
    failure_count = len(failures)
    rate = failure_count / total if total else 0
    if rate <= threshold:
        return None, cursor_data

    # Cooldown
    last = cursor_data.get("last_failure_rate_alert")
    if last:
        last_dt = datetime.fromisoformat(last.replace("Z", "+00:00"))
        if datetime.now(UTC) - last_dt < timedelta(minutes=cooldown_minutes):
            return None, cursor_data

    # Top failure types from tags
    type_counts: Counter[str] = Counter()
    for r in failures:
        tags = instance.get_run_tags(run_id=r.run_id)
        for k, v in tags:
            if k == FAILURE_TAG:
                for t in (x.strip() for x in v.split(",")):
                    if t:
                        type_counts[t] += 1
                break

    lines = [
        f"Failure rate: {int(rate * 100)}% ({failure_count}/{total} runs in the last {window_minutes} min)",
        "",
        "Top failure types:",
    ]
    for tag, count in type_counts.most_common(10):
        lines.append(f"  - {tag}: {count}")
    body = "\n".join(lines)

    new_cursor = dict(cursor_data)
    new_cursor["last_failure_rate_alert"] = datetime.now(UTC).isoformat()
    return body, new_cursor


def _check_backfills(
    context: SensorEvaluationContext,
    instance: Any,
    cursor_data: dict[str, Any],
) -> tuple[str | None, str | None, dict[str, Any]]:
    """Check for backfills that completed. Returns (alert_title, alert_body, updated_cursor)."""
    tracked = dict(cursor_data.get("tracked_backfills", {}))
    backfills = instance.get_backfills(limit=100)

    # Update tracked: add any in-progress
    for bf in backfills:
        bid = bf.backfill_id
        status = bf.status
        if status in BACKFILL_IN_PROGRESS:
            tracked[bid] = status.value
        elif status in BACKFILL_TERMINAL and bid in tracked:
            # Completed — build alert and remove from tracked
            num_partitions = bf.get_num_partitions() if hasattr(bf, "get_num_partitions") else 0
            failure_count = getattr(bf, "failure_count", 0) or 0
            success_count = max(0, num_partitions - failure_count) if num_partitions else 0

            start_ts = bf.backfill_timestamp
            end_ts = getattr(bf, "backfill_end_timestamp", None) or datetime.now(UTC)
            if isinstance(start_ts, int | float):
                start_dt = datetime.fromtimestamp(start_ts, tz=UTC)
            else:
                start_dt = start_ts if hasattr(start_ts, "tzinfo") else datetime.now(UTC)
            if isinstance(end_ts, int | float):
                end_dt = datetime.fromtimestamp(end_ts, tz=UTC)
            else:
                end_dt = end_ts if hasattr(end_ts, "tzinfo") else datetime.now(UTC)
            duration = end_dt - start_dt
            hours, rem = divmod(int(duration.total_seconds()), 3600)
            mins = rem // 60
            duration_str = f"{hours}h {mins}m" if hours else f"{mins}m"

            pct = (success_count / num_partitions * 100) if num_partitions else 0
            body = (
                f"Duration: {duration_str}\n"
                f"Partitions: {num_partitions:,} total\n"
                f"  - Success: {success_count:,} ({pct:.1f}%)\n"
                f"  - Failed: {failure_count:,} ({100 - pct:.1f}%)"
            )
            del tracked[bid]
            new_cursor = dict(cursor_data)
            new_cursor["tracked_backfills"] = tracked
            return f"Backfill Complete: {bid}", body, new_cursor

    # Remove from tracked if no longer in backfills (e.g. purged)
    current_ids = {b.backfill_id for b in backfills}
    for bid in list(tracked):
        if bid not in current_ids:
            del tracked[bid]

    new_cursor = dict(cursor_data)
    new_cursor["tracked_backfills"] = tracked
    return None, None, new_cursor


def _check_stuck_runs(
    context: SensorEvaluationContext,
    instance: Any,
    threshold_minutes: int,
    cooldown_minutes: int,
    cursor_data: dict[str, Any],
) -> tuple[str | None, dict[str, Any]]:
    """Check for runs stuck in STARTED. Returns (alert_body, updated_cursor)."""
    threshold = datetime.now(UTC) - timedelta(minutes=threshold_minutes)
    recs = instance.get_run_records(
        limit=50,
        filters=RunsFilter(
            statuses=[DagsterRunStatus.STARTED],
            updated_before=threshold,
        ),
    )
    if not recs:
        return None, cursor_data

    # Cooldown
    last = cursor_data.get("last_stuck_runs_alert")
    if last:
        last_dt = datetime.fromisoformat(last.replace("Z", "+00:00"))
        if datetime.now(UTC) - last_dt < timedelta(minutes=cooldown_minutes):
            return None, cursor_data

    lines = []
    for rec in recs[:20]:
        run = rec.dagster_run
        lines.append(f"  - {run.run_id} ({run.job_name})")
    if len(recs) > 20:
        lines.append(f"  ... and {len(recs) - 20} more")
    body = f"Stuck runs (STARTED >{threshold_minutes} min):\n" + "\n".join(lines)

    new_cursor = dict(cursor_data)
    new_cursor["last_stuck_runs_alert"] = datetime.now(UTC).isoformat()
    return body, new_cursor


@sensor(
    name="system_health_sensor",
    description="Polls Dagster state every 2 min; sends Telegram alerts for high failure rate, backfill completion, stuck runs",
    minimum_interval_seconds=120,
    required_resource_keys={"telegram"},
    default_status=DefaultSensorStatus.STOPPED,  # Enable in UI when Telegram is configured
)
def system_health_sensor(context: SensorEvaluationContext) -> SkipReason | None:
    """Evaluate health checks and send Telegram alerts with cooldowns."""
    telegram = context.resources.telegram
    if not telegram.enabled or not telegram.bot_token or not telegram.chat_id:
        return SkipReason("Telegram not configured (TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID empty)")

    instance = context.instance
    cursor_data = _parse_cursor(context.cursor)

    # Configurable thresholds (can be moved to resource/config later)
    failure_threshold = 0.5
    failure_window_minutes = 60
    failure_min_runs = 5
    stuck_threshold_minutes = 30
    cooldown_minutes = 30

    # Run checks
    body, cursor_data = _check_failure_rate(
        context,
        instance,
        failure_threshold,
        failure_window_minutes,
        failure_min_runs,
        cooldown_minutes,
        cursor_data,
    )
    if body:
        telegram.send_alert("System Alert: High Failure Rate", body)
        context.update_cursor(json.dumps(cursor_data))
        return

    title, body, cursor_data = _check_backfills(context, instance, cursor_data)
    if title and body:
        telegram.send_alert(title, body)
        context.update_cursor(json.dumps(cursor_data))
        return

    body, cursor_data = _check_stuck_runs(
        context,
        instance,
        stuck_threshold_minutes,
        cooldown_minutes,
        cursor_data,
    )
    if body:
        telegram.send_alert("System Alert: Stuck Runs", body)
        context.update_cursor(json.dumps(cursor_data))
        return

    context.update_cursor(json.dumps(cursor_data))
