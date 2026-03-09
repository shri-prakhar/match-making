"""Ground truth sync sensor: syncs introduced/hired candidates from Airtable ATS to Postgres."""

from collections import defaultdict
from datetime import UTC, datetime
from uuid import uuid4

from dagster import SensorEvaluationContext, SkipReason, sensor
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from talent_matching.db import get_session
from talent_matching.models.ground_truth import GroundTruthOutcome

GROUND_TRUTH_COLUMNS = [
    "Potential Talent Fit",
    "CLIENT INTRODUCTION",
    "Hired",
]


def _extract_linked_ids(fields: dict, column: str) -> list[str]:
    """Extract linked record IDs from an Airtable linked record field."""
    value = fields.get(column, [])
    if isinstance(value, list):
        return [v for v in value if isinstance(v, str) and v.startswith("rec")]
    return []


def _sync_ground_truth(context: SensorEvaluationContext) -> tuple[int, int]:
    """Sync ATS records to ground_truth_outcomes. Returns (inserted, updated)."""
    ats = context.resources.airtable_ats
    records = ats.fetch_all_records_for_ground_truth()

    now = datetime.now(UTC)

    # Build (job_id, cand_id) -> set of columns they appear in
    pairs: dict[tuple[str, str], set[str]] = defaultdict(set)
    for rec in records:
        job_id = rec.get("id")
        if not job_id:
            continue
        fields = rec.get("fields", {})
        for col in GROUND_TRUTH_COLUMNS:
            for cand_id in _extract_linked_ids(fields, col):
                if cand_id:
                    pairs[(job_id, cand_id)].add(col)

    if not pairs:
        return (0, 0)

    session = get_session()
    inserted = 0
    updated = 0

    for (job_id, cand_id), columns in pairs.items():
        existing = session.execute(
            select(GroundTruthOutcome).where(
                GroundTruthOutcome.job_airtable_record_id == job_id,
                GroundTruthOutcome.candidate_airtable_record_id == cand_id,
            )
        ).scalar_one_or_none()

        if existing:
            changed = False
            if "Potential Talent Fit" in columns and existing.potential_talent_fit_at is None:
                existing.potential_talent_fit_at = now
                changed = True
            if "CLIENT INTRODUCTION" in columns and existing.client_introduction_at is None:
                existing.client_introduction_at = now
                changed = True
            if "Hired" in columns and existing.hired_at is None:
                existing.hired_at = now
                changed = True
            if changed:
                existing.last_updated_at = now
                src = list(existing.source_columns or [])
                for c in columns:
                    if c not in src:
                        src.append(c)
                existing.source_columns = src
                session.add(existing)
                updated += 1
        else:
            stmt = insert(GroundTruthOutcome).values(
                id=uuid4(),
                job_airtable_record_id=job_id,
                candidate_airtable_record_id=cand_id,
                potential_talent_fit_at=now if "Potential Talent Fit" in columns else None,
                client_introduction_at=now if "CLIENT INTRODUCTION" in columns else None,
                hired_at=now if "Hired" in columns else None,
                first_seen_at=now,
                last_updated_at=now,
                source_columns=list(columns),
            )
            session.execute(stmt)
            inserted += 1

    session.commit()
    session.close()
    return (inserted, updated)


@sensor(
    minimum_interval_seconds=600,
    description=(
        "Syncs ground-truth outcomes from Airtable ATS (Potential Talent Fit, "
        "CLIENT INTRODUCTION, Hired) to ground_truth_outcomes table."
    ),
    required_resource_keys={"airtable_ats"},
)
def ground_truth_sync_sensor(context: SensorEvaluationContext):
    """Poll ATS and upsert (job, candidate) pairs into ground_truth_outcomes."""
    context.log.info("[ground_truth_sync_sensor] Starting sync...")
    inserted, updated = _sync_ground_truth(context)
    context.log.info(
        f"[ground_truth_sync_sensor] Done: {inserted} inserted, {updated} updated"
    )
    return SkipReason(f"Synced: {inserted} new, {updated} updated")
