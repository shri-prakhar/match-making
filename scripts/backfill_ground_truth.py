#!/usr/bin/env python3
"""One-time backfill of ground_truth_outcomes from current Airtable ATS state.

Usage:
    poetry run with-local-db python scripts/backfill_ground_truth.py
    poetry run with-remote-db python scripts/backfill_ground_truth.py

Requires: AIRTABLE_BASE_ID, AIRTABLE_ATS_TABLE_ID, AIRTABLE_API_KEY in .env
"""

import os
import sys
from collections import defaultdict
from datetime import UTC, datetime
from uuid import uuid4

from dotenv import load_dotenv
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from talent_matching.db import get_session  # noqa: E402
from talent_matching.models.ground_truth import GroundTruthOutcome  # noqa: E402
from talent_matching.resources.airtable import AirtableATSResource  # noqa: E402


def _extract_linked_ids(fields: dict, column: str) -> list[str]:
    """Extract linked record IDs from an Airtable linked record field."""
    value = fields.get(column, [])
    if isinstance(value, list):
        return [v for v in value if isinstance(v, str) and v.startswith("rec")]
    return []


def main() -> None:
    base_id = os.environ.get("AIRTABLE_BASE_ID")
    table_id = os.environ.get("AIRTABLE_ATS_TABLE_ID", "tblrbhITEIBOxwcQV")
    api_key = os.environ.get("AIRTABLE_API_KEY") or os.environ.get("AIRTABLE_SCHEMA_TOKEN")
    if not base_id or not api_key:
        print("Set AIRTABLE_BASE_ID and AIRTABLE_API_KEY in .env")
        sys.exit(1)

    ats = AirtableATSResource(
        base_id=base_id,
        table_id=table_id,
        api_key=api_key,
    )

    columns = ["Potential Talent Fit", "CLIENT INTRODUCTION", "Hired"]

    print("Fetching ATS records...")
    records = ats.fetch_all_records_for_ground_truth()
    print(f"  Fetched {len(records)} records")

    pairs: dict[tuple[str, str], set[str]] = defaultdict(set)
    for rec in records:
        job_id = rec.get("id")
        if not job_id:
            continue
        fields = rec.get("fields", {})
        for col in columns:
            for cand_id in _extract_linked_ids(fields, col):
                if cand_id:
                    pairs[(job_id, cand_id)].add(col)

    print(f"  Found {len(pairs)} (job, candidate) pairs")

    now = datetime.now(UTC)
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

    print(f"Done: {inserted} inserted, {updated} updated")


if __name__ == "__main__":
    main()
