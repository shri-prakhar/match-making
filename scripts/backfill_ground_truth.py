#!/usr/bin/env python3
"""One-time backfill of ground_truth_outcomes from current Airtable ATS state.

Usage:
    poetry run with-local-db python scripts/backfill_ground_truth.py
    poetry run with-remote-db python scripts/backfill_ground_truth.py
    On server: poetry run python scripts/backfill_ground_truth.py --local

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

from talent_matching.script_env import apply_local_db  # noqa: E402
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
    apply_local_db()
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

    print("Fetching ATS records...", flush=True)
    records = ats.fetch_all_records_for_ground_truth()
    print(f"  Fetched {len(records)} records", flush=True)

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

    # Load existing rows in one query (all rows - table is small)
    existing: dict[tuple[str, str], tuple] = {}
    rows = session.execute(
        select(
            GroundTruthOutcome.job_airtable_record_id,
            GroundTruthOutcome.candidate_airtable_record_id,
            GroundTruthOutcome.potential_talent_fit_at,
            GroundTruthOutcome.client_introduction_at,
            GroundTruthOutcome.hired_at,
            GroundTruthOutcome.source_columns,
        )
    ).all()
    existing = {(r[0], r[1]): r[2:6] for r in rows}

    inserted = 0
    updated = 0

    for (job_id, cand_id), cols in pairs.items():
        key = (job_id, cand_id)
        if key in existing:
            ptf_at, ci_at, hired_at, src_cols = existing[key]
            changed = False
            new_ptf = now if "Potential Talent Fit" in cols and ptf_at is None else ptf_at
            new_ci = now if "CLIENT INTRODUCTION" in cols and ci_at is None else ci_at
            new_hired = now if "Hired" in cols and hired_at is None else hired_at
            if new_ptf != ptf_at or new_ci != ci_at or new_hired != hired_at:
                changed = True
            new_src = list(src_cols or [])
            for c in cols:
                if c not in new_src:
                    new_src.append(c)
            if changed:
                session.execute(
                    GroundTruthOutcome.__table__.update()
                    .where(
                        GroundTruthOutcome.job_airtable_record_id == job_id,
                        GroundTruthOutcome.candidate_airtable_record_id == cand_id,
                    )
                    .values(
                        potential_talent_fit_at=new_ptf,
                        client_introduction_at=new_ci,
                        hired_at=new_hired,
                        last_updated_at=now,
                        source_columns=new_src,
                    )
                )
                updated += 1
        else:
            stmt = insert(GroundTruthOutcome).values(
                id=uuid4(),
                job_airtable_record_id=job_id,
                candidate_airtable_record_id=cand_id,
                potential_talent_fit_at=now if "Potential Talent Fit" in cols else None,
                client_introduction_at=now if "CLIENT INTRODUCTION" in cols else None,
                hired_at=now if "Hired" in cols else None,
                first_seen_at=now,
                last_updated_at=now,
                source_columns=list(cols),
            )
            session.execute(stmt)
            inserted += 1

    session.commit()
    session.close()

    print(f"Done: {inserted} inserted, {updated} updated")


if __name__ == "__main__":
    main()
