"""Backfill normalized_candidates.desired_job_categories to canonical (Talent) only.

Run against remote DB: poetry run with-remote-db python scripts/backfill_desired_job_categories_canonical.py
Run against local DB:  poetry run with-local-db python scripts/backfill_desired_job_categories_canonical.py
On server: poetry run python scripts/backfill_desired_job_categories_canonical.py --local

Loads canonical list from scoring_weights, resolves each candidate's desired_job_categories
to canonical values only, and updates rows that change or become empty (for review).
"""

import sys

from sqlalchemy import select, update

from talent_matching.db import get_session
from talent_matching.models.candidates import NormalizedCandidate
from talent_matching.models.scoring_weights import ScoringWeightsRecord
from talent_matching.script_env import apply_local_db
from talent_matching.utils.job_category import resolve_desired_job_categories_to_canonical


def main() -> None:
    apply_local_db()
    session = get_session()

    canonical_list = [
        r
        for r in session.execute(
            select(ScoringWeightsRecord.job_category).order_by(ScoringWeightsRecord.job_category)
        )
        .scalars()
        .all()
        if r and str(r).strip()
    ]
    if not canonical_list:
        print("No canonical job categories in scoring_weights; nothing to do.", file=sys.stderr)
        session.close()
        return

    rows = session.execute(select(NormalizedCandidate)).scalars().all()
    updated = 0
    emptied = 0
    for row in rows:
        candidate = row[0]
        raw_list = list(candidate.desired_job_categories or [])
        resolved = resolve_desired_job_categories_to_canonical(raw_list, canonical_list)
        if resolved == (candidate.desired_job_categories or []):
            continue
        session.execute(
            update(NormalizedCandidate)
            .where(NormalizedCandidate.id == candidate.id)
            .values(desired_job_categories=resolved if resolved else None)
        )
        updated += 1
        if not resolved:
            emptied += 1
            print(
                f"Emptied desired_job_categories: airtable_record_id={candidate.airtable_record_id!r}, raw={raw_list!r}"
            )
    session.commit()
    session.close()
    print(f"Updated {updated} row(s), {emptied} now empty (review above)")


if __name__ == "__main__":
    main()
