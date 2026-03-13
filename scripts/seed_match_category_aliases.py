"""Seed match_category_aliases for job categories (e.g. Compliance -> [Operations, Legal]).

Run against remote DB: poetry run with-remote-db python scripts/seed_match_category_aliases.py
Run against local DB:  poetry run with-local-db python scripts/seed_match_category_aliases.py
On server: poetry run python scripts/seed_match_category_aliases.py --local

Only updates rows where match_category_aliases is currently NULL (idempotent).
Use --force to overwrite existing aliases.
"""

import argparse
import sys

from sqlalchemy import select, update

from talent_matching.db import get_session
from talent_matching.models.scoring_weights import ScoringWeightsRecord
from talent_matching.script_env import apply_local_db

# Job category -> list of additional match categories (candidates with these in desired_job_categories will match)
SEED_ALIASES = {
    "Compliance": ["Operations", "Legal"],
}


def main() -> None:
    apply_local_db()
    parser = argparse.ArgumentParser(description="Seed match_category_aliases on scoring_weights")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing match_category_aliases; default is only set when NULL",
    )
    args = parser.parse_args()

    session = get_session()
    updated = 0
    for job_category, aliases in SEED_ALIASES.items():
        row = session.execute(
            select(ScoringWeightsRecord).where(ScoringWeightsRecord.job_category == job_category)
        ).scalar_one_or_none()
        if row is None:
            print(f"Skip {job_category!r}: no scoring_weights row", file=sys.stderr)
            continue
        if (
            not args.force
            and row.match_category_aliases is not None
            and len(row.match_category_aliases or []) > 0
        ):
            print(
                f"Skip {job_category!r}: already has aliases (use --force to overwrite)",
                file=sys.stderr,
            )
            continue
        stmt = update(ScoringWeightsRecord).where(ScoringWeightsRecord.job_category == job_category)
        if not args.force:
            stmt = stmt.where(ScoringWeightsRecord.match_category_aliases.is_(None))
        result = session.execute(stmt.values(match_category_aliases=aliases))
        session.commit()
        if result.rowcount > 0:
            updated += 1
            print(f"Set {job_category!r} -> {aliases}")
    session.close()
    print(f"Updated {updated} row(s)")


if __name__ == "__main__":
    main()
