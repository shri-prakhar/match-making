"""Backfill raw_candidates.normalization_input_hash from current row data.

Use after a full materialization so the candidate sensor can skip records whose
only Airtable change was (N) write-back. Run against remote DB so the sensor
(which reads from remote) sees the hashes.

From your machine (tunnel to remote DB):
  poetry run with-remote-db python scripts/backfill_normalization_input_hashes.py

On the server (DB is local; no tunnel):
  poetry run python scripts/backfill_normalization_input_hashes.py --local
  # or: python scripts/backfill_normalization_input_hashes.py --local

  poetry run python scripts/backfill_normalization_input_hashes.py --local --dry-run
  poetry run python scripts/backfill_normalization_input_hashes.py --local --limit 100
"""

import argparse

from sqlalchemy import select

from talent_matching.db import get_session
from talent_matching.models.raw import RawCandidate
from talent_matching.script_env import apply_local_db
from talent_matching.utils.airtable_mapper import (
    NORMALIZATION_INPUT_FIELDS,
    compute_normalization_input_hash,
)

BATCH_SIZE = 500


def main() -> None:
    apply_local_db()
    parser = argparse.ArgumentParser(
        description="Backfill normalization_input_hash for all raw_candidates from current row data."
    )
    parser.add_argument(
        "--local",
        action="store_true",
        help="Use local Postgres (localhost:5432). Use when running on the server; loads .env.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only count rows and report what would be updated; do not write.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max number of rows to update (default: all).",
    )
    args = parser.parse_args()

    session = get_session()
    q = select(RawCandidate).order_by(RawCandidate.airtable_record_id)
    if args.limit is not None:
        q = q.limit(args.limit)
    result = session.execute(q).scalars().yield_per(BATCH_SIZE)

    updated = 0
    total = 0
    for row in result:
        total += 1
        record = {k: getattr(row, k, None) for k in NORMALIZATION_INPUT_FIELDS}
        h = compute_normalization_input_hash(record)
        if row.normalization_input_hash != h:
            if not args.dry_run:
                row.normalization_input_hash = h
            updated += 1
        if not args.dry_run and total % BATCH_SIZE == 0:
            session.commit()
            print(f"  Committed batch {total}", flush=True)

    if args.dry_run:
        print(f"Would set normalization_input_hash on {updated} of {total} raw_candidates.")
        session.close()
        return

    session.commit()
    session.close()
    print(f"Updated normalization_input_hash for {updated} of {total} raw_candidates.")


if __name__ == "__main__":
    main()
