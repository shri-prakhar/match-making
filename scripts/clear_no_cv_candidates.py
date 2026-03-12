#!/usr/bin/env python3
"""Remove from the DB candidates who have no CV/summary (excluded from matchmaking).

Such candidates should not run any LLM and should not be considered in matchmaking.
This script deletes:
- normalized_candidates rows where model_version IS NULL (no LLM was run)
- candidate_vectors for those candidates (keyed by raw_candidate_id)
- Related rows (matches, candidate_skills, etc.) are removed by CASCADE on normalized_candidates.

Usage:
    poetry run with-local-db python scripts/clear_no_cv_candidates.py [--dry-run]
    poetry run with-remote-db python scripts/clear_no_cv_candidates.py [--dry-run]
    On server: poetry run python scripts/clear_no_cv_candidates.py --local [--dry-run]

Use --dry-run to only print what would be deleted.
"""

import argparse
import sys

sys.path.insert(0, __import__("pathlib").Path(__file__).resolve().parents[1])

from scripts.inspect_utils import get_connection
from talent_matching.script_env import apply_local_db


def main() -> int:
    apply_local_db()
    parser = argparse.ArgumentParser(description="Clear DB of candidates with no CV/summary")
    parser.add_argument(
        "--local",
        action="store_true",
        help="Use local Postgres (when running on the server).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print what would be deleted",
    )
    args = parser.parse_args()

    conn = get_connection()
    cur = conn.cursor()

    # Faulty = no LLM was run (model_version IS NULL). These are the minimal rows we used to write.
    cur.execute(
        """
        SELECT id, raw_candidate_id, airtable_record_id, full_name
        FROM normalized_candidates
        WHERE model_version IS NULL
        """
    )
    rows = cur.fetchall()
    if not rows:
        print("No faulty (no-CV) candidates found in normalized_candidates.")
        cur.close()
        conn.close()
        return 0

    raw_ids = [r[1] for r in rows]
    print(f"Found {len(rows)} normalized_candidates with model_version IS NULL (no CV data):")
    for r in rows[:20]:
        print(f"  {r[3]} ({r[2]})")
    if len(rows) > 20:
        print(f"  ... and {len(rows) - 20} more")

    if args.dry_run:
        print("\n[DRY RUN] Would delete:")
        print(f"  - candidate_vectors for {len(raw_ids)} raw_candidate_id(s)")
        print(
            f"  - {len(rows)} normalized_candidates row(s) (CASCADE: matches, candidate_skills, etc.)"
        )
        cur.close()
        conn.close()
        return 0

    # Delete candidate_vectors (FK to raw_candidates.id; not CASCADE from normalized_candidates)
    cur.execute(
        "DELETE FROM candidate_vectors WHERE candidate_id = ANY(%s)",
        (raw_ids,),
    )
    vec_deleted = cur.rowcount

    # Delete normalized_candidates (CASCADE removes matches, candidate_skills, candidate_experiences, etc.)
    cur.execute("DELETE FROM normalized_candidates WHERE model_version IS NULL")
    norm_deleted = cur.rowcount

    conn.commit()
    print(f"\nDeleted {vec_deleted} candidate_vectors and {norm_deleted} normalized_candidates.")
    cur.close()
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
