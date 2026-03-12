#!/usr/bin/env python3
"""Clear DB data for candidate partitions that failed at normalization.

Removes normalized_candidates, candidate_vectors, and CASCADE-related rows
(matches, candidate_skills, etc.) for the given partitions so no stale data
remains when normalization fails (e.g. InsufficientCvDataError,
MissingDesiredJobCategoryError).

Usage:
  poetry run with-remote-db python scripts/clear_candidate_partition_data.py --partitions recXXX recYYY
  poetry run with-remote-db python scripts/clear_candidate_partition_data.py --partitions-file partitions.txt
  poetry run with-local-db python scripts/clear_candidate_partition_data.py --partitions recXXX

Use --dry-run to only print what would be cleared.
"""

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(PROJECT_ROOT / ".env")

from talent_matching.db import get_session  # noqa: E402
from talent_matching.utils.clear_candidate_data import clear_candidate_partition_data  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Clear DB data for candidate partitions (e.g. after normalization failure)"
    )
    parser.add_argument(
        "--partitions",
        nargs="*",
        help="Partition keys (airtable_record_id) to clear",
    )
    parser.add_argument(
        "--partitions-file",
        type=Path,
        metavar="PATH",
        help="File with one partition key per line",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print partitions that would be cleared",
    )
    args = parser.parse_args()

    partition_keys: list[str] = []
    if args.partitions:
        partition_keys.extend(args.partitions)
    if args.partitions_file:
        path = Path(args.partitions_file).resolve()
        partition_keys.extend(
            line.strip() for line in path.read_text().splitlines() if line.strip()
        )

    if not partition_keys:
        print("No partitions given. Use --partitions or --partitions-file.")
        return 1

    if args.dry_run:
        print(f"[DRY RUN] Would clear data for {len(partition_keys)} partition(s):")
        for pk in partition_keys[:50]:
            print(f"  {pk}")
        if len(partition_keys) > 50:
            print(f"  ... and {len(partition_keys) - 50} more")
        return 0

    session = get_session()
    cleared = 0
    for pk in partition_keys:
        if clear_candidate_partition_data(session, pk):
            cleared += 1
            print(f"Cleared: {pk}")
    session.commit()
    session.close()
    print(f"Cleared data for {cleared}/{len(partition_keys)} partition(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
