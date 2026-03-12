"""Cancel all non-terminal Dagster runs in the DB (run storage).

Use when the daemon/UI are stuck and GraphQL terminate does not work. Run this
against the same Postgres that Dagster uses so that when Dagster restarts, all
runs show as CANCELED and no run workers are resumed.

Usage:
  # On the server (Postgres on localhost:5432):
  poetry run python scripts/hard_reset_dagster.py --local

  # From laptop with tunnel to remote Postgres:
  poetry run with-remote-db python scripts/hard_reset_dagster.py

  # Dry run: only report how many runs would be canceled
  poetry run python scripts/hard_reset_dagster.py --local --dry-run
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Apply --local before any DB access
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import talent_matching.script_env  # noqa: E402

talent_matching.script_env.apply_local_db()

from dotenv import load_dotenv  # noqa: E402

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

# Non-terminal statuses in Dagster run storage (runs.status column)
NON_TERMINAL_STATUSES = (
    "NOT_STARTED",
    "QUEUED",
    "STARTING",
    "STARTED",
    "CANCELING",
)
TARGET_STATUS = "CANCELED"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Set all in-progress Dagster runs to CANCELED in the DB."
    )
    parser.add_argument(
        "--local",
        action="store_true",
        help="Use local Postgres (localhost:5432). Use on the server.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only report how many runs would be updated; do not change the DB.",
    )
    args = parser.parse_args()

    from sqlalchemy import text

    from talent_matching.db import get_engine

    engine = get_engine()
    params = {f"s{i}": s for i, s in enumerate(NON_TERMINAL_STATUSES)}
    in_clause = ", ".join([f":s{i}" for i in range(len(NON_TERMINAL_STATUSES))])

    with engine.connect() as conn:
        count_result = conn.execute(
            text(f"SELECT COUNT(*) FROM runs WHERE status IN ({in_clause})"),
            params,
        )
        count = count_result.scalar_one()

    if count == 0:
        print("No non-terminal runs in the DB.")
        return 0

    print(f"Found {count} run(s) with status in {NON_TERMINAL_STATUSES}.")
    if args.dry_run:
        print("Dry run: not updating.")
        return 0

    with engine.begin() as conn:
        conn.execute(
            text(f"UPDATE runs SET status = :target WHERE status IN ({in_clause})"),
            {**params, "target": TARGET_STATUS},
        )

    print(f"Updated {count} run(s) to status {TARGET_STATUS}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
