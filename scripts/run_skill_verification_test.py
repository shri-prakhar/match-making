#!/usr/bin/env python3
"""Run skill verification pipeline for a single candidate with GitHub.

Finds a candidate with github_url, adds partition if needed, and materializes
candidate_github_commit_history + candidate_skill_verification.

Usage:
    poetry run python scripts/run_skill_verification_test.py [partition_id]
    poetry run python scripts/run_skill_verification_test.py rechGJvgloO4z6uYD

If no partition_id given, finds first candidate with github_url from DB.
"""

import os
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(project_root / ".env")


def get_candidate_with_github(partition_id: str | None) -> str | None:
    """Get partition ID of a candidate with GitHub URL."""
    import psycopg2
    from psycopg2.extras import RealDictCursor

    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "talent_matching"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", ""),
    )
    cur = conn.cursor(cursor_factory=RealDictCursor)

    if partition_id:
        cur.execute(
            "SELECT airtable_record_id, full_name, github_url FROM raw_candidates WHERE airtable_record_id = %s",
            (partition_id,),
        )
        row = cur.fetchone()
        conn.close()
        return partition_id if row and row.get("github_url") else None

    cur.execute(
        """
        SELECT airtable_record_id, full_name, github_url
        FROM raw_candidates
        WHERE github_url IS NOT NULL AND github_url != ''
        LIMIT 1
        """
    )
    row = cur.fetchone()
    conn.close()
    return row["airtable_record_id"] if row else None


def main():
    partition_id = sys.argv[1] if len(sys.argv) > 1 else None
    pid = get_candidate_with_github(partition_id)

    if not pid:
        if partition_id:
            print(f"No candidate with github_url found for partition: {partition_id}")
        else:
            print("No candidate with github_url found in raw_candidates.")
            print("Add a candidate with GitHub link to Airtable, run candidate_ingest, then retry.")
        sys.exit(1)

    print(f"Running skill verification for partition: {pid}")
    print()

    # Use dagster asset materialize - requires workspace and instance
    # For local run we need DAGSTER_HOME and the workspace
    env = os.environ.copy()
    env["POSTGRES_HOST"] = env.get("POSTGRES_HOST", "localhost")
    env["POSTGRES_PORT"] = env.get("POSTGRES_PORT", "5432")

    # Select the skill verification assets (and their upstream deps within the pipeline)
    # candidate_github_commit_history depends on normalized_candidates
    # candidate_skill_verification depends on normalized_candidates + candidate_github_commit_history
    select_assets = "candidate_github_commit_history,candidate_skill_verification"

    cmd = [
        "poetry",
        "run",
        "dagster",
        "asset",
        "materialize",
        "--select",
        select_assets,
        "--partition",
        pid,
        "-w",
        "docker/workspace-local.yaml",
        "-l",
        "talent_matching",
    ]

    print("Command:", " ".join(cmd))
    print()
    print("Note: This requires dagster-webserver/code server. For remote:")
    print("  1. poetry run remote-ui  (start tunnels)")
    print("  2. In UI: Jobs → candidate_pipeline → Backfill → select partition", pid)
    print()
    print("Or run locally with: poetry run dagster dev")
    print("Then in another terminal:")
    print(
        f"  poetry run dagster asset materialize --select {select_assets} --partition {pid} -w docker/workspace-local.yaml -l talent_matching"
    )
    print()

    # Try running - may fail if no daemon/instance
    import subprocess

    result = subprocess.run(cmd, cwd=project_root, env=env)
    sys.exit(result.returncode)


if __name__ == "__main__":
    main()
