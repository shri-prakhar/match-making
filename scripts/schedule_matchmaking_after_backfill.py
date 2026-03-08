#!/usr/bin/env python3
"""Query ATS for Matchmaking Done + Client Introduction jobs, then schedule matchmaking
runs after the candidate pipeline backfill finishes.

Deploy to remote first so the daemon has the latest code, then wait for the backfill
run and launch matchmaking for the fetched partitions.

Prerequisites:
  - poetry run remote-ui RUNNING (webserver at localhost:3000, tunnel to remote)
  - AIRTABLE_BASE_ID, AIRTABLE_API_KEY, REMOTE_HOST in .env
  - Partitions must exist in jobs dynamic partition (sync_airtable_jobs_job or add via UI)

Usage:
  poetry run python scripts/schedule_matchmaking_after_backfill.py <backfill_run_id>
  poetry run python scripts/schedule_matchmaking_after_backfill.py abc123 --deploy
  poetry run python scripts/schedule_matchmaking_after_backfill.py abc123 --deploy --poll-interval 15
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path

import httpx

# Match rerun_matchmaking_done_jobs.py and Airtable single-select values
ATS_MATCHMAKING_DONE = "Matchmaking Done"
ATS_CLIENT_INTRODUCTION = "Client Introduction"
ATS_TABLE_ID = "tblrbhITEIBOxwcQV"
PROJECT_ROOT = Path(__file__).resolve().parent.parent


def deploy_to_remote() -> bool:
    """Deploy latest code to remote (git pull + docker compose). Returns True on success."""
    remote_host = os.getenv("REMOTE_HOST")
    remote_dir = os.getenv("REMOTE_PROJECT_DIR", "/root/match-making")
    if not remote_host:
        print("REMOTE_HOST not set in .env", file=sys.stderr)
        return False
    cmd = [
        "ssh",
        remote_host,
        f"cd {remote_dir} && git pull && docker compose -f docker-compose.prod.yml up --build -d",
    ]
    print("Deploying to remote (git pull + rebuild)...")
    result = subprocess.run(cmd)
    if result.returncode != 0:
        print(
            "Deploy failed. Re-run with --force to overwrite local changes on remote.",
            file=sys.stderr,
        )
        return False
    print("Deploy complete.")
    return True


def fetch_ats_jobs_by_statuses(statuses: list[str]) -> list[dict]:
    """Fetch ATS records matching any of the given Job Status values."""
    base_id = os.getenv("AIRTABLE_BASE_ID")
    api_key = os.getenv("AIRTABLE_API_KEY")
    table_id = os.getenv("AIRTABLE_ATS_TABLE_ID", ATS_TABLE_ID)
    if not base_id or not api_key:
        print("Set AIRTABLE_BASE_ID and AIRTABLE_API_KEY in .env", file=sys.stderr)
        sys.exit(1)

    # OR({Job Status} = "X", {Job Status} = "Y", ...)
    conditions = "".join(f'{{Job Status}} = "{s}", ' for s in statuses)
    formula = "OR(" + conditions.rstrip(", ") + ")"
    url = f"https://api.airtable.com/v0/{base_id}/{table_id}"
    headers = {"Authorization": f"Bearer {api_key}"}
    records: list[dict] = []
    offset: str | None = None

    with httpx.Client(timeout=30.0) as client:
        while True:
            params: list[tuple[str, str]] = [("filterByFormula", formula)]
            if offset:
                params.append(("offset", offset))
            response = client.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            records.extend(data.get("records", []))
            offset = data.get("offset")
            if not offset:
                break
    return records


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Schedule matchmaking for ATS jobs (Matchmaking Done + Client Introduction) "
        "after candidate pipeline backfill finishes."
    )
    parser.add_argument(
        "run_or_backfill_id",
        help="Run ID or backfill ID of the candidate pipeline to wait for",
    )
    parser.add_argument(
        "--backfill-id",
        action="store_true",
        help="Treat first arg as backfill ID (poll partitionBackfillOrError)",
    )
    parser.add_argument(
        "--deploy",
        action="store_true",
        help="Deploy to remote first (git pull + rebuild on REMOTE_HOST)",
    )
    parser.add_argument(
        "--poll-interval",
        type=int,
        default=30,
        help="Seconds between run status checks (default: 30)",
    )
    parser.add_argument(
        "--on-failure",
        action="store_true",
        help="Launch matchmaking even when upstream run fails",
    )
    args = parser.parse_args()

    from dotenv import load_dotenv

    load_dotenv(PROJECT_ROOT / ".env")

    if args.deploy and not deploy_to_remote():
        return 1

    print("\nFetching ATS jobs with Job Status = 'Matchmaking Done' or 'Client Introduction'...")
    records = fetch_ats_jobs_by_statuses([ATS_MATCHMAKING_DONE, ATS_CLIENT_INTRODUCTION])

    if not records:
        print("No jobs found with those statuses.")
        return 0

    partition_ids = [r["id"] for r in records if r.get("id")]
    by_status: dict[str, list[str]] = {}
    for r in records:
        rid = r.get("id")
        if not rid:
            continue
        status = (r.get("fields", {}).get("Job Status") or "?").strip()
        by_status.setdefault(status, []).append(rid)

    print(f"Found {len(partition_ids)} jobs:")
    for status, ids in sorted(by_status.items()):
        print(f"  {status}: {len(ids)} — {', '.join(ids[:3])}{'...' if len(ids) > 3 else ''}")

    partitions_arg = ",".join(partition_ids)
    launch_script = PROJECT_ROOT / "scripts" / "launch_matchmaking_after_run.py"
    cmd = [
        sys.executable,
        str(launch_script),
        "--poll-interval",
        str(args.poll_interval),
    ]
    if args.backfill_id:
        cmd.extend(["--backfill-id", args.run_or_backfill_id, partitions_arg])
    else:
        cmd.extend([args.run_or_backfill_id, partitions_arg])
    if args.on_failure:
        cmd.append("--on-failure")

    return subprocess.run(cmd).returncode


if __name__ == "__main__":
    sys.exit(main())
