#!/usr/bin/env python3
"""Launch a full ats_matchmaking_pipeline backfill for all ATS jobs in given Job Status values.

Fetches ATS record IDs where Job Status is one of: Matchmaking Ready, Matchmaking Done,
Ongoing Recruiting, Client Introduction, In Interview. Then submits a backfill for ats_matchmaking_pipeline
on the remote instance (so the remote daemon runs the full pipeline for each partition).

Prerequisites:
  - REMOTE_HOST in .env (and optional REMOTE_PROJECT_DIR)
  - AIRTABLE_BASE_ID, AIRTABLE_API_KEY (and optional AIRTABLE_ATS_TABLE_ID) in .env
  - Remote has the project deployed

By default the script does not deploy or sync; it fetches ATS record IDs by status
and launches the ats_matchmaking_pipeline backfill on the remote. Use --sync to run
sync_airtable_jobs_job on the remote first so all ATS record IDs exist as partitions.

Usage:
  poetry run python scripts/launch_ats_matchmaking_backfill_by_status.py  # backfill only (no deploy/sync)
  poetry run python scripts/launch_ats_matchmaking_backfill_by_status.py --sync  # run sync_airtable_jobs_job on remote first, then backfill
  poetry run python scripts/launch_ats_matchmaking_backfill_by_status.py --partitions recA,recB,recC  # backfill only these partitions
"""

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

import httpx
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

# Job Status values to include (ATS single-select)
STATUSES = [
    "Matchmaking Ready",
    "Matchmaking Done",
    "Ongoing Recruiting",
    "Client Introduction",
    "In Interview",
]
ATS_TABLE_ID = "tblrbhITEIBOxwcQV"


def fetch_ats_record_ids_by_statuses(statuses: list[str]) -> list[str]:
    """Fetch ATS record IDs where Job Status is one of the given values."""
    base_id = os.getenv("AIRTABLE_BASE_ID")
    api_key = os.getenv("AIRTABLE_API_KEY")
    table_id = os.getenv("AIRTABLE_ATS_TABLE_ID", ATS_TABLE_ID)
    if not base_id or not api_key:
        print("Set AIRTABLE_BASE_ID and AIRTABLE_API_KEY in .env", file=sys.stderr)
        sys.exit(1)

    conditions = "".join(f'{{Job Status}} = "{s}", ' for s in statuses)
    formula = "OR(" + conditions.rstrip(", ") + ")"
    url = f"https://api.airtable.com/v0/{base_id}/{table_id}"
    headers = {"Authorization": f"Bearer {api_key}"}
    record_ids: list[str] = []
    offset: str | None = None

    with httpx.Client(timeout=60.0) as client:
        while True:
            params: list[tuple[str, str]] = [("filterByFormula", formula)]
            if offset:
                params.append(("offset", offset))
            response = client.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            for rec in data.get("records", []):
                record_ids.append(rec["id"])
            offset = data.get("offset")
            if not offset:
                break
    return record_ids


def run_remote_sync_jobs(remote_host: str, remote_dir: str) -> bool:
    """Run sync_airtable_jobs_job on remote so all ATS record IDs exist as partitions. Returns True on success."""
    remote_cmd = (
        f"cd {remote_dir} && docker compose -f docker-compose.prod.yml run --rm "
        "-e POSTGRES_HOST=postgres "
        f"-v {remote_dir}/docker/workspace.yaml:/workspace.yaml:ro "
        f"-v {remote_dir}/docker/dagster.yaml:/opt/dagster/dagster_home/dagster.yaml:ro "
        "-e DAGSTER_HOME=/opt/dagster/dagster_home "
        "dagster-code "
        "dagster job launch "
        "-w /workspace.yaml "
        "-j sync_airtable_jobs_job "
        "-l talent_matching"
    )
    result = subprocess.run(["ssh", remote_host, remote_cmd])
    if result.returncode != 0:
        return False
    print("Waiting 60s for sync to register partitions...")
    time.sleep(60)
    return True


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Launch ats_matchmaking_pipeline backfill for ATS jobs in selected statuses."
    )
    parser.add_argument(
        "--sync",
        action="store_true",
        help="Run sync_airtable_jobs_job on remote first so partitions exist (default: skip)",
    )
    parser.add_argument(
        "--partitions",
        type=str,
        help="Comma-separated partition IDs to backfill only (e.g. recA,recB).",
    )
    args = parser.parse_args()

    remote_host = os.getenv("REMOTE_HOST")
    remote_dir = os.getenv("REMOTE_PROJECT_DIR", "/root/match-making")
    if not remote_host:
        print("REMOTE_HOST not set in .env. Cannot launch remote backfill.", file=sys.stderr)
        return 1

    if args.partitions:
        record_ids = [p.strip() for p in args.partitions.split(",") if p.strip()]
        if not record_ids:
            print("--partitions gave no partition IDs.", file=sys.stderr)
            return 1
        print(f"Using {len(record_ids)} partition(s) from --partitions.")
    else:
        if args.sync:
            print("Step 1: Syncing ATS job partitions on remote (sync_airtable_jobs_job)...")
            if not run_remote_sync_jobs(remote_host, remote_dir):
                print("Sync failed. Omit --sync if partitions already exist.", file=sys.stderr)
                return 1
            print("Sync launched and waited.\n")

        print("Step 2: Fetching ATS jobs with Job Status in:", ", ".join(STATUSES))
        record_ids = fetch_ats_record_ids_by_statuses(STATUSES)
        if not record_ids:
            print("No ATS records found with those statuses.")
            return 0

        print(f"Found {len(record_ids)} partition(s).")
    partitions_arg = ",".join(record_ids)

    # Run backfill on remote for ats_matchmaking_pipeline (full pipeline with LLM + upload)
    remote_cmd = (
        f"cd {remote_dir} && docker compose -f docker-compose.prod.yml run --rm "
        "-e POSTGRES_HOST=postgres "
        f"-v {remote_dir}/docker/workspace.yaml:/workspace.yaml:ro "
        f"-v {remote_dir}/docker/dagster.yaml:/opt/dagster/dagster_home/dagster.yaml:ro "
        "-e DAGSTER_HOME=/opt/dagster/dagster_home "
        "dagster-code "
        "dagster job backfill "
        f"-w /workspace.yaml "
        "-j ats_matchmaking_pipeline "
        f"--partitions {partitions_arg!r} "
        "-l talent_matching "
        "--noprompt"
    )
    print("Launching ats_matchmaking_pipeline backfill on remote...")
    result = subprocess.run(["ssh", remote_host, remote_cmd])
    if result.returncode != 0:
        return result.returncode
    print("Backfill submitted. Check: poetry run remote-ui, then http://localhost:3000 → Backfills")
    return 0


if __name__ == "__main__":
    sys.exit(main())
