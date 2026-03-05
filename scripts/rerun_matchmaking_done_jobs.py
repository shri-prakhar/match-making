#!/usr/bin/env python3
"""Launch matchmaking runs for all ATS jobs with Job Status = 'Matchmaking Done'.

Useful to backfill the Matches table after fixing AIRTABLE_MATCHES_TABLE_ID on remote.

Prerequisites:
  - poetry run remote-ui running (tunnels for gRPC + Postgres)

Usage:
  poetry run with-remote-db python scripts/rerun_matchmaking_done_jobs.py
"""

import os
import subprocess
import sys

import httpx

ATS_MATCHMAKING_DONE_STATUS = "Matchmaking Done "  # trailing space matches Airtable
ATS_TABLE_ID = os.getenv("AIRTABLE_ATS_TABLE_ID", "tblrbhITEIBOxwcQV")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def fetch_matchmaking_done_records() -> list[dict]:
    """Fetch ATS records with Job Status = Matchmaking Done."""
    if not AIRTABLE_BASE_ID or not AIRTABLE_API_KEY:
        print("Set AIRTABLE_BASE_ID and AIRTABLE_API_KEY in .env")
        sys.exit(1)
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{ATS_TABLE_ID}"
    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}"}
    formula = f'{{Job Status}} = "{ATS_MATCHMAKING_DONE_STATUS}"'
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


def launch_run(partition_id: str) -> bool:
    """Launch matchmaking run via the launch script. Returns True on success."""
    result = subprocess.run(
        [os.path.join(PROJECT_ROOT, "scripts", "launch_remote_matchmaking_run.sh"), partition_id],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "POSTGRES_HOST": os.environ.get("POSTGRES_HOST", "localhost"),
            "POSTGRES_PORT": os.environ.get("POSTGRES_PORT", "15432"),
        },
    )
    return result.returncode == 0


def main() -> int:
    print("Fetching ATS jobs with Job Status = 'Matchmaking Done'...")
    records = fetch_matchmaking_done_records()
    if not records:
        print("No jobs with Matchmaking Done status found.")
        return 0

    print(f"Found {len(records)} jobs. Launching matchmaking runs...")
    for rec in records:
        record_id = rec.get("id")
        if not record_id:
            continue
        fields = rec.get("fields", {})
        title = (fields.get("Open Position (Job Title)") or "Unknown")[:40]
        company = fields.get("Company")
        if isinstance(company, list):
            company = company[0] if company else "?"
        else:
            company = company or "?"
        print(f"  Launching {record_id}: {title} @ {company}...")
        if launch_run(record_id):
            print("    Submitted.")
        else:
            print("    Failed.")
            return 1
    print(f"\nDone. Launched {len(records)} runs. Check Dagster UI (http://localhost:3000 → Runs).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
