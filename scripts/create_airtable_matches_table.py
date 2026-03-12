#!/usr/bin/env python3
"""Create the Airtable Matches table for LLM-refined shortlist details.

Creates a new table "Matches" with:
  - Job: linked record → ATS table
  - Candidate: linked record → Talent table
  - Score: number (1-10)
  - Pros: long text
  - Cons: long text
  - Rank: number

When Job links to ATS, Airtable adds a reverse link on the ATS table so each
job row shows its matches. You may need to rename that column in Airtable.

Requires: AIRTABLE_BASE_ID, AIRTABLE_ATS_TABLE_ID, AIRTABLE_TABLE_ID (Talent),
          AIRTABLE_SCHEMA_TOKEN (token with schema.bases:read + schema.bases:write)

Usage (source .env into shell first; do not read .env from script):
  set -a && source .env && set +a && poetry run python scripts/create_airtable_matches_table.py

Or:
  export $(grep -v '^#' .env | xargs) && poetry run python scripts/create_airtable_matches_table.py
"""

import os
import sys

import httpx

MATCHES_TABLE_NAME = "Matches"
ATS_TABLE_ID_ENV = "AIRTABLE_ATS_TABLE_ID"
TALENT_TABLE_ID_ENV = "AIRTABLE_TABLE_ID"


def get_tables(base_id: str, token: str) -> dict:
    """GET full tables schema for the base."""
    url = f"https://api.airtable.com/v0/meta/bases/{base_id}/tables"
    headers = {"Authorization": f"Bearer {token}"}
    with httpx.Client(timeout=30.0) as client:
        response = client.get(url, headers=headers)
        response.raise_for_status()
        return response.json()


def table_exists_by_name(data: dict, name: str) -> bool:
    """Check if a table with the given name exists."""
    for t in data.get("tables", []):
        if t.get("name") == name:
            return True
    return False


def create_matches_table(
    base_id: str,
    ats_table_id: str,
    talent_table_id: str,
    token: str,
) -> str:
    """Create the Matches table. Returns the new table ID."""
    url = f"https://api.airtable.com/v0/meta/bases/{base_id}/tables"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    body = {
        "name": MATCHES_TABLE_NAME,
        "description": "LLM-refined match details: score, pros, cons per candidate-job",
        "fields": [
            {
                "name": "Name",
                "type": "singleLineText",
                "description": "Match identifier (Job + Candidate)",
            },
            {
                "name": "Job",
                "type": "multipleRecordLinks",
                "description": "Links to the ATS job record",
                "options": {"linkedTableId": ats_table_id},
            },
            {
                "name": "Candidate",
                "type": "multipleRecordLinks",
                "description": "Links to the Talent record",
                "options": {"linkedTableId": talent_table_id},
            },
            {
                "name": "Score",
                "type": "number",
                "description": "LLM fit score 1-10",
                "options": {"precision": 0},
            },
            {
                "name": "Pros",
                "type": "multilineText",
                "description": "Strengths for this role",
            },
            {
                "name": "Cons",
                "type": "multilineText",
                "description": "Gaps or concerns",
            },
            {
                "name": "Rank",
                "type": "number",
                "description": "Position in shortlist (1 = best)",
                "options": {"precision": 0},
            },
            {
                "name": "Date Created",
                "type": "dateTime",
                "description": "When this match was created (matchmaking run timestamp)",
                "options": {
                    "dateFormat": {"name": "iso", "format": "YYYY-MM-DD"},
                    "timeFormat": {"name": "24hour", "format": "HH:mm"},
                    "timeZone": "utc",
                },
            },
        ],
    }
    with httpx.Client(timeout=30.0) as client:
        response = client.post(url, headers=headers, json=body)
        if response.status_code == 403:
            print(
                "\n403 Forbidden: token needs schema.bases:read and schema.bases:write. "
                "Create a token at https://airtable.com/create/tokens with schema scope, "
                "set AIRTABLE_SCHEMA_TOKEN in .env, source it, and run again."
            )
            response.raise_for_status()
        if response.status_code != 200:
            print(f"Response {response.status_code}: {response.text}")
            response.raise_for_status()
    result = response.json()
    table_id = result.get("id", "")
    print(f"Created table '{MATCHES_TABLE_NAME}' with id: {table_id}")
    return table_id


def main() -> int:
    base_id = os.getenv("AIRTABLE_BASE_ID")
    ats_table_id = os.getenv(ATS_TABLE_ID_ENV, "tblrbhITEIBOxwcQV")
    talent_table_id = os.getenv(TALENT_TABLE_ID_ENV)
    token = os.getenv("AIRTABLE_SCHEMA_TOKEN") or os.getenv("AIRTABLE_API_KEY")

    if not base_id or not token:
        print(
            "Set AIRTABLE_BASE_ID and AIRTABLE_SCHEMA_TOKEN (or AIRTABLE_API_KEY) in .env. "
            "Source .env into your shell before running: set -a && source .env && set +a"
        )
        return 1

    if not talent_table_id:
        talent_table_id = "tblOkLOSo4Zjwp0yF"
        print(f"Using default Talent table id: {talent_table_id}")

    print("Fetching base schema...")
    data = get_tables(base_id, token)

    if table_exists_by_name(data, MATCHES_TABLE_NAME):
        print(f"Table '{MATCHES_TABLE_NAME}' already exists. Nothing to do.")
        for t in data.get("tables", []):
            if t.get("name") == MATCHES_TABLE_NAME:
                print(f"  Table id: {t.get('id')}")
                print("  Add AIRTABLE_MATCHES_TABLE_ID=" + t.get("id", "") + " to .env")
                break
        return 0

    print(f"Creating table '{MATCHES_TABLE_NAME}'...")
    table_id = create_matches_table(base_id, ats_table_id, talent_table_id, token)

    print("\nDone. Add to .env:")
    print(f"  AIRTABLE_MATCHES_TABLE_ID={table_id}")
    print("\nThe ATS table should now have a linked column showing matches per job.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
