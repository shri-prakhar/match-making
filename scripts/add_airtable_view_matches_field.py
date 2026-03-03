#!/usr/bin/env python3
"""Add the 'View Matches' URL field to the ATS table.

This field will be populated by the pipeline with a link to the Matches table
view filtered by the job. Recruiters click it to see score, pros, cons per candidate.

Prerequisites:
  1. Create a shared view of the Matches table in Airtable:
     - Open Matches table → create or use a view → Share view → Copy link
     - The link looks like: https://airtable.com/shrXXXXXXXXXX
  2. Add to .env: AIRTABLE_MATCHES_VIEW_URL=https://airtable.com/shrXXXXXXXXXX

Requires: AIRTABLE_BASE_ID, AIRTABLE_ATS_TABLE_ID, AIRTABLE_SCHEMA_TOKEN

Usage (source .env into shell first):
  set -a && source .env && set +a && poetry run python scripts/add_airtable_view_matches_field.py
"""

import os
import sys

import httpx

ATS_TABLE_ID_ENV = "AIRTABLE_ATS_TABLE_ID"
VIEW_MATCHES_FIELD = "View Matches"


def get_existing_field_names(base_id: str, table_id: str, token: str) -> set[str]:
    """GET table schema and return existing field names."""
    url = f"https://api.airtable.com/v0/meta/bases/{base_id}/tables"
    headers = {"Authorization": f"Bearer {token}"}
    with httpx.Client(timeout=30.0) as client:
        response = client.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
    for t in data.get("tables", []):
        if t.get("id") == table_id:
            return {f.get("name") for f in t.get("fields", []) if f.get("name")}
    return set()


def create_field(base_id: str, table_id: str, token: str, name: str, spec: dict) -> None:
    """POST to create one field on the table."""
    url = f"https://api.airtable.com/v0/meta/bases/{base_id}/tables/{table_id}/fields"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    body = {"name": name, "type": spec["type"]}
    if "options" in spec:
        body["options"] = spec["options"]
    if "description" in spec:
        body["description"] = spec["description"]
    with httpx.Client(timeout=30.0) as client:
        response = client.post(url, headers=headers, json=body)
        if response.status_code == 403:
            print(
                "\n403 Forbidden: token needs schema.bases:read and schema.bases:write. "
                "Create a token at https://airtable.com/create/tokens with schema scope."
            )
            response.raise_for_status()
        if response.status_code != 200:
            print(f"Response {response.status_code}: {response.text}")
            response.raise_for_status()


def main() -> int:
    base_id = os.getenv("AIRTABLE_BASE_ID")
    ats_table_id = os.getenv(ATS_TABLE_ID_ENV, "tblrbhITEIBOxwcQV")
    token = os.getenv("AIRTABLE_SCHEMA_TOKEN") or os.getenv("AIRTABLE_API_KEY")

    if not base_id or not token:
        print(
            "Set AIRTABLE_BASE_ID and AIRTABLE_SCHEMA_TOKEN (or AIRTABLE_API_KEY) in .env. "
            "Source .env into your shell: set -a && source .env && set +a"
        )
        return 1

    existing = get_existing_field_names(base_id, ats_table_id, token)
    if VIEW_MATCHES_FIELD in existing:
        print(f"Field '{VIEW_MATCHES_FIELD}' already exists on ATS table. Nothing to do.")
        return 0

    print(f"Adding '{VIEW_MATCHES_FIELD}' URL field to ATS table...")
    create_field(
        base_id,
        ats_table_id,
        token,
        VIEW_MATCHES_FIELD,
        {
            "type": "url",
            "description": "Link to Matches table view filtered by this job (populated by pipeline)",
        },
    )
    print(f"Done. Add AIRTABLE_MATCHES_VIEW_URL to .env (shared view URL of Matches table).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
