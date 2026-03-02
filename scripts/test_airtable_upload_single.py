#!/usr/bin/env python3
# ruff: noqa: E402
"""Test Airtable PATCH for one normalized candidate (debug 422).

Loads one NormalizedCandidate from the DB by airtable_record_id, builds the same
payload as airtable_candidate_sync, and PATCHes to Airtable. On 422, prints the
full response body so you can see Airtable's validation error.

Usage:
  poetry run python scripts/test_airtable_upload_single.py [RECORD_ID]
  # or
  RECORD_ID=reci1JNCB8rIZuVDO poetry run python scripts/test_airtable_upload_single.py

Requires: .env with POSTGRES_*, AIRTABLE_BASE_ID, AIRTABLE_TABLE_ID, and
  AIRTABLE_WRITE_TOKEN or AIRTABLE_API_KEY (with data.records:write).

If you get 422 INVALID_MULTIPLE_CHOICE_OPTIONS: in Airtable change these (N) columns
from "Multiple select" to "Long text": (N) Skills Summary, (N) Companies Summary,
(N) Desired Job Categories, (N) Notable Achievements, (N) Verified Communities.
"""

import json
import os
import sys

import httpx
from dotenv import load_dotenv

load_dotenv()

# Add project root for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from talent_matching.resources.matchmaking import MatchmakingResource  # noqa: E402
from talent_matching.utils.airtable_mapper import (
    normalized_candidate_to_airtable_fields,  # noqa: E402
)


def main() -> None:
    record_id = (sys.argv[1] if len(sys.argv) > 1 else None) or os.getenv(
        "RECORD_ID", "reci1JNCB8rIZuVDO"
    )

    base_id = os.getenv("AIRTABLE_BASE_ID")
    table_id = os.getenv("AIRTABLE_TABLE_ID")
    api_key = os.getenv("AIRTABLE_WRITE_TOKEN") or os.getenv("AIRTABLE_API_KEY")
    if not base_id or not table_id or not api_key:
        print(
            "Set AIRTABLE_BASE_ID, AIRTABLE_TABLE_ID, and AIRTABLE_WRITE_TOKEN or AIRTABLE_API_KEY in .env"
        )
        sys.exit(1)

    matchmaking = MatchmakingResource()
    candidate = matchmaking.get_normalized_candidate_by_airtable_record_id(record_id)
    if not candidate:
        print(f"No normalized candidate in DB for airtable_record_id={record_id!r}")
        sys.exit(1)

    fields = normalized_candidate_to_airtable_fields(candidate)
    print(f"Record ID: {record_id}")
    print(f"Fields to PATCH ({len(fields)}):")
    for k, v in list(fields.items())[:15]:
        preview = repr(v)[:60] + ("..." if len(repr(v)) > 60 else "")
        print(f"  {k!r}: {preview}")
    if len(fields) > 15:
        print(f"  ... and {len(fields) - 15} more")

    url = f"https://api.airtable.com/v0/{base_id}/{table_id}/{record_id}"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    body = {"fields": fields}

    print("\nPATCH request...")
    with httpx.Client(timeout=60.0) as client:
        response = client.patch(url, headers=headers, json=body)

    print(f"Status: {response.status_code}")
    if response.status_code == 422:
        print("Response body (422 Unprocessable Entity):")
        text = response.text
        print(text)
        if response.headers.get("content-type", "").startswith("application/json"):
            parsed = response.json()
            print("\nParsed JSON:")
            print(json.dumps(parsed, indent=2))
        sys.exit(1)
    if response.status_code != 200:
        print(f"Response: {response.text}")
        sys.exit(1)

    print("OK: record updated.")
    result = response.json()
    print(f"Updated record keys: {list(result.get('fields', {}).keys())[:10]}...")


if __name__ == "__main__":
    main()
