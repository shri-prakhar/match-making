"""Add Matchmaking Last Run (ATS) and Date Created (Matches) columns.

Run this before using the updated upload_matches_to_ats asset.

Usage:
  set -a && source .env && set +a && poetry run python scripts/add_matchmaking_date_columns.py
"""

import os
import sys

from dotenv import load_dotenv

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from scripts.airtable_create_columns_lib import create_field, get_existing_field_names

load_dotenv()

ATS_MATCHMAKING_LAST_RUN_SPEC = {
    "type": "dateTime",
    "options": {
        "dateFormat": {"name": "iso", "format": "YYYY-MM-DD"},
        "timeFormat": {"name": "24hour", "format": "HH:mm"},
        "timeZone": "utc",
    },
}

MATCHES_DATE_CREATED_SPEC = {
    "type": "dateTime",
    "options": {
        "dateFormat": {"name": "iso", "format": "YYYY-MM-DD"},
        "timeFormat": {"name": "24hour", "format": "HH:mm"},
        "timeZone": "utc",
    },
}


def main() -> int:
    base_id = os.getenv("AIRTABLE_BASE_ID")
    ats_table_id = os.getenv("AIRTABLE_ATS_TABLE_ID", "tblrbhITEIBOxwcQV")
    matches_table_id = os.getenv("AIRTABLE_MATCHES_TABLE_ID")
    token = os.getenv("AIRTABLE_SCHEMA_TOKEN") or os.getenv("AIRTABLE_API_KEY")

    if not base_id or not token:
        print("Set AIRTABLE_BASE_ID and AIRTABLE_SCHEMA_TOKEN (or AIRTABLE_API_KEY) in .env")
        return 1

    if not matches_table_id:
        print("AIRTABLE_MATCHES_TABLE_ID not set; will only add Matchmaking Last Run to ATS")
        matches_table_id = None

    # ATS table: Matchmaking Last Run
    print("Checking ATS table for Matchmaking Last Run...")
    existing_ats = get_existing_field_names(base_id, ats_table_id, token)
    if "Matchmaking Last Run" not in existing_ats:
        print("  Creating: Matchmaking Last Run")
        create_field(
            base_id, ats_table_id, token, "Matchmaking Last Run", ATS_MATCHMAKING_LAST_RUN_SPEC
        )
    else:
        print("  Matchmaking Last Run already exists")

    # Matches table: Date Created
    if matches_table_id:
        print("Checking Matches table for Date Created...")
        existing_matches = get_existing_field_names(base_id, matches_table_id, token)
        if "Date Created" not in existing_matches:
            print("  Creating: Date Created")
            create_field(
                base_id, matches_table_id, token, "Date Created", MATCHES_DATE_CREATED_SPEC
            )
        else:
            print("  Date Created already exists")

    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
