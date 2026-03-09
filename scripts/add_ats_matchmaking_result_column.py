"""Add Matchmaking Result column to the ATS table.

This field indicates matchmaking outcome:
- "No suitable candidates found" when the LLM selects 0 candidates
- "X candidates proposed" when candidates are found

Usage:
  set -a && source .env && set +a && poetry run python scripts/add_ats_matchmaking_result_column.py
"""

import os
import sys

from dotenv import load_dotenv

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from scripts.airtable_create_columns_lib import create_field, get_existing_field_names

load_dotenv()

FIELD_NAME = "Matchmaking Result"
FIELD_SPEC = {"type": "singleLineText"}


def main() -> int:
    base_id = os.getenv("AIRTABLE_BASE_ID")
    ats_table_id = os.getenv("AIRTABLE_ATS_TABLE_ID", "tblrbhITEIBOxwcQV")
    token = os.getenv("AIRTABLE_SCHEMA_TOKEN") or os.getenv("AIRTABLE_API_KEY")

    if not base_id or not token:
        print("Set AIRTABLE_BASE_ID and AIRTABLE_SCHEMA_TOKEN (or AIRTABLE_API_KEY) in .env")
        return 1

    print(f"Checking ATS table ({ats_table_id}) for '{FIELD_NAME}'...")
    existing = get_existing_field_names(base_id, ats_table_id, token)

    if FIELD_NAME in existing:
        print(f"  {FIELD_NAME}: already exists")
    else:
        print(f"  Creating: {FIELD_NAME}")
        create_field(base_id, ats_table_id, token, FIELD_NAME, FIELD_SPEC)

    print("\nDone.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
