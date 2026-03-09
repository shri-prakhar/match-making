"""Add scoring breakdown columns to the Airtable Matches table.

Adds: Combined Score, Role Similarity, Domain Similarity, Culture Similarity,
Skills Fit, Compensation Fit, Experience Fit, Location Fit, Matching Skills,
Missing Skills.

Usage:
  set -a && source .env && set +a && poetry run python scripts/add_matches_scoring_columns.py
"""

import os
import sys

from dotenv import load_dotenv

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from scripts.airtable_create_columns_lib import create_field, get_existing_field_names

load_dotenv()

SCORING_COLUMNS: list[tuple[str, dict]] = [
    ("Combined Score", {"type": "number", "options": {"precision": 2}}),
    ("Role Similarity", {"type": "number", "options": {"precision": 4}}),
    ("Domain Similarity", {"type": "number", "options": {"precision": 4}}),
    ("Culture Similarity", {"type": "number", "options": {"precision": 4}}),
    ("Skills Fit", {"type": "number", "options": {"precision": 4}}),
    ("Compensation Fit", {"type": "number", "options": {"precision": 4}}),
    ("Experience Fit", {"type": "number", "options": {"precision": 4}}),
    ("Location Fit", {"type": "number", "options": {"precision": 4}}),
    ("Matching Skills", {"type": "multilineText"}),
    ("Missing Skills", {"type": "multilineText"}),
]


def main() -> int:
    base_id = os.getenv("AIRTABLE_BASE_ID")
    matches_table_id = os.getenv("AIRTABLE_MATCHES_TABLE_ID")
    token = os.getenv("AIRTABLE_SCHEMA_TOKEN") or os.getenv("AIRTABLE_API_KEY")

    if not base_id or not token:
        print("Set AIRTABLE_BASE_ID and AIRTABLE_SCHEMA_TOKEN (or AIRTABLE_API_KEY) in .env")
        return 1
    if not matches_table_id:
        print("AIRTABLE_MATCHES_TABLE_ID not set in .env")
        return 1

    print(f"Checking Matches table ({matches_table_id}) for scoring columns...")
    existing = get_existing_field_names(base_id, matches_table_id, token)

    created = 0
    for name, spec in SCORING_COLUMNS:
        if name in existing:
            print(f"  {name}: already exists")
        else:
            print(f"  Creating: {name}")
            create_field(base_id, matches_table_id, token, name, spec)
            created += 1

    print(f"\nDone. Created {created} new column(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
