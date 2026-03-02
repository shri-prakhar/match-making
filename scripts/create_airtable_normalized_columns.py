"""Create (N)-prefixed normalized candidate columns in the Airtable candidates table.

Uses the Airtable API to add the columns required for airtable_candidate_sync.
Requires an Airtable Personal Access Token with schema read + write access to the base
(create at https://airtable.com/create/tokens and add the schema scope for your base).
If you get 403/401 on GET or POST, the token may need "schema.bases:read" and write
for the base, or your plan may require a separate Metadata API token.

Safety / rollback:
  - This script writes a local backup (records + schema) to scripts/airtable_backups/
    before creating any columns, unless you pass --skip-backup.
  - Airtable also has built-in base snapshots: in the base, click the history icon
    (upper-right) → Snapshots → Take a snapshot. Restoring creates a new base
    (see https://support.airtable.com/docs/taking-and-restoring-base-snapshots).

Usage:
  From project root (with .env loaded):
    poetry run python scripts/create_airtable_normalized_columns.py
  Skip local backup (e.g. you already took an Airtable snapshot):
    poetry run python scripts/create_airtable_normalized_columns.py --skip-backup
  Or with explicit env:
    AIRTABLE_BASE_ID=appXXX AIRTABLE_TABLE_ID=tblXXX AIRTABLE_API_KEY=patXXX \\
    python scripts/create_airtable_normalized_columns.py

Skips columns that already exist. Uses the same (N) names as AIRTABLE_CANDIDATES_WRITEBACK_FIELDS.
"""

import os
import sys

from dotenv import load_dotenv

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from scripts.airtable_create_columns_lib import run
from talent_matching.utils.airtable_mapper import AIRTABLE_CANDIDATES_WRITEBACK_FIELDS

load_dotenv()

# Airtable field type and options per normalized field (model attribute name)
FIELD_SPECS: dict[str, dict] = {
    "full_name": {"type": "singleLineText"},
    "email": {"type": "email"},
    "phone": {"type": "phoneNumber"},
    "location_city": {"type": "singleLineText"},
    "location_country": {"type": "singleLineText"},
    "location_region": {"type": "singleLineText"},
    "timezone": {"type": "singleLineText"},
    "professional_summary": {"type": "multilineText"},
    "current_role": {"type": "singleLineText"},
    "seniority_level": {
        "type": "singleSelect",
        "options": {
            "choices": [
                {"name": "junior"},
                {"name": "mid"},
                {"name": "senior"},
                {"name": "lead"},
                {"name": "principal"},
                {"name": "executive"},
            ]
        },
    },
    "years_of_experience": {"type": "number", "options": {"precision": 0}},
    "desired_job_categories": {"type": "multilineText"},
    "skills_summary": {"type": "multilineText"},
    "companies_summary": {"type": "multilineText"},
    "notable_achievements": {"type": "multilineText"},
    "verified_communities": {"type": "multilineText"},
    "compensation_min": {"type": "number", "options": {"precision": 0}},
    "compensation_max": {"type": "number", "options": {"precision": 0}},
    "compensation_currency": {"type": "singleLineText"},
    "job_count": {"type": "number", "options": {"precision": 0}},
    "job_switches_count": {"type": "number", "options": {"precision": 0}},
    "average_tenure_months": {"type": "number", "options": {"precision": 0}},
    "longest_tenure_months": {"type": "number", "options": {"precision": 0}},
    "education_highest_degree": {"type": "singleLineText"},
    "education_field": {"type": "singleLineText"},
    "education_institution": {"type": "singleLineText"},
    "hackathon_wins_count": {"type": "number", "options": {"precision": 0}},
    "hackathon_total_prize_usd": {"type": "number", "options": {"precision": 0}},
    "solana_hackathon_wins": {"type": "number", "options": {"precision": 0}},
    "x_handle": {"type": "singleLineText"},
    "linkedin_handle": {"type": "singleLineText"},
    "github_handle": {"type": "singleLineText"},
    "social_followers_total": {"type": "number", "options": {"precision": 0}},
    "verification_status": {
        "type": "singleSelect",
        "options": {
            "choices": [
                {"name": "unverified"},
                {"name": "verified"},
            ]
        },
    },
    "verification_notes": {"type": "multilineText"},
    "verified_at": {
        "type": "dateTime",
        "options": {
            "dateFormat": {"name": "iso", "format": "YYYY-MM-DD"},
            "timeFormat": {"name": "24hour", "format": "HH:mm"},
            "timeZone": "utc",
        },
    },
    "prompt_version": {"type": "singleLineText"},
    "model_version": {"type": "singleLineText"},
    "confidence_score": {"type": "number", "options": {"precision": 4}},
    "normalized_at": {
        "type": "dateTime",
        "options": {
            "dateFormat": {"name": "iso", "format": "YYYY-MM-DD"},
            "timeFormat": {"name": "24hour", "format": "HH:mm"},
            "timeZone": "utc",
        },
    },
}


def main() -> None:
    skip_backup = "--skip-backup" in sys.argv
    base_id = os.getenv("AIRTABLE_BASE_ID")
    table_id = os.getenv("AIRTABLE_TABLE_ID")
    token = os.getenv("AIRTABLE_SCHEMA_TOKEN") or os.getenv("AIRTABLE_API_KEY")
    if not base_id or not table_id or not token:
        print(
            "Set AIRTABLE_BASE_ID, AIRTABLE_TABLE_ID, and AIRTABLE_API_KEY (or AIRTABLE_SCHEMA_TOKEN) in .env"
        )
        sys.exit(1)

    print(f"Base: {base_id}, Table: {table_id}")

    run(
        base_id,
        table_id,
        token,
        AIRTABLE_CANDIDATES_WRITEBACK_FIELDS,
        FIELD_SPECS,
        skip_backup=skip_backup,
        backup_prefix="candidates",
    )


if __name__ == "__main__":
    main()
