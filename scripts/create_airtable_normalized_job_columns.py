"""Create (N)-prefixed normalized job columns in the Airtable ATS table.

Uses the Airtable Meta API to add the columns required for airtable_job_sync.
airtable_job_sync writes to the ATS table (same table as Job Status, AI PROPOSTED CANDIDATES).

Requires an Airtable Personal Access Token with schema read + write access to the base
(create at https://airtable.com/create/tokens and add the schema scope for your base).

Safety / rollback:
  - This script writes a local backup (records + schema) to scripts/airtable_backups/
    before creating any columns, unless you pass --skip-backup.
  - Airtable also has built-in base snapshots: in the base, click the history icon
    (upper-right) -> Snapshots -> Take a snapshot.

Usage:
  From project root (with .env loaded):
    poetry run python scripts/create_airtable_normalized_job_columns.py
  Skip local backup:
    poetry run python scripts/create_airtable_normalized_job_columns.py --skip-backup
  Or with explicit env:
    AIRTABLE_BASE_ID=appXXX AIRTABLE_ATS_TABLE_ID=tblXXX AIRTABLE_API_KEY=patXXX \\
    python scripts/create_airtable_normalized_job_columns.py

Skips columns that already exist. Uses the same (N) names as AIRTABLE_JOBS_WRITEBACK_FIELDS.
"""

import os
import sys

from dotenv import load_dotenv

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from scripts.airtable_create_columns_lib import run
from talent_matching.utils.airtable_mapper import (
    AIRTABLE_JOBS_WRITEBACK_FIELDS,
    SMART_IDEAL_CANDIDATE_PROFILE_FIELD,
)

load_dotenv()

# Airtable field type and options per normalized job field (model attribute name)
JOB_FIELD_SPECS: dict[str, dict] = {
    "job_title": {"type": "singleLineText"},
    "job_category": {"type": "singleLineText"},
    "role_type": {"type": "singleLineText"},
    "company_name": {"type": "singleLineText"},
    "company_stage": {"type": "singleLineText"},
    "company_size": {"type": "singleLineText"},
    "role_summary": {"type": "multilineText"},
    "responsibilities": {"type": "multilineText"},
    "nice_to_haves": {"type": "multilineText"},
    "benefits": {"type": "multilineText"},
    "team_context": {"type": "multilineText"},
    "seniority_level": {
        "type": "singleSelect",
        "options": {
            "choices": [
                {"name": "junior"},
                {"name": "mid"},
                {"name": "senior"},
                {"name": "lead"},
                {"name": "principal"},
            ]
        },
    },
    "education_required": {"type": "singleLineText"},
    "domain_experience": {"type": "multilineText"},
    "tech_stack": {"type": "multilineText"},
    "location_type": {
        "type": "singleSelect",
        "options": {
            "choices": [
                {"name": "remote"},
                {"name": "hybrid"},
                {"name": "onsite"},
            ]
        },
    },
    "locations": {"type": "multilineText"},
    "timezone_requirements": {"type": "singleLineText"},
    "employment_type": {"type": "multilineText"},
    "min_years_experience": {"type": "number", "options": {"precision": 0}},
    "max_years_experience": {"type": "number", "options": {"precision": 0}},
    "salary_min": {"type": "number", "options": {"precision": 0}},
    "salary_max": {"type": "number", "options": {"precision": 0}},
    "salary_currency": {"type": "singleLineText"},
    "has_equity": {"type": "checkbox", "options": {"icon": "check", "color": "greenBright"}},
    "has_token_compensation": {
        "type": "checkbox",
        "options": {"icon": "check", "color": "greenBright"},
    },
    "narrative_experience": {"type": "multilineText"},
    "narrative_domain": {"type": "multilineText"},
    "narrative_personality": {"type": "multilineText"},
    "narrative_impact": {"type": "multilineText"},
    "narrative_technical": {"type": "multilineText"},
    "narrative_role": {"type": "multilineText"},
    "must_have_skills": {"type": "multilineText"},
    "nice_to_have_skills": {"type": "multilineText"},
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

START_MATCHMAKING_SPEC = {"type": "checkbox", "options": {"icon": "check", "color": "yellowBright"}}
SMART_IDEAL_CANDIDATE_PROFILE_SPEC = {"type": "multilineText"}


def main() -> None:
    skip_backup = "--skip-backup" in sys.argv
    base_id = os.getenv("AIRTABLE_BASE_ID")
    table_id = os.getenv("AIRTABLE_ATS_TABLE_ID")
    token = os.getenv("AIRTABLE_SCHEMA_TOKEN") or os.getenv("AIRTABLE_API_KEY")
    if not base_id or not table_id or not token:
        print(
            "Set AIRTABLE_BASE_ID, AIRTABLE_ATS_TABLE_ID, and "
            "AIRTABLE_API_KEY (or AIRTABLE_SCHEMA_TOKEN) in .env"
        )
        sys.exit(1)

    print(f"Base: {base_id}, ATS Table: {table_id}")

    extra_columns = [
        ("Start Matchmaking", START_MATCHMAKING_SPEC),
        (SMART_IDEAL_CANDIDATE_PROFILE_FIELD, SMART_IDEAL_CANDIDATE_PROFILE_SPEC),
    ]

    run(
        base_id,
        table_id,
        token,
        AIRTABLE_JOBS_WRITEBACK_FIELDS,
        JOB_FIELD_SPECS,
        skip_backup=skip_backup,
        backup_prefix="jobs",
        extra_columns=extra_columns,
    )


if __name__ == "__main__":
    main()
