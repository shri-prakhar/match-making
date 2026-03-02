"""Create (N)-prefixed normalized job columns in the Airtable jobs table.

Uses the Airtable Meta API to add the columns required for airtable_job_sync.

Requires an Airtable Personal Access Token with schema read + write access to the base
(create at https://airtable.com/create/tokens and add the schema scope for your base).

Safety / rollback:
  - This script writes a local backup (records + schema) to scripts/airtable_backups/
    before creating any columns, unless you pass --skip-backup.
  - Airtable also has built-in base snapshots: in the base, click the history icon
    (upper-right) -> Snapshots -> Take a snapshot.

Usage:
  From project root (with .env loaded):
    uv run python scripts/create_airtable_normalized_job_columns.py
  Skip local backup:
    uv run python scripts/create_airtable_normalized_job_columns.py --skip-backup
  Or with explicit env:
    AIRTABLE_BASE_ID=appXXX AIRTABLE_JOBS_TABLE_ID=tblXXX AIRTABLE_API_KEY=patXXX \\
    python scripts/create_airtable_normalized_job_columns.py

Skips columns that already exist. Uses the same (N) names as AIRTABLE_JOBS_WRITEBACK_FIELDS.
"""

import json
import os
import sys
from datetime import UTC, datetime

import httpx
from dotenv import load_dotenv

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from talent_matching.utils.airtable_mapper import (
    AIRTABLE_JOBS_WRITEBACK_FIELDS,
    SMART_IDEAL_CANDIDATE_PROFILE_FIELD,
)

load_dotenv()

# Airtable field type and options per normalized job field (model attribute name).
# Types: singleLineText, multilineText, number, singleSelect, checkbox, dateTime
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

# The Start Matchmaking checkbox (not part of (N) fields but needed for the feedback loop)
START_MATCHMAKING_SPEC = {"type": "checkbox", "options": {"icon": "check", "color": "yellowBright"}}

# SMART IDEAL CANDIDATE PROFILE: filled by airtable_job_sync from normalized narrative prose
SMART_IDEAL_CANDIDATE_PROFILE_SPEC = {"type": "multilineText"}

BACKUP_DIR = os.path.join(os.path.dirname(__file__), "airtable_backups")


def get_tables_response(base_id: str, token: str) -> dict:
    url = f"https://api.airtable.com/v0/meta/bases/{base_id}/tables"
    headers = {"Authorization": f"Bearer {token}"}
    with httpx.Client(timeout=30.0) as client:
        response = client.get(url, headers=headers)
        response.raise_for_status()
        return response.json()


def get_existing_field_names(base_id: str, table_id: str, token: str) -> set[str]:
    data = get_tables_response(base_id, token)
    for table in data.get("tables", []):
        if table.get("id") == table_id:
            return {f.get("name") for f in table.get("fields", []) if f.get("name")}
    return set()


def fetch_all_records(base_id: str, table_id: str, token: str) -> list[dict]:
    url = f"https://api.airtable.com/v0/{base_id}/{table_id}"
    headers = {"Authorization": f"Bearer {token}"}
    all_records: list[dict] = []
    offset: str | None = None
    with httpx.Client(timeout=60.0) as client:
        while True:
            params = {} if offset is None else {"offset": offset}
            response = client.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            all_records.extend(data.get("records", []))
            offset = data.get("offset")
            if not offset:
                break
    return all_records


def backup_table_to_file(base_id: str, table_id: str, token: str) -> str:
    os.makedirs(BACKUP_DIR, exist_ok=True)
    ts = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    filename = f"jobs_{base_id}_{table_id}_{ts}.json"
    filepath = os.path.join(BACKUP_DIR, filename)

    data = get_tables_response(base_id, token)
    table_schema = {"id": table_id, "fields": []}
    for t in data.get("tables", []):
        if t.get("id") == table_id:
            table_schema = {"name": t.get("name"), "id": t.get("id"), "fields": t.get("fields", [])}
            break

    print("Fetching all records for backup...")
    records = fetch_all_records(base_id, table_id, token)
    print(f"  {len(records)} records")

    payload = {
        "backed_up_at": datetime.now(UTC).isoformat(),
        "base_id": base_id,
        "table_id": table_id,
        "schema": table_schema,
        "record_count": len(records),
        "records": records,
    }
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    return filepath


def create_field(base_id: str, table_id: str, token: str, name: str, spec: dict) -> None:
    url = f"https://api.airtable.com/v0/meta/bases/{base_id}/tables/{table_id}/fields"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    body = {"name": name, "type": spec["type"]}
    if "options" in spec:
        body["options"] = spec["options"]
    with httpx.Client(timeout=30.0) as client:
        response = client.post(url, headers=headers, json=body)
        if response.status_code == 403:
            print(
                "\n403 Forbidden: token does not have schema write access. "
                "Create a new token at https://airtable.com/create/tokens with "
                "'schema.bases:write' for this base, set AIRTABLE_SCHEMA_TOKEN in .env, and run again."
            )
            response.raise_for_status()
        if response.status_code == 422:
            err = (
                response.json()
                if response.headers.get("content-type", "").startswith("application/json")
                else {}
            )
            err_type = err.get("error", {}).get("type", "") if isinstance(err, dict) else ""
            if err_type == "DUPLICATE_OR_EMPTY_FIELD_NAME":
                print(f"  Skipping {name!r} (already exists).")
                return
            print(f"\n422 Unprocessable Entity for field {name!r}. Response: {response.text}")
            response.raise_for_status()
        response.raise_for_status()


def main() -> None:
    skip_backup = "--skip-backup" in sys.argv
    base_id = os.getenv("AIRTABLE_BASE_ID")
    table_id = os.getenv("AIRTABLE_JOBS_TABLE_ID")
    # Schema token for Meta API (create fields, read schema)
    schema_token = os.getenv("AIRTABLE_SCHEMA_TOKEN") or os.getenv("AIRTABLE_API_KEY")
    # Data token for Records API (backup); falls back to schema_token if unset
    data_token = os.getenv("AIRTABLE_API_KEY") or schema_token
    if not base_id or not table_id or not schema_token:
        print(
            "Set AIRTABLE_BASE_ID, AIRTABLE_JOBS_TABLE_ID, and "
            "AIRTABLE_API_KEY (or AIRTABLE_SCHEMA_TOKEN) in .env"
        )
        sys.exit(1)

    print(f"Base: {base_id}, Jobs Table: {table_id}")

    if not skip_backup:
        print("Writing local backup (records + schema)...")
        filepath = backup_table_to_file(base_id, table_id, data_token)
        print(f"  Backup saved to: {filepath}")
    else:
        print("Skipping backup (--skip-backup).")

    print("Fetching existing field names...")
    existing = get_existing_field_names(base_id, table_id, schema_token)
    print(f"Found {len(existing)} existing fields")

    to_create: list[tuple[str, dict]] = []

    # (N)-prefixed normalized job columns
    for our_key, airtable_name in AIRTABLE_JOBS_WRITEBACK_FIELDS.items():
        if airtable_name not in existing:
            to_create.append(
                (airtable_name, JOB_FIELD_SPECS.get(our_key, {"type": "singleLineText"}))
            )

    # Start Matchmaking checkbox
    if "Start Matchmaking" not in existing:
        to_create.append(("Start Matchmaking", START_MATCHMAKING_SPEC))

    # SMART IDEAL CANDIDATE PROFILE (filled from normalized job narratives)
    if SMART_IDEAL_CANDIDATE_PROFILE_FIELD not in existing:
        to_create.append((SMART_IDEAL_CANDIDATE_PROFILE_FIELD, SMART_IDEAL_CANDIDATE_PROFILE_SPEC))

    if not to_create:
        print("All columns already exist. Nothing to do.")
        return

    print(f"Creating {len(to_create)} columns...")
    for name, spec in to_create:
        print(f"  Creating: {name} ({spec['type']})")
        create_field(base_id, table_id, schema_token, name, spec)
    print("Done.")


if __name__ == "__main__":
    main()
