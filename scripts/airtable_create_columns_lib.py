"""Shared logic for creating Airtable (N)-prefixed normalized columns.

Used by create_airtable_normalized_columns.py (candidates) and
create_airtable_normalized_job_columns.py (jobs). Both scripts pass
table ID, writeback field mapping, field specs, and optional extra columns.
"""

import json
import os
from datetime import UTC, datetime

import httpx

BACKUP_DIR = os.path.join(os.path.dirname(__file__), "airtable_backups")


def get_tables_response(base_id: str, token: str) -> dict:
    """GET full tables schema for the base."""
    url = f"https://api.airtable.com/v0/meta/bases/{base_id}/tables"
    headers = {"Authorization": f"Bearer {token}"}
    with httpx.Client(timeout=30.0) as client:
        response = client.get(url, headers=headers)
        response.raise_for_status()
        return response.json()


def get_existing_field_names(base_id: str, table_id: str, token: str) -> set[str]:
    """GET table schema and return the set of existing field names."""
    data = get_tables_response(base_id, token)
    for table in data.get("tables", []):
        if table.get("id") == table_id:
            return {f.get("name") for f in table.get("fields", []) if f.get("name")}
    return set()


def fetch_all_records(base_id: str, table_id: str, token: str) -> list[dict]:
    """Fetch all records from the table (follows offset pagination)."""
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


def backup_table_to_file(
    base_id: str,
    table_id: str,
    token: str,
    backup_filename_prefix: str,
) -> str:
    """Save a copy of the table schema and all records to a timestamped JSON file.

    Returns the path to the written file.
    """
    os.makedirs(BACKUP_DIR, exist_ok=True)
    ts = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    filename = f"{backup_filename_prefix}_{base_id}_{table_id}_{ts}.json"
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
    """POST to create one field on the table."""
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
                "Create a new token at https://airtable.com/create/tokens with schema write for this base, "
                "set AIRTABLE_SCHEMA_TOKEN in .env, and run again."
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
                print(f"  Skipping {name!r} (already exists or invalid name).")
                return
            print(f"\n422 Unprocessable Entity for field {name!r}. Response: {response.text}")
            response.raise_for_status()
        response.raise_for_status()


def run(
    base_id: str,
    table_id: str,
    token: str,
    writeback_fields: dict[str, str],
    field_specs: dict[str, dict],
    *,
    skip_backup: bool = False,
    backup_prefix: str = "table",
    extra_columns: list[tuple[str, dict]] | None = None,
) -> None:
    """Backup (optional), then create missing (N) columns and any extra columns.

    writeback_fields: mapping from our_key to Airtable field name (e.g. from AIRTABLE_*_WRITEBACK_FIELDS).
    field_specs: mapping from our_key to Airtable field spec {type, options?}.
    extra_columns: optional list of (field_name, spec) to create in addition to writeback columns.
    """
    if not skip_backup:
        print("Writing local backup (records + schema)...")
        filepath = backup_table_to_file(base_id, table_id, token, backup_prefix)
        print(f"  Backup saved to: {filepath}")
    else:
        print("Skipping backup (--skip-backup).")

    print("Fetching existing field names...")
    existing = get_existing_field_names(base_id, table_id, token)
    print(f"Found {len(existing)} existing fields")

    to_create: list[tuple[str, dict]] = []
    for our_key, airtable_name in writeback_fields.items():
        if airtable_name not in existing:
            to_create.append((airtable_name, field_specs.get(our_key, {"type": "singleLineText"})))

    for name, spec in extra_columns or []:
        if name not in existing:
            to_create.append((name, spec))

    if not to_create:
        print("All columns already exist. Nothing to do.")
        return

    print(f"Creating {len(to_create)} columns...")
    for name, spec in to_create:
        print(f"  Creating: {name}")
        create_field(base_id, table_id, token, name, spec)
    print("Done.")
