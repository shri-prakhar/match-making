#!/usr/bin/env python3
"""Try to change (N) multiple-select columns to Long text via Airtable Metadata API.

Airtable's multiple-select columns only accept existing options; we send newline-separated
strings. This script attempts PATCH to update the field type to multilineText.

If the API returns 422 (Airtable does not allow changing field type via API), you must
change these columns manually in Airtable:
  (N) Desired Job Categories, (N) Skills Summary, (N) Companies Summary,
  (N) Notable Achievements, (N) Verified Communities
→ Field type: Multiple select → Long text.

Requires: AIRTABLE_BASE_ID, AIRTABLE_TABLE_ID, AIRTABLE_SCHEMA_TOKEN (or AIRTABLE_API_KEY)
with schema write access.

Usage:
  poetry run python scripts/airtable_change_multiselect_to_longtext.py
"""

import os
import sys

import httpx
from dotenv import load_dotenv

load_dotenv()

# Names of (N) columns that must be Long text (not Multiple select)
FIELDS_TO_LONGTEXT = [
    "(N) Desired Job Categories",
    "(N) Skills Summary",
    "(N) Companies Summary",
    "(N) Notable Achievements",
    "(N) Verified Communities",
]


def get_tables(base_id: str, token: str) -> dict:
    url = f"https://api.airtable.com/v0/meta/bases/{base_id}/tables"
    headers = {"Authorization": f"Bearer {token}"}
    with httpx.Client(timeout=30.0) as client:
        response = client.get(url, headers=headers)
        response.raise_for_status()
        return response.json()


def update_field(
    base_id: str,
    table_id: str,
    field_id: str,
    token: str,
    field_name: str,
    new_type: str = "multilineText",
) -> None:
    """PATCH field to new type. Airtable Metadata API may support PATCH on a field."""
    url = f"https://api.airtable.com/v0/meta/bases/{base_id}/tables/{table_id}/fields/{field_id}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    # Include name in case API requires it; type + name is common for update
    body = {"name": field_name, "type": new_type}
    with httpx.Client(timeout=30.0) as client:
        response = client.patch(url, headers=headers, json=body)
        if response.status_code != 200:
            print(f"    Response {response.status_code}: {response.text}")
        response.raise_for_status()


def main() -> int:
    base_id = os.getenv("AIRTABLE_BASE_ID")
    table_id = os.getenv("AIRTABLE_TABLE_ID")
    token = os.getenv("AIRTABLE_SCHEMA_TOKEN") or os.getenv("AIRTABLE_API_KEY")
    if not base_id or not table_id or not token:
        print(
            "Set AIRTABLE_BASE_ID, AIRTABLE_TABLE_ID, and AIRTABLE_SCHEMA_TOKEN (or AIRTABLE_API_KEY) in .env"
        )
        return 1

    data = get_tables(base_id, token)
    tables = data.get("tables", [])
    table = None
    for t in tables:
        if t.get("id") == table_id:
            table = t
            break
    if not table:
        print(f"Table {table_id} not found in base {base_id}")
        return 1

    fields = table.get("fields", [])
    name_to_field = {f.get("name"): f for f in fields if f.get("name")}

    updated = 0
    for name in FIELDS_TO_LONGTEXT:
        field = name_to_field.get(name)
        if not field:
            print(f"  Skip {name!r}: not found in table")
            continue
        field_id = field.get("id")
        current_type = field.get("type", "")
        if current_type == "multilineText":
            print(f"  Skip {name!r}: already Long text")
            continue
        if current_type != "multipleSelects":
            print(f"  Skip {name!r}: type is {current_type!r}, not multipleSelects")
            continue
        if not field_id:
            print(f"  Skip {name!r}: no field id in schema")
            continue
        print(f"  Updating {name!r} ({field_id}) to multilineText...")
        try:
            update_field(base_id, table_id, field_id, token, field_name=name)
            updated += 1
            print("    Done.")
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 422:
                print(
                    "    Airtable API does not allow changing this field type. "
                    "Change it manually: Field type → Long text."
                )
            else:
                raise

    if updated == 0:
        print("\nNo fields were updated by the API.")
        print("Change these 5 columns manually in Airtable to Long text:")
        for name in FIELDS_TO_LONGTEXT:
            print(f"  • {name}")
    else:
        print(f"\nUpdated {updated} field(s) to Long text.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
