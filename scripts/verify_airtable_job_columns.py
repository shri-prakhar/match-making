#!/usr/bin/env python3
"""Verify Airtable job-related column mappings against live schema.

Usage:
    set -a && source .env && set +a && poetry run python scripts/verify_airtable_job_columns.py

Requires:
    AIRTABLE_BASE_ID
    AIRTABLE_JOBS_TABLE_ID
    AIRTABLE_ATS_TABLE_ID
    AIRTABLE_API_KEY or AIRTABLE_SCHEMA_TOKEN
"""

import os
import sys

from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.airtable_create_columns_lib import get_tables_response
from talent_matching.resources.airtable import AirtableATSResource
from talent_matching.utils.airtable_mapper import AIRTABLE_JOBS_COLUMN_MAPPING

load_dotenv()

ATS_SENSOR_JOB_FIELDS = {
    "Company",
    "Desired Job Category",
    "Job Description Link",
    "Job Description Text",
    "Job Status",
    "Level",
    "Nice-to-have",
    "Non Negotiables",
    "Open Position (Job Title)",
    "Preferred Location",
    "Projected Salary",
    "Work Set Up Preference",
}

RELEVANT_JOB_KEYWORDS = (
    "job",
    "position",
    "company",
    "location",
    "salary",
    "negotiable",
    "nice",
    "level",
    "work set up",
    "description",
    "details",
    "role",
)


def _get_token() -> str | None:
    return os.getenv("AIRTABLE_SCHEMA_TOKEN") or os.getenv("AIRTABLE_API_KEY")


def _table_by_id(tables: list[dict], table_id: str) -> dict | None:
    for table in tables:
        if table.get("id") == table_id:
            return table
    return None


def _field_names(table: dict) -> set[str]:
    return {field.get("name") for field in table.get("fields", []) if field.get("name")}


def _relevant_unmapped_fields(field_names: set[str], expected_fields: set[str]) -> list[str]:
    candidates = []
    for name in field_names - expected_fields:
        lowered = name.lower()
        if any(keyword in lowered for keyword in RELEVANT_JOB_KEYWORDS):
            candidates.append(name)
    return sorted(candidates)


def _print_table_report(
    label: str,
    table: dict,
    expected_fields: set[str],
) -> int:
    field_names = _field_names(table)
    missing = sorted(expected_fields - field_names)
    relevant_unmapped = _relevant_unmapped_fields(field_names, expected_fields)

    print(f"\n{label}")
    print("-" * len(label))
    print(f"Table: {table.get('name')} ({table.get('id')})")
    print(f"Fields in schema: {len(field_names)}")
    print(f"Expected mapped fields: {len(expected_fields)}")

    if missing:
        print("\nMissing expected fields:")
        for name in missing:
            print(f"  - {name}")
    else:
        print("\nMissing expected fields: none")

    if relevant_unmapped:
        print("\nRelevant schema fields not currently mapped / requested:")
        for name in relevant_unmapped:
            print(f"  - {name}")
    else:
        print("\nRelevant schema fields not currently mapped / requested: none")

    return 1 if missing else 0


def main() -> int:
    base_id = os.getenv("AIRTABLE_BASE_ID")
    jobs_table_id = os.getenv("AIRTABLE_JOBS_TABLE_ID")
    ats_table_id = os.getenv("AIRTABLE_ATS_TABLE_ID", "tblrbhITEIBOxwcQV")
    token = _get_token()

    if not base_id or not jobs_table_id or not ats_table_id or not token:
        print(
            "Set AIRTABLE_BASE_ID, AIRTABLE_JOBS_TABLE_ID, AIRTABLE_ATS_TABLE_ID, "
            "and AIRTABLE_API_KEY (or AIRTABLE_SCHEMA_TOKEN) in .env"
        )
        return 1

    data = get_tables_response(base_id, token)
    tables = data.get("tables", [])

    jobs_table = _table_by_id(tables, jobs_table_id)
    ats_table = _table_by_id(tables, ats_table_id)

    if jobs_table is None:
        print(f"Jobs table not found in schema: {jobs_table_id}")
        return 1
    if ats_table is None:
        print(f"ATS table not found in schema: {ats_table_id}")
        return 1

    status = 0
    status |= _print_table_report(
        "Jobs Table Mapping Check",
        jobs_table,
        set(AIRTABLE_JOBS_COLUMN_MAPPING.keys()),
    )
    status |= _print_table_report(
        "ATS Table Mapping Check",
        ats_table,
        set(AirtableATSResource.ATS_JOB_FIELDS) | ATS_SENSOR_JOB_FIELDS,
    )

    return status


if __name__ == "__main__":
    raise SystemExit(main())
