#!/usr/bin/env python3
"""Explore ATS table: count records and candidate links per candidate-linked column.

Usage:
    set -a && source .env && set +a && poetry run python scripts/explore_ats_candidate_columns.py

Requires: AIRTABLE_BASE_ID, AIRTABLE_ATS_TABLE_ID (or default), AIRTABLE_API_KEY in .env
"""

import os
import sys

import httpx
from dotenv import load_dotenv

load_dotenv()

# Candidate-linked columns to analyze (linked record fields pointing to Talent)
CANDIDATE_COLUMNS = [
    "CLIENT INTRODUCTION",
    "Shortlisted Talent",
    "Potential Talent Fit",
    "Potential Talent Fit Nick",
    "Potential Talent Fit NOE",
    "Hired",
    "AI PROPOSTED CANDIDATES",
    "Recruiter AI Result Rejection",
]


def extract_linked_ids(fields: dict, column: str) -> list[str]:
    """Extract linked record IDs from an Airtable linked record field."""
    value = fields.get(column, [])
    if isinstance(value, list):
        return [v for v in value if isinstance(v, str) and v.startswith("rec")]
    return []


def main() -> None:
    base_id = os.environ.get("AIRTABLE_BASE_ID")
    table_id = os.environ.get("AIRTABLE_ATS_TABLE_ID", "tblrbhITEIBOxwcQV")
    api_key = os.environ.get("AIRTABLE_API_KEY") or os.environ.get("AIRTABLE_SCHEMA_TOKEN")
    if not base_id or not api_key:
        print("Set AIRTABLE_BASE_ID and AIRTABLE_API_KEY (or AIRTABLE_SCHEMA_TOKEN) in .env")
        sys.exit(1)

    url = f"https://api.airtable.com/v0/{base_id}/{table_id}"
    headers = {"Authorization": f"Bearer {api_key}"}

    print(f"Fetching ATS records from {base_id}/{table_id}...")
    print(f"Columns to analyze: {CANDIDATE_COLUMNS}\n")

    records: list[dict] = []
    offset: str | None = None

    with httpx.Client(timeout=60.0) as client:
        while True:
            params: list[tuple[str, str]] = []
            for col in CANDIDATE_COLUMNS + ["Open Position (Job Title)", "Job Status", "Company"]:
                params.append(("fields[]", col))
            if offset:
                params.append(("offset", offset))
            response = client.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            page = data.get("records", [])
            records.extend(page)
            offset = data.get("offset")
            if not offset:
                break

    total_records = len(records)
    print(f"Total ATS records: {total_records}")

    # List all field names in first record (to catch typos or alternate names)
    if records:
        all_field_names = set()
        for r in records:
            all_field_names.update(r.get("fields", {}).keys())
        linked_like = sorted(
            [
                f
                for f in all_field_names
                if "talent" in f.lower()
                or "hired" in f.lower()
                or "introduction" in f.lower()
                or "fit" in f.lower()
                or "rejection" in f.lower()
                or "candidate" in f.lower()
            ]
        )
        print(f"\nAll candidate-related field names in ATS: {linked_like}\n")

    # Per-column stats
    print("=" * 80)
    print("CANDIDATE-LINKED COLUMNS: RECORDS WITH DATA / TOTAL CANDIDATE LINKS")
    print("=" * 80)

    for col in CANDIDATE_COLUMNS:
        records_with_data = 0
        total_links = 0
        sample_record_ids: list[str] = []
        sample_counts: list[int] = []
        for rec in records:
            ids = extract_linked_ids(rec.get("fields", {}), col)
            if ids:
                records_with_data += 1
                total_links += len(ids)
                if len(sample_record_ids) < 3:
                    sample_record_ids.append(rec.get("id", ""))
                    sample_counts.append(len(ids))
        pct = 100 * records_with_data / total_records if total_records else 0
        print(f"\n  {col}")
        print(f"    Records with ≥1 candidate: {records_with_data}/{total_records} ({pct:.1f}%)")
        print(f"    Total candidate links:    {total_links}")
        if sample_record_ids:
            print(
                f"    Sample (rec, count):      {list(zip(sample_record_ids[:3], sample_counts))}"
            )

    # Unique candidates per column (deduplicated across records)
    print("\n" + "=" * 80)
    print("UNIQUE CANDIDATES PER COLUMN (across all ATS records)")
    print("=" * 80)
    for col in CANDIDATE_COLUMNS:
        all_ids: set[str] = set()
        for rec in records:
            all_ids.update(extract_linked_ids(rec.get("fields", {}), col))
        print(f"  {col}: {len(all_ids)} unique candidates")

    # Overlap: candidates in multiple columns
    print("\n" + "=" * 80)
    print("COLUMNS WITH MOST DATA (recommended for ground-truth)")
    print("=" * 80)
    col_stats = []
    for col in CANDIDATE_COLUMNS:
        records_with = sum(1 for r in records if extract_linked_ids(r.get("fields", {}), col))
        total_links = sum(len(extract_linked_ids(r.get("fields", {}), col)) for r in records)
        unique = len(set().union(*(extract_linked_ids(r.get("fields", {}), col) for r in records)))
        col_stats.append((col, records_with, total_links, unique))
    col_stats.sort(key=lambda x: -x[3])  # by unique count
    for col, recs, links, uniq in col_stats:
        print(f"  {uniq:4} unique | {recs:3} records | {links:4} links  {col}")

    print()


if __name__ == "__main__":
    main()
