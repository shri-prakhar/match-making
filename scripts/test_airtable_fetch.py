#!/usr/bin/env python
"""Test script to verify Airtable integration works end-to-end."""

import os
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv  # noqa: E402

# Load environment variables
load_dotenv(project_root / ".env")

from talent_matching.resources.airtable import AirtableResource  # noqa: E402


def main():
    # Create the resource
    airtable = AirtableResource(
        base_id=os.getenv("AIRTABLE_BASE_ID", ""),
        table_id=os.getenv("AIRTABLE_TABLE_ID", ""),
        api_key=os.getenv("AIRTABLE_API_KEY", ""),
    )

    print("=" * 60)
    print("AIRTABLE CONNECTION TEST")
    print("=" * 60)
    print(f"Base ID:  {airtable.base_id}")
    print(f"Table ID: {airtable.table_id}")
    print(f"API Key:  {airtable.api_key[:15]}..." if airtable.api_key else "API Key: NOT SET")
    print()

    if not airtable.api_key:
        print("ERROR: AIRTABLE_API_KEY not set in .env")
        sys.exit(1)

    # Fetch all records
    print("Fetching records from Airtable...")
    records = airtable.fetch_all_records()

    print(f"\n✓ Successfully fetched {len(records)} candidates!")
    print("=" * 60)

    # Show first 3 candidates as a sample
    for i, record in enumerate(records[:3]):
        print(f"\n--- Candidate {i + 1} ---")
        print(f"  Record ID:    {record.get('airtable_record_id')}")
        print(f"  Full Name:    {record.get('full_name')}")
        print(f"  Location:     {record.get('location_raw')}")

        skills = record.get("skills_raw", "")
        if skills:
            print(
                f"  Skills:       {skills[:60]}..."
                if len(skills) > 60
                else f"  Skills:       {skills}"
            )
        else:
            print("  Skills:       None")

        cv_url = record.get("cv_url", "")
        if cv_url:
            print(
                f"  CV URL:       {cv_url[:60]}..."
                if len(cv_url) > 60
                else f"  CV URL:       {cv_url}"
            )
        else:
            print("  CV URL:       None")

        print(f"  LinkedIn:     {record.get('linkedin_url')}")
        print(f"  GitHub:       {record.get('github_url')}")
        print(f"  Data Version: {record.get('_data_version')}")

    if len(records) > 3:
        print(f"\n... and {len(records) - 3} more candidates")

    print("\n" + "=" * 60)
    print("TEST PASSED ✓")
    print("=" * 60)


if __name__ == "__main__":
    main()
