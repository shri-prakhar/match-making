#!/usr/bin/env python
"""Test the full candidate pipeline with a sample of 20 candidates."""

import os
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(project_root / ".env")

from talent_matching.resources.airtable import AirtableResource  # noqa: E402


def main():
    sample_size = 20

    # Create the resource
    airtable = AirtableResource(
        base_id=os.getenv("AIRTABLE_BASE_ID", ""),
        table_id=os.getenv("AIRTABLE_TABLE_ID", ""),
        api_key=os.getenv("AIRTABLE_API_KEY", ""),
    )

    print("=" * 80)
    print(f"TESTING PIPELINE WITH {sample_size} CANDIDATES")
    print("=" * 80)

    # Fetch all records and take first 20
    print("\nFetching candidates from Airtable...")
    all_records = airtable.fetch_all_records()
    sample_records = all_records[:sample_size]

    print(f"✓ Fetched {len(all_records)} total, testing with {len(sample_records)}")
    print("=" * 80)

    # Process each candidate through the pipeline stages
    for i, record in enumerate(sample_records, 1):
        print(f"\n{'─' * 80}")
        print(f"CANDIDATE {i}/{sample_size}: {record.get('full_name', 'Unknown')}")
        print(f"{'─' * 80}")

        # Stage 1: airtable_candidates (already have this data)
        print("\n[1] airtable_candidates ✓")
        print(f"    Record ID:      {record.get('airtable_record_id')}")
        print(f"    Data Version:   {record.get('_data_version')}")

        # Stage 2: raw_candidates (would be stored in PostgreSQL)
        print("\n[2] raw_candidates → PostgreSQL")
        print(f"    Full Name:      {record.get('full_name')}")
        print(f"    Location:       {record.get('location_raw')}")

        skills = record.get("skills_raw")
        if skills:
            if isinstance(skills, list):
                skills_str = ", ".join(skills[:5])
                if len(skills) > 5:
                    skills_str += f" (+{len(skills) - 5} more)"
            else:
                skills_str = str(skills)[:60]
            print(f"    Skills:         {skills_str}")
        else:
            print("    Skills:         None")

        print(f"    CV URL:         {'Yes' if record.get('cv_url') else 'No'}")
        print(f"    LinkedIn:       {record.get('linkedin_url') or 'None'}")
        print(f"    GitHub:         {record.get('github_url') or 'None'}")
        print(f"    X/Twitter:      {record.get('x_profile_url') or 'None'}")

        # Professional summary (truncated)
        summary = record.get("professional_summary")
        if summary:
            summary_preview = summary[:100].replace("\n", " ")
            print(f"    Summary:        {summary_preview}...")
        else:
            print("    Summary:        None")

        # Stage 3: normalized_candidates (would use LLM)
        print("\n[3] normalized_candidates → LLM (mock)")
        print("    Status:         Would extract structured profile via LLM")

        # Stage 4: candidate_vectors (would generate embeddings)
        print("\n[4] candidate_vectors → pgvector (mock)")
        print("    Status:         Would generate 3 vectors (experience, domain, personality)")

    # Summary
    print("\n" + "=" * 80)
    print("PIPELINE TEST SUMMARY")
    print("=" * 80)

    # Stats
    has_cv = sum(1 for r in sample_records if r.get("cv_url"))
    has_linkedin = sum(1 for r in sample_records if r.get("linkedin_url"))
    has_github = sum(1 for r in sample_records if r.get("github_url"))
    has_skills = sum(1 for r in sample_records if r.get("skills_raw"))
    has_summary = sum(1 for r in sample_records if r.get("professional_summary"))

    print(f"\nData Completeness ({sample_size} candidates):")
    print(f"  ├── CV URL:         {has_cv:3d}/{sample_size} ({100 * has_cv / sample_size:.0f}%)")
    print(
        f"  ├── Skills:         {has_skills:3d}/{sample_size} ({100 * has_skills / sample_size:.0f}%)"
    )
    print(
        f"  ├── Summary:        {has_summary:3d}/{sample_size} ({100 * has_summary / sample_size:.0f}%)"
    )
    print(
        f"  ├── LinkedIn:       {has_linkedin:3d}/{sample_size} ({100 * has_linkedin / sample_size:.0f}%)"
    )
    print(
        f"  └── GitHub:         {has_github:3d}/{sample_size} ({100 * has_github / sample_size:.0f}%)"
    )

    print("\nPipeline Stages:")
    print("  [1] airtable_candidates  ✓ Fetched from API")
    print("  [2] raw_candidates       ✓ Mapped to model fields")
    print("  [3] normalized_candidates  ○ LLM normalization (stub)")
    print("  [4] candidate_vectors      ○ Embedding generation (stub)")

    print("\n" + "=" * 80)
    print("TEST COMPLETE ✓")
    print("=" * 80)


if __name__ == "__main__":
    main()
