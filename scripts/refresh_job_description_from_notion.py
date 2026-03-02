#!/usr/bin/env python3
"""Refresh raw_jobs.job_description from a Notion page when it was previously missing.

Use when a job was ingested before the public-Notion fallback was deployed, so
job_description is "(No content from Notion)". Fetches content via the public
notion.site API and updates the row. Does not re-run normalization; run the
matchmaking pipeline for that partition to recompute normalized_jobs and matches.

Usage:
    python scripts/refresh_job_description_from_notion.py <airtable_record_id>
    python scripts/refresh_job_description_from_notion.py recIqBsuF33YrIrMX

Requires: .env with POSTGRES_* (and NOTION_API_KEY not needed for public pages).
"""

import os
import sys

from dotenv import load_dotenv

load_dotenv()

# Allow running from repo root; dagster code uses talent_matching.*
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text  # noqa: E402

from talent_matching.db import get_engine  # noqa: E402
from talent_matching.resources.notion import (  # noqa: E402
    _fetch_public_page_content,
    extract_notion_page_id,
)


def main() -> None:
    if len(sys.argv) != 2:
        print("Usage: python scripts/refresh_job_description_from_notion.py <airtable_record_id>")
        sys.exit(1)

    record_id = sys.argv[1].strip()
    if not record_id.startswith("rec"):
        print("Expected Airtable record ID (e.g. recIqBsuF33YrIrMX)")
        sys.exit(1)

    engine = get_engine()
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT id, source_url, job_title, LENGTH(job_description) AS len "
                "FROM raw_jobs WHERE airtable_record_id = :rid"
            ),
            {"rid": record_id},
        ).fetchone()

        if not row:
            print(f"No raw_job found for airtable_record_id={record_id}")
            sys.exit(1)

        raw_id, source_url, job_title, desc_len = row
        print(f"Job: {job_title} (raw_jobs.id={raw_id})")
        print(f"Source URL: {source_url}")
        print(f"Current job_description length: {desc_len}")

        if not source_url or "notion" not in source_url.lower():
            print("No Notion URL on this job; nothing to refresh.")
            sys.exit(0)

        page_id = extract_notion_page_id(source_url)
        if not page_id:
            print("Could not parse Notion page ID from URL.")
            sys.exit(1)

        content = _fetch_public_page_content(source_url, page_id)
        if not content or not content.strip():
            print("Failed to fetch content from Notion (public API returned empty).")
            sys.exit(1)

        print(f"Fetched {len(content)} chars from Notion.")
        conn.execute(
            text("UPDATE raw_jobs SET job_description = :desc WHERE id = :id"),
            {"desc": content, "id": raw_id},
        )
        conn.commit()

    print(
        f"Updated raw_jobs for {record_id}. Re-run matchmaking for partition {record_id} to recompute normalized_jobs and matches."
    )


if __name__ == "__main__":
    main()
