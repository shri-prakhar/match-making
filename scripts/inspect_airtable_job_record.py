#!/usr/bin/env python
"""Print Job Description Text and Job Description Link for an ATS record from Airtable.

When a Job Description Link is set, use --notion to fetch that page via the Notion API
and print the returned text (and its length) so you can verify API return vs link length.
"""

import os
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(project_root / ".env")

from talent_matching.resources.airtable import AirtableATSResource  # noqa: E402
from talent_matching.resources.notion import NotionResource  # noqa: E402


def main() -> None:
    argv = sys.argv[1:]
    record_id = argv[0] if argv else "reczqy86gsYH3AuEA"
    fetch_notion = "--notion" in argv
    if fetch_notion:
        argv = [a for a in argv if a != "--notion"]
        record_id = argv[0] if argv else record_id

    base_id = os.getenv("AIRTABLE_BASE_ID")
    table_id = os.getenv("AIRTABLE_ATS_TABLE_ID", "tblrbhITEIBOxwcQV")
    api_key = os.getenv("AIRTABLE_API_KEY")
    if not base_id or not api_key:
        print("Set AIRTABLE_BASE_ID and AIRTABLE_API_KEY in .env", file=sys.stderr)
        sys.exit(1)

    resource = AirtableATSResource(base_id=base_id, table_id=table_id, api_key=api_key)
    record = resource.fetch_record_by_id(record_id)
    fields = record.get("fields", {})

    desc_text = fields.get("Job Description Text") or ""
    desc_link = fields.get("Job Description Link") or ""

    print(f"Record ID: {record_id}")
    print(f"  Open Position (Job Title): {fields.get('Open Position (Job Title)', '')}")
    print(f"  Job Description Text (Airtable): {repr(desc_text)} (len={len(desc_text)})")
    print(f"  Job Description Link: {repr(desc_link)}")
    if not desc_text.strip() and not desc_link.strip():
        print("\n  -> Confirmed: neither job description nor job description link provided.")
    elif not desc_text.strip() and desc_link.strip():
        print("\n  -> Job Description Text is empty; link is set. Pipeline fetches Notion.")

    if fetch_notion and desc_link.strip():
        notion_api_key = os.getenv("NOTION_API_KEY", "")
        notion = NotionResource(api_key=notion_api_key)
        print("\n  Notion API return (text from API, not link length):")
        returned = notion.fetch_page_content(desc_link.strip())
        if returned is None:
            fallback = "(No content from Notion)"
            print("    -> None (fetch failed or empty page)")
            print(f"    -> Pipeline uses fallback: {repr(fallback)} (len={len(fallback)})")
        else:
            print(f"    -> len={len(returned)} chars")
            print(f"    -> text: {repr(returned[:500])}{'...' if len(returned) > 500 else ''}")


if __name__ == "__main__":
    main()
