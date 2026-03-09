#!/usr/bin/env python3
"""List Airtable job records that have no name and appear 'dead' (no meaningful content).

Report-only: no deletions. Use this to review and confirm before any destructive operation.

Usage:
    set -a && source .env && set +a && poetry run python scripts/list_dead_airtable_job_records.py

Requires: AIRTABLE_BASE_ID, AIRTABLE_ATS_TABLE_ID, AIRTABLE_API_KEY (data token).
"""

from __future__ import annotations

import os
import sys

from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import httpx

from scripts.airtable_create_columns_lib import fetch_all_records

load_dotenv()

# Minimum length to consider description "meaningful" (aligns with pipeline guards)
MIN_DESC_LEN = 50


def _data_token() -> str | None:
    """Token for data (records) API. Use API_KEY; schema token is for schema ops only."""
    return os.getenv("AIRTABLE_API_KEY") or os.getenv("AIRTABLE_SCHEMA_TOKEN")


def _text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        if not value:
            return ""
        if isinstance(value[0], dict) and "url" in value[0]:
            return (value[0].get("url") or "").strip()
        return " ".join(str(v) for v in value).strip()
    return str(value).strip()


def _description_text(fields: dict, text_keys: list[str]) -> str:
    out = ""
    for key in text_keys:
        v = fields.get(key)
        if v is None:
            continue
        t = _text(v)
        if len(t) > len(out):
            out = t
    return out


def _has_link(fields: dict, link_key: str) -> bool:
    v = fields.get(link_key)
    if v is None:
        return False
    if isinstance(v, str) and v.strip():
        return True
    if isinstance(v, list) and v and isinstance(v[0], dict):
        return bool((v[0].get("url") or "").strip())
    return False


# ─── ATS table ───────────────────────────────────────────────────────────────
ATS_NAME_FIELD = "Open Position (Job Title)"
ATS_DESC_TEXT_FIELD = "Job Description Text"
ATS_LINK_FIELD = "Job Description Link"


def _ats_record_dead(record: dict) -> tuple[bool, str]:
    fields = record.get("fields", {})
    name = _text(fields.get(ATS_NAME_FIELD) or "")
    desc = _text(fields.get(ATS_DESC_TEXT_FIELD) or "")
    has_link = _has_link(fields, ATS_LINK_FIELD)

    no_name = len(name) < 2
    no_desc = len(desc) < MIN_DESC_LEN and not has_link
    dead = no_name and no_desc

    summary_parts = []
    if no_name:
        summary_parts.append("no title")
    else:
        summary_parts.append(f"title={name[:40]!r}")
    if no_desc:
        summary_parts.append("no/short description")
    else:
        if has_link:
            summary_parts.append("has link")
        if len(desc) >= MIN_DESC_LEN:
            summary_parts.append(f"desc_len={len(desc)}")
    return dead, "; ".join(summary_parts)


def _report_table(
    label: str,
    table_id: str,
    records: list[dict],
    is_dead_fn,
) -> list[dict]:
    dead_list: list[dict] = []
    for rec in records:
        rid = rec.get("id", "")
        dead, summary = is_dead_fn(rec)
        if dead:
            dead_list.append({"id": rid, "createdTime": rec.get("createdTime"), "summary": summary})
    dead_list.sort(key=lambda x: (x.get("createdTime") or "", x["id"]))

    print(f"\n{label}")
    print("-" * max(40, len(label)))
    print(f"Table ID: {table_id}")
    print(f"Total records: {len(records)}")
    print(f"Dead (no name + no/short description): {len(dead_list)}")
    if dead_list:
        print("\nDead record IDs and summary:")
        for r in dead_list:
            print(f"  {r['id']}  createdTime={r.get('createdTime', '')}  ({r['summary']})")
    return dead_list


def main() -> int:
    base_id = os.getenv("AIRTABLE_BASE_ID")
    ats_table_id = os.getenv("AIRTABLE_ATS_TABLE_ID", "tblrbhITEIBOxwcQV")
    token = _data_token()
    if not base_id or not ats_table_id or not token:
        print("Set AIRTABLE_BASE_ID, AIRTABLE_ATS_TABLE_ID, and AIRTABLE_API_KEY in .env")
        return 1

    print("Fetching all records from ATS table (report-only, no deletions)...")
    try:
        ats_records = fetch_all_records(base_id, ats_table_id, token)
    except httpx.HTTPStatusError as e:
        print(f"  Error: ATS table returned {e.response.status_code}. {e}")
        return 1

    _report_table(
        "ATS table",
        ats_table_id,
        ats_records,
        _ats_record_dead,
    )

    print("\n---")
    print("No destructive action was taken. To remove these from Airtable, delete them manually")
    print("or run a separate script after you confirm.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
