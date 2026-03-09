#!/usr/bin/env python3
"""Batch human vs system analysis for jobs with recruiter selections.

Fetches ATS records that have human-selected candidates (CLIENT INTRODUCTION,
Shortlisted Talent, Potential Talent Fit, Hired), runs analyze_human_vs_system
for each, and outputs aggregated summary stats for trend analysis.

Usage:
    poetry run with-remote-db python scripts/batch_analyze_human_vs_system.py
    poetry run with-remote-db python scripts/batch_analyze_human_vs_system.py --limit 20
    poetry run with-remote-db python scripts/batch_analyze_human_vs_system.py --output summary.json

Requires:
    - AIRTABLE_BASE_ID, AIRTABLE_ATS_TABLE_ID, AIRTABLE_API_KEY in .env
    - Matches already computed for jobs (run matchmaking pipeline first)
"""

import argparse
import importlib.util
import json
import os
import sys

import httpx
from dotenv import load_dotenv

load_dotenv()

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# Import from same scripts dir (scripts is not a package)
_analyze_path = os.path.join(os.path.dirname(__file__), "analyze_human_vs_system.py")
_spec = importlib.util.spec_from_file_location("analyze_human_vs_system", _analyze_path)
_analyze_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_analyze_mod)
HUMAN_SELECTION_COLUMNS = _analyze_mod.HUMAN_SELECTION_COLUMNS
analyze_one = _analyze_mod.analyze_one
extract_linked_ids = _analyze_mod.extract_linked_ids


def fetch_all_ats_records(limit: int | None = None) -> list[dict]:
    """Fetch ATS records with pagination. Optionally limit total count."""
    base_id = os.environ["AIRTABLE_BASE_ID"]
    table_id = os.environ.get("AIRTABLE_ATS_TABLE_ID", "tblrbhITEIBOxwcQV")
    token = os.environ["AIRTABLE_API_KEY"]
    url = f"https://api.airtable.com/v0/{base_id}/{table_id}"
    headers = {"Authorization": f"Bearer {token}"}

    records: list[dict] = []
    offset: str | None = None

    with httpx.Client(timeout=60.0) as client:
        while True:
            params: list[tuple[str, str]] = []
            if offset:
                params.append(("offset", offset))
            response = client.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            page = data.get("records", [])
            records.extend(page)
            if limit and len(records) >= limit:
                records = records[:limit]
                break
            offset = data.get("offset")
            if not offset:
                break
    return records


def has_human_selections(record: dict) -> bool:
    """True if any human selection column has linked records."""
    fields = record.get("fields", {})
    for col in HUMAN_SELECTION_COLUMNS:
        ids = extract_linked_ids(fields, col)
        if ids:
            return True
    return False


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Batch human vs system analysis for jobs with recruiter selections"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max ATS records to fetch (default: all)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Write summary JSON to file (default: print to stdout)",
    )
    args = parser.parse_args()

    print("Fetching ATS records...")
    records = fetch_all_ats_records(limit=args.limit)
    print(f"  Fetched {len(records)} ATS records")

    with_human = [r for r in records if has_human_selections(r)]
    print(f"  {len(with_human)} have human selections")

    if not with_human:
        print("No jobs with human selections. Exiting.")
        sys.exit(0)

    results: list[dict] = []
    for i, rec in enumerate(with_human):
        record_id = rec.get("id", "")
        job_title = rec.get("fields", {}).get("Open Position (Job Title)", "Unknown")
        print(f"  [{i + 1}/{len(with_human)}] {record_id} ({job_title})...", end=" ", flush=True)
        stats = analyze_one(record_id, verbose=False)
        if stats:
            results.append(stats)
            print(
                f"ok (found {stats['found_in_system']}/{stats['total_human']}, avg_rank={stats.get('avg_rank', '--')})"
            )
        else:
            print("skip (no human picks or no job in DB)")

    # Aggregate summary
    summary: dict = {
        "jobs_analyzed": len(results),
        "jobs_skipped": len(with_human) - len(results),
        "total_human_selections": sum(r["total_human"] for r in results),
        "total_found_in_system": sum(r["found_in_system"] for r in results),
        "total_not_in_system": sum(r["not_in_system"] for r in results),
        "results": results,
    }
    if results:
        ranks = [r["avg_rank"] for r in results if "avg_rank" in r]
        if ranks:
            summary["avg_rank_across_jobs"] = sum(ranks) / len(ranks)
        top5 = [r.get("in_top5", 0) for r in results if "in_top5" in r]
        top10 = [r.get("in_top10", 0) for r in results if "in_top10" in r]
        top15 = [r.get("in_top15", 0) for r in results if "in_top15" in r]
        human_ranks = [r.get("human_ranks_count", 0) for r in results if "human_ranks_count" in r]
        if human_ranks and sum(human_ranks) > 0:
            summary["aggregate_in_top5"] = sum(top5)
            summary["aggregate_in_top10"] = sum(top10)
            summary["aggregate_in_top15"] = sum(top15)
            summary["aggregate_human_ranks_count"] = sum(human_ranks)

    output = json.dumps(summary, indent=2)
    if args.output:
        with open(args.output, "w") as f:
            f.write(output)
        print(f"\nWrote summary to {args.output}")
    else:
        print("\n" + output)


if __name__ == "__main__":
    main()
