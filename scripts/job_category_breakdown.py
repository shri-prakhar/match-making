#!/usr/bin/env python3
"""Produce a full breakdown: jobs and human picks that failed job_category, with job category vs candidate desired_job_categories.

Reads a JSON report from find_bad_ai_jobs_and_diagnose_human_picks.py (--output), fetches
desired_job_categories from normalized_candidates for each candidate that failed with reason
job_category, and prints/writes a per-job, per-candidate breakdown.

Usage:
    poetry run with-remote-db python scripts/job_category_breakdown.py
    poetry run with-remote-db python scripts/job_category_breakdown.py bad_ai_jobs_report_nontech.json --output docs/job-category-breakdown.md
    On server: poetry run python scripts/job_category_breakdown.py --local
"""

import argparse
import json
import os
import sys

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from talent_matching.script_env import apply_local_db


def get_connection():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", 5432)),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"],
    )


def load_candidates_desired_categories(conn, airtable_record_ids: list[str]) -> dict[str, dict]:
    """Return map: airtable_record_id -> { full_name, desired_job_categories }."""
    if not airtable_record_ids:
        return {}
    cur = conn.cursor(cursor_factory=RealDictCursor)
    placeholders = ",".join(["%s"] * len(airtable_record_ids))
    cur.execute(
        f"""SELECT airtable_record_id, full_name, desired_job_categories
            FROM normalized_candidates
            WHERE airtable_record_id IN ({placeholders})""",
        airtable_record_ids,
    )
    rows = cur.fetchall()
    cur.close()
    out = {}
    for r in rows:
        aid = r.get("airtable_record_id")
        if aid:
            out[aid] = {
                "full_name": r.get("full_name") or "—",
                "desired_job_categories": r.get("desired_job_categories") or [],
            }
    return out


def run(report_path: str, output_path: str | None) -> str:
    with open(report_path) as f:
        report = json.load(f)

    jobs = report.get("jobs", [])
    # Collect all candidate IDs that failed due to job_category
    job_category_candidate_ids: list[str] = []
    job_entries: list[dict] = []
    for j in jobs:
        job_category = (j.get("job_category") or "").strip() or "—"
        job_title = (j.get("job_title") or "—").strip()
        company = (j.get("company_name") or j.get("company") or "—").strip()
        job_at_id = j.get("job_airtable_record_id") or ""
        missing = j.get("missing_candidates", [])
        job_cat_candidates = [
            m for m in missing
            if (m.get("exclusion_reason") == "job_category" and m.get("candidate_airtable_record_id"))
        ]
        for c in job_cat_candidates:
            job_category_candidate_ids.append(c["candidate_airtable_record_id"])
        job_entries.append({
            "job_airtable_record_id": job_at_id,
            "job_title": job_title,
            "company": company,
            "job_category": job_category,
            "job_category_candidates": job_cat_candidates,
        })

    conn = get_connection()
    # desired_job_categories can be stored as list or array; ensure we have unique ids for query
    unique_ids = list(dict.fromkeys(job_category_candidate_ids))
    cand_info = load_candidates_desired_categories(conn, unique_ids)
    conn.close()

    lines: list[str] = []
    lines.append("# Job category filter breakdown: jobs and human picks")
    lines.append("")
    lines.append("For each job (with its **job category**), every human-selected candidate who was excluded because of the job_category filter is listed with their **desired job categories** from the candidate profile. The pipeline only scores candidates whose `desired_job_categories` contain the job’s `job_category` (case-insensitive).")
    lines.append("")

    for entry in job_entries:
        job_at_id = entry["job_airtable_record_id"]
        job_title = entry["job_title"]
        company = entry["company"]
        job_category = entry["job_category"]
        candidates = entry["job_category_candidates"]
        if not candidates:
            continue
        lines.append(f"## {job_title} @ {company}")
        lines.append("")
        lines.append(f"- **Job category:** `{job_category}`")
        lines.append(f"- **Job ATS record id:** `{job_at_id}`")
        lines.append(f"- **Human picks excluded by job_category:** {len(candidates)}")
        lines.append("")
        lines.append("| Candidate (name) | Candidate Airtable ID | Desired job categories | Match? |")
        lines.append("|------------------|------------------------|------------------------|--------|")

        for c in candidates:
            cand_at_id = c["candidate_airtable_record_id"]
            info = cand_info.get(cand_at_id, {})
            name = (info.get("full_name") or "—")[:40]
            desired = info.get("desired_job_categories") or []
            if isinstance(desired, list):
                desired_str = ", ".join(str(x) for x in desired) if desired else "(none)"
            else:
                desired_str = str(desired)
            desired_str = desired_str[:80] + ("…" if len(desired_str) > 80 else "")
            job_cat_lower = job_category.lower()
            desired_lower = [str(x).lower() for x in desired] if desired else []
            match = "Yes" if job_cat_lower in desired_lower or any(job_cat_lower in d for d in desired_lower) else "No"
            lines.append(f"| {name} | `{cand_at_id}` | {desired_str} | {match} |")

        lines.append("")

    body = "\n".join(lines)
    if output_path:
        with open(output_path, "w") as f:
            f.write(body)
    return body


def main() -> int:
    apply_local_db()
    parser = argparse.ArgumentParser(
        description="Break down job category filter: job category vs candidate desired categories"
    )
    parser.add_argument(
        "--local",
        action="store_true",
        help="Use local Postgres (when running on the server).",
    )
    parser.add_argument(
        "report",
        nargs="?",
        default="bad_ai_jobs_report_nontech.json",
        help="Path to JSON report from find_bad_ai_jobs_and_diagnose_human_picks.py (default: bad_ai_jobs_report_nontech.json)",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        default=None,
        help="Write markdown to this path (default: print to stdout)",
    )
    args = parser.parse_args()
    if not os.path.isfile(args.report):
        print(f"Report file not found: {args.report}", file=sys.stderr)
        return 1
    out = run(args.report, args.output)
    if not args.output:
        print(out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
