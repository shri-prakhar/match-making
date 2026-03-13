#!/usr/bin/env python3
"""List zero-match jobs from the 30 status-based partitions and diagnose CLIENT INTRODUCTION candidates.

For jobs with 0 matches, fetches human-selected candidates (CLIENT INTRODUCTION, etc.) from Airtable
and reports why each was not found: not in normalized_candidates, filtered by location, filtered by
job category, or passed filters but not in shortlist (not in top 30 or dropped at refinement).

Usage:
  poetry run with-remote-db python scripts/diagnose_zero_match_jobs.py
  poetry run with-local-db python scripts/diagnose_zero_match_jobs.py
  poetry run with-remote-db python scripts/diagnose_zero_match_jobs.py --partitions recA,recB
  On server: poetry run python scripts/diagnose_zero_match_jobs.py --local

Requires: AIRTABLE_BASE_ID, AIRTABLE_API_KEY, POSTGRES_* in .env. For remote DB, tunnel must be up.
"""

import argparse
import os
import sys
from pathlib import Path

import httpx
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.inspect_utils import get_connection  # noqa: E402
from talent_matching.matchmaking.location_filter import (  # noqa: E402
    candidate_matches_country,
    candidate_matches_region,
    candidate_passes_location_or_timezone,
    job_locations_to_countries,
    job_locations_to_regions,
    parse_job_preferred_locations,
)
from talent_matching.script_env import apply_local_db  # noqa: E402
from talent_matching.utils.job_category import norm_cat  # noqa: E402

# Same statuses as launch_ats_matchmaking_backfill_by_status
STATUSES = [
    "Matchmaking Ready",
    "Matchmaking Done",
    "Ongoing Recruiting",
    "Client Introduction",
    "In Interview",
]
ATS_TABLE_ID = "tblrbhITEIBOxwcQV"
HUMAN_SELECTION_COLUMNS = [
    "CLIENT INTRODUCTION",
    "Shortlisted Talent",
    "Potential Talent Fit",
    "Hired",
]


def fetch_ats_record_ids_by_statuses(statuses: list[str]) -> list[str]:
    """Fetch ATS record IDs where Job Status is one of the given values."""
    base_id = os.getenv("AIRTABLE_BASE_ID")
    api_key = os.getenv("AIRTABLE_API_KEY")
    table_id = os.getenv("AIRTABLE_ATS_TABLE_ID", ATS_TABLE_ID)
    if not base_id or not api_key:
        print("Set AIRTABLE_BASE_ID and AIRTABLE_API_KEY in .env", file=sys.stderr)
        sys.exit(1)
    conditions = "".join(f'{{Job Status}} = "{s}", ' for s in statuses)
    formula = "OR(" + conditions.rstrip(", ") + ")"
    url = f"https://api.airtable.com/v0/{base_id}/{table_id}"
    headers = {"Authorization": f"Bearer {api_key}"}
    record_ids: list[str] = []
    offset: str | None = None
    with httpx.Client(timeout=60.0) as client:
        while True:
            params: list[tuple[str, str]] = [("filterByFormula", formula)]
            if offset:
                params.append(("offset", offset))
            response = client.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            for rec in data.get("records", []):
                record_ids.append(rec["id"])
            offset = data.get("offset")
            if not offset:
                break
    return record_ids


def fetch_ats_record(record_id: str) -> dict:
    """Fetch a single ATS record with all fields."""
    base_id = os.environ["AIRTABLE_BASE_ID"]
    table_id = os.environ.get("AIRTABLE_ATS_TABLE_ID", ATS_TABLE_ID)
    token = os.environ["AIRTABLE_API_KEY"]
    url = f"https://api.airtable.com/v0/{base_id}/{table_id}/{record_id}"
    headers = {"Authorization": f"Bearer {token}"}
    with httpx.Client(timeout=30.0) as client:
        response = client.get(url, headers=headers)
        response.raise_for_status()
        return response.json()


def extract_linked_ids(fields: dict, column: str) -> list[str]:
    """Extract linked record IDs from an Airtable linked record field."""
    value = fields.get(column, [])
    if isinstance(value, list):
        return [v for v in value if isinstance(v, str) and v.startswith("rec")]
    return []


def get_zero_match_jobs(conn, partition_ids: list[str]) -> list[tuple[str, str, int]]:
    """Return list of (partition_id, job_title, match_count) for each partition. Include all; caller can filter to 0."""
    cur = conn.cursor(cursor_factory=RealDictCursor)
    out: list[tuple[str, str, int]] = []
    for pid in partition_ids:
        cur.execute(
            """SELECT nj.id AS job_id, nj.job_title
               FROM normalized_jobs nj WHERE nj.airtable_record_id = %s""",
            (pid,),
        )
        row = cur.fetchone()
        if not row:
            out.append((pid, "(no normalized job)", 0))
            continue
        job_id = row["job_id"]
        job_title = row["job_title"] or "(no title)"
        cur.execute("SELECT COUNT(*) AS c FROM matches WHERE job_id = %s", (job_id,))
        count = cur.fetchone()["c"]
        out.append((pid, job_title, count))
    cur.close()
    return out


def get_job_location_and_category(conn, partition_id: str) -> dict | None:
    """Return dict with location_raw, timezone_requirements, job_category for the job. None if job not found."""
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        """SELECT rj.location_raw, nj.timezone_requirements, nj.job_category, nj.id AS job_id
           FROM normalized_jobs nj
           JOIN raw_jobs rj ON rj.id = nj.raw_job_id
           WHERE nj.airtable_record_id = %s""",
        (partition_id,),
    )
    row = cur.fetchone()
    cur.close()
    return dict(row) if row else None


def get_candidate_by_airtable_id(conn, airtable_record_id: str) -> dict | None:
    """Return normalized_candidate row as dict (id, airtable_record_id, full_name, location_*, desired_job_categories, timezone) or None."""
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        """SELECT id, airtable_record_id, full_name, location_region, location_country, location_city,
                  desired_job_categories, timezone
           FROM normalized_candidates WHERE airtable_record_id = %s""",
        (airtable_record_id,),
    )
    row = cur.fetchone()
    cur.close()
    return dict(row) if row else None


def candidate_in_matches_for_job(conn, candidate_id, job_id) -> bool:
    """True if there is a match row for this candidate and job."""
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM matches WHERE candidate_id = %s AND job_id = %s LIMIT 1",
        (candidate_id, job_id),
    )
    found = cur.fetchone() is not None
    cur.close()
    return found


def would_candidate_pass_location(
    candidate: dict,
    location_raw: str | None,
    timezone_requirements: str | None,
) -> tuple[bool, str]:
    """Apply same 3-step location logic as location_prefiltered_candidates. Returns (passed, reason)."""
    job_locations = parse_job_preferred_locations((location_raw or "").strip() or None)
    if job_locations is None:
        return True, "no location filter"
    cand_dict = {
        "location_region": candidate.get("location_region"),
        "location_country": candidate.get("location_country"),
        "location_city": candidate.get("location_city"),
        "timezone": candidate.get("timezone"),
    }
    if candidate_passes_location_or_timezone(cand_dict, job_locations, timezone_requirements):
        return True, "strict (exact/region/timezone)"
    allowed_countries = job_locations_to_countries(job_locations)
    if allowed_countries and candidate_matches_country(cand_dict, allowed_countries):
        return True, "country expansion"
    allowed_regions = job_locations_to_regions(job_locations)
    if allowed_regions and candidate_matches_region(cand_dict, allowed_regions):
        return True, "region expansion"
    return False, "failed all (strict, country, region)"


_match_categories_cache: dict[str, set[str]] = {}


def get_match_categories_for_job_category(job_category: str) -> set[str]:
    """Resolved match categories for this job (DB only, no LLM). Cached by job_category."""
    key = (job_category or "").strip()
    if key not in _match_categories_cache:
        from talent_matching.resources.matchmaking import MatchmakingResource

        resource = MatchmakingResource()
        _match_categories_cache[key] = resource.get_match_categories_for_job_category(
            job_category, openrouter=None, context=None
        )
    return _match_categories_cache[key]


def would_candidate_pass_job_category(candidate: dict, job_match_categories_norm: set[str]) -> bool:
    """True if (job_match_categories_norm & desired_normalized) is non-empty."""
    desired = candidate.get("desired_job_categories") or []
    desired_normalized = {norm_cat(c) for c in desired if (c or "").strip()}
    return bool(job_match_categories_norm & desired_normalized)


def diagnose_candidate(
    conn,
    partition_id: str,
    job_info: dict | None,
    candidate_airtable_id: str,
    match_count_for_job: int,
) -> tuple[str, str | None]:
    """Returns (reason, detail). reason is one of: not_in_normalized_candidates | filtered_by_location | filtered_by_job_category | passed_but_not_in_shortlist."""
    candidate = get_candidate_by_airtable_id(conn, candidate_airtable_id)
    if not candidate:
        return "not_in_normalized_candidates", None

    if not job_info:
        return "job_not_found", None

    location_raw = job_info.get("location_raw")
    timezone_req = job_info.get("timezone_requirements")
    job_category = (job_info.get("job_category") or "").strip()
    job_id = job_info.get("job_id")

    passed_loc, loc_detail = would_candidate_pass_location(candidate, location_raw, timezone_req)
    if not passed_loc:
        return "filtered_by_location", loc_detail

    job_match_categories_norm = get_match_categories_for_job_category(job_category)
    if not would_candidate_pass_job_category(candidate, job_match_categories_norm):
        return "filtered_by_job_category", None

    if match_count_for_job == 0:
        return (
            "passed_filters_but_zero_matches_bug",
            "pool was 0 for job; candidate passed location and category",
        )

    in_shortlist = candidate_in_matches_for_job(conn, candidate["id"], job_id)
    if in_shortlist:
        return "in_shortlist", "candidate is in matches table for this job"
    return "passed_but_not_in_shortlist", "not in top 30 or dropped at refinement (must-haves)"


def location_expansion_detail(
    candidate: dict,
    location_raw: str | None,
    timezone_requirements: str | None,
) -> dict:
    """Return dict with job_locations, allowed_countries, allowed_regions, candidate location for logging."""
    job_locations = parse_job_preferred_locations((location_raw or "").strip() or None)
    if job_locations is None:
        return {
            "job_locations": None,
            "allowed_countries": set(),
            "allowed_regions": set(),
            "candidate": {
                k: candidate.get(k)
                for k in ("location_region", "location_country", "location_city")
            },
        }
    allowed_countries = job_locations_to_countries(job_locations)
    allowed_regions = job_locations_to_regions(job_locations)
    return {
        "job_locations": job_locations,
        "allowed_countries": allowed_countries,
        "allowed_regions": allowed_regions,
        "candidate": {
            "location_region": candidate.get("location_region"),
            "location_country": candidate.get("location_country"),
            "location_city": candidate.get("location_city"),
        },
    }


def main():
    apply_local_db()
    parser = argparse.ArgumentParser(
        description="List zero-match jobs and diagnose CLIENT INTRODUCTION candidates"
    )
    parser.add_argument(
        "--partitions",
        type=str,
        default=None,
        help="Comma-separated partition IDs (default: fetch 30 by Job Status)",
    )
    parser.add_argument(
        "--verbose-location",
        action="store_true",
        help="Print location expansion detail for filtered_by_location candidates",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        metavar="FILE",
        help="Write full analysis report to FILE (markdown)",
    )
    args = parser.parse_args()
    report_lines: list[str] = [] if args.output else None

    def out(s: str = "") -> None:
        print(s)
        if report_lines is not None:
            report_lines.append(s)

    def flush_report() -> None:
        if args.output and report_lines is not None:
            Path(args.output).write_text("\n".join(report_lines), encoding="utf-8")

    if args.partitions:
        partition_ids = [p.strip() for p in args.partitions.split(",") if p.strip()]
    else:
        partition_ids = fetch_ats_record_ids_by_statuses(STATUSES)

    conn = get_connection()

    # Step 1: match counts and zero-match list
    results = get_zero_match_jobs(conn, partition_ids)
    zero_match = [(pid, title) for pid, title, c in results if c == 0]
    # Only consider zero-match jobs that have a normalized job (so we can diagnose)
    zero_match_with_job = [
        (pid, title) for pid, title in zero_match if get_job_location_and_category(conn, pid)
    ]
    out(f"Partitions: {len(partition_ids)}")
    out(f"Zero-match jobs: {len(zero_match)} (with normalized job: {len(zero_match_with_job)})")
    for pid, title, c in results:
        marker = "  <-- zero" if c == 0 else ""
        out(f"  {pid}  matches={c}  {title[:50]}{marker}")

    if not zero_match_with_job:
        out("\nNo zero-match jobs with normalized job data. Exiting.")
        conn.close()
        flush_report()
        if args.output:
            print(f"Wrote report to {args.output}")
        return

    # Step 2: fetch CLIENT INTRODUCTION (and other columns) for zero-match jobs
    out("\n" + "=" * 60)
    out("  HUMAN-SELECTED CANDIDATES (zero-match jobs)")
    out("=" * 60)

    jobs_with_human_picks: list[tuple[str, str, dict[str, list[str]]]] = []
    for pid, title in zero_match_with_job:
        try:
            ats_record = fetch_ats_record(pid)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                continue
            raise
        ats_fields = ats_record.get("fields", {})
        human_selections: dict[str, list[str]] = {}
        for col in HUMAN_SELECTION_COLUMNS:
            ids = extract_linked_ids(ats_fields, col)
            if ids:
                human_selections[col] = ids
        if human_selections:
            jobs_with_human_picks.append((pid, title, human_selections))
            out(f"\n  {pid}  {title[:50]}")
            for col, ids in human_selections.items():
                out(f"    {col}: {len(ids)}  {ids[:5]}{'...' if len(ids) > 5 else ''}")

    if not jobs_with_human_picks:
        out(
            "\nNo human-selected candidates in CLIENT INTRODUCTION (or other columns) for zero-match jobs."
        )
        conn.close()
        flush_report()
        if args.output:
            print(f"Wrote report to {args.output}")
        return

    # Step 3: diagnose each human-selected candidate
    out("\n" + "=" * 60)
    out("  DIAGNOSIS (why each candidate was not found)")
    out("=" * 60)

    reason_counts: dict[str, int] = {}
    for pid, title, human_selections in jobs_with_human_picks:
        job_info = get_job_location_and_category(conn, pid)
        match_count = next((c for p, _, c in results if p == pid), 0)
        all_candidate_ids = set()
        for ids in human_selections.values():
            all_candidate_ids.update(ids)
        for cand_airtable_id in sorted(all_candidate_ids):
            reason, detail = diagnose_candidate(conn, pid, job_info, cand_airtable_id, match_count)
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
            cand = get_candidate_by_airtable_id(conn, cand_airtable_id)
            name = (cand.get("full_name") or cand_airtable_id) if cand else cand_airtable_id
            cols = [col for col, ids in human_selections.items() if cand_airtable_id in ids]
            out(f"\n  Job: {pid}  {title[:40]}")
            out(f"  Candidate: {cand_airtable_id}  {name}")
            out(f"  In columns: {', '.join(cols)}")
            out(f"  Reason: {reason}" + (f"  ({detail})" if detail else ""))
            if args.verbose_location and reason == "filtered_by_location" and cand:
                job_info_inner = job_info or get_job_location_and_category(conn, pid)
                if job_info_inner:
                    loc_detail = location_expansion_detail(
                        cand,
                        job_info_inner.get("location_raw"),
                        job_info_inner.get("timezone_requirements"),
                    )
                    out(f"  Location detail: job_locations={loc_detail['job_locations']!r}")
                    out(f"    allowed_countries={loc_detail['allowed_countries']}")
                    out(f"    allowed_regions={loc_detail['allowed_regions']}")
                    out(f"    candidate={loc_detail['candidate']}")
        flush_report()

    out("\n" + "=" * 60)
    out("  SUMMARY (diagnosis reasons)")
    out("=" * 60)
    for reason, count in sorted(reason_counts.items(), key=lambda x: -x[1]):
        out(f"  {reason}: {count}")

    conn.close()
    flush_report()
    if args.output:
        print(f"Wrote report to {args.output}")


if __name__ == "__main__":
    main()
