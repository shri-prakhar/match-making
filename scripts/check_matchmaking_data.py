#!/usr/bin/env python3
"""Quick check of DB row counts for matchmaking: normalized_jobs, normalized_candidates, vectors, matches.

Also supports --gaps to identify jobs that may receive non-fitting candidates due to missing
information (thin job description, zero must-haves, missing recruiter fields, etc.).

Usage:
    poetry run with-local-db python scripts/check_matchmaking_data.py
    poetry run with-remote-db python scripts/check_matchmaking_data.py
    poetry run with-remote-db python scripts/check_matchmaking_data.py --gaps [--limit N]
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv

load_dotenv()

import psycopg2  # noqa: E402
from psycopg2.extras import RealDictCursor  # noqa: E402


def check_gaps(conn, limit: int | None) -> None:
    """Identify jobs with missing or thin information that can cause poor match quality."""
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute(
        """
        SELECT
            nj.id,
            nj.airtable_record_id,
            nj.job_title,
            nj.company_name,
            nj.job_category,
            LENGTH(COALESCE(nj.job_description, '')) AS job_desc_len,
            CASE WHEN rj.job_description IS NOT NULL AND LENGTH(TRIM(COALESCE(rj.job_description, ''))) >= 50
                 THEN 1 ELSE 0 END AS raw_desc_ok,
            LENGTH(TRIM(COALESCE(rj.non_negotiables, ''))) AS non_negotiables_len,
            LENGTH(TRIM(COALESCE(rj.location_raw, ''))) AS location_raw_len,
            LENGTH(TRIM(COALESCE(rj.nice_to_have, ''))) AS nice_to_have_len,
            (SELECT COUNT(*) FROM job_vectors jv
             WHERE jv.job_id = rj.id) AS job_vector_count
        FROM normalized_jobs nj
        JOIN raw_jobs rj ON rj.airtable_record_id = nj.airtable_record_id
        WHERE EXISTS (
            SELECT 1 FROM matches m WHERE m.job_id = nj.id
        )
        ORDER BY nj.airtable_record_id
        """
    )
    rows = cur.fetchall()

    cur.execute(
        """
        SELECT jrs.job_id::text, COUNT(*) AS n
        FROM job_required_skills jrs
        WHERE jrs.requirement_type = 'MUST_HAVE'
        GROUP BY jrs.job_id
        """
    )
    must_have_by_job = {r["job_id"]: r["n"] for r in cur.fetchall()}

    if limit:
        rows = rows[:limit]

    print("\n" + "=" * 100)
    print("JOBS WITH POTENTIAL MATCHMAKING INFO GAPS (may cause non-fitting candidates)")
    print("=" * 100)
    print("See docs/plan/matchmaking-information-gaps-analysis.md for details.\n")

    if not rows:
        print("  No jobs with matches found.")
        cur.close()
        return

    print(f"Total jobs with matches: {len(rows)}")
    print()

    issue_count = 0
    for r in rows:
        rec_id = r["airtable_record_id"]
        nj_id = str(r["id"])
        must_haves = must_have_by_job.get(nj_id, 0)

        job_desc_len = r.get("job_desc_len") or 0
        raw_desc_ok = r.get("raw_desc_ok") or 0
        non_neg_len = r.get("non_negotiables_len") or 0
        loc_len = r.get("location_raw_len") or 0
        nice_len = r.get("nice_to_have_len") or 0
        job_vec_count = r.get("job_vector_count") or 0
        job_cat = (r.get("job_category") or "").strip()

        gaps = []
        if job_desc_len < 50:
            gaps.append("thin_job_desc")
        if raw_desc_ok == 0 and job_desc_len < 50:
            gaps.append("raw_desc_empty")
        if must_haves == 0:
            gaps.append("zero_must_haves")
        if non_neg_len == 0:
            gaps.append("no_non_negotiables")
        if loc_len == 0:
            gaps.append("no_location")
        if nice_len == 0:
            gaps.append("no_nice_to_have")
        if not job_cat:
            gaps.append("no_job_category")
        if job_vec_count < 3:
            gaps.append("few_job_vectors")

        if gaps:
            issue_count += 1
            title = (r.get("job_title") or "—")[:40]
            company = (r.get("company_name") or "—")[:25]
            print(f"  {rec_id}  {title} @ {company}")
            print(f"    Gaps: {', '.join(gaps)}")
            print(
                f"    (desc={job_desc_len} chars, must_haves={must_haves}, "
                f"non_neg={non_neg_len}, loc={loc_len}, vecs={job_vec_count})"
            )
            print()

    if issue_count == 0:
        print("  No jobs with identified gaps.")
    else:
        print(f"  Found {issue_count} job(s) with potential gaps.")
        print()

    cur.close()


def main():
    parser = argparse.ArgumentParser(description="Check matchmaking data and identify info gaps")
    parser.add_argument(
        "--gaps",
        action="store_true",
        help="Identify jobs with missing info that may cause non-fitting candidates",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of jobs to check (for --gaps)",
    )
    args = parser.parse_args()

    conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", 5432)),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"],
    )

    if args.gaps:
        check_gaps(conn, args.limit)
        conn.close()
        return

    cur = conn.cursor(cursor_factory=RealDictCursor)

    tables = [
        ("normalized_jobs", "id"),
        ("normalized_candidates", "id"),
        ("job_vectors", "job_id"),
        ("candidate_vectors", "candidate_id"),
        ("matches", "id"),
    ]
    print("Table row counts:")
    print("-" * 40)
    for table, _ in tables:
        cur.execute(f"SELECT COUNT(*) AS n FROM {table}")
        n = cur.fetchone()["n"]
        print(f"  {table}: {n}")
    print()

    cur.execute(
        "SELECT job_id, candidate_id, match_score, rank FROM matches ORDER BY job_id, rank LIMIT 10"
    )
    rows = cur.fetchall()
    print("Sample matches (up to 10):")
    print("-" * 40)
    if not rows:
        print("  (none)")
    for r in rows:
        print(
            f"  job={r['job_id']} candidate={r['candidate_id']} score={r['match_score']} rank={r['rank']}"
        )

    cur.execute("SELECT airtable_record_id, id FROM normalized_jobs ORDER BY id LIMIT 5")
    jobs = cur.fetchall()
    print()
    print("Sample normalized_jobs (airtable_record_id, id):")
    print("-" * 40)
    for r in jobs:
        print(f"  {r['airtable_record_id']} -> {r['id']}")

    cur.execute("SELECT airtable_record_id, id FROM normalized_candidates ORDER BY id LIMIT 5")
    cands = cur.fetchall()
    print()
    print("Sample normalized_candidates (airtable_record_id, id):")
    print("-" * 40)
    for r in cands:
        print(f"  {r['airtable_record_id']} -> {r['id']}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
