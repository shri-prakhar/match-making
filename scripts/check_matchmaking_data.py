#!/usr/bin/env python3
"""Quick check of DB row counts for matchmaking: normalized_jobs, normalized_candidates, vectors, matches.

Usage:
    poetry run with-local-db python scripts/check_matchmaking_data.py
    poetry run with-remote-db python scripts/check_matchmaking_data.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv

load_dotenv()

import psycopg2  # noqa: E402
from psycopg2.extras import RealDictCursor  # noqa: E402


def main():
    conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", 5432)),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"],
    )
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
