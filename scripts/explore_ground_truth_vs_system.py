#!/usr/bin/env python3
"""Explore ground-truth outcomes vs system match scores.

Loads ground_truth_outcomes, joins with normalized_jobs/candidates and matches,
and compares ground-truth candidate scores/ranks to system top picks.

Usage:
    poetry run with-remote-db python scripts/explore_ground_truth_vs_system.py
    poetry run with-remote-db python scripts/explore_ground_truth_vs_system.py --limit 5
    poetry run with-remote-db python scripts/explore_ground_truth_vs_system.py --hired-only
    On server: poetry run python scripts/explore_ground_truth_vs_system.py --local

Requires:
    - ground_truth_outcomes populated (run backfill_ground_truth.py first)
    - For remote: poetry run remote-ui or local-matchmaking must be running
"""

import argparse
import os
import sys

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from talent_matching.config.scoring import get_weights_for_job_category  # noqa: E402
from talent_matching.script_env import apply_local_db  # noqa: E402


def get_connection():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", 5432)),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"],
    )


def _fmt(v) -> str:
    return f"{v:.2f}" if v is not None else "--"


def main() -> None:
    apply_local_db()
    parser = argparse.ArgumentParser(
        description="Compare ground-truth outcomes from Airtable vs system match scores"
    )
    parser.add_argument(
        "--local",
        action="store_true",
        help="Use local Postgres (when running on the server).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Max jobs to analyze (default: 20)",
    )
    parser.add_argument(
        "--hired-only",
        action="store_true",
        help="Only analyze jobs with ground-truth hired candidates",
    )
    parser.add_argument(
        "--introduced-only",
        action="store_true",
        help="Only analyze jobs with ground-truth client_introduction candidates",
    )
    args = parser.parse_args()

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # 1. Dataset summary
    cur.execute(
        """SELECT
            COUNT(*) AS total_pairs,
            COUNT(DISTINCT job_airtable_record_id) AS unique_jobs,
            COUNT(DISTINCT candidate_airtable_record_id) AS unique_candidates,
            COUNT(*) FILTER (WHERE potential_talent_fit_at IS NOT NULL) AS ptf_count,
            COUNT(*) FILTER (WHERE client_introduction_at IS NOT NULL) AS intro_count,
            COUNT(*) FILTER (WHERE hired_at IS NOT NULL) AS hired_count
           FROM ground_truth_outcomes"""
    )
    summary = cur.fetchone()
    print("=" * 80)
    print("GROUND TRUTH DATASET SUMMARY")
    print("=" * 80)
    print(f"  Total (job, candidate) pairs: {summary['total_pairs']}")
    print(f"  Unique jobs:                  {summary['unique_jobs']}")
    print(f"  Unique candidates:           {summary['unique_candidates']}")
    print(f"  Potential Talent Fit:         {summary['ptf_count']}")
    print(f"  CLIENT INTRODUCTION:         {summary['intro_count']}")
    print(f"  Hired:                        {summary['hired_count']}")
    print()

    # 2. Get job IDs with ground-truth (prioritize hired, then introduced)
    filter_clause = ""
    if args.hired_only:
        filter_clause = " AND hired_at IS NOT NULL"
    elif args.introduced_only:
        filter_clause = " AND client_introduction_at IS NOT NULL"

    cur.execute(
        f"""
        SELECT DISTINCT job_airtable_record_id
        FROM ground_truth_outcomes
        WHERE 1=1 {filter_clause}
        ORDER BY job_airtable_record_id
        LIMIT %s
        """,
        (args.limit,),
    )
    job_ids = [r["job_airtable_record_id"] for r in cur.fetchall()]

    # Load all ground-truth pairs for these jobs
    if not job_ids:
        print("No ground-truth jobs to analyze.")
        cur.close()
        conn.close()
        return

    placeholders = ",".join(["%s"] * len(job_ids))
    cur.execute(
        f"""
        SELECT job_airtable_record_id, candidate_airtable_record_id,
               potential_talent_fit_at, client_introduction_at, hired_at,
               source_columns
        FROM ground_truth_outcomes
        WHERE job_airtable_record_id IN ({placeholders})
        """,
        job_ids,
    )
    gt_rows = cur.fetchall()

    # Group by job
    jobs_by_id: dict[str, list[dict]] = {}
    for r in gt_rows:
        jid = r["job_airtable_record_id"]
        if jid not in jobs_by_id:
            jobs_by_id[jid] = []
        jobs_by_id[jid].append(dict(r))

    print("=" * 80)
    print("GROUND TRUTH vs SYSTEM: PER-JOB ANALYSIS")
    print("=" * 80)

    for job_at_id in job_ids:
        gt_candidates = jobs_by_id[job_at_id]

        # Resolve job: normalized_jobs.airtable_record_id = ATS record ID, or via raw_jobs
        cur.execute(
            """SELECT id, job_title, company_name, job_category
               FROM normalized_jobs WHERE airtable_record_id = %s""",
            (job_at_id,),
        )
        job = cur.fetchone()
        if not job:
            cur.execute(
                """SELECT nj.id, nj.job_title, nj.company_name, nj.job_category
                   FROM normalized_jobs nj
                   JOIN raw_jobs rj ON nj.raw_job_id = rj.id
                   WHERE rj.airtable_record_id = %s""",
                (job_at_id,),
            )
            job = cur.fetchone()
        if not job:
            print(f"\n  Job {job_at_id}: NOT in normalized_jobs (skipping)")
            continue

        job_id = job["id"]
        job_title = (job.get("job_title") or "—")[:40]
        company = (job.get("company_name") or "—")[:25]
        weights = get_weights_for_job_category(job.get("job_category"))

        # Load system matches for this job
        cur.execute(
            """SELECT m.rank, m.match_score,
                      m.role_similarity_score, m.domain_similarity_score, m.culture_similarity_score,
                      m.skills_match_score, m.llm_fit_score,
                      nc.full_name, nc.airtable_record_id
               FROM matches m
               JOIN normalized_candidates nc ON m.candidate_id = nc.id
               WHERE m.job_id = %s
               ORDER BY m.rank NULLS LAST, m.match_score DESC""",
            (job_id,),
        )
        system_matches = cur.fetchall()
        matches_by_airtable_id = {
            m["airtable_record_id"]: m for m in system_matches if m["airtable_record_id"]
        }

        # Ground-truth candidates: resolve names and get system scores
        cand_at_ids = [r["candidate_airtable_record_id"] for r in gt_candidates]
        placeholders = ",".join(["%s"] * len(cand_at_ids))
        cur.execute(
            f"""SELECT id, full_name, airtable_record_id FROM normalized_candidates
                WHERE airtable_record_id IN ({placeholders})""",
            cand_at_ids,
        )
        cand_info = {r["airtable_record_id"]: r for r in cur.fetchall()}

        print(f"\n  Job: {job_title} @ {company}")

        # Ground-truth outcomes
        print("\n    Ground-truth candidates:")
        for r in gt_candidates:
            at_id = r["candidate_airtable_record_id"]
            name = cand_info.get(at_id, {}).get("full_name", at_id) if cand_info else at_id
            outcome = []
            if r["hired_at"]:
                outcome.append("HIRED")
            if r["client_introduction_at"]:
                outcome.append("intro")
            if r["potential_talent_fit_at"]:
                outcome.append("ptf")
            outcome_str = ", ".join(outcome) or "—"

            sys_match = matches_by_airtable_id.get(at_id)
            if sys_match:
                rank = sys_match["rank"]
                score = float(sys_match["match_score"]) * 100 if sys_match["match_score"] else 0
                llm = sys_match.get("llm_fit_score")
                rs, ds, cs = (
                    sys_match.get("role_similarity_score"),
                    sys_match.get("domain_similarity_score"),
                    sys_match.get("culture_similarity_score"),
                )
                vec = (
                    (
                        weights.role_weight * rs
                        + weights.domain_weight * ds
                        + weights.culture_weight * cs
                    )
                    * 100
                    if rs is not None and ds is not None and cs is not None
                    else None
                )
                print(
                    f"      {name[:35]:35} | {outcome_str:12} | rank {rank:3} | score {_fmt(score)} | LLM {llm or '—'}/10 | vec {_fmt(vec)}"
                )
            else:
                print(f"      {name[:35]:35} | {outcome_str:12} | NOT in system matches")

        # System top 5
        print("\n    System top 5:")
        for m in system_matches[:5]:
            name = (m["full_name"] or "—")[:35]
            at_id = m["airtable_record_id"] or "—"
            rank = m["rank"]
            score = float(m["match_score"]) * 100 if m["match_score"] else 0
            llm = m.get("llm_fit_score")
            is_gt = " <<< GT" if at_id in cand_at_ids else ""
            print(f"      #{rank} {name:35} | score {_fmt(score)} | LLM {llm or '—'}/10{is_gt}")

        # Overlap
        gt_in_system = set(cand_at_ids) & set(matches_by_airtable_id.keys())
        gt_ranks = [
            matches_by_airtable_id[aid]["rank"]
            for aid in gt_in_system
            if matches_by_airtable_id[aid].get("rank")
        ]
        if gt_ranks:
            print(
                f"\n    Overlap: {len(gt_in_system)}/{len(cand_at_ids)} GT in system | avg rank: {sum(gt_ranks)/len(gt_ranks):.1f}"
            )

    cur.close()
    conn.close()
    print("\n" + "=" * 80)


if __name__ == "__main__":
    main()
