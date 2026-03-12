#!/usr/bin/env python3
"""Find jobs where AI suggestions score poorly (1-4/10) and diagnose why human picks are missing.

Queries Postgres for jobs with low average llm_fit_score on the shortlist, fetches
human-selected candidates from Airtable (and optionally ground_truth_outcomes), then
for each human-selected candidate not in the matches shortlist runs the same checks
as score_candidate_against_job to determine exclusion reason: not_in_db, location_prefilter,
job_category, skill_threshold, or rank_beyond_30.

Usage:
    poetry run with-remote-db python scripts/find_bad_ai_jobs_and_diagnose_human_picks.py
    poetry run with-remote-db python scripts/find_bad_ai_jobs_and_diagnose_human_picks.py --max-jobs 5
    poetry run with-remote-db python scripts/find_bad_ai_jobs_and_diagnose_human_picks.py --non-technical
    poetry run with-remote-db python scripts/find_bad_ai_jobs_and_diagnose_human_picks.py --max-llm 4 --output report.json
    On server: poetry run python scripts/find_bad_ai_jobs_and_diagnose_human_picks.py --local

Requires:
    - POSTGRES_* and AIRTABLE_BASE_ID, AIRTABLE_ATS_TABLE_ID, AIRTABLE_API_KEY in .env
    - For remote DB: poetry run remote-ui or local-matchmaking must be running
"""

import argparse
import importlib.util
import json
import os
import re
import subprocess
import sys

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from talent_matching.script_env import apply_local_db  # noqa: E402

# Reuse human selection columns and Airtable fetch from analyze_human_vs_system (file-based import)
_analyze_path = os.path.join(os.path.dirname(__file__), "analyze_human_vs_system.py")
_spec = importlib.util.spec_from_file_location("analyze_human_vs_system", _analyze_path)
_analyze_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_analyze_mod)
HUMAN_SELECTION_COLUMNS = _analyze_mod.HUMAN_SELECTION_COLUMNS
extract_linked_ids = _analyze_mod.extract_linked_ids
fetch_ats_record = _analyze_mod.fetch_ats_record

# Default: jobs where shortlist (rank 1-15) has average LLM score <= this
DEFAULT_MAX_LLM_AVG = 4.0

# Technical job_category substrings to exclude when --non-technical is set
NON_TECHNICAL_EXCLUDE_KEYWORDS = (
    "software",
    "engineer",
    "developer",
    "data scientist",
    "data engineer",
    "technical",
    "programming",
    "devops",
    "sre",
)


def get_connection():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", 5432)),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"],
    )


def find_bad_ai_jobs(
    conn,
    max_llm_avg: float = DEFAULT_MAX_LLM_AVG,
    non_technical_only: bool = False,
    limit: int | None = None,
) -> list[dict]:
    """Return jobs that have shortlist (rank 1-15) with average llm_fit_score <= max_llm_avg."""
    cur = conn.cursor(cursor_factory=RealDictCursor)
    # Subquery: per job, avg llm_fit_score over rows with rank <= 15
    cur.execute(
        """
        SELECT nj.airtable_record_id,
               nj.job_title,
               nj.job_category,
               nj.company_name,
               AVG(m.llm_fit_score) AS avg_llm_shortlist,
               COUNT(*) FILTER (WHERE m.rank IS NOT NULL AND m.rank <= 15) AS shortlist_count
        FROM matches m
        JOIN normalized_jobs nj ON m.job_id = nj.id
        WHERE m.rank IS NOT NULL AND m.rank <= 15
        GROUP BY nj.id, nj.airtable_record_id, nj.job_title, nj.job_category, nj.company_name
        HAVING AVG(m.llm_fit_score) <= %s
        ORDER BY AVG(m.llm_fit_score) ASC
        """,
        (max_llm_avg,),
    )
    rows = cur.fetchall()
    cur.close()

    out = [dict(r) for r in rows]
    if non_technical_only:
        out = [r for r in out if not _is_technical_job_category(r.get("job_category") or "")]
    if limit is not None:
        out = out[:limit]
    return out


def _is_technical_job_category(job_category: str) -> bool:
    lower = (job_category or "").strip().lower()
    return any(kw in lower for kw in NON_TECHNICAL_EXCLUDE_KEYWORDS)


def get_human_selected_candidate_ids(ats_record_id: str) -> set[str]:
    """Fetch ATS record from Airtable and return set of candidate airtable record ids from human columns."""
    record = fetch_ats_record(ats_record_id)
    fields = record.get("fields", {})
    ids: set[str] = set()
    for col in HUMAN_SELECTION_COLUMNS:
        ids.update(extract_linked_ids(fields, col))
    return ids


def get_ground_truth_candidate_ids(conn, job_airtable_record_id: str) -> set[str]:
    """Return set of candidate airtable record ids from ground_truth_outcomes for this job."""
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        """SELECT candidate_airtable_record_id
           FROM ground_truth_outcomes
           WHERE job_airtable_record_id = %s""",
        (job_airtable_record_id,),
    )
    rows = cur.fetchall()
    cur.close()
    return {
        r["candidate_airtable_record_id"] for r in rows if r.get("candidate_airtable_record_id")
    }


def get_match_candidate_ids(conn, job_airtable_record_id: str) -> set[str]:
    """Return set of candidate airtable record ids that appear in matches for this job."""
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        """SELECT nc.airtable_record_id
           FROM matches m
           JOIN normalized_jobs nj ON m.job_id = nj.id
           JOIN normalized_candidates nc ON m.candidate_id = nc.id
           WHERE nj.airtable_record_id = %s AND nc.airtable_record_id IS NOT NULL""",
        (job_airtable_record_id,),
    )
    rows = cur.fetchall()
    cur.close()
    return {r["airtable_record_id"] for r in rows if r.get("airtable_record_id")}


def candidates_in_normalized(conn, candidate_airtable_ids: list[str]) -> set[str]:
    """Return subset of candidate_airtable_ids that exist in normalized_candidates."""
    if not candidate_airtable_ids:
        return set()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    placeholders = ",".join(["%s"] * len(candidate_airtable_ids))
    cur.execute(
        f"""SELECT airtable_record_id FROM normalized_candidates
            WHERE airtable_record_id IN ({placeholders})""",
        candidate_airtable_ids,
    )
    rows = cur.fetchall()
    cur.close()
    return {r["airtable_record_id"] for r in rows if r.get("airtable_record_id")}


def diagnose_candidate_exclusion(
    job_partition_id: str,
    candidate_partition_id: str,
    script_dir: str,
) -> tuple[str, float | None, int | None]:
    """Run score_candidate_against_job and parse stdout for exclusion reason and optional score/rank.

    Returns (exclusion_reason, combined_score_100_or_none, would_be_rank_or_none).
    """
    script_path = os.path.join(script_dir, "score_candidate_against_job.py")
    result = subprocess.run(
        [sys.executable, script_path, job_partition_id, candidate_partition_id],
        capture_output=True,
        text=True,
        env=os.environ,
        cwd=os.path.dirname(os.path.dirname(script_dir)),
    )
    stdout = result.stdout or ""
    stderr = result.stderr or ""

    # Order of checks matters: first FAIL wins
    if (
        "FAIL: Candidate does not pass location" in stdout
        or "they never entered the scoring pool" in stdout
    ):
        return ("location_prefilter", None, None)
    if "FAIL: Candidate has no desired_job_categories" in stdout or "FAIL: Job category" in stdout:
        return ("job_category", None, None)
    if "FAIL: skill_fit_score" in stdout and "SKILL_MIN_THRESHOLD" in stdout:
        return ("skill_threshold", None, None)
    if "Candidate is NOT in top 30" in stdout or "Candidate is NOT in top 30" in stderr:
        return ("rank_beyond_30", None, None)

    # "Candidate would be rank ~X"
    rank_match = re.search(r"Candidate would be rank ~(\d+)\s*\(score\s*([\d.]+)", stdout)
    if rank_match:
        rank = int(rank_match.group(1))
        score = float(rank_match.group(2))
        return ("rank_beyond_30", score, rank)

    # No job / no candidate (script returns 1)
    if result.returncode != 0:
        if "No normalized job" in stderr or "No normalized job" in stdout:
            return ("job_not_in_db", None, None)
        if "No normalized candidate" in stderr or "No normalized candidate" in stdout:
            return ("not_in_db", None, None)
        return ("script_error", None, None)

    # In stored matches (shouldn't happen if we only call for not-in-matches)
    if "Candidate IS in stored matches" in stdout:
        return ("in_matches", None, None)

    return ("unknown", None, None)


def run(
    max_llm_avg: float = DEFAULT_MAX_LLM_AVG,
    non_technical_only: bool = False,
    max_jobs: int | None = None,
    use_ground_truth: bool = True,
    output_path: str | None = None,
    verbose: bool = True,
) -> dict:
    """Main workflow: find bad-AI jobs, get human picks, diagnose missing candidates, return report dict."""
    conn = get_connection()
    script_dir = os.path.dirname(os.path.abspath(__file__))

    bad_jobs = find_bad_ai_jobs(
        conn,
        max_llm_avg=max_llm_avg,
        non_technical_only=non_technical_only,
        limit=max_jobs,
    )

    if verbose:
        print(
            f"Found {len(bad_jobs)} job(s) with shortlist avg llm_fit_score <= {max_llm_avg}"
            + (" (non-technical only)" if non_technical_only else "")
        )
        if not bad_jobs:
            print("Nothing to analyze.")
        else:
            print("Diagnosing human-selected candidates not in shortlist...")

    report: dict = {
        "max_llm_avg": max_llm_avg,
        "non_technical_only": non_technical_only,
        "jobs_analyzed": len(bad_jobs),
        "jobs": [],
    }

    for i, job_row in enumerate(bad_jobs):
        job_at_id = job_row["airtable_record_id"]
        job_title = job_row.get("job_title") or "—"
        job_category = job_row.get("job_category") or "—"
        company = job_row.get("company_name") or "—"
        avg_llm = float(job_row["avg_llm_shortlist"] or 0)

        human_ids = get_human_selected_candidate_ids(job_at_id)
        if use_ground_truth:
            gt_ids = get_ground_truth_candidate_ids(conn, job_at_id)
            human_ids = human_ids | gt_ids

        in_shortlist_ids = get_match_candidate_ids(conn, job_at_id)
        missing_ids = human_ids - in_shortlist_ids
        in_db_ids = candidates_in_normalized(conn, list(missing_ids))

        job_report = {
            "job_airtable_record_id": job_at_id,
            "job_title": job_title,
            "job_category": job_category,
            "company": company,
            "avg_llm_shortlist": round(avg_llm, 2),
            "human_selected_count": len(human_ids),
            "in_shortlist_count": len(human_ids & in_shortlist_ids),
            "not_in_shortlist_count": len(missing_ids),
            "missing_candidates": [],
        }

        for cand_at_id in sorted(missing_ids):
            if cand_at_id not in in_db_ids:
                job_report["missing_candidates"].append(
                    {
                        "candidate_airtable_record_id": cand_at_id,
                        "exclusion_reason": "not_in_db",
                        "combined_score_100": None,
                        "would_be_rank": None,
                    }
                )
                continue
            reason, score, rank = diagnose_candidate_exclusion(job_at_id, cand_at_id, script_dir)
            job_report["missing_candidates"].append(
                {
                    "candidate_airtable_record_id": cand_at_id,
                    "exclusion_reason": reason,
                    "combined_score_100": round(score, 2) if score is not None else None,
                    "would_be_rank": rank,
                }
            )

        report["jobs"].append(job_report)

        if verbose:
            print(f"\n[{i+1}/{len(bad_jobs)}] {job_at_id}  {job_title[:40]} @ {company}")
            print(
                f"    avg_llm={avg_llm:.2f}  human={len(human_ids)}  in_shortlist={len(human_ids & in_shortlist_ids)}  missing={len(missing_ids)}"
            )
            for c in job_report["missing_candidates"]:
                r = c["exclusion_reason"]
                s = c.get("combined_score_100")
                rk = c.get("would_be_rank")
                extra = f" score={s:.1f}" if s is not None else ""
                if rk is not None:
                    extra += f" rank~{rk}"
                print(f"      {c['candidate_airtable_record_id']}  {r}{extra}")

    conn.close()

    if output_path:
        with open(output_path, "w") as f:
            json.dump(report, f, indent=2)
        if verbose:
            print(f"\nWrote report to {output_path}")

    return report


def main() -> int:
    apply_local_db()
    parser = argparse.ArgumentParser(
        description="Find bad-AI jobs and diagnose why human-selected candidates are missing from the shortlist"
    )
    parser.add_argument(
        "--local",
        action="store_true",
        help="Use local Postgres (when running on the server).",
    )
    parser.add_argument(
        "--max-llm",
        type=float,
        default=DEFAULT_MAX_LLM_AVG,
        help=f"Max average LLM score (1-10) on shortlist to consider job 'bad' (default: {DEFAULT_MAX_LLM_AVG})",
    )
    parser.add_argument(
        "--non-technical",
        action="store_true",
        help="Only include jobs whose job_category is not technical (exclude Engineer, Developer, etc.)",
    )
    parser.add_argument(
        "--max-jobs",
        type=int,
        default=None,
        help="Max number of bad-AI jobs to analyze (default: all)",
    )
    parser.add_argument(
        "--no-ground-truth",
        action="store_true",
        help="Do not merge in candidates from ground_truth_outcomes",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Write JSON report to this path",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Minimal stdout",
    )
    args = parser.parse_args()

    run(
        max_llm_avg=args.max_llm,
        non_technical_only=args.non_technical,
        max_jobs=args.max_jobs,
        use_ground_truth=not args.no_ground_truth,
        output_path=args.output,
        verbose=not args.quiet,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
