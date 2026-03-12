#!/usr/bin/env python3
"""List candidate partitions that would fail under current validation (need rerun).

Identifies raw_candidates that would hit InsufficientCvDataError (combined length < 500)
or MissingDesiredJobCategoryError (no desired job category). Correct candidates are skipped.

Usage:
  poetry run with-remote-db python scripts/list_candidates_needing_rerun.py
  poetry run with-remote-db python scripts/list_candidates_needing_rerun.py --run   # then launch backfill

Output: counts by reason, partition list, and optional backfill launch.
"""

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv  # noqa: E402
from psycopg2.extras import RealDictCursor  # noqa: E402

load_dotenv(PROJECT_ROOT / ".env")

from scripts.inspect_utils import get_connection  # noqa: E402
from talent_matching.assets.candidates import MIN_CV_CONTENT_LENGTH  # noqa: E402
from talent_matching.utils.airtable_mapper import (  # noqa: E402
    is_airtable_error_value,
    parse_comma_separated,
)


def _airtable_parts_from_raw(row: dict) -> list[str]:
    """Build same airtable parts as normalized_candidates (exclude Airtable error values)."""
    parts = []
    if row.get("full_name"):
        parts.append(f"Name: {row['full_name']}")
    if row.get("professional_summary"):
        parts.append(f"Summary: {row['professional_summary']}")
    if row.get("skills_raw"):
        parts.append(f"Skills: {row['skills_raw']}")
    we = row.get("work_experience_raw")
    if we and not is_airtable_error_value(we):
        parts.append(f"Experience: {we}")
    if row.get("cv_text"):
        parts.append(f"CV Content:\n{row['cv_text']}")
    if row.get("location_raw"):
        parts.append(f"Location: {row['location_raw']}")
    if row.get("proof_of_work"):
        parts.append(f"Proof of Work: {row['proof_of_work']}")
    if row.get("desired_job_categories_raw"):
        parts.append(f"Desired Roles: {row['desired_job_categories_raw']}")
    if row.get("salary_range_raw"):
        parts.append(
            "Salary Expectations (interpret 'k' as thousands, e.g. 60-70k = 60,000-70,000 yearly): "
            f"{row['salary_range_raw']}"
        )
    if row.get("github_url"):
        parts.append(f"GitHub: {row['github_url']}")
    if row.get("linkedin_url"):
        parts.append(f"LinkedIn: {row['linkedin_url']}")
    if row.get("x_profile_url"):
        parts.append(f"Twitter/X: {row['x_profile_url']}")
    if row.get("earn_profile_url"):
        parts.append(f"Earn Profile: {row['earn_profile_url']}")
    return parts


def main() -> int:
    do_run = "--run" in sys.argv
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        """
        SELECT airtable_record_id, full_name, desired_job_categories_raw,
               professional_summary, skills_raw, work_experience_raw, cv_text,
               location_raw, proof_of_work, salary_range_raw,
               github_url, linkedin_url, x_profile_url, earn_profile_url, cv_text_pdf
        FROM raw_candidates
        """
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    # Reasons: no_desired_job_category, insufficient_cv_data
    need_rerun: list[tuple[str, list[str]]] = []  # (partition_id, [reasons])

    for row in rows:
        pid = row["airtable_record_id"]
        if not pid:
            continue
        reasons = []
        desired = parse_comma_separated(row.get("desired_job_categories_raw"))
        if not desired:
            reasons.append("no_desired_job_category")
        parts = _airtable_parts_from_raw(row)
        cv_airtable = "\n\n".join(parts) if parts else ""
        cv_pdf = (row.get("cv_text_pdf") or "").strip()
        total_len = len(cv_airtable.strip()) + len(cv_pdf)
        if total_len < MIN_CV_CONTENT_LENGTH:
            reasons.append("insufficient_cv_data")
        if reasons:
            need_rerun.append((pid, reasons))

    # Dedupe by partition (each partition has one row)
    by_reason: dict[str, list[str]] = {"no_desired_job_category": [], "insufficient_cv_data": []}
    for pid, reasons in need_rerun:
        for r in reasons:
            if pid not in by_reason[r]:
                by_reason[r].append(pid)

    n_no_desired = len(by_reason["no_desired_job_category"])
    n_insufficient = len(by_reason["insufficient_cv_data"])
    all_ids = sorted(set(pid for pid, _ in need_rerun))
    total = len(all_ids)
    both = len(
        [
            pid
            for pid, reasons in need_rerun
            if "no_desired_job_category" in reasons and "insufficient_cv_data" in reasons
        ]
    )

    print("Candidates that would fail current validation (need rerun)")
    print("=" * 60)
    print(f"  Total partitions needing rerun: {total}")
    print(f"  - No desired job category:      {n_no_desired}")
    print(f"  - Insufficient CV (< {MIN_CV_CONTENT_LENGTH} chars): {n_insufficient}")
    if both:
        print(f"  - Both reasons:                 {both}")
    print()
    if total == 0:
        print("  No candidates need rerun. Exiting.")
        return 0

    # Per-reason lists (for clarity)
    only_no_desired = [
        p
        for p in by_reason["no_desired_job_category"]
        if p not in by_reason["insufficient_cv_data"]
    ]
    only_insufficient = [
        p
        for p in by_reason["insufficient_cv_data"]
        if p not in by_reason["no_desired_job_category"]
    ]
    if only_no_desired:
        print("  Reason: no_desired_job_category only:")
        print("    " + ", ".join(only_no_desired))
    if only_insufficient:
        print("  Reason: insufficient_cv_data only:")
        print("    " + ", ".join(only_insufficient))
    if both:
        both_ids = [
            pid
            for pid, reasons in need_rerun
            if set(reasons) >= {"no_desired_job_category", "insufficient_cv_data"}
        ]
        print("  Reason: both:")
        print("    " + ", ".join(both_ids))
    print()
    print("All partition IDs (for backfill):")
    print("  " + ", ".join(all_ids))
    print()

    if do_run:
        remote_host = os.environ.get("REMOTE_HOST")
        remote_dir = os.environ.get("REMOTE_PROJECT_DIR", "/root/match-making")
        if not remote_host:
            print("REMOTE_HOST not set. Run backfill manually:", file=sys.stderr)
            print(
                f"  dagster job backfill -w /workspace.yaml -j candidate_pipeline --partitions '{','.join(all_ids)}' -l talent_matching --noprompt",
                file=sys.stderr,
            )
            return 1
        partitions_arg = ",".join(all_ids)
        cmd = (
            f"cd {remote_dir} && docker compose -f docker-compose.prod.yml run --rm "
            "-e POSTGRES_HOST=postgres "
            f"-v {remote_dir}/docker/workspace.yaml:/workspace.yaml:ro "
            f"-v {remote_dir}/docker/dagster.yaml:/opt/dagster/dagster_home/dagster.yaml:ro "
            "-e DAGSTER_HOME=/opt/dagster/dagster_home "
            "dagster-code "
            "dagster job backfill -w /workspace.yaml -j candidate_pipeline "
            f"--partitions {partitions_arg!r} -l talent_matching --noprompt"
        )
        import subprocess

        print("Launching backfill on remote...")
        result = subprocess.run(["ssh", remote_host, cmd])
        if result.returncode != 0:
            return result.returncode
        print("Backfill launched. Check: poetry run remote-ui → http://localhost:3000 → Backfills")
    else:
        print("To launch backfill for these partitions on remote, run:")
        print("  poetry run with-remote-db python scripts/list_candidates_needing_rerun.py --run")

    return 0


if __name__ == "__main__":
    sys.exit(main())
