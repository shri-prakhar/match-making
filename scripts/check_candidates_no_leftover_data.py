#!/usr/bin/env python3
"""Check that failed-at-normalization candidates have no leftover DB data.

Uses the same criteria as list_candidates_needing_rerun to get partition IDs
that would fail validation (no desired job category / insufficient CV). Then
checks normalized_candidates, candidate_vectors, and matches for those partitions.

Usage:
  poetry run with-remote-db python scripts/check_candidates_no_leftover_data.py
  poetry run with-remote-db python scripts/check_candidates_no_leftover_data.py --clear  # clear all 278
"""

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
    do_clear = "--clear" in sys.argv
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # Same query and logic as list_candidates_needing_rerun
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

    need_rerun_ids: list[str] = []
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
            need_rerun_ids.append(pid)

    if not need_rerun_ids:
        print("No candidate partitions in 'would fail validation' set. Nothing to check.")
        cur.close()
        conn.close()
        return 0

    n_partitions = len(need_rerun_ids)
    print(f"Candidates that would fail validation (need rerun): {n_partitions} partitions")
    print()

    # Check normalized_candidates for these partition keys
    cur.execute(
        """
        SELECT airtable_record_id, id, full_name
        FROM normalized_candidates
        WHERE airtable_record_id = ANY(%s)
        """,
        (need_rerun_ids,),
    )
    norm_rows = cur.fetchall()
    norm_count = len(norm_rows)

    # Check candidate_vectors: raw_candidates.id for these partitions
    cur.execute(
        """
        SELECT cv.candidate_id, r.airtable_record_id
        FROM candidate_vectors cv
        JOIN raw_candidates r ON r.id = cv.candidate_id
        WHERE r.airtable_record_id = ANY(%s)
        """,
        (need_rerun_ids,),
    )
    vec_rows = cur.fetchall()
    vec_count = len(vec_rows)

    # Check matches: candidate_id is normalized_candidates.id (UUID)
    norm_ids = [r["id"] for r in norm_rows]
    match_count = 0
    if norm_ids:
        cur.execute(
            "SELECT COUNT(*) AS c FROM matches WHERE candidate_id = ANY(%s::uuid[])",
            ([str(u) for u in norm_ids],),
        )
        match_count = cur.fetchone()["c"]

    cur.close()
    conn.close()

    print("Leftover data for those partitions:")
    print(f"  normalized_candidates: {norm_count}")
    print(f"  candidate_vectors:     {vec_count}")
    print(f"  matches:               {match_count}")
    print()

    if norm_count > 0:
        print("Partitions that still have normalized_candidates (first 20):")
        for r in norm_rows[:20]:
            print(f"  {r['airtable_record_id']}  {r['full_name']}")
        if len(norm_rows) > 20:
            print(f"  ... and {len(norm_rows) - 20} more")
        print()

    if vec_count > 0:
        print("Partitions that still have candidate_vectors (first 20):")
        seen = set()
        for r in vec_rows:
            pid = r["airtable_record_id"]
            if pid not in seen:
                seen.add(pid)
                print(f"  {pid}")
                if len(seen) >= 20:
                    break
        if vec_count > 20:
            print(f"  ... and more rows (total {vec_count} vector rows)")
        print()

    if norm_count == 0 and vec_count == 0 and match_count == 0:
        print("OK: No leftover data for failed-at-normalization candidates.")
        return 0

    if do_clear:
        conn.close()
        from talent_matching.db import get_session  # noqa: E402
        from talent_matching.utils.clear_candidate_data import (
            clear_candidate_partition_data,  # noqa: E402
        )

        session = get_session()
        cleared = 0
        for pk in need_rerun_ids:
            if clear_candidate_partition_data(session, pk):
                cleared += 1
        session.commit()
        session.close()
        print(f"Cleared data for {cleared}/{n_partitions} partitions.")
        return 0

    print("To clear leftover data, run:")
    print("  poetry run with-remote-db python scripts/check_candidates_no_leftover_data.py --clear")
    return 1


if __name__ == "__main__":
    sys.exit(main())
