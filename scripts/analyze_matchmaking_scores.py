#!/usr/bin/env python3
"""Analyze how matchmaking scores were computed for a job partition.

Queries the DB for: job required/nice-to-have skills, candidate skills,
and stored match rows (matching_skills, missing_skills, all scores).
Prints how the combined score and skills fit came about.

Usage:
    poetry run with-remote-db python scripts/analyze_matchmaking_scores.py recIqBsuF33YrIrMX
    On server: poetry run python scripts/analyze_matchmaking_scores.py --local [partition_id]
"""

import os
import sys

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

load_dotenv()

from talent_matching.script_env import apply_local_db  # noqa: E402


def main() -> None:
    apply_local_db()
    partition_id = sys.argv[1] if len(sys.argv) > 1 else "recIqBsuF33YrIrMX"
    if partition_id == "--local" and len(sys.argv) > 2:
        partition_id = sys.argv[2]
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = int(os.environ.get("POSTGRES_PORT", 5432))
    conn = psycopg2.connect(
        host=host,
        port=port,
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"],
    )
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # Job by partition
    cur.execute(
        """SELECT id, raw_job_id, job_title, company_name, airtable_record_id
           FROM normalized_jobs WHERE airtable_record_id = %s""",
        (partition_id,),
    )
    job = cur.fetchone()
    if not job:
        print(f"No normalized job for partition_id={partition_id}")
        cur.close()
        conn.close()
        sys.exit(1)

    job_id = job["id"]
    job_title = job.get("job_title") or "—"
    company = job.get("company_name") or "—"

    # Job required skills (must_have / nice_to_have) with skill name
    cur.execute(
        """SELECT jrs.requirement_type, jrs.min_years, jrs.min_level, s.name AS skill_name
           FROM job_required_skills jrs
           JOIN skills s ON jrs.skill_id = s.id
           WHERE jrs.job_id = %s
           ORDER BY jrs.requirement_type DESC, s.name""",
        (job_id,),
    )
    job_skills = cur.fetchall()

    _must_have = [r["skill_name"] for r in job_skills if r["requirement_type"] == "must_have"]
    _nice_to_have = [r["skill_name"] for r in job_skills if r["requirement_type"] == "nice_to_have"]

    # Match rows for this job
    cur.execute(
        """SELECT m.rank, m.match_score,
                  m.role_similarity_score, m.domain_similarity_score, m.culture_similarity_score,
                  m.skills_match_score, m.compensation_match_score,
                  m.experience_match_score, m.location_match_score,
                  m.matching_skills, m.missing_skills,
                  m.llm_fit_score, m.strengths, m.red_flags,
                  nc.id AS candidate_id, nc.full_name, nc.airtable_record_id AS candidate_partition_id
           FROM matches m
           JOIN normalized_candidates nc ON m.candidate_id = nc.id
           WHERE m.job_id = %s
           ORDER BY m.rank NULLS LAST""",
        (job_id,),
    )
    matches = cur.fetchall()

    # Candidate skills for these candidates
    candidate_ids = [m["candidate_id"] for m in matches]
    cur.execute(
        """SELECT cs.candidate_id, s.name AS skill_name, cs.rating, cs.years_experience
           FROM candidate_skills cs
           JOIN skills s ON cs.skill_id = s.id
           WHERE cs.candidate_id = ANY(%s::uuid[])
           ORDER BY cs.candidate_id, s.name""",
        ([str(cid) for cid in candidate_ids],),
    )
    cand_skill_rows = cur.fetchall()

    cand_skills_by_id: dict = {}
    for r in cand_skill_rows:
        cid = str(r["candidate_id"])
        cand_skills_by_id.setdefault(cid, []).append(r)

    cur.close()
    conn.close()

    # ─── Report ─────────────────────────────────────────────────────
    print("=" * 80)
    print(f"MATCHMAKING SCORE ANALYSIS: {job_title} @ {company}")
    print(f"  Job partition: {partition_id}")
    print("=" * 80)

    print("\n--- JOB REQUIRED SKILLS (from normalized_jobs + job_required_skills) ---")
    if not job_skills:
        print("  (none)  → skill_coverage_score returns 1.0, so skills_match_score = 1.0")
    else:
        for r in job_skills:
            rt = r["requirement_type"]
            name = r["skill_name"] or "—"
            years = f", min_years={r['min_years']}" if r.get("min_years") is not None else ""
            level = f", min_level={r['min_level']}" if r.get("min_level") is not None else ""
            print(f"  • {name}  [{rt}]{years}{level}")
        print("  Must-have weight = 3, nice-to-have weight = 1 in skill_coverage_score.")

    print("\n--- HOW COMBINED SCORE IS COMPUTED ---")
    print(
        "  combined_01 = 0.35*vector + 0.40*skill_fit + 0.10*comp + 0.15*location - seniority_deduction (cap 0.2)"
    )
    print("  vector = 0.40*role_sim + 0.35*domain_sim + 0.25*culture_sim")
    print(
        "  skill_fit = 0.80*skill_coverage + 0.20*skill_semantic (when ≥1 job skill matches candidate); else skill_fit = skill_coverage"
    )

    for m in matches:
        cid = str(m["candidate_id"])
        name = (m["full_name"] or "—")[:40]
        part = m["candidate_partition_id"] or "—"
        print("\n" + "-" * 80)
        print(f"CANDIDATE: {name}  (partition: {part})")
        print("-" * 80)

        rs = m["role_similarity_score"]
        ds = m["domain_similarity_score"]
        cs = m["culture_similarity_score"]
        vec = (0.40 * (rs or 0) + 0.35 * (ds or 0) + 0.25 * (cs or 0)) * 100
        print(
            f"  Vector: role={rs:.4f}, domain={ds:.4f}, culture={cs:.4f}  → weighted 0–100 ≈ {vec:.2f}"
        )

        sf = m["skills_match_score"]
        comp = m["compensation_match_score"]
        exp = m["experience_match_score"]
        loc = m["location_match_score"]
        print(
            f"  Skills fit: {sf:.4f}  |  Compensation: {comp:.4f}  |  Experience: {exp:.4f}  |  Location: {loc:.4f}"
        )

        comb = float(m["match_score"] or 0)
        vector_01 = 0.40 * (rs or 0) + 0.35 * (ds or 0) + 0.25 * (cs or 0)
        contrib_vec = 0.35 * vector_01
        contrib_skill = 0.40 * (sf or 0)
        contrib_comp = 0.10 * (comp or 0)
        contrib_loc = 0.15 * (loc or 0)
        base_sum = contrib_vec + contrib_skill + contrib_comp + contrib_loc
        print("  Combined (before seniority): 0.35*vector + 0.40*skill + 0.10*comp + 0.15*loc")
        print(
            f"    vector_01 = {vector_01:.4f}  →  0.35*{vector_01:.4f} + 0.40*{sf:.4f} + 0.10*{comp:.4f} + 0.15*{loc:.4f}"
        )
        print(
            f"    ≈ {contrib_vec:.4f} + {contrib_skill:.4f} + {contrib_comp:.4f} + {contrib_loc:.4f} = {base_sum:.4f}"
        )
        print(f"  Stored match_score (0–1): {comb:.4f}  → combined % = {comb*100:.2f}")

        matching = m["matching_skills"] or []
        missing = m["missing_skills"] or []
        print(
            f"  Matching skills (job required/nice-to-have that candidate has): {matching if matching else '(none)'}"
        )
        print(
            f"  Missing skills (job required/nice-to-have that candidate lacks): {missing if missing else '(none)'}"
        )
        if job_skills and not matching and not missing:
            print(
                "  → At run time this job had no required skills in the DB, so skill_coverage=1.0 and matching/missing were empty."
            )

        cand_skills = cand_skills_by_id.get(cid, [])
        print(f"  Candidate's skills ({len(cand_skills)}):")
        for sk in sorted(cand_skills, key=lambda x: (x["skill_name"] or "")):
            print(
                f"    • {sk['skill_name']}: rating={sk['rating']}/10, years={sk.get('years_experience')}"
            )

        llm = m.get("llm_fit_score")
        if llm is not None:
            print(f"  LLM fit score: {llm}/10")
        if m.get("red_flags"):
            flags = m["red_flags"]
            s = ", ".join(flags) if isinstance(flags, list) else str(flags)
            print(f"  LLM cons: {s[:250]}{'...' if len(s) > 250 else ''}")

    print("\n" + "=" * 80)


if __name__ == "__main__":
    main()
