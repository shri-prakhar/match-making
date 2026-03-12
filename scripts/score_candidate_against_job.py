#!/usr/bin/env python3
"""Score one candidate against one job and explain why they were or weren't included.

Uses the same location prefilter, job-category filter, skill threshold, and scoring
as the matches asset. Prints pass/fail for each stage and the candidate's score/rank.

Usage:
    poetry run with-remote-db python scripts/score_candidate_against_job.py <job_partition_id> <candidate_partition_id>
    poetry run with-remote-db python scripts/score_candidate_against_job.py recnuhHToY0I7s8wy recDlDQ9iPdfF30v3
    On server: poetry run python scripts/score_candidate_against_job.py --local <job_id> <candidate_id>
"""

import os
import sys
from uuid import UUID

import numpy as np
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv()

from talent_matching.script_env import apply_local_db
from talent_matching.config.scoring import get_weights_for_job_category
from talent_matching.matchmaking.location_filter import (
    parse_job_preferred_locations,
    candidate_passes_location_or_timezone,
)
from talent_matching.matchmaking.scoring import (
    compensation_fit,
    cosine_similarity,
    location_score,
    skill_coverage_score,
    skill_semantic_score,
    seniority_penalty_and_experience_score,
    candidate_seniority_scale,
    job_required_seniority_scale,
    seniority_scale_fit,
    seniority_level_penalty,
    tenure_instability_penalty,
    job_is_high_stakes,
)
from talent_matching.resources.matchmaking import MatchmakingResource

# Align with matches asset
SKILL_MIN_THRESHOLD = 0.30
TOP_N_PER_JOB = 30


def get_connection():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", 5432)),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"],
    )


def load_job(conn, job_partition_id: str) -> dict | None:
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        """SELECT id, raw_job_id, airtable_record_id, job_title, company_name, job_category,
                  salary_min, salary_max, min_years_experience, max_years_experience,
                  timezone_requirements, location_type, seniority_level
           FROM normalized_jobs WHERE airtable_record_id = %s""",
        (job_partition_id,),
    )
    row = cur.fetchone()
    cur.close()
    return dict(row) if row else None


def load_raw_job_location(conn, raw_job_id: str) -> str | None:
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        "SELECT location_raw FROM raw_jobs WHERE id = %s",
        (raw_job_id,),
    )
    row = cur.fetchone()
    cur.close()
    return row["location_raw"] if row else None


def load_job_vectors(conn, raw_job_id: str) -> dict[str, list[float]]:
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        "SELECT vector_type, vector FROM job_vectors WHERE job_id = %s",
        (raw_job_id,),
    )
    rows = cur.fetchall()
    cur.close()
    out = {}
    for r in rows:
        vt = r.get("vector_type")
        vec = r.get("vector")
        if vt and vec is not None:
            out[vt] = list(vec) if not isinstance(vec, list) else vec
    return out


def load_candidate(conn, candidate_partition_id: str) -> dict | None:
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        """SELECT id, raw_candidate_id, airtable_record_id, full_name,
                  desired_job_categories, job_status, skills_summary,
                  years_of_experience, compensation_min, compensation_max, timezone,
                  location_city, location_country, location_region,
                  seniority_level, average_tenure_months, job_count,
                  normalized_json, leadership_score, technical_depth_score
           FROM normalized_candidates WHERE airtable_record_id = %s""",
        (candidate_partition_id,),
    )
    row = cur.fetchone()
    cur.close()
    return dict(row) if row else None


def load_candidate_vectors(conn, raw_candidate_id: str) -> dict[str, list[float]]:
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        "SELECT vector_type, vector FROM candidate_vectors WHERE candidate_id = %s",
        (raw_candidate_id,),
    )
    rows = cur.fetchall()
    cur.close()
    out = {}
    for r in rows:
        vt = r.get("vector_type")
        vec = r.get("vector")
        if vt and vec is not None:
            out[vt] = list(vec) if not isinstance(vec, list) else vec
    return out


def load_stored_matches(conn, job_id: str) -> list[dict]:
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        """SELECT m.rank, m.match_score, nc.full_name, nc.airtable_record_id
           FROM matches m
           JOIN normalized_candidates nc ON m.candidate_id = nc.id
           WHERE m.job_id = %s
           ORDER BY m.rank NULLS LAST""",
        (job_id,),
    )
    rows = cur.fetchall()
    cur.close()
    return [dict(r) for r in rows]


def main() -> int:
    apply_local_db()
    if len(sys.argv) < 3:
        print(
            "Usage: score_candidate_against_job.py <job_partition_id> <candidate_partition_id>",
            file=sys.stderr,
        )
        return 1
    job_partition_id = sys.argv[1]
    candidate_partition_id = sys.argv[2]

    conn = get_connection()
    matchmaking = MatchmakingResource()

    job = load_job(conn, job_partition_id)
    if not job:
        print(f"No normalized job for partition {job_partition_id}.", file=sys.stderr)
        conn.close()
        return 1
    job_id = str(job["id"])
    raw_job_id = str(job["raw_job_id"])

    candidate = load_candidate(conn, candidate_partition_id)
    if not candidate:
        print(f"No normalized candidate for partition {candidate_partition_id}.", file=sys.stderr)
        conn.close()
        return 1
    cand_id = str(candidate["id"])
    raw_cand_id = str(candidate["raw_candidate_id"])

    # Job location for prefilter
    location_raw = load_raw_job_location(conn, raw_job_id)
    job_locations = parse_job_preferred_locations(location_raw)
    job_timezone = job.get("timezone_requirements")

    # Build candidate dict as expected by location filter (keys like location_region, etc.)
    cand_for_filter = {
        "location_city": candidate.get("location_city"),
        "location_country": candidate.get("location_country"),
        "location_region": candidate.get("location_region"),
        "timezone": candidate.get("timezone"),
    }

    print("=" * 70)
    print(f"  Score candidate vs job: {candidate.get('full_name')} vs {job.get('job_title')} @ {job.get('company_name')}")
    print("=" * 70)
    print(f"  Job partition:    {job_partition_id}")
    print(f"  Job location:     {location_raw or 'None'}")
    print(f"  Candidate:        {candidate_partition_id}  ({candidate.get('full_name')})")
    print(f"  Candidate loc:    {candidate.get('location_city')}, {candidate.get('location_country')} ({candidate.get('location_region')})")
    print()

    # 1. Location prefilter
    print("--- 1. Location prefilter ---")
    if job_locations is None:
        print("  PASS (no job location filter)")
    else:
        passes = candidate_passes_location_or_timezone(cand_for_filter, job_locations, job_timezone)
        if passes:
            print(f"  PASS (candidate matches location or timezone for {job_locations})")
        else:
            print(f"  FAIL: Candidate does not pass location/timezone for job locations {job_locations}.")
            print("  This is why they were excluded: they never entered the scoring pool.")
            conn.close()
            return 0
    print()

    # 2. Job category in desired_job_categories
    job_category = (job.get("job_category") or "").strip()
    desired = candidate.get("desired_job_categories") or []
    desired_normalized = {(c or "").strip().lower() for c in desired if (c or "").strip()}
    print("--- 2. Job category filter ---")
    if not job_category:
        print("  SKIP (job has no job_category)")
    elif not desired_normalized:
        print(f"  FAIL: Candidate has no desired_job_categories. Job category is '{job_category}'.")
        print("  This is why they were excluded.")
        conn.close()
        return 0
    elif job_category.lower() not in desired_normalized:
        print(f"  FAIL: Job category '{job_category}' not in candidate's desired_job_categories: {list(desired_normalized)}.")
        print("  This is why they were excluded.")
        conn.close()
        return 0
    else:
        print(f"  PASS (job category '{job_category}' in desired)")
    print()

    # 3. Load vectors and required skills
    jvecs = load_job_vectors(conn, raw_job_id)
    cvecs = load_candidate_vectors(conn, raw_cand_id)
    job_role_vec = jvecs.get("role_description")
    job_domain_vec = jvecs.get("domain")
    job_personality_vec = jvecs.get("personality")

    req_skills = matchmaking.get_job_required_skills([job_id]).get(job_id, [])
    cand_skills_list = matchmaking.get_candidate_skills([cand_id]).get(cand_id, [])
    must_have = [s["skill_name"] for s in req_skills if s.get("requirement_type") == "must_have"]
    nice_to_have = [s["skill_name"] for s in req_skills if s.get("requirement_type") == "nice_to_have"]
    req_skills_with_min_years = [
        (s["skill_name"], int(s["min_years"]), s.get("requirement_type") or "must_have")
        for s in req_skills
        if s.get("min_years") is not None
    ]
    weights = get_weights_for_job_category(job.get("job_category"))

    cand_skills_map = {}
    for cs in cand_skills_list:
        name = (cs.get("skill_name") or "").strip()
        if name:
            cand_skills_map[name] = (
                (cs.get("rating") or 5) / 10.0,
                cs.get("years_experience"),
            )
    candidate_skill_names = set(cand_skills_map.keys())
    matching = [s for s in must_have + nice_to_have if s in candidate_skill_names]
    missing_must = [s for s in must_have if s not in candidate_skill_names]
    missing_nice = [s for s in nice_to_have if s not in candidate_skill_names]

    # Convert cvecs to dict of lists for scoring (skill_coverage_score expects lists)
    cvecs_lists = {k: (v.tolist() if hasattr(v, "tolist") else list(v)) for k, v in cvecs.items()}
    jvecs_lists = {k: (v.tolist() if isinstance(v, np.ndarray) else v) for k, v in jvecs.items()}

    # Role similarity: max over position_* or experience
    position_keys = [k for k in cvecs if k.startswith("position_")]
    if job_role_vec is not None and position_keys:
        role_sim = max(
            cosine_similarity(job_role_vec, cvecs[k]) for k in position_keys
        )
    elif job_role_vec is not None and cvecs.get("experience") is not None:
        role_sim = cosine_similarity(job_role_vec, cvecs["experience"])
    else:
        role_sim = 0.0
    domain_sim = (
        cosine_similarity(job_domain_vec, cvecs.get("domain"))
        if job_domain_vec is not None and cvecs.get("domain") is not None
        else 0.0
    )
    culture_sim = (
        cosine_similarity(job_personality_vec, cvecs.get("personality"))
        if job_personality_vec is not None and cvecs.get("personality") is not None
        else 0.0
    )
    job_impact_vec = jvecs.get("impact")
    job_technical_vec = jvecs.get("technical")
    impact_sim = (
        cosine_similarity(job_impact_vec, cvecs.get("impact"))
        if job_impact_vec is not None and cvecs.get("impact") is not None
        else 0.0
    )
    technical_sim = (
        cosine_similarity(job_technical_vec, cvecs.get("technical"))
        if job_technical_vec is not None and cvecs.get("technical") is not None
        else 0.0
    )
    vector_score = (
        weights.role_weight * role_sim
        + weights.domain_weight * domain_sim
        + weights.culture_weight * culture_sim
        + weights.impact_weight * impact_sim
        + weights.technical_weight * technical_sim
    )

    cand_scale = candidate_seniority_scale(candidate)
    job_req_scale = job_required_seniority_scale(job)
    scale_fit = seniority_scale_fit(cand_scale, job_req_scale)
    level_deduction = seniority_level_penalty(
        job.get("seniority_level"),
        candidate.get("seniority_level"),
        weights.seniority_level_max_deduction,
    )
    tenure_deduction_raw = tenure_instability_penalty(candidate, job_is_high_stakes(job))
    tenure_deduction = min(weights.tenure_instability_max_deduction, tenure_deduction_raw)

    skill_coverage = skill_coverage_score(req_skills, cand_skills_map, jvecs_lists, cvecs_lists)
    skill_semantic = skill_semantic_score(
        job_role_vec, cvecs_lists, req_skills=req_skills, job_skill_vecs=jvecs_lists
    )
    if matching:
        skill_fit_score = (
            weights.skill_rating_weight * skill_coverage
            + weights.skill_semantic_weight * skill_semantic
        )
    else:
        skill_fit_score = skill_coverage

    job_min_years = job.get("min_years_experience")
    if job_min_years is not None and not isinstance(job_min_years, int):
        job_min_years = int(job_min_years) if job_min_years else None
    cand_years = candidate.get("years_of_experience")
    if cand_years is not None and not isinstance(cand_years, int):
        cand_years = int(cand_years) if cand_years else None
    seniority_penalty, experience_match_score = seniority_penalty_and_experience_score(
        job_min_years,
        job.get("max_years_experience"),
        cand_years,
        req_skills_with_min_years,
        cand_skills_map,
    )

    job_salary_min = job.get("salary_min")
    job_salary_max = job.get("salary_max")
    comp_min = candidate.get("compensation_min")
    comp_max = candidate.get("compensation_max")
    if job_salary_min is not None and not isinstance(job_salary_min, (int, float)):
        job_salary_min = float(job_salary_min)
    if job_salary_max is not None and not isinstance(job_salary_max, (int, float)):
        job_salary_max = float(job_salary_max)
    if comp_min is not None and not isinstance(comp_min, (int, float)):
        comp_min = float(comp_min)
    if comp_max is not None and not isinstance(comp_max, (int, float)):
        comp_max = float(comp_max)
    compensation_match_score = compensation_fit(
        job_salary_min, job_salary_max, comp_min, comp_max
    )
    location_match_score = location_score(
        candidate.get("timezone"),
        job_timezone,
        job.get("location_type"),
    )

    print("--- 3. Skill threshold ---")
    if skill_fit_score < SKILL_MIN_THRESHOLD:
        print(f"  FAIL: skill_fit_score {skill_fit_score:.4f} < SKILL_MIN_THRESHOLD ({SKILL_MIN_THRESHOLD}).")
        print("  This is why they were excluded (below minimum skill fit).")
        print(f"  Matching skills: {matching}")
        print(f"  Missing must-have: {missing_must}")
        print(f"  Missing nice-to-have: {missing_nice}")
        conn.close()
        return 0
    print(f"  PASS (skill_fit_score {skill_fit_score:.4f} >= {SKILL_MIN_THRESHOLD})")
    print()

    # Combined score (same formula as matches asset)
    base = (
        weights.vector_weight * vector_score
        + weights.skill_fit_weight * skill_fit_score
        + weights.compensation_weight * compensation_match_score
        + weights.location_weight * location_match_score
        + weights.seniority_scale_weight * scale_fit
    )
    years_deduction = min(weights.seniority_max_deduction, seniority_penalty / 100.0)
    seniority_deduction = years_deduction + level_deduction + tenure_deduction
    combined_01 = max(0.0, min(1.0, base - seniority_deduction))
    combined_100 = round(combined_01 * 100.0, 2)

    print("--- 4. Scoring breakdown ---")
    print(f"  Vector (raw):     {vector_score:.4f}  (Role: {role_sim:.4f}  Domain: {domain_sim:.4f}  Culture: {culture_sim:.4f}  Impact: {impact_sim:.4f}  Technical: {technical_sim:.4f})")
    print(f"  Skill fit:        {skill_fit_score:.4f}")
    print(f"  Compensation:     {compensation_match_score:.4f}")
    print(f"  Experience:       {experience_match_score:.4f}")
    print(f"  Location:         {location_match_score:.4f}")
    print(f"  Seniority scale fit: {scale_fit:.4f}  (cand scale: {cand_scale:.4f}, job req: {job_req_scale if job_req_scale is not None else 'N/A'})")
    print(f"  Seniority penalty (years): -{seniority_penalty:.1f} (cap: -{weights.seniority_max_deduction})")
    print(f"  Level deduction:  -{level_deduction:.4f}   Tenure deduction: -{tenure_deduction:.4f}")
    print(f"  Combined score:   {combined_100:.2f} (0-100) / {combined_01:.4f} (0-1)")
    print(f"  Matching skills: {matching}")
    print(f"  Missing:         {missing_must + missing_nice}")
    print()

    # Compare to stored matches
    stored = load_stored_matches(conn, job_id)
    conn.close()

    print("--- 5. Rank vs stored matches ---")
    if not stored:
        print("  No stored matches for this job. Candidate would be rank 1.")
        return 0
    # Where would they rank? (stored are already sorted by rank; we compare by score)
    their_score = combined_01
    rank = 1
    for m in stored:
        if m["airtable_record_id"] == candidate_partition_id:
            print(f"  Candidate IS in stored matches at rank {m['rank']} (match_score {m['match_score']}).")
            return 0
        if (m["match_score"] or 0) > their_score:
            rank += 1
    if rank > TOP_N_PER_JOB:
        print(f"  Candidate is NOT in top {TOP_N_PER_JOB}. Their score {combined_100:.2f} would place them below rank {TOP_N_PER_JOB}.")
        print(f"  Stored match rank 1 score: {float(stored[0]['match_score'] or 0) * 100:.2f}")
        print(f"  Stored match rank {len(stored)} score: {float(stored[-1]['match_score'] or 0) * 100:.2f}")
    else:
        print(f"  Candidate would be rank ~{rank} (score {combined_100:.2f}).")
        print(f"  Stored top 3: " + ", ".join(f"#{m['rank']} {m['full_name']} ({float(m['match_score'] or 0)*100:.1f})" for m in stored[:3]))
    print("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main())
