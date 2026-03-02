#!/usr/bin/env python3
"""Run matchmaking scoring on existing normalized jobs and candidates; print results.

Uses the same logic as the matches asset: vector (role/domain/culture, raw no rescaling),
skill_fit (80% rating coverage, 20% semantic only when a skill matches), seniority penalty,
compensation, location; top 20 per job. Prints full breakdown once per candidate.

Usage:
    uv run python scripts/run_matchmaking_scoring.py

Requires:
    - Normalized jobs and candidates in PostgreSQL
    - Job vectors and candidate vectors in pgvector (run job_vectors and candidate_vectors assets first)
"""

import math
import os
import sys
from typing import Any

import psycopg2
from dotenv import load_dotenv
from pgvector.psycopg2 import register_vector
from psycopg2.extras import RealDictCursor

# Add project root for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv()

from talent_matching.resources.matchmaking import MatchmakingResource  # noqa: E402

ROLE_WEIGHT = 0.4
DOMAIN_WEIGHT = 0.35
CULTURE_WEIGHT = 0.25
TOP_N_PER_JOB = 20
# Combined = 35% vector + 35% skill fit + 10% comp + 20% location − seniority deduction
VECTOR_WEIGHT = 0.35
SKILL_FIT_WEIGHT = 0.35
COMPENSATION_WEIGHT = 0.10
LOCATION_WEIGHT = 0.20
# When at least one skill matches: 80% rating-based coverage, 20% semantic (tie-breaker)
SKILL_RATING_WEIGHT = 0.8
SKILL_SEMANTIC_WEIGHT = 0.2
SENIORITY_PENALTY_PER_YEAR = 2
SENIORITY_PENALTY_PER_SKILL_YEAR = 1
SENIORITY_PENALTY_CAP = 10
SENIORITY_MAX_DEDUCTION = 0.2
SEMANTIC_PARTIAL_CREDIT_CAP = 0.5


def _cosine_similarity(a: list[float], b: list[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(x * x for x in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    sim = dot / (norm_a * norm_b)
    return max(0.0, min(1.0, sim))


def _compensation_fit(job_salary_min, job_salary_max, cand_comp_min, cand_comp_max) -> float:
    if (
        job_salary_min is None
        or job_salary_max is None
        or cand_comp_min is None
        or cand_comp_max is None
    ):
        return 0.5
    if job_salary_max <= job_salary_min:
        return 0.5
    overlap_start = max(job_salary_min, cand_comp_min)
    overlap_end = min(job_salary_max, cand_comp_max)
    if overlap_end <= overlap_start:
        return 0.0
    return min(1.0, (overlap_end - overlap_start) / (job_salary_max - job_salary_min))


def _location_score(
    candidate_timezone: str | None,
    job_timezone_requirements: str | None,
    job_location_type: str | None,
) -> float:
    weight = 1.0
    if job_location_type:
        lt = (job_location_type or "").strip().lower()
        weight = 0.7 if lt == "hybrid" else (0.5 if lt != "remote" else 1.0)
    if not candidate_timezone and not job_timezone_requirements:
        return 0.5
    if not candidate_timezone or not job_timezone_requirements:
        return 0.5 * weight

    def parse_tz(s):
        stripped = str(s).strip()
        if not stripped or stripped.lower() == "null":
            return None
        upper = stripped.upper().replace(" ", "")
        if upper.startswith(("UTC", "GMT")):
            rest = upper[3:].strip()
            if not rest or rest == "0":
                return 0.0
            sign = 1 if rest.startswith("+") else -1
            rest = rest.lstrip("+-")
            if ":" in rest:
                parts = rest.split(":")
                if parts[0].isdigit() and parts[1].isdigit():
                    return sign * (int(parts[0]) + int(parts[1]) / 60)
            num = rest.split("/")[0].split("-")[0].split("+")[0]
            if num.isdigit():
                return sign * float(num)
            return None
        if "/" not in stripped:
            return None
        from datetime import datetime
        from zoneinfo import ZoneInfo

        zi = ZoneInfo(stripped)
        offset = datetime.now(zi).utcoffset()
        if offset is not None:
            return offset.total_seconds() / 3600
        return None

    c_tz = parse_tz(candidate_timezone)
    j_tz_str = (job_timezone_requirements or "").strip()
    if " to " in j_tz_str:
        parts = j_tz_str.split(" to ")
        j_lo = parse_tz(parts[0]) if parts else None
        j_hi = parse_tz(parts[1]) if len(parts) > 1 else None
    else:
        single = parse_tz(j_tz_str)
        j_lo = single
        j_hi = single
    if c_tz is None or (j_lo is None and j_hi is None):
        return 0.5 * weight
    if j_lo is None:
        j_lo = j_hi
    if j_hi is None:
        j_hi = j_lo
    if j_lo > j_hi:
        j_lo, j_hi = j_hi, j_lo
    if j_lo <= c_tz <= j_hi:
        return 1.0 * weight
    diff = min(abs(c_tz - j_lo), abs(c_tz - j_hi))
    return max(0.0, 1.0 - diff / 12.0) * weight


def _skill_coverage_score(
    req_skills: list,
    cand_skills_map: dict,
    job_skill_vecs: dict | None = None,
    cand_skill_vecs: dict | None = None,
) -> float:
    """Must-have weight 3x nice-to-have; missing skill contributes 0 (no flat penalty).

    Semantic partial credit: when a required skill has no exact canonical match,
    the most similar candidate skill vector is found and partial credit is granted.
    """
    if not req_skills:
        return 1.0
    total_weight = scored = 0.0

    cand_vec_keys = (
        [k for k in cand_skill_vecs if k.startswith("skill_")] if cand_skill_vecs else []
    )

    for s in req_skills:
        name = (s.get("skill_name") or "").strip()
        if not name:
            continue
        w = 3.0 if (s.get("requirement_type") or "must_have") == "must_have" else 1.0
        total_weight += w
        rating, _years = cand_skills_map.get(name, (0.0, None))

        if rating == 0.0 and job_skill_vecs and cand_skill_vecs and cand_vec_keys:
            job_vec = job_skill_vecs.get(_skill_key_from_name(name))
            if job_vec:
                max_sim = max(
                    _cosine_similarity(job_vec, cand_skill_vecs[k]) for k in cand_vec_keys
                )
                rating = max_sim * SEMANTIC_PARTIAL_CREDIT_CAP

        level_factor = 1.0
        min_level = s.get("min_level")
        if min_level is not None and rating > 0:
            min_level_norm = min_level / 10.0
            if rating < min_level_norm:
                level_factor = rating / min_level_norm

        scored += rating * w * level_factor
    return min(1.0, scored / total_weight) if total_weight else 1.0


def _skill_key_from_name(skill_name: str) -> str:
    return f"skill_{skill_name.lower().replace(' ', '_').replace('.', '')}"[:150]


def _skill_semantic_score(
    job_role_vec: list | None,
    cand_skill_vecs: dict,
    req_skills: list | None = None,
    job_skill_vecs: dict | None = None,
) -> float:
    """Per-skill job expected_capability vs candidate skill_* when both exist; else role vs max cand skill."""
    if req_skills and job_skill_vecs:
        total_weight = 0.0
        weighted_sim = 0.0
        for s in req_skills:
            name = (s.get("skill_name") or "").strip()
            if not name:
                continue
            key = _skill_key_from_name(name)
            job_vec = job_skill_vecs.get(key)
            cand_vec = cand_skill_vecs.get(key)
            if job_vec and cand_vec:
                w = 3.0 if (s.get("requirement_type") or "must_have") == "must_have" else 1.0
                total_weight += w
                weighted_sim += _cosine_similarity(job_vec, cand_vec) * w
        if total_weight > 0:
            return weighted_sim / total_weight
    if not job_role_vec:
        return 0.5
    skill_keys = [k for k in cand_skill_vecs if k.startswith("skill_")]
    if not skill_keys:
        return 0.0
    sims = [_cosine_similarity(job_role_vec, cand_skill_vecs[k]) for k in skill_keys]
    return max(sims) if sims else 0.0


def _seniority_penalty_and_experience_score(
    job_min_years,
    job_max_years,
    cand_years,
    req_skills_with_min_years: list,
    cand_skills_map: dict,
) -> tuple[float, float]:
    penalty = 0.0
    if job_min_years is not None and cand_years is not None and cand_years < job_min_years:
        short = job_min_years - cand_years
        penalty += min(SENIORITY_PENALTY_CAP, short * SENIORITY_PENALTY_PER_YEAR)
    for skill_name, min_years, _ in req_skills_with_min_years:
        _, cand_y = cand_skills_map.get(skill_name, (0.0, None))
        if cand_y is not None and min_years is not None and cand_y < min_years:
            penalty += (min_years - cand_y) * SENIORITY_PENALTY_PER_SKILL_YEAR
    max_penalty = SENIORITY_PENALTY_CAP + 5 * SENIORITY_PENALTY_PER_SKILL_YEAR
    exp_score = max(0.0, 1.0 - penalty / max_penalty) if max_penalty else 1.0
    return penalty, exp_score


def get_connection():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ.get("POSTGRES_PORT", 5432)),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"],
    )


def load_normalized_jobs(conn) -> list[dict[str, Any]]:
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        """SELECT id, raw_job_id, job_title, job_category, company_name,
           salary_min, salary_max, min_years_experience, max_years_experience,
           location_type, timezone_requirements
           FROM normalized_jobs ORDER BY id LIMIT 50"""
    )
    rows = cur.fetchall()
    cur.close()
    return [dict(r) for r in rows]


def load_normalized_candidates(conn) -> list[dict[str, Any]]:
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        """SELECT id, raw_candidate_id, airtable_record_id, full_name, skills_summary,
           years_of_experience, compensation_min, compensation_max, timezone,
           desired_job_categories
           FROM normalized_candidates ORDER BY id"""
    )
    rows = cur.fetchall()
    cur.close()
    return [dict(r) for r in rows]


def load_job_vectors(conn) -> list[dict[str, Any]]:
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        """SELECT job_id, vector_type, vector FROM job_vectors ORDER BY job_id, vector_type LIMIT 1000"""
    )
    rows = cur.fetchall()
    cur.close()
    out = []
    for r in rows:
        vec = r.get("vector")
        if vec is not None and not isinstance(vec, list):
            vec = list(vec)
        out.append(
            {
                "job_id": str(r["job_id"]),
                "vector_type": r["vector_type"],
                "vector": vec,
            }
        )
    return out


def load_candidate_vectors(conn) -> list[dict[str, Any]]:
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        """SELECT candidate_id, vector_type, vector FROM candidate_vectors ORDER BY candidate_id, vector_type"""
    )
    rows = cur.fetchall()
    cur.close()
    out = []
    for r in rows:
        vec = r.get("vector")
        if vec is not None and not isinstance(vec, list):
            vec = list(vec)
        out.append(
            {
                "candidate_id": str(r["candidate_id"]),
                "vector_type": r["vector_type"],
                "vector": vec,
            }
        )
    return out


def run_scoring(
    normalized_jobs: list[dict],
    normalized_candidates: list[dict],
    job_vectors: list[dict],
    candidate_vectors: list[dict],
    job_required_skills: dict[str, list],
    candidate_skills_map: dict[str, list],
) -> list[dict[str, Any]]:
    job_vecs_by_raw: dict[str, dict[str, list[float]]] = {}
    for rec in job_vectors:
        raw_id = str(rec.get("job_id", ""))
        if not raw_id:
            continue
        if raw_id not in job_vecs_by_raw:
            job_vecs_by_raw[raw_id] = {}
        vt = rec.get("vector_type") or ""
        vec = rec.get("vector")
        if vec is not None and vt:
            job_vecs_by_raw[raw_id][vt] = vec

    cand_vecs_by_raw: dict[str, dict[str, list[float]]] = {}
    for rec in candidate_vectors:
        raw_id = str(rec.get("candidate_id", ""))
        if not raw_id:
            continue
        if raw_id not in cand_vecs_by_raw:
            cand_vecs_by_raw[raw_id] = {}
        vt = rec.get("vector_type") or ""
        vec = rec.get("vector")
        if vec is not None and vt:
            cand_vecs_by_raw[raw_id][vt] = vec

    match_results: list[dict[str, Any]] = []

    for job in normalized_jobs:
        job_id_norm = job.get("id")
        raw_job_id = str(job.get("raw_job_id", ""))
        if not job_id_norm or not raw_job_id:
            continue
        jvecs = job_vecs_by_raw.get(raw_job_id, {})
        job_role_vec = jvecs.get("role_description")
        job_domain_vec = jvecs.get("domain")
        job_personality_vec = jvecs.get("personality")
        req_skills = job_required_skills.get(str(job_id_norm), [])
        must_have = [
            s["skill_name"] for s in req_skills if s.get("requirement_type") == "must_have"
        ]
        nice_to_have = [
            s["skill_name"] for s in req_skills if s.get("requirement_type") == "nice_to_have"
        ]
        req_skills_with_min_years = [
            (s["skill_name"], int(s["min_years"]), s.get("requirement_type") or "must_have")
            for s in req_skills
            if s.get("min_years") is not None
        ]
        job_min_years = job.get("min_years_experience")
        if job_min_years is not None and not isinstance(job_min_years, int):
            job_min_years = int(job_min_years) if job_min_years else None
        _jsmin, _jsmax = job.get("salary_min"), job.get("salary_max")
        job_salary_min = float(_jsmin) if _jsmin is not None else None
        job_salary_max = float(_jsmax) if _jsmax is not None else None
        job_location_type = job.get("location_type")
        job_timezone = job.get("timezone_requirements")
        job_category = (job.get("job_category") or "").strip()

        rows: list[tuple] = []
        for candidate in normalized_candidates:
            cand_id_norm = candidate.get("id")
            raw_cand_id = str(candidate.get("raw_candidate_id", ""))
            if not cand_id_norm or not raw_cand_id:
                continue
            # Strict filter: job category must match one of the candidate's desired job categories
            if job_category:
                desired = candidate.get("desired_job_categories") or []
                desired_normalized = {
                    (c or "").strip().lower() for c in desired if (c or "").strip()
                }
                if not desired_normalized or job_category.lower() not in desired_normalized:
                    continue
            cvecs = cand_vecs_by_raw.get(raw_cand_id, {})

            position_keys = [k for k in cvecs if k.startswith("position_")]
            if job_role_vec and position_keys:
                role_sim = max(_cosine_similarity(job_role_vec, cvecs[k]) for k in position_keys)
            elif job_role_vec and cvecs.get("experience"):
                role_sim = _cosine_similarity(job_role_vec, cvecs["experience"])
            else:
                role_sim = 0.0
            domain_sim = (
                _cosine_similarity(job_domain_vec, cvecs["domain"])
                if job_domain_vec and cvecs.get("domain")
                else 0.0
            )
            culture_sim = (
                _cosine_similarity(job_personality_vec, cvecs["personality"])
                if job_personality_vec and cvecs.get("personality")
                else 0.0
            )
            vector_score = (
                ROLE_WEIGHT * role_sim + DOMAIN_WEIGHT * domain_sim + CULTURE_WEIGHT * culture_sim
            )

            cand_skills_list = candidate_skills_map.get(str(cand_id_norm), [])
            cand_skills_map_for_cand = {}
            for cs in cand_skills_list:
                name = (cs.get("skill_name") or "").strip()
                if name:
                    cand_skills_map_for_cand[name] = (
                        (cs.get("rating") or 5) / 10.0,
                        cs.get("years_experience"),
                    )

            candidate_skill_names = set(cand_skills_map_for_cand.keys())
            missing_must = [s for s in must_have if s not in candidate_skill_names]
            missing_nice = [s for s in nice_to_have if s not in candidate_skill_names]
            matching = [s for s in must_have + nice_to_have if s in candidate_skill_names]

            skill_coverage = _skill_coverage_score(
                req_skills, cand_skills_map_for_cand, jvecs, cvecs
            )
            skill_semantic = _skill_semantic_score(
                job_role_vec, cvecs, req_skills=req_skills, job_skill_vecs=jvecs
            )
            if matching:
                skill_fit_score = (
                    SKILL_RATING_WEIGHT * skill_coverage + SKILL_SEMANTIC_WEIGHT * skill_semantic
                )
            else:
                skill_fit_score = skill_coverage

            cand_years = candidate.get("years_of_experience")
            if cand_years is not None and not isinstance(cand_years, int):
                cand_years = int(cand_years) if cand_years else None
            seniority_penalty, experience_match_score = _seniority_penalty_and_experience_score(
                job_min_years,
                job.get("max_years_experience"),
                cand_years,
                req_skills_with_min_years,
                cand_skills_map_for_cand,
            )

            _cmin, _cmax = candidate.get("compensation_min"), candidate.get("compensation_max")
            comp_min = float(_cmin) if _cmin is not None else None
            comp_max = float(_cmax) if _cmax is not None else None
            compensation_match_score = _compensation_fit(
                job_salary_min, job_salary_max, comp_min, comp_max
            )
            location_match_score = _location_score(
                candidate.get("timezone"), job_timezone, job_location_type
            )

            rows.append(
                (
                    vector_score,
                    role_sim,
                    domain_sim,
                    culture_sim,
                    skill_fit_score,
                    seniority_penalty,
                    compensation_match_score,
                    experience_match_score,
                    location_match_score,
                    matching,
                    missing_must + missing_nice,
                    str(cand_id_norm),
                    (candidate.get("full_name") or "—")[:40],
                    candidate.get("airtable_record_id") or "—",
                )
            )

        scored = []
        for r in rows:
            (
                v_raw,
                role_sim,
                domain_sim,
                culture_sim,
                skill_fit,
                sen_pen,
                comp_score,
                exp_score,
                loc_score,
                matching,
                missing,
                cid,
                name,
                partition_id,
            ) = r
            # Use raw vector score (no per-job min-max rescaling)
            base = (
                VECTOR_WEIGHT * v_raw
                + SKILL_FIT_WEIGHT * skill_fit
                + COMPENSATION_WEIGHT * comp_score
                + LOCATION_WEIGHT * loc_score
            )
            seniority_deduction = min(SENIORITY_MAX_DEDUCTION, sen_pen / 100.0)
            combined_01 = max(0.0, min(1.0, base - seniority_deduction))
            scored.append(
                (
                    combined_01,
                    v_raw,
                    role_sim,
                    domain_sim,
                    culture_sim,
                    skill_fit,
                    comp_score,
                    exp_score,
                    loc_score,
                    sen_pen,
                    matching,
                    missing,
                    cid,
                    name,
                    partition_id,
                )
            )

        scored.sort(key=lambda t: t[0], reverse=True)
        job_title = (job.get("job_title") or "—")[:35]
        company = (job.get("company_name") or "—")[:25]
        for rank, (
            combined_01,
            v_raw,
            role_sim,
            domain_sim,
            culture_sim,
            skill_fit,
            comp_score,
            exp_score,
            loc_score,
            sen_pen,
            matching,
            missing,
            cid,
            name,
            partition_id,
        ) in enumerate(scored[:TOP_N_PER_JOB], start=1):
            match_results.append(
                {
                    "job_id": str(job_id_norm),
                    "job_title": job_title,
                    "company": company,
                    "candidate_id": cid,
                    "candidate_name": name,
                    "candidate_partition_id": partition_id,
                    "rank": rank,
                    "combined_score": round(combined_01 * 100.0, 2),
                    "vector_score_rescaled_100": round(v_raw * 100.0, 2),
                    "match_score_0_1": round(combined_01, 4),
                    "role_sim": round(role_sim, 4),
                    "domain_sim": round(domain_sim, 4),
                    "culture_sim": round(culture_sim, 4),
                    "skills_match_score": round(skill_fit, 4),
                    "compensation_match_score": round(comp_score, 4),
                    "experience_match_score": round(exp_score, 4),
                    "location_match_score": round(loc_score, 4),
                    "seniority_penalty": round(sen_pen, 1),
                    "matching_skills": matching,
                    "missing_skills": missing,
                }
            )

    return match_results


def main():
    print("\n" + "=" * 80)
    print("  MATCHMAKING SCORING TEST (existing normalized jobs & candidates)")
    print("=" * 80)

    conn = get_connection()
    register_vector(conn)

    normalized_jobs = load_normalized_jobs(conn)
    normalized_candidates = load_normalized_candidates(conn)
    print(f"\n  Normalized jobs:     {len(normalized_jobs)}")
    print(f"  Normalized candidates: {len(normalized_candidates)}")

    if not normalized_jobs:
        print("\n  No normalized jobs. Run the job pipeline first (normalize at least one job).")
        conn.close()
        sys.exit(1)
    if not normalized_candidates:
        print("\n  No normalized candidates. Run the candidate pipeline first.")
        conn.close()
        sys.exit(1)

    job_vectors = load_job_vectors(conn)
    candidate_vectors = load_candidate_vectors(conn)
    print(f"  Job vector rows:    {len(job_vectors)}")
    print(f"  Candidate vector rows: {len(candidate_vectors)}")

    if not job_vectors:
        print("\n  No job vectors. Materialize job_vectors asset for your job(s) first.")
    if not candidate_vectors:
        print("\n  No candidate vectors. Materialize candidate_vectors for your candidates first.")

    matchmaking = MatchmakingResource()
    job_ids = [str(j["id"]) for j in normalized_jobs]
    cand_ids = [str(c["id"]) for c in normalized_candidates]
    job_required_skills = matchmaking.get_job_required_skills(job_ids)
    candidate_skills_map = matchmaking.get_candidate_skills(cand_ids)

    matches = run_scoring(
        normalized_jobs,
        normalized_candidates,
        job_vectors,
        candidate_vectors,
        job_required_skills,
        candidate_skills_map,
    )
    conn.close()

    if not matches:
        print(
            "\n  No matches produced (check that job/candidate vectors exist for the same raw IDs)."
        )
        sys.exit(0)

    print("\n" + "-" * 80)
    print("  SCORING RESULTS (top 20 per job)")
    print("-" * 80)
    print(
        "  Formula: 40% vector (raw) + 40% skill fit + 10% compensation + 10% location − seniority deduction (cap 20%)"
    )
    print(
        "  Skill fit: 80% rating-based coverage, 20% semantic (only when at least one skill matches)."
    )
    print("  Inspect a candidate: python scripts/inspect_candidate.py <partition_id>")
    print("-" * 80)

    for m in matches:
        job_title = m["job_title"]
        company = m["company"]
        name = m["candidate_name"]
        partition_id = m["candidate_partition_id"]
        rank = m["rank"]
        combined = m["combined_score"]
        match_0_1 = m["match_score_0_1"]
        vec_100 = m["vector_score_rescaled_100"]
        rs, ds, cs = m["role_sim"], m["domain_sim"], m["culture_sim"]
        skill_fit = m["skills_match_score"]
        comp_score = m["compensation_match_score"]
        exp_score = m["experience_match_score"]
        loc_score = m["location_match_score"]
        sen_pen = m["seniority_penalty"]
        matching = m["matching_skills"]
        missing = m["missing_skills"]
        print(f"\n  Job: {job_title} @ {company}")
        print(f"  Rank {rank}: {name}  (partition: {partition_id})")
        print(f"    Combined score: {combined:.2f}  (stored match_score 0–1: {match_0_1})")
        print(
            f"    Vector (0–100): {vec_100:.2f}  |  Role: {rs:.4f}  Domain: {ds:.4f}  Culture: {cs:.4f}"
        )
        print(
            f"    Skills fit: {skill_fit:.4f}  Compensation: {comp_score:.4f}  Experience: {exp_score:.4f}  Location: {loc_score:.4f}"
        )
        print(f"    Seniority penalty: −{sen_pen:.1f}")
        if matching:
            print(f"    Matching skills: {', '.join(matching)}")
        if missing:
            print(f"    Missing skills:  {', '.join(missing)}")

    print("\n" + "=" * 80)
    print(f"  Total match rows: {len(matches)}")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    main()
