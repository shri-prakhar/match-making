"""Shared matchmaking scoring helpers.

Used by the matches asset (talent_matching.assets.jobs) and by
scripts/run_matchmaking_scoring.py so scoring logic stays in one place.
"""

import math
from typing import Any

import numpy as np

from talent_matching.skills.resolver import skill_vector_key

# Combined score = weighted blend (35% vector, 40% skill fit, 10% comp, 15% location) − seniority deduction
SENIORITY_PENALTY_PER_YEAR = 2
SENIORITY_PENALTY_PER_SKILL_YEAR = 1
SENIORITY_PENALTY_CAP = 10
SENIORITY_MAX_DEDUCTION = 0.2
SEMANTIC_PARTIAL_CREDIT_CAP = 0.5


def cosine_similarity_batch(query: np.ndarray, matrix: np.ndarray) -> np.ndarray:
    """Batch cosine similarity: query (D,) vs matrix (N, D) -> (N,) similarities in [0, 1].

    Uses vectorized NumPy for 10-50x speedup over per-row Python loops.
    """
    if matrix.size == 0:
        return np.array([], dtype=np.float64)
    q = np.asarray(query, dtype=np.float64).ravel()
    m = np.asarray(matrix, dtype=np.float64)
    if m.ndim == 1:
        m = m.reshape(1, -1)
    if q.shape[0] != m.shape[1]:
        return np.zeros(m.shape[0], dtype=np.float64)
    q_norm = np.linalg.norm(q)
    if q_norm == 0:
        return np.zeros(m.shape[0], dtype=np.float64)
    m_norms = np.linalg.norm(m, axis=1)
    m_norms[m_norms == 0] = 1.0
    dots = np.dot(m, q)
    sims = dots / (m_norms * q_norm)
    return np.clip(sims, 0.0, 1.0)


def cosine_similarity(a: list[float], b: list[float]) -> float:
    """Cosine similarity in [0, 1]. Returns 0 if either vector is zero."""
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(x * x for x in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    sim = dot / (norm_a * norm_b)
    return max(0.0, min(1.0, sim))


def compensation_fit(
    job_salary_min: float | None,
    job_salary_max: float | None,
    cand_comp_min: float | None,
    cand_comp_max: float | None,
) -> float:
    """Overlap of job pay band with candidate expectations; 0-1. Neutral 0.5 if missing."""
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
    job_range = job_salary_max - job_salary_min
    overlap = (overlap_end - overlap_start) / job_range
    return min(1.0, overlap)


def location_score(
    candidate_timezone: str | None,
    job_timezone_requirements: str | None,
    job_location_type: str | None,
) -> float:
    """Timezone overlap 0-1; weighted by location_type (remote=1, hybrid=0.7, onsite=0.5). Neutral 0.5 if missing."""
    weight = 1.0
    if job_location_type:
        lt = (job_location_type or "").strip().lower()
        if lt == "remote":
            weight = 1.0
        elif lt == "hybrid":
            weight = 0.7
        else:
            weight = 0.5
    if not candidate_timezone and not job_timezone_requirements:
        return 0.5
    if not candidate_timezone or not job_timezone_requirements:
        return 0.5 * weight

    def parse_tz(s: str) -> float | None:
        """Parse a timezone string to a UTC offset in hours.

        Handles UTC offset format ("UTC-5", "UTC+05:30", "GMT+1") and
        IANA timezone names ("America/New_York", "Asia/Kolkata").
        """
        stripped = s.strip()
        if not stripped or stripped.lower() == "null":
            return None
        upper = stripped.upper().replace(" ", "")
        if upper.startswith(("UTC", "GMT")):
            prefix_len = 3
            rest = upper[prefix_len:].strip()
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
    lo: float = j_lo if j_lo is not None else (j_hi or 0.0)
    hi: float = j_hi if j_hi is not None else (j_lo or 0.0)
    if lo > hi:
        lo, hi = hi, lo
    if lo <= c_tz <= hi:
        return 1.0 * weight
    diff = min(abs(c_tz - lo), abs(c_tz - hi))
    overlap = max(0.0, 1.0 - diff / 12.0)
    return overlap * weight


def skill_coverage_score(
    req_skills: list[dict[str, Any]],
    cand_skills_map: dict[str, tuple[float, int | None]],
    job_skill_vecs: dict[str, list[float]] | None = None,
    cand_skill_vecs: dict[str, list[float]] | None = None,
) -> float:
    """0-1: how well candidate skills cover job required skills (name + proficiency).

    Must-have skills have 3x weight of nice-to-have. Missing skill contributes 0
    for that component (no separate flat penalty).

    When a job specifies min_level for a skill, candidates below that threshold
    receive a proportional penalty (rating / required_level). Candidates at or
    above the threshold get full credit.

    When a required skill has no exact canonical match in the candidate's profile
    but both job and candidate skill vectors are available, the most similar
    candidate skill vector is found and partial credit is granted, capped by
    SEMANTIC_PARTIAL_CREDIT_CAP.
    """
    if not req_skills:
        return 1.0
    total_weight = 0.0
    scored = 0.0

    cand_vec_keys = (
        [k for k in cand_skill_vecs if k.startswith("skill_")] if cand_skill_vecs else []
    )

    for s in req_skills:
        name = (s.get("skill_name") or "").strip()
        if not name:
            continue
        req_type = s.get("requirement_type") or "must_have"
        w = 3.0 if req_type == "must_have" else 1.0
        total_weight += w
        rating, _years = cand_skills_map.get(name, (0.0, None))

        if rating == 0.0 and job_skill_vecs and cand_skill_vecs and cand_vec_keys:
            job_vec = job_skill_vecs.get(skill_vector_key(name))
            if job_vec:
                max_sim = max(cosine_similarity(job_vec, cand_skill_vecs[k]) for k in cand_vec_keys)
                rating = max_sim * SEMANTIC_PARTIAL_CREDIT_CAP

        level_factor = 1.0
        min_level = s.get("min_level")
        if min_level is not None and rating > 0:
            min_level_norm = min_level / 10.0
            if rating < min_level_norm:
                level_factor = rating / min_level_norm

        scored += rating * w * level_factor
    if total_weight == 0:
        return 1.0
    return min(1.0, scored / total_weight)


def skill_semantic_score(
    job_role_vec: list[float] | None,
    cand_skill_vecs: dict[str, list[float]],
    req_skills: list[dict[str, Any]] | None = None,
    job_skill_vecs: dict[str, list[float]] | None = None,
) -> float:
    """0-1: per-skill job expected_capability vs candidate skill_* when both exist; else role vs max cand skill."""
    if req_skills and job_skill_vecs:
        total_weight = 0.0
        weighted_sim = 0.0
        for s in req_skills:
            name = (s.get("skill_name") or "").strip()
            if not name:
                continue
            key = skill_vector_key(name)
            job_vec = job_skill_vecs.get(key)
            cand_vec = cand_skill_vecs.get(key)
            if job_vec and cand_vec:
                w = 3.0 if (s.get("requirement_type") or "must_have") == "must_have" else 1.0
                total_weight += w
                weighted_sim += cosine_similarity(job_vec, cand_vec) * w
        if total_weight > 0:
            return weighted_sim / total_weight
    # Fallback: job role_description vs max similarity to candidate skill_* vectors
    if not job_role_vec:
        return 0.5
    skill_keys = [k for k in cand_skill_vecs if k.startswith("skill_")]
    if not skill_keys:
        return 0.0
    sims = [cosine_similarity(job_role_vec, cand_skill_vecs[k]) for k in skill_keys]
    return max(sims) if sims else 0.0


def seniority_penalty_and_experience_score(
    job_min_years: int | None,
    job_max_years: int | None,
    cand_years: int | None,
    req_skills_with_min_years: list[tuple[str, int, str]],
    cand_skills_map: dict[str, tuple[float, int | None]],
) -> tuple[float, float]:
    """Returns (penalty_points, experience_match_score 0-1)."""
    penalty = 0.0
    if job_min_years is not None and cand_years is not None and cand_years < job_min_years:
        short = job_min_years - cand_years
        penalty += min(SENIORITY_PENALTY_CAP, short * SENIORITY_PENALTY_PER_YEAR)
    for skill_name, min_years, _req_type in req_skills_with_min_years:
        _, cand_y = cand_skills_map.get(skill_name, (0.0, None))
        if cand_y is not None and min_years is not None and cand_y < min_years:
            penalty += (min_years - cand_y) * SENIORITY_PENALTY_PER_SKILL_YEAR
    max_penalty = SENIORITY_PENALTY_CAP + 5 * SENIORITY_PENALTY_PER_SKILL_YEAR
    experience_match_score = max(0.0, 1.0 - penalty / max_penalty) if max_penalty else 1.0
    return penalty, experience_match_score
