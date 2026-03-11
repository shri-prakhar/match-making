"""Centralized scoring weights for matchmaking.

Used by the matches asset, analysis scripts, and reporting tools.
Weights can vary by job category; default values preserve current behavior.
This structure is intended for future ML tuning (same fields, values updated by ML pipeline).
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class ScoringWeights:
    """Tunable matchmaking parameters. One set per job category; default = current global behavior."""

    # Vector similarity sub-weights (role, domain, culture, impact, optional technical)
    role_weight: float
    domain_weight: float
    culture_weight: float
    impact_weight: float
    technical_weight: float
    # Combined score blend: vector + skill_fit + comp + location + seniority_scale_fit − deductions
    vector_weight: float
    skill_fit_weight: float
    compensation_weight: float
    location_weight: float
    seniority_scale_weight: float
    # Skill fit: when at least one required skill matches, rating-based vs semantic
    skill_rating_weight: float
    skill_semantic_weight: float
    # Seniority: max deductions applied to combined score (0–1 each)
    seniority_max_deduction: float
    seniority_level_max_deduction: float
    tenure_instability_max_deduction: float


# Default weights (aligned with seed script _BASE). Vector sub-weights sum to 1.0.
# Top-level: slightly skill-heavy; skill-fit sub: more semantic for near-match skills.
_DEFAULT_WEIGHTS = ScoringWeights(
    role_weight=0.35,
    domain_weight=0.30,
    culture_weight=0.20,
    impact_weight=0.15,
    technical_weight=0.0,
    vector_weight=0.28,
    skill_fit_weight=0.42,
    compensation_weight=0.10,
    location_weight=0.15,
    seniority_scale_weight=0.05,
    skill_rating_weight=0.75,
    skill_semantic_weight=0.25,
    seniority_max_deduction=0.2,
    seniority_level_max_deduction=0.1,
    tenure_instability_max_deduction=0.1,
)

# Category-specific overrides (empty for now; ML or manual tuning can add entries).
_WEIGHTS_BY_CATEGORY: dict[str, ScoringWeights] = {}


def default_weights_dict() -> dict[str, float]:
    """Return default weight values as a dict for creating a DB record or ScoringWeights."""
    w = _DEFAULT_WEIGHTS
    return {
        "role_weight": w.role_weight,
        "domain_weight": w.domain_weight,
        "culture_weight": w.culture_weight,
        "impact_weight": w.impact_weight,
        "technical_weight": w.technical_weight,
        "vector_weight": w.vector_weight,
        "skill_fit_weight": w.skill_fit_weight,
        "compensation_weight": w.compensation_weight,
        "location_weight": w.location_weight,
        "seniority_scale_weight": w.seniority_scale_weight,
        "skill_rating_weight": w.skill_rating_weight,
        "skill_semantic_weight": w.skill_semantic_weight,
        "seniority_max_deduction": w.seniority_max_deduction,
        "seniority_level_max_deduction": w.seniority_level_max_deduction,
        "tenure_instability_max_deduction": w.tenure_instability_max_deduction,
    }


def get_weights_for_job_category(job_category: str | None) -> ScoringWeights:
    """Return scoring weights for the given job category.

    When job_category is missing, empty, or not in the registry, returns the default weights
    (current production behavior). Same structure is used so ML can later tune per category.
    """
    key = (job_category or "").strip()
    if not key:
        return _DEFAULT_WEIGHTS
    return _WEIGHTS_BY_CATEGORY.get(key, _DEFAULT_WEIGHTS)


# Legacy exports: keep backward compatibility for code that still imports constants.
# Prefer get_weights_for_job_category(job.get("job_category")) in new code.
ROLE_WEIGHT = _DEFAULT_WEIGHTS.role_weight
DOMAIN_WEIGHT = _DEFAULT_WEIGHTS.domain_weight
CULTURE_WEIGHT = _DEFAULT_WEIGHTS.culture_weight
VECTOR_WEIGHT = _DEFAULT_WEIGHTS.vector_weight
SKILL_FIT_WEIGHT = _DEFAULT_WEIGHTS.skill_fit_weight
COMPENSATION_WEIGHT = _DEFAULT_WEIGHTS.compensation_weight
LOCATION_WEIGHT = _DEFAULT_WEIGHTS.location_weight
SKILL_RATING_WEIGHT = _DEFAULT_WEIGHTS.skill_rating_weight
SKILL_SEMANTIC_WEIGHT = _DEFAULT_WEIGHTS.skill_semantic_weight
