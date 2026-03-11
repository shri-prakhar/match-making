"""Tests for matchmaking scoring weights and per-job-category resolution.

Ensures get_weights_for_job_category returns the default weights when category
is missing or unknown, and that default values match config/scoring.py _DEFAULT_WEIGHTS.
"""

from talent_matching.config.scoring import (
    COMPENSATION_WEIGHT,
    CULTURE_WEIGHT,
    DOMAIN_WEIGHT,
    LOCATION_WEIGHT,
    ROLE_WEIGHT,
    SKILL_FIT_WEIGHT,
    SKILL_RATING_WEIGHT,
    SKILL_SEMANTIC_WEIGHT,
    VECTOR_WEIGHT,
    ScoringWeights,
    default_weights_dict,
    get_weights_for_job_category,
)


class TestGetWeightsForJobCategory:
    """get_weights_for_job_category returns default for missing/unknown category."""

    def test_none_returns_default(self):
        w = get_weights_for_job_category(None)
        assert isinstance(w, ScoringWeights)
        assert w.role_weight == 0.35
        assert w.vector_weight == 0.28
        assert w.seniority_max_deduction == 0.2
        assert w.impact_weight == 0.15
        assert w.seniority_scale_weight == 0.05
        assert w.seniority_level_max_deduction == 0.1
        assert w.tenure_instability_max_deduction == 0.1

    def test_empty_string_returns_default(self):
        w = get_weights_for_job_category("")
        assert w.role_weight == 0.35
        assert w.skill_fit_weight == 0.42

    def test_whitespace_only_returns_default(self):
        w = get_weights_for_job_category("   ")
        assert w.domain_weight == 0.30
        assert w.culture_weight == 0.20

    def test_unknown_category_returns_default(self):
        w = get_weights_for_job_category("UnknownCategory")
        assert w.compensation_weight == 0.10
        assert w.location_weight == 0.15
        assert w.skill_rating_weight == 0.75
        assert w.skill_semantic_weight == 0.25

    def test_default_weights_sum_vector_sub_weights_to_one(self):
        w = get_weights_for_job_category(None)
        vector_sub = (
            w.role_weight
            + w.domain_weight
            + w.culture_weight
            + w.impact_weight
            + w.technical_weight
        )
        assert abs(vector_sub - 1.0) < 1e-9

    def test_default_weights_sum_top_level_to_one(self):
        w = get_weights_for_job_category(None)
        total = (
            w.vector_weight
            + w.skill_fit_weight
            + w.compensation_weight
            + w.location_weight
            + w.seniority_scale_weight
        )
        assert abs(total - 1.0) < 1e-9

    def test_default_weights_skill_fit_sub_weights_sum_to_one(self):
        w = get_weights_for_job_category(None)
        assert abs((w.skill_rating_weight + w.skill_semantic_weight) - 1.0) < 1e-9


class TestDefaultWeightsDict:
    """default_weights_dict() returns dict suitable for DB record creation."""

    def test_returns_all_weight_keys(self):
        d = default_weights_dict()
        assert "role_weight" in d
        assert "seniority_max_deduction" in d
        assert "impact_weight" in d
        assert "seniority_scale_weight" in d
        assert "seniority_level_max_deduction" in d
        assert "tenure_instability_max_deduction" in d
        assert len(d) == 15

    def test_values_match_default_weights(self):
        d = default_weights_dict()
        w = get_weights_for_job_category(None)
        assert d["role_weight"] == w.role_weight
        assert d["seniority_max_deduction"] == w.seniority_max_deduction


class TestLegacyExportsMatchDefault:
    """Legacy constant exports equal default ScoringWeights (backward compatibility)."""

    def test_legacy_exports_equal_default_weights(self):
        w = get_weights_for_job_category(None)
        assert ROLE_WEIGHT == w.role_weight
        assert DOMAIN_WEIGHT == w.domain_weight
        assert CULTURE_WEIGHT == w.culture_weight
        assert VECTOR_WEIGHT == w.vector_weight
        assert SKILL_FIT_WEIGHT == w.skill_fit_weight
        assert COMPENSATION_WEIGHT == w.compensation_weight
        assert LOCATION_WEIGHT == w.location_weight
        assert SKILL_RATING_WEIGHT == w.skill_rating_weight
        assert SKILL_SEMANTIC_WEIGHT == w.skill_semantic_weight
