"""Tests for shared matchmaking scoring (talent_matching.matchmaking.scoring)."""

from talent_matching.matchmaking.scoring import (
    SENIORITY_MAX_DEDUCTION,
    candidate_seniority_scale,
    compensation_fit,
    cosine_similarity,
    cosine_similarity_batch,
    job_is_high_stakes,
    job_required_seniority_scale,
    location_score,
    seniority_level_ordinal,
    seniority_level_penalty,
    seniority_penalty_and_experience_score,
    seniority_scale_fit,
    skill_coverage_score,
    skill_semantic_score,
    tenure_instability_penalty,
)


class TestCosineSimilarity:
    """Tests for cosine_similarity."""

    def test_identical_vectors_return_one(self):
        a = [1.0, 0.0, 0.0]
        assert cosine_similarity(a, a) == 1.0

    def test_orthogonal_vectors_return_zero(self):
        a = [1.0, 0.0, 0.0]
        b = [0.0, 1.0, 0.0]
        assert cosine_similarity(a, b) == 0.0

    def test_opposite_vectors_clamped_to_zero(self):
        a = [1.0, 0.0, 0.0]
        b = [-1.0, 0.0, 0.0]
        assert cosine_similarity(a, b) == 0.0

    def test_empty_or_mismatched_return_zero(self):
        assert cosine_similarity([], [1.0]) == 0.0
        assert cosine_similarity([1.0], [1.0, 2.0]) == 0.0

    def test_zero_vector_return_zero(self):
        assert cosine_similarity([0.0, 0.0], [1.0, 1.0]) == 0.0

    def test_result_in_unit_interval(self):
        a = [0.5, 0.5, 0.0]
        b = [0.5, 0.0, 0.5]
        s = cosine_similarity(a, b)
        assert 0.0 <= s <= 1.0


class TestCosineSimilarityBatch:
    """Tests for cosine_similarity_batch (NumPy vectorized)."""

    def test_batch_matches_scalar(self):
        import numpy as np

        query = [1.0, 0.0, 0.0]
        matrix = [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.5, 0.5, 0.0]]
        batch = cosine_similarity_batch(np.array(query), np.array(matrix))
        assert batch.shape == (3,)
        assert batch[0] == cosine_similarity(query, matrix[0])
        assert batch[1] == cosine_similarity(query, matrix[1])
        assert batch[2] == cosine_similarity(query, matrix[2])

    def test_empty_matrix_returns_empty(self):
        import numpy as np

        batch = cosine_similarity_batch(np.array([1.0, 0.0]), np.array([]).reshape(0, 2))
        assert batch.shape == (0,)

    def test_zero_query_returns_zeros(self):
        import numpy as np

        batch = cosine_similarity_batch(np.array([0.0, 0.0]), np.array([[1.0, 0.0], [0.0, 1.0]]))
        assert np.all(batch == 0.0)


class TestCompensationFit:
    """Tests for compensation_fit."""

    def test_full_overlap_returns_one(self):
        assert compensation_fit(100.0, 200.0, 100.0, 200.0) == 1.0

    def test_no_overlap_returns_zero(self):
        assert compensation_fit(100.0, 200.0, 250.0, 300.0) == 0.0

    def test_partial_overlap(self):
        # job 100-200, candidate 150-250 -> overlap 150-200 = 50/100
        assert compensation_fit(100.0, 200.0, 150.0, 250.0) == 0.5

    def test_missing_any_returns_neutral_half(self):
        assert compensation_fit(None, 200.0, 100.0, 200.0) == 0.5
        assert compensation_fit(100.0, None, 100.0, 200.0) == 0.5
        assert compensation_fit(100.0, 200.0, None, 200.0) == 0.5
        assert compensation_fit(100.0, 200.0, 100.0, None) == 0.5

    def test_zero_range_job_returns_neutral(self):
        assert compensation_fit(100.0, 100.0, 100.0, 200.0) == 0.5


class TestLocationScore:
    """Tests for location_score."""

    def test_remote_weight_one(self):
        # No timezone info -> neutral
        s = location_score(None, None, "remote")
        assert s == 0.5

    def test_hybrid_weight_when_no_tz_returns_neutral(self):
        # When both timezones missing, implementation returns neutral 0.5 (weight not applied)
        s = location_score(None, None, "hybrid")
        assert s == 0.5

    def test_onsite_weight_when_no_tz_returns_neutral(self):
        # When both timezones missing, implementation returns neutral 0.5
        s = location_score(None, None, "onsite")
        assert s == 0.5

    def test_utc_offset_in_range(self):
        # Candidate UTC+0, job accepts UTC-1 to UTC+1 -> match
        s = location_score("UTC+0", "UTC-1 to UTC+1", "remote")
        assert s == 1.0

    def test_utc_offset_out_of_range(self):
        # Candidate UTC+10, job UTC-5 to UTC+2 -> far
        s = location_score("UTC+10", "UTC-5 to UTC+2", "remote")
        assert s < 1.0
        assert s >= 0.0

    def test_iana_timezone_parsed(self):
        # Candidate in New York, job accepts US Eastern
        s = location_score("America/New_York", "America/New_York", "remote")
        assert 0.5 <= s <= 1.0


class TestSkillCoverageScore:
    """Tests for skill_coverage_score."""

    def test_no_required_skills_returns_one(self):
        assert skill_coverage_score([], {}) == 1.0

    def test_exact_match_full_rating(self):
        req = [{"skill_name": "Python", "requirement_type": "must_have"}]
        cand = {"Python": (1.0, 5)}
        assert skill_coverage_score(req, cand) == 1.0

    def test_missing_skill_contributes_zero(self):
        req = [{"skill_name": "Python", "requirement_type": "must_have"}]
        cand = {}
        assert skill_coverage_score(req, cand) == 0.0

    def test_must_have_weighed_higher_than_nice_to_have(self):
        req = [
            {"skill_name": "Python", "requirement_type": "must_have"},
            {"skill_name": "Rust", "requirement_type": "nice_to_have"},
        ]
        cand = {"Python": (0.5, None), "Rust": (0.0, None)}
        s = skill_coverage_score(req, cand)
        assert 0.0 < s < 1.0
        # Python 0.5 * 3 + Rust 0 * 1 -> 1.5 / 4 = 0.375
        assert abs(s - 0.375) < 0.01

    def test_min_level_proportional_penalty(self):
        req = [{"skill_name": "Python", "requirement_type": "must_have", "min_level": 8}]
        cand = {"Python": (0.5, None)}  # 0.5 normalized vs 0.8 required
        s = skill_coverage_score(req, cand)
        assert 0.0 < s < 1.0


class TestSkillSemanticScore:
    """Tests for skill_semantic_score."""

    def test_no_job_role_vec_returns_neutral(self):
        assert skill_semantic_score(None, {}, None, None) == 0.5

    def test_no_cand_skill_vecs_returns_zero(self):
        assert skill_semantic_score([0.1] * 10, {}, None, None) == 0.0

    def test_per_skill_match_when_both_vecs(self):
        req = [{"skill_name": "Python", "requirement_type": "must_have"}]
        job_vecs = {"skill_python": [1.0, 0.0]}
        cand_vecs = {"skill_python": [1.0, 0.0]}
        s = skill_semantic_score(None, cand_vecs, req, job_vecs)
        assert s == 1.0

    def test_fallback_role_vs_max_cand_skill(self):
        job_role = [1.0, 0.0, 0.0]
        cand_vecs = {"skill_python": [1.0, 0.0, 0.0]}
        s = skill_semantic_score(job_role, cand_vecs, None, None)
        assert s == 1.0


class TestSeniorityPenaltyAndExperienceScore:
    """Tests for seniority_penalty_and_experience_score."""

    def test_no_penalty_when_candidate_meets_min_years(self):
        penalty, score = seniority_penalty_and_experience_score(5, 10, 7, [], {})
        assert penalty == 0.0
        assert score == 1.0

    def test_penalty_when_candidate_below_job_min_years(self):
        penalty, score = seniority_penalty_and_experience_score(5, 10, 2, [], {})
        assert penalty > 0.0
        assert score < 1.0

    def test_skill_min_years_penalty(self):
        penalty, score = seniority_penalty_and_experience_score(
            3,
            10,
            5,
            [("Python", 5, "must_have")],
            {"Python": (0.8, 2)},  # 2 years vs 5 required
        )
        assert penalty > 0.0
        assert score < 1.0

    def test_experience_score_in_unit_interval(self):
        _, score = seniority_penalty_and_experience_score(10, 20, 1, [], {})
        assert 0.0 <= score <= 1.0


class TestSeniorityLevelOrdinal:
    """Tests for seniority_level_ordinal."""

    def test_known_levels_return_ordinal(self):
        assert seniority_level_ordinal("JUNIOR") == 0
        assert seniority_level_ordinal("junior") == 0
        assert seniority_level_ordinal("MID") == 1
        assert seniority_level_ordinal("SENIOR") == 2
        assert seniority_level_ordinal("EXECUTIVE") == 6

    def test_unknown_or_none_return_none(self):
        assert seniority_level_ordinal(None) is None
        assert seniority_level_ordinal("") is None
        assert seniority_level_ordinal("unknown") is None


class TestSeniorityLevelPenalty:
    """Tests for seniority_level_penalty."""

    def test_no_penalty_when_candidate_at_or_above_job_level(self):
        assert seniority_level_penalty("SENIOR", "SENIOR", 0.1) == 0.0
        assert seniority_level_penalty("SENIOR", "STAFF", 0.1) == 0.0
        assert seniority_level_penalty("MID", "MID", 0.1) == 0.0

    def test_penalty_when_candidate_below_job_level(self):
        assert seniority_level_penalty("SENIOR", "MID", 0.1) > 0.0
        assert seniority_level_penalty("SENIOR", "JUNIOR", 0.1) > 0.0

    def test_unknown_level_no_penalty(self):
        assert seniority_level_penalty(None, "SENIOR", 0.1) == 0.0
        assert seniority_level_penalty("SENIOR", None, 0.1) == 0.0


class TestCandidateSeniorityScale:
    """Tests for candidate_seniority_scale."""

    def test_scale_in_unit_interval(self):
        assert 0.0 <= candidate_seniority_scale({}) <= 1.0
        assert 0.0 <= candidate_seniority_scale({"seniority_level": "SENIOR"}) <= 1.0

    def test_higher_level_higher_scale(self):
        j = candidate_seniority_scale({"seniority_level": "JUNIOR"})
        s = candidate_seniority_scale({"seniority_level": "SENIOR"})
        e = candidate_seniority_scale({"seniority_level": "EXECUTIVE"})
        assert j < s < e

    def test_years_nudge_scale(self):
        base = candidate_seniority_scale({"seniority_level": "SENIOR"})
        with_years = candidate_seniority_scale(
            {"seniority_level": "SENIOR", "years_of_experience": 10}
        )
        assert with_years >= base


class TestJobRequiredSeniorityScale:
    """Tests for job_required_seniority_scale."""

    def test_none_when_no_level(self):
        assert job_required_seniority_scale({}) is None
        assert job_required_seniority_scale({"seniority_level": None}) is None

    def test_returns_scale_when_level_set(self):
        s = job_required_seniority_scale({"seniority_level": "SENIOR"})
        assert s is not None
        assert 0.0 <= s <= 1.0


class TestSeniorityScaleFit:
    """Tests for seniority_scale_fit."""

    def test_one_when_no_job_requirement(self):
        assert seniority_scale_fit(0.3, None) == 1.0

    def test_one_when_candidate_meets_or_exceeds(self):
        assert seniority_scale_fit(0.6, 0.5) == 1.0
        assert seniority_scale_fit(0.5, 0.5) == 1.0

    def test_below_one_when_candidate_below_required(self):
        fit = seniority_scale_fit(0.3, 0.6)
        assert 0.0 <= fit < 1.0


class TestTenureInstabilityPenalty:
    """Tests for tenure_instability_penalty (linear on avg tenure: 0mo→1, 18mo→0)."""

    def test_zero_when_job_not_high_stakes(self):
        assert (
            tenure_instability_penalty(
                {"average_tenure_months": 6, "job_count": 10},
                False,
            )
            == 0.0
        )

    def test_linear_scale_on_average_tenure(self):
        # 0 months → penalty 1; 9 months → 0.5; 18+ → 0
        assert tenure_instability_penalty({"average_tenure_months": 0}, True) == 1.0
        assert tenure_instability_penalty({"average_tenure_months": 9}, True) == 0.5
        assert tenure_instability_penalty({"average_tenure_months": 18}, True) == 0.0
        assert tenure_instability_penalty({"average_tenure_months": 36}, True) == 0.0

    def test_penalty_when_high_stakes_and_short_tenure(self):
        # avg_tenure 10 → 1 - 10/18 = 8/18
        p = tenure_instability_penalty({"average_tenure_months": 10}, True)
        assert abs(p - (1.0 - 10 / 18)) < 1e-9
        assert 0.0 < p < 1.0

    def test_zero_when_avg_tenure_missing(self):
        assert tenure_instability_penalty({}, True) == 0.0
        assert tenure_instability_penalty({"job_count": 10}, True) == 0.0

    def test_zero_when_high_stakes_but_stable_candidate(self):
        cand = {"average_tenure_months": 36, "job_count": 2}
        assert tenure_instability_penalty(cand, True) == 0.0


class TestJobIsHighStakes:
    """Tests for job_is_high_stakes."""

    def test_true_for_senior_plus(self):
        assert job_is_high_stakes({"seniority_level": "SENIOR"}) is True
        assert job_is_high_stakes({"seniority_level": "STAFF"}) is True
        assert job_is_high_stakes({"seniority_level": "LEAD"}) is True
        assert job_is_high_stakes({"seniority_level": "EXECUTIVE"}) is True

    def test_false_for_junior_mid(self):
        assert job_is_high_stakes({"seniority_level": "JUNIOR"}) is False
        assert job_is_high_stakes({"seniority_level": "MID"}) is False

    def test_false_when_missing(self):
        assert job_is_high_stakes({}) is False
        assert job_is_high_stakes({"seniority_level": None}) is False


class TestConstants:
    """Constants used by scoring are defined."""

    def test_seniority_max_deduction_exported(self):
        assert SENIORITY_MAX_DEDUCTION == 0.2
