"""Tests for job category match mapping: resolver, canonical resolution, and filter logic."""

from unittest.mock import MagicMock, patch

from talent_matching.resources.matchmaking import MatchmakingResource
from talent_matching.utils.job_category import (
    norm_cat,
    resolve_desired_job_categories_to_canonical,
)


class TestNormCat:
    def test_strip_lower(self):
        assert norm_cat("Operations") == "operations"
        assert norm_cat("  Compliance  ") == "compliance"

    def test_strip_surrounding_quotes(self):
        assert norm_cat('"Operations"') == "operations"
        assert norm_cat('  "Legal"  ') == "legal"

    def test_empty(self):
        assert norm_cat("") == ""
        assert norm_cat(None) == ""


class TestResolveDesiredJobCategoriesToCanonical:
    def test_exact_match(self):
        canonical = ["Compliance", "Operations", "Legal"]
        assert resolve_desired_job_categories_to_canonical(["Operations", "Legal"], canonical) == [
            "Operations",
            "Legal",
        ]

    def test_normalized_match(self):
        canonical = ["Compliance", "Operations"]
        assert resolve_desired_job_categories_to_canonical(
            ["operations", "OPERATIONS"], canonical
        ) == ["Operations"]

    def test_non_canonical_dropped(self):
        canonical = ["Compliance", "Operations"]
        assert resolve_desired_job_categories_to_canonical(["Ops", "Compliance"], canonical) == [
            "Compliance"
        ]

    def test_empty_canonical_returns_empty(self):
        assert resolve_desired_job_categories_to_canonical(["Operations"], []) == []
        assert resolve_desired_job_categories_to_canonical(["Operations"], None) == []


class TestGetMatchCategoriesForJobCategory:
    def test_returns_primary_only_when_no_aliases(self):
        resource = MatchmakingResource()
        mock_row = MagicMock()
        mock_row.match_category_aliases = None
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = mock_row
        session.close = MagicMock()
        with (
            patch.object(MatchmakingResource, "_get_session", return_value=session),
            patch.object(
                MatchmakingResource,
                "get_allowed_job_categories",
                return_value=["Compliance", "Operations", "Legal"],
            ),
        ):
            out = resource.get_match_categories_for_job_category("Compliance")

        assert out == {"compliance"}

    def test_returns_primary_and_aliases_when_set(self):
        resource = MatchmakingResource()
        mock_row = MagicMock()
        mock_row.match_category_aliases = ["Operations", "Legal"]
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = mock_row
        session.close = MagicMock()
        with (
            patch.object(MatchmakingResource, "_get_session", return_value=session),
            patch.object(
                MatchmakingResource,
                "get_allowed_job_categories",
                return_value=["Compliance", "Operations", "Legal"],
            ),
        ):
            out = resource.get_match_categories_for_job_category("Compliance")

        assert out == {"compliance", "operations", "legal"}

    def test_excludes_primary_when_not_in_talent_list(self):
        """When job_category is ATS-only (e.g. Compliance not in Talent), only aliases in allowed list are returned."""
        resource = MatchmakingResource()
        mock_row = MagicMock()
        mock_row.match_category_aliases = ["Operations", "Legal"]
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = mock_row
        session.close = MagicMock()
        with (
            patch.object(MatchmakingResource, "_get_session", return_value=session),
            patch.object(
                MatchmakingResource,
                "get_allowed_job_categories",
                return_value=["Operations", "Legal"],
            ),
        ):
            out = resource.get_match_categories_for_job_category("Compliance")

        assert out == {"operations", "legal"}
        assert "compliance" not in out

    def test_empty_job_category_returns_empty_set(self):
        resource = MatchmakingResource()
        out = resource.get_match_categories_for_job_category("")
        assert out == set()
        out = resource.get_match_categories_for_job_category(None)
        assert out == set()


class TestMatchFilterLogic:
    """Candidate passes job category filter when desired set intersects job match categories."""

    def test_candidate_with_operations_passes_when_job_compliance_maps_to_operations(self):
        job_match_categories_norm = {"compliance", "operations", "legal"}
        desired_normalized = {"operations"}
        assert (job_match_categories_norm & desired_normalized) == {"operations"}
        assert bool(job_match_categories_norm & desired_normalized) is True

    def test_candidate_with_no_overlap_fails(self):
        job_match_categories_norm = {"compliance"}
        desired_normalized = {"operations"}
        assert (job_match_categories_norm & desired_normalized) == set()
