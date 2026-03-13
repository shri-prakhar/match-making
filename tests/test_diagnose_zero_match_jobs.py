"""Tests for diagnose_zero_match_jobs script: location and job category pass logic."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.diagnose_zero_match_jobs import (
    location_expansion_detail,
    would_candidate_pass_job_category,
    would_candidate_pass_location,
)


class TestWouldCandidatePassLocation:
    """Test location filter logic used by diagnosis script."""

    def test_no_location_filter_passes(self):
        passed, detail = would_candidate_pass_location({"location_country": "Germany"}, None, None)
        assert passed is True
        assert "no location filter" in detail

    def test_global_location_filter_passes(self):
        passed, detail = would_candidate_pass_location(
            {"location_country": "Germany"}, "Global", None
        )
        assert passed is True
        assert "no location filter" in detail

    def test_europe_job_germany_candidate_passes_strict_or_region(self):
        # Job "Europe" -> candidate with Germany: region expansion includes Germany
        passed, detail = would_candidate_pass_location(
            {
                "location_region": None,
                "location_country": "Germany",
                "location_city": None,
                "timezone": None,
            },
            "Europe",
            None,
        )
        assert passed is True
        assert detail in ("strict (exact/region/timezone)", "country expansion", "region expansion")

    def test_europe_job_usa_candidate_fails(self):
        passed, detail = would_candidate_pass_location(
            {
                "location_country": "United States",
                "location_region": None,
                "location_city": None,
                "timezone": None,
            },
            "Europe",
            None,
        )
        assert passed is False
        assert "failed all" in detail

    def test_empty_candidate_location_passes_when_job_has_filter(self):
        # location_filter is conservative: no location data => pass (candidate_matches_location returns True)
        passed, detail = would_candidate_pass_location(
            {
                "location_region": None,
                "location_country": None,
                "location_city": None,
                "timezone": None,
            },
            "Germany",
            None,
        )
        assert passed is True
        assert "strict" in detail or "no location" in detail


class TestWouldCandidatePassJobCategory:
    """Test job category filter logic used by diagnosis script."""

    def test_intersection_non_empty_passes(self):
        candidate = {"desired_job_categories": ["Operations", "Legal"]}
        job_match_categories_norm = {"operations", "compliance"}
        assert would_candidate_pass_job_category(candidate, job_match_categories_norm) is True

    def test_intersection_empty_fails(self):
        candidate = {"desired_job_categories": ["Engineering"]}
        job_match_categories_norm = {"operations", "compliance"}
        assert would_candidate_pass_job_category(candidate, job_match_categories_norm) is False

    def test_empty_desired_fails(self):
        candidate = {"desired_job_categories": []}
        assert would_candidate_pass_job_category(candidate, {"operations"}) is False

    def test_normalized_match(self):
        candidate = {"desired_job_categories": ["  Operations  ", "Legal"]}
        job_match_categories_norm = {"operations", "legal"}
        assert would_candidate_pass_job_category(candidate, job_match_categories_norm) is True


class TestLocationExpansionDetail:
    """Test location expansion detail for verbose output."""

    def test_no_location_raw_returns_none_locations(self):
        out = location_expansion_detail(
            {"location_country": "Germany"},
            None,
            None,
        )
        assert out["job_locations"] is None
        assert out["allowed_countries"] == set()
        assert out["allowed_regions"] == set()

    def test_europe_returns_countries_and_regions(self):
        out = location_expansion_detail(
            {"location_country": "Germany", "location_region": None, "location_city": None},
            "Europe",
            None,
        )
        assert out["job_locations"] == ["Europe"]
        assert (
            "germany" in out["allowed_countries"]
            or "germany" in str(out["allowed_countries"]).lower()
        )
        assert "europe" in out["allowed_regions"]
        assert out["candidate"]["location_country"] == "Germany"
