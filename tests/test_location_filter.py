"""Tests for location pre-filter (talent_matching.matchmaking.location_filter)."""

from talent_matching.matchmaking.location_filter import (
    candidate_matches_location,
    parse_job_preferred_locations,
)


class TestParseJobPreferredLocations:
    """Tests for parse_job_preferred_locations."""

    def test_no_filter_values_return_none(self):
        assert parse_job_preferred_locations("Global") is None
        assert parse_job_preferred_locations("global") is None
        assert parse_job_preferred_locations("No hard requirements") is None
        assert parse_job_preferred_locations("no hard requirements") is None

    def test_empty_or_null_return_none(self):
        assert parse_job_preferred_locations("") is None
        assert parse_job_preferred_locations(None) is None
        assert parse_job_preferred_locations("   ") is None

    def test_comma_separated_returns_list(self):
        assert parse_job_preferred_locations("Europe, Germany") == ["Europe", "Germany"]
        assert parse_job_preferred_locations("Singapore") == ["Singapore"]
        assert parse_job_preferred_locations("New York, Munich, Dubai") == [
            "New York",
            "Munich",
            "Dubai",
        ]

    def test_global_in_list_returns_none(self):
        assert parse_job_preferred_locations("Europe, Global") is None
        assert parse_job_preferred_locations("Global, Germany") is None

    def test_trimmed_values(self):
        assert parse_job_preferred_locations("  Europe  ,  Germany  ") == [
            "Europe",
            "Germany",
        ]


class TestCandidateMatchesLocation:
    """Tests for candidate_matches_location."""

    def test_region_match(self):
        candidate = {
            "location_region": "Europe",
            "location_country": None,
            "location_city": None,
        }
        assert candidate_matches_location(candidate, ["Europe"]) is True

    def test_country_match(self):
        candidate = {
            "location_region": None,
            "location_country": "Germany",
            "location_city": None,
        }
        assert candidate_matches_location(candidate, ["Germany"]) is True

    def test_city_match(self):
        candidate = {
            "location_region": None,
            "location_country": None,
            "location_city": "Singapore",
        }
        assert candidate_matches_location(candidate, ["Singapore"]) is True

    def test_region_to_country_mapping(self):
        candidate = {
            "location_region": None,
            "location_country": "Germany",
            "location_city": None,
        }
        assert candidate_matches_location(candidate, ["Europe"]) is True

    def test_region_to_country_apac(self):
        candidate = {
            "location_region": None,
            "location_country": "Singapore",
            "location_city": None,
        }
        assert candidate_matches_location(candidate, ["APAC"]) is True

    def test_no_location_data_passes_conservative(self):
        candidate = {
            "location_region": None,
            "location_country": None,
            "location_city": None,
        }
        assert candidate_matches_location(candidate, ["Europe"]) is True

    def test_no_match(self):
        candidate = {
            "location_region": "Asia",
            "location_country": "Japan",
            "location_city": "Tokyo",
        }
        assert candidate_matches_location(candidate, ["Europe", "United States"]) is False

    def test_country_alias_usa(self):
        candidate = {
            "location_region": None,
            "location_country": "United States",
            "location_city": None,
        }
        assert candidate_matches_location(candidate, ["USA"]) is True
        assert candidate_matches_location(candidate, ["US"]) is True

    def test_country_alias_uk(self):
        candidate = {
            "location_region": None,
            "location_country": "United Kingdom",
            "location_city": None,
        }
        assert candidate_matches_location(candidate, ["UK"]) is True

    def test_job_singapore_candidate_city_singapore(self):
        candidate = {
            "location_region": None,
            "location_country": "Singapore",
            "location_city": "Singapore",
        }
        assert candidate_matches_location(candidate, ["Singapore"]) is True

    def test_case_insensitive(self):
        candidate = {
            "location_region": "europe",
            "location_country": "germany",
            "location_city": "munich",
        }
        assert candidate_matches_location(candidate, ["Europe"]) is True
        assert candidate_matches_location(candidate, ["GERMANY"]) is True
        assert candidate_matches_location(candidate, ["Munich"]) is True
