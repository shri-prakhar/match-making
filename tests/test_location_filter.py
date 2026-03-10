"""Tests for location pre-filter (talent_matching.matchmaking.location_filter)."""

from talent_matching.matchmaking.location_filter import (
    candidate_matches_location,
    candidate_passes_location_or_timezone,
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


class TestCandidatePassesLocationOrTimezone:
    """Tests for candidate_passes_location_or_timezone (location or same/adjacent timezone)."""

    def test_location_match_passes_even_without_job_timezone(self):
        candidate = {
            "location_city": "Shanghai",
            "location_country": "China",
            "timezone": "Asia/Shanghai",
        }
        assert candidate_passes_location_or_timezone(candidate, ["Shanghai"], None) is True

    def test_same_timezone_passes_when_location_does_not_match(self):
        # Job Shanghai; candidate in Hong Kong (same UTC+8) but not exact location match
        candidate = {
            "location_city": "Hong Kong",
            "location_country": "Hong Kong",
            "timezone": "Asia/Hong_Kong",
        }
        assert candidate_matches_location(candidate, ["Shanghai"]) is False
        assert (
            candidate_passes_location_or_timezone(candidate, ["Shanghai"], "Asia/Shanghai") is True
        )

    def test_adjacent_timezone_passes_within_two_hours(self):
        # Job UTC+8 (Shanghai); candidate UTC+7 (Bangkok) = 1 hour diff
        candidate = {
            "location_city": "Bangkok",
            "location_country": "Thailand",
            "timezone": "Asia/Bangkok",
        }
        assert candidate_passes_location_or_timezone(candidate, ["Shanghai"], "UTC+8") is True

    def test_far_timezone_fails(self):
        candidate = {
            "location_city": "New York",
            "location_country": "USA",
            "timezone": "America/New_York",
        }
        assert candidate_passes_location_or_timezone(candidate, ["Shanghai"], "UTC+8") is False

    def test_no_job_timezone_requirements_only_location_match(self):
        candidate = {
            "location_city": "Hong Kong",
            "location_country": "Hong Kong",
            "timezone": "Asia/Hong_Kong",
        }
        assert candidate_passes_location_or_timezone(candidate, ["Shanghai"], None) is False
        assert candidate_passes_location_or_timezone(candidate, ["Shanghai"], "") is False
