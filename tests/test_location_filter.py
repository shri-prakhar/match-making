"""Tests for location pre-filter (talent_matching.matchmaking.location_filter)."""

from talent_matching.matchmaking.location_filter import (
    MIN_POOL_SIZE,
    candidate_matches_country,
    candidate_matches_location,
    candidate_matches_region,
    candidate_passes_location_or_timezone,
    get_region_for_country,
    job_locations_to_countries,
    job_locations_to_regions,
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

    def test_candidate_city_ny_matches_job_new_york(self):
        """Candidate with city 'NY' (abbreviation) should match job 'New York' (CVs often use NY)."""
        candidate = {
            "location_region": None,
            "location_country": None,
            "location_city": "NY",
        }
        assert candidate_matches_location(candidate, ["New York"]) is True
        assert candidate_matches_location(candidate, ["New York", "Munich"]) is True

    def test_with_provided_maps_uses_db_shaped_data(self):
        """When country_aliases/region_countries are provided, filter uses them instead of hardcoded."""
        country_aliases = {"custom_city": "united states", "de": "germany"}
        region_countries = {"europe": {"germany", "france"}, "north america": {"united states"}}
        candidate = {
            "location_region": None,
            "location_country": None,
            "location_city": "custom_city",
        }
        assert (
            candidate_matches_location(
                candidate,
                ["United States"],
                country_aliases=country_aliases,
                region_countries=region_countries,
            )
            is True
        )
        assert job_locations_to_countries(
            ["custom_city"],
            country_aliases=country_aliases,
            region_countries=region_countries,
        ) == {"united states"}
        assert (
            get_region_for_country(
                "germany",
                country_aliases=country_aliases,
                region_countries=region_countries,
            )
            == "europe"
        )


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


class TestJobLocationsToCountries:
    """Tests for job_locations_to_countries."""

    def test_city_resolves_to_country(self):
        assert job_locations_to_countries(["Shanghai"]) == {"china"}
        assert job_locations_to_countries(["Singapore"]) == {"singapore"}
        assert job_locations_to_countries(["Tokyo"]) == {"japan"}

    def test_region_expands_to_all_countries(self):
        countries = job_locations_to_countries(["Europe"])
        assert "germany" in countries
        assert "france" in countries
        assert "united kingdom" in countries

    def test_country_passthrough(self):
        assert job_locations_to_countries(["Germany"]) == {"germany"}
        assert job_locations_to_countries(["USA"]) == {"united states"}

    def test_city_abbreviation_ny_resolves_to_united_states(self):
        """Job preferred location 'NY' (abbreviation) resolves to United States."""
        assert job_locations_to_countries(["NY"]) == {"united states"}

    def test_empty_input(self):
        assert job_locations_to_countries([]) == set()
        assert job_locations_to_countries(None) == set()


class TestJobLocationsToRegions:
    """Tests for job_locations_to_regions."""

    def test_city_via_country_to_region(self):
        # Shanghai -> China -> one region (COUNTRY_TO_REGION uses first in sorted key order)
        regions = job_locations_to_regions(["Shanghai"])
        assert regions in ({"asia"}, {"apac"})
        # Singapore maps to one of asia/apac
        assert job_locations_to_regions(["Singapore"]) in ({"asia"}, {"apac"})

    def test_region_direct(self):
        assert "europe" in job_locations_to_regions(["Europe"])
        # Germany is in both europe and emea; COUNTRY_TO_REGION picks first (emea)
        assert job_locations_to_regions(["Germany"]) in ({"europe"}, {"emea"})

    def test_empty_input(self):
        assert job_locations_to_regions([]) == set()


class TestGetRegionForCountry:
    """Tests for get_region_for_country."""

    def test_known_countries(self):
        # China/Germany in multiple regions; we get first in sorted key order (apac, emea)
        assert get_region_for_country("China") in ("asia", "apac")
        assert get_region_for_country("Germany") in ("europe", "emea")
        assert get_region_for_country("United States") == "north america"
        assert get_region_for_country("USA") == "north america"

    def test_unknown_returns_none(self):
        assert get_region_for_country("Atlantis") is None
        assert get_region_for_country("") is None


class TestCandidateMatchesCountry:
    """Tests for candidate_matches_country."""

    def test_match(self):
        c = {"location_country": "China", "location_region": None, "location_city": None}
        assert candidate_matches_country(c, {"china", "japan"}) is True
        assert candidate_matches_country(c, {"germany"}) is False

    def test_no_country_fails(self):
        c = {"location_country": None, "location_region": "Asia"}
        assert candidate_matches_country(c, {"asia"}) is False

    def test_empty_allowed_fails(self):
        c = {"location_country": "Germany"}
        assert candidate_matches_country(c, set()) is False


class TestCandidateMatchesRegion:
    """Tests for candidate_matches_region."""

    def test_region_match(self):
        c = {"location_region": "Europe", "location_country": None}
        assert candidate_matches_region(c, {"europe"}) is True
        assert candidate_matches_region(c, {"asia"}) is False

    def test_country_derived_region(self):
        c = {"location_region": None, "location_country": "China"}
        # China maps to one of asia/apac; at least one must match
        assert (
            candidate_matches_region(c, {"asia"}) is True
            or candidate_matches_region(c, {"apac"}) is True
        )

    def test_no_location_fails(self):
        c = {"location_region": None, "location_country": None}
        assert candidate_matches_region(c, {"europe"}) is False

    def test_empty_allowed_fails(self):
        c = {"location_region": "Europe"}
        assert candidate_matches_region(c, set()) is False


class TestLocationExpansionBehavior:
    """Integration-style: strict -> country -> region expansion when pool < MIN_POOL_SIZE."""

    def test_min_pool_size_constant(self):
        assert MIN_POOL_SIZE == 15

    def test_expansion_logic_strict_sufficient(self):
        # Verify helpers used by expansion: Shanghai -> China -> one of Asia/APAC.
        countries = job_locations_to_countries(["Shanghai"])
        regions = job_locations_to_regions(["Shanghai"])
        assert "china" in countries
        assert regions and (regions <= {"asia", "apac"})
        c = {"location_country": "China", "location_region": None, "location_city": "Beijing"}
        assert candidate_matches_country(c, countries) is True
        assert candidate_matches_region(c, regions) is True
