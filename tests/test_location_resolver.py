"""Tests for location resolver (talent_matching.location.resolver)."""

from talent_matching.location.resolver import get_region_for_country


class TestGetRegionForCountry:
    """Tests for get_region_for_country with in-memory region_countries dict."""

    def test_returns_region_when_country_in_one_region(self):
        region_countries = {
            "europe": {"germany", "france", "italy"},
            "north america": {"united states", "canada"},
        }
        assert get_region_for_country("germany", region_countries) == "europe"
        assert get_region_for_country("united states", region_countries) == "north america"

    def test_first_region_wins_when_country_in_multiple(self):
        region_countries = {
            "asia": {"russia", "china"},
            "europe": {"germany", "russia"},
        }
        # sorted(region_countries.keys()) -> ["asia", "europe"], so "asia" wins
        assert get_region_for_country("russia", region_countries) == "asia"

    def test_returns_none_for_unknown_country(self):
        region_countries = {"europe": {"germany"}}
        assert get_region_for_country("atlantis", region_countries) is None

    def test_returns_none_for_empty_country(self):
        region_countries = {"europe": {"germany"}}
        assert get_region_for_country("", region_countries) is None
        assert get_region_for_country("   ", region_countries) is None
