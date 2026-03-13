"""Centralized location resolution: load country/city/region aliases and region-countries from DB.

Used by location_filter (with optional maps) and by assets that have a session.
When tables are empty, callers fall back to hardcoded REGION_COUNTRIES / COUNTRY_ALIASES.
"""

from sqlalchemy import select
from sqlalchemy.orm import Session

from talent_matching.models.location import (
    LocationCityAlias,
    LocationCountryAlias,
    LocationRegionAlias,
    LocationRegionCountry,
)


def load_country_aliases(session: Session) -> dict[str, str] | None:
    """Load alias -> country_canonical (lowercase) from location_country_aliases.

    Returns None if the table is empty so callers can fall back to hardcoded.
    """
    rows = session.execute(
        select(LocationCountryAlias.alias, LocationCountryAlias.country_canonical)
    ).all()
    if not rows:
        return None
    return {alias.strip().lower(): country.strip().lower() for alias, country in rows}


def load_city_aliases(session: Session) -> dict[str, str] | None:
    """Load alias -> city_canonical (lowercase slug) from location_city_aliases.

    Returns None if the table is empty so callers can skip city resolution.
    """
    rows = session.execute(select(LocationCityAlias.alias, LocationCityAlias.city_canonical)).all()
    if not rows:
        return None
    return {alias.strip().lower(): city.strip().lower() for alias, city in rows}


def load_region_aliases(session: Session) -> dict[str, str] | None:
    """Load alias -> region_canonical (lowercase) from location_region_aliases.

    Returns None if the table is empty so callers can skip region-alias resolution.
    """
    rows = session.execute(
        select(LocationRegionAlias.alias, LocationRegionAlias.region_canonical)
    ).all()
    if not rows:
        return None
    return {alias.strip().lower(): region.strip().lower() for alias, region in rows}


def load_region_countries(session: Session) -> dict[str, set[str]] | None:
    """Load region -> set(country) (lowercase) from location_region_countries.

    Returns None if the table is empty so callers can fall back to hardcoded.
    """
    rows = session.execute(
        select(LocationRegionCountry.region, LocationRegionCountry.country)
    ).all()
    if not rows:
        return None
    out: dict[str, set[str]] = {}
    for region, country in rows:
        r = region.strip().lower()
        c = country.strip().lower()
        if r not in out:
            out[r] = set()
        out[r].add(c)
    return out


def load_location_maps(
    session: Session,
) -> tuple[
    dict[str, str] | None,
    dict[str, set[str]] | None,
    dict[str, str] | None,
    dict[str, str] | None,
]:
    """Load country_aliases, region_countries, city_aliases, region_aliases.

    Returns (country_aliases, region_countries, city_aliases, region_aliases).
    Any may be None if the table is empty.
    """
    return (
        load_country_aliases(session),
        load_region_countries(session),
        load_city_aliases(session),
        load_region_aliases(session),
    )


def get_region_for_country(
    country: str,
    region_countries: dict[str, set[str]],
) -> str | None:
    """Return the region (lowercase) for a country given loaded region_countries.

    First region wins when a country appears in multiple regions (sorted key order).
    Caller must pass the result of load_region_countries (or hardcoded equivalent).
    """
    if not country or not (country := (country or "").strip().lower()):
        return None
    for region in sorted(region_countries.keys()):
        if country in region_countries[region]:
            return region
    return None
