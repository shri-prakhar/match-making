"""Location resolution: DB-backed country/city/region aliases and region-country mappings."""

from talent_matching.location.resolver import (
    get_region_for_country,
    load_city_aliases,
    load_country_aliases,
    load_location_maps,
    load_region_aliases,
    load_region_countries,
)

__all__ = [
    "get_region_for_country",
    "load_city_aliases",
    "load_country_aliases",
    "load_location_maps",
    "load_region_aliases",
    "load_region_countries",
]
