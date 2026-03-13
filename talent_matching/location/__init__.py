"""Location resolution: DB-backed country aliases and region-country mappings."""

from talent_matching.location.resolver import (
    get_region_for_country,
    load_country_aliases,
    load_location_maps,
    load_region_countries,
)

__all__ = [
    "get_region_for_country",
    "load_country_aliases",
    "load_location_maps",
    "load_region_countries",
]
