"""Location pre-filter for matchmaking.

Parses job Preferred Location, matches candidates by region/country/city,
and handles region-to-country mapping (e.g. job "Europe" matches candidate "Germany").
"""

NO_FILTER_VALUES: frozenset[str] = frozenset({"global", "no hard requirements"})

# Minimum candidate pool size before we stop expanding (strict -> country -> region).
MIN_POOL_SIZE = 15

# Region -> set of country names (lowercase). Used when job has "Europe" and candidate has "Germany".
REGION_COUNTRIES: dict[str, set[str]] = {
    "europe": {
        "albania",
        "andorra",
        "armenia",
        "austria",
        "azerbaijan",
        "belarus",
        "belgium",
        "bosnia and herzegovina",
        "bulgaria",
        "croatia",
        "cyprus",
        "czech republic",
        "denmark",
        "estonia",
        "finland",
        "france",
        "georgia",
        "germany",
        "greece",
        "hungary",
        "iceland",
        "ireland",
        "italy",
        "kazakhstan",
        "kosovo",
        "latvia",
        "liechtenstein",
        "lithuania",
        "luxembourg",
        "malta",
        "moldova",
        "monaco",
        "montenegro",
        "netherlands",
        "north macedonia",
        "norway",
        "poland",
        "portugal",
        "romania",
        "russia",
        "san marino",
        "serbia",
        "slovakia",
        "slovenia",
        "spain",
        "sweden",
        "switzerland",
        "turkey",
        "ukraine",
        "united kingdom",
        "uk",
        "vatican city",
    },
    "north america": {
        "united states",
        "usa",
        "us",
        "canada",
        "mexico",
        "guatemala",
        "belize",
        "el salvador",
        "honduras",
        "nicaragua",
        "costa rica",
        "panama",
        "cuba",
        "jamaica",
        "haiti",
        "dominican republic",
        "bahamas",
        "barbados",
        "trinidad and tobago",
    },
    "south america": {
        "argentina",
        "bolivia",
        "brazil",
        "chile",
        "colombia",
        "ecuador",
        "guyana",
        "paraguay",
        "peru",
        "suriname",
        "uruguay",
        "venezuela",
    },
    "asia": {
        "afghanistan",
        "bangladesh",
        "bhutan",
        "brunei",
        "cambodia",
        "china",
        "india",
        "indonesia",
        "iran",
        "iraq",
        "israel",
        "japan",
        "jordan",
        "kazakhstan",
        "korea",
        "south korea",
        "north korea",
        "kuwait",
        "kyrgyzstan",
        "laos",
        "lebanon",
        "malaysia",
        "maldives",
        "mongolia",
        "myanmar",
        "nepal",
        "oman",
        "pakistan",
        "palestine",
        "philippines",
        "qatar",
        "saudi arabia",
        "singapore",
        "sri lanka",
        "syria",
        "taiwan",
        "tajikistan",
        "thailand",
        "timor-leste",
        "turkmenistan",
        "uae",
        "united arab emirates",
        "uzbekistan",
        "vietnam",
        "viet nam",
        "yemen",
    },
    "apac": {
        "australia",
        "new zealand",
        "japan",
        "china",
        "south korea",
        "singapore",
        "hong kong",
        "taiwan",
        "india",
        "indonesia",
        "malaysia",
        "philippines",
        "thailand",
        "vietnam",
        "viet nam",
        "new zealand",
    },
    "emea": {
        "united kingdom",
        "uk",
        "germany",
        "france",
        "italy",
        "spain",
        "netherlands",
        "belgium",
        "switzerland",
        "austria",
        "sweden",
        "norway",
        "denmark",
        "finland",
        "ireland",
        "poland",
        "portugal",
        "greece",
        "czech republic",
        "romania",
        "hungary",
        "russia",
        "turkey",
        "uae",
        "united arab emirates",
        "saudi arabia",
        "south africa",
        "nigeria",
        "egypt",
        "israel",
        "morocco",
        "kenya",
        "ghana",
    },
    "middle east": {
        "bahrain",
        "cyprus",
        "egypt",
        "iran",
        "iraq",
        "israel",
        "jordan",
        "kuwait",
        "lebanon",
        "oman",
        "palestine",
        "qatar",
        "saudi arabia",
        "syria",
        "turkey",
        "uae",
        "united arab emirates",
        "yemen",
    },
    "south east asia": {
        "brunei",
        "cambodia",
        "indonesia",
        "laos",
        "malaysia",
        "myanmar",
        "philippines",
        "singapore",
        "thailand",
        "timor-leste",
        "vietnam",
        "viet nam",
    },
    "balkan": {
        "albania",
        "bosnia and herzegovina",
        "bulgaria",
        "croatia",
        "kosovo",
        "montenegro",
        "north macedonia",
        "romania",
        "serbia",
        "slovenia",
    },
    "africa": {
        "algeria",
        "angola",
        "benin",
        "botswana",
        "burkina faso",
        "burundi",
        "cameroon",
        "cape verde",
        "central african republic",
        "chad",
        "comoros",
        "congo",
        "djibouti",
        "egypt",
        "equatorial guinea",
        "eritrea",
        "eswatini",
        "ethiopia",
        "gabon",
        "gambia",
        "ghana",
        "guinea",
        "guinea-bissau",
        "ivory coast",
        "kenya",
        "lesotho",
        "liberia",
        "libya",
        "madagascar",
        "malawi",
        "mali",
        "mauritania",
        "mauritius",
        "morocco",
        "mozambique",
        "namibia",
        "niger",
        "nigeria",
        "rwanda",
        "sao tome",
        "senegal",
        "seychelles",
        "sierra leone",
        "somalia",
        "south africa",
        "sudan",
        "tanzania",
        "togo",
        "tunisia",
        "uganda",
        "zambia",
        "zimbabwe",
    },
    "australia": {
        "australia",
    },
}

# Country aliases for flexible matching (alias -> canonical lowercase).
# Includes city/location keys so job strings like "Shanghai" resolve to a country.
COUNTRY_ALIASES: dict[str, str] = {
    "usa": "united states",
    "us": "united states",
    "uk": "united kingdom",
    "uae": "united arab emirates",
    "kl": "malaysia",  # Kuala Lumpur is in Malaysia
    "kuala lumpur": "malaysia",
    "viet nam": "vietnam",
    # Cities / common job preferred locations -> country
    "shanghai": "china",
    "beijing": "china",
    "hong kong": "hong kong",
    "singapore": "singapore",
    "tokyo": "japan",
    "london": "united kingdom",
    "new york": "united states",
    "ny": "united states",  # common CV abbreviation for New York
    "la": "united states",  # Los Angeles
    "sf": "united states",  # San Francisco
    "munich": "germany",
    "dubai": "united arab emirates",
    "berlin": "germany",
    "paris": "france",
    "amsterdam": "netherlands",
    "sydney": "australia",
    "melbourne": "australia",
    "bangalore": "india",
    "mumbai": "india",
    "seoul": "south korea",
    "taipei": "taiwan",
    "bangkok": "thailand",
    "jakarta": "indonesia",
    "manila": "philippines",
    "ho chi minh city": "vietnam",
    "san francisco": "united states",
    "toronto": "canada",
    "zurich": "switzerland",
    "tel aviv": "israel",
}

# Country -> region (lowercase). Built from REGION_COUNTRIES; first region wins for duplicates.
_COUNTRY_TO_REGION: dict[str, str] = {}
for _region in sorted(REGION_COUNTRIES.keys()):
    for _c in REGION_COUNTRIES[_region]:
        if _c not in _COUNTRY_TO_REGION:
            _COUNTRY_TO_REGION[_c] = _region
COUNTRY_TO_REGION: dict[str, str] = _COUNTRY_TO_REGION


def _normalize(s: str | None) -> str:
    """Strip and lowercase for comparison."""
    if s is None:
        return ""
    return (s or "").strip().lower()


def _resolve_country(s: str, country_aliases: dict[str, str] | None = None) -> str:
    """Resolve alias to canonical country name.

    Uses country_aliases when provided, else fallback COUNTRY_ALIASES.
    """
    key = _normalize(s)
    aliases = country_aliases if country_aliases is not None else COUNTRY_ALIASES
    return aliases.get(key, key)


def get_region_for_country(
    country: str,
    *,
    country_aliases: dict[str, str] | None = None,
    region_countries: dict[str, set[str]] | None = None,
) -> str | None:
    """Return the region (lowercase) for a country, or None if unknown.

    When region_countries is provided (from DB), derive country->region from it
    (first region wins for duplicates, sorted). Otherwise use COUNTRY_TO_REGION.
    Resolves country via country_aliases when provided.
    """
    if not country or not (country := (country or "").strip()):
        return None
    canonical = _resolve_country(country, country_aliases)
    key = _normalize(canonical)
    if region_countries is not None:
        for region in sorted(region_countries.keys()):
            if key in region_countries[region]:
                return region
        return None
    return COUNTRY_TO_REGION.get(key)


def job_locations_to_countries(
    job_locations: list[str],
    *,
    country_aliases: dict[str, str] | None = None,
    region_countries: dict[str, set[str]] | None = None,
) -> set[str]:
    """Resolve job location strings to a set of canonical (lowercase) countries.

    For each normalized job location: if it is a region key, add all countries
    in that region; else if it is in country aliases (e.g. city), add that
    country; else treat as country (via _resolve_country) and add it.
    Uses provided maps when given, else REGION_COUNTRIES / COUNTRY_ALIASES.
    """
    regions = region_countries if region_countries is not None else REGION_COUNTRIES
    out: set[str] = set()
    for loc in job_locations or []:
        j = _normalize(loc)
        if not j:
            continue
        if j in regions:
            out.update(regions[j])
            continue
        resolved = _resolve_country(loc, country_aliases)
        if resolved:
            out.add(_normalize(resolved))
    return out


def job_locations_to_regions(
    job_locations: list[str],
    *,
    country_aliases: dict[str, str] | None = None,
    region_countries: dict[str, set[str]] | None = None,
) -> set[str]:
    """Resolve job location strings to a set of region names (lowercase).

    Uses job_locations_to_countries then maps each country to its region(s).
    Returns the set of region names (e.g. 'europe', 'asia').
    """
    countries = job_locations_to_countries(
        job_locations,
        country_aliases=country_aliases,
        region_countries=region_countries,
    )
    regions_out: set[str] = set()
    for c in countries:
        r = get_region_for_country(
            c,
            country_aliases=country_aliases,
            region_countries=region_countries,
        )
        if r:
            regions_out.add(r)
    return regions_out


def candidate_matches_country(
    candidate: dict,
    allowed_countries: set[str],
    *,
    country_aliases: dict[str, str] | None = None,
) -> bool:
    """True if candidate's location_country (resolved) is in allowed_countries.

    If candidate has no country, returns False (no expansion on missing data).
    """
    if not allowed_countries:
        return False
    cand_country = candidate.get("location_country")
    if not cand_country or not str(cand_country).strip():
        return False
    resolved = _resolve_country(str(cand_country), country_aliases)
    key = _normalize(resolved)
    return key in allowed_countries


def candidate_matches_region(
    candidate: dict,
    allowed_regions: set[str],
    *,
    country_aliases: dict[str, str] | None = None,
    region_countries: dict[str, set[str]] | None = None,
) -> bool:
    """True if candidate's region or country-derived region is in allowed_regions.

    If candidate has location_region, normalize and check. Else if candidate has
    location_country, resolve to region and check. Otherwise False.
    """
    if not allowed_regions:
        return False
    cand_region = _normalize(candidate.get("location_region") or "")
    if cand_region and cand_region in allowed_regions:
        return True
    cand_country = candidate.get("location_country")
    if cand_country and str(cand_country).strip():
        r = get_region_for_country(
            str(cand_country),
            country_aliases=country_aliases,
            region_countries=region_countries,
        )
        if r and r in allowed_regions:
            return True
    return False


def parse_job_preferred_locations(location_raw: str | None) -> list[str] | None:
    """Parse comma-separated location_raw into list of trimmed strings.

    If any value (lowercased) is in NO_FILTER_VALUES, return None (no filter).
    Else return list of non-empty values.

    Examples:
        >>> parse_job_preferred_locations("Europe, Germany")
        ['Europe', 'Germany']
        >>> parse_job_preferred_locations("Global")
        None
        >>> parse_job_preferred_locations("")
        None
    """
    if not location_raw or not (location_raw := location_raw.strip()):
        return None

    parts = [p.strip() for p in location_raw.split(",") if p.strip()]
    if not parts:
        return None

    for p in parts:
        if _normalize(p) in NO_FILTER_VALUES:
            return None

    return parts


def candidate_matches_location(
    candidate: dict,
    job_locations: list[str],
    *,
    country_aliases: dict[str, str] | None = None,
    region_countries: dict[str, set[str]] | None = None,
) -> bool:
    """Return True if candidate's location matches any job location.

    Candidate passes if any job location matches any of:
    - candidate["location_region"]
    - candidate["location_country"]
    - candidate["location_city"]

    Uses region_countries for region-to-country mapping when provided, else REGION_COUNTRIES.
    Uses country_aliases for flexible matching when provided, else COUNTRY_ALIASES.

    If candidate has no location data (region, country, city all null/empty): pass (conservative).
    """
    regions = region_countries if region_countries is not None else REGION_COUNTRIES
    cand_region = _normalize(candidate.get("location_region") or "")
    cand_country = _resolve_country(candidate.get("location_country") or "", country_aliases)
    cand_city = _normalize(candidate.get("location_city") or "")

    has_no_location = not cand_region and not cand_country and not cand_city
    if has_no_location:
        return True

    cand_values = {cand_region, cand_country, cand_city} - {""}
    # Resolve candidate city through aliases (e.g. "ny" -> "united states") so job "New York" matches
    if cand_city:
        resolved_city = _resolve_country(cand_city, country_aliases)
        if resolved_city and resolved_city != cand_city:
            cand_values.add(resolved_city)

    for job_loc in job_locations:
        j = _normalize(job_loc)
        if not j:
            continue

        if j in cand_values:
            return True

        if j in regions:
            countries = regions[j]
            if cand_country and cand_country in countries:
                return True

        resolved_job = _resolve_country(job_loc, country_aliases)
        if resolved_job in cand_values:
            return True

    return False


def candidate_passes_location_or_timezone(
    candidate: dict,
    job_locations: list[str],
    job_timezone_requirements: str | None,
    max_hours_adjacent: float = 2.0,
    *,
    country_aliases: dict[str, str] | None = None,
    region_countries: dict[str, set[str]] | None = None,
) -> bool:
    """True if candidate passes location filter or is in same/adjacent timezone.

    Uses existing candidate_matches_location for exact/region match. When the job has
    timezone_requirements (from normalized_jobs), also allows candidates whose timezone
    is within max_hours_adjacent of the job (reuses scoring.timezones_same_or_adjacent).
    """
    if candidate_matches_location(
        candidate,
        job_locations,
        country_aliases=country_aliases,
        region_countries=region_countries,
    ):
        return True
    if not job_timezone_requirements or not (job_timezone_requirements or "").strip():
        return False
    from talent_matching.matchmaking.scoring import timezones_same_or_adjacent

    cand_tz = candidate.get("timezone")
    return timezones_same_or_adjacent(
        cand_tz,
        job_timezone_requirements,
        max_hours_diff=max_hours_adjacent,
    )
