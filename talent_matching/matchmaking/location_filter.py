"""Location pre-filter for matchmaking.

Parses job Preferred Location, matches candidates by region/country/city,
and handles region-to-country mapping (e.g. job "Europe" matches candidate "Germany").
"""

NO_FILTER_VALUES: frozenset[str] = frozenset({"global", "no hard requirements"})

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

# Country aliases for flexible matching (alias -> canonical lowercase)
COUNTRY_ALIASES: dict[str, str] = {
    "usa": "united states",
    "us": "united states",
    "uk": "united kingdom",
    "uae": "united arab emirates",
    "kl": "malaysia",  # Kuala Lumpur is in Malaysia
    "kuala lumpur": "malaysia",
    "viet nam": "vietnam",
}


def _normalize(s: str | None) -> str:
    """Strip and lowercase for comparison."""
    if s is None:
        return ""
    return (s or "").strip().lower()


def _resolve_country(s: str) -> str:
    """Resolve alias to canonical country name."""
    key = _normalize(s)
    return COUNTRY_ALIASES.get(key, key)


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


def candidate_matches_location(candidate: dict, job_locations: list[str]) -> bool:
    """Return True if candidate's location matches any job location.

    Candidate passes if any job location matches any of:
    - candidate["location_region"]
    - candidate["location_country"]
    - candidate["location_city"]

    Uses REGION_COUNTRIES for region-to-country mapping (e.g. job "Europe", candidate country "Germany").
    Uses COUNTRY_ALIASES for flexible matching (e.g. "USA" -> "United States").

    If candidate has no location data (region, country, city all null/empty): pass (conservative).
    """
    cand_region = _normalize(candidate.get("location_region") or "")
    cand_country = _resolve_country(candidate.get("location_country") or "")
    cand_city = _normalize(candidate.get("location_city") or "")

    has_no_location = not cand_region and not cand_country and not cand_city
    if has_no_location:
        return True

    cand_values = {cand_region, cand_country, cand_city} - {""}

    for job_loc in job_locations:
        j = _normalize(job_loc)
        if not j:
            continue

        if j in cand_values:
            return True

        if j in REGION_COUNTRIES:
            countries = REGION_COUNTRIES[j]
            if cand_country and cand_country in countries:
                return True

        resolved_job = _resolve_country(job_loc)
        if resolved_job in cand_values:
            return True

    return False
