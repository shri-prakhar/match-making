"""Location normalization LLM operations.

Used by the location normalization job (and seed) to map location strings to
canonical countries. Mirrors the skill normalization flow:
1. assign_locations_to_bags — assign unprocessed strings to existing bags or new_groups
2. cluster_new_locations — cluster new_groups into new bags (canonical country + aliases)

Also: normalize_locations_to_countries (flat map) and assign_countries_to_regions for seed.
"""

import json
import logging
from itertools import groupby
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from talent_matching.resources.openrouter import OpenRouterResource

logger = logging.getLogger(__name__)

DEFAULT_MODEL = "openai/gpt-4o"
MAX_OUTPUT_TOKENS = 8192
MAX_BATCH_SIZE = 200


def _first_letter(s: str) -> str:
    return (s.strip().lower() or " ")[0]


def _chunk_by_letter(items: list[str]) -> list[list[str]]:
    """Sort by first letter, group, then merge into batches of at most MAX_BATCH_SIZE."""
    sorted_items = sorted(items, key=_first_letter)
    letter_groups = [list(g) for _, g in groupby(sorted_items, key=_first_letter)]
    batches: list[list[str]] = []
    current: list[str] = []
    for lg in letter_groups:
        if current and len(current) + len(lg) > MAX_BATCH_SIZE:
            batches.append(current)
            current = []
        current.extend(lg)
    if current:
        batches.append(current)
    return batches


# ---------------------------------------------------------------------------
# 1. Assign unprocessed location strings to existing bags or new_groups
# ---------------------------------------------------------------------------


def _build_assign_locations_prompt(
    existing_bags: list[dict[str, Any]],
    unprocessed_strings: list[str],
) -> str:
    bags_text = json.dumps(existing_bags)
    names_text = json.dumps(unprocessed_strings)
    return f"""We have existing **location bags** (each bag has one canonical country name and a list of equivalent aliases: cities, abbreviations, informal names that refer to that country). Below are **new location strings** that are not yet in any bag. For each new string, assign it to the canonical of an existing bag if it refers to the same country (e.g. "NY" → united states, "London" → united kingdom, "SG" → singapore). If no existing bag fits, put the string in new_groups. Use only canonicals from the existing bags; do not invent country names.

Rules:
- Same country only: cities, abbreviations, and alternate names for the same country go to that bag (e.g. "New York", "NY", "USA", "US" → united states).
- Use lowercase for canonical and in output.
- When in doubt, do NOT merge — put the string in new_groups. A false non-merge is harmless; a false merge corrupts location matching.

Existing bags (canonical + aliases):
{bags_text}

Unprocessed location strings (assign each to a bag or to new_groups):
{names_text}

Return valid JSON only with exactly two keys:
- "assignments": array of {{ "location": "<string>", "assign_to_canonical": "<existing canonical country>" }}
- "new_groups": array of location strings that did not match any existing bag

Every unprocessed string must appear in exactly one of assignments or new_groups."""


async def assign_locations_to_bags(
    openrouter: "OpenRouterResource",
    existing_bags: list[dict[str, Any]],
    unprocessed_strings: list[str],
    *,
    model: str | None = None,
    dagster_log: Any = None,
) -> dict[str, Any]:
    """Assign unprocessed location strings to existing bags or new_groups via LLM.

    Same pattern as assign_skills_to_bags: each string goes to an existing
    canonical (country) or into new_groups for later clustering.
    """
    if not unprocessed_strings:
        return {"assignments": [], "new_groups": []}

    bags_for_prompt = [
        {"canonical": b["canonical"], "aliases": b["aliases"]} for b in existing_bags
    ]
    prompt = _build_assign_locations_prompt(bags_for_prompt, unprocessed_strings)
    response = await openrouter.complete(
        messages=[{"role": "user", "content": prompt}],
        model=model or DEFAULT_MODEL,
        operation="location_assign_to_bags",
        response_format={"type": "json_object"},
        temperature=0.0,
        max_tokens=MAX_OUTPUT_TOKENS,
    )
    content = response["choices"][0]["message"]["content"]
    finish_reason = response["choices"][0].get("finish_reason", "stop")

    if finish_reason == "length" and len(unprocessed_strings) > 1:
        mid = len(unprocessed_strings) // 2
        log_fn = dagster_log.warning if dagster_log else logger.warning
        log_fn(
            "location_assign_to_bags: output truncated, bisecting %s items",
            len(unprocessed_strings),
        )
        left = await assign_locations_to_bags(
            openrouter,
            existing_bags,
            unprocessed_strings[:mid],
            model=model,
            dagster_log=dagster_log,
        )
        right = await assign_locations_to_bags(
            openrouter,
            existing_bags,
            unprocessed_strings[mid:],
            model=model,
            dagster_log=dagster_log,
        )
        return {
            "assignments": left["assignments"] + right["assignments"],
            "new_groups": left["new_groups"] + right["new_groups"],
        }

    data = json.loads(content)
    if not isinstance(data, dict):
        raise ValueError("location_assign_to_bags: LLM returned non-dict")
    assignments = data.get("assignments") or []
    new_groups = data.get("new_groups") or []
    if not isinstance(assignments, list):
        assignments = []
    if not isinstance(new_groups, list):
        new_groups = []
    return {"assignments": assignments, "new_groups": new_groups}


# ---------------------------------------------------------------------------
# 2. Cluster new_groups into new bags (canonical country + aliases)
# ---------------------------------------------------------------------------


def _build_cluster_locations_prompt(location_strings: list[str]) -> str:
    names_text = json.dumps(location_strings)
    return f"""Group the following location strings into clusters that refer to the same country. Each cluster has one **canonical** (the standard country name, lowercase) and **aliases** (the other strings in that cluster: cities, abbreviations, etc.). If a string has no equivalents in this list, include it as a cluster with that single string as canonical and empty aliases.

Location strings:
{names_text}

Return valid JSON only with exactly one key:
- "clusters": array of {{ "canonical": "<country name, lowercase>", "aliases": ["<other string>", ...] }}

Rules:
- Every location string must appear exactly once (either as canonical or in aliases of some cluster).
- "Canonical" must be a standard country name (e.g. united states, united kingdom, germany, singapore). Use the most common English form, lowercase.
- Aliases are cities, abbreviations, or alternate names that refer to that country (e.g. for united states: "ny", "new york", "usa", "us", "california" as alias is wrong — use a city/abbrev that implies the country).
- When in doubt, keep as a singleton cluster (canonical = the string normalized, aliases = [])."""


async def cluster_new_locations(
    openrouter: "OpenRouterResource",
    location_strings: list[str],
    *,
    model: str | None = None,
    dagster_log: Any = None,
) -> list[dict[str, Any]]:
    """Cluster unmatched location strings into new bags (canonical country + aliases).

    Solves cold-start: when no existing bags fit, group by country and pick canonical.
    """
    if not location_strings:
        return []

    prompt = _build_cluster_locations_prompt(location_strings)
    response = await openrouter.complete(
        messages=[{"role": "user", "content": prompt}],
        model=model or DEFAULT_MODEL,
        operation="location_clustering",
        response_format={"type": "json_object"},
        temperature=0.0,
        max_tokens=MAX_OUTPUT_TOKENS,
    )
    content = response["choices"][0]["message"]["content"]
    finish_reason = response["choices"][0].get("finish_reason", "stop")

    if finish_reason == "length" and len(location_strings) > 1:
        mid = len(location_strings) // 2
        log_fn = dagster_log.warning if dagster_log else logger.warning
        log_fn("location_clustering: output truncated, bisecting %s items", len(location_strings))
        left = await cluster_new_locations(
            openrouter, location_strings[:mid], model=model, dagster_log=dagster_log
        )
        right = await cluster_new_locations(
            openrouter, location_strings[mid:], model=model, dagster_log=dagster_log
        )
        return left + right

    data = json.loads(content)
    if not isinstance(data, dict) or "clusters" not in data:
        raise ValueError(
            'location_clustering: LLM returned invalid structure (expected { "clusters": [...] })'
        )
    clusters = data["clusters"]
    if not isinstance(clusters, list):
        return []
    return [c for c in clusters if isinstance(c, dict) and "canonical" in c]


# ---------------------------------------------------------------------------
# 3. City bags (canonical city slug + aliases)
# ---------------------------------------------------------------------------


def _build_assign_cities_prompt(
    existing_bags: list[dict[str, Any]],
    unprocessed_strings: list[str],
) -> str:
    bags_text = json.dumps(existing_bags)
    names_text = json.dumps(unprocessed_strings)
    return f"""We have existing **city bags** (each bag has one canonical city slug, e.g. new_york, and a list of equivalent aliases: abbreviations, alternate spellings). Below are **new location strings** that are likely cities. For each new string, assign it to the canonical of an existing bag if it refers to the same city (e.g. "NYC" → new_york, "New York City" → new_york). If no existing bag fits, put the string in new_groups. Use only canonicals from the existing bags.

Rules:
- Same city only: abbreviations and alternate names for the same city go to that bag.
- Canonical is a lowercase slug (e.g. new_york, london, san_francisco). Use only canonicals from existing bags.
- If the string is a country or region (e.g. USA, Europe), put it in new_groups — we will not add those to the city table.
- When in doubt, put in new_groups.

Existing bags (canonical + aliases):
{bags_text}

Unprocessed strings (assign each to a bag or to new_groups):
{names_text}

Return valid JSON only:
- "assignments": array of {{ "location": "<string>", "assign_to_canonical": "<existing city slug>" }}
- "new_groups": array of strings that did not match any bag

Every unprocessed string must appear in exactly one of assignments or new_groups."""


async def assign_cities_to_bags(
    openrouter: "OpenRouterResource",
    existing_bags: list[dict[str, Any]],
    unprocessed_strings: list[str],
    *,
    model: str | None = None,
    dagster_log: Any = None,
) -> dict[str, Any]:
    """Assign unprocessed city-like strings to existing city bags or new_groups."""
    if not unprocessed_strings:
        return {"assignments": [], "new_groups": []}

    bags_for_prompt = [
        {"canonical": b["canonical"], "aliases": b["aliases"]} for b in existing_bags
    ]
    prompt = _build_assign_cities_prompt(bags_for_prompt, unprocessed_strings)
    response = await openrouter.complete(
        messages=[{"role": "user", "content": prompt}],
        model=model or DEFAULT_MODEL,
        operation="city_assign_to_bags",
        response_format={"type": "json_object"},
        temperature=0.0,
        max_tokens=MAX_OUTPUT_TOKENS,
    )
    content = response["choices"][0]["message"]["content"]
    data = json.loads(content)
    assignments = data.get("assignments") or []
    new_groups = data.get("new_groups") or []
    if not isinstance(assignments, list):
        assignments = []
    if not isinstance(new_groups, list):
        new_groups = []
    return {"assignments": assignments, "new_groups": new_groups}


def _build_cluster_cities_prompt(location_strings: list[str]) -> str:
    names_text = json.dumps(location_strings)
    return f"""Group the following location strings into clusters that refer to the same **city**. Each cluster has one **canonical** (a lowercase slug, e.g. new_york, london, san_francisco — use underscores) and **aliases** (the other strings: abbreviations, alternate spellings). If a string has no equivalents, include it as a cluster with that string as canonical (slug-ified) and empty aliases.

Location strings (these are city-like only; do not cluster countries or regions):
{names_text}

Return valid JSON only:
- "clusters": array of {{ "canonical": "<city slug, lowercase, underscores>", "aliases": ["<other string>", ...] }}

Rules:
- Every string must appear exactly once (as canonical or in aliases).
- Canonical must be a city slug (e.g. new_york), not a country name.
- When in doubt, singleton cluster."""


async def cluster_new_cities(
    openrouter: "OpenRouterResource",
    location_strings: list[str],
    *,
    model: str | None = None,
    dagster_log: Any = None,
) -> list[dict[str, Any]]:
    """Cluster unmatched city-like strings into new city bags (canonical slug + aliases)."""
    if not location_strings:
        return []

    prompt = _build_cluster_cities_prompt(location_strings)
    response = await openrouter.complete(
        messages=[{"role": "user", "content": prompt}],
        model=model or DEFAULT_MODEL,
        operation="city_clustering",
        response_format={"type": "json_object"},
        temperature=0.0,
        max_tokens=MAX_OUTPUT_TOKENS,
    )
    content = response["choices"][0]["message"]["content"]
    data = json.loads(content)
    if not isinstance(data, dict) or "clusters" not in data:
        raise ValueError('city_clustering: expected { "clusters": [...] }')
    clusters = data["clusters"]
    if not isinstance(clusters, list):
        return []
    return [c for c in clusters if isinstance(c, dict) and "canonical" in c]


# ---------------------------------------------------------------------------
# 4. Region bags (canonical region name + aliases)
# ---------------------------------------------------------------------------


def _build_assign_regions_prompt(
    existing_bags: list[dict[str, Any]],
    unprocessed_strings: list[str],
) -> str:
    bags_text = json.dumps(existing_bags)
    names_text = json.dumps(unprocessed_strings)
    return f"""We have existing **region bags** (each bag has one canonical region name, e.g. europe, asia, and a list of equivalent aliases: abbreviations like EU, EMEA). Below are **new location strings** that are likely regions. For each new string, assign it to the canonical of an existing bag if it refers to the same region. If no existing bag fits, put the string in new_groups. Use only canonicals from the existing bags.

Rules:
- Same region only. Use only canonicals from existing bags (lowercase).
- If the string is a country or city, put it in new_groups.
- When in doubt, put in new_groups.

Existing bags (canonical + aliases):
{bags_text}

Unprocessed strings:
{names_text}

Return valid JSON only:
- "assignments": array of {{ "location": "<string>", "assign_to_canonical": "<existing region>" }}
- "new_groups": array of strings that did not match any bag

Every unprocessed string must appear in exactly one of assignments or new_groups."""


async def assign_regions_to_bags(
    openrouter: "OpenRouterResource",
    existing_bags: list[dict[str, Any]],
    unprocessed_strings: list[str],
    *,
    model: str | None = None,
    dagster_log: Any = None,
) -> dict[str, Any]:
    """Assign unprocessed region-like strings to existing region bags or new_groups."""
    if not unprocessed_strings:
        return {"assignments": [], "new_groups": []}

    bags_for_prompt = [
        {"canonical": b["canonical"], "aliases": b["aliases"]} for b in existing_bags
    ]
    prompt = _build_assign_regions_prompt(bags_for_prompt, unprocessed_strings)
    response = await openrouter.complete(
        messages=[{"role": "user", "content": prompt}],
        model=model or DEFAULT_MODEL,
        operation="region_assign_to_bags",
        response_format={"type": "json_object"},
        temperature=0.0,
        max_tokens=MAX_OUTPUT_TOKENS,
    )
    content = response["choices"][0]["message"]["content"]
    data = json.loads(content)
    assignments = data.get("assignments") or []
    new_groups = data.get("new_groups") or []
    if not isinstance(assignments, list):
        assignments = []
    if not isinstance(new_groups, list):
        new_groups = []
    return {"assignments": assignments, "new_groups": new_groups}


def _build_cluster_regions_prompt(location_strings: list[str]) -> str:
    names_text = json.dumps(location_strings)
    return f"""Group the following location strings into clusters that refer to the same **region** (e.g. Europe, EMEA, Asia-Pacific). Each cluster has one **canonical** (lowercase region name, e.g. europe, emea, apac) and **aliases** (the other strings). If a string has no equivalents, include it as a singleton cluster.

Location strings (region-like only):
{names_text}

Return valid JSON only:
- "clusters": array of {{ "canonical": "<region name, lowercase>", "aliases": ["<other string>", ...] }}

Rules:
- Every string must appear exactly once. Canonical must be a region name, not a country."""


async def cluster_new_regions(
    openrouter: "OpenRouterResource",
    location_strings: list[str],
    *,
    model: str | None = None,
    dagster_log: Any = None,
) -> list[dict[str, Any]]:
    """Cluster unmatched region-like strings into new region bags."""
    if not location_strings:
        return []

    prompt = _build_cluster_regions_prompt(location_strings)
    response = await openrouter.complete(
        messages=[{"role": "user", "content": prompt}],
        model=model or DEFAULT_MODEL,
        operation="region_clustering",
        response_format={"type": "json_object"},
        temperature=0.0,
        max_tokens=MAX_OUTPUT_TOKENS,
    )
    content = response["choices"][0]["message"]["content"]
    data = json.loads(content)
    if not isinstance(data, dict) or "clusters" not in data:
        raise ValueError('region_clustering: expected { "clusters": [...] }')
    clusters = data["clusters"]
    if not isinstance(clusters, list):
        return []
    return [c for c in clusters if isinstance(c, dict) and "canonical" in c]


# ---------------------------------------------------------------------------
# Flat map (used by seed script when no bags exist yet)
# ---------------------------------------------------------------------------


def _build_normalize_locations_prompt(
    existing_canonical_countries: list[str],
    location_strings: list[str],
) -> str:
    countries_text = json.dumps(sorted(existing_canonical_countries))
    locations_text = json.dumps(location_strings)
    return f"""You are a geographic location normalizer. Map each of the following location strings to exactly one canonical country from the allowed list. Location strings may be city names, abbreviations (e.g. NY, UK, UAE), or informal names; map them to the correct country in lowercase.

Allowed canonical countries (use only these exact strings, lowercase):
{countries_text}

Location strings to map:
{locations_text}

Rules:
- Use only country names from the allowed list. Output the exact string (lowercase).
- Cities and abbreviations: map to the country they belong to (e.g. NY → united states, London → united kingdom, Singapore → singapore).
- If a location is ambiguous (e.g. "Georgia" could be country or US state), prefer the country that is most commonly meant in an international job-matching context.
- If you truly cannot determine the country for a string, use "unknown" only for that item (we will skip inserting it).

Return valid JSON only with exactly one key:
- "mappings": array of {{ "location": "<original string, lowercase>", "country_canonical": "<country from allowed list or 'unknown'>" }}

Every location string must appear exactly once in the mappings array."""


async def normalize_locations_to_countries(
    openrouter: "OpenRouterResource",
    unprocessed_strings: list[str],
    existing_canonical_countries: list[str],
    *,
    model: str | None = None,
    dagster_log: Any = None,
) -> list[dict[str, str]]:
    """Map location strings to canonical countries via LLM.

    Args:
        openrouter: OpenRouter resource for API calls.
        unprocessed_strings: List of location strings (cities, abbreviations, etc.) to map.
        existing_canonical_countries: Allowed canonical country names (lowercase).
        model: Model to use; defaults to gpt-4o.
        dagster_log: Optional Dagster logger.

    Returns:
        List of { "location": str, "country_canonical": str }. Skip entries with
        country_canonical == "unknown" when applying to DB.
    """
    if not unprocessed_strings or not existing_canonical_countries:
        return []

    prompt = _build_normalize_locations_prompt(
        existing_canonical_countries,
        unprocessed_strings,
    )
    response = await openrouter.complete(
        messages=[{"role": "user", "content": prompt}],
        model=model or DEFAULT_MODEL,
        operation="location_normalization",
        response_format={"type": "json_object"},
        temperature=0.0,
        max_tokens=MAX_OUTPUT_TOKENS,
    )
    content = response["choices"][0]["message"]["content"]
    finish_reason = response["choices"][0].get("finish_reason", "stop")

    if finish_reason == "length":
        if len(unprocessed_strings) <= 1:
            raise RuntimeError("location_normalization: output truncated even with a single item")
        mid = len(unprocessed_strings) // 2
        log_fn = dagster_log.warning if dagster_log else logger.warning
        log_fn(
            f"location_normalization: output truncated for {len(unprocessed_strings)} items — bisecting"
        )
        left = await normalize_locations_to_countries(
            openrouter,
            unprocessed_strings[:mid],
            existing_canonical_countries,
            model=model,
            dagster_log=dagster_log,
        )
        right = await normalize_locations_to_countries(
            openrouter,
            unprocessed_strings[mid:],
            existing_canonical_countries,
            model=model,
            dagster_log=dagster_log,
        )
        return left + right

    data = json.loads(content)
    if not isinstance(data, dict) or "mappings" not in data:
        msg = 'location_normalization: LLM returned invalid structure (expected { "mappings": [...] })'
        if dagster_log:
            dagster_log.error(msg)
        raise ValueError(msg)
    mappings = data["mappings"]
    if not isinstance(mappings, list):
        raise ValueError("location_normalization: mappings is not a list")
    return [
        m for m in mappings if isinstance(m, dict) and "location" in m and "country_canonical" in m
    ]


def _build_assign_countries_to_regions_prompt(
    region_names: list[str],
    country_list: list[str],
) -> str:
    regions_text = json.dumps(sorted(region_names))
    countries_text = json.dumps(sorted(country_list))
    return f"""You are a geographic classifier. Assign each of the following countries to one or more regions from the allowed list. Output lowercase.

Allowed regions (use only these exact strings, lowercase):
{regions_text}

Countries to assign:
{countries_text}

Rules:
- Each country can belong to one or more regions (e.g. Russia in both europe and asia; Turkey in europe and middle east).
- Use only region names from the allowed list.
- Return one entry per (region, country) pair so that a country in two regions appears twice.

Return valid JSON only with exactly one key:
- "pairs": array of {{ "region": "<region name>", "country": "<country name>" }}

Include every country at least once."""


async def assign_countries_to_regions(
    openrouter: "OpenRouterResource",
    countries: list[str],
    region_names: list[str],
    *,
    model: str | None = None,
    dagster_log: Any = None,
) -> list[dict[str, str]]:
    """Assign each country to one or more regions via LLM (for seed).

    Args:
        openrouter: OpenRouter resource.
        countries: List of country names (lowercase) to assign.
        region_names: Allowed region names (e.g. europe, north america, asia).
        model: Model to use.
        dagster_log: Optional Dagster logger.

    Returns:
        List of { "region": str, "country": str } for inserting into location_region_countries.
    """
    if not countries or not region_names:
        return []

    prompt = _build_assign_countries_to_regions_prompt(region_names, countries)
    response = await openrouter.complete(
        messages=[{"role": "user", "content": prompt}],
        model=model or DEFAULT_MODEL,
        operation="location_region_assignment",
        response_format={"type": "json_object"},
        temperature=0.0,
        max_tokens=MAX_OUTPUT_TOKENS,
    )
    content = response["choices"][0]["message"]["content"]
    data = json.loads(content)
    if not isinstance(data, dict) or "pairs" not in data:
        raise ValueError(
            'location_region_assignment: LLM returned invalid structure (expected { "pairs": [...] })'
        )
    pairs = data["pairs"]
    if not isinstance(pairs, list):
        raise ValueError("location_region_assignment: pairs is not a list")
    return [p for p in pairs if isinstance(p, dict) and "region" in p and "country" in p]
