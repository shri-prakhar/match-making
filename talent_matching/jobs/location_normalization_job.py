"""Single location normalization job: country, then city, then region (assign + cluster, write aliases).

Runs nightly. Discovers location strings from candidates and jobs, assigns to existing bags
or clusters into new ones, writes location_country_aliases, location_city_aliases,
location_region_aliases. On an empty DB with no candidate/job data, each step is a no-op.
"""

import asyncio
from collections import defaultdict
from uuid import uuid4

from dagster import OpExecutionContext, ScheduleDefinition, job, op
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert

from talent_matching.db import get_session
from talent_matching.jobs.asset_jobs import openrouter_retry_policy
from talent_matching.llm.operations.normalize_locations import (
    assign_cities_to_bags,
    assign_locations_to_bags,
    assign_regions_to_bags,
    cluster_new_cities,
    cluster_new_locations,
    cluster_new_regions,
)
from talent_matching.matchmaking.location_filter import (
    REGION_COUNTRIES,
    parse_job_preferred_locations,
)
from talent_matching.models.candidates import NormalizedCandidate
from talent_matching.models.location import (
    LocationCityAlias,
    LocationCountryAlias,
    LocationRegionAlias,
    LocationRegionCountry,
)
from talent_matching.models.raw import RawJob

# ----- Country -----


@op(description="Load existing country bags and unprocessed location strings from DB")
def get_existing_bags_and_unprocessed_locations(context: OpExecutionContext) -> dict:
    """Return existing bags (canonical country + aliases) and unprocessed location strings."""
    session = get_session()

    rows = session.execute(
        select(
            func.lower(LocationCountryAlias.country_canonical).label("country"),
            func.lower(LocationCountryAlias.alias).label("alias"),
        )
    ).all()
    bag_map: dict[str, list[str]] = defaultdict(list)
    all_aliases: set[str] = set()
    for country, alias in rows:
        if country and alias:
            bag_map[country].append(alias)
            all_aliases.add(alias)
        if country:
            all_aliases.add(country)

    existing_bags = [
        {"canonical": country, "aliases": sorted(aliases)}
        for country, aliases in sorted(bag_map.items())
    ]

    region_rows = session.execute(select(LocationRegionCountry.region).distinct()).all()
    known_region_keys = {r[0].strip().lower() for r in region_rows if r[0]}
    if not known_region_keys:
        known_region_keys = set(REGION_COUNTRIES.keys())

    cand_loc = set()
    for col in ("location_city", "location_country", "location_region"):
        r = session.execute(
            select(getattr(NormalizedCandidate, col)).where(
                getattr(NormalizedCandidate, col).is_not(None),
                getattr(NormalizedCandidate, col) != "",
            )
        ).all()
        for (v,) in r:
            if v and str(v).strip():
                cand_loc.add(str(v).strip().lower())

    raw_rows = session.execute(
        select(RawJob.location_raw).where(
            RawJob.location_raw.is_not(None),
            RawJob.location_raw != "",
        )
    ).all()
    for (location_raw,) in raw_rows:
        if location_raw:
            parsed = parse_job_preferred_locations(location_raw)
            if parsed:
                for p in parsed:
                    if p and p.strip():
                        cand_loc.add(p.strip().lower())

    session.close()

    unprocessed = sorted(cand_loc - all_aliases - known_region_keys)

    context.log.info(f"Country: {len(existing_bags)} existing bags, {len(unprocessed)} unprocessed")
    return {"existing_bags": existing_bags, "unprocessed": unprocessed}


@op(
    required_resource_keys={"openrouter"},
    tags={"dagster/concurrency_key": "openrouter_api"},
    description="Assign unprocessed location strings to country bags or new_groups via LLM",
)
def assign_locations_to_bags_op(context: OpExecutionContext, data: dict) -> dict:
    existing_bags = data["existing_bags"]
    unprocessed = data["unprocessed"]
    if not unprocessed:
        context.log.info("No unprocessed; skipping LLM")
        return {"assignments": [], "new_groups": [], "existing_bags": existing_bags}

    result = asyncio.run(
        assign_locations_to_bags(
            context.resources.openrouter,
            existing_bags,
            unprocessed,
            dagster_log=context.log,
        )
    )
    result["existing_bags"] = existing_bags
    context.log.info(
        f"LLM returned {len(result.get('assignments') or [])} assignments, "
        f"{len(result.get('new_groups') or [])} new_groups"
    )
    return result


@op(
    required_resource_keys={"openrouter"},
    tags={"dagster/concurrency_key": "openrouter_api"},
    description="Cluster new_groups into new country bags via LLM",
)
def cluster_unmatched_locations(context: OpExecutionContext, data: dict) -> dict:
    new_groups = data.get("new_groups") or []
    if not new_groups:
        data["clusters"] = []
        return data

    clusters = asyncio.run(
        cluster_new_locations(
            context.resources.openrouter,
            new_groups,
            dagster_log=context.log,
        )
    )
    data["clusters"] = clusters
    context.log.info(f"Clustered {len(new_groups)} into {len(clusters)} country bags")
    return data


@op(description="Apply country assignments and clusters: insert location_country_aliases")
def apply_country_assignments(context: OpExecutionContext, data: dict) -> dict:
    assignments = data.get("assignments") or []
    existing_bags = data.get("existing_bags") or []
    clusters = data.get("clusters") or []

    canonical_set = {b["canonical"] for b in existing_bags}
    session = get_session()
    existing_lower = {
        row[0] for row in session.execute(select(func.lower(LocationCountryAlias.alias))).all()
    }

    applied = 0
    skipped = 0

    for item in assignments:
        loc = (item.get("location") or "").strip().lower()
        canonical = (item.get("assign_to_canonical") or "").strip().lower()
        if not loc or not canonical or canonical not in canonical_set:
            skipped += 1
            continue
        if loc in existing_lower:
            skipped += 1
            continue
        session.execute(
            insert(LocationCountryAlias).values(
                id=uuid4(),
                alias=loc,
                country_canonical=canonical,
                added_by="llm",
            )
        )
        existing_lower.add(loc)
        applied += 1

    for cluster in clusters:
        canonical = (cluster.get("canonical") or "").strip().lower()
        aliases = cluster.get("aliases") or []
        if not canonical:
            continue
        if canonical not in existing_lower:
            session.execute(
                insert(LocationCountryAlias).values(
                    id=uuid4(),
                    alias=canonical,
                    country_canonical=canonical,
                    added_by="llm",
                )
            )
            existing_lower.add(canonical)
            applied += 1
        for alias in aliases:
            a = (alias or "").strip().lower()
            if not a or a == canonical or a in existing_lower:
                continue
            session.execute(
                insert(LocationCountryAlias).values(
                    id=uuid4(),
                    alias=a,
                    country_canonical=canonical,
                    added_by="llm",
                )
            )
            existing_lower.add(a)
            applied += 1

    session.commit()
    session.close()
    context.log.info(f"Country: applied {applied} aliases ({skipped} skipped)")
    return {"applied": applied, "skipped": skipped}


# ----- City (runs after country) -----


@op(description="Load existing city bags and unprocessed location strings from DB")
def get_existing_city_bags_and_unprocessed(
    context: OpExecutionContext, _after_country: dict
) -> dict:
    """Return existing city bags and unprocessed strings. Runs after country step."""
    session = get_session()

    rows = session.execute(
        select(
            func.lower(LocationCityAlias.city_canonical).label("city"),
            func.lower(LocationCityAlias.alias).label("alias"),
        )
    ).all()
    bag_map = defaultdict(list)
    all_aliases = set()
    for city, alias in rows:
        if city and alias:
            bag_map[city].append(alias)
            all_aliases.add(alias)
        if city:
            all_aliases.add(city)

    existing_bags = [
        {"canonical": city, "aliases": sorted(aliases)} for city, aliases in sorted(bag_map.items())
    ]

    region_rows = session.execute(select(LocationRegionCountry.region).distinct()).all()
    known_region_keys = {r[0].strip().lower() for r in region_rows if r[0]}
    if not known_region_keys:
        known_region_keys = set(REGION_COUNTRIES.keys())

    cand_loc = set()
    for col in ("location_city", "location_country", "location_region"):
        r = session.execute(
            select(getattr(NormalizedCandidate, col)).where(
                getattr(NormalizedCandidate, col).is_not(None),
                getattr(NormalizedCandidate, col) != "",
            )
        ).all()
        for (v,) in r:
            if v and str(v).strip():
                cand_loc.add(str(v).strip().lower())

    raw_rows = session.execute(
        select(RawJob.location_raw).where(
            RawJob.location_raw.is_not(None),
            RawJob.location_raw != "",
        )
    ).all()
    for (location_raw,) in raw_rows:
        if location_raw:
            parsed = parse_job_preferred_locations(location_raw)
            if parsed:
                for p in parsed:
                    if p and p.strip():
                        cand_loc.add(p.strip().lower())

    session.close()

    unprocessed = sorted(cand_loc - all_aliases - known_region_keys)

    context.log.info(f"City: {len(existing_bags)} existing bags, {len(unprocessed)} unprocessed")
    return {"existing_bags": existing_bags, "unprocessed": unprocessed}


@op(
    required_resource_keys={"openrouter"},
    tags={"dagster/concurrency_key": "openrouter_api"},
    description="Assign unprocessed strings to city bags or new_groups via LLM",
)
def assign_cities_to_bags_op(context: OpExecutionContext, data: dict) -> dict:
    existing_bags = data["existing_bags"]
    unprocessed = data["unprocessed"]
    if not unprocessed:
        context.log.info("No unprocessed; skipping LLM")
        return {"assignments": [], "new_groups": [], "existing_bags": existing_bags}

    result = asyncio.run(
        assign_cities_to_bags(
            context.resources.openrouter,
            existing_bags,
            unprocessed,
            dagster_log=context.log,
        )
    )
    result["existing_bags"] = existing_bags
    return result


@op(
    required_resource_keys={"openrouter"},
    tags={"dagster/concurrency_key": "openrouter_api"},
    description="Cluster new_groups into new city bags via LLM",
)
def cluster_unmatched_cities(context: OpExecutionContext, data: dict) -> dict:
    new_groups = data.get("new_groups") or []
    if not new_groups:
        data["clusters"] = []
        return data

    clusters = asyncio.run(
        cluster_new_cities(
            context.resources.openrouter,
            new_groups,
            dagster_log=context.log,
        )
    )
    data["clusters"] = clusters
    return data


@op(description="Apply city assignments and clusters: insert location_city_aliases")
def apply_city_assignments(context: OpExecutionContext, data: dict) -> dict:
    assignments = data.get("assignments") or []
    existing_bags = data.get("existing_bags") or []
    clusters = data.get("clusters") or []

    canonical_set = {b["canonical"] for b in existing_bags}
    session = get_session()
    existing_lower = {
        row[0] for row in session.execute(select(func.lower(LocationCityAlias.alias))).all()
    }

    applied = 0
    skipped = 0

    for item in assignments:
        loc = (item.get("location") or "").strip().lower()
        canonical = (item.get("assign_to_canonical") or "").strip().lower()
        if not loc or not canonical or canonical not in canonical_set:
            skipped += 1
            continue
        if loc in existing_lower:
            skipped += 1
            continue
        session.execute(
            insert(LocationCityAlias).values(
                id=uuid4(),
                alias=loc,
                city_canonical=canonical,
                added_by="llm",
            )
        )
        existing_lower.add(loc)
        applied += 1

    for cluster in clusters:
        canonical = (cluster.get("canonical") or "").strip().lower()
        aliases = cluster.get("aliases") or []
        if not canonical:
            continue
        if canonical not in existing_lower:
            session.execute(
                insert(LocationCityAlias).values(
                    id=uuid4(),
                    alias=canonical,
                    city_canonical=canonical,
                    added_by="llm",
                )
            )
            existing_lower.add(canonical)
            applied += 1
        for alias in aliases:
            a = (alias or "").strip().lower()
            if not a or a == canonical or a in existing_lower:
                continue
            session.execute(
                insert(LocationCityAlias).values(
                    id=uuid4(),
                    alias=a,
                    city_canonical=canonical,
                    added_by="llm",
                )
            )
            existing_lower.add(a)
            applied += 1

    session.commit()
    session.close()
    context.log.info(f"City: applied {applied} aliases ({skipped} skipped)")
    return {"applied": applied, "skipped": skipped}


# ----- Region (runs after city) -----


@op(description="Load existing region bags and unprocessed region-like strings from DB")
def get_existing_region_bags_and_unprocessed(
    context: OpExecutionContext, _after_city: dict
) -> dict:
    """Return existing region bags and unprocessed strings. Runs after city step."""
    session = get_session()

    rows = session.execute(
        select(
            func.lower(LocationRegionAlias.region_canonical).label("region"),
            func.lower(LocationRegionAlias.alias).label("alias"),
        )
    ).all()
    bag_map = defaultdict(list)
    all_aliases = set()
    for region, alias in rows:
        if region and alias:
            bag_map[region].append(alias)
            all_aliases.add(alias)
        if region:
            all_aliases.add(region)

    existing_bags = [
        {"canonical": region, "aliases": sorted(aliases)}
        for region, aliases in sorted(bag_map.items())
    ]

    region_countries_rows = session.execute(select(LocationRegionCountry.region).distinct()).all()
    known_region_keys = {r[0].strip().lower() for r in region_countries_rows if r[0]}
    if not known_region_keys:
        known_region_keys = set(REGION_COUNTRIES.keys())

    cand_region = set()
    r = session.execute(
        select(NormalizedCandidate.location_region).where(
            NormalizedCandidate.location_region.is_not(None),
            NormalizedCandidate.location_region != "",
        )
    ).all()
    for (v,) in r:
        if v and str(v).strip():
            cand_region.add(str(v).strip().lower())

    raw_rows = session.execute(
        select(RawJob.location_raw).where(
            RawJob.location_raw.is_not(None),
            RawJob.location_raw != "",
        )
    ).all()
    for (location_raw,) in raw_rows:
        if location_raw:
            parsed = parse_job_preferred_locations(location_raw)
            if parsed:
                for p in parsed:
                    n = (p or "").strip().lower()
                    if n and n in known_region_keys:
                        cand_region.add(n)

    session.close()

    unprocessed = sorted(cand_region - all_aliases)

    context.log.info(f"Region: {len(existing_bags)} existing bags, {len(unprocessed)} unprocessed")
    return {"existing_bags": existing_bags, "unprocessed": unprocessed}


@op(
    required_resource_keys={"openrouter"},
    tags={"dagster/concurrency_key": "openrouter_api"},
    description="Assign unprocessed region strings to region bags or new_groups via LLM",
)
def assign_regions_to_bags_op(context: OpExecutionContext, data: dict) -> dict:
    existing_bags = data["existing_bags"]
    unprocessed = data["unprocessed"]
    if not unprocessed:
        context.log.info("No unprocessed; skipping LLM")
        return {"assignments": [], "new_groups": [], "existing_bags": existing_bags}

    result = asyncio.run(
        assign_regions_to_bags(
            context.resources.openrouter,
            existing_bags,
            unprocessed,
            dagster_log=context.log,
        )
    )
    result["existing_bags"] = existing_bags
    return result


@op(
    required_resource_keys={"openrouter"},
    tags={"dagster/concurrency_key": "openrouter_api"},
    description="Cluster new_groups into new region bags via LLM",
)
def cluster_unmatched_regions(context: OpExecutionContext, data: dict) -> dict:
    new_groups = data.get("new_groups") or []
    if not new_groups:
        data["clusters"] = []
        return data

    clusters = asyncio.run(
        cluster_new_regions(
            context.resources.openrouter,
            new_groups,
            dagster_log=context.log,
        )
    )
    data["clusters"] = clusters
    return data


@op(description="Apply region assignments and clusters: insert location_region_aliases")
def apply_region_assignments(context: OpExecutionContext, data: dict) -> dict:
    assignments = data.get("assignments") or []
    existing_bags = data.get("existing_bags") or []
    clusters = data.get("clusters") or []

    canonical_set = {b["canonical"] for b in existing_bags}
    session = get_session()
    existing_lower = {
        row[0] for row in session.execute(select(func.lower(LocationRegionAlias.alias))).all()
    }

    applied = 0
    skipped = 0

    for item in assignments:
        loc = (item.get("location") or "").strip().lower()
        canonical = (item.get("assign_to_canonical") or "").strip().lower()
        if not loc or not canonical or canonical not in canonical_set:
            skipped += 1
            continue
        if loc in existing_lower:
            skipped += 1
            continue
        session.execute(
            insert(LocationRegionAlias).values(
                id=uuid4(),
                alias=loc,
                region_canonical=canonical,
                added_by="llm",
            )
        )
        existing_lower.add(loc)
        applied += 1

    for cluster in clusters:
        canonical = (cluster.get("canonical") or "").strip().lower()
        aliases = cluster.get("aliases") or []
        if not canonical:
            continue
        if canonical not in existing_lower:
            session.execute(
                insert(LocationRegionAlias).values(
                    id=uuid4(),
                    alias=canonical,
                    region_canonical=canonical,
                    added_by="llm",
                )
            )
            existing_lower.add(canonical)
            applied += 1
        for alias in aliases:
            a = (alias or "").strip().lower()
            if not a or a == canonical or a in existing_lower:
                continue
            session.execute(
                insert(LocationRegionAlias).values(
                    id=uuid4(),
                    alias=a,
                    region_canonical=canonical,
                    added_by="llm",
                )
            )
            existing_lower.add(a)
            applied += 1

    session.commit()
    session.close()
    context.log.info(f"Region: applied {applied} aliases ({skipped} skipped)")
    return {"applied": applied, "skipped": skipped}


# ----- Single job + schedule -----


@job(
    description="Normalize location strings into country, city, and region bags (run nightly)",
    op_retry_policy=openrouter_retry_policy,
)
def location_normalization_job():
    """Run country → city → region normalization in sequence."""
    data_country = get_existing_bags_and_unprocessed_locations()
    country_assign = assign_locations_to_bags_op(data_country)
    country_cluster = cluster_unmatched_locations(country_assign)
    country_apply = apply_country_assignments(country_cluster)

    data_city = get_existing_city_bags_and_unprocessed(country_apply)
    city_assign = assign_cities_to_bags_op(data_city)
    city_cluster = cluster_unmatched_cities(city_assign)
    city_apply = apply_city_assignments(city_cluster)

    data_region = get_existing_region_bags_and_unprocessed(city_apply)
    region_assign = assign_regions_to_bags_op(data_region)
    region_cluster = cluster_unmatched_regions(region_assign)
    apply_region_assignments(region_cluster)


location_normalization_schedule = ScheduleDefinition(
    name="location_normalization_daily",
    cron_schedule="0 3 * * *",
    job=location_normalization_job,
    description="Run location normalization (country, city, region) daily at 03:00 UTC",
)
