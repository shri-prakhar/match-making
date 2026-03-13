"""Location normalization job: assign unprocessed location strings to bags via LLM (like skill normalization).

No seed script: run this job once (or on schedule); it discovers locations from candidates and jobs,
assigns them to existing bags or clusters into new ones, and writes location_country_aliases.
On an empty DB with no candidate/job data, unprocessed is empty and the job is a no-op.
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
    assign_locations_to_bags,
    cluster_new_locations,
)
from talent_matching.matchmaking.location_filter import (
    REGION_COUNTRIES,
    parse_job_preferred_locations,
)
from talent_matching.models.candidates import NormalizedCandidate
from talent_matching.models.location import LocationCountryAlias, LocationRegionCountry
from talent_matching.models.raw import RawJob


@op(description="Load existing location bags and unprocessed location strings from DB")
def get_existing_bags_and_unprocessed_locations(context: OpExecutionContext) -> dict:
    """Return existing bags (canonical country + aliases) and unprocessed location strings."""
    session = get_session()

    # Build bags: group by country_canonical -> list of alias
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
            all_aliases.add(country)  # canonicals count as known

    existing_bags = [
        {"canonical": country, "aliases": sorted(aliases)}
        for country, aliases in sorted(bag_map.items())
    ]

    # Region names to exclude from unprocessed (we don't map "europe" -> country)
    region_rows = session.execute(select(LocationRegionCountry.region).distinct()).all()
    known_region_keys = {r[0].strip().lower() for r in region_rows if r[0]}
    if not known_region_keys:
        known_region_keys = set(REGION_COUNTRIES.keys())

    # Collect distinct location strings from candidates and jobs
    cand_loc = set()
    for col in ("location_city", "location_country", "location_region"):
        rows = session.execute(
            select(getattr(NormalizedCandidate, col)).where(
                getattr(NormalizedCandidate, col).is_not(None),
                getattr(NormalizedCandidate, col) != "",
            )
        ).all()
        for (v,) in rows:
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

    # Unprocessed: not in any bag (alias set), not a region name
    unprocessed = sorted(cand_loc - all_aliases - known_region_keys)

    context.log.info(
        f"Location normalization: {len(existing_bags)} existing bags, {len(unprocessed)} unprocessed strings"
    )
    return {
        "existing_bags": existing_bags,
        "unprocessed": unprocessed,
    }


@op(
    required_resource_keys={"openrouter"},
    tags={"dagster/concurrency_key": "openrouter_api"},
    description="Assign unprocessed location strings to existing bags or new_groups via LLM",
)
def assign_locations_to_bags_op(context: OpExecutionContext, data: dict) -> dict:
    """Call LLM to assign each unprocessed string to an existing bag or new_groups."""
    existing_bags = data["existing_bags"]
    unprocessed = data["unprocessed"]
    if not unprocessed:
        context.log.info("No unprocessed locations; skipping LLM")
        return {
            "assignments": [],
            "new_groups": [],
            "existing_bags": existing_bags,
        }

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
        f"LLM returned {len(result.get('assignments') or [])} assignments and "
        f"{len(result.get('new_groups') or [])} new_groups"
    )
    return result


@op(
    required_resource_keys={"openrouter"},
    tags={"dagster/concurrency_key": "openrouter_api"},
    description="Cluster new_groups into new location bags via LLM",
)
def cluster_unmatched_locations(context: OpExecutionContext, data: dict) -> dict:
    """Cluster new_groups into new bags (canonical country + aliases)."""
    new_groups = data.get("new_groups") or []
    if not new_groups:
        context.log.info("No new_groups to cluster; skipping")
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
    context.log.info(f"Clustered {len(new_groups)} unmatched into {len(clusters)} location bags")
    return data


@op(description="Apply assignments and clusters: insert location_country_aliases rows")
def apply_location_assignments(context: OpExecutionContext, data: dict) -> dict:
    """Write alias rows for assignments (to existing canonicals) and for new clusters."""
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
        if not loc or not canonical:
            continue
        if canonical not in canonical_set:
            skipped += 1
            context.log.warning(
                "Assignment canonical %r not in existing bags; skipping %r",
                canonical,
                loc,
            )
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
        # Insert canonical -> canonical so the country name itself resolves
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
            if not a or a == canonical:
                continue
            if a in existing_lower:
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

    context.log.info(f"Applied {applied} location aliases ({skipped} skipped from assignments)")
    return {"applied": applied, "skipped": skipped}


@job(
    description="Normalize unprocessed location strings into bags via LLM (assign + cluster, like skill normalization)",
    op_retry_policy=openrouter_retry_policy,
)
def location_normalization_job():
    """Discover location strings, assign to existing bags or cluster into new bags, insert aliases."""
    data = get_existing_bags_and_unprocessed_locations()
    with_assign = assign_locations_to_bags_op(data)
    with_clusters = cluster_unmatched_locations(with_assign)
    apply_location_assignments(with_clusters)


location_normalization_schedule = ScheduleDefinition(
    name="location_normalization_daily",
    cron_schedule="0 3 * * *",
    job=location_normalization_job,
    description="Run location normalization daily at 03:00 UTC",
)
