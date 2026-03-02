"""Timezone lookup job: resolve candidate (city, country) to IANA timezones via LLM."""

import asyncio

from dagster import OpExecutionContext, ScheduleDefinition, job, op
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert

from talent_matching.db import get_session
from talent_matching.jobs.asset_jobs import openrouter_retry_policy
from talent_matching.llm.operations.resolve_timezones import resolve_timezones
from talent_matching.models.candidates import NormalizedCandidate
from talent_matching.models.location_timezones import LocationTimezone


@op(description="Find unique (city, country) pairs not yet resolved in location_timezones")
def get_unresolved_locations(context: OpExecutionContext) -> dict:
    session = get_session()

    rows = session.execute(
        select(
            NormalizedCandidate.location_city,
            NormalizedCandidate.location_country,
        )
        .where(NormalizedCandidate.location_country.is_not(None))
        .distinct()
    ).all()

    existing = set(session.execute(select(LocationTimezone.city, LocationTimezone.country)).all())
    session.close()

    unresolved = [
        {"city": city, "country": country}
        for city, country in rows
        if (city, country) not in existing
    ]
    context.log.info(
        f"Found {len(unresolved)} unresolved (city, country) pairs "
        f"out of {len(rows)} total distinct pairs"
    )
    return {"unresolved": unresolved}


@op(
    description="Resolve unresolved locations to IANA timezones via LLM",
    required_resource_keys={"openrouter"},
)
def resolve_timezones_via_llm(context: OpExecutionContext, data: dict) -> dict:
    unresolved = data["unresolved"]
    if not unresolved:
        context.log.info("No unresolved locations, skipping LLM call")
        return {"resolved": []}

    openrouter = context.resources.openrouter
    resolved = asyncio.run(resolve_timezones(openrouter, unresolved))
    valid = [r for r in resolved if r.get("timezone")]
    context.log.info(f"Resolved {len(valid)}/{len(unresolved)} locations via LLM")
    return {"resolved": resolved}


@op(description="Cache resolved timezones and backfill normalized_candidates.timezone")
def apply_timezone_lookups(context: OpExecutionContext, data: dict) -> dict:
    resolved = data["resolved"]
    if not resolved:
        context.log.info("No resolved timezones to apply")
        return {"inserted": 0, "updated": 0}

    session = get_session()
    inserted = 0
    for r in resolved:
        tz = r.get("timezone")
        if not tz:
            continue
        stmt = insert(LocationTimezone).values(
            id=func.gen_random_uuid(),
            city=r.get("city"),
            country=r["country"],
            timezone=tz,
            utc_offset=r.get("utc_offset"),
            confidence=r.get("confidence", "high"),
            resolved_by="llm",
        )
        stmt = stmt.on_conflict_do_nothing(constraint="uq_location_timezones_city_country")
        result = session.execute(stmt)
        inserted += result.rowcount
    session.commit()

    updated = session.execute(
        NormalizedCandidate.__table__.update()
        .where(
            NormalizedCandidate.location_country.is_not(None),
            NormalizedCandidate.timezone.is_(None),
            NormalizedCandidate.location_city == LocationTimezone.city,
            NormalizedCandidate.location_country == LocationTimezone.country,
        )
        .values(timezone=LocationTimezone.timezone)
    ).rowcount

    updated += session.execute(
        NormalizedCandidate.__table__.update()
        .where(
            NormalizedCandidate.location_country.is_not(None),
            NormalizedCandidate.location_city.is_(None),
            NormalizedCandidate.timezone.is_(None),
            LocationTimezone.city.is_(None),
            NormalizedCandidate.location_country == LocationTimezone.country,
        )
        .values(timezone=LocationTimezone.timezone)
    ).rowcount

    session.commit()
    session.close()

    context.log.info(f"Cached {inserted} new timezone lookups, updated {updated} candidates")
    return {"inserted": inserted, "updated": updated}


@job(
    description="Resolve timezones for candidate locations via LLM lookup (batch backfill)",
    op_retry_policy=openrouter_retry_policy,
)
def timezone_lookup_job():
    """Batch-resolve timezones for all candidates missing timezone data."""
    data = get_unresolved_locations()
    resolved = resolve_timezones_via_llm(data)
    apply_timezone_lookups(resolved)


timezone_lookup_schedule = ScheduleDefinition(
    name="timezone_lookup_daily",
    cron_schedule="0 3 * * *",
    job=timezone_lookup_job,
    description="Resolve timezones for candidate locations daily at 03:00 UTC",
)
