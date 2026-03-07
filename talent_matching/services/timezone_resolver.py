"""Inline timezone resolution for candidate ingest.

Resolves a candidate's (city, country) to an IANA timezone using a two-tier
strategy: cache lookup in location_timezones, then LLM fallback. Results are
cached for future lookups.
"""

import asyncio
import logging
import os
from uuid import uuid4
from zoneinfo import available_timezones

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from talent_matching.models.location_timezones import LocationTimezone

logger = logging.getLogger(__name__)

VALID_TIMEZONES = available_timezones()


def _is_valid_iana(tz: str | None) -> bool:
    return tz is not None and tz in VALID_TIMEZONES


def resolve_candidate_timezone(
    session: Session,
    city: str | None,
    country: str | None,
    llm_timezone: str | None,
) -> str | None:
    """Resolve a candidate's timezone, returning an IANA identifier or None.

    Resolution order:
    1. If the CV normalization LLM already provided a valid IANA timezone, use it.
    2. If country is known, check the location_timezones cache table.
    3. If not cached, call the LLM to resolve and cache the result.
    4. If no country is known, return None.
    """
    if _is_valid_iana(llm_timezone):
        return llm_timezone

    if not country:
        return None

    cached = _lookup_cached(session, city, country)
    if cached is not None:
        return cached

    resolved = _resolve_via_llm(city, country)
    if resolved:
        _cache_result(session, city, country, resolved)
        return resolved.get("timezone")

    return None


def _lookup_cached(session: Session, city: str | None, country: str) -> str | None:
    """Check location_timezones for an exact (city, country) match.

    Uses scalars().first() instead of scalar_one_or_none() because
    PostgreSQL unique constraints don't prevent duplicate NULL values,
    so (NULL, country) pairs can have multiple rows.
    """
    stmt = select(LocationTimezone.timezone).where(
        LocationTimezone.country == country,
        LocationTimezone.city == city if city else LocationTimezone.city.is_(None),
    )
    result = session.execute(stmt).scalars().first()
    if result:
        logger.debug("Cache hit for (%s, %s): %s", city, country, result)
        return result

    if city:
        fallback = (
            session.execute(
                select(LocationTimezone.timezone).where(
                    LocationTimezone.country == country,
                    LocationTimezone.city.is_(None),
                )
            )
            .scalars()
            .first()
        )
        if fallback:
            logger.debug("Country-level cache hit for (%s, %s): %s", city, country, fallback)
            return fallback

    return None


def _resolve_via_llm(city: str | None, country: str) -> dict | None:
    """Make a single LLM call to resolve one (city, country) pair."""
    from talent_matching.llm.operations.resolve_timezones import resolve_timezones
    from talent_matching.resources.openrouter import OpenRouterResource

    api_key = os.environ.get("OPENROUTER_API_KEY")
    if not api_key:
        logger.warning("OPENROUTER_API_KEY not set, skipping timezone resolution")
        return None

    openrouter = OpenRouterResource(api_key=api_key, default_model="openai/gpt-4o-mini")

    locations = [{"city": city, "country": country}]
    results = asyncio.run(resolve_timezones(openrouter, locations))

    if results and results[0].get("timezone"):
        logger.info("LLM resolved (%s, %s) -> %s", city, country, results[0]["timezone"])
        return results[0]

    logger.info("LLM could not resolve timezone for (%s, %s)", city, country)
    return None


def _cache_result(session: Session, city: str | None, country: str, resolved: dict) -> None:
    """Insert resolved timezone into the location_timezones cache."""
    stmt = insert(LocationTimezone).values(
        id=uuid4(),
        city=city,
        country=country,
        timezone=resolved["timezone"],
        utc_offset=resolved.get("utc_offset"),
        confidence=resolved.get("confidence", "high"),
        resolved_by="llm",
    )
    stmt = stmt.on_conflict_do_nothing()
    session.execute(stmt)
    session.flush()
