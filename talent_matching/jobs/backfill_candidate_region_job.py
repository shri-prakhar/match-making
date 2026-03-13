"""Backfill normalized_candidates.location_region from location_country.

Updates only rows where location_country is set and location_region is null/empty.
Uses the same country→region mapping as matchmaking (location_filter) so no LLM.
Run once to fill existing records without re-running full candidate normalization.
"""

from collections import defaultdict

from dagster import OpExecutionContext, job, op
from sqlalchemy import or_, select, update

from talent_matching.db import get_session
from talent_matching.location.resolver import load_location_maps
from talent_matching.matchmaking.location_filter import get_region_for_country
from talent_matching.models.candidates import NormalizedCandidate


@op(description="Backfill location_region for candidates with country but no region")
def backfill_candidate_region(context: OpExecutionContext) -> dict:
    """Select candidates with location_country and missing location_region; set region from mapping."""
    session = get_session()
    country_aliases, region_countries, _city_aliases, _region_aliases = load_location_maps(session)
    rows = session.execute(
        select(NormalizedCandidate.id, NormalizedCandidate.location_country).where(
            NormalizedCandidate.location_country.is_not(None),
            or_(
                NormalizedCandidate.location_region.is_(None),
                NormalizedCandidate.location_region == "",
            ),
        )
    ).all()
    session.close()

    # id (UUID) -> region (only when we can infer)
    to_update: list[tuple] = []
    for id_, country in rows:
        if country:
            region = get_region_for_country(
                country,
                country_aliases=country_aliases,
                region_countries=region_countries,
            )
            if region:
                to_update.append((id_, region))

    if not to_update:
        context.log.info("No candidates to backfill (all have region or unknown country)")
        return {"updated": 0}

    # Group by region for fewer UPDATEs
    by_region: dict[str, list] = defaultdict(list)
    for id_, region in to_update:
        by_region[region].append(id_)

    session = get_session()
    total = 0
    for region, ids in by_region.items():
        result = session.execute(
            update(NormalizedCandidate)
            .where(NormalizedCandidate.id.in_(ids))
            .values(location_region=region)
        )
        total += result.rowcount
    session.commit()
    session.close()

    context.log.info(
        f"Backfilled location_region for {total} candidates ({len(by_region)} regions)"
    )
    return {"updated": total}


@job(
    description="Backfill location_region for normalized_candidates with country but no region (no LLM)",
)
def backfill_candidate_region_job():
    """One-off or occasional backfill of location_region from location_country."""
    backfill_candidate_region()
