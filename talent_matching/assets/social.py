"""Social profile metrics assets.

This module defines the asset graph for fetching social profile metrics:
1. candidate_twitter_metrics: Twitter/X activity metrics
2. candidate_linkedin_metrics: LinkedIn professional network metrics

These assets depend on normalized_candidates and fetch supplementary
signals for BD/Marketing/Growth roles and social presence scoring.
"""

from typing import Any

from dagster import (
    AssetExecutionContext,
    AssetIn,
    asset,
)

from talent_matching.resources.linkedin import LinkedInAPIResource
from talent_matching.resources.twitter import TwitterAPIResource


@asset(
    ins={"normalized_candidates": AssetIn()},
    description="Twitter/X activity metrics for candidates with public profiles",
    group_name="social_metrics",
    metadata={
        "table": "candidate_twitter_metrics",
        "api": "x_api_v2",
        "metrics": ["followers_count", "following_count", "tweet_count", "listed_count"],
    },
)
def candidate_twitter_metrics(
    context: AssetExecutionContext,
    normalized_candidates: list[dict[str, Any]],
    twitter_api: TwitterAPIResource,
) -> list[dict[str, Any]]:
    """Fetch Twitter/X metrics for candidates with X handles.

    For each candidate with an x_handle in their normalized profile,
    fetch their public metrics from the X API v2.

    Metrics collected:
    - followers_count: Number of followers
    - following_count: Number of accounts followed
    - tweet_count: Total tweets posted
    - listed_count: Number of lists the user appears on

    Rate Limits:
    - Basic tier: 300 requests per 15-minute window
    """
    context.log.info(f"Fetching Twitter metrics for {len(normalized_candidates)} candidates")

    metrics = []
    fetched_count = 0
    skipped_count = 0

    for candidate in normalized_candidates:
        candidate_id = candidate.get("candidate_id")
        normalized_json = candidate.get("normalized_json", {})

        # Check if candidate has an X handle
        x_handle = normalized_json.get("x_handle")
        if not x_handle:
            skipped_count += 1
            continue

        # Fetch metrics from Twitter API
        twitter_data = twitter_api.get_user_metrics(x_handle)

        # Check for errors
        if twitter_data.get("_meta", {}).get("error"):
            context.log.warning(
                f"Failed to fetch Twitter metrics for {x_handle}: {twitter_data['_meta']['error']}"
            )
            continue

        metrics.append(
            {
                "candidate_id": candidate_id,
                "twitter_username": twitter_data["username"],
                "twitter_url": twitter_data["twitter_url"],
                "followers_count": twitter_data["followers_count"],
                "following_count": twitter_data["following_count"],
                "tweet_count": twitter_data["tweet_count"],
                "listed_count": twitter_data["listed_count"],
                "_meta": twitter_data.get("_meta", {}),
            }
        )
        fetched_count += 1

    context.log.info(
        f"Fetched Twitter metrics: {fetched_count} successful, {skipped_count} skipped (no handle)"
    )

    return metrics


@asset(
    ins={"normalized_candidates": AssetIn()},
    description="LinkedIn professional network metrics for candidates",
    group_name="social_metrics",
    metadata={
        "table": "candidate_linkedin_metrics",
        "api": "proxycurl",
        "metrics": ["followers_count", "connections_count", "headline"],
    },
)
def candidate_linkedin_metrics(
    context: AssetExecutionContext,
    normalized_candidates: list[dict[str, Any]],
    linkedin_api: LinkedInAPIResource,
) -> list[dict[str, Any]]:
    """Fetch LinkedIn metrics for candidates with LinkedIn profiles.

    For each candidate with a linkedin_handle in their normalized profile,
    fetch their professional network metrics.

    Note: LinkedIn's official API is heavily restricted. This asset
    supports multiple data sources:
    - proxycurl: Third-party enrichment API (~$0.01-0.03/lookup)
    - manual: Stub indicating human verification required
    - mock: Test data for development

    Metrics collected (when available):
    - followers_count: Number of LinkedIn followers
    - connections_count: Approximate connection count
    - headline: Professional headline
    """
    context.log.info(f"Fetching LinkedIn metrics for {len(normalized_candidates)} candidates")

    metrics = []
    fetched_count = 0
    skipped_count = 0
    manual_required = 0

    for candidate in normalized_candidates:
        candidate_id = candidate.get("candidate_id")
        normalized_json = candidate.get("normalized_json", {})

        # Check if candidate has a LinkedIn handle/URL
        linkedin_handle = normalized_json.get("linkedin_handle")
        if not linkedin_handle:
            skipped_count += 1
            continue

        # Fetch metrics from LinkedIn API
        linkedin_data = linkedin_api.get_user_metrics(linkedin_handle)

        # Check for errors
        if linkedin_data.get("_meta", {}).get("error"):
            context.log.warning(
                f"Failed to fetch LinkedIn metrics for {linkedin_handle}: "
                f"{linkedin_data['_meta']['error']}"
            )
            continue

        # Track if manual entry is required
        if linkedin_data.get("_meta", {}).get("requires_manual_entry"):
            manual_required += 1

        metrics.append(
            {
                "candidate_id": candidate_id,
                "linkedin_username": linkedin_data["linkedin_username"],
                "linkedin_url": linkedin_data["linkedin_url"],
                "followers_count": linkedin_data["followers_count"],
                "connections_count": linkedin_data["connections_count"],
                "headline": linkedin_data["headline"],
                "source": linkedin_data["source"],
                "_meta": linkedin_data.get("_meta", {}),
            }
        )
        fetched_count += 1

    context.log.info(
        f"Fetched LinkedIn metrics: {fetched_count} successful, "
        f"{skipped_count} skipped (no handle), "
        f"{manual_required} require manual entry"
    )

    return metrics


@asset(
    ins={
        "candidate_twitter_metrics": AssetIn(),
        "candidate_linkedin_metrics": AssetIn(),
    },
    description="Aggregated social followers count for candidates",
    group_name="social_metrics",
    metadata={
        "updates": "normalized_candidates.social_followers_total",
    },
)
def social_followers_aggregation(
    context: AssetExecutionContext,
    candidate_twitter_metrics: list[dict[str, Any]],
    candidate_linkedin_metrics: list[dict[str, Any]],
) -> dict[str, int]:
    """Aggregate social followers across platforms for each candidate.

    Combines followers from Twitter/X and LinkedIn (when available)
    to update the social_followers_total field on normalized_candidates.

    This supports queries like:
    - WHERE social_followers_total >= 5000 (for BD/Growth roles)
    """
    context.log.info("Aggregating social followers across platforms")

    # Build aggregation map
    aggregation: dict[str, int] = {}

    # Add Twitter followers
    for metrics in candidate_twitter_metrics:
        candidate_id = metrics.get("candidate_id")
        if candidate_id:
            aggregation[candidate_id] = aggregation.get(candidate_id, 0) + metrics.get(
                "followers_count", 0
            )

    # Add LinkedIn followers (when available)
    for metrics in candidate_linkedin_metrics:
        candidate_id = metrics.get("candidate_id")
        followers = metrics.get("followers_count")
        if candidate_id and followers is not None:
            aggregation[candidate_id] = aggregation.get(candidate_id, 0) + followers

    context.log.info(f"Aggregated social followers for {len(aggregation)} candidates")

    # In a full implementation, this would update normalized_candidates
    # For now, return the aggregation map
    return aggregation
