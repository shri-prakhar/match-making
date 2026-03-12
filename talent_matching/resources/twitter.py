"""Twitter/X API resource for fetching social metrics.

This module provides both a mock implementation for testing and production
implementation using the X API v2.

API Documentation:
- X API v2 Overview: https://developer.x.com/en/docs/x-api
- User Lookup: https://developer.x.com/en/docs/x-api/users/lookup/api-reference
- Public Metrics: https://developer.x.com/en/docs/x-api/data-dictionary/object-model/user

Pricing:
- Free tier: Write-only, no user lookup
- Basic tier ($100/month): User lookup with public_metrics (recommended)
- Pro tier ($5,000/month): Full Search API, higher limits
"""

import random
from datetime import datetime
from typing import Any

import httpx
from dagster import ConfigurableResource
from pydantic import Field


class TwitterAPIResource(ConfigurableResource):
    """Twitter/X API resource for fetching user statistics.

    In production, this uses the X API v2 with Bearer Token authentication.
    Mock mode returns plausible test data for development.

    Rate Limits (Basic tier):
    - User lookup: 300 requests per 15-minute window
    """

    bearer_token: str = Field(
        default="",
        description="X API Bearer Token (leave empty for mock mode)",
    )
    mock_mode: bool = Field(
        default=True,
        description="If True, return mock data instead of calling X API",
    )
    api_base_url: str = Field(
        default="https://api.twitter.com/2",
        description="X API v2 base URL",
    )

    def get_user_metrics(self, username: str) -> dict[str, Any]:
        """Fetch Twitter/X metrics for a user by username.

        Args:
            username: Twitter/X username (without @ symbol)

        Returns:
            Dictionary containing Twitter metrics including:
            - username: The Twitter handle
            - twitter_url: Profile URL
            - followers_count: Number of followers
            - following_count: Number of accounts followed
            - tweet_count: Total tweets posted
            - listed_count: Number of lists the user appears on
            - _meta: Metadata about the fetch
        """
        # Clean username (remove @ if present)
        username = username.lstrip("@").strip()

        if not username:
            return self._empty_metrics(username, error="Empty username")

        if self.mock_mode or not self.bearer_token:
            return self._mock_user_metrics(username)

        return self._fetch_user_metrics(username)

    def _fetch_user_metrics(self, username: str) -> dict[str, Any]:
        """Fetch real metrics from X API v2.

        Endpoint: GET /2/users/by/username/:username
        Required fields: user.fields=public_metrics
        """
        url = f"{self.api_base_url}/users/by/username/{username}"
        params = {"user.fields": "public_metrics,created_at,description,verified"}
        headers = {
            "Authorization": f"Bearer {self.bearer_token}",
            "Content-Type": "application/json",
        }

        with httpx.Client() as client:
            response = client.get(url, params=params, headers=headers)

            if response.status_code == 404:
                return self._empty_metrics(username, error="User not found")

            if response.status_code == 429:
                return self._empty_metrics(username, error="Rate limit exceeded")

            if response.status_code != 200:
                return self._empty_metrics(username, error=f"API error: {response.status_code}")

            data = response.json()

            if "data" not in data:
                return self._empty_metrics(username, error="No data in response")

            user_data = data["data"]
            public_metrics = user_data.get("public_metrics", {})

            return {
                "username": user_data.get("username", username),
                "twitter_url": f"https://x.com/{user_data.get('username', username)}",
                "followers_count": public_metrics.get("followers_count", 0),
                "following_count": public_metrics.get("following_count", 0),
                "tweet_count": public_metrics.get("tweet_count", 0),
                "listed_count": public_metrics.get("listed_count", 0),
                "verified": user_data.get("verified", False),
                "created_at": user_data.get("created_at"),
                "description": user_data.get("description"),
                "_meta": {
                    "fetched_at": datetime.now().isoformat(),
                    "mock": False,
                    "source": "x_api_v2",
                },
            }

    def _mock_user_metrics(self, username: str) -> dict[str, Any]:
        """Generate mock Twitter metrics.

        Uses username hash for deterministic results.
        """
        seed = hash(username) % (2**32)
        rng = random.Random(seed)

        # Generate plausible follower counts
        # Most users have <1000, some have more
        follower_tier = rng.random()
        if follower_tier < 0.6:
            followers = rng.randint(50, 1000)
        elif follower_tier < 0.85:
            followers = rng.randint(1000, 10000)
        elif follower_tier < 0.95:
            followers = rng.randint(10000, 100000)
        else:
            followers = rng.randint(100000, 1000000)

        # Following is usually less than followers for established accounts
        following = rng.randint(100, min(5000, followers * 2))

        # Tweet count correlates loosely with account age
        tweet_count = rng.randint(100, 50000)

        # Listed count is usually much smaller
        listed_count = rng.randint(0, max(1, followers // 100))

        return {
            "username": username,
            "twitter_url": f"https://x.com/{username}",
            "followers_count": followers,
            "following_count": following,
            "tweet_count": tweet_count,
            "listed_count": listed_count,
            "verified": rng.random() < 0.1,  # 10% chance of verified
            "created_at": None,
            "description": f"Mock profile for {username}",
            "_meta": {
                "fetched_at": datetime.now().isoformat(),
                "mock": True,
                "source": "mock",
            },
        }

    def _empty_metrics(self, username: str, error: str) -> dict[str, Any]:
        """Return empty metrics structure with error info."""
        return {
            "username": username,
            "twitter_url": f"https://x.com/{username}" if username else None,
            "followers_count": 0,
            "following_count": 0,
            "tweet_count": 0,
            "listed_count": 0,
            "verified": False,
            "created_at": None,
            "description": None,
            "_meta": {
                "fetched_at": datetime.now().isoformat(),
                "mock": False,
                "error": error,
                "source": "error",
            },
        }

    def check_rate_limit(self) -> dict[str, Any]:
        """Check current X API rate limit status.

        Note: X API doesn't have a dedicated rate limit endpoint.
        Rate limit info is returned in response headers.

        Returns:
            Dictionary with rate limit information
        """
        if self.mock_mode or not self.bearer_token:
            return {
                "limit": 300,
                "remaining": 299,
                "reset_at": None,
                "mock": True,
            }

        # In production, would parse X-Rate-Limit headers from a test request
        return {
            "limit": 300,
            "remaining": 299,
            "reset_at": None,
            "mock": True,
            "note": "Rate limit tracking requires parsing response headers",
        }
