"""LinkedIn API resource for fetching professional network metrics.

LinkedIn's official API is heavily restricted:
- Marketing API requires partner status
- Sign In with LinkedIn only provides name/photo/email
- No public metrics (followers, connections) via official API

This module supports multiple data sources:
1. Proxycurl (recommended): Third-party enrichment API
2. Manual: Human reviewers enter data during verification
3. Mock: Test data for development

API Documentation:
- Proxycurl: https://nubela.co/proxycurl/docs
- Proxycurl Pricing: https://nubela.co/proxycurl/pricing (~$0.01-0.03/lookup)
- LinkedIn Official (limited): https://learn.microsoft.com/en-us/linkedin/
"""

import random
from datetime import datetime
from typing import Any

import httpx
from dagster import ConfigurableResource
from pydantic import Field


class LinkedInAPIResource(ConfigurableResource):
    """LinkedIn API resource for fetching user statistics.

    Supports multiple data sources due to LinkedIn API restrictions.
    Default is mock mode for development; production uses Proxycurl.

    Data Sources:
    - mock: Returns plausible test data
    - proxycurl: Uses Proxycurl API for profile enrichment
    - manual: Returns stub indicating manual entry required
    """

    proxycurl_api_key: str = Field(
        default="",
        description="Proxycurl API key (leave empty for mock mode)",
    )
    data_source: str = Field(
        default="mock",
        description="Data source: 'mock', 'proxycurl', or 'manual'",
    )
    proxycurl_base_url: str = Field(
        default="https://nubela.co/proxycurl/api/v2",
        description="Proxycurl API base URL",
    )

    def get_user_metrics(self, linkedin_url_or_username: str) -> dict[str, Any]:
        """Fetch LinkedIn metrics for a user.

        Args:
            linkedin_url_or_username: Either a full LinkedIn URL
                (e.g., https://linkedin.com/in/johndoe) or just the
                username/vanity URL (e.g., johndoe)

        Returns:
            Dictionary containing LinkedIn metrics including:
            - linkedin_username: The LinkedIn handle
            - linkedin_url: Profile URL
            - followers_count: Number of followers (may be None)
            - connections_count: Approximate connections (may be None)
            - headline: Professional headline
            - source: Data source used
            - _meta: Metadata about the fetch
        """
        # Normalize input to extract username
        username = self._extract_username(linkedin_url_or_username)
        linkedin_url = f"https://www.linkedin.com/in/{username}"

        if not username:
            return self._empty_metrics(username, error="Empty username")

        if self.data_source == "mock" or (
            self.data_source == "proxycurl" and not self.proxycurl_api_key
        ):
            return self._mock_user_metrics(username, linkedin_url)

        if self.data_source == "manual":
            return self._manual_stub(username, linkedin_url)

        if self.data_source == "proxycurl":
            return self._fetch_proxycurl(username, linkedin_url)

        return self._empty_metrics(username, error=f"Unknown data source: {self.data_source}")

    def _extract_username(self, input_str: str) -> str:
        """Extract LinkedIn username from URL or direct input."""
        input_str = input_str.strip()

        # Handle full URLs
        if "linkedin.com" in input_str:
            # Extract from patterns like:
            # https://www.linkedin.com/in/johndoe
            # https://linkedin.com/in/johndoe/
            # linkedin.com/in/johndoe
            parts = input_str.split("/in/")
            if len(parts) > 1:
                username = parts[1].strip("/").split("/")[0].split("?")[0]
                return username

        # Assume it's a direct username
        return input_str.lstrip("@").strip()

    def _fetch_proxycurl(self, username: str, linkedin_url: str) -> dict[str, Any]:
        """Fetch profile data from Proxycurl API.

        Endpoint: GET /api/v2/linkedin
        Required: linkedin_profile_url parameter
        """
        url = f"{self.proxycurl_base_url}/linkedin"
        params = {
            "linkedin_profile_url": linkedin_url,
            "fallback_to_cache": "on-error",
            "use_cache": "if-present",
        }
        headers = {
            "Authorization": f"Bearer {self.proxycurl_api_key}",
        }

        with httpx.Client() as client:
            response = client.get(url, params=params, headers=headers)

            if response.status_code == 404:
                return self._empty_metrics(username, error="Profile not found")

            if response.status_code == 429:
                return self._empty_metrics(username, error="Rate limit exceeded")

            if response.status_code == 402:
                return self._empty_metrics(username, error="Insufficient credits")

            if response.status_code != 200:
                return self._empty_metrics(username, error=f"API error: {response.status_code}")

            data = response.json()

            # Proxycurl returns follower_count directly when available
            followers = data.get("follower_count")
            # Connections are not always available
            connections = data.get("connections")

            return {
                "linkedin_username": username,
                "linkedin_url": linkedin_url,
                "followers_count": followers,
                "connections_count": connections,
                "headline": data.get("headline"),
                "summary": data.get("summary"),
                "full_name": data.get("full_name"),
                "source": "proxycurl",
                "_meta": {
                    "fetched_at": datetime.now().isoformat(),
                    "mock": False,
                    "source": "proxycurl",
                    "profile_id": data.get("public_identifier"),
                },
            }

    def _mock_user_metrics(self, username: str, linkedin_url: str) -> dict[str, Any]:
        """Generate mock LinkedIn metrics.

        Uses username hash for deterministic results.
        """
        seed = hash(username) % (2**32)
        rng = random.Random(seed)

        # LinkedIn connection tiers (500+ shows as "500+")
        connection_tier = rng.random()
        if connection_tier < 0.3:
            connections = rng.randint(50, 200)
        elif connection_tier < 0.6:
            connections = rng.randint(200, 500)
        elif connection_tier < 0.85:
            connections = 500  # Shows as "500+"
        else:
            connections = rng.randint(500, 30000)

        # Followers often correlate with connections but can be higher
        # for thought leaders
        follower_multiplier = rng.uniform(0.5, 3.0)
        followers = int(connections * follower_multiplier)

        # Generate plausible headlines
        titles = [
            "Senior Software Engineer",
            "Full Stack Developer",
            "Product Manager",
            "Engineering Manager",
            "CTO",
            "Founder",
            "DevRel",
            "Solutions Architect",
            "Blockchain Developer",
            "Technical Lead",
        ]
        companies = [
            "Building the future",
            "@ Stealth Startup",
            "@ Web3 Company",
            "| Open to opportunities",
            "| DeFi enthusiast",
        ]
        headline = f"{rng.choice(titles)} {rng.choice(companies)}"

        return {
            "linkedin_username": username,
            "linkedin_url": linkedin_url,
            "followers_count": followers,
            "connections_count": connections,
            "headline": headline,
            "summary": f"Mock summary for {username}",
            "full_name": None,
            "source": "mock",
            "_meta": {
                "fetched_at": datetime.now().isoformat(),
                "mock": True,
                "source": "mock",
            },
        }

    def _manual_stub(self, username: str, linkedin_url: str) -> dict[str, Any]:
        """Return stub indicating manual entry is required.

        Used when data_source='manual' to indicate human reviewers
        should populate the metrics during verification.
        """
        return {
            "linkedin_username": username,
            "linkedin_url": linkedin_url,
            "followers_count": None,
            "connections_count": None,
            "headline": None,
            "summary": None,
            "full_name": None,
            "source": "manual",
            "_meta": {
                "fetched_at": datetime.now().isoformat(),
                "mock": False,
                "source": "manual",
                "requires_manual_entry": True,
                "instructions": "Human reviewer should populate metrics during verification",
            },
        }

    def _empty_metrics(self, username: str, error: str) -> dict[str, Any]:
        """Return empty metrics structure with error info."""
        return {
            "linkedin_username": username,
            "linkedin_url": f"https://www.linkedin.com/in/{username}" if username else None,
            "followers_count": None,
            "connections_count": None,
            "headline": None,
            "summary": None,
            "full_name": None,
            "source": "error",
            "_meta": {
                "fetched_at": datetime.now().isoformat(),
                "mock": False,
                "error": error,
                "source": "error",
            },
        }

    def check_credits(self) -> dict[str, Any]:
        """Check remaining Proxycurl credits.

        Only applicable when using Proxycurl as data source.
        """
        if self.data_source != "proxycurl" or not self.proxycurl_api_key:
            return {
                "credits_remaining": None,
                "mock": True,
                "note": "Credit check only available with Proxycurl",
            }

        url = f"{self.proxycurl_base_url}/credit-balance"
        headers = {
            "Authorization": f"Bearer {self.proxycurl_api_key}",
        }

        with httpx.Client() as client:
            response = client.get(url, headers=headers)

            if response.status_code != 200:
                return {
                    "credits_remaining": None,
                    "error": f"API error: {response.status_code}",
                }

            data = response.json()
            return {
                "credits_remaining": data.get("credit_balance"),
                "mock": False,
            }
