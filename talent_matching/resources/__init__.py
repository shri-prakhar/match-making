"""Dagster resources for the talent matching pipeline."""

from talent_matching.resources.airtable import (
    AirtableATSResource,
    AirtableJobsResource,
    AirtableResource,
)
from talent_matching.resources.github import GitHubAPIResource
from talent_matching.resources.linkedin import LinkedInAPIResource
from talent_matching.resources.matchmaking import MatchmakingResource
from talent_matching.resources.notion import NotionResource
from talent_matching.resources.openrouter import OpenRouterResource
from talent_matching.resources.telegram import TelegramResource
from talent_matching.resources.twitter import TwitterAPIResource

__all__ = [
    "AirtableResource",
    "AirtableATSResource",
    "AirtableJobsResource",
    "GitHubAPIResource",
    "TwitterAPIResource",
    "LinkedInAPIResource",
    "MatchmakingResource",
    "NotionResource",
    "OpenRouterResource",
    "TelegramResource",
]
