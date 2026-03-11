"""Dagster definitions for the Talent Matching system.

This module is the entry point for Dagster. It wires together:
- All assets (candidates, jobs, matches)
- Resources (OpenRouter for LLM + embeddings, GitHub API, Airtable)
- IO Managers (PostgreSQL, pgvector)
- Sensors (Airtable polling)
- Jobs (candidate pipeline, sync, sample)
- Configuration for different environments (dev, staging, prod)
"""

import os
from typing import Any

from dagster import (
    Definitions,
    EnvVar,
    load_assets_from_modules,
)
from dotenv import load_dotenv

from talent_matching.assets import candidates, jobs, social
from talent_matching.io_managers import PgVectorIOManager, PostgresMetricsIOManager
from talent_matching.jobs import (
    ats_matchmaking_pipeline_job,
    backfill_candidate_region_job,
    candidate_ingest_job,
    candidate_pipeline_job,
    candidate_vectors_job,
    job_ingest_job,
    job_pipeline_job,
    matchmaking_backfill_job,
    sample_candidates_job,
    skill_normalization_job,
    skill_normalization_schedule,
    sync_airtable_candidates_job,
    sync_airtable_jobs_job,
    timezone_lookup_job,
    timezone_lookup_schedule,
    upload_normalized_jobs_to_airtable_job,
    upload_normalized_to_airtable_job,
)
from talent_matching.resources import (
    AirtableATSResource,
    AirtableResource,
    GitHubAPIResource,
    LinkedInAPIResource,
    MatchmakingResource,
    NotionResource,
    OpenRouterResource,
    TwitterAPIResource,
)
from talent_matching.resources.telegram import TelegramResource
from talent_matching.sensors.airtable_sensor import (
    airtable_candidate_sensor,
)
from talent_matching.sensors.alerting import system_health_sensor
from talent_matching.sensors.ats_matchmaking_sensor import ats_matchmaking_sensor
from talent_matching.sensors.ground_truth_sensor import ground_truth_sync_sensor
from talent_matching.sensors.run_failure_sensor import run_failure_tagger

# Load environment variables from .env file (must be before resource initialization)
load_dotenv()

# Load all assets from the modules
all_assets = load_assets_from_modules([candidates, jobs, social])


def get_environment() -> str:
    """Get current environment from env var."""
    return os.getenv("ENVIRONMENT", "development")


# Development resources (mock implementations)
# Using Dagster EnvVar for deferred resolution and better config visibility
dev_resources = {
    # Airtable resource for fetching candidates (optional AIRTABLE_WRITE_TOKEN for PATCH)
    "airtable": AirtableResource(
        base_id=EnvVar("AIRTABLE_BASE_ID"),
        table_id=EnvVar("AIRTABLE_TABLE_ID"),
        api_key=EnvVar("AIRTABLE_API_KEY"),
        write_api_key=os.getenv("AIRTABLE_WRITE_TOKEN") or None,
    ),
    # Job record source: ATS table (airtable_jobs and airtable_ats both point to ATS)
    "airtable_jobs": AirtableATSResource(
        base_id=EnvVar("AIRTABLE_BASE_ID"),
        table_id=os.getenv("AIRTABLE_ATS_TABLE_ID", "tblrbhITEIBOxwcQV"),
        api_key=EnvVar("AIRTABLE_API_KEY"),
        write_api_key=os.getenv("AIRTABLE_WRITE_TOKEN") or None,
        matches_table_id=os.getenv("AIRTABLE_MATCHES_TABLE_ID") or None,
        matches_view_url=os.getenv("AIRTABLE_MATCHES_VIEW_URL") or None,
    ),
    # ATS table for Job Process / Smart Job Profiles workflow (sensor, upload)
    "airtable_ats": AirtableATSResource(
        base_id=EnvVar("AIRTABLE_BASE_ID"),
        table_id=os.getenv("AIRTABLE_ATS_TABLE_ID", "tblrbhITEIBOxwcQV"),
        api_key=EnvVar("AIRTABLE_API_KEY"),
        write_api_key=os.getenv("AIRTABLE_WRITE_TOKEN") or None,
        matches_table_id=os.getenv("AIRTABLE_MATCHES_TABLE_ID") or None,
        matches_view_url=os.getenv("AIRTABLE_MATCHES_VIEW_URL") or None,
    ),
    # Notion API for fetching job description page content (optional key; tries without if unset)
    "notion": NotionResource(api_key=os.getenv("NOTION_API_KEY", "")),
    # OpenRouter LLM resource with cost tracking (also handles embeddings)
    "openrouter": OpenRouterResource(
        api_key=EnvVar("OPENROUTER_API_KEY"),
        default_model="openai/gpt-4o-mini",
    ),
    # GitHub API resource (requires GITHUB_TOKEN in .env for 5000 req/hr)
    "github": GitHubAPIResource(
        api_token=EnvVar("GITHUB_TOKEN"),
    ),
    # Twitter/X API resource
    "twitter_api": TwitterAPIResource(
        mock_mode=True,
    ),
    # LinkedIn API resource
    "linkedin_api": LinkedInAPIResource(
        data_source="mock",
    ),
    # IO Managers and matchmaking — all share the process-wide engine from talent_matching.db
    "postgres_io": PostgresMetricsIOManager(),
    "pgvector_io": PgVectorIOManager(),
    "matchmaking": MatchmakingResource(),
    # Telegram alerts (no-op when TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID unset)
    "telegram": TelegramResource(
        bot_token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
        chat_id=os.getenv("TELEGRAM_CHAT_ID", ""),
    ),
}


def get_resources() -> dict[str, Any]:
    """Get resources based on current environment."""
    env = get_environment()

    if env == "production":
        # In production, we would return prod_resources
        # For now, use dev resources
        return dev_resources
    elif env == "staging":
        # Staging could use real LLMs with cheaper models
        return dev_resources
    else:
        # Development uses mock resources
        return dev_resources


# All jobs available in the dashboard
all_jobs: list[Any] = [
    # Asset jobs (partitioned) - use Backfill in UI to select partitions
    candidate_pipeline_job,
    candidate_vectors_job,
    candidate_ingest_job,
    upload_normalized_to_airtable_job,
    job_pipeline_job,
    job_ingest_job,
    upload_normalized_jobs_to_airtable_job,
    # Ops jobs (non-partitioned)
    sync_airtable_candidates_job,
    sync_airtable_jobs_job,
    sample_candidates_job,
    skill_normalization_job,
    timezone_lookup_job,
    backfill_candidate_region_job,
    # ATS-driven pipeline (sensor-triggered)
    ats_matchmaking_pipeline_job,
    # Backfill: normalization + scoring without LLM refinement
    matchmaking_backfill_job,
]

# Schedules
all_schedules = [
    skill_normalization_schedule,
    timezone_lookup_schedule,
]

# All sensors
all_sensors = [
    airtable_candidate_sensor,
    ats_matchmaking_sensor,
    ground_truth_sync_sensor,
    run_failure_tagger,
    system_health_sensor,
]

# Create the Dagster Definitions object
defs = Definitions(
    assets=all_assets,
    resources=get_resources(),
    jobs=all_jobs,
    schedules=all_schedules,
    sensors=all_sensors,
)


def main() -> None:
    """Entry point for CLI usage."""
    print("Talent Matching Dagster project loaded successfully!")
    print(f"Environment: {get_environment()}")
    print(f"Assets: {len(all_assets)}")
    print(f"Jobs: {len(all_jobs)}")
    print(f"Sensors: {len(all_sensors)}")
    print("\nAvailable jobs:")
    for job in all_jobs:
        print(f"  - {job.name}")
    print("\nRun 'dagster dev' to start the development server.")


if __name__ == "__main__":
    main()
