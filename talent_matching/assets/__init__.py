"""Dagster assets for the talent matching pipeline.

This module is a convenience re-export for external use. Dagster definitions load
assets via load_assets_from_modules([candidates, jobs, social]) from the submodules
directly. Partition definitions (candidate_partitions, job_partitions) and
Airtable ingest assets (airtable_candidates, airtable_jobs) live only in the
submodules and are not re-exported here.
"""

from talent_matching.assets.candidates import (
    airtable_candidate_sync,
    candidate_role_fitness,
    candidate_vectors,
    normalized_candidates,
    raw_candidates,
)
from talent_matching.assets.jobs import (
    airtable_job_sync,
    job_vectors,
    llm_refined_shortlist,
    matches,
    normalized_jobs,
    raw_jobs,
)
from talent_matching.assets.social import (
    candidate_linkedin_metrics,
    candidate_twitter_metrics,
    social_followers_aggregation,
)

__all__ = [
    # Candidate assets
    "raw_candidates",
    "normalized_candidates",
    "candidate_vectors",
    "candidate_role_fitness",
    "airtable_candidate_sync",
    # Job assets
    "raw_jobs",
    "normalized_jobs",
    "job_vectors",
    "airtable_job_sync",
    # Social metrics
    "candidate_twitter_metrics",
    "candidate_linkedin_metrics",
    "social_followers_aggregation",
    # Matching
    "matches",
    "llm_refined_shortlist",
]
