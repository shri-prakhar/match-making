"""Sync Airtable partitions and sample candidates ops/jobs."""

from dagster import OpExecutionContext, job, op

from talent_matching.assets.candidates import candidate_partitions
from talent_matching.assets.jobs import job_partitions


@op(required_resource_keys={"airtable"}, tags={"dagster/concurrency_key": "airtable_api"})
def sync_airtable_candidates_partitions(context: OpExecutionContext) -> dict:
    """Sync all Airtable candidate record IDs as dynamic partitions."""
    airtable = context.resources.airtable

    context.log.info("Fetching all record IDs from Airtable...")
    all_record_ids = airtable.get_all_record_ids()
    context.log.info(f"Found {len(all_record_ids)} records in Airtable")

    existing_partitions = set(
        context.instance.get_dynamic_partitions(partitions_def_name=candidate_partitions.name)
    )
    context.log.info(f"Existing partitions: {len(existing_partitions)}")

    new_record_ids = set(all_record_ids) - existing_partitions
    removed_record_ids = existing_partitions - set(all_record_ids)

    if new_record_ids:
        context.log.info(f"Adding {len(new_record_ids)} new partitions...")
        context.instance.add_dynamic_partitions(
            partitions_def_name=candidate_partitions.name,
            partition_keys=list(new_record_ids),
        )
    else:
        context.log.info("No new partitions to add")

    if removed_record_ids:
        context.log.info(
            f"Removing {len(removed_record_ids)} partitions (deleted from Airtable)..."
        )
        for key in removed_record_ids:
            context.instance.delete_dynamic_partition(
                partitions_def_name=candidate_partitions.name,
                partition_key=key,
            )
    else:
        context.log.info("No partitions to remove")

    context.log.info("")
    context.log.info("Next: Go to Jobs → candidate_pipeline → Backfill to process partitions")

    return {
        "total_records": len(all_record_ids),
        "existing_partitions": len(existing_partitions),
        "new_partitions": len(new_record_ids),
        "removed_partitions": len(removed_record_ids),
    }


@op(required_resource_keys={"airtable_jobs"}, tags={"dagster/concurrency_key": "airtable_api"})
def sync_airtable_jobs_partitions(context: OpExecutionContext) -> dict:
    """Sync all Airtable ATS job record IDs as dynamic partitions."""
    airtable_jobs_resource = context.resources.airtable_jobs
    context.log.info("Fetching all record IDs from Airtable ATS table...")
    all_record_ids = airtable_jobs_resource.get_all_record_ids()
    context.log.info(f"Found {len(all_record_ids)} job records")

    existing = set(context.instance.get_dynamic_partitions(partitions_def_name=job_partitions.name))
    context.log.info(f"Existing partitions: {len(existing)}")

    new_record_ids = set(all_record_ids) - existing
    removed_record_ids = existing - set(all_record_ids)

    if new_record_ids:
        context.log.info(f"Adding {len(new_record_ids)} new partitions...")
        context.instance.add_dynamic_partitions(
            partitions_def_name=job_partitions.name,
            partition_keys=list(new_record_ids),
        )
    else:
        context.log.info("No new partitions to add")

    if removed_record_ids:
        context.log.info(
            f"Removing {len(removed_record_ids)} partitions (deleted from Airtable)..."
        )
        for key in removed_record_ids:
            context.instance.delete_dynamic_partition(
                partitions_def_name=job_partitions.name,
                partition_key=key,
            )
    else:
        context.log.info("No partitions to remove")

    context.log.info("Next: Go to Jobs → job_pipeline → Backfill to process job partitions")
    return {
        "total_records": len(all_record_ids),
        "existing_partitions": len(existing),
        "new_partitions": len(new_record_ids),
        "removed_partitions": len(removed_record_ids),
    }


@job(description="Sync Airtable candidate records as dynamic partitions")
def sync_airtable_candidates_job():
    """Register all Airtable candidate record IDs as dynamic partitions."""
    sync_airtable_candidates_partitions()


@job(description="Sync Airtable ATS job records as dynamic partitions")
def sync_airtable_jobs_job():
    """Register all Airtable ATS job record IDs as dynamic partitions."""
    sync_airtable_jobs_partitions()


@op(required_resource_keys={"airtable"}, tags={"dagster/concurrency_key": "airtable_api"})
def fetch_sample_candidates(context: OpExecutionContext, sample_size: int = 20) -> list:
    """Fetch a sample of candidates from Airtable for testing."""
    airtable = context.resources.airtable

    context.log.info(f"Fetching {sample_size} sample candidates from Airtable...")
    all_records = airtable.fetch_all_records()
    sample = all_records[:sample_size]

    context.log.info(f"Fetched {len(sample)} candidates")
    for i, record in enumerate(sample[:5]):
        context.log.info(f"  {i + 1}. {record.get('full_name', 'Unknown')}")
    if len(sample) > 5:
        context.log.info(f"  ... and {len(sample) - 5} more")

    return sample


@op
def log_sample_stats(context: OpExecutionContext, candidates: list) -> dict:
    """Log statistics about the sample candidates."""
    total = len(candidates)
    if total == 0:
        context.log.warning("No candidates to analyze")
        return {"total": 0}

    stats = {
        "total": total,
        "has_cv": sum(1 for c in candidates if c.get("cv_url")),
        "has_skills": sum(1 for c in candidates if c.get("skills_raw")),
        "has_summary": sum(1 for c in candidates if c.get("professional_summary")),
        "has_linkedin": sum(1 for c in candidates if c.get("linkedin_url")),
        "has_github": sum(1 for c in candidates if c.get("github_url")),
    }

    context.log.info("=" * 60)
    context.log.info("SAMPLE STATISTICS")
    context.log.info("=" * 60)
    context.log.info(f"Total candidates: {stats['total']}")
    context.log.info(f"With CV URL:      {stats['has_cv']} ({100 * stats['has_cv'] // total}%)")
    context.log.info(
        f"With Skills:      {stats['has_skills']} ({100 * stats['has_skills'] // total}%)"
    )
    context.log.info(
        f"With Summary:     {stats['has_summary']} ({100 * stats['has_summary'] // total}%)"
    )
    context.log.info(
        f"With LinkedIn:    {stats['has_linkedin']} ({100 * stats['has_linkedin'] // total}%)"
    )
    context.log.info(
        f"With GitHub:      {stats['has_github']} ({100 * stats['has_github'] // total}%)"
    )
    context.log.info("=" * 60)

    return stats


@job(description="Fetch 20 candidates and log data quality statistics")
def sample_candidates_job():
    """Fetch a sample of candidates and analyze data completeness."""
    candidates = fetch_sample_candidates()
    log_sample_stats(candidates)
