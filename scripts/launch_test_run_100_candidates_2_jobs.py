#!/usr/bin/env python3
"""Launch a test run: 100 candidates through candidate_pipeline and 2 jobs through ats_matchmaking_pipeline.

Fetches 100 candidate partition IDs and 2 job partition IDs from the DB, then submits
the candidate_pipeline backfill and ats_matchmaking_pipeline backfill on the remote.

Prerequisites:
  - REMOTE_HOST in .env (optional REMOTE_PROJECT_DIR)
  - DB access: run with poetry run with-remote-db (tunnel to remote Postgres) so the script
    can query for partition IDs. Or run on the server with POSTGRES_HOST=postgres.
  - Remote has the project deployed (so dagster-code container and daemon can run backfills)

Usage:
  poetry run with-remote-db python scripts/launch_test_run_100_candidates_2_jobs.py
  poetry run with-remote-db python scripts/launch_test_run_100_candidates_2_jobs.py --sync-candidates  # sync candidate partitions first
  poetry run with-remote-db python scripts/launch_test_run_100_candidates_2_jobs.py --candidates 50   # use 50 candidates instead of 100
  poetry run with-remote-db python scripts/launch_test_run_100_candidates_2_jobs.py --all-candidates --candidates-only  # sync from Airtable (add/remove partitions) then full backfill
  On server: poetry run python scripts/launch_test_run_100_candidates_2_jobs.py --local
"""

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import select

from talent_matching.script_env import apply_local_db

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")


def get_candidate_partition_ids_from_airtable() -> list[str] | None:
    """Return all candidate record IDs from Airtable (current list; matches sync job).
    Returns None if Airtable env vars are not set."""
    base_id = os.getenv("AIRTABLE_BASE_ID")
    table_id = os.getenv("AIRTABLE_TABLE_ID")
    api_key = os.getenv("AIRTABLE_API_KEY")
    if not base_id or not table_id or not api_key:
        return None
    from talent_matching.resources.airtable import AirtableResource

    resource = AirtableResource(base_id=base_id, table_id=table_id, api_key=api_key)
    return resource.get_all_record_ids()


def get_candidate_partition_ids(limit: int, offset: int = 0) -> list[str]:
    """Return up to `limit` candidate partition IDs (airtable_record_id) from raw_candidates.
    Uses stable order (airtable_record_id) so offset gives a different slice."""
    from talent_matching.db import get_session
    from talent_matching.models.raw import RawCandidate

    session = get_session()
    stmt = (
        select(RawCandidate.airtable_record_id)
        .where(RawCandidate.airtable_record_id.isnot(None))
        .order_by(RawCandidate.airtable_record_id)
        .offset(offset)
        .limit(limit)
    )
    rows = session.execute(stmt).fetchall()
    session.close()
    return [r[0] for r in rows if r[0] and str(r[0]).strip()]


def get_job_partition_ids(limit: int) -> list[str]:
    """Return up to `limit` job partition IDs (airtable_record_id) from raw_jobs."""
    from talent_matching.db import get_session
    from talent_matching.models.raw import RawJob

    session = get_session()
    rows = session.execute(
        select(RawJob.airtable_record_id).where(
            RawJob.airtable_record_id.isnot(None)
        ).limit(limit)
    ).fetchall()
    session.close()
    return [r[0] for r in rows if r[0] and str(r[0]).strip()]


def run_sync_candidates(remote_host: str | None, remote_dir: str, on_server: bool) -> bool:
    """Run sync_airtable_candidates_job. When on_server, run locally; else SSH. Returns True on success."""
    cmd = (
        f"cd {remote_dir} && docker compose -f docker-compose.prod.yml run --rm "
        "-e POSTGRES_HOST=postgres "
        f"-v {remote_dir}/docker/workspace.yaml:/workspace.yaml:ro "
        f"-v {remote_dir}/docker/dagster.yaml:/opt/dagster/dagster_home/dagster.yaml:ro "
        "-e DAGSTER_HOME=/opt/dagster/dagster_home "
        "dagster-code "
        "dagster job launch -w /workspace.yaml -j sync_airtable_candidates_job -l talent_matching"
    )
    if on_server:
        result = subprocess.run(["bash", "-c", cmd])
    else:
        result = subprocess.run(["ssh", remote_host, cmd])
    if result.returncode != 0:
        return False
    print("Waiting 60s for candidate partitions to register...")
    time.sleep(60)
    return True


# Max partitions per backfill CLI call to avoid "Argument list too long" (SSH + shell).
# ~400 keeps total command line well under typical ARG_MAX.
MAX_PARTITIONS_PER_BACKFILL = 400


def run_remote_backfill(
    remote_host: str,
    remote_dir: str,
    job_name: str,
    partitions_arg: str,
    on_server: bool = False,
) -> int:
    """Run dagster job backfill on remote (or locally when on_server). Returns exit code."""
    cmd = (
        f"cd {remote_dir} && docker compose -f docker-compose.prod.yml run --rm "
        "-e POSTGRES_HOST=postgres "
        f"-v {remote_dir}/docker/workspace.yaml:/workspace.yaml:ro "
        f"-v {remote_dir}/docker/dagster.yaml:/opt/dagster/dagster_home/dagster.yaml:ro "
        "-e DAGSTER_HOME=/opt/dagster/dagster_home "
        "dagster-code "
        f"dagster job backfill -w /workspace.yaml -j {job_name} "
        f"--partitions {partitions_arg!r} -l talent_matching --noprompt"
    )
    if on_server:
        return subprocess.run(["bash", "-c", cmd]).returncode
    return subprocess.run(["ssh", remote_host, cmd]).returncode


def run_remote_backfill_chunked(
    remote_host: str,
    remote_dir: str,
    job_name: str,
    partition_ids: list[str],
    on_server: bool = False,
) -> int:
    """Launch one or more backfills so each CLI call has at most MAX_PARTITIONS_PER_BACKFILL partitions.
    Returns 0 if all succeeded, else last non-zero exit code."""
    if not partition_ids:
        return 0
    chunks = [
        partition_ids[i : i + MAX_PARTITIONS_PER_BACKFILL]
        for i in range(0, len(partition_ids), MAX_PARTITIONS_PER_BACKFILL)
    ]
    for i, chunk in enumerate(chunks):
        partitions_arg = ",".join(chunk)
        print(f"  Backfill chunk {i + 1}/{len(chunks)} ({len(chunk)} partitions)...")
        code = run_remote_backfill(
            remote_host, remote_dir, job_name, partitions_arg, on_server=on_server
        )
        if code != 0:
            return code
    return 0


def main() -> int:
    apply_local_db()
    parser = argparse.ArgumentParser(
        description="Launch test run: N candidates (candidate_pipeline) + 2 jobs (ats_matchmaking_pipeline)."
    )
    parser.add_argument(
        "--local",
        action="store_true",
        help="Use local Postgres (when running on the server).",
    )
    parser.add_argument(
        "--candidates",
        type=int,
        default=100,
        help="Number of candidate partitions to backfill (default 100)",
    )
    parser.add_argument(
        "--jobs",
        type=int,
        default=2,
        help="Number of job partitions to backfill (default 2)",
    )
    parser.add_argument(
        "--offset",
        type=int,
        default=0,
        help="Skip first N candidate partitions (use e.g. 10 to get a different set of 10)",
    )
    parser.add_argument(
        "--sync-candidates",
        action="store_true",
        help="Run sync_airtable_candidates_job on remote first (ensures partition keys exist)",
    )
    parser.add_argument(
        "--candidates-only",
        action="store_true",
        help="Only run candidate_pipeline backfill; skip matchmaking",
    )
    parser.add_argument(
        "--jobs-only",
        action="store_true",
        help="Only run ats_matchmaking_pipeline backfill; skip candidates",
    )
    parser.add_argument(
        "--on-server",
        action="store_true",
        help="Run on the remote server (script must be executed on server host, not in container). No tunnel needed.",
    )
    parser.add_argument(
        "--print-ids",
        action="store_true",
        help="Only query and print CANDIDATE_IDS=... and JOB_IDS=... then exit (for use by a wrapper).",
    )
    parser.add_argument(
        "--all-candidates",
        action="store_true",
        help="Sync from Airtable first (update partitions: add new, remove deleted), then backfill all current candidate partitions (IDs from Airtable). Implies --sync-candidates.",
    )
    args = parser.parse_args()

    on_server = args.on_server
    remote_host = os.getenv("REMOTE_HOST")
    remote_dir = os.getenv("REMOTE_PROJECT_DIR", "/root/match-making")
    if not on_server and not remote_host:
        print("REMOTE_HOST not set in .env (or use --on-server to run on the server).", file=sys.stderr)
        return 1
    if on_server:
        remote_dir = os.getcwd()

    candidate_ids: list[str] = []
    job_ids: list[str] = []

    if not args.jobs_only:
        if args.all_candidates:
            # Sync first to update partitions (add new, remove deleted), then get IDs from Airtable
            print("Step 1: Syncing candidate partitions from Airtable (add new, remove deleted)...")
            if not run_sync_candidates(remote_host, remote_dir, on_server):
                print("Sync failed.", file=sys.stderr)
                return 1
            print("Sync done.\n")
            candidate_ids = get_candidate_partition_ids_from_airtable()
            if not candidate_ids:
                print(
                    "AIRTABLE_BASE_ID, AIRTABLE_TABLE_ID, AIRTABLE_API_KEY not set; cannot get partition list from Airtable.",
                    file=sys.stderr,
                )
                return 1
            print(f"Candidate partitions: {len(candidate_ids)} (from Airtable, current list)")
        else:
            candidate_ids = get_candidate_partition_ids(args.candidates, offset=args.offset)
            if len(candidate_ids) < args.candidates:
                print(
                    f"Only {len(candidate_ids)} candidate partition(s) found in DB (requested {args.candidates}).",
                    file=sys.stderr,
                )
                if not candidate_ids:
                    print("Run sync and ingest first, or use --jobs-only for matchmaking only.", file=sys.stderr)
                    return 1
            else:
                candidate_ids = candidate_ids[: args.candidates]
            print(f"Candidate partitions: {len(candidate_ids)} (from DB)")

    if not args.candidates_only:
        job_ids = get_job_partition_ids(args.jobs)
        if len(job_ids) < args.jobs:
            print(
                f"Only {len(job_ids)} job partition(s) found in DB (requested {args.jobs}).",
                file=sys.stderr,
            )
            if not job_ids:
                print("Run job sync/ingest first, or use --candidates-only.", file=sys.stderr)
                return 1
        else:
            job_ids = job_ids[: args.jobs]
        print(f"Job partitions: {len(job_ids)} (from DB)")

    if args.print_ids:
        print(f"CANDIDATE_IDS={','.join(candidate_ids)}")
        print(f"JOB_IDS={','.join(job_ids)}")
        return 0

    if args.sync_candidates and not args.jobs_only and not args.all_candidates:
        print("Step 1: Syncing candidate partitions (sync_airtable_candidates_job)...")
        if not run_sync_candidates(remote_host, remote_dir, on_server):
            print("Sync failed.", file=sys.stderr)
            return 1
        print("Sync done.\n")

    if not args.jobs_only:
        step = "Step 2" if (args.sync_candidates or args.all_candidates) and not args.jobs_only else "Step 1"
        print(f"{step}: Launching candidate_pipeline backfill ({len(candidate_ids)} partitions)...")
        if len(candidate_ids) > MAX_PARTITIONS_PER_BACKFILL:
            code = run_remote_backfill_chunked(
                remote_host, remote_dir, "candidate_pipeline", candidate_ids, on_server=on_server
            )
        else:
            partitions_arg = ",".join(candidate_ids)
            code = run_remote_backfill(
                remote_host, remote_dir, "candidate_pipeline", partitions_arg, on_server=on_server
            )
        if code != 0:
            return code
        print("Candidate backfill(s) submitted.\n")

    if not args.candidates_only:
        step = "Step 3" if (args.sync_candidates or not args.jobs_only) else "Step 1"
        print(f"{step}: Launching ats_matchmaking_pipeline backfill ({len(job_ids)} partitions)...")
        partitions_arg = ",".join(job_ids)
        code = run_remote_backfill(
            remote_host, remote_dir, "ats_matchmaking_pipeline", partitions_arg, on_server=on_server
        )
        if code != 0:
            return code
        print("Matchmaking backfill submitted.")

    print("")
    print("Check progress: poetry run remote-ui, then http://localhost:3000 → Backfills")
    return 0


if __name__ == "__main__":
    sys.exit(main())
