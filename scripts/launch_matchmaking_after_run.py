#!/usr/bin/env python3
"""Launch ats_matchmaking_pipeline for partitions after an upstream run or backfill finishes.

Polls the Dagster GraphQL API until the specified run or backfill reaches a terminal
status, then launches matchmaking for each partition.

Prerequisites:
  - poetry run remote-ui RUNNING (webserver at localhost:3000, tunnel to remote)
  - Partitions must exist in the jobs dynamic partition (add via UI if needed)
  - Deploy to remote first so the daemon has latest code (schedule_matchmaking_after_backfill --deploy or run_remote_matchmaking.sh)

Usage:
  poetry run python scripts/launch_matchmaking_after_run.py <run_id> recABC,recDEF,recGHI
  poetry run python scripts/launch_matchmaking_after_run.py --backfill-id jdhtiqny recABC,recDEF --poll-interval 15
  poetry run python scripts/launch_matchmaking_after_run.py --backfill-id jdhtiqny recABC --on-failure
"""

import argparse
import json
import sys
import time

import httpx

GRAPHQL_URL = "http://localhost:3000/graphql"
REPO_LOCATION = "talent_matching"
REPO_NAME = "__repository__"
JOB_NAME = "ats_matchmaking_pipeline"

RUN_STATUS_QUERY = """
query RunStatus($runId: ID!) {
  runOrError(runId: $runId) {
    __typename
    ... on Run {
      status
    }
    ... on RunNotFoundError {
      message
    }
  }
}
"""

BACKFILL_STATUS_QUERY = """
query BackfillStatus($backfillId: String!) {
  partitionBackfillOrError(backfillId: $backfillId) {
    __typename
    ... on PartitionBackfill {
      id
      status
    }
    ... on BackfillNotFoundError {
      message
    }
  }
}
"""

LAUNCH_MUTATION = """
mutation LaunchRun($executionParams: ExecutionParams!) {
  launchRun(executionParams: $executionParams) {
    __typename
    ... on LaunchRunSuccess {
      run { runId }
    }
    ... on RunConfigValidationInvalid {
      errors { message }
    }
    ... on PythonError {
      message
    }
  }
}
"""

TERMINAL_STATUSES = frozenset({"SUCCESS", "FAILURE", "CANCELED", "CANCELING"})
BACKFILL_TERMINAL_STATUSES = frozenset(
    {"COMPLETED", "COMPLETED_FAILED", "FAILED", "FAILING", "CANCELED"}
)


def get_run_status(run_id: str) -> tuple[str | None, str | None]:
    """Query run status. Returns (status, error_message)."""
    variables = {"runId": run_id}
    with httpx.Client(timeout=30.0) as client:
        resp = client.post(
            GRAPHQL_URL,
            json={"query": RUN_STATUS_QUERY, "variables": variables},
            headers={"Content-Type": "application/json"},
        )
        resp.raise_for_status()
        data = resp.json()

    if "errors" in data:
        return None, json.dumps(data["errors"])

    result = data.get("data", {}).get("runOrError", {})
    typename = result.get("__typename")

    if typename == "Run":
        return result.get("status"), None
    if typename == "RunNotFoundError":
        return None, result.get("message", "Run not found")
    return None, f"Unexpected response: {result}"


def get_backfill_status(backfill_id: str) -> tuple[str | None, str | None]:
    """Query backfill status. Returns (status, error_message)."""
    variables = {"backfillId": backfill_id}
    with httpx.Client(timeout=30.0) as client:
        resp = client.post(
            GRAPHQL_URL,
            json={"query": BACKFILL_STATUS_QUERY, "variables": variables},
            headers={"Content-Type": "application/json"},
        )
        resp.raise_for_status()
        data = resp.json()

    if "errors" in data:
        return None, json.dumps(data["errors"])

    result = data.get("data", {}).get("partitionBackfillOrError", {})
    typename = result.get("__typename")

    if typename == "PartitionBackfill":
        return result.get("status"), None
    if typename == "BackfillNotFoundError":
        return None, result.get("message", "Backfill not found")
    return None, f"Unexpected response: {result}"


def launch_matchmaking_run(partition_id: str) -> dict:
    """Launch ats_matchmaking_pipeline for the given partition. Returns GraphQL response."""
    variables = {
        "executionParams": {
            "selector": {
                "repositoryLocationName": REPO_LOCATION,
                "repositoryName": REPO_NAME,
                "jobName": JOB_NAME,
            },
            "runConfigData": {},
            "executionMetadata": {
                "tags": [{"key": "dagster/partition", "value": partition_id}],
            },
        }
    }
    with httpx.Client(timeout=120.0) as client:
        resp = client.post(
            GRAPHQL_URL,
            json={"query": LAUNCH_MUTATION, "variables": variables},
            headers={"Content-Type": "application/json"},
        )
        resp.raise_for_status()
        return resp.json()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Launch ats_matchmaking_pipeline after an upstream run finishes."
    )
    parser.add_argument(
        "run_or_backfill_id",
        nargs="?",
        help="Run ID or backfill ID to wait for",
    )
    parser.add_argument(
        "partitions",
        nargs="?",
        help="Comma-separated partition IDs (e.g. recABC,recDEF,recGHI)",
    )
    parser.add_argument(
        "--backfill-id",
        metavar="ID",
        help="Treat first arg as backfill ID (poll partitionBackfillOrError)",
    )
    parser.add_argument(
        "--poll-interval",
        type=int,
        default=30,
        help="Seconds between status checks (default: 30)",
    )
    parser.add_argument(
        "--on-failure",
        action="store_true",
        help="Launch matchmaking even when upstream run/backfill fails",
    )
    args = parser.parse_args()

    if args.backfill_id:
        target_id = args.backfill_id
        partitions_arg = args.run_or_backfill_id or args.partitions or ""
    else:
        target_id = args.run_or_backfill_id
        partitions_arg = args.partitions or ""
    if not target_id:
        parser.error("Provide run_id or --backfill-id <id>")
    partition_ids = [p.strip() for p in partitions_arg.split(",") if p.strip()]
    if not partition_ids:
        print("No partitions specified", file=sys.stderr)
        return 1

    is_backfill = bool(args.backfill_id)
    terminal_statuses = BACKFILL_TERMINAL_STATUSES if is_backfill else TERMINAL_STATUSES
    success_status = "COMPLETED" if is_backfill else "SUCCESS"

    print(
        f"Waiting for {'backfill' if is_backfill else 'run'} {target_id} to finish "
        f"(poll every {args.poll_interval}s)..."
    )
    while True:
        if is_backfill:
            status, err = get_backfill_status(target_id)
        else:
            status, err = get_run_status(target_id)
        if err:
            print(f"Error querying: {err}", file=sys.stderr)
            return 1

        if status in terminal_statuses:
            print(
                f"{'Backfill' if is_backfill else 'Run'} {target_id} finished with status: {status}"
            )
            break

        print(f"  Status: {status}")
        time.sleep(args.poll_interval)

    if status == success_status:
        pass
    elif args.on_failure:
        print("Upstream failed; proceeding anyway (--on-failure)")
    else:
        print(
            f"Upstream failed (status={status}). Use --on-failure to proceed anyway.",
            file=sys.stderr,
        )
        return 1

    print(f"\nLaunching {JOB_NAME} for {len(partition_ids)} partition(s)...")
    for partition_id in partition_ids:
        print(f"  Launching partition: {partition_id}")
        data = launch_matchmaking_run(partition_id)
        if "errors" in data:
            print(f"    GraphQL errors: {json.dumps(data['errors'], indent=2)}", file=sys.stderr)
            return 1
        result = data.get("data", {}).get("launchRun", {})
        typename = result.get("__typename")
        if typename == "LaunchRunSuccess":
            run_id = result.get("run", {}).get("runId")
            print(f"    Run submitted: {run_id}")
        elif typename == "RunConfigValidationInvalid":
            print(f"    Config validation failed: {result.get('errors')}", file=sys.stderr)
            return 1
        elif typename == "PythonError":
            print(f"    Server error: {result.get('message')}", file=sys.stderr)
            return 1
        else:
            print(f"    Unexpected response: {result}", file=sys.stderr)
            return 1

    print("\nDone. Check status: http://localhost:3000 → Runs")
    return 0


if __name__ == "__main__":
    sys.exit(main())
