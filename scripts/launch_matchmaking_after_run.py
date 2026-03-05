#!/usr/bin/env python3
"""Launch ats_matchmaking_pipeline for partitions after an upstream run finishes.

Polls the Dagster GraphQL API until the specified run reaches a terminal status,
then launches matchmaking for each partition.

Prerequisites:
  - poetry run remote-ui RUNNING (webserver at localhost:3000, tunnel to remote)
  - Partitions must exist in the jobs dynamic partition (add via UI if needed)

Usage:
  poetry run python scripts/launch_matchmaking_after_run.py jdhtiqny recABC,recDEF,recGHI
  poetry run python scripts/launch_matchmaking_after_run.py jdhtiqny recABC --poll-interval 15
  poetry run python scripts/launch_matchmaking_after_run.py jdhtiqny recABC --on-failure
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
    with httpx.Client(timeout=60.0) as client:
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
        "run_id",
        help="Run ID to wait for (e.g. jdhtiqny)",
    )
    parser.add_argument(
        "partitions",
        help="Comma-separated partition IDs (e.g. recABC,recDEF,recGHI)",
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
        help="Launch matchmaking even when upstream run fails",
    )
    args = parser.parse_args()

    partition_ids = [p.strip() for p in args.partitions.split(",") if p.strip()]
    if not partition_ids:
        print("No partitions specified", file=sys.stderr)
        return 1

    print(f"Waiting for run {args.run_id} to finish (poll every {args.poll_interval}s)...")
    while True:
        status, err = get_run_status(args.run_id)
        if err:
            print(f"Error querying run: {err}", file=sys.stderr)
            return 1

        if status in TERMINAL_STATUSES:
            print(f"Run {args.run_id} finished with status: {status}")
            break

        print(f"  Run status: {status}")
        time.sleep(args.poll_interval)

    if status == "SUCCESS":
        pass
    elif args.on_failure:
        print("Upstream failed; proceeding anyway (--on-failure)")
    else:
        print(
            f"Upstream run failed (status={status}). Use --on-failure to proceed anyway.",
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
