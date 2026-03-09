#!/usr/bin/env python3
"""Launch ats_matchmaking_pipeline via GraphQL API (webserver).

Uses the same path as the Dagster UI "Launch run" button. The run is created
in the instance DB and picked up by the remote daemon.

Prerequisites:
  - poetry run remote-ui RUNNING (webserver at localhost:3000, tunnel to remote)
  - Partition must exist in the jobs dynamic partition (add via UI if needed)

Usage:
  poetry run python scripts/launch_matchmaking_via_graphql.py [partition_id]
  poetry run python scripts/launch_matchmaking_via_graphql.py rec2bjCVT0rRh0Bia
"""

import json
import sys

import httpx

GRAPHQL_URL = "http://localhost:3000/graphql"
REPO_LOCATION = "talent_matching"
REPO_NAME = "__repository__"
JOB_NAME = "ats_matchmaking_pipeline"

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


def launch_run(partition_id: str) -> dict:
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
    partition_id = sys.argv[1] if len(sys.argv) > 1 else "recIqBsuF33YrIrMX"
    print(f"Launching {JOB_NAME} for partition: {partition_id}")
    print(f"  (via GraphQL at {GRAPHQL_URL})")
    data = launch_run(partition_id)
    if "errors" in data:
        print("GraphQL errors:", json.dumps(data["errors"], indent=2), file=sys.stderr)
        return 1
    result = data.get("data", {}).get("launchRun", {})
    typename = result.get("__typename")
    if typename == "LaunchRunSuccess":
        run_id = result.get("run", {}).get("runId")
        print(f"  Run submitted: {run_id}")
        print("  Check status: http://localhost:3000 → Runs")
        return 0
    if typename == "RunConfigValidationInvalid":
        print("Config validation failed:", result.get("errors"), file=sys.stderr)
        return 1
    if typename == "PythonError":
        print("Server error:", result.get("message"), file=sys.stderr)
        return 1
    print("Unexpected response:", json.dumps(result, indent=2), file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
