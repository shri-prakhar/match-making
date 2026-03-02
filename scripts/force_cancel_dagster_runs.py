"""Force-cancel stuck or in-progress Dagster runs via GraphQL.

Use when the UI cancel fails or runs are stuck (e.g. after daemon restart).
Uses terminatePolicy: MARK_AS_CANCELED_IMMEDIATELY so runs are marked canceled
without waiting for graceful shutdown.

Usage:
  # Cancel all in-progress matchmaking runs (default)
  poetry run python scripts/force_cancel_dagster_runs.py

  # Cancel all in-progress runs (any job)
  poetry run python scripts/force_cancel_dagster_runs.py --job ''

  # Dry run: only list run IDs, do not terminate
  poetry run python scripts/force_cancel_dagster_runs.py --dry-run

  # Custom Dagster GraphQL URL
  poetry run python scripts/force_cancel_dagster_runs.py --url http://localhost:3000/graphql
"""

from __future__ import annotations

import argparse
import json
import sys

import httpx

# Statuses we consider "in progress" and safe to force-cancel
IN_PROGRESS_STATUSES = ["STARTING", "QUEUED", "STARTED", "CANCELING"]

LIST_RUNS_QUERY = """
query ListRuns($filter: RunsFilter, $limit: Int) {
  runsOrError(filter: $filter, limit: $limit) {
    __typename
    ... on Runs {
      results {
        runId
        pipelineName
        status
      }
    }
    ... on InvalidPipelineRunsFilterError {
      message
    }
    ... on PythonError {
      message
    }
  }
}
"""

TERMINATE_RUNS_MUTATION = """
mutation TerminateRuns($runIds: [String!]!, $terminatePolicy: TerminateRunPolicy) {
  terminateRuns(runIds: $runIds, terminatePolicy: $terminatePolicy) {
    __typename
    ... on TerminateRunsResult {
      terminateRunResults {
        __typename
        ... on TerminateRunSuccess {
          run { runId }
        }
        ... on TerminateRunFailure {
          message
        }
        ... on RunNotFoundError {
          runId
          message
        }
      }
    }
  }
}
"""

TERMINATE_RUNS_MUTATION_NO_POLICY = """
mutation TerminateRuns($runIds: [String!]!) {
  terminateRuns(runIds: $runIds) {
    __typename
    ... on TerminateRunsResult {
      terminateRunResults {
        __typename
        ... on TerminateRunSuccess {
          run { runId }
        }
        ... on TerminateRunFailure {
          message
        }
        ... on RunNotFoundError {
          runId
          message
        }
      }
    }
  }
}
"""


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Force-cancel in-progress Dagster runs via GraphQL (MARK_AS_CANCELED_IMMEDIATELY)."
    )
    parser.add_argument(
        "--url",
        default="http://localhost:3000/graphql",
        help="Dagster GraphQL endpoint",
    )
    parser.add_argument(
        "--job",
        default="matchmaking",
        help="Job name to filter (e.g. matchmaking). Use empty string for all jobs.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=500,
        help="Max runs to fetch per status (default 500)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only list run IDs, do not send terminate mutation",
    )
    args = parser.parse_args()

    with httpx.Client(timeout=30.0) as client:
        run_ids = []
        for status in IN_PROGRESS_STATUSES:
            variables = {
                "filter": {"statuses": [status]},
                "limit": args.limit,
            }
            resp = client.post(
                args.url,
                json={"query": LIST_RUNS_QUERY, "variables": variables},
                headers={"Content-Type": "application/json"},
            )
            resp.raise_for_status()
            data = resp.json()
            if "errors" in data:
                print("GraphQL errors:", json.dumps(data["errors"], indent=2), file=sys.stderr)
                return 1
            roe = data.get("data", {}).get("runsOrError", {})
            if roe.get("__typename") == "InvalidPipelineRunsFilterError":
                print("Filter error:", roe.get("message", roe), file=sys.stderr)
                return 1
            if roe.get("__typename") == "PythonError":
                print("Server error:", roe.get("message", roe), file=sys.stderr)
                return 1
            results = roe.get("results") or []
            for r in results:
                if args.job and r.get("pipelineName") != args.job:
                    continue
                run_ids.append(r["runId"])

        run_ids = list(dict.fromkeys(run_ids))
        if not run_ids:
            print("No in-progress runs found.")
            return 0

        print(f"Found {len(run_ids)} in-progress run(s):")
        for rid in run_ids:
            print(f"  {rid}")

        if args.dry_run:
            print("Dry run: not terminating.")
            return 0

        mutation = TERMINATE_RUNS_MUTATION
        variables = {
            "runIds": run_ids,
            "terminatePolicy": "MARK_AS_CANCELED_IMMEDIATELY",
        }
        resp = client.post(
            args.url,
            json={"query": mutation, "variables": variables},
            headers={"Content-Type": "application/json"},
        )
        resp.raise_for_status()
        data = resp.json()
        if "errors" in data:
            err_msg = json.dumps(data["errors"], indent=2)
            if "Unknown argument" in err_msg or "terminatePolicy" in err_msg:
                mutation = TERMINATE_RUNS_MUTATION_NO_POLICY
                variables = {"runIds": run_ids}
                resp = client.post(
                    args.url,
                    json={"query": mutation, "variables": variables},
                    headers={"Content-Type": "application/json"},
                )
                resp.raise_for_status()
                data = resp.json()
            if "errors" in data:
                print(
                    "Terminate mutation errors:",
                    json.dumps(data["errors"], indent=2),
                    file=sys.stderr,
                )
                return 1
        result = data.get("data", {}).get("terminateRuns", {})
        if result.get("__typename") != "TerminateRunsResult":
            print("Unexpected terminate result:", result, file=sys.stderr)
            return 1
        results = result.get("terminateRunResults") or []
        ok = sum(1 for r in results if r.get("__typename") == "TerminateRunSuccess")
        fail = [r for r in results if r.get("__typename") != "TerminateRunSuccess"]
        print(f"Marked {ok} run(s) as canceled.")
        if fail:
            for r in fail:
                print(f"  Failure: {r.get('message') or r.get('runId') or r}", file=sys.stderr)
        return 0 if not fail else 1


if __name__ == "__main__":
    sys.exit(main())
