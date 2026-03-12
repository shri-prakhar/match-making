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

  # Cancel all runs on local Dagster (explicit localhost:3000)
  poetry run python scripts/force_cancel_dagster_runs.py --local --job ''

  # Custom Dagster GraphQL URL
  poetry run python scripts/force_cancel_dagster_runs.py --url http://localhost:3000/graphql

When using remote-ui, the UI is at localhost:3000 but runs execute on the remote. If the
script reports "Marked N as canceled" but runs don't stop, the remote run worker may be
stuck (e.g. blocked on I/O or LLM). Restart the remote dagster-daemon or run container.
"""

from __future__ import annotations

import argparse
import json
import os
import sys

import httpx

LOCAL_GRAPHQL_URL = "http://localhost:3000/graphql"

# Statuses we consider "in progress" and safe to force-cancel
IN_PROGRESS_STATUSES = ["NOT_STARTED", "STARTING", "QUEUED", "STARTED", "CANCELING"]

# Dagster 2.x uses jobName; 1.x uses pipelineName. Request both for compatibility.
LIST_RUNS_QUERY = """
query ListRuns($filter: RunsFilter, $limit: Int) {
  runsOrError(filter: $filter, limit: $limit) {
    __typename
    ... on Runs {
      results {
        runId
        pipelineName
        jobName
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
        "--local",
        action="store_true",
        help="Use local Dagster (localhost:3000). Overrides DAGSTER_GRAPHQL_URL if set.",
    )
    parser.add_argument(
        "--url",
        default=None,
        help="Dagster GraphQL endpoint (default: localhost:3000 or DAGSTER_GRAPHQL_URL).",
    )
    parser.add_argument(
        "--job",
        default="ats_matchmaking_pipeline",
        help="Job name to filter (e.g. ats_matchmaking_pipeline). Use empty string for all jobs.",
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
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="Terminate runs in batches of this size (default 50) to avoid timeouts",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Print each run's job name and status; print per-run terminate result",
    )
    args = parser.parse_args()

    url = args.url
    if args.local:
        url = LOCAL_GRAPHQL_URL
    elif url is None:
        url = os.environ.get("DAGSTER_GRAPHQL_URL", LOCAL_GRAPHQL_URL)

    job_filter = "all jobs" if args.job == "" else args.job
    print(f"GraphQL URL: {url}", file=sys.stderr)
    print(f"Job filter: {job_filter}", file=sys.stderr)

    with httpx.Client(timeout=30.0) as client:
        run_ids = []
        run_info: list[tuple[str, str, str]] = []  # (run_id, pipeline_name, status)
        for status in IN_PROGRESS_STATUSES:
            variables = {
                "filter": {"statuses": [status]},
                "limit": args.limit,
            }
            resp = client.post(
                url,
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
                job_name = r.get("jobName") or r.get("pipelineName")
                if args.job and job_name != args.job:
                    continue
                run_ids.append(r["runId"])
                run_info.append((r["runId"], job_name or "?", r.get("status") or "?"))

        run_ids = list(dict.fromkeys(run_ids))
        if not run_ids:
            print("No in-progress runs found.", file=sys.stderr)
            print(
                "  Check: URL correct? Job name (use --job '' for all)? Runs in STARTED/QUEUED/...?",
                file=sys.stderr,
            )
            return 0

        print(f"Found {len(run_ids)} in-progress run(s):")
        for rid in run_ids:
            if args.verbose:
                info = next((t for t in run_info if t[0] == rid), (rid, "?", "?"))
                print(f"  {rid}  job={info[1]}  status={info[2]}")
            else:
                print(f"  {rid}")

        if args.dry_run:
            print("Dry run: not terminating.")
            return 0

        # Terminate in batches to avoid request timeouts
        batch_size = max(1, args.batch_size)
        terminate_timeout = 300.0
        total_ok = 0
        all_fail: list = []
        mutation = TERMINATE_RUNS_MUTATION
        use_policy = True

        for i in range(0, len(run_ids), batch_size):
            batch = run_ids[i : i + batch_size]
            if use_policy:
                mutation = TERMINATE_RUNS_MUTATION
                variables = {
                    "runIds": batch,
                    "terminatePolicy": "MARK_AS_CANCELED_IMMEDIATELY",
                }
            else:
                mutation = TERMINATE_RUNS_MUTATION_NO_POLICY
                variables = {"runIds": batch}
            resp = client.post(
                url,
                json={"query": mutation, "variables": variables},
                headers={"Content-Type": "application/json"},
                timeout=terminate_timeout,
            )
            resp.raise_for_status()
            data = resp.json()
            if "errors" in data:
                err_msg = json.dumps(data["errors"], indent=2)
                if use_policy and ("Unknown argument" in err_msg or "terminatePolicy" in err_msg):
                    use_policy = False
                    mutation = TERMINATE_RUNS_MUTATION_NO_POLICY
                    variables = {"runIds": batch}
                    resp = client.post(
                        url,
                        json={"query": mutation, "variables": variables},
                        headers={"Content-Type": "application/json"},
                        timeout=terminate_timeout,
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
            for r in results:
                if r.get("__typename") == "TerminateRunSuccess":
                    total_ok += 1
                    if args.verbose:
                        print(f"  OK {r.get('run', {}).get('runId', '?')}")
                else:
                    all_fail.append(r)
                    if args.verbose:
                        print(
                            f"  FAIL {r.get('runId', '?')}: {r.get('message', r)}", file=sys.stderr
                        )
            if not args.verbose:
                print(f"Batch {1 + i // batch_size}: canceled {len(batch)} run(s).")

        print(f"Marked {total_ok} run(s) as canceled.")
        if all_fail:
            for r in all_fail:
                print(f"  Failure: {r.get('message') or r.get('runId') or r}", file=sys.stderr)
        return 0 if not all_fail else 1


if __name__ == "__main__":
    sys.exit(main())
