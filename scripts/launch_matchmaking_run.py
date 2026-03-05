#!/usr/bin/env python3
"""Launch ats_matchmaking_pipeline run via Dagster GraphQL (webserver).

Uses DagsterGraphQLClient which talks to the webserver. The webserver must be
running (poetry run remote-ui) and uses the tunneled Postgres, so the run is
written to the remote DB and executed by the remote daemon.

Usage:
  poetry run remote-ui   # in another terminal
  poetry run python scripts/launch_matchmaking_run.py [partition_id]
"""

import os
import sys

from dagster_graphql import DagsterGraphQLClient

PARTITION_ID = os.environ.get("PARTITION_ID", "recIqBsuF33YrIrMX")


def main() -> int:
    partition_id = sys.argv[1] if len(sys.argv) > 1 else PARTITION_ID
    host = os.environ.get("DAGSTER_UI_HOST", "localhost")
    port = int(os.environ.get("DAGSTER_UI_PORT", "3000"))

    client = DagsterGraphQLClient(host, port_number=port)
    run_id = client.submit_job_execution(
        "ats_matchmaking_pipeline",
        repository_location_name="talent_matching",
        repository_name="__repository__",
        tags={"dagster/partition": partition_id},
    )
    print(f"Launched run {run_id} for partition {partition_id}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
