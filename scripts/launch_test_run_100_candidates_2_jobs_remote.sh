#!/usr/bin/env bash
# Launch test run (100 candidates + 2 jobs) entirely on the remote server. No tunnel needed.
#
# Prerequisites: REMOTE_HOST in .env; remote has project deployed.
#
# Usage:
#   ./scripts/launch_test_run_100_candidates_2_jobs_remote.sh
#   ./scripts/launch_test_run_100_candidates_2_jobs_remote.sh --sync-candidates
#   ./scripts/launch_test_run_100_candidates_2_jobs_remote.sh --candidates 50

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
if [ -f "$PROJECT_ROOT/.env" ]; then
  set -a
  source "$PROJECT_ROOT/.env"
  set +a
fi

REMOTE_HOST="${REMOTE_HOST:-}"
REMOTE_DIR="${REMOTE_PROJECT_DIR:-/root/match-making}"
CANDIDATES=100
JOBS=2
SYNC=""

for arg in "$@"; do
  case "$arg" in
    --sync-candidates) SYNC="y" ;;
  esac
done

if [ -z "$REMOTE_HOST" ]; then
  echo "REMOTE_HOST not set in .env"
  exit 1
fi

echo "Step 1: Querying partition IDs on remote (run script in container with --print-ids)..."
OUTPUT=$(ssh "$REMOTE_HOST" "cd $REMOTE_DIR && docker compose -f docker-compose.prod.yml run --rm \
  -v \$(pwd):/opt/dagster/app:ro \
  -e PYTHONPATH=/opt/dagster/app \
  -e POSTGRES_HOST=postgres \
  dagster-code \
  python scripts/launch_test_run_100_candidates_2_jobs.py --print-ids --candidates $CANDIDATES --jobs $JOBS" 2>/dev/null | grep -E '^CANDIDATE_IDS=|^JOB_IDS=')

CANDIDATE_IDS=""
JOB_IDS=""
while IFS= read -r line; do
  case "$line" in
    CANDIDATE_IDS=*) CANDIDATE_IDS="${line#CANDIDATE_IDS=}" ;;
    JOB_IDS=*)      JOB_IDS="${line#JOB_IDS=}" ;;
  esac
done <<< "$OUTPUT"

if [ -z "$CANDIDATE_IDS" ] && [ -z "$JOB_IDS" ]; then
  echo "Failed to get partition IDs. Check that raw_candidates and raw_jobs have data on the remote DB."
  exit 1
fi

run_backfill() {
  local job_name="$1"
  local partitions="$2"
  [ -z "$partitions" ] && return 0
  echo "Launching $job_name backfill..."
  # Pass partitions as a single quoted argument to the remote
  ssh "$REMOTE_HOST" "cd $REMOTE_DIR && docker compose -f docker-compose.prod.yml run --rm \
    -e POSTGRES_HOST=postgres \
    -v $REMOTE_DIR/docker/workspace.yaml:/workspace.yaml:ro \
    -v $REMOTE_DIR/docker/dagster.yaml:/opt/dagster/dagster_home/dagster.yaml:ro \
    -e DAGSTER_HOME=/opt/dagster/dagster_home \
    dagster-code \
    dagster job backfill -w /workspace.yaml -j $job_name --partitions \"$partitions\" -l talent_matching --noprompt"
}

if [ "$SYNC" = "y" ]; then
  echo "Step 2: Syncing candidate partitions..."
  ssh "$REMOTE_HOST" "cd $REMOTE_DIR && docker compose -f docker-compose.prod.yml run --rm \
    -e POSTGRES_HOST=postgres \
    -v $REMOTE_DIR/docker/workspace.yaml:/workspace.yaml:ro \
    -v $REMOTE_DIR/docker/dagster.yaml:/opt/dagster/dagster_home/dagster.yaml:ro \
    -e DAGSTER_HOME=/opt/dagster/dagster_home \
    dagster-code \
    dagster job launch -w /workspace.yaml -j sync_airtable_candidates_job -l talent_matching"
  echo "Waiting 60s for partitions to register..."
  sleep 60
fi

echo "Step 2: Launching candidate_pipeline backfill..."
run_backfill "candidate_pipeline" "$CANDIDATE_IDS"

echo "Step 3: Launching ats_matchmaking_pipeline backfill..."
run_backfill "ats_matchmaking_pipeline" "$JOB_IDS"

echo ""
echo "Backfills submitted. Check: poetry run remote-ui, then http://localhost:3000 → Backfills"
