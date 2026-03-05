#!/bin/bash
#
# Launch one ats_matchmaking_pipeline run on the REMOTE backend.
# The run is written to the remote Postgres (via tunnel) so the remote daemon executes it.
#
# Prerequisites:
#   - poetry run remote-ui RUNNING (tunnel localhost:15432 → remote Postgres, 4266 → remote gRPC)
#   - .env has POSTGRES_*; we force POSTGRES_HOST=localhost and POSTGRES_PORT=15432 for the tunnel
#
# If no run appears: launch from the UI instead (Jobs → ats_matchmaking_pipeline → Launch run).
#
# Usage:
#   ./scripts/launch_remote_matchmaking_run.sh [partition_id]
#   ./scripts/launch_remote_matchmaking_run.sh recIqBsuF33YrIrMX

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PARTITION_ID="${1:-recIqBsuF33YrIrMX}"

if [ -f "$PROJECT_ROOT/.env" ]; then
    set -a
    source "$PROJECT_ROOT/.env"
    set +a
fi

# Force run to be written to REMOTE DB via tunnel. POSTGRES_HOST must be localhost.
export POSTGRES_HOST=localhost
export POSTGRES_PORT=15432
unset DAGSTER_POSTGRES_PORT 2>/dev/null || true

export DAGSTER_HOME=$(mktemp -d)
ln -sf "$PROJECT_ROOT/dagster.yaml" "$DAGSTER_HOME/dagster.yaml"
trap 'rm -rf "$DAGSTER_HOME"' EXIT

RUN_ID="${RUN_ID:-$(uuidgen 2>/dev/null || python3 -c 'import uuid; print(uuid.uuid4())')}"

echo "Launching ats_matchmaking_pipeline for partition: $PARTITION_ID"
echo "  (run written to remote DB at $POSTGRES_HOST:$POSTGRES_PORT)"
echo ""

cd "$PROJECT_ROOT"
poetry run dagster job launch \
  -w docker/workspace-local.yaml \
  -j ats_matchmaking_pipeline \
  --tags "{\"dagster/partition\": \"$PARTITION_ID\"}" \
  --run-id "$RUN_ID" \
  -l talent_matching

echo ""
echo "Run submitted."
echo "  Run ID:    $RUN_ID"
echo "  Partition: $PARTITION_ID"
echo "  Status:    http://localhost:3000 → Runs"
