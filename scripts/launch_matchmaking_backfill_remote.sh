#!/bin/bash
#
# Launch matchmaking_backfill on the REMOTE server (runs inside remote Docker).
# The backfill is created in the remote instance, so the remote daemon picks it up.
#
# Prerequisites:
#   - REMOTE_HOST in .env
#   - Deploy first so the remote has matchmaking_backfill job
#
# Usage:
#   ./scripts/launch_matchmaking_backfill_remote.sh           # backfill ALL partitions
#   ./scripts/launch_matchmaking_backfill_remote.sh recA,recB  # backfill specific partitions
#
# Check progress: poetry run remote-ui, then http://localhost:3000 → Backfills

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

if [ -f "$PROJECT_ROOT/.env" ]; then
    set -a
    source "$PROJECT_ROOT/.env"
    set +a
fi

REMOTE_HOST="${REMOTE_HOST:-}"
REMOTE_DIR="${REMOTE_PROJECT_DIR:-/root/match-making}"

if [ -z "$REMOTE_HOST" ]; then
    echo "Error: REMOTE_HOST not set in .env"
    exit 1
fi

echo "Launching matchmaking_backfill on remote server..."
echo "  Remote: $REMOTE_HOST"
echo ""

# Mount workspace and dagster.yaml so backfill uses remote instance
if [ -n "${1:-}" ]; then
    echo "  Partitions: $1"
    ssh "$REMOTE_HOST" "cd $REMOTE_DIR && docker compose -f docker-compose.prod.yml run --rm \
        -e POSTGRES_HOST=postgres \
        -v $REMOTE_DIR/docker/workspace.yaml:/workspace.yaml:ro \
        -v $REMOTE_DIR/docker/dagster.yaml:/opt/dagster/dagster_home/dagster.yaml:ro \
        -e DAGSTER_HOME=/opt/dagster/dagster_home \
        dagster-code \
        dagster job backfill \
        -w /workspace.yaml \
        -j matchmaking_backfill \
        --partitions '$1' \
        -l talent_matching \
        --noprompt"
else
    echo "  Partitions: ALL"
    ssh "$REMOTE_HOST" "cd $REMOTE_DIR && docker compose -f docker-compose.prod.yml run --rm \
        -e POSTGRES_HOST=postgres \
        -v $REMOTE_DIR/docker/workspace.yaml:/workspace.yaml:ro \
        -v $REMOTE_DIR/docker/dagster.yaml:/opt/dagster/dagster_home/dagster.yaml:ro \
        -e DAGSTER_HOME=/opt/dagster/dagster_home \
        dagster-code \
        dagster job backfill \
        -w /workspace.yaml \
        -j matchmaking_backfill \
        --all \
        -l talent_matching \
        --noprompt"
fi

echo ""
echo "Backfill submitted. Check: poetry run remote-ui, then http://localhost:3000 → Backfills"
