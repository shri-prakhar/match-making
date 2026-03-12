#!/bin/bash
#
# Launch matchmaking_backfill for all partitions (or specified partitions).
#
# Prerequisites:
#   - poetry run remote-ui RUNNING (tunnel localhost:15432 → remote Postgres, 4266 → remote gRPC)
#   - .env has POSTGRES_*; we force POSTGRES_HOST=localhost and POSTGRES_PORT=15432 for the tunnel
#
# Usage:
#   ./scripts/launch_matchmaking_backfill.sh           # backfill ALL partitions
#   ./scripts/launch_matchmaking_backfill.sh recA,recB  # backfill specific partitions
#
# The backfill is submitted to the remote instance (same DB as remote-ui).
# Check progress: http://localhost:3000 → Backfills

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

if [ -f "$PROJECT_ROOT/.env" ]; then
    set -a
    source "$PROJECT_ROOT/.env"
    set +a
fi

# Force instance to use REMOTE DB via tunnel
export POSTGRES_HOST=localhost
export POSTGRES_PORT="${POSTGRES_REMOTE_TUNNEL_PORT:-15432}"
unset DAGSTER_POSTGRES_PORT 2>/dev/null || true

# Use same instance config as remote daemon (writes to tunneled Postgres)
export DAGSTER_HOME=$(mktemp -d)
ln -sf "$PROJECT_ROOT/dagster.yaml" "$DAGSTER_HOME/dagster.yaml"
trap 'rm -rf "$DAGSTER_HOME"' EXIT

echo "Launching matchmaking_backfill (no LLM refinement)"
echo "  Instance: remote DB at $POSTGRES_HOST:$POSTGRES_PORT"
echo ""

cd "$PROJECT_ROOT"

if [ -n "${1:-}" ]; then
    echo "  Partitions: $1"
    poetry run dagster job backfill \
        -w docker/workspace-local.yaml \
        -j matchmaking_backfill \
        --partitions "$1" \
        -l talent_matching \
        --noprompt
else
    echo "  Partitions: ALL"
    poetry run dagster job backfill \
        -w docker/workspace-local.yaml \
        -j matchmaking_backfill \
        --all \
        -l talent_matching \
        --noprompt
fi

echo ""
echo "Backfill submitted. Check: http://localhost:3000 → Backfills"
