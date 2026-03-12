#!/bin/bash
#
# Run matchmaking pipeline LOCALLY for fast debug loops (no deploy needed).
# Uses remote Postgres via SSH tunnel so you have real data; code executes on your machine.
#
# Architecture:
#   LOCAL                              REMOTE
#   ┌─────────────────────────────┐    ┌──────────────────┐
#   │ dagster dev                 │    │ PostgreSQL       │
#   │  (webserver + daemon + code)│◄───│ (tunnel 5432→   │
#   │  runs execute here          │    │  localhost:15432)│
#   └─────────────────────────────┘    └──────────────────┘
#
# Prerequisites:
#   - .env with REMOTE_HOST, POSTGRES_*
#   - SSH access to remote
#
# Usage:
#   ./scripts/local-matchmaking-dev.sh
#   # Then in another terminal:
#   #   ./scripts/launch_remote_matchmaking_run.sh rec2bjCVT0rRh0Bia
#   # Or use UI: http://localhost:3000 → Jobs → ats_matchmaking_pipeline → Launch run
#
# To inspect results:
#   poetry run with-remote-db python scripts/inspect_matches.py <partition_id>

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

if [ -f .env ]; then
    set -a
    source .env
    set +a
fi

REMOTE_HOST="${REMOTE_HOST:-}"
if [ -z "$REMOTE_HOST" ]; then
    echo "Error: REMOTE_HOST not set in .env"
    exit 1
fi

LOCAL_PG_PORT="${POSTGRES_REMOTE_TUNNEL_PORT:-15432}"
REMOTE_PG_PORT="${POSTGRES_PORT:-5432}"

cleanup() {
    echo ""
    echo "Shutting down..."
    [ -n "${TUNNEL_PID:-}" ] && kill "$TUNNEL_PID" 2>/dev/null || true
    echo "SSH tunnel closed."
}
trap cleanup EXIT INT TERM

echo "============================================"
echo "  Local Matchmaking Dev (fast debug loop)"
echo "============================================"
echo ""
echo "  Remote:       $REMOTE_HOST"
echo "  Tunnel:       PG $REMOTE_PG_PORT → localhost:$LOCAL_PG_PORT"
echo "  UI:           http://localhost:3000"
echo ""
echo "  Code runs locally; DB is remote (tunneled)."
echo "  No deploy needed — edit code and re-run."
echo ""

# ── Open Postgres tunnel ─────────────────────────────────────────────────────

echo "[1/2] Opening Postgres tunnel..."
ssh -N -f \
    -L "$LOCAL_PG_PORT:localhost:$REMOTE_PG_PORT" \
    -o ExitOnForwardFailure=yes \
    -o ServerAliveInterval=15 \
    -o ServerAliveCountMax=3 \
    -o TCPKeepAlive=yes \
    "$REMOTE_HOST"

TUNNEL_PID=$(pgrep -f "ssh -N -f.*$REMOTE_HOST" | tail -1)
echo "       Tunnel open (PID: ${TUNNEL_PID:-unknown})"
echo ""

# ── Run dagster dev ─────────────────────────────────────────────────────────

echo "[2/2] Starting dagster dev (local webserver + daemon + code)..."
echo ""

export POSTGRES_HOST=localhost
export POSTGRES_PORT="$LOCAL_PG_PORT"
export DAGSTER_HOME="${DAGSTER_HOME:-$PROJECT_ROOT}"

# Use workspace with location_name: talent_matching so runs match remote-ui/daemon
poetry run dagster dev -w "$PROJECT_ROOT/docker/workspace-local-dev.yaml"
