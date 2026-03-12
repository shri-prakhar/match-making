#!/bin/bash
#
# Hard reset Dagster: stop daemon/code-server, cancel all runs in the DB, and
# ensure any run subprocesses are gone (stopping containers kills them).
#
# Use when the UI/GraphQL terminate is stuck and runs keep running. This stops
# the Dagster containers, marks all in-progress runs as CANCELED in Postgres,
# and leaves containers stopped so you can restart cleanly.
#
# Prerequisites:
#   - From laptop: REMOTE_HOST (and optionally REMOTE_PROJECT_DIR) in .env; SSH access.
#   - On server: run from project root with .env and poetry; docker compose available.
#
# Usage:
#   # From laptop (SSH to server, stop containers, cancel runs in DB):
#   ./scripts/hard_reset_dagster.sh
#
#   # On the server (stop containers and cancel runs locally):
#   ./scripts/hard_reset_dagster.sh
#
#   # Only cancel runs in DB (no container stop). From laptop with tunnel:
#   poetry run with-remote-db python scripts/hard_reset_dagster.py
#
# After a full hard reset, start Dagster again on the server:
#   cd $REMOTE_DIR && docker compose -f docker-compose.prod.yml up -d
#

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
if [ -f "$PROJECT_ROOT/.env" ]; then
    set -a
    source "$PROJECT_ROOT/.env"
    set +a
fi

REMOTE_HOST="${REMOTE_HOST:-}"
REMOTE_DIR="${REMOTE_PROJECT_DIR:-/root/match-making}"
# When running on this host (no REMOTE_HOST), use project root
RUN_DIR="${REMOTE_HOST:+$REMOTE_DIR}"
RUN_DIR="${RUN_DIR:-$PROJECT_ROOT}"
COMPOSE_FILE="docker-compose.prod.yml"

do_stop_and_cancel() {
    local dir="$1"
    local where="${2:-on this host}"
    echo "Hard reset Dagster ($where)"
    echo "  1. Stopping dagster-daemon and dagster-code..."
    (cd "$dir" && docker compose -f "$COMPOSE_FILE" stop dagster-daemon dagster-code)
    echo "  2. Cancelling all non-terminal runs in the DB..."
    (cd "$dir" && poetry run python scripts/hard_reset_dagster.py --local)
    echo "Done. Containers are stopped. To start again:"
    echo "  cd $dir && docker compose -f $COMPOSE_FILE up -d"
}

if [ -n "$REMOTE_HOST" ]; then
    echo "Running hard reset on remote: $REMOTE_HOST (dir: $REMOTE_DIR)"
    ssh "$REMOTE_HOST" "REMOTE_DIR='$REMOTE_DIR' COMPOSE_FILE='$COMPOSE_FILE' bash -s" <<'REMOTE_SCRIPT'
set -euo pipefail
if [ -f "$REMOTE_DIR/.env" ]; then set -a; . "$REMOTE_DIR/.env"; set +a; fi
do_stop_and_cancel() {
    echo "Hard reset Dagster (on server)"
    echo "  1. Stopping dagster-daemon and dagster-code..."
    cd "$REMOTE_DIR" && docker compose -f docker-compose.prod.yml stop dagster-daemon dagster-code
    echo "  2. Cancelling all non-terminal runs in the DB..."
    cd "$REMOTE_DIR" && poetry run python scripts/hard_reset_dagster.py --local
    echo "Done. Containers are stopped. To start again:"
    echo "  cd $REMOTE_DIR && docker compose -f docker-compose.prod.yml up -d"
}
do_stop_and_cancel
REMOTE_SCRIPT
else
    do_stop_and_cancel "$RUN_DIR" "on this host"
fi
