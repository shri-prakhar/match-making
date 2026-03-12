#!/bin/bash
# Run ground-truth backfill on the remote server (fast: no tunnel latency).
#
# Prerequisites:
#   - REMOTE_HOST in .env
#   - Deploy first so the image has scripts/ and latest code
#
# Usage:
#   ./scripts/run_backfill_on_remote.sh

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

echo "Running ground-truth backfill on remote server..."
ssh "$REMOTE_HOST" "cd $REMOTE_DIR && docker compose -f docker-compose.prod.yml run --rm -e POSTGRES_HOST=postgres dagster-code python scripts/backfill_ground_truth.py"
echo "Done."
