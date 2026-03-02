#!/bin/bash
#
# Deploy latest code to the remote server and run the ATS matchmaking pipeline
# for a given job partition. Then print how to check results later.
#
# Prerequisites:
#   - REMOTE_HOST in .env (or pass as first arg)
#   - SSH access to the remote server
#   - Remote server has the project cloned and Docker set up (deploy/setup-remote.sh)
#
# Usage:
#   ./scripts/run_remote_matchmaking.sh [job_partition_id]
#   ./scripts/run_remote_matchmaking.sh recIqBsuF33YrIrMX
#
# If no partition id is given, uses recIqBsuF33YrIrMX (Growth Analyst job).
#
# After the script:
#   1. Open the Dagster UI (poetry run remote-ui) and launch the job for the partition.
#   2. Later, run inspect_matches to verify results (see output below).

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
FORCE_DEPLOY=false
if [ "${1:-}" = "--force" ]; then
    FORCE_DEPLOY=true
    shift
fi
PARTITION_ID="${1:-recIqBsuF33YrIrMX}"

if [ -f "$PROJECT_ROOT/.env" ]; then
    set -a
    source "$PROJECT_ROOT/.env"
    set +a
fi

REMOTE_HOST="${REMOTE_HOST:-}"
# Where the project lives on the remote (see deploy/setup-remote.sh). Override with REMOTE_PROJECT_DIR in .env.
REMOTE_DIR="${REMOTE_PROJECT_DIR:-/root/match-making}"

if [ -z "$REMOTE_HOST" ]; then
    echo "Error: REMOTE_HOST not set. Set it in .env or pass the partition id as first argument."
    echo "Usage: $0 [job_partition_id]"
    echo "       job_partition_id = Airtable record ID of the job (e.g. recIqBsuF33YrIrMX)"
    exit 1
fi

echo "============================================"
echo "  Remote Matchmaking: deploy and run"
echo "============================================"
echo ""
echo "  Remote:       $REMOTE_HOST"
echo "  Remote dir:   $REMOTE_DIR"
echo "  Job partition: $PARTITION_ID"
echo ""

# ── Step 1: Deploy latest on remote ───────────────────────────────────────────

echo "[1/2] Deploying latest code on remote (git pull + rebuild)..."
if $FORCE_DEPLOY; then
    ssh "$REMOTE_HOST" "cd $REMOTE_DIR && git fetch origin && git reset --hard origin/main && docker compose -f docker-compose.prod.yml up --build -d"
else
    ssh "$REMOTE_HOST" "cd $REMOTE_DIR && git pull && docker compose -f docker-compose.prod.yml up --build -d" || {
        echo "      git pull failed (e.g. untracked files?). Re-run with --force to overwrite: $0 --force $PARTITION_ID"
        exit 1
    }
fi
echo "      Done."
echo ""

# ── Step 2: Instructions ──────────────────────────────────────────────────────

echo "[2/2] Trigger the matchmaking run from the Dagster UI"
echo ""
echo "  1. Start the remote UI (if not already running):"
echo "       poetry run remote-ui"
echo ""
echo "  2. In the browser (http://localhost:3000):"
echo "     - Go to Jobs → ats_matchmaking_pipeline"
echo "     - Click \"Launch run\" or \"Backfill\""
echo "     - Select partition: $PARTITION_ID"
echo "     - Launch the run"
echo ""
echo "  3. If partition $PARTITION_ID is not in the list:"
echo "     - Go to Assets → select normalized_jobs (or any job asset)"
echo "     - Or add the partition: Dynamic Partitions → jobs → Add partition key: $PARTITION_ID"
echo "     - Then launch the run for that partition"
echo ""
echo "============================================"
echo "  Check results later"
echo "============================================"
echo ""
echo "  With the remote UI running (so DB is tunneled to localhost:15432), run:"
echo ""
echo "      poetry run python scripts/inspect_matches.py $PARTITION_ID"
echo ""
echo "  That prints the stored matches for this job (same format as the pipeline output)."
echo ""
echo "  Expected for Growth Analyst: skills like Marketing Strategy, Data Analysis,"
echo "  Client Relationship Management, Market Research, Communication — not Java/Spring/SQL."
echo ""
