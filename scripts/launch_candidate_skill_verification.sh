#!/bin/bash
#
# Launch skill verification for a single candidate (candidate_github_commit_history
# + candidate_skill_verification) on the REMOTE backend.
#
# Prerequisites:
#   - poetry run remote-ui is RUNNING (tunnel to Postgres + gRPC)
#   - Candidate must have normalized_candidates materialized (run full pipeline first if needed)
#   - Candidate must have github_url in raw_candidates
#
# Usage:
#   ./scripts/launch_candidate_skill_verification.sh [partition_id]
#   ./scripts/launch_candidate_skill_verification.sh rechGJvgloO4z6uYD
#
# To find a candidate with GitHub: poetry run python scripts/run_skill_verification_test.py

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PARTITION_ID="${1:-}"

if [ -f "$PROJECT_ROOT/.env" ]; then
    set -a
    source "$PROJECT_ROOT/.env"
    set +a
fi

# Force run to be written to REMOTE DB (tunnel)
export POSTGRES_HOST="${POSTGRES_HOST:-localhost}"
export POSTGRES_PORT="${POSTGRES_REMOTE_TUNNEL_PORT:-15432}"
unset DAGSTER_POSTGRES_PORT 2>/dev/null || true

# Instance config
export DAGSTER_HOME=$(mktemp -d)
ln -sf "$PROJECT_ROOT/dagster.yaml" "$DAGSTER_HOME/dagster.yaml"
trap 'rm -rf "$DAGSTER_HOME"' EXIT

if [ -z "$PARTITION_ID" ]; then
    echo "Finding candidate with GitHub URL and normalized profile..."
    PARTITION_ID=$(poetry run python -c "
import os
import sys
sys.path.insert(0, '.')
from dotenv import load_dotenv
load_dotenv()
import psycopg2
from psycopg2.extras import RealDictCursor
conn = psycopg2.connect(
    host=os.getenv('POSTGRES_HOST', 'localhost'),
    port=int(os.getenv('POSTGRES_PORT', '5432')),
    dbname=os.getenv('POSTGRES_DB', 'talent_matching'),
    user=os.getenv('POSTGRES_USER', 'postgres'),
    password=os.getenv('POSTGRES_PASSWORD', ''),
)
cur = conn.cursor(cursor_factory=RealDictCursor)
cur.execute('''
    SELECT r.airtable_record_id, r.full_name, r.github_url
    FROM raw_candidates r
    JOIN normalized_candidates n ON n.airtable_record_id = r.airtable_record_id
    WHERE r.github_url IS NOT NULL AND r.github_url != ''
    LIMIT 1
''')
row = cur.fetchone()
conn.close()
if row:
    print(row['airtable_record_id'])
    print(f\"  # {row['full_name']}: {row['github_url'][:50]}...\", file=sys.stderr)
else:
    sys.exit(1)
" 2>/dev/null) || {
    echo "No candidate with github_url found. Run sync_airtable_candidates first, or pass partition_id."
    exit 1
}
fi

echo "Launching skill verification for partition: $PARTITION_ID"
echo "  (run written to remote DB at $POSTGRES_HOST:$POSTGRES_PORT)"
echo ""

cd "$PROJECT_ROOT"
# Use -m to load definitions; workspace not supported by asset materialize
poetry run dagster asset materialize \
  -m talent_matching.definitions \
  --select "candidate_github_commit_history,candidate_skill_verification" \
  --partition "$PARTITION_ID"

echo ""
echo "Run submitted. Check status in Dagster UI (http://localhost:3000 → Runs)."
echo "Inspect results: poetry run with-remote-db python scripts/inspect_candidate.py $PARTITION_ID"
