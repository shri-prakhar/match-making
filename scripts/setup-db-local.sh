#!/usr/bin/env bash
#
# Set up the database locally: start PostgreSQL via Docker Compose and run migrations.
#
# Usage:
#   poetry run setup-db
#   # or
#   set -a && source .env && set +a && ./scripts/setup-db-local.sh
#

set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_DIR"

echo "============================================"
echo "  Local DB Setup"
echo "============================================"
echo ""

# Source .env if present (for POSTGRES_* vars)
if [ -f .env ]; then
  set -a
  source .env
  set +a
fi

# ── 1. Start PostgreSQL ─────────────────────────────────────────────────────

echo "[1/2] Starting PostgreSQL..."

docker compose up -d postgres

echo "       Waiting for PostgreSQL to become ready..."
until docker exec talent_matching_db pg_isready -U "${POSTGRES_USER:-talent}" -d "${POSTGRES_DB:-talent_matching}" > /dev/null 2>&1; do
  sleep 2
done
echo "       PostgreSQL is ready."

# ── 2. Run migrations ───────────────────────────────────────────────────────

echo "[2/2] Running Alembic migrations..."

poetry run alembic upgrade head

echo ""
echo "============================================"
echo "  Local DB setup complete!"
echo "============================================"
echo ""
echo "  Connect with: POSTGRES_HOST=localhost POSTGRES_PORT=${POSTGRES_PORT:-5432}"
echo ""
