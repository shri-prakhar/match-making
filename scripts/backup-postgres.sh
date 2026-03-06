#!/bin/bash
# Automated PostgreSQL backup and event log pruning for talent_matching.
# Intended to run as a cron job on the Docker host.
#
# Install (on the remote server):
#   mkdir -p /data/backups
#   cp scripts/backup-postgres.sh /usr/local/bin/backup-postgres.sh
#   chmod +x /usr/local/bin/backup-postgres.sh
#   echo "0 */6 * * * /usr/local/bin/backup-postgres.sh >> /data/backups/cron.log 2>&1" | crontab -
#
# This keeps the last 7 days of backups (28 files at 6-hour intervals).

set -euo pipefail

BACKUP_DIR="/data/backups"
CONTAINER="talent_matching_db"
DB_USER="${POSTGRES_USER:-talent}"
DB_NAME="${POSTGRES_DB:-talent_matching}"
RETENTION_DAYS=7
LOG_RETENTION_DAYS=14

mkdir -p "$BACKUP_DIR"

echo "$(date '+%Y-%m-%d %H:%M:%S') Starting backup and pruning..."

# --- Prune old Dagster event logs (older than 14 days) ---

# Delete user-level logs (DEBUG, INFO, WARNING)
docker exec "$CONTAINER" psql -U "$DB_USER" "$DB_NAME" -c "
DELETE FROM event_logs
WHERE dagster_event_type IS NULL
  AND timestamp < CURRENT_DATE - INTERVAL '${LOG_RETENTION_DAYS} days';
"

# Delete unimportant system events (keeps ASSET_MATERIALIZATION, ASSET_OBSERVATION, LOGS_CAPTURED)
docker exec "$CONTAINER" psql -U "$DB_USER" "$DB_NAME" -c "
DELETE FROM event_logs
WHERE timestamp < CURRENT_DATE - INTERVAL '${LOG_RETENTION_DAYS} days'
  AND dagster_event_type IN (
    'ASSET_MATERIALIZATION_PLANNED',
    'ENGINE_EVENT',
    'HANDLED_OUTPUT',
    'LOADED_INPUT',
    'PIPELINE_CANCELING',
    'PIPELINE_ENQUEUED',
    'PIPELINE_STARTING',
    'PIPELINE_START',
    'PIPELINE_SUCCESS',
    'PIPELINE_FAILURE',
    'PIPELINE_CANCELED',
    'RESOURCE_INIT_FAILURE',
    'RESOURCE_INIT_STARTED',
    'RESOURCE_INIT_SUCCESS',
    'STEP_INPUT',
    'STEP_OUTPUT',
    'STEP_START',
    'STEP_SUCCESS',
    'STEP_FAILURE',
    'STEP_WORKER_STARTED',
    'STEP_WORKER_STARTING',
    'STEP_UP_FOR_RETRY',
    'STEP_RESTARTED',
    'STEP_SKIPPED',
    'OBJECT_STORE_OPERATION',
    'RUN_CANCELING',
    'RUN_DEQUEUED',
    'RUN_ENQUEUED',
    'RUN_STARTING',
    'RUN_START',
    'RUN_CANCELED',
    'RUN_SUCCESS',
    'RUN_FAILURE'
  );
"

# VACUUM to reclaim space from deleted rows
docker exec "$CONTAINER" psql -U "$DB_USER" "$DB_NAME" -c "VACUUM ANALYZE event_logs;"

# --- Backup ---

TIMESTAMP=$(date +%Y%m%d_%H%M)
BACKUP_FILE="${BACKUP_DIR}/dagster_${TIMESTAMP}.sql.gz"

docker exec "$CONTAINER" pg_dump -U "$DB_USER" "$DB_NAME" | gzip > "$BACKUP_FILE"

# Prune old backups
find "$BACKUP_DIR" -name "dagster_*.sql.gz" -mtime +"$RETENTION_DAYS" -delete

echo "$(date '+%Y-%m-%d %H:%M:%S') Done: ${BACKUP_FILE} ($(du -h "$BACKUP_FILE" | cut -f1))"
