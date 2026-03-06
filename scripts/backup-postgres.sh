#!/bin/bash
# Automated PostgreSQL backup for the talent_matching database.
# Intended to run as a cron job on the Docker host.
#
# Install (on the remote server):
#   mkdir -p /data/backups
#   cp scripts/backup-postgres.sh /usr/local/bin/backup-postgres.sh
#   chmod +x /usr/local/bin/backup-postgres.sh
#   echo "0 */6 * * * /usr/local/bin/backup-postgres.sh" | crontab -
#
# This keeps the last 7 days of backups (28 files at 6-hour intervals).

set -euo pipefail

BACKUP_DIR="/data/backups"
CONTAINER="talent_matching_db"
DB_USER="${POSTGRES_USER:-talent}"
DB_NAME="${POSTGRES_DB:-talent_matching}"
RETENTION_DAYS=7

mkdir -p "$BACKUP_DIR"

TIMESTAMP=$(date +%Y%m%d_%H%M)
BACKUP_FILE="${BACKUP_DIR}/dagster_${TIMESTAMP}.sql.gz"

docker exec "$CONTAINER" pg_dump -U "$DB_USER" "$DB_NAME" | gzip > "$BACKUP_FILE"

# Prune backups older than retention period
find "$BACKUP_DIR" -name "dagster_*.sql.gz" -mtime +"$RETENTION_DAYS" -delete

echo "Backup complete: ${BACKUP_FILE} ($(du -h "$BACKUP_FILE" | cut -f1))"
