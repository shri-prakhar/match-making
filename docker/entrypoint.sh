#!/bin/bash
set -e

if [[ "$1" == "dagster" && "$2" == "api" ]]; then
    echo "Running database migrations..."
    alembic upgrade head

elif [[ "$1" == "dagster-daemon" ]]; then
    echo "Skipping migrations (daemon)."
    # DefaultRunLauncher runs jobs as subprocesses of dagster-code.
    # After a container rebuild those processes are dead, but run_monitoring
    # cannot detect this (no PID in the new container). Mark any STARTED
    # runs as FAILURE so they don't block the queue forever.
    echo "Cleaning up orphaned runs from previous container..."
    python -c "
import os, psycopg2
conn = psycopg2.connect(
    host=os.environ['POSTGRES_HOST'],
    port=os.environ.get('POSTGRES_PORT', '5432'),
    user=os.environ['POSTGRES_USER'],
    password=os.environ['POSTGRES_PASSWORD'],
    dbname=os.environ['POSTGRES_DB'],
)
conn.autocommit = True
cur = conn.cursor()
cur.execute(\"\"\"
    UPDATE runs SET status = 'FAILURE', update_timestamp = NOW()
    WHERE status IN ('STARTED', 'STARTING')
      AND update_timestamp < NOW() - INTERVAL '2 minutes'
\"\"\")
print(f'  Marked {cur.rowcount} orphaned run(s) as FAILURE.')
cur.close()
conn.close()
" 2>&1 || echo "  (cleanup skipped -- DB not ready or no orphans)"

else
    echo "Skipping migrations (not the code server)."
fi

echo "Starting service..."
exec "$@"
