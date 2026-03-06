#!/bin/bash
set -e

# Only run migrations from the code server (dagster api grpc).
# The daemon starts after the code server is healthy, so migrations
# are guaranteed to be applied before it needs them.
if [[ "$1" == "dagster" && "$2" == "api" ]]; then
    echo "Running database migrations..."
    alembic upgrade head
else
    echo "Skipping migrations (not the code server)."
fi

echo "Starting service..."
exec "$@"
