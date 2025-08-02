#!/bin/bash
set -e

echo "Starting SMS Sync Cron Service..."
echo "Cron jobs configured:"
cat /etc/cron.d/sync-cron

# Start health check server in background
python3 /app/health.py &

# Start cron in foreground
exec cron -f