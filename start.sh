#!/bin/bash
set -e

echo "Starting SMS Sync Cron Service..."
echo "Cron jobs configured:"
cat /etc/cron.d/sync-cron

# Start cron in foreground
exec cron -f