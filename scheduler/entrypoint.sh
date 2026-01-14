#!/bin/bash
set -e

echo "=========================================="
echo "QuantOps Scheduler Starting"
echo "Timezone: $(cat /etc/timezone)"
echo "Current time: $(date)"
echo "=========================================="

# Pass environment variables to cron
printenv | grep -v "no_proxy" >> /etc/environment

# Start cron daemon in foreground
echo "Starting cron daemon..."
cron -f &

# Follow the cron log
tail -f /var/log/cron.log
