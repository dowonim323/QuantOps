#!/bin/bash
set -e

echo "=========================================="
echo "QuantOps Scheduler Starting"
echo "Timezone: $(cat /etc/timezone)"
echo "Current time: $(date)"
echo "=========================================="

printenv | grep -v "no_proxy" >> /etc/environment

if [ -n "$DISCORD_WEBHOOK_TRADE" ]; then
    curl -s -X POST "$DISCORD_WEBHOOK_TRADE" \
        -H "Content-Type: application/json" \
        -d "{\"content\": \":rocket: **QuantOps Scheduler Started**\n\`\`\`Timezone: $(cat /etc/timezone)\nTime: $(date)\nHostname: $(hostname)\`\`\`\"}" \
        || echo "Failed to send Discord notification"
fi

echo "Starting cron daemon..."
cron -f &

tail -f /var/log/cron.log
