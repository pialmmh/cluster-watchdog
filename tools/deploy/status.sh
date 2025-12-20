#!/bin/bash
#
# Check Cluster Watchdog status
# Usage: ./tools/deploy/status.sh
#

PID=$(pgrep -f "cluster-watchdog.*quarkus-run.jar")

if [ -z "$PID" ]; then
    echo "Status: NOT RUNNING"
    exit 1
fi

echo "╔════════════════════════════════════════════════════════════╗"
echo "║              Cluster Watchdog Status                       ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""
echo "Status: RUNNING"
echo "PID:    $PID"
echo ""

# Check ports
echo "Listening Ports:"
ss -lntp 2>/dev/null | grep "$PID" | awk '{print $4}' | sed 's/.*:/  - Port /' || echo "  (unable to determine)"

echo ""

# Health check
echo "Health Check:"
HEALTH=$(curl -s http://localhost:8090/q/health 2>/dev/null)
if [ $? -eq 0 ]; then
    echo "  ✓ API responding"
    echo "$HEALTH" | head -5
else
    echo "  ✗ API not responding"
fi

echo ""
echo "Logs: tail -f /tmp/cluster-watchdog.log"
