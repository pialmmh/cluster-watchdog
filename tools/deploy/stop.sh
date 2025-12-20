#!/bin/bash
#
# Stop Cluster Watchdog
# Usage: ./tools/deploy/stop.sh
#

echo "Stopping Cluster Watchdog..."

PID=$(pgrep -f "cluster-watchdog.*quarkus-run.jar")

if [ -z "$PID" ]; then
    echo "Cluster Watchdog is not running"
    exit 0
fi

kill $PID

# Wait for graceful shutdown
for i in {1..10}; do
    if ! kill -0 $PID 2>/dev/null; then
        echo "Cluster Watchdog stopped successfully"
        exit 0
    fi
    sleep 1
done

# Force kill if still running
if kill -0 $PID 2>/dev/null; then
    echo "Force killing Cluster Watchdog..."
    kill -9 $PID
    sleep 1
fi

if ! kill -0 $PID 2>/dev/null; then
    echo "Cluster Watchdog stopped"
else
    echo "ERROR: Failed to stop Cluster Watchdog"
    exit 1
fi
