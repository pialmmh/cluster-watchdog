#!/bin/bash
#
# Start Cluster Watchdog locally
# Usage: ./tools/deploy/start.sh <tenant>
# Example: ./tools/deploy/start.sh link3
#

TENANT="${1:-}"

if [ -z "$TENANT" ]; then
    echo "Usage: $0 <tenant>"
    echo ""
    echo "Available tenants: link3, bdcom"
    echo ""
    echo "Examples:"
    echo "  $0 link3    # Start with Link3 config"
    echo "  $0 bdcom    # Start with BDCOM config"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
JAR_FILE="$BASE_DIR/target/quarkus-app/quarkus-run.jar"
LOG_FILE="/tmp/cluster-watchdog.log"
TENANT_CONFIG="$SCRIPT_DIR/tenant-conf/${TENANT}/application.yml"

# Check if JAR exists
if [ ! -f "$JAR_FILE" ]; then
    echo "ERROR: JAR not found: $JAR_FILE"
    echo "Run: ./tools/deploy/build.sh"
    exit 1
fi

# Check if tenant config exists
if [ ! -f "$TENANT_CONFIG" ]; then
    echo "ERROR: Tenant config not found: $TENANT_CONFIG"
    exit 1
fi

# Check if already running
if pgrep -f "cluster-watchdog.*quarkus-run.jar" > /dev/null; then
    echo "ERROR: Cluster Watchdog is already running"
    echo "Stop it first: ./tools/deploy/stop.sh"
    exit 1
fi

# Ensure Java 21
if [ -d "/usr/lib/jvm/java-21-openjdk-amd64" ]; then
    export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
    export PATH=$JAVA_HOME/bin:$PATH
fi

echo "╔════════════════════════════════════════════════════════════╗"
echo "║              Starting Cluster Watchdog                     ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""
echo "Tenant: $TENANT"
echo "Config: $TENANT_CONFIG"
echo "Log:    $LOG_FILE"
echo ""

# Start the application
nohup java \
    -Dquarkus.config.locations="$TENANT_CONFIG" \
    -jar "$JAR_FILE" \
    > "$LOG_FILE" 2>&1 &

PID=$!
echo "PID: $PID"
echo ""

# Wait for startup
echo "Waiting for startup..."
for i in {1..30}; do
    if grep -q "started in" "$LOG_FILE" 2>/dev/null || grep -q "Listening on" "$LOG_FILE" 2>/dev/null; then
        echo ""
        echo "╔════════════════════════════════════════════════════════════╗"
        echo "║          Cluster Watchdog Started Successfully             ║"
        echo "╚════════════════════════════════════════════════════════════╝"
        echo ""
        echo "API:          http://localhost:8090/api/status"
        echo "Health:       http://localhost:8090/q/health"
        echo ""
        echo "View logs: tail -f $LOG_FILE"
        exit 0
    fi

    # Check if process died
    if ! kill -0 $PID 2>/dev/null; then
        echo ""
        echo "ERROR: Application failed to start"
        echo "Check logs: tail -50 $LOG_FILE"
        exit 1
    fi

    sleep 0.5
done

echo ""
echo "WARNING: Startup taking longer than expected"
echo "Check logs: tail -f $LOG_FILE"
