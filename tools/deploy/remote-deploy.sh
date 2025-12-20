#!/bin/bash
#
# Deploy Cluster Watchdog to remote server
# Usage: ./tools/deploy/remote-deploy.sh <tenant>
# Example: ./tools/deploy/remote-deploy.sh link3
#

set -e

TENANT="${1:-}"

if [ -z "$TENANT" ]; then
    echo "Usage: $0 <tenant>"
    echo ""
    echo "Available tenants: link3, bdcom"
    echo ""
    echo "Examples:"
    echo "  $0 link3    # Deploy to Link3 servers"
    echo "  $0 bdcom    # Deploy to BDCOM servers"
    echo ""
    echo "Note: Configure server details in tools/deploy/tenant-conf/<tenant>.conf"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
CONFIG_FILE="$SCRIPT_DIR/tenant-conf/${TENANT}.conf"
TENANT_APP_CONFIG="$SCRIPT_DIR/tenant-conf/${TENANT}/application.yml"
JAR_DIR="$BASE_DIR/target/quarkus-app"

# Check if config file exists
if [ ! -f "$CONFIG_FILE" ]; then
    echo "❌ ERROR: Config file not found: $CONFIG_FILE"
    exit 1
fi

# Check if tenant application.yml exists
if [ ! -f "$TENANT_APP_CONFIG" ]; then
    echo "❌ ERROR: Tenant application.yml not found: $TENANT_APP_CONFIG"
    exit 1
fi

# Check if JAR exists
if [ ! -f "$JAR_DIR/quarkus-run.jar" ]; then
    echo "❌ ERROR: JAR not found. Run: ./tools/deploy/build.sh"
    exit 1
fi

# Parse config file
parse_config() {
    local file="$1"
    local key="$2"
    grep "^${key}=" "$file" | cut -d'=' -f2 | tr -d ' '
}

HOST=$(parse_config "$CONFIG_FILE" "host")
PORT=$(parse_config "$CONFIG_FILE" "port")
USER=$(parse_config "$CONFIG_FILE" "user")

if [ -z "$HOST" ]; then
    echo "❌ ERROR: No host found in $CONFIG_FILE"
    exit 1
fi

[ -z "$PORT" ] && PORT="22"
[ -z "$USER" ] && USER="root"

SERVER="$USER@$HOST"

echo "╔════════════════════════════════════════════════════════════╗"
echo "║        Deploying Cluster Watchdog to Remote Server         ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""
echo "Tenant:  $TENANT"
echo "Server:  $SERVER:$PORT"
echo ""

# Step 1: Stop service
echo "Step 1/4: Stopping remote service..."
ssh -p "$PORT" "$SERVER" "sudo systemctl stop cluster-watchdog 2>/dev/null || true"
echo "  ✓ Service stopped"

# Step 2: Copy files
echo "Step 2/4: Copying files..."
ssh -p "$PORT" "$SERVER" "sudo mkdir -p /opt/cluster-watchdog && sudo chown $USER /opt/cluster-watchdog"
scp -P "$PORT" -r "$JAR_DIR"/* "$SERVER:/opt/cluster-watchdog/"
scp -P "$PORT" "$TENANT_APP_CONFIG" "$SERVER:/opt/cluster-watchdog/application.yml"
scp -P "$PORT" "$SCRIPT_DIR/cluster-watchdog.service" "$SERVER:/tmp/"
echo "  ✓ Files copied"

# Step 3: Install service
echo "Step 3/4: Installing service..."
ssh -p "$PORT" "$SERVER" "sudo mv /tmp/cluster-watchdog.service /etc/systemd/system/ && sudo systemctl daemon-reload && sudo systemctl enable cluster-watchdog"
echo "  ✓ Service installed"

# Step 4: Start service
echo "Step 4/4: Starting service..."
ssh -p "$PORT" "$SERVER" "sudo systemctl start cluster-watchdog"
sleep 2
ssh -p "$PORT" "$SERVER" "sudo systemctl status cluster-watchdog --no-pager" || true

echo ""
echo "╔════════════════════════════════════════════════════════════╗"
echo "║              Deployment Completed Successfully             ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""
echo "Useful commands:"
echo "  ssh -p $PORT $SERVER"
echo "  sudo systemctl status cluster-watchdog"
echo "  sudo journalctl -u cluster-watchdog -f"
