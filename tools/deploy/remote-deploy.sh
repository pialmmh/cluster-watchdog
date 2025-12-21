#!/bin/bash
#
# Deploy Cluster Watchdog to remote server
# Usage: ./tools/deploy/remote-deploy.sh <tenant> <node>
# Example: ./tools/deploy/remote-deploy.sh link3 node1
#

set -e

TENANT="${1:-}"
NODE="${2:-}"

if [ -z "$TENANT" ] || [ -z "$NODE" ]; then
    echo "Usage: $0 <tenant> <node>"
    echo ""
    echo "Examples:"
    echo "  $0 link3 node1    # Deploy to Link3 Node 1"
    echo "  $0 link3 node2    # Deploy to Link3 Node 2"
    echo "  $0 link3 node3    # Deploy to Link3 Node 3"
    echo ""
    echo "Note: Configure server details in tools/deploy/tenant-conf/<tenant>/deploy.conf"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
DEPLOY_CONF="$SCRIPT_DIR/tenant-conf/${TENANT}/deploy.conf"
JAR_DIR="$BASE_DIR/target/quarkus-app"
RESOURCES_DIR="$BASE_DIR/src/main/resources"

# Check if deploy.conf exists
if [ ! -f "$DEPLOY_CONF" ]; then
    echo "ERROR: Deploy config not found: $DEPLOY_CONF"
    exit 1
fi

# Check if JAR exists
if [ ! -f "$JAR_DIR/quarkus-run.jar" ]; then
    echo "ERROR: JAR not found. Run: ./tools/deploy/build.sh"
    exit 1
fi

# Parse INI-style config file for a specific section
parse_section() {
    local file="$1"
    local section="$2"
    local key="$3"

    awk -F '=' -v section="[$section]" -v key="$key" '
        $0 == section { in_section=1; next }
        /^\[/ { in_section=0 }
        in_section && $1 ~ "^"key"[[:space:]]*$" {
            gsub(/^[[:space:]]+|[[:space:]]+$/, "", $2)
            print $2
            exit
        }
    ' "$file"
}

# Get node configuration from deploy.conf
HOST=$(parse_section "$DEPLOY_CONF" "$NODE" "host")
PORT=$(parse_section "$DEPLOY_CONF" "$NODE" "port")
USER=$(parse_section "$DEPLOY_CONF" "$NODE" "user")
NODE_ID=$(parse_section "$DEPLOY_CONF" "$NODE" "node_id")
OVERLAY_IP=$(parse_section "$DEPLOY_CONF" "$NODE" "overlay_ip")
WG_INTERFACE=$(parse_section "$DEPLOY_CONF" "$NODE" "wg_interface")
DESCRIPTION=$(parse_section "$DEPLOY_CONF" "$NODE" "description")

# Default WireGuard interface if not specified
[ -z "$WG_INTERFACE" ] && WG_INTERFACE="wg0"

if [ -z "$HOST" ]; then
    echo "ERROR: Node '$NODE' not found in $DEPLOY_CONF"
    echo ""
    echo "Available nodes:"
    grep '^\[' "$DEPLOY_CONF" | tr -d '[]'
    exit 1
fi

[ -z "$PORT" ] && PORT="22"
[ -z "$USER" ] && USER="root"

SERVER="$USER@$HOST"

echo ""
echo "========================================"
echo "  Deploying Cluster Watchdog"
echo "========================================"
echo ""
echo "Tenant:       $TENANT"
echo "Node:         $NODE (ID: $NODE_ID)"
echo "Server:       $SERVER:$PORT"
echo "Overlay IP:   $OVERLAY_IP"
echo "WG Interface: $WG_INTERFACE"
echo "Desc:         $DESCRIPTION"
echo ""

# Create temporary deployment directory
DEPLOY_TMP=$(mktemp -d)
trap "rm -rf $DEPLOY_TMP" EXIT

echo "Step 1/5: Preparing deployment package..."

# Copy JAR files
cp -r "$JAR_DIR"/* "$DEPLOY_TMP/"

# Copy only the active tenant properties and modify application.properties
cp "$RESOURCES_DIR/application.properties" "$DEPLOY_TMP/app/application.properties"
cp "$RESOURCES_DIR/application-${TENANT}.properties" "$DEPLOY_TMP/app/application-${TENANT}.properties"

# Set active profile in application.properties
sed -i "s/^quarkus.profile=.*/quarkus.profile=${TENANT}/" "$DEPLOY_TMP/app/application.properties"

echo "  Prepared with profile: $TENANT"

# Create systemd service file with node-specific environment
cat > "$DEPLOY_TMP/cluster-watchdog.service" << EOF
[Unit]
Description=Cluster Watchdog Agent - $TENANT $NODE
After=network.target docker.service

[Service]
Type=simple
User=root
WorkingDirectory=/opt/cluster-watchdog
Environment=JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
Environment=WATCHDOG_NODE_ID=$NODE_ID
Environment=WATCHDOG_OVERLAY_IP=$OVERLAY_IP
Environment=WATCHDOG_WG_INTERFACE=$WG_INTERFACE
ExecStart=/usr/lib/jvm/java-21-openjdk-amd64/bin/java -jar /opt/cluster-watchdog/quarkus-run.jar
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

echo "  Created systemd service for node $NODE_ID"

# Step 2: Stop remote service
echo "Step 2/5: Stopping remote service..."
ssh -p "$PORT" "$SERVER" "sudo systemctl stop cluster-watchdog 2>/dev/null || true"
echo "  Stopped"

# Step 3: Copy files
echo "Step 3/5: Copying files to server..."
ssh -p "$PORT" "$SERVER" "sudo mkdir -p /opt/cluster-watchdog && sudo chown $USER /opt/cluster-watchdog"
scp -P "$PORT" -r "$DEPLOY_TMP"/* "$SERVER:/opt/cluster-watchdog/"
echo "  Files copied"

# Step 4: Install service
echo "Step 4/5: Installing systemd service..."
ssh -p "$PORT" "$SERVER" "sudo mv /opt/cluster-watchdog/cluster-watchdog.service /etc/systemd/system/ && sudo systemctl daemon-reload && sudo systemctl enable cluster-watchdog"
echo "  Service installed"

# Step 5: Start service
echo "Step 5/5: Starting service..."
ssh -p "$PORT" "$SERVER" "sudo systemctl start cluster-watchdog"
sleep 3
ssh -p "$PORT" "$SERVER" "sudo systemctl status cluster-watchdog --no-pager" || true

echo ""
echo "========================================"
echo "  Deployment Completed"
echo "========================================"
echo ""
echo "API endpoints:"
echo "  http://$HOST:8090/api/status"
echo "  http://$HOST:8090/api/network/connectivity"
echo "  http://$HOST:8090/api/kafka"
echo ""
echo "Useful commands:"
echo "  ssh -p $PORT $SERVER"
echo "  sudo systemctl status cluster-watchdog"
echo "  sudo journalctl -u cluster-watchdog -f"
echo ""
