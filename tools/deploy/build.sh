#!/bin/bash
#
# Build Cluster Watchdog Uber JAR
# Usage: ./tools/deploy/build.sh
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

echo "╔════════════════════════════════════════════════════════════╗"
echo "║           Building Cluster Watchdog JAR                    ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""

cd "$BASE_DIR"

# Ensure Java 21
if [ -d "/usr/lib/jvm/java-21-openjdk-amd64" ]; then
    export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
    export PATH=$JAVA_HOME/bin:$PATH
fi

echo "Java version:"
java -version
echo ""

echo "Building uber JAR..."
mvn clean package -DskipTests

JAR_FILE="$BASE_DIR/target/quarkus-app/quarkus-run.jar"

if [ -f "$JAR_FILE" ]; then
    echo ""
    echo "╔════════════════════════════════════════════════════════════╗"
    echo "║                  BUILD SUCCESSFUL                          ║"
    echo "╚════════════════════════════════════════════════════════════╝"
    echo ""
    echo "JAR: $JAR_FILE"
    echo ""
    echo "Next steps:"
    echo "  ./tools/deploy/start.sh link3           # Start locally"
    echo "  ./tools/deploy/remote-deploy.sh link3   # Deploy to remote"
else
    echo "ERROR: Build failed - JAR file not found"
    exit 1
fi
