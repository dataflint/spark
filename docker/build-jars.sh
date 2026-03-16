#!/bin/bash
set -e

# Build DataFlint JARs for Docker
# This script builds the UI and plugin JARs locally

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
OUTPUT_DIR="$SCRIPT_DIR/jars"

echo "=== Building DataFlint JARs ==="
echo "Project root: $PROJECT_ROOT"
echo "Output directory: $OUTPUT_DIR"

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Step 1: Build the UI
echo ""
echo "=== Step 1: Building UI ==="
cd "$PROJECT_ROOT/spark-ui"

if [ ! -d "node_modules" ]; then
    echo "Installing npm dependencies..."
    npm ci
fi

echo "Building and deploying UI..."
npm run deploy

# Step 2: Build the plugin JARs
echo ""
echo "=== Step 2: Building Plugin JARs ==="
cd "$PROJECT_ROOT/spark-plugin"

# Set JVM memory for sbt
export SBT_OPTS="-Xmx4G -Xss2M -XX:+UseG1GC"

echo "Building Spark 3.x plugin JARs..."
sbt "+pluginspark3/assembly"

echo "Building Spark 4.x plugin JAR..."
sbt "pluginspark4/assembly"

# Step 3: Copy JARs to output directory
echo ""
echo "=== Step 3: Copying JARs ==="

# Spark 3.x JARs
cp pluginspark3/target/scala-2.12/spark_2.12-*.jar "$OUTPUT_DIR/" 2>/dev/null || echo "No Scala 2.12 JAR found (optional)"
cp pluginspark3/target/scala-2.13/spark_2.13-*.jar "$OUTPUT_DIR/" 2>/dev/null || echo "No Scala 2.13 JAR found (optional)"

# Spark 4.x JAR
cp pluginspark4/target/scala-2.13/dataflint-spark4_2.13-*.jar "$OUTPUT_DIR/" 2>/dev/null || echo "No Spark 4.x JAR found (optional)"

echo ""
echo "=== Build Complete ==="
echo "JARs available in: $OUTPUT_DIR"
ls -la "$OUTPUT_DIR"

echo ""
echo "To build Docker image with local JARs:"
echo "  docker build -t dataflint-history-server --build-arg USE_LOCAL_JAR=true ."