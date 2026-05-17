#!/bin/bash
set -e

# Run DataFlint Gluten/Velox Example
#
# This script:
#   1. Builds the DataFlint UI and plugin jar
#   2. Packages the Gluten example app
#   3. Downloads the Gluten nightly bundle jar (cached)
#   4. Builds and runs the Docker container
#
# Prerequisites: Node.js 20+, Java 8+, sbt, Docker
#
# Usage:
#   ./run-gluten-example.sh              # full build + run
#   ./run-gluten-example.sh --skip-build # skip sbt/npm, just rebuild Docker
#   ./run-gluten-example.sh --amd64      # force x86_64 (Rosetta 2 emulation)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
JARS_DIR="$SCRIPT_DIR/jars"
TEST_DATA_DIR="$SCRIPT_DIR/test_data"
SPARK_EVENTS_DIR="$SCRIPT_DIR/spark-events"

SPARK_VERSION="${SPARK_VERSION:-3.5.7}"
SCALA_VERSION="${SCALA_VERSION:-2.12}"

SKIP_BUILD=false
FORCE_AMD64=false

for arg in "$@"; do
  case $arg in
    --skip-build) SKIP_BUILD=true ;;
    --amd64) FORCE_AMD64=true ;;
  esac
done

# Detect architecture for Gluten jar download
ARCH=$(uname -m)
if [ "$FORCE_AMD64" = true ]; then
  GLUTEN_ARCH="linux_amd64"
  DOCKER_PLATFORM="--platform linux/amd64"
elif [ "$ARCH" = "arm64" ] || [ "$ARCH" = "aarch64" ]; then
  GLUTEN_ARCH="linux_aarch64"
  DOCKER_PLATFORM=""
else
  GLUTEN_ARCH="linux_amd64"
  DOCKER_PLATFORM=""
fi

GLUTEN_JAR_NAME="gluten-velox-bundle-spark3.5_2.12-${GLUTEN_ARCH}-1.7.0-SNAPSHOT.jar"
GLUTEN_JAR_URL="https://nightlies.apache.org/gluten/nightly-release-jdk8/${GLUTEN_JAR_NAME}"

echo "=== DataFlint Gluten/Velox Example ==="
echo "Project root:  $PROJECT_ROOT"
echo "Spark version: $SPARK_VERSION"
echo "Architecture:  $GLUTEN_ARCH"
echo "Gluten jar:    $GLUTEN_JAR_NAME"
echo ""

mkdir -p "$JARS_DIR"
mkdir -p "$SPARK_EVENTS_DIR"

# --- Step 1: Download Gluten nightly jar (cached) ---
echo "=== Step 1: Downloading Gluten nightly jar ==="
if [ -f "$JARS_DIR/$GLUTEN_JAR_NAME" ]; then
  echo "Gluten jar already cached: $JARS_DIR/$GLUTEN_JAR_NAME"
else
  echo "Downloading: $GLUTEN_JAR_URL"
  curl -fSL -o "$JARS_DIR/$GLUTEN_JAR_NAME" "$GLUTEN_JAR_URL"
  echo "Downloaded successfully."
fi

if [ "$SKIP_BUILD" = false ]; then
  # --- Step 2: Build DataFlint UI ---
  echo ""
  echo "=== Step 2: Building DataFlint UI ==="
  cd "$PROJECT_ROOT/spark-ui"
  if [ ! -d "node_modules" ]; then
    echo "Installing npm dependencies..."
    npm ci
  fi
  echo "Building and deploying UI into plugin resources..."
  npm run deploy

  # --- Step 3: Build DataFlint plugin jar ---
  echo ""
  echo "=== Step 3: Building DataFlint plugin jar ==="
  cd "$PROJECT_ROOT/spark-plugin"
  export SBT_OPTS="-Xmx4G -Xss2M -XX:+UseG1GC"
  sbt "pluginspark3/assembly"

  # --- Step 4: Package example jar ---
  echo ""
  echo "=== Step 4: Packaging example jar ==="
  sbt "example_3_5_1/package"
fi

# --- Step 5: Copy jars to docker context ---
echo ""
echo "=== Step 5: Copying jars to Docker context ==="

# DataFlint plugin jar
PLUGIN_JAR=$(find "$PROJECT_ROOT/spark-plugin/pluginspark3/target/scala-${SCALA_VERSION}" -name "spark_${SCALA_VERSION}-*.jar" -type f | head -1)
if [ -z "$PLUGIN_JAR" ]; then
  echo "ERROR: DataFlint plugin jar not found. Run without --skip-build first."
  exit 1
fi
cp "$PLUGIN_JAR" "$JARS_DIR/dataflint-plugin.jar"
echo "Copied DataFlint plugin: $(basename "$PLUGIN_JAR")"

# Example jar
EXAMPLE_JAR=$(find "$PROJECT_ROOT/spark-plugin/example_3_5_1/target/scala-${SCALA_VERSION}" -name "DataflintSparkExample351_${SCALA_VERSION}-*.jar" -type f | head -1)
if [ -z "$EXAMPLE_JAR" ]; then
  echo "ERROR: Example jar not found. Run without --skip-build first."
  exit 1
fi
cp "$EXAMPLE_JAR" "$JARS_DIR/example.jar"
echo "Copied example jar: $(basename "$EXAMPLE_JAR")"

echo "Gluten jar: $GLUTEN_JAR_NAME"

# --- Step 6: Copy test data ---
echo ""
echo "=== Step 6: Copying test data ==="
rm -rf "$TEST_DATA_DIR"
cp -r "$PROJECT_ROOT/spark-plugin/test_data" "$TEST_DATA_DIR"
echo "Copied test_data/"

# --- Step 7: Build and run Docker ---
echo ""
echo "=== Step 7: Building and running Docker container ==="
cd "$SCRIPT_DIR"

# Stop any previous container
docker compose down 2>/dev/null || true

# Build with platform flag if needed
if [ -n "$DOCKER_PLATFORM" ]; then
  DOCKER_DEFAULT_PLATFORM=linux/amd64 docker compose up --build
else
  docker compose up --build
fi
