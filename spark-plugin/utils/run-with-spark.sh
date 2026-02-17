#!/usr/bin/env bash
#
# Run a PySpark script using a specific Spark version from the local .spark-versions/ cache.
#
# Usage:
#   ./run-with-spark.sh <spark-version> <script.py> [extra spark-submit args...]
#
# Examples:
#   ./run-with-spark.sh 3.5.8 ../example_3_5_1/map_in_pandas_example.py
#   ./run-with-spark.sh 3.1.3 ../example_3_5_1/map_in_pandas_example.py
#   ./run-with-spark.sh 3.3.4 ../example_3_5_1/map_in_arrow_example.py
#
# The script will:
#   1. Locate the Spark installation in .spark-versions/<version>/
#   2. Auto-download the version if not present
#   3. Set SPARK_HOME and use that version's spark-submit to run the script
#
# Note: mapInArrow requires Spark 3.3+ (PySpark API introduced via SPARK-37227)
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SPARK_VERSIONS_DIR="${SCRIPT_DIR}/.spark-versions"

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <spark-version> <script.py> [extra spark-submit args...]"
  echo ""
  echo "Available downloaded versions:"
  if [[ -d "${SPARK_VERSIONS_DIR}" ]]; then
    for d in "${SPARK_VERSIONS_DIR}"/*/; do
      if [[ -f "${d}bin/spark-submit" ]]; then
        version="$(basename "$d")"
        echo "  ${version}"
      fi
    done
  else
    echo "  (none — run download-spark-versions.sh first)"
  fi
  exit 1
fi

SPARK_VERSION="$1"
PYSPARK_SCRIPT="$2"
shift 2

SPARK_HOME="${SPARK_VERSIONS_DIR}/${SPARK_VERSION}"

# Auto-download if not present
if [[ ! -f "${SPARK_HOME}/bin/spark-submit" ]]; then
  echo "Spark ${SPARK_VERSION} not found locally. Attempting to download..."
  "${SCRIPT_DIR}/download-spark-versions.sh" "${SPARK_VERSION}"
  echo ""
fi

if [[ ! -f "${SPARK_HOME}/bin/spark-submit" ]]; then
  echo "Error: Spark ${SPARK_VERSION} installation not found at ${SPARK_HOME}"
  exit 1
fi

if [[ ! -f "${PYSPARK_SCRIPT}" ]]; then
  echo "Error: Script not found: ${PYSPARK_SCRIPT}"
  exit 1
fi

export SPARK_HOME
export PATH="${SPARK_HOME}/bin:${PATH}"

# Detect Spark major version
SPARK_MAJOR=$(echo "${SPARK_VERSION}" | cut -d'.' -f1)

# For Spark 4.x, use Java 17 if JAVA_17 env variable is set
if [[ "${SPARK_MAJOR}" == "4" ]]; then
  if [[ -n "${JAVA_17}" ]] && [[ -d "${JAVA_17}" ]]; then
    export JAVA_HOME="${JAVA_17}"
    export PATH="${JAVA_HOME}/bin:${PATH}"
    echo "Using Java 17 for Spark 4.x: ${JAVA_HOME}"
  else
    # Check if current Java is 17+
    JAVA_VERSION=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}' | cut -d'.' -f1)
    if [[ "${JAVA_VERSION}" -lt 17 ]]; then
      echo "╔══════════════════════════════════════════════════════════════╗"
      echo "║  ERROR: Spark 4.x requires Java 17 or higher"
      echo "║  Current Java version: ${JAVA_VERSION}"
      echo "║  "
      echo "║  Please set JAVA_17 environment variable:"
      echo "║    export JAVA_17=/path/to/java17/home"
      echo "║  "
      echo "║  Or set JAVA_HOME directly:"
      echo "║    export JAVA_HOME=/path/to/java17"
      echo "║    export PATH=\$JAVA_HOME/bin:\$PATH"
      echo "╚══════════════════════════════════════════════════════════════╝"
      echo ""
      exit 1
    fi
  fi
fi

# Get current Java version for display
JAVA_VERSION=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}' | cut -d'.' -f1)

# Ensure driver and worker use the same Python interpreter
PYTHON_BIN="$(which python3)"
export PYSPARK_PYTHON="${PYTHON_BIN}"
export PYSPARK_DRIVER_PYTHON="${PYTHON_BIN}"

# Build PYTHONPATH from this Spark version's python libs (must come first to shadow any pip pyspark)
PY4J_ZIP="$(ls "${SPARK_HOME}"/python/lib/py4j-*.zip 2>/dev/null | head -1)"
export PYTHONPATH="${SPARK_HOME}/python${PY4J_ZIP:+:${PY4J_ZIP}}${PYTHONPATH:+:${PYTHONPATH}}"

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║  SPARK_HOME : ${SPARK_HOME}"
echo "║  Spark ver  : ${SPARK_VERSION}"
echo "║  Java ver   : ${JAVA_VERSION}"
echo "║  Script     : ${PYSPARK_SCRIPT}"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

exec python3 "${PYSPARK_SCRIPT}" "$@"
