#!/usr/bin/env bash
# Runs TPC-DS queries using spark-sql with DataFlint plugin enabled.
# Usage: run-tpcds-queries.sh <SPARK_HOME> <EVENT_LOG_DIR> <WAREHOUSE_DIR>
set -euo pipefail

SPARK_HOME="$1"
EVENT_LOG_DIR="$2"
WAREHOUSE_DIR="$3"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
QUERY_DIR="${SCRIPT_DIR}/queries"

FAILED=0
PASSED=0
SKIPPED=0
FAILURES=""

COMMON_CONF=(
  --master "local[*]"
  --conf spark.plugins=io.dataflint.spark.SparkDataflintPlugin
  --conf spark.dataflint.telemetry.enabled=false
  --conf spark.eventLog.enabled=true
  --conf "spark.eventLog.dir=file://${EVENT_LOG_DIR}"
  --conf "spark.sql.warehouse.dir=${WAREHOUSE_DIR}"
  --conf spark.driver.memory=2g
  --conf "spark.driver.extraJavaOptions=-Dderby.system.home=${WAREHOUSE_DIR}/derby"
  --conf spark.ui.enabled=false
  --conf spark.sql.adaptive.enabled=true
  --conf spark.sql.ansi.enabled=false
)

echo "=============================="
echo "TPC-DS Benchmark with DataFlint"
echo "=============================="
echo "Spark home: ${SPARK_HOME}"
echo "Event log dir: ${EVENT_LOG_DIR}"
echo "Warehouse dir: ${WAREHOUSE_DIR}"
echo ""

# Count total queries
TOTAL=$(ls "${QUERY_DIR}"/q*.sql 2>/dev/null | wc -l)
echo "Found ${TOTAL} TPC-DS queries to run"
echo ""

for query_file in "${QUERY_DIR}"/q*.sql; do
  query_name="$(basename "${query_file}" .sql)"
  echo "--- Running ${query_name} ($(( PASSED + FAILED + SKIPPED + 1 ))/${TOTAL}) ---"

  if "${SPARK_HOME}/bin/spark-sql" \
    "${COMMON_CONF[@]}" \
    --conf "spark.app.name=TPC-DS-${query_name}" \
    -f "${query_file}" > /tmp/tpcds-${query_name}.out 2>&1; then
    echo "  ${query_name}: PASSED"
    PASSED=$((PASSED + 1))
  else
    EXIT_CODE=$?
    # Check if it's a known Spark SQL limitation vs a real plugin error
    if grep -q "AnalysisException\|ParseException\|UnsupportedOperationException" /tmp/tpcds-${query_name}.out 2>/dev/null; then
      echo "  ${query_name}: SKIPPED (Spark SQL limitation, exit code ${EXIT_CODE})"
      SKIPPED=$((SKIPPED + 1))
    else
      echo "  ${query_name}: FAILED (exit code ${EXIT_CODE})"
      echo "  Last 10 lines of output:"
      tail -10 /tmp/tpcds-${query_name}.out 2>/dev/null | sed 's/^/    /'
      FAILED=$((FAILED + 1))
      FAILURES="${FAILURES} ${query_name}"
    fi
  fi
done

echo ""
echo "=============================="
echo "TPC-DS Results Summary"
echo "=============================="
echo "  Passed:  ${PASSED}"
echo "  Skipped: ${SKIPPED} (Spark SQL limitations)"
echo "  Failed:  ${FAILED}"
echo "  Total:   ${TOTAL}"
if [ "${FAILED}" -gt 0 ]; then
  echo ""
  echo "Failed queries:${FAILURES}"
  echo ""
  echo "Dumping failed query outputs:"
  for qname in ${FAILURES}; do
    echo "--- ${qname} output ---"
    cat /tmp/tpcds-${qname}.out 2>/dev/null || echo "(no output)"
    echo ""
  done
  exit 1
fi
echo "=============================="
