#!/usr/bin/env bash
# Verify History Server with DataFlint UI integration
# Checks that the History Server is running, applications are listed,
# and DataFlint UI endpoints are accessible.
set -euo pipefail

HISTORY_URL="http://localhost:18080"
FAILURES=0
PASSES=0

echo "=============================="
echo "History Server & DataFlint UI Verification"
echo "=============================="

# Wait for applications to appear (History Server needs time to parse event logs)
echo "Waiting for applications to appear..."
for i in $(seq 1 30); do
  APP_COUNT=$(curl -sf "${HISTORY_URL}/api/v1/applications" 2>/dev/null | python3 -c "import sys,json; print(len(json.load(sys.stdin)))" 2>/dev/null || echo "0")
  if [ "${APP_COUNT}" -gt 0 ]; then
    echo "Found ${APP_COUNT} application(s) after ${i} attempts"
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo "FAIL: No applications appeared within 60 seconds"
    exit 1
  fi
  sleep 2
done

# 1. Check base URL is reachable
echo ""
echo "--- Check: History Server base URL ---"
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "${HISTORY_URL}")
if [ "$HTTP_CODE" != "200" ]; then
  echo "FAIL: Base URL returned HTTP ${HTTP_CODE}"
  FAILURES=$((FAILURES + 1))
else
  echo "PASS: Base URL returned HTTP 200"
  PASSES=$((PASSES + 1))
fi

# 2. Check applications API endpoint
echo ""
echo "--- Check: Applications API ---"
APPS_JSON=$(curl -sf "${HISTORY_URL}/api/v1/applications" || echo "CURL_FAILED")
if [ "$APPS_JSON" = "CURL_FAILED" ]; then
  echo "FAIL: Could not fetch applications API"
  FAILURES=$((FAILURES + 1))
else
  APP_COUNT=$(echo "$APPS_JSON" | python3 -c "import sys,json; print(len(json.load(sys.stdin)))")
  echo "PASS: Applications API returned ${APP_COUNT} application(s)"
  PASSES=$((PASSES + 1))
  if [ "$APP_COUNT" -lt 1 ]; then
    echo "FAIL: Expected at least 1 application"
    FAILURES=$((FAILURES + 1))
  fi
fi

# 3. For each application (up to 5), verify DataFlint UI endpoints
echo ""
echo "--- Check: DataFlint UI endpoints ---"
APP_IDS=$(echo "$APPS_JSON" | python3 -c "
import sys, json
apps = json.load(sys.stdin)
for app in apps:
    print(app['id'])
" 2>/dev/null || echo "")

if [ -z "$APP_IDS" ]; then
  echo "FAIL: Could not extract application IDs"
  FAILURES=$((FAILURES + 1))
else
  CHECKED=0
  for APP_ID in $APP_IDS; do
    if [ "$CHECKED" -ge 5 ]; then
      echo "  (checked 5 apps, skipping remaining)"
      break
    fi

    echo ""
    echo "  App: ${APP_ID}"

    # Check DataFlint UI page
    DF_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
      "${HISTORY_URL}/history/${APP_ID}/dataflint/")
    if [ "$DF_CODE" != "200" ]; then
      echo "    FAIL: DataFlint UI returned HTTP ${DF_CODE}"
      FAILURES=$((FAILURES + 1))
    else
      echo "    PASS: DataFlint UI accessible (HTTP 200)"
      PASSES=$((PASSES + 1))
    fi

    # Check DataFlint applicationinfo JSON endpoint
    AI_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
      "${HISTORY_URL}/history/${APP_ID}/dataflint/applicationinfo/json/")
    if [ "$AI_CODE" != "200" ]; then
      echo "    FAIL: DataFlint applicationinfo returned HTTP ${AI_CODE}"
      FAILURES=$((FAILURES + 1))
    else
      echo "    PASS: DataFlint applicationinfo JSON accessible (HTTP 200)"
      PASSES=$((PASSES + 1))
    fi

    # Check standard Spark API for SQL executions
    SQL_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
      "${HISTORY_URL}/api/v1/applications/${APP_ID}/sql/")
    if [ "$SQL_CODE" != "200" ]; then
      echo "    WARN: SQL API returned HTTP ${SQL_CODE} (may not have SQL executions)"
    else
      SQL_COUNT=$(curl -sf "${HISTORY_URL}/api/v1/applications/${APP_ID}/sql/" | \
        python3 -c "import sys,json; print(len(json.load(sys.stdin)))" 2>/dev/null || echo "0")
      echo "    INFO: ${SQL_COUNT} SQL execution(s) found"
    fi

    CHECKED=$((CHECKED + 1))
  done
fi

echo ""
echo "=============================="
echo "Verification Summary"
echo "=============================="
echo "  Passed: ${PASSES}"
echo "  Failed: ${FAILURES}"
if [ "$FAILURES" -gt 0 ]; then
  echo ""
  echo "VERIFICATION FAILED"
  exit 1
fi
echo ""
echo "ALL CHECKS PASSED"
echo "=============================="
