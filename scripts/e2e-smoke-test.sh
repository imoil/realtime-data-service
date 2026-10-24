#!/usr/bin/env bash
# =============================================================================
# Automated End-to-End Smoke Verification Script (Linux / macOS)
# =============================================================================
set -euo pipefail

echo "=========================================================="
echo "Running Telemetry End-to-End Smoke Verification..."
echo "=========================================================="

GATEWAY_URL="${GATEWAY_URL:-http://localhost:8080}"
CLICKHOUSE_URL="${CLICKHOUSE_URL:-http://localhost:8123}"
FRONTEND_URL="${FRONTEND_URL:-http://localhost:5173}"

PASS_COUNT=0
FAIL_COUNT=0

assert_step() {
  local name="$1"
  local cmd="$2"
  echo -n "Checking: $name ... "
  if eval "$cmd" > /dev/null 2>&1; then
    echo "PASS"
    PASS_COUNT=$((PASS_COUNT + 1))
  else
    echo "FAIL"
    FAIL_COUNT=$((FAIL_COUNT + 1))
  fi
}

# 1. ClickHouse ping
assert_step "ClickHouse HTTP /ping" "curl -s -f '$CLICKHOUSE_URL/ping' | grep -q 'Ok'"

# 2. ClickHouse IoT tables check
assert_step "ClickHouse 'iot' database rollup tables" "curl -s -f '$CLICKHOUSE_URL/?query=EXISTS+TABLE+iot.sensor_rollup_1m' | grep -q '1'"

# 3. Gateway HTTP AM4-Arrow endpoint
assert_step "Gateway /api/v1/telemetry/am4-arrow status 200" "curl -s -f -o /tmp/smoke_arrow.bin -w '%{http_code}' '$GATEWAY_URL/api/v1/telemetry/am4-arrow?sensorId=1&range=1h&resolution=1m' | grep -q '200'"

# 4. Verify Arrow binary payload is non-empty
assert_step "Gateway Arrow stream payload non-empty" "[ -s /tmp/smoke_arrow.bin ]"

# 5. Frontend HTTP status
assert_step "Frontend Web UI accessible" "curl -s -f '$FRONTEND_URL' | grep -qi 'doctype html'"

echo "=========================================================="
echo "Smoke Verification Summary: $PASS_COUNT PASSED, $FAIL_COUNT FAILED"
echo "=========================================================="

if [ $FAIL_COUNT -eq 0 ]; then
  echo "ALL SMOKE CHECKS PASSED!"
  exit 0
else
  echo "SOME CHECKS FAILED."
  exit 1
fi
