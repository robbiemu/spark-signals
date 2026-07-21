#!/usr/bin/env bash
set -euo pipefail

repo_root=${1:-"$HOME/projects/spark-signals"}
bridge="$repo_root/target/release/spark-otel-bridge"
requests=$(mktemp)
bridge_log=$(mktemp)

cleanup() {
  kill "${bridge_pid:-}" "${mock_pid:-}" >/dev/null 2>&1 || true
  wait "${bridge_pid:-}" "${mock_pid:-}" >/dev/null 2>&1 || true
  rm -f "$requests" "$bridge_log"
}
trap cleanup EXIT

set -a
. "$repo_root/deploy/runtime/bridge.env"
set +a

start_mock() {
  python3 "$repo_root/deploy/mock-otlp.py" --port 14318 --output "$requests" \
    --expected-authorization 'Bearer fixture-token' &
  mock_pid=$!
  sleep 1
}

start_mock

if systemctl is-active --quiet spark-agent.service; then
  agent_scope=system
elif systemctl --user is-active --quiet spark-agent.service; then
  agent_scope=user
else
  printf 'spark-agent service is not active\n' >&2
  exit 1
fi

OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:14318 \
OTEL_EXPORTER_OTLP_HEADERS='authorization=Bearer%20fixture-token' \
OTEL_METRIC_EXPORT_INTERVAL=1000 \
OTEL_BLRP_SCHEDULE_DELAY=1000 \
  "$bridge" >"$bridge_log" 2>&1 &
bridge_pid=$!
sleep 2

for _ in $(seq 1 15); do
  if grep -q '^/v1/metrics$' "$requests" && grep -q '^/v1/logs$' "$requests"; then
    break
  fi
  sleep 1
done

grep -q '^/v1/metrics$' "$requests"
grep -q '^/v1/logs$' "$requests"

kill "$mock_pid"
wait "$mock_pid" >/dev/null 2>&1 || true
before=$(wc -l <"$requests")
sleep 3
kill -0 "$bridge_pid"
if test "$agent_scope" = system; then
  test "$(systemctl is-active spark-agent.service)" = active
else
  test "$(systemctl --user is-active spark-agent.service)" = active
fi

start_mock
for _ in $(seq 1 15); do
  if test "$(wc -l <"$requests")" -gt "$before"; then
    printf 'OTLP auth, metrics/logs, outage isolation, and recovery passed\n'
    exit 0
  fi
  sleep 1
done

exit 1
