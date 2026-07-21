#!/usr/bin/env bash
set -euo pipefail

repo_root=${1:-"$HOME/projects/spark-signals"}
agent="$repo_root/target/release/spark-agent"
set -a
. "$repo_root/.env.test"
set +a

sample_file=$(mktemp)
trap 'rm -f "$sample_file"' EXIT
"$agent" --once --stdout --site "$SPARK_TEST_SITE" \
  --node "$SPARK_TEST_NODE" >"$sample_file"
reported=$(jq -r 'select(.kind == "metric_batch") | .points[] |
  select(.name == "system.memory.linux.total") | .value' "$sample_file")
expected=$(awk '$1 == "MemTotal:" { print $2 * 1024 }' /proc/meminfo)

awk -v reported="$reported" -v expected="$expected" 'BEGIN { exit !(reported == expected) }'
printf 'MemTotal bytes: reported=%s expected=%s\n' "$reported" "$expected"

jq -s -e '[.[] | select(.kind == "metric_batch") | .points[] |
  select(.value == null)] | all(.quality == "error" or
  .quality == "unsupported" or .quality == "stale")' "$sample_file" >/dev/null
printf 'Null observation quality states: valid\n'

if systemctl is-active --quiet spark-agent.service; then
  pid=$(systemctl show -p MainPID --value spark-agent.service)
  listeners=$(ss -lntup | grep -c "pid=$pid," || true)
  printf 'Listening network sockets: %s\n' "$listeners"
  test "$listeners" -eq 0
elif systemctl --user is-active --quiet spark-agent.service; then
  pid=$(systemctl --user show -p MainPID --value spark-agent.service)
  listeners=$(ss -lntup | grep -c "pid=$pid," || true)
  printf 'Listening network sockets: %s\n' "$listeners"
  test "$listeners" -eq 0
fi
