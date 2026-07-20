#!/usr/bin/env bash
set -euo pipefail

repo_root=${1:-"$HOME/projects/spark-signals"}
agent="$repo_root/target/release/spark-agent"

sample=$($agent --once --stdout --site home --node spark-885a)
reported=$(jq -r '.points[] | select(.name == "system.memory.linux.total") | .value' <<<"$sample")
expected=$(awk '$1 == "MemTotal:" { print $2 * 1024 }' /proc/meminfo)

awk -v reported="$reported" -v expected="$expected" 'BEGIN { exit !(reported == expected) }'
printf 'MemTotal bytes: reported=%s expected=%s\n' "$reported" "$expected"

jq -e '[.points[] | select(.value == null)] | all(.quality == "error" or .quality == "unsupported" or .quality == "stale")' <<<"$sample" >/dev/null
printf 'Null observation quality states: valid\n'

if systemctl --user is-active --quiet spark-agent.service; then
  pid=$(systemctl --user show -p MainPID --value spark-agent.service)
  listeners=$(ss -lntup | grep -c "pid=$pid," || true)
  printf 'Listening network sockets: %s\n' "$listeners"
  test "$listeners" -eq 0
fi
