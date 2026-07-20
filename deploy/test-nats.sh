#!/usr/bin/env bash
set -euo pipefail

repo_root=${1:-"$HOME/projects/spark-signals"}
agent="$repo_root/target/release/spark-agent"
server_container="spark-signals-nats-test-$$"
sample_file=$(mktemp)

cleanup() {
  docker stop "$server_container" >/dev/null 2>&1 || true
  rm -f "$sample_file"
}
trap cleanup EXIT

docker run --detach --rm --name "$server_container" \
  --publish 127.0.0.1:14222:4222 nats:2-alpine >/dev/null
sleep 1

docker run --rm --network host natsio/nats-box:latest \
  nats sub --server nats://127.0.0.1:14222 --count 1 --raw 'spark.v1.>' \
  >"$sample_file" &
subscriber_pid=$!
sleep 1

timeout 6 "$agent" --nats-url nats://127.0.0.1:14222 \
  --site home --node spark-885a || test $? -eq 124
wait "$subscriber_pid"

jq -e '.schema == "spark.signal/v1" and .node.id == "spark-885a" and (.points | length > 0)' \
  "$sample_file" >/dev/null
printf 'NATS publication received and schema-validated\n'

