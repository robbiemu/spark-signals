#!/usr/bin/env bash
set -euo pipefail

repo_root=${1:-"$HOME/projects/spark-signals"}
agent="$repo_root/target/release/spark-agent"
server_container="spark-signals-nats-test-$$"
sample_file=$(mktemp)
agent_password=$(openssl rand -hex 24)
bridge_password=$(openssl rand -hex 24)

cleanup() {
  docker stop "$server_container" >/dev/null 2>&1 || true
  rm -f "$sample_file"
}
trap cleanup EXIT

docker run --detach --rm --name "$server_container" \
  --publish 127.0.0.1:14222:4222 \
  --env SPARK_AGENT_PASSWORD="$agent_password" \
  --env SPARK_BRIDGE_PASSWORD="$bridge_password" \
  --volume "$repo_root/deploy/nats/nats-server.conf:/etc/nats/nats-server.conf:ro" \
  nats:2-alpine --config /etc/nats/nats-server.conf \
  --addr 0.0.0.0 --port 4222 >/dev/null
sleep 1

docker run --rm --network host natsio/nats-box:latest \
  nats sub --server nats://spark-bridge:"$bridge_password"@127.0.0.1:14222 \
  --count 1 --raw 'spark.v1.home.spark-885a.sample.system' \
  >"$sample_file" &
subscriber_pid=$!
sleep 1

timeout 6 "$agent" --nats-url nats://127.0.0.1:14222 \
  --nats-user spark-agent --nats-password "$agent_password" \
  --site home --node spark-885a || test $? -eq 124
wait "$subscriber_pid"

jq -e '.schema == "spark.signal/v1" and .node.id == "spark-885a" and
  .kind == "metric_batch" and (.points | length > 0)' \
  "$sample_file" >/dev/null
printf 'Authenticated NATS publication received and schema-validated\n'
