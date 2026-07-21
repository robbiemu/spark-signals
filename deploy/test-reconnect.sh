#!/usr/bin/env bash
set -euo pipefail

repo_root=${1:-"$HOME/projects/spark-signals"}
compose="$repo_root/deploy/nats/compose.yml"
sample=$(mktemp)
subscriber_log=$(mktemp)

cleanup() {
  docker compose -f "$compose" up -d >/dev/null 2>&1 || true
  rm -f "$sample" "$subscriber_log"
}
trap cleanup EXIT

set -a
. "$repo_root/deploy/runtime/nats.env"
. "$repo_root/deploy/runtime/agent.env"
set +a
: "${SPARK_SITE:?SPARK_SITE is required in deploy/runtime/agent.env}"
: "${SPARK_NODE:?SPARK_NODE is required in deploy/runtime/agent.env}"
inventory_subject="spark.v1.${SPARK_SITE}.${SPARK_NODE}.inventory"

timeout 40 docker run --rm --network host natsio/nats-box:latest \
  nats sub \
  --server "nats://spark-bridge:$SPARK_BRIDGE_PASSWORD@127.0.0.1:4222" \
  --count 2 --raw "$inventory_subject" \
  >"$sample" 2>"$subscriber_log" &
subscriber_pid=$!

sleep 2
systemctl --user restart spark-agent.service
sleep 3
docker compose -f "$compose" stop >/dev/null
sleep 8
docker compose -f "$compose" start >/dev/null
wait "$subscriber_pid"

jq -s -e 'length == 2 and
  .[0].kind == "inventory" and .[1].kind == "inventory" and
  .[1].sequence >= .[0].sequence' "$sample" >/dev/null
test "$(systemctl --user is-active spark-agent.service)" = active
printf 'NATS outage reconnect replayed complete inventory state\n'
