#!/usr/bin/env bash
set -euo pipefail

repo_root=${1:-"$HOME/projects/spark-signals"}
agent="$repo_root/target/release/spark-agent"
config=$(mktemp)
auth=$(mktemp)
samples=$(mktemp)

cleanup() {
  kill "${mock_pid:-}" >/dev/null 2>&1 || true
  wait "${mock_pid:-}" >/dev/null 2>&1 || true
  rm -f "$config" "$auth" "$samples"
}
trap cleanup EXIT

chmod 600 "$auth"
printf 'Authorization: Bearer fixture-token\n' >"$auth"
printf '[[llm]]\nid = "fixture"\nbackend = "sglang"\nbase_url = "http://127.0.0.1:13000"\nauth_header_file = "%s"\n' \
  "$auth" >"$config"

python3 "$repo_root/deploy/mock-llm-metrics.py" --port 13000 &
mock_pid=$!
sleep 1

timeout 24 "$agent" --stdout --site test --node fixture \
  --interval-seconds 1 --config "$config" >"$samples" || test $? -eq 124

jq -s -e '
  [.[] | select(.kind == "metric_batch") |
    select(any(.points[]; .attributes["llm.endpoint.id"] == "fixture"))] as $batches |
  ($batches | length) >= 2 and
  ($batches[-1].points | any(.name == "spark.llm.available" and .value == 1)) and
  ($batches[-1].points | any(.name == "spark.llm.requests.running" and .value == 2)) and
  ($batches[-1].points | any(.name == "spark.llm.requests.queued" and .value == 1)) and
  ($batches[-1].points | any(.name == "spark.llm.tokens.input" and .value > 0)) and
  ($batches[-1].points | any(.name == "spark.llm.tokens.output" and .value > 0)) and
  ($batches[-1].points | any(.name == "spark.llm.tokens.prefill.rate" and .value > 0)) and
  ($batches[-1].points | any(.name == "spark.llm.tokens.generation.rate" and .value > 0))
' "$samples" >/dev/null

printf 'Authenticated SGLang availability, queue, token deltas, and rates passed\n'
