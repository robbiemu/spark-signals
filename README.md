# spark-signals

Host-native telemetry for NVIDIA DGX Spark. `spark-agent` samples Linux health
signals, wraps them in the versioned `spark.signal/v1` contract, and publishes
self-contained snapshots to NATS Core. The prototype can also emit JSON Lines
to stdout for fixture validation and diagnostics.

This project takes collection lessons from
[MiaAI-Lab/sparkDash](https://github.com/MiaAI-Lab/sparkDash) while separating
measurement from presentation. In particular, missing or failed observations
remain absent and carry an explicit quality state; they are never converted to
plausible numeric zeroes.

## Prototype scope

The current prototype includes:

- a versioned JSON envelope and finite metric catalogue;
- `/proc` collectors for uptime, CPU utilization, load, Linux unified-memory
  facts, swap capacity, and CPU/memory PSI;
- baseline-aware CPU utilization (the first reading is explicitly unavailable);
- a single periodic Tokio scheduler;
- JSON Lines diagnostics and a coalescing NATS publication channel; and
- a hardened example systemd unit.

NVIDIA/NVML, storage, network, OTEL bridge, services, and LLM probes are tracked
in [ROADMAP.md](ROADMAP.md).

## Run

Print one observation:

```console
cargo run -p spark-agent -- --once --stdout --site home --node spark-885a
```

Publish periodically to NATS (and also print for diagnostics):

```console
cargo run -p spark-agent -- \
  --nats-url nats://127.0.0.1:4222 \
  --stdout \
  --site home \
  --node spark-885a
```

When `--nats-url` is omitted, stdout output is enabled automatically. The NATS
subject is `spark.v1.<site>.<node>.sample.system`. Site and node components are
restricted to ASCII letters, digits, `_`, and `-`.

For prototype installs that keep the binary under `~/projects`, use
`deploy/systemd/spark-agent.user.service`. The system-wide production example
is `deploy/systemd/spark-agent.service` and expects a dedicated service account
and `/usr/local/bin/spark-agent`.

## Validate

```console
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

On a deployed Linux host, `deploy/validate-host.sh` compares the agent's
reported `MemTotal` to `/proc/meminfo`, checks unavailable-value quality states,
and verifies that the stdout-only prototype service has no listening sockets.
`deploy/test-nats.sh` uses short-lived Docker containers to verify a real NATS
Core publication and removes them when the check finishes.

## License

MIT
