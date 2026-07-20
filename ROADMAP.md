# Spark Signals Roadmap

This roadmap turns the July 19, 2026 technical specification into verifiable
delivery slices. Checkboxes describe repository status, not aspirations that
have already been validated on every DGX Spark configuration.

## Prototype — schema and Linux agent core

- [x] Define `spark.signal/v1` envelopes, nodes, quality states, and instruments.
- [x] Keep a finite v1 metric catalogue and reject unknown metric names.
- [x] Add Linux parsers for `/proc/stat`, `/proc/meminfo`, `/proc/uptime`,
  `/proc/loadavg`, and `/proc/pressure/{cpu,memory}`.
- [x] Publish `MemAvailable`, swap capacity, UMA allocatable values, CPU load,
  CPU utilization, uptime, and pressure observations.
- [x] Carry observation time, monotonic time, boot ID, sequence, collection
  duration, validity period, source, and quality.
- [x] Treat the initial CPU rate baseline as unavailable rather than zero.
- [x] Use one Tokio scheduler and continue collection without consumers.
- [x] Provide JSON Lines output for fixture and host validation.
- [x] Coalesce unsent routine state in a bounded NATS watch channel.
- [x] Add a non-root, no-listening-port systemd example.
- [ ] Run the prototype through longer idle and inference-load soak tests.

Validation record (2026-07-19, `spark-885a`): ARM64 release build, unit tests,
strict Clippy, live `/proc/meminfo` comparison, null-quality checks, user systemd
service startup, no-listening-socket check, and Dockerized NATS Core publication
all passed. Idle service RSS observed at approximately 3.5 MiB; this is an
observation from one short run, not yet a footprint guarantee.

## Phase 2 — NVIDIA and complete Linux health

- [ ] Inventory all hwmon devices, labels, limits, and thermal-zone types.
- [ ] Add network counters, errors, drops, carrier state, and rate baselines.
- [ ] Add filesystem capacity/inodes/read-only state and block I/O rates.
- [ ] Add paging, global/cgroup OOM, reclaim, and configured cgroup signals.
- [ ] Dynamically load NVML and capability-probe every NVIDIA field.
- [ ] Add guarded, named-field `nvidia-smi --query-*` fallback only where proven.
- [ ] Publish GPU identity, utilization, temperature, power, clocks, throttle
  reasons, Xid events, and opt-in per-process allocation.
- [ ] Validate that GB10 never claims dedicated VRAM or undocumented bandwidth.
- [ ] Add GB10 fixtures and compare output against the source kernel/NVIDIA APIs.

## Phase 3 — resilient distribution

- [ ] Split bounded event FIFO from coalesced state samples.
- [ ] Publish status, inventory, and full state in order after reconnect.
- [ ] Add per-node publish-only NATS credentials and TLS/NKey/JWT examples.
- [ ] Add agent self-health, reconnect, dropped-event, and collector-age metrics.
- [ ] Exercise indefinite NATS outage and reconnect acceptance tests.
- [ ] Evaluate selective JetStream retention for critical events.

## Phase 4 — OTEL bridge and Maple

- [ ] Add `spark-otel-bridge` with a bounded NATS receive/export pipeline.
- [ ] Map catalogue metrics to pinned OpenTelemetry semantic conventions.
- [ ] Translate health events to OTEL logs and reject oversized/unknown messages.
- [ ] Configure OTLP/HTTP metrics and logs with injected authorization headers.
- [ ] Test Maple outage behavior without affecting edge collection.

## Phase 5 — configured services and inference adapters

- [ ] Collect only explicitly configured systemd units.
- [ ] Add authenticated/TLS-capable SGLang, vLLM, and llama.cpp adapters.
- [ ] Publish endpoint health, queue depth, cumulative tokens, and derived rates.
- [ ] Redact model paths, API keys, command lines, and other high-cardinality data.

## Phase 6 — consumers and operational hardening

- [ ] Build a pure NATS consumer UI with per-domain freshness and quality states.
- [ ] Decide between direct NATS WebSocket and an authenticated thin gateway.
- [ ] Measure and enforce the agent/bridge RSS, CPU, binary-size, and payload budgets.
- [ ] Validate the final systemd sandbox against NVML/device access, relaxing only
  demonstrated restrictions.
- [ ] Complete idle, inference, memory-pressure, broker/exporter outage, service
  restart, and agent restart acceptance scenarios.
