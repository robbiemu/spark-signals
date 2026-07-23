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

Validation record (2026-07-19, DGX Spark GB10 host): ARM64 release build, unit tests,
strict Clippy, live `/proc/meminfo` comparison, null-quality checks, user systemd
service startup, no-listening-socket check, and Dockerized NATS Core publication
all passed. Idle service RSS observed at approximately 3.5 MiB; this is an
observation from one short run, not yet a footprint guarantee.

The production deployment uses dedicated system identities, root-owned files,
and `multi-user.target`; the login-owned units remain development-only. The
one-time installer can migrate away from an existing login-owned user service
and restores it if the system agent cannot start.

## Phase 2 — NVIDIA and complete Linux health

- [x] Inventory all hwmon devices, labels, limits, and thermal-zone types.
- [x] Add network counters, errors, drops, carrier state, and rate baselines.
- [x] Add filesystem capacity/inodes/read-only state and block I/O rates.
- [x] Add paging, global/cgroup OOM, reclaim, and configured cgroup signals.
- [x] Dynamically load NVML and capability-probe every NVIDIA field.
- [x] Add guarded, named-field `nvidia-smi --query-*` fallback only where proven.
- [x] Publish GPU identity, utilization, temperature, power, clocks, throttle
  reasons, encoder/decoder use, Xid capability/events, and opt-in per-process
  allocation.
- [x] Validate that GB10 never claims dedicated VRAM or undocumented bandwidth.
- [x] Add a named-field GB10 fallback fixture and compare unsupported fields
  against the source NVIDIA output.

Validation record (2026-07-19, DGX Spark GB10 host): the ARM64 release build passed;
live Linux memory, network, NVMe, filesystem, temperature, NVML utilization,
temperature, power, clocks, and throttle observations were collected. GB10
memory-clock support remained explicit `unsupported`, and the inventory reports
unified memory plus 273 GB/s as capability metadata rather than measurement.
Filesystem collection excludes pseudo and read-only squashfs mounts; the live
payload maximum was 14,075 bytes, below the 64 KiB schema ceiling. An actual Xid
fault was not induced, but NVML event capability and the Xid stream point were
observed.

## Phase 3 — resilient distribution

- [x] Split bounded event FIFO from coalesced state samples.
- [x] Publish status, inventory, and full state in order after reconnect.
- [x] Add a per-node publish-only username/password ACL and client-side TLS/CA
  support, with a documented NKey/JWT migration path.
- [x] Add agent collection-error, reconnect, dropped-event, and per-domain
  collector-age self metrics.
- [x] Exercise a broker outage and verify complete inventory replay after
  reconnect; a longer indefinite-outage soak remains.
- [x] Evaluate selective JetStream retention for critical events and document
  the Core NATS durability limitation; retention is not enabled yet.

Validation record (2026-07-19, DGX Spark GB10 host): authenticated Dockerized NATS
publication passed, the broker survived a stop/start exercise, the agent stayed
active, and a connected consumer received replayed inventory after reconnect.
Live subscription checks received all six sample subjects.

## Phase 4 — OTEL bridge and target plugins

- [x] Add `spark-otel-bridge` with a bounded NATS receive/export pipeline.
- [x] Map catalogue metrics to OTEL instruments with stable Spark resource and
  measurement attributes, pinned to semantic-convention revision 1.41.1.
- [x] Translate health events to OTEL logs and reject oversized/unknown messages.
- [x] Configure OTLP/HTTP metrics and logs through compile-time `standard` and
  authenticated Maple target plugins with a backend-neutral prepared target.
- [x] Test metrics and logs, injected authorization, receiver outage isolation,
  and recovery against a local OTLP receiver.

Validation record (2026-07-20, DGX Spark GB10 host): the bridge exported protobuf
metrics and logs to isolated mock endpoints, then to the real authenticated
Maple ingress. Maple queries returned more than 66 metric names, including
`system.uptime` and `nvidia.gpu.utilization`, plus Spark inventory and
unavailable-metric logs with the expected `host.id`. The root-owned credential,
in-process Basic header, privilege drop, bridge lifecycle independence, and zero
trace output were verified. The Maple endpoint required operator-managed name
resolution on this node.

## Phase 5 — configured services and inference adapters

- [x] Collect only explicitly configured systemd units.
- [x] Add authenticated/TLS-capable SGLang, vLLM, llama.cpp, OpenAI-compatible,
  and configurable Prometheus adapters.
- [x] Publish endpoint health, queue depth, cumulative-token deltas, and derived
  rates when the engine exposes them.
- [x] Exclude model paths, API keys, command lines, prompts, responses, and
  unconstrained source labels.
- [ ] Define a versioned agent-side provider interface for live
  configured-service and inference probe definitions, distinct from OTLP target
  plugins. Specify stable identities, validated atomic snapshots, safe
  non-secret authentication-file references, and reconciliation semantics for
  added, removed, changed, unchanged, invalid, and partial provider state.
  Preserve sampling state for unchanged probes, reset affected baselines when a
  definition changes, retain the last valid snapshot after rejected updates,
  and emit bounded provider-health events.

Validation record (2026-07-19, DGX Spark GB10 host): both configured SGLang endpoints
were stopped and appeared on the live NATS stream as explicit unreachable/error
observations. Configured inference systemd units appeared as inactive rather
than disappearing. Prometheus aggregation and custom-name mappings passed unit
tests. A lightweight authenticated SGLang-compatible endpoint also validated
availability, queue depth, token deltas, and derived rates without starting the
RAM-intensive inference services.

## Phase 6 — consumers and operational hardening

- [ ] Add a built-in `dgx-spark-inference` agent provider using the generic live
  provider interface. Define and consume a versioned, producer-owned discovery
  contract that atomically reflects current resolved roles, active models,
  runtime kinds, systemd units, endpoints, metric paths, context capacities,
  and non-secret authentication-file references. Watch and periodically
  reconcile it so model switches, unloads, reloads, role additions, and endpoint
  changes take effect without restarting either service or parsing the
  inference system's internal operator files.
- [ ] Establish a versioned release process with release criteria, protected
  tags, release notes, checksummed Linux AArch64 bundles containing the binaries
  and deployment assets, documented configuration migrations, and tested
  upgrade and rollback paths. Replace commit-based operator installation with
  release selection when that process is ready.
- [ ] Define qualification and configuration contracts for additional OTLP
  target plugins by evaluating Grafana Cloud, Honeycomb, New Relic, and Datadog
  through the existing `standard` target. Require a demonstrated need such as
  protected credential loading, endpoint validation, or override policy before
  selecting a provider-specific implementation.
- [ ] Build a pure NATS consumer UI with per-domain freshness and quality states.
- [ ] Decide between direct NATS WebSocket and an authenticated thin gateway.
- [ ] Measure and enforce the agent/bridge RSS, CPU, binary-size, and payload budgets.
- [ ] Validate the final systemd sandbox against NVML/device access, relaxing only
  demonstrated restrictions.
- [ ] Define platform abstraction interfaces for host and accelerator telemetry,
  configured-service health probes, and agent/bridge supervision, retaining
  systemd as one Linux implementation rather than a core dependency.
- [ ] Complete idle, inference, memory-pressure, broker/exporter outage, service
  restart, and agent restart acceptance scenarios.

## Phase 7 — modular integrations and platform extensions

- [ ] Implement and validate the provider-specific OTLP target plugins selected
  by the Phase 6 qualification contract, retaining the backend-neutral prepared
  target boundary and interoperability with the `standard` target.
- [ ] Implement and validate platform providers for selected non-DGX
  environments, including possible macOS/Apple Silicon, Linux/AMD accelerator,
  and clustered hosts, without weakening privilege isolation, quality,
  freshness, or service-status semantics.
