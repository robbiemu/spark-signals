# OpenTelemetry mapping

The bridge mapping is pinned to OpenTelemetry Semantic Conventions revision
`1.41.1`. It exports that revision as the resource attribute
`telemetry.semconv.revision`. The Rust OTEL SDK and OTLP protocol libraries are
separately pinned in `Cargo.lock` to the 0.32 series.

Spark Signals keeps the source metric name when it exactly matches the pinned
system convention, including:

| Metric | Instrument | Unit | Required attributes |
| --- | --- | --- | --- |
| `system.cpu.utilization` | gauge | `1` | aggregate host CPU when no logical CPU attribute exists |
| `system.filesystem.usage` | gauge | `By` | `mountpoint`, `state` |
| `system.filesystem.limit` | gauge | `By` | `mountpoint` |
| `system.disk.io` | counter delta | `By` | `device`, `direction` |
| `system.disk.operation_time` | counter delta | `ms` | `device`, `direction` |
| `system.network.io` | counter delta | `By` | interface and direction |
| `system.network.packet.count` | counter delta | `{packet}` | interface and direction |
| `system.network.packet.dropped` | counter delta | `{packet}` | interface and direction |
| `system.network.errors` | counter delta | `{error}` | interface and direction |
| `system.uptime` | gauge | `s` | none |

Linux-specific facts whose pinned semantics match are retained under names such
as `system.memory.linux.available`. Metrics with no exact standard meaning use
the `spark.*` or `nvidia.*` namespaces. Source names are not silently rewritten
to a similar but different standard instrument.

Every recorded data point adds `host.name`, `host.id`, `spark.site`,
`spark.node.id`, `spark.signal.boot_id`, `measurement.quality`, and
`measurement.source`. Source-controlled point attributes are validated against
the finite v1 attribute catalogue before any OTEL instrument is created.

Health events become OTEL logs with the host and Spark identifiers, boot ID,
sequence, observation time, domain, code, and severity. Unavailable metric
values are logged with quality and error code rather than recorded as zero. The
unavailable log uses real OTEL attributes for the metric name, validated v1
point attributes, measurement source and quality, error code, and node identity.
Unsupported capability state is informational and deduplicated until the same
capability is measured again; transient collection failures remain error logs.
Unknown point-attribute keys and unrecognized measurement sources are never
copied into a log record.

OTLP/HTTP uses protobuf at `/v1/metrics` and `/v1/logs`. The development mock
path supports standard `OTEL_EXPORTER_OTLP_*` environment variables and
`deploy/test-otel.sh` verifies header injection, both signal paths, receiver
outage isolation, and recovery.

Production Maple mode instead takes `--maple-credential` and validates
`srvmini2-maple-otlp-client/v1`. The credential endpoint is authoritative and
the bridge constructs the two signal URLs itself. Any general, metrics, or logs
OTLP header environment variable makes secure mode fail closed so it cannot
override the credential-derived Basic authorization header.
