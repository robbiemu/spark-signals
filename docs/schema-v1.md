# Spark Signals schema v1

Every NATS payload is one self-contained JSON object with `schema` equal to
`spark.signal/v1`. The envelope includes node identity, boot ID, a per-process
sequence, wall and monotonic observation times, collection duration, and the
period for which the signal remains valid.

The `kind` field selects one of four payloads: `metric_batch`, `inventory`,
`agent_status`, or `health_event`. Metric points contain a catalogue name,
instrument kind, unit, source, attributes, and quality. A missing measurement
has `value: null` with `unsupported`, `error`, or `stale`; it is never encoded as
a plausible zero.

Consumers must reject payloads larger than 64 KiB, unsupported schema values,
malformed JSON, and metric names outside the finite v1 catalogue. NATS subjects
have the form:

```text
spark.v1.<site>.<node>.status.agent
spark.v1.<site>.<node>.inventory
spark.v1.<site>.<node>.sample.system
spark.v1.<site>.<node>.sample.network
spark.v1.<site>.<node>.sample.storage
spark.v1.<site>.<node>.sample.nvidia
spark.v1.<site>.<node>.sample.service
spark.v1.<site>.<node>.sample.llm
spark.v1.<site>.<node>.event.health
```

Metric points and health events may contain at most 24 attributes; each value
is limited to 256 bytes and each key must be in the finite attribute catalogue.
See [metric-catalogue.md](metric-catalogue.md) for the complete v1 names.

On connection and reconnection, the publisher sends its complete latest state
in sequence order. Routine state coalesces while disconnected; health events
use a separate bounded FIFO and Core NATS does not make them durable.

The deployed Core NATS listener is TCP-only and loopback-bound. Browser
subscriptions are not currently exposed: choosing authenticated direct NATS
WebSocket access or a thin gateway is a Phase 6 decision.
