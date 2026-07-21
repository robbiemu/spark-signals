# OTLP target plugins

`spark-otel-bridge` is a backend-neutral NATS-to-OpenTelemetry bridge. Built-in
target plugins adapt its OTLP exporters to a destination at compile time; they
do not change the Spark signal format or introduce a telemetry protocol.

The bridge core owns NATS connectivity and buffering, signal decoding and OTEL
translation, resource attributes, provider lifecycle, privilege drop, and
plugin selection. An `OtlpTargetPlugin` owns target-specific configuration,
pre-drop credential access, validation, endpoint and header construction,
override policy, and safe diagnostics. Preparation returns only a
`PreparedOtlpTarget`: optional metrics and logs endpoints, HTTP headers, OTLP
protocol, plugin name, and non-secret metadata. Exporter code therefore has no
Maple types or branches.

## Selecting and configuring a target

Set `SPARK_OTEL_TARGET` or pass `--otel-target`. The default is `standard`.

```dotenv
SPARK_OTEL_TARGET=standard
OTEL_EXPORTER_OTLP_ENDPOINT=http://collector.invalid:4318
OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
```

The `standard` plugin adds no overrides and leaves ordinary
`OTEL_EXPORTER_OTLP_*` processing to the OpenTelemetry SDK. It needs no root
credential and fits development, tests, and conventional OTLP collectors.

```dotenv
SPARK_OTEL_TARGET=maple
SPARK_OTEL_MAPLE_CREDENTIAL=/absolute/path/to/credential.json
SPARK_OTEL_MAPLE_CREDENTIAL_SCHEMA=deployment-supplied-schema
SPARK_OTEL_MAPLE_PRODUCER=deployment-supplied-producer
```

The `maple` plugin securely prepares an authenticated target before privilege
drop. Its contract is in [Maple integration](maple-integration.md). Unknown
targets, incomplete settings, or settings for the wrong plugin fail closed.
`--validate-config` prepares and validates without starting exporters or NATS.

## Future target candidates

Grafana Cloud, Honeycomb, New Relic, and Datadog are candidates for future
built-in target plugins. All support OpenTelemetry workflows, so evaluation
should begin with the `standard` plugin rather than assuming provider-specific
code is necessary. Add a dedicated plugin only when it provides a concrete
benefit such as protected credential-file loading, provider-specific endpoint
derivation and validation, safer authentication-header construction, or strict
rejection of conflicting environment overrides.

Self-hosted Grafana components behind an OpenTelemetry Collector or Grafana
Alloy should normally remain a `standard` target: the collector is the backend
adapter in that topology. Before promoting any candidate to a built-in plugin,
document its metrics-and-logs support, authentication lifecycle, configuration
migration, interoperability tests, and operational ownership.

## Adding a built-in target

Add an isolated module under `crates/spark-otel-bridge/src/plugins`, implement
`OtlpTargetPlugin`, add its namespaced options to `TargetOptions`, and register
its name in `plugins::select`. Return a backend-neutral `PreparedOtlpTarget`;
do not modify NATS, translation, or provider-lifecycle code. Add selection,
validation, preparation, and deployment-migration tests and documentation.

Plugins are compiled into the bridge. Dynamic shared-library loading is
intentionally deferred: a stable binary ABI and arbitrary runtime code-loading
policy would add compatibility and security obligations this interface does
not need.
