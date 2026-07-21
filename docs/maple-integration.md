# Maple target plugin

Maple is one built-in authenticated OTLP target profile. See
[OTLP target plugins](otel-target-plugins.md) for the backend-neutral bridge
boundary and standard target.

The production bridge consumes the managed producer identity `spark-signals`
and exports OTLP over HTTP/protobuf to the credential's approved base endpoint.
It exports metrics and logs; Maple's onboarding trace is an identity-controller
verification and does not imply that Spark Signals emits traces.

The ignored `deploy/runtime/bridge.env` supplies the deployment-specific
contract without exposing its path or namespace in the public repository:

```dotenv
SPARK_OTEL_TARGET=maple
SPARK_OTEL_MAPLE_CREDENTIAL=/absolute/path/to/credential.json
SPARK_OTEL_MAPLE_CREDENTIAL_SCHEMA=deployment-supplied-schema
SPARK_OTEL_MAPLE_PRODUCER=deployment-supplied-producer
```

`SPARK_OTEL_MAPLE_CREDENTIAL` must name a regular root-owned mode-`0600` file. Its
directory chain must
be root-owned and not group- or world-writable. Do not copy its contents through
chat, logs, argv, environment variables, repository files, or a user-readable
temporary file.

At startup the bridge:

1. opens the final path with `O_NOFOLLOW`;
2. validates file ownership, mode, type, and size;
3. rejects unknown JSON fields and validates the configured schema, producer,
   endpoint, protocol, and managed username form;
4. derives `/v1/metrics` and `/v1/logs` from the approved base endpoint;
5. constructs the Basic authorization header only in memory;
6. clears supplementary groups and changes permanently to
   `spark-signals-bridge`; and
7. initializes the OTEL exporters and NATS subscriber after the privilege drop.

The system installer enables the bridge only when the protected bridge
environment exists, `--validate-config` succeeds, and the process remains
active. The root-owned credential's base
endpoint is authoritative. Secure mode rejects OTLP endpoint, protocol, and
header environment variables so they cannot override it.

Acceptance was completed on 2026-07-20: Maple returned more than 66 Spark metric
names and Spark bridge logs carrying the expected `host.id`, while traces remained
empty as designed. The deployment required operator-managed name resolution for
the Maple endpoint. Absence of exporter errors alone is not sufficient if this
acceptance test is repeated after a deployment change.
