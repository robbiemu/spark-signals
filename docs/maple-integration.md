# Maple integration

The production bridge consumes the managed producer identity `spark-signals`
and exports OTLP over HTTP/protobuf to the credential's approved base endpoint.
It exports metrics and logs; Maple's onboarding trace is an identity-controller
verification and does not imply that Spark Signals emits traces.

The credential must be delivered to the Spark host at:

```text
/etc/srvmini2/spark-signals/maple-otlp-client.json
```

It must remain a regular root-owned mode-`0600` file. The directory chain must
be root-owned and not group- or world-writable. Do not copy its contents through
chat, logs, argv, environment variables, repository files, or a user-readable
temporary file.

At startup the bridge:

1. opens the final path with `O_NOFOLLOW`;
2. validates file ownership, mode, type, and size;
3. rejects unknown JSON fields and validates the exact schema, producer,
   endpoint, protocol, and managed username form;
4. derives `/v1/metrics` and `/v1/logs` from the approved base endpoint;
5. constructs the Basic authorization header only in memory;
6. clears supplementary groups and changes permanently to
   `spark-signals-bridge`; and
7. initializes the OTEL exporters and NATS subscriber after the privilege drop.

The system installer enables the bridge only when this credential exists and
the process remains active after validation. The root-owned credential's base
endpoint is authoritative. Secure mode rejects OTLP endpoint, protocol, and
header environment variables so they cannot override it.

Acceptance was completed on 2026-07-20: Maple returned more than 66 Spark metric
names and Spark bridge logs carrying `host.id=spark-885a`, while traces remained
empty as designed. The deployment required operator-managed name resolution for
the Maple endpoint. Absence of exporter errors alone is not sufficient if this
acceptance test is repeated after a deployment change.
