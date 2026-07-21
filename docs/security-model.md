# Security model

The agent runs as an unprivileged systemd service, opens no listening socket,
and reads ordinary local kernel and NVIDIA interfaces. It publishes only to its
own node subject. The OTEL bridge has the inverse NATS permission: subscribe to
Spark Signals and publish nothing.

Prototype secrets live under ignored `deploy/runtime/` files. The system
installer copies NATS environment files to `/etc/spark-signals` as root-owned,
mode-`0640` files readable only by the corresponding service group. Secrets
must not be placed in command-line arguments, TOML, logs, signals, or commits.
TLS CA and credential files are supported for NATS and LLM probes.

The example broker binds to loopback. Exposing NATS to another host requires TLS
and a deliberate firewall change. Username/password ACLs are suitable for the
single-host prototype; per-node NKeys or operator JWTs are the production
upgrade when several hosts share a broker.

Production uses separate `spark-signals-agent` and `spark-signals-bridge`
system accounts and units under `multi-user.target`. It has no runtime
dependency on the SSH/installing user and needs no systemd lingering. The
development user units are retained only for unprivileged iteration.

Maple authorization is not accepted from an environment file by the production
installer. The bridge opens the producer credential with `O_NOFOLLOW`, requires
a regular root-owned mode-`0600` file under a root-controlled directory chain,
limits its size, and validates its exact schema, producer, endpoint, protocol,
and username shape. It constructs the Basic header in memory, then permanently
drops root and supplementary groups before initializing OTEL or NATS. The
credential, password, and header are never placed in argv, the environment,
logs, or telemetry.

If a NATS password is exposed, `deploy/rotate-nats-bridge-password.sh` rotates
the bridge-only credential in the broker and both runtime files, recreates the
broker, restarts the bridge, and proves that the new credential succeeds while
the old credential is rejected. The rotation never prints either value and
rolls back files and services if verification fails.

Model probes collect only endpoint availability and numeric operational
metrics. The agent does not collect prompts, responses, model paths, API keys,
process command lines, or unconstrained Prometheus labels.
