# Security model

The agent runs as an unprivileged systemd service, opens no listening socket,
and reads ordinary local kernel and NVIDIA interfaces. It publishes only to its
own node subject. The OTEL bridge has the inverse NATS permission: subscribe to
Spark Signals and publish nothing.

Deployment secrets live under ignored `deploy/runtime/` files or external
credential/header files. They are supplied through environment variables or
NATS credential files and must not be placed in command-line arguments, TOML,
logs, signals, or commits. TLS CA files are supported for NATS and LLM probes.

The example broker binds to loopback. Exposing NATS to another host requires TLS
and a deliberate firewall change. Username/password ACLs are suitable for the
single-host prototype; per-node NKeys or operator JWTs are the production
upgrade when several hosts share a broker.

The user service is enabled with systemd. For it to start before the first user
login after boot, an administrator must run `sudo loginctl enable-linger
robbie` once.

Model probes collect only endpoint availability and numeric operational
metrics. The agent does not collect prompts, responses, model paths, API keys,
process command lines, or unconstrained Prometheus labels.
