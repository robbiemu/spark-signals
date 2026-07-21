# spark-signals

Host-native telemetry for NVIDIA DGX Spark. Here, **agent** means the host-monitoring
process, not an LLM agent. `spark-agent` samples Linux, NVIDIA, configured systemd,
and model-server health, then publishes versioned signals to NATS Core.
`spark-otel-bridge` converts the same stream to OTLP/HTTP metrics and logs. JSON
Lines remains available for fixture validation and diagnostics.

This project takes collection lessons from
[MiaAI-Lab/sparkDash](https://github.com/MiaAI-Lab/sparkDash) while separating
measurement from presentation. In particular, missing or failed observations
remain absent and carry an explicit quality state; they are never converted to
plausible numeric zeroes.

## Current scope

The current prototype includes:

- a versioned JSON envelope and finite metric catalogue;
- Linux CPU, memory, PSI, network, block-I/O, filesystem, and temperature
  collectors;
- dynamically loaded NVML metrics with a named-field `nvidia-smi` fallback;
- opt-in configured systemd probes and SGLang, vLLM, llama.cpp,
  OpenAI-compatible, or custom model-server adapters;
- baseline-aware CPU utilization (the first reading is explicitly unavailable);
- bounded/coalescing NATS publishing with authentication, TLS, and reconnect
  state replay; and
- an OTLP/HTTP metrics and logs bridge with bounded input and schema validation.

See [ROADMAP.md](ROADMAP.md) for validation status and remaining work through
Phase 5. Phase 6 UI and operational hardening have intentionally not started.

For a start-to-finish local installation, see the
[user guide](docs/user-guide.md).

## Run

Print one observation:

```console
cargo run -p spark-agent -- --once --stdout --site example --node spark-node-01
```

Publish periodically to NATS (and also print for diagnostics):

```console
cargo run -p spark-agent -- \
  --nats-url nats://127.0.0.1:4222 \
  --stdout \
  --site example \
  --node spark-node-01
```

Observe configured services and LLM endpoints:

```console
cargo run -p spark-agent -- \
  --config deploy/example-config/agent.toml \
  --stdout --site example --node spark-node-01
```

When `--nats-url` is omitted, stdout output is enabled automatically. Subjects
are documented in [docs/schema-v1.md](docs/schema-v1.md). Site and node
components are restricted to ASCII letters, digits, `_`, and `-`.

Adapter details and custom metric mappings are in
[docs/llm-adapters.md](docs/llm-adapters.md). The example broker ACL and secret
handling model are described in [docs/security-model.md](docs/security-model.md).
Username/password and JWT/NKey deployment paths are documented in
[docs/nats-credentials.md](docs/nats-credentials.md).
The pinned OTEL conventions and instrument mapping are in
[docs/otel-mapping.md](docs/otel-mapping.md).
The bridge's compile-time target-plugin architecture and configuration are in
[docs/otel-target-plugins.md](docs/otel-target-plugins.md); Maple-specific
credential handling is in [docs/maple-integration.md](docs/maple-integration.md).
The finite metric and attribute names are listed in
[docs/metric-catalogue.md](docs/metric-catalogue.md).

Production deployment uses dedicated system identities and does not depend on a
login account or systemd lingering. Copy `deploy/example-config/agent.toml` to
ignored `deploy/runtime/agent.toml` and set the deployment-specific service
names and endpoints there. Build the release binaries, then run the root
installer with the repository path:

```console
cargo build --release --workspace
```

> [!IMPORTANT]
> Review scripts before running them with root privileges.

```console
sudo ./deploy/install-system.sh "$PWD"
```

When migrating an existing development user service, pass that login name as
the optional second argument so the installer can disable the legacy units.

The installer copies root-owned binaries and configuration out of the home
directory and installs the agent as `spark-signals-agent`. When a protected
`deploy/runtime/bridge.env` exists and its selected target passes the bridge's
non-exporting preflight, the installer also enables the bridge; otherwise that
unit remains disabled. Start from `deploy/example-config/bridge.env` for a
standard OTLP target. The `.user.service` files remain development-only examples.

## Validate

```console
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

On a deployed Linux host, `deploy/validate-host.sh` compares the agent's
reported `MemTotal` to `/proc/meminfo`, checks unavailable-value quality states,
and verifies that the agent service has no listening sockets.
`deploy/test-nats.sh` uses short-lived Docker containers to verify an
authenticated NATS Core publication and removes them when the check finishes.

## License

MIT
