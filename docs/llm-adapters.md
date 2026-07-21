# LLM service adapters

`spark-agent` observes model servers through explicitly configured endpoints. It
does not inspect process command lines, model paths, request prompts, or API
payloads. An endpoint remains represented while it is stopped: the agent emits
`spark.llm.available` with no numeric value, `quality=error`, and an error code.

Built-in Prometheus mappings are provided for SGLang, vLLM, and llama.cpp.
SGLang needs `--enable-metrics`. The OpenAI-compatible adapter performs an
authenticated `GET /v1/models` availability check but cannot infer queue or
token metrics from that API. The `custom` backend maps any Prometheus names to
the finite Spark Signals LLM catalogue, so another engine can be integrated by
configuration instead of an agent rebuild.

Each `[[llm]]` entry has a stable, non-secret `id`, a `backend`, and a loopback
or HTTPS `base_url`. Optional fields are:

- `metrics_path` (default `/metrics`)
- `auth_header_file`, whose entire content is one `Name: value` header
- `tls_ca_file`, a PEM CA certificate for a private HTTPS endpoint
- `served_model_id`, an explicitly configured public identifier (never a path)
- `context_capacity`, the configured context capacity in tokens
- `[llm.metric_names]` overrides for `running`, `queued`, `input_tokens`,
  `output_tokens`, and `generation_rate`
  (plus optional `uptime`)

Keep header files outside the repository, mode `0600`, and readable only by the
service account. Secrets are loaded once at startup and are never attached to a
signal. See `deploy/example-config/agent.toml` for built-in and custom examples.

Token totals are read as cumulative Prometheus counters. The agent publishes
counter deltas and derives rates from consecutive successful samples. The first
sample and a reset counter are reported as an initializing baseline rather than
zero. An engine-provided generation rate, when available, is also accepted.

All endpoints use a three-second timeout and are sampled on the agent's medium
cadence. A failed endpoint never blocks Linux or NVIDIA collection.
Each endpoint also reports collection-error state and time since its last
successful response.
