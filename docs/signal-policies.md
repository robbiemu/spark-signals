# Signal emission policies

The agent loads every `.toml` file in `signal_policies_dir` in lexical path order. Files are strictly parsed, and metric and attribute names must exist in the finite v1 schema catalogue. Sources are limited to the agent's known collectors. Invalid configuration rejects startup or the complete SIGHUP reload; a failed reload preserves the last valid policy and state.

Each rule contains `source`, `metric`, optional `attributes`, `policy`, and `reason`. Attribute selectors are subsets: an empty map applies to every series for the source and metric, while a rule with more attributes is more specific and takes precedence. Equal-specificity selectors that could match the same point are rejected rather than resolved by file order.

```toml
[[signal_policy]]
source = "nvml"
metric = "nvidia.gpu.clock.frequency"
attributes = { "clock.domain" = "memory" }
policy = "availability-change"
reason = "SUPPRESS_REPEATED_UNAVAILABLE"
```

Policies have these meanings:

- `enabled`: emit every collected observation. This is the default.
- `disabled`: omit matching observations from emitted metric batches.
- `state-change`: emit the first observation and then only changes to the value, quality, or error code for that complete metric series.
- `availability-change`: emit every available observation, emit the first unavailable observation, suppress repeated unavailable observations, and emit again on recovery.

The policy layer runs in the emitter, before serialization or publication to NATS. It is not an OTEL subscriber filter. General policies do not replace hardware capability profiles: hardware `disabled` rules prevent the underlying unsupported property call, while signal policies control emission of points returned by collectors.

## Timed discovery

`--discover-signals` runs the normal collectors for a bounded period, writes a policy file, and exits. Startup jitter is skipped and stdout emission is quiet unless `--stdout` is explicitly supplied. The duration defaults to `1d` and accepts `s`, `m`, `h`, or `d`. `--discovery-output` selects a non-default TOML output file.

```text
spark-agent --config /etc/spark-signals/agent.toml \
  --discover-signals \
  --discovery-duration 1d \
  --discovery-output /var/lib/spark-signals/discovered-signal-policies.toml
```

Discovery observes raw collector points before general signal-policy suppression. Each complete source, metric, and attribute key is classified as:

- `valid/changing`: at least two distinct successful values; omitted from the generated file.
- `valid/unchanging`: at least one successful value, but only one distinct value; generated as `disabled` with reason `DISCOVERY_VALID_UNCHANGING`.
- `invalid`: no measured, derived, or estimated value during the complete window; generated as `disabled` with reason `DISCOVERY_INVALID`.

A transient failure does not make a series invalid after any successful observation. The output is written atomically with mode `0640` and is directly loadable as a signal-policy file. The production unit provisions `/var/lib/spark-signals` as agent-writable state; `/etc/spark-signals/signal-policies` remains root-controlled. Review and install a generated file with:

```text
sudo install -o root -g root -m 0644 \
  /var/lib/spark-signals/discovered-signal-policies.toml \
  /etc/spark-signals/signal-policies/discovered.toml
sudo systemctl reload spark-agent.service
```
