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
