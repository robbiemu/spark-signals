# Hardware capability profiles

The agent reads every `.toml` file in its configured hardware-capability directory in lexical path order. File order is only for deterministic validation; it is never a precedence mechanism.

Each profile has one or more hardware selectors and one or more capability rules. A selector may use the exact NVML GPU product name, the stable combined PCI device/vendor identifier, or both. All configured selectors must match at least one discovered GPU identity. A selector containing both fields is more specific than a selector containing one field.

Rules are keyed by source, v1 metric name, and a normalized map of v1 metric attributes. Metric and attribute names must exist in the finite v1 schema catalogue. This implementation intentionally accepts only the `nvml` source. Unknown fields, sources, metrics, attributes, empty profiles, and duplicate profile IDs are rejected.

When matching profiles define the same capability key, the most-specific selector wins. Equal-specificity matches are ambiguous and reject the complete reload instead of being resolved by file order. A failed SIGHUP reload leaves the last valid agent and capability configuration active.

Policies have these meanings:

- `disabled`: never make the underlying collector call and record the capability and reason in bounded inventory state.
- `enabled`: make the call even if a previous automatic probe found it unsupported, allowing an operator to force a reprobe after an update.
- `auto`: probe until NVML definitively returns `NotSupported`, then cache that state for the current GPU/driver lifecycle. Timeouts, permission errors, uninitialized NVML, GPU loss, and all other errors remain operational errors and are retried.

The automatic cache is cleared on configuration reload, process restart, or a detected GPU product, PCI device ID, or NVIDIA driver-version change.
