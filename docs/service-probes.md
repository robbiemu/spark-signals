# Configured service probes

The `spark-agent` host-monitoring process observes only the system `systemd`
units explicitly listed in `agent.toml`. It does not discover, start, stop, or
restart services.

Add one `[[service]]` table for each unit:

```toml
[[service]]
name = "inference-primary.service"
```

Use the complete system unit name, including the `.service` suffix. Confirm the
name before adding it:

```bash
systemctl status inference-primary.service
```

This probe queries the system service manager, not a login user's
`systemctl --user` manager. A stopped unit is a valid target and is reported as
inactive. A missing unit or failed `systemctl show` query produces an unavailable
`spark.service.active` observation with error code `SYSTEMD_QUERY_FAILED`.

For each configured unit, the agent reports:

- `spark.service.active`, with the unit's substate and, when available, main PID
  and active-enter timestamp;
- `spark.service.restarts`; and
- cgroup memory OOM, reclaim, and related event deltas when the unit exposes a
  readable control group.

After the initial observation, an active/inactive transition also emits a
`SERVICE_STATE_CHANGED` health event. Initial cgroup counter deltas are
unavailable while the agent establishes a baseline.

See the [metric catalogue](metric-catalogue.md) for the finite service metric and
attribute names.
