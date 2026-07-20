# Metric catalogue v1

The `spark.signal/v1` contract accepts only the metric and attribute names in
this document. The executable source of truth is `METRIC_CATALOGUE_V1` and
`ATTRIBUTE_CATALOGUE_V1` in `spark-schema`; decoding rejects additions that do
not arrive under a new compatible schema revision.

Quality, source, unit, instrument kind, and availability are carried on every
metric point. A metric can therefore be present with a null value and an
`unsupported`, `error`, or `stale` quality instead of a false zero.

## Host, CPU, pressure, and temperature

```text
system.uptime
system.cpu.context_switches
system.cpu.frequency
system.cpu.load_average.1m
system.cpu.online
system.cpu.tasks.blocked
system.cpu.tasks.runnable
system.cpu.utilization
system.temperature
spark.pressure.cpu.some
spark.pressure.io.full
spark.pressure.io.some
spark.pressure.memory.full
spark.pressure.memory.some
```

## Unified memory

```text
system.memory.active
system.memory.buffers
system.memory.cached
system.memory.cgroup.events
system.memory.dirty
system.memory.hugepages.free
system.memory.hugepages.reserved
system.memory.hugepages.total
system.memory.inactive
system.memory.linux.available
system.memory.linux.free
system.memory.linux.total
system.memory.oom_kills
system.memory.page_faults.major
system.memory.paging
system.memory.reclaim
system.memory.slab.reclaimable
system.memory.slab.unreclaimable
system.memory.swap.free
system.memory.swap.total
system.memory.writeback
spark.uma.allocatable_with_swap
spark.uma.allocatable_without_swap
```

## Storage and network

```text
system.disk.io
system.disk.operation.count
system.disk.operation_time
system.disk.queue_time
system.filesystem.inodes
system.filesystem.limit
system.filesystem.read_only
system.filesystem.usage
system.network.carrier_changes
system.network.errors
system.network.io
system.network.link.speed
system.network.link.up
system.network.packet.count
system.network.packet.dropped
```

## NVIDIA

```text
nvidia.gpu.clock.frequency
nvidia.gpu.decoder.utilization
nvidia.gpu.encoder.utilization
nvidia.gpu.memory_controller.utilization
nvidia.gpu.performance_state
nvidia.gpu.power.draw
nvidia.gpu.process.memory.allocation
nvidia.gpu.temperature
nvidia.gpu.throttle
nvidia.gpu.utilization
nvidia.gpu.xid.count
```

The catalogue deliberately has no fabricated GB10 framebuffer-use or memory-
bandwidth measurement. Unified-memory and 273 GB/s facts belong to inventory;
unsupported NVML fields retain an explicit unavailable quality.

## Agent, configured services, and inference

```text
spark.agent.collection.duration
spark.agent.collection.errors
spark.agent.collector.age
spark.agent.events.dropped
spark.agent.nats.reconnects
spark.service.active
spark.service.restarts
spark.llm.available
spark.llm.collection.errors
spark.llm.context.capacity
spark.llm.requests.queued
spark.llm.requests.running
spark.llm.response.age
spark.llm.tokens.generation.rate
spark.llm.tokens.input
spark.llm.tokens.output
spark.llm.tokens.prefill.rate
spark.llm.uptime
```

## Attribute controls

At most 24 attributes may appear on one metric point or health event, and an
attribute value may contain at most 256 bytes. The finite v1 keys are:

```text
aggregation
cgroup.memory.event
cgroup.scope
channel
clock.domain
collector.domain
cpu.logical_number
device
direction
filesystem.device
filesystem.type
gpu.id
gpu.index
llm.backend
llm.endpoint.id
llm.model.id
mountpoint
network.interface.name
network.io.direction
nvidia.performance_state
nvidia.xid.code
process.pid
sensor
state
systemd.active_enter_timestamp_monotonic_us
systemd.substate
systemd.unit
temperature.limit.critical_celsius
temperature.limit.max_celsius
```

Model paths, prompts, responses, command lines, API keys, IP addresses, and
free-form metric labels are not part of this contract.
