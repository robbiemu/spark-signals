use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

pub const SCHEMA_V1: &str = "spark.signal/v1";

pub const METRIC_CATALOGUE_V1: &[&str] = &[
    "nvidia.gpu.clock.frequency",
    "nvidia.gpu.decoder.utilization",
    "nvidia.gpu.encoder.utilization",
    "nvidia.gpu.memory_controller.utilization",
    "nvidia.gpu.power.draw",
    "nvidia.gpu.performance_state",
    "nvidia.gpu.process.memory.allocation",
    "nvidia.gpu.temperature",
    "nvidia.gpu.throttle",
    "nvidia.gpu.utilization",
    "nvidia.gpu.xid.count",
    "spark.agent.collection.duration",
    "spark.agent.collection.errors",
    "spark.agent.collector.age",
    "spark.agent.events.dropped",
    "spark.agent.nats.reconnects",
    "spark.llm.available",
    "spark.llm.collection.errors",
    "spark.llm.context.capacity",
    "spark.llm.requests.queued",
    "spark.llm.requests.running",
    "spark.llm.tokens.generation.rate",
    "spark.llm.tokens.input",
    "spark.llm.tokens.output",
    "spark.llm.tokens.prefill.rate",
    "spark.llm.response.age",
    "spark.llm.uptime",
    "spark.pressure.cpu.some",
    "spark.pressure.io.full",
    "spark.pressure.io.some",
    "spark.pressure.memory.full",
    "spark.pressure.memory.some",
    "spark.service.active",
    "spark.service.restarts",
    "spark.uma.allocatable_with_swap",
    "spark.uma.allocatable_without_swap",
    "system.cpu.context_switches",
    "system.cpu.frequency",
    "system.cpu.load_average.1m",
    "system.cpu.tasks.blocked",
    "system.cpu.tasks.runnable",
    "system.cpu.online",
    "system.cpu.utilization",
    "system.disk.io",
    "system.disk.operation.count",
    "system.disk.operation_time",
    "system.disk.queue_time",
    "system.filesystem.inodes",
    "system.filesystem.limit",
    "system.filesystem.read_only",
    "system.filesystem.usage",
    "system.memory.active",
    "system.memory.buffers",
    "system.memory.cached",
    "system.memory.cgroup.events",
    "system.memory.dirty",
    "system.memory.hugepages.free",
    "system.memory.hugepages.reserved",
    "system.memory.hugepages.total",
    "system.memory.inactive",
    "system.memory.linux.available",
    "system.memory.linux.free",
    "system.memory.linux.total",
    "system.memory.oom_kills",
    "system.memory.page_faults.major",
    "system.memory.paging",
    "system.memory.reclaim",
    "system.memory.slab.reclaimable",
    "system.memory.slab.unreclaimable",
    "system.memory.swap.free",
    "system.memory.swap.total",
    "system.memory.writeback",
    "system.network.errors",
    "system.network.carrier_changes",
    "system.network.io",
    "system.network.link.speed",
    "system.network.link.up",
    "system.network.packet.count",
    "system.network.packet.dropped",
    "system.temperature",
    "system.uptime",
];

pub const ATTRIBUTE_CATALOGUE_V1: &[&str] = &[
    "aggregation",
    "cgroup.memory.event",
    "cgroup.scope",
    "channel",
    "clock.domain",
    "collector.domain",
    "cpu.logical_number",
    "device",
    "direction",
    "gpu.id",
    "gpu.index",
    "filesystem.device",
    "filesystem.type",
    "llm.backend",
    "llm.endpoint.id",
    "llm.model.id",
    "mountpoint",
    "network.interface.name",
    "network.io.direction",
    "nvidia.performance_state",
    "nvidia.xid.code",
    "process.pid",
    "sensor",
    "state",
    "systemd.active_enter_timestamp_monotonic_us",
    "systemd.substate",
    "systemd.unit",
    "temperature.limit.critical_celsius",
    "temperature.limit.max_celsius",
];

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Quality {
    Measured,
    Derived,
    Estimated,
    Unsupported,
    Error,
    Stale,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InstrumentKind {
    Gauge,
    CounterDelta,
    HistogramObservation,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MetricPoint {
    pub name: String,
    pub instrument: InstrumentKind,
    pub value: Option<f64>,
    pub unit: String,
    pub quality: Quality,
    pub source: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub attributes: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
}

impl MetricPoint {
    /// Creates a v1 catalogue gauge.
    ///
    /// # Panics
    ///
    /// Panics when `name` is not in the finite v1 metric catalogue.
    #[must_use]
    pub fn gauge(name: &str, value: f64, unit: &str, quality: Quality, source: &str) -> Self {
        assert!(metric_name_allowed(name), "metric outside the v1 catalogue");
        Self {
            name: name.to_owned(),
            instrument: InstrumentKind::Gauge,
            value: Some(value),
            unit: unit.to_owned(),
            quality,
            source: source.to_owned(),
            attributes: BTreeMap::new(),
            error_code: None,
        }
    }

    /// Creates a measured counter delta from the v1 catalogue.
    ///
    /// # Panics
    ///
    /// Panics when `name` is not in the finite v1 metric catalogue.
    #[must_use]
    pub fn counter_delta(name: &str, value: f64, unit: &str, source: &str) -> Self {
        let mut point = Self::gauge(name, value, unit, Quality::Derived, source);
        point.instrument = InstrumentKind::CounterDelta;
        point
    }

    #[must_use]
    pub fn with_attribute(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.attributes.insert(key.into(), value.into());
        self
    }

    /// Creates an unavailable v1 catalogue gauge with a null value.
    ///
    /// # Panics
    ///
    /// Panics when `name` is not in the finite v1 metric catalogue.
    #[must_use]
    pub fn unavailable(name: &str, unit: &str, quality: Quality, source: &str, code: &str) -> Self {
        assert!(metric_name_allowed(name), "metric outside the v1 catalogue");
        Self {
            name: name.to_owned(),
            instrument: InstrumentKind::Gauge,
            value: None,
            unit: unit.to_owned(),
            quality,
            source: source.to_owned(),
            attributes: BTreeMap::new(),
            error_code: Some(code.to_owned()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Node {
    pub site: String,
    pub id: String,
    pub host_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Envelope {
    pub schema: String,
    pub node: Node,
    pub boot_id: String,
    pub sequence: u64,
    pub observed_at: String,
    pub monotonic_ns: u64,
    pub collection_duration_ms: u64,
    pub valid_for_ms: u64,
    #[serde(flatten)]
    pub signal: Signal,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Signal {
    MetricBatch {
        points: Vec<MetricPoint>,
    },
    Inventory {
        #[serde(default)]
        attributes: BTreeMap<String, String>,
    },
    AgentStatus {
        online: bool,
        version: String,
    },
    HealthEvent {
        severity: Severity,
        code: String,
        message: String,
        #[serde(default)]
        attributes: BTreeMap<String, String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    Info,
    Warning,
    Error,
    Critical,
}

#[must_use]
pub fn metric_name_allowed(name: &str) -> bool {
    METRIC_CATALOGUE_V1.contains(&name)
}

#[must_use]
pub fn attribute_name_allowed(name: &str) -> bool {
    ATTRIBUTE_CATALOGUE_V1.contains(&name)
}

pub const MAX_SIGNAL_BYTES: usize = 64 * 1024;
pub const MAX_ATTRIBUTES_PER_SIGNAL_ITEM: usize = 24;
pub const MAX_ATTRIBUTE_VALUE_BYTES: usize = 256;

/// Decodes and validates one v1 signal envelope.
///
/// # Errors
///
/// Returns [`DecodeError`] when the payload is oversized or malformed, uses a
/// different schema version, or contains a metric outside the v1 catalogue.
pub fn decode_v1(payload: &[u8]) -> Result<Envelope, DecodeError> {
    if payload.len() > MAX_SIGNAL_BYTES {
        return Err(DecodeError::Oversized);
    }
    let envelope: Envelope = serde_json::from_slice(payload).map_err(|_| DecodeError::Malformed)?;
    if envelope.schema != SCHEMA_V1 {
        return Err(DecodeError::UnsupportedSchema);
    }
    match &envelope.signal {
        Signal::MetricBatch { points } => {
            if points.iter().any(|point| !metric_name_allowed(&point.name)) {
                return Err(DecodeError::UnknownMetric);
            }
            if points
                .iter()
                .any(|point| !attributes_valid(&point.attributes))
            {
                return Err(DecodeError::UnknownAttribute);
            }
        }
        Signal::HealthEvent { attributes, .. } if !attributes_valid(attributes) => {
            return Err(DecodeError::UnknownAttribute);
        }
        _ => {}
    }
    Ok(envelope)
}

fn attributes_valid(attributes: &BTreeMap<String, String>) -> bool {
    attributes.len() <= MAX_ATTRIBUTES_PER_SIGNAL_ITEM
        && attributes.iter().all(|(key, value)| {
            attribute_name_allowed(key) && value.len() <= MAX_ATTRIBUTE_VALUE_BYTES
        })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodeError {
    Oversized,
    Malformed,
    UnsupportedSchema,
    UnknownMetric,
    UnknownAttribute,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unavailable_value_serializes_as_null() {
        let point = MetricPoint::unavailable(
            "system.cpu.utilization",
            "1",
            Quality::Stale,
            "procfs",
            "BASELINE_INITIALIZING",
        );
        let value = serde_json::to_value(point).unwrap();
        assert!(value["value"].is_null());
        assert_eq!(value["quality"], "stale");
    }

    #[test]
    fn rejects_oversized_messages_before_decoding() {
        assert_eq!(
            decode_v1(&vec![b' '; MAX_SIGNAL_BYTES + 1]),
            Err(DecodeError::Oversized)
        );
    }

    #[test]
    fn rejects_unknown_metric_attributes() {
        let mut point = MetricPoint::gauge(
            "system.cpu.utilization",
            0.5,
            "1",
            Quality::Measured,
            "fixture",
        );
        point
            .attributes
            .insert("unbounded.label".to_owned(), "x".to_owned());
        let envelope = Envelope {
            schema: SCHEMA_V1.to_owned(),
            node: Node {
                site: "test".to_owned(),
                id: "node".to_owned(),
                host_name: "node".to_owned(),
            },
            boot_id: "fixture".to_owned(),
            sequence: 1,
            observed_at: "2026-07-19T00:00:00Z".to_owned(),
            monotonic_ns: 1,
            collection_duration_ms: 1,
            valid_for_ms: 1,
            signal: Signal::MetricBatch {
                points: vec![point],
            },
        };
        assert_eq!(
            decode_v1(&serde_json::to_vec(&envelope).unwrap()),
            Err(DecodeError::UnknownAttribute)
        );
    }
}
