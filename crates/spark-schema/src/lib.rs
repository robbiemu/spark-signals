use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

pub const SCHEMA_V1: &str = "spark.signal/v1";

pub const METRIC_CATALOGUE_V1: &[&str] = &[
    "system.cpu.load_average.1m",
    "system.cpu.utilization",
    "system.memory.linux.available",
    "system.memory.linux.total",
    "system.memory.linux.free",
    "system.memory.cached",
    "system.memory.buffers",
    "system.memory.swap.free",
    "system.memory.swap.total",
    "system.uptime",
    "spark.pressure.cpu.some",
    "spark.pressure.memory.full",
    "spark.pressure.memory.some",
    "spark.uma.allocatable_with_swap",
    "spark.uma.allocatable_without_swap",
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
    MetricBatch { points: Vec<MetricPoint> },
}

#[must_use]
pub fn metric_name_allowed(name: &str) -> bool {
    METRIC_CATALOGUE_V1.contains(&name)
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
}
