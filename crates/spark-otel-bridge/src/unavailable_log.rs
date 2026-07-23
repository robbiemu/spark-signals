use std::collections::{BTreeMap, BTreeSet};

use opentelemetry::{
    Key,
    logs::{AnyValue, LogRecord as _, Logger as _, LoggerProvider as _, Severity as OtelSeverity},
};
use opentelemetry_sdk::logs::{SdkLogger, SdkLoggerProvider};
use spark_schema::{
    Envelope, MAX_ATTRIBUTE_VALUE_BYTES, MetricPoint, Quality, attribute_name_allowed,
    metric_name_allowed,
};

pub(crate) struct UnavailableLogEmitter {
    logger: SdkLogger,
    unavailable_capabilities: BTreeSet<UnavailableCapabilityKey>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct UnavailableCapabilityKey {
    metric: String,
    source: String,
    attributes: BTreeMap<String, String>,
}

struct UnavailableLog {
    severity: OtelSeverity,
    severity_text: &'static str,
    attributes: Vec<(Key, AnyValue)>,
}

impl UnavailableLogEmitter {
    pub(crate) fn new(logger_provider: &SdkLoggerProvider) -> Self {
        Self {
            logger: logger_provider.logger("spark-otel-bridge.metrics"),
            unavailable_capabilities: BTreeSet::new(),
        }
    }

    pub(crate) fn emit(&mut self, envelope: &Envelope, point: &MetricPoint) {
        if !self.observe_unavailable(point) {
            return;
        }
        if let Some(log) = convert(envelope, point) {
            let mut record = self.logger.create_log_record();
            record.set_event_name("spark.metric.unavailable");
            record.set_target("spark-otel-bridge");
            record.set_severity_number(log.severity);
            record.set_severity_text(log.severity_text);
            record.set_body(AnyValue::from("metric unavailable"));
            record.add_attributes(log.attributes);
            self.logger.emit(record);
        }
    }

    fn observe_unavailable(&mut self, point: &MetricPoint) -> bool {
        self.unavailable_capabilities.insert(capability_key(point))
    }

    pub(crate) fn observe_available(&mut self, point: &MetricPoint) {
        self.unavailable_capabilities.remove(&capability_key(point));
    }
}

fn capability_key(point: &MetricPoint) -> UnavailableCapabilityKey {
    UnavailableCapabilityKey {
        metric: point.name.clone(),
        source: point.source.clone(),
        attributes: point.attributes.clone(),
    }
}

fn convert(envelope: &Envelope, point: &MetricPoint) -> Option<UnavailableLog> {
    if !metric_name_allowed(&point.name)
        || !source_allowed(&point.source)
        || !safe_identifier(&envelope.node.site)
        || !safe_identifier(&envelope.node.id)
        || !safe_identifier(&envelope.node.host_name)
        || !safe_identifier(&envelope.boot_id)
    {
        return None;
    }
    let error_code = point.error_code.as_deref().filter(|code| {
        !code.is_empty()
            && code.len() <= MAX_ATTRIBUTE_VALUE_BYTES
            && code
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
    })?;
    let (severity, severity_text) = match point.quality {
        Quality::Unsupported => (OtelSeverity::Info, "INFO"),
        Quality::Error => (OtelSeverity::Error, "ERROR"),
        Quality::Stale | Quality::Measured | Quality::Derived | Quality::Estimated => {
            (OtelSeverity::Warn, "WARN")
        }
    };
    let mut attributes = vec![
        (Key::new("metric.name"), AnyValue::from(point.name.clone())),
        (
            Key::new("measurement.quality"),
            AnyValue::from(quality_name(&point.quality)),
        ),
        (
            Key::new("measurement.source"),
            AnyValue::from(point.source.clone()),
        ),
        (
            Key::new("error.code"),
            AnyValue::from(error_code.to_owned()),
        ),
        (
            Key::new("host.name"),
            AnyValue::from(envelope.node.host_name.clone()),
        ),
        (
            Key::new("host.id"),
            AnyValue::from(envelope.node.id.clone()),
        ),
        (
            Key::new("spark.site"),
            AnyValue::from(envelope.node.site.clone()),
        ),
        (
            Key::new("spark.node.id"),
            AnyValue::from(envelope.node.id.clone()),
        ),
        (
            Key::new("spark.signal.boot_id"),
            AnyValue::from(envelope.boot_id.clone()),
        ),
        (
            Key::new("spark.signal.sequence"),
            AnyValue::from(i64::try_from(envelope.sequence).unwrap_or(i64::MAX)),
        ),
    ];
    attributes.extend(
        point
            .attributes
            .iter()
            .filter(|(key, value)| {
                attribute_name_allowed(key)
                    && !value.is_empty()
                    && value.len() <= MAX_ATTRIBUTE_VALUE_BYTES
            })
            .map(|(key, value)| (Key::new(key.clone()), AnyValue::from(value.clone()))),
    );
    Some(UnavailableLog {
        severity,
        severity_text,
        attributes,
    })
}

fn source_allowed(source: &str) -> bool {
    matches!(
        source,
        "agent"
            | "cgroupfs"
            | "config"
            | "http"
            | "nvidia-smi"
            | "nvml"
            | "procfs"
            | "prometheus"
            | "sysfs"
            | "systemd"
    )
}

fn safe_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_ATTRIBUTE_VALUE_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b':'))
}

const fn quality_name(quality: &Quality) -> &'static str {
    match quality {
        Quality::Measured => "measured",
        Quality::Derived => "derived",
        Quality::Estimated => "estimated",
        Quality::Unsupported => "unsupported",
        Quality::Error => "error",
        Quality::Stale => "stale",
    }
}

#[cfg(test)]
mod tests {
    use spark_schema::{Node, SCHEMA_V1, Signal};

    use super::*;

    const TEST_ENV: &str = include_str!("../../../.env.test");

    fn test_env(name: &str) -> &'static str {
        TEST_ENV
            .lines()
            .filter_map(|line| line.split_once('='))
            .find_map(|(key, value)| (key == name).then_some(value))
            .unwrap_or_else(|| panic!("missing test environment value: {name}"))
    }

    fn fixture_envelope() -> Envelope {
        Envelope {
            schema: SCHEMA_V1.to_owned(),
            node: Node {
                site: test_env("SPARK_TEST_SITE").to_owned(),
                id: test_env("SPARK_TEST_NODE").to_owned(),
                host_name: test_env("SPARK_TEST_NODE").to_owned(),
            },
            boot_id: "00000000-0000-0000-0000-000000000000".to_owned(),
            sequence: 42,
            observed_at: "2026-07-21T00:00:00Z".to_owned(),
            monotonic_ns: 1,
            collection_duration_ms: 1,
            valid_for_ms: 6_000,
            signal: Signal::MetricBatch { points: Vec::new() },
        }
    }

    fn unavailable_memory_clock() -> MetricPoint {
        MetricPoint::unavailable(
            "nvidia.gpu.clock.frequency",
            "MHz",
            Quality::Unsupported,
            "nvml",
            "NVML_NOTSUPPORTED",
        )
        .with_attribute("clock.domain", "memory")
        .with_attribute("gpu.id", "GPU-GB10-FIXTURE")
        .with_attribute("gpu.index", "0")
    }

    fn string_attribute<'a>(log: &'a UnavailableLog, name: &str) -> Option<&'a str> {
        log.attributes.iter().find_map(|(key, value)| {
            (key.as_str() == name).then_some(value).and_then(|value| {
                if let AnyValue::String(value) = value {
                    Some(value.as_str())
                } else {
                    None
                }
            })
        })
    }

    #[test]
    fn preserves_validated_metric_identity_attributes() {
        let log = convert(&fixture_envelope(), &unavailable_memory_clock()).unwrap();
        assert_eq!(log.severity, OtelSeverity::Info);
        assert_eq!(
            string_attribute(&log, "metric.name"),
            Some("nvidia.gpu.clock.frequency")
        );
        assert_eq!(string_attribute(&log, "clock.domain"), Some("memory"));
        assert_eq!(string_attribute(&log, "gpu.id"), Some("GPU-GB10-FIXTURE"));
        assert_eq!(string_attribute(&log, "gpu.index"), Some("0"));
        assert_eq!(
            string_attribute(&log, "measurement.quality"),
            Some("unsupported")
        );
        assert_eq!(string_attribute(&log, "measurement.source"), Some("nvml"));
        assert_eq!(
            string_attribute(&log, "error.code"),
            Some("NVML_NOTSUPPORTED")
        );
        assert_eq!(
            string_attribute(&log, "spark.node.id"),
            Some(test_env("SPARK_TEST_NODE"))
        );
    }

    #[test]
    fn rejects_unvalidated_attributes_and_sources() {
        let mut point = unavailable_memory_clock();
        point
            .attributes
            .insert("secret.prompt".to_owned(), "do not export".to_owned());
        let log = convert(&fixture_envelope(), &point).unwrap();
        assert!(
            log.attributes
                .iter()
                .all(|(key, _)| key.as_str() != "secret.prompt")
        );
        point.source = "arbitrary-secret-source".to_owned();
        assert!(convert(&fixture_envelope(), &point).is_none());
    }

    #[test]
    fn operational_unavailable_metrics_remain_errors() {
        let mut point = unavailable_memory_clock();
        point.quality = Quality::Error;
        point.error_code = Some("NVML_TIMEOUT".to_owned());
        let log = convert(&fixture_envelope(), &point).unwrap();
        assert_eq!(log.severity, OtelSeverity::Error);
    }

    #[test]
    fn repeated_failures_are_suppressed_until_recovery() {
        let provider = SdkLoggerProvider::builder().build();
        let mut emitter = UnavailableLogEmitter::new(&provider);
        let mut point = unavailable_memory_clock();
        point.quality = Quality::Error;
        point.error_code = Some("NVML_TIMEOUT".to_owned());

        assert!(emitter.observe_unavailable(&point));

        point.error_code = Some("NVML_GPU_LOST".to_owned());
        assert!(!emitter.observe_unavailable(&point));

        emitter.observe_available(&point);
        assert!(emitter.observe_unavailable(&point));
    }
}
