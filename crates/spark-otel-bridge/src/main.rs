use std::{collections::HashMap, path::PathBuf};

use anyhow::{Context, Result};
use clap::Parser;
use futures_util::StreamExt;
use opentelemetry::{
    KeyValue, global,
    metrics::{Counter, Gauge, Histogram, Meter},
};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::{Protocol, WithExportConfig};
use opentelemetry_sdk::{Resource, logs::SdkLoggerProvider, metrics::SdkMeterProvider};
use spark_schema::{Envelope, InstrumentKind, MetricPoint, Quality, Severity, Signal, decode_v1};
use tokio::sync::mpsc;
use tracing_subscriber::{filter::LevelFilter, layer::SubscriberExt, util::SubscriberInitExt};

mod unavailable_log;

use unavailable_log::UnavailableLogEmitter;

const OTEL_SEMCONV_REVISION: &str = "1.41.1";

#[derive(Parser)]
#[command(about = "Translate Spark Signals from NATS to OTLP/HTTP")]
struct Args {
    #[arg(long, env = "NATS_URL", default_value = "nats://127.0.0.1:4222")]
    nats_url: String,
    #[arg(long, env = "NATS_SUBJECT", default_value = "spark.v1.>")]
    subject: String,
    #[arg(long, env = "NATS_CREDENTIALS")]
    nats_credentials: Option<PathBuf>,
    #[arg(long, env = "NATS_USER", requires = "nats_password")]
    nats_user: Option<String>,
    #[arg(long, env = "NATS_PASSWORD", requires = "nats_user")]
    nats_password: Option<String>,
    #[arg(long, env = "NATS_CA")]
    nats_ca: Option<PathBuf>,
}

struct Instruments {
    meter: Meter,
    unavailable_logs: UnavailableLogEmitter,
    gauges: HashMap<String, Gauge<f64>>,
    counters: HashMap<String, Counter<f64>>,
    histograms: HashMap<String, Histogram<f64>>,
}

impl Instruments {
    fn new(logger_provider: &SdkLoggerProvider) -> Self {
        Self {
            meter: global::meter("spark-otel-bridge"),
            unavailable_logs: UnavailableLogEmitter::new(logger_provider),
            gauges: HashMap::new(),
            counters: HashMap::new(),
            histograms: HashMap::new(),
        }
    }

    fn record(&mut self, envelope: &Envelope, point: &MetricPoint) {
        let Some(value) = point.value else {
            self.unavailable_logs.emit(envelope, point);
            return;
        };
        self.unavailable_logs.observe_available(point);
        let mut attributes = vec![
            KeyValue::new("host.name", envelope.node.host_name.clone()),
            KeyValue::new("host.id", envelope.node.id.clone()),
            KeyValue::new("spark.site", envelope.node.site.clone()),
            KeyValue::new("spark.node.id", envelope.node.id.clone()),
            KeyValue::new("spark.signal.boot_id", envelope.boot_id.clone()),
            KeyValue::new("measurement.quality", quality_name(&point.quality)),
            KeyValue::new("measurement.source", point.source.clone()),
        ];
        attributes.extend(
            point
                .attributes
                .iter()
                .map(|(key, value)| KeyValue::new(key.clone(), value.clone())),
        );
        match point.instrument {
            InstrumentKind::Gauge => self
                .gauges
                .entry(point.name.clone())
                .or_insert_with(|| {
                    self.meter
                        .f64_gauge(point.name.clone())
                        .with_unit(point.unit.clone())
                        .build()
                })
                .record(value, &attributes),
            InstrumentKind::CounterDelta => {
                if value >= 0.0 {
                    self.counters
                        .entry(point.name.clone())
                        .or_insert_with(|| {
                            self.meter
                                .f64_counter(point.name.clone())
                                .with_unit(point.unit.clone())
                                .build()
                        })
                        .add(value, &attributes);
                }
            }
            InstrumentKind::HistogramObservation => self
                .histograms
                .entry(point.name.clone())
                .or_insert_with(|| {
                    self.meter
                        .f64_histogram(point.name.clone())
                        .with_unit(point.unit.clone())
                        .build()
                })
                .record(value, &attributes),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let (meter_provider, logger_provider) = initialize_otel()?;
    let mut options = async_nats::ConnectOptions::new()
        .name("spark-otel-bridge")
        .max_reconnects(None);
    if let (Some(user), Some(password)) = (&args.nats_user, &args.nats_password) {
        options = options.user_and_password(user.clone(), password.clone());
    }
    if let Some(path) = &args.nats_credentials {
        options = options
            .credentials_file(path)
            .await
            .context("loading NATS credentials")?;
    }
    if let Some(path) = &args.nats_ca {
        options = options
            .add_root_certificates(path.clone())
            .require_tls(true);
    }
    let client = options
        .connect(&args.nats_url)
        .await
        .with_context(|| format!("connecting to {}", args.nats_url))?;
    let mut subscription = client
        .subscribe(args.subject.clone())
        .await
        .with_context(|| format!("subscribing to {}", args.subject))?;
    let (sender, mut receiver) = mpsc::channel(256);
    tokio::spawn(async move {
        while let Some(message) = subscription.next().await {
            if sender.try_send(message).is_err() {
                tracing::warn!("bounded NATS receive queue full; message dropped");
            }
        }
    });
    tracing::info!(nats.url = %args.nats_url, nats.subject = %args.subject, "Spark OTEL bridge started");
    let mut instruments = Instruments::new(&logger_provider);
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            message = receiver.recv() => {
                let Some(message) = message else { anyhow::bail!("NATS receive task ended"); };
                let envelope = match decode_v1(&message.payload) {
                    Ok(envelope) => envelope,
                    Err(error) => {
                        tracing::warn!(nats.subject = %message.subject, error = ?error, "discarding invalid Spark signal");
                        continue;
                    }
                };
                match &envelope.signal {
                    Signal::MetricBatch { points } => for point in points { instruments.record(&envelope, point); },
                    Signal::HealthEvent { severity, code, message, attributes } => emit_health(&envelope, severity, code, message, attributes),
                    Signal::Inventory { attributes } => tracing::info!(host.name = %envelope.node.host_name, spark.node.id = %envelope.node.id, inventory = %serde_json::to_string(attributes).unwrap_or_default(), "Spark inventory received"),
                    Signal::AgentStatus { online, version } => tracing::info!(host.name = %envelope.node.host_name, spark.node.id = %envelope.node.id, agent.online = online, agent.version = %version, "Spark agent status received"),
                }
            }
        }
    }
    client.flush().await?;
    meter_provider.shutdown()?;
    logger_provider.shutdown()?;
    Ok(())
}

fn initialize_otel() -> Result<(SdkMeterProvider, SdkLoggerProvider)> {
    let resource = Resource::builder()
        .with_service_name("spark-otel-bridge")
        .with_attribute(KeyValue::new(
            "telemetry.semconv.revision",
            OTEL_SEMCONV_REVISION,
        ))
        .build();
    let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpBinary)
        .build()
        .context("building OTLP metric exporter")?;
    let meter_provider = SdkMeterProvider::builder()
        .with_resource(resource.clone())
        .with_periodic_exporter(metric_exporter)
        .build();
    global::set_meter_provider(meter_provider.clone());
    let log_exporter = opentelemetry_otlp::LogExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpBinary)
        .build()
        .context("building OTLP log exporter")?;
    let logger_provider = SdkLoggerProvider::builder()
        .with_resource(resource)
        .with_batch_exporter(log_exporter)
        .build();
    tracing_subscriber::registry()
        .with(LevelFilter::INFO)
        .with(OpenTelemetryTracingBridge::new(&logger_provider))
        .with(tracing_subscriber::fmt::layer())
        .try_init()
        .context("initializing tracing")?;
    Ok((meter_provider, logger_provider))
}

fn emit_health(
    envelope: &Envelope,
    severity: &Severity,
    code: &str,
    message: &str,
    attributes: &std::collections::BTreeMap<String, String>,
) {
    let attributes = serde_json::to_string(attributes).unwrap_or_default();
    let domain = event_domain(code);
    match severity {
        Severity::Info => {
            tracing::info!(event.code = code, event.domain = domain, event.attributes = %attributes, host.name = %envelope.node.host_name, host.id = %envelope.node.id, spark.node.id = %envelope.node.id, spark.signal.boot_id = %envelope.boot_id, spark.signal.sequence = envelope.sequence, observed_at = %envelope.observed_at, "{message}");
        }
        Severity::Warning => {
            tracing::warn!(event.code = code, event.domain = domain, event.attributes = %attributes, host.name = %envelope.node.host_name, host.id = %envelope.node.id, spark.node.id = %envelope.node.id, spark.signal.boot_id = %envelope.boot_id, spark.signal.sequence = envelope.sequence, observed_at = %envelope.observed_at, "{message}");
        }
        Severity::Error | Severity::Critical => {
            tracing::error!(event.code = code, event.domain = domain, event.severity = ?severity, event.attributes = %attributes, host.name = %envelope.node.host_name, host.id = %envelope.node.id, spark.node.id = %envelope.node.id, spark.signal.boot_id = %envelope.boot_id, spark.signal.sequence = envelope.sequence, observed_at = %envelope.observed_at, "{message}");
        }
    }
}

fn event_domain(code: &str) -> &'static str {
    if code.starts_with("NVIDIA_") {
        "nvidia"
    } else if code.starts_with("SERVICE_") {
        "service"
    } else if code.starts_with("NATS_") {
        "nats"
    } else {
        "agent"
    }
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
    use super::*;

    #[test]
    fn classifies_health_event_domains() {
        assert_eq!(event_domain("NVIDIA_XID"), "nvidia");
        assert_eq!(event_domain("SERVICE_STATE_CHANGED"), "service");
        assert_eq!(event_domain("NATS_RECONNECTED"), "nats");
        assert_eq!(event_domain("COLLECTOR_DEGRADED"), "agent");
    }

    #[test]
    fn semantic_convention_revision_is_pinned() {
        assert_eq!(OTEL_SEMCONV_REVISION, "1.41.1");
    }
}
