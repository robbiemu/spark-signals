use std::{
    collections::HashMap,
    env,
    fs::File,
    io::Read,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use clap::Parser;
use futures_util::StreamExt;
#[cfg(target_os = "linux")]
use nix::unistd::{Gid, User, setgid, setgroups, setuid};
use nix::{
    fcntl::{OFlag, open},
    sys::stat::Mode,
    unistd::Uid,
};
use opentelemetry::{
    KeyValue, global,
    metrics::{Counter, Gauge, Histogram, Meter},
};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::{Protocol, WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::{Resource, logs::SdkLoggerProvider, metrics::SdkMeterProvider};
use serde::Deserialize;
use spark_schema::{Envelope, InstrumentKind, MetricPoint, Quality, Severity, Signal, decode_v1};
use tokio::sync::mpsc;
use tracing_subscriber::{filter::LevelFilter, layer::SubscriberExt, util::SubscriberInitExt};
use zeroize::{Zeroize, Zeroizing};

mod unavailable_log;

use unavailable_log::UnavailableLogEmitter;

const OTEL_SEMCONV_REVISION: &str = "1.41.1";
const MAPLE_CREDENTIAL_SCHEMA: &str = "srvmini2-maple-otlp-client/v1";
const MAPLE_PROTOCOL: &str = "http/protobuf";
const MAX_MAPLE_CREDENTIAL_BYTES: u64 = 16 * 1024;

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
    #[arg(long)]
    maple_credential: Option<PathBuf>,
    #[arg(long, default_value = "spark-signals")]
    maple_producer: String,
    #[arg(long, default_value = "spark-signals-bridge")]
    run_as_user: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MapleCredential {
    #[serde(rename = "$schema")]
    schema: String,
    endpoint: String,
    password: String,
    producer: String,
    protocol: String,
    username: String,
}

impl Drop for MapleCredential {
    fn drop(&mut self) {
        self.password.zeroize();
    }
}

struct MapleExporterConfig {
    metrics_endpoint: String,
    logs_endpoint: String,
    headers: HashMap<String, String>,
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
    let maple = args
        .maple_credential
        .as_deref()
        .map(|path| load_maple_exporter_config(path, &args.maple_producer))
        .transpose()?;
    if maple.is_some() {
        reject_otel_override_environment()?;
    }
    drop_root_privileges(&args.run_as_user)?;
    let (meter_provider, logger_provider) = initialize_otel(maple.as_ref())?;
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

fn load_maple_exporter_config(path: &Path, expected_producer: &str) -> Result<MapleExporterConfig> {
    if !path.is_absolute() {
        anyhow::bail!("Maple credential path must be absolute");
    }
    validate_root_owned_parent_chain(path)?;
    let descriptor = open(
        path,
        OFlag::O_RDONLY | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
        Mode::empty(),
    )
    .with_context(|| format!("opening Maple credential file {}", path.display()))?;
    let file = File::from(descriptor);
    let metadata = file
        .metadata()
        .context("reading Maple credential metadata")?;
    validate_maple_credential_metadata(
        metadata.is_file(),
        metadata.uid(),
        metadata.mode(),
        metadata.len(),
    )?;
    let mut bytes = Zeroizing::new(Vec::new());
    file.take(MAX_MAPLE_CREDENTIAL_BYTES + 1)
        .read_to_end(&mut bytes)
        .context("reading Maple credential")?;
    if bytes.len() as u64 > MAX_MAPLE_CREDENTIAL_BYTES {
        anyhow::bail!("Maple credential exceeds the size limit");
    }
    let credential: MapleCredential =
        serde_json::from_slice(&bytes).context("decoding Maple credential")?;
    validate_maple_credential(&credential, expected_producer)?;
    Ok(maple_exporter_config(&credential))
}

fn maple_exporter_config(credential: &MapleCredential) -> MapleExporterConfig {
    let mut basic =
        String::with_capacity(credential.username.len() + credential.password.len() + 1);
    basic.push_str(&credential.username);
    basic.push(':');
    basic.push_str(&credential.password);
    let mut encoded = BASE64_STANDARD.encode(basic.as_bytes());
    basic.zeroize();
    let mut headers = HashMap::new();
    headers.insert("authorization".to_owned(), format!("Basic {encoded}"));
    encoded.zeroize();
    MapleExporterConfig {
        metrics_endpoint: format!("{}/v1/metrics", credential.endpoint.trim_end_matches('/')),
        logs_endpoint: format!("{}/v1/logs", credential.endpoint.trim_end_matches('/')),
        headers,
    }
}

fn validate_root_owned_parent_chain(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .context("Maple credential path has no parent")?;
    for directory in parent.ancestors() {
        if directory.as_os_str().is_empty() {
            continue;
        }
        let metadata = directory.symlink_metadata().with_context(|| {
            format!("inspecting Maple credential parent {}", directory.display())
        })?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            anyhow::bail!("Maple credential parent is not a real directory");
        }
        if metadata.uid() != 0 || metadata.mode() & 0o022 != 0 {
            anyhow::bail!("Maple credential parent is not root-controlled");
        }
    }
    Ok(())
}

fn validate_maple_credential_metadata(
    regular_file: bool,
    owner: u32,
    mode: u32,
    size: u64,
) -> Result<()> {
    if !regular_file {
        anyhow::bail!("Maple credential is not a regular file");
    }
    if owner != 0 {
        anyhow::bail!("Maple credential is not owned by root");
    }
    if mode & 0o7777 != 0o600 {
        anyhow::bail!("Maple credential mode is not 0600");
    }
    if size == 0 || size > MAX_MAPLE_CREDENTIAL_BYTES {
        anyhow::bail!("Maple credential size is invalid");
    }
    Ok(())
}

fn validate_maple_credential(credential: &MapleCredential, expected_producer: &str) -> Result<()> {
    if credential.schema != MAPLE_CREDENTIAL_SCHEMA {
        anyhow::bail!("Maple credential schema is invalid");
    }
    if credential.producer != expected_producer {
        anyhow::bail!("Maple credential producer is invalid");
    }
    let endpoint_authority = credential
        .endpoint
        .strip_prefix("http://")
        .or_else(|| credential.endpoint.strip_prefix("https://"));
    if !endpoint_authority.is_some_and(|authority| {
        !authority.is_empty()
            && !authority.contains(['/', '?', '#'])
            && !authority.chars().any(char::is_whitespace)
    }) {
        anyhow::bail!("Maple credential endpoint is invalid");
    }
    if credential.protocol != MAPLE_PROTOCOL {
        anyhow::bail!("Maple credential protocol is invalid");
    }
    let username_prefix = format!("maple-{expected_producer}-");
    let valid_username = credential
        .username
        .strip_prefix(&username_prefix)
        .is_some_and(|suffix| {
            !suffix.is_empty() && suffix.bytes().all(|byte| byte.is_ascii_alphanumeric())
        });
    if !valid_username || credential.username.contains(':') {
        anyhow::bail!("Maple credential username is invalid");
    }
    if credential.password.is_empty()
        || credential.password.contains('\r')
        || credential.password.contains('\n')
    {
        anyhow::bail!("Maple credential password is invalid");
    }
    Ok(())
}

fn reject_otel_override_environment() -> Result<()> {
    for key in [
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
        "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT",
        "OTEL_EXPORTER_OTLP_PROTOCOL",
        "OTEL_EXPORTER_OTLP_METRICS_PROTOCOL",
        "OTEL_EXPORTER_OTLP_LOGS_PROTOCOL",
        "OTEL_EXPORTER_OTLP_HEADERS",
        "OTEL_EXPORTER_OTLP_METRICS_HEADERS",
        "OTEL_EXPORTER_OTLP_LOGS_HEADERS",
    ] {
        if env::var_os(key).is_some() {
            anyhow::bail!(
                "OTLP endpoint, protocol, and authorization must come from the Maple credential file"
            );
        }
    }
    Ok(())
}

fn drop_root_privileges(account: &str) -> Result<()> {
    if !Uid::effective().is_root() {
        return Ok(());
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = account;
        anyhow::bail!("root privilege drop is supported only on Linux");
    }
    #[cfg(target_os = "linux")]
    {
        let user = User::from_name(account)
            .context("looking up bridge runtime account")?
            .with_context(|| format!("bridge runtime account does not exist: {account}"))?;
        setgroups(&[]).context("clearing bridge supplementary groups")?;
        setgid(user.gid).context("setting bridge runtime group")?;
        setuid(user.uid).context("setting bridge runtime user")?;
        if Uid::effective().is_root()
            || Uid::effective() != user.uid
            || Gid::effective() != user.gid
        {
            anyhow::bail!("bridge privilege drop did not reach the configured account");
        }
        Ok(())
    }
}

fn initialize_otel(
    maple: Option<&MapleExporterConfig>,
) -> Result<(SdkMeterProvider, SdkLoggerProvider)> {
    let resource = Resource::builder()
        .with_service_name("spark-otel-bridge")
        .with_attribute(KeyValue::new(
            "telemetry.semconv.revision",
            OTEL_SEMCONV_REVISION,
        ))
        .build();
    let mut metric_exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpBinary);
    if let Some(config) = maple {
        metric_exporter = metric_exporter
            .with_endpoint(config.metrics_endpoint.clone())
            .with_headers(config.headers.clone());
    }
    let metric_exporter = metric_exporter
        .build()
        .context("building OTLP metric exporter")?;
    let meter_provider = SdkMeterProvider::builder()
        .with_resource(resource.clone())
        .with_periodic_exporter(metric_exporter)
        .build();
    global::set_meter_provider(meter_provider.clone());
    let mut log_exporter = opentelemetry_otlp::LogExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpBinary);
    if let Some(config) = maple {
        log_exporter = log_exporter
            .with_endpoint(config.logs_endpoint.clone())
            .with_headers(config.headers.clone());
    }
    let log_exporter = log_exporter.build().context("building OTLP log exporter")?;
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

    const TEST_ENV: &str = include_str!("../../../.env.test");

    fn test_env(name: &str) -> &'static str {
        TEST_ENV
            .lines()
            .filter_map(|line| line.split_once('='))
            .find_map(|(key, value)| (key == name).then_some(value))
            .unwrap_or_else(|| panic!("missing test environment value: {name}"))
    }

    fn fixture_credential() -> MapleCredential {
        MapleCredential {
            schema: MAPLE_CREDENTIAL_SCHEMA.to_owned(),
            endpoint: test_env("MAPLE_TEST_ENDPOINT").to_owned(),
            password: test_env("MAPLE_TEST_AUTH_INPUT").to_owned(),
            producer: "spark-signals".to_owned(),
            protocol: MAPLE_PROTOCOL.to_owned(),
            username: test_env("MAPLE_TEST_USERNAME").to_owned(),
        }
    }

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

    #[test]
    fn validates_maple_contract_and_builds_signal_endpoints() {
        let credential = fixture_credential();
        validate_maple_credential(&credential, "spark-signals").unwrap();
        let config = maple_exporter_config(&credential);
        assert_eq!(
            config.metrics_endpoint,
            format!("{}/v1/metrics", test_env("MAPLE_TEST_ENDPOINT"))
        );
        assert_eq!(
            config.logs_endpoint,
            format!("{}/v1/logs", test_env("MAPLE_TEST_ENDPOINT"))
        );
        let encoded = config.headers["authorization"]
            .strip_prefix("Basic ")
            .expect("Basic authorization scheme");
        let decoded = BASE64_STANDARD.decode(encoded).expect("base64 header");
        assert_eq!(
            decoded,
            format!("{}:{}", credential.username, credential.password).as_bytes()
        );
    }

    #[test]
    fn rejects_wrong_maple_producer_endpoint_or_protocol() {
        let mut credential = fixture_credential();
        credential.producer = "another-producer".to_owned();
        assert!(validate_maple_credential(&credential, "spark-signals").is_err());
        credential.producer = "spark-signals".to_owned();
        credential.endpoint = "file:///tmp/not-an-otlp-endpoint".to_owned();
        assert!(validate_maple_credential(&credential, "spark-signals").is_err());
        credential.endpoint = test_env("MAPLE_TEST_ENDPOINT").to_owned();
        credential.protocol = "grpc".to_owned();
        assert!(validate_maple_credential(&credential, "spark-signals").is_err());
    }

    #[test]
    fn enforces_root_owned_regular_mode_0600_credential() {
        assert!(validate_maple_credential_metadata(true, 0, 0o100_600, 512).is_ok());
        assert!(validate_maple_credential_metadata(true, 1000, 0o100_600, 512).is_err());
        assert!(validate_maple_credential_metadata(true, 0, 0o100_640, 512).is_err());
        assert!(validate_maple_credential_metadata(false, 0, 0o100_600, 512).is_err());
    }

    #[test]
    fn denies_unknown_maple_credential_fields() {
        let fixture = fixture_credential();
        let json = serde_json::json!({
            "$schema": fixture.schema,
            "endpoint": fixture.endpoint,
            "password": fixture.password,
            "producer": fixture.producer,
            "protocol": fixture.protocol,
            "username": fixture.username,
            "unexpected": true
        });
        assert!(serde_json::from_value::<MapleCredential>(json).is_err());
    }
}
