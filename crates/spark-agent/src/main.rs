use std::{
    collections::{BTreeMap, HashMap},
    fs,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result};
use chrono::{SecondsFormat, Utc};
use clap::Parser;
use spark_linux::{SystemCollector, inventory as linux_inventory};
use spark_nvidia::NvidiaCollector;
use spark_probes::{AgentConfig, LlmProbe, ServiceProbe};
use spark_schema::{Envelope, MetricPoint, Node, Quality, SCHEMA_V1, Severity, Signal};
use tokio::{
    sync::{mpsc, watch},
    time::Instant,
};

#[derive(Parser)]
#[command(about = "DGX Spark host telemetry agent")]
struct Args {
    #[arg(long, env = "SPARK_SITE", default_value = "home", value_parser = valid_subject_component)]
    site: String,
    #[arg(long, env = "SPARK_NODE", value_parser = valid_subject_component)]
    node: Option<String>,
    #[arg(long, env = "NATS_URL")]
    nats_url: Option<String>,
    #[arg(long, env = "NATS_CREDENTIALS")]
    nats_credentials: Option<PathBuf>,
    #[arg(long, env = "NATS_USER", requires = "nats_password")]
    nats_user: Option<String>,
    #[arg(long, env = "NATS_PASSWORD", requires = "nats_user")]
    nats_password: Option<String>,
    #[arg(long, env = "NATS_CA")]
    nats_ca: Option<PathBuf>,
    #[arg(long, env = "SPARK_CONFIG")]
    config: Option<PathBuf>,
    #[arg(long)]
    include_gpu_process_allocations: bool,
    #[arg(long)]
    stdout: bool,
    #[arg(long)]
    once: bool,
    #[arg(long, default_value_t = 2, value_parser = clap::value_parser!(u64).range(1..=300))]
    interval_seconds: u64,
    #[arg(long, default_value_t = 10, value_parser = clap::value_parser!(u64).range(2..=600))]
    medium_interval_seconds: u64,
    #[arg(long, default_value_t = 60, value_parser = clap::value_parser!(u64).range(10..=3600))]
    slow_interval_seconds: u64,
}

#[derive(Debug, Clone)]
struct Publication {
    sequence: u64,
    subject: String,
    payload: Arc<Vec<u8>>,
}

struct EnvelopeFactory {
    node: Node,
    boot_id: String,
    process_start: Instant,
    sequence: u64,
}

struct CollectorAges {
    started: Instant,
    last_success: HashMap<&'static str, Instant>,
}

impl CollectorAges {
    fn new() -> Self {
        Self {
            started: Instant::now(),
            last_success: HashMap::new(),
        }
    }

    fn observe(&mut self, domain: &'static str, points: &[MetricPoint]) -> MetricPoint {
        let now = Instant::now();
        if points.iter().any(|point| {
            point.value.is_some()
                && matches!(
                    point.quality,
                    Quality::Measured | Quality::Derived | Quality::Estimated
                )
        }) {
            self.last_success.insert(domain, now);
        }
        let age = self.last_success.get(domain).map_or_else(
            || now.duration_since(self.started),
            |last| now.duration_since(*last),
        );
        MetricPoint::gauge(
            "spark.agent.collector.age",
            age.as_secs_f64(),
            "s",
            Quality::Derived,
            "agent",
        )
        .with_attribute("collector.domain", domain)
    }
}

impl EnvelopeFactory {
    fn build(&mut self, signal: Signal, duration: Duration, valid_for: Duration) -> Envelope {
        self.sequence = self.sequence.saturating_add(1);
        Envelope {
            schema: SCHEMA_V1.to_owned(),
            node: self.node.clone(),
            boot_id: self.boot_id.clone(),
            sequence: self.sequence,
            observed_at: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
            monotonic_ns: u64::try_from(self.process_start.elapsed().as_nanos())
                .unwrap_or(u64::MAX),
            collection_duration_ms: u64::try_from(duration.as_millis()).unwrap_or(u64::MAX),
            valid_for_ms: u64::try_from(valid_for.as_millis()).unwrap_or(u64::MAX),
            signal,
        }
    }
}

#[tokio::main]
#[allow(clippy::too_many_lines, clippy::cast_precision_loss)]
async fn main() -> Result<()> {
    let args = Args::parse();
    let mut config = args
        .config
        .as_ref()
        .map(AgentConfig::load)
        .transpose()?
        .unwrap_or_default();
    let host_name = read_trimmed("/etc/hostname").unwrap_or_else(|_| "unknown".to_owned());
    let node_id = args
        .node
        .clone()
        .unwrap_or_else(|| sanitize_component(&host_name));
    let node = Node {
        site: args.site.clone(),
        id: node_id.clone(),
        host_name,
    };
    let mut factory = EnvelopeFactory {
        node,
        boot_id: read_trimmed("/proc/sys/kernel/random/boot_id")
            .unwrap_or_else(|_| "unknown".to_owned()),
        process_start: Instant::now(),
        sequence: 0,
    };
    let interval = Duration::from_secs(args.interval_seconds);
    if args.medium_interval_seconds < args.interval_seconds
        || args.slow_interval_seconds < args.medium_interval_seconds
    {
        anyhow::bail!("cadences must satisfy hot <= medium <= slow");
    }
    let print_stdout = args.stdout || args.nats_url.is_none();
    let publish_events = args.nats_url.is_some();
    let (state_sender, state_receiver) = watch::channel(BTreeMap::<String, Publication>::new());
    let (event_sender, event_receiver) = mpsc::channel::<Publication>(128);
    let reconnects = Arc::new(AtomicU64::new(0));
    let dropped_events = Arc::new(AtomicU64::new(0));

    let nvidia = NvidiaCollector::new(
        args.include_gpu_process_allocations,
        config.hardware_capabilities_dir.as_deref(),
    )?;
    let mut inventory = host_inventory();
    let mut last_nvidia_inventory = nvidia.inventory();
    inventory.extend(last_nvidia_inventory.clone());
    publish_state(
        &state_sender,
        &args.site,
        &node_id,
        "status.agent",
        factory.build(
            Signal::AgentStatus {
                online: true,
                version: env!("CARGO_PKG_VERSION").to_owned(),
            },
            Duration::ZERO,
            Duration::from_secs(10),
        ),
        print_stdout,
    )?;
    publish_state(
        &state_sender,
        &args.site,
        &node_id,
        "inventory",
        factory.build(
            Signal::Inventory {
                attributes: inventory,
            },
            Duration::ZERO,
            Duration::from_mins(2),
        ),
        print_stdout,
    )?;

    if let Some(url) = args.nats_url.clone() {
        tokio::spawn(nats_publisher(
            url,
            args.nats_credentials.clone(),
            args.nats_user.clone(),
            args.nats_password.clone(),
            args.nats_ca.clone(),
            state_receiver,
            event_receiver,
            Arc::clone(&reconnects),
        ));
    }

    let mut linux = SystemCollector::default();
    let mut llm = LlmProbe::new(config.llm.clone())?;
    let mut services = ServiceProbe::default();
    let mut service_states = HashMap::<String, bool>::new();
    let mut system_health_states = HashMap::<String, u8>::new();
    let mut collector_had_errors = false;
    let mut collector_ages = CollectorAges::new();
    let mut reported_reconnects = 0_u64;
    let mut reported_dropped_events = 0_u64;
    let mut tick_count = 0_u64;
    let medium_ticks = args.medium_interval_seconds.div_ceil(args.interval_seconds);
    let slow_ticks = args.slow_interval_seconds.div_ceil(args.interval_seconds);
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    #[cfg(unix)]
    let mut reload_signal = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())
        .context("installing SIGHUP handler")?;
    if !args.once {
        tokio::time::sleep(stable_jitter(&node_id, interval)).await;
    }

    loop {
        #[cfg(unix)]
        tokio::select! {
            _ = ticker.tick() => {}
            _ = reload_signal.recv() => {
                let Some(path) = &args.config else {
                    eprintln!("SIGHUP ignored: no configuration path was supplied");
                    continue;
                };
                match AgentConfig::load(path).and_then(|next| {
                    let probe = LlmProbe::new(next.llm.clone())?;
                    nvidia.reload_capabilities(next.hardware_capabilities_dir.as_deref())?;
                    Ok((next, probe))
                }) {
                    Ok((next, probe)) => {
                        config = next;
                        llm = probe;
                        last_nvidia_inventory = nvidia.inventory();
                        let mut inventory = host_inventory();
                        inventory.extend(last_nvidia_inventory.clone());
                        publish_state(
                            &state_sender,
                            &args.site,
                            &node_id,
                            "inventory",
                            factory.build(
                                Signal::Inventory { attributes: inventory },
                                Duration::ZERO,
                                Duration::from_mins(2),
                            ),
                            print_stdout,
                        )?;
                        eprintln!("agent configuration reloaded");
                    }
                    Err(error) => eprintln!("configuration reload failed; keeping previous configuration: {error}"),
                }
                continue;
            }
        }
        #[cfg(not(unix))]
        ticker.tick().await;
        tick_count = tick_count.saturating_add(1);
        let medium_due = tick_count == 1 || tick_count.is_multiple_of(medium_ticks);
        let started = Instant::now();
        let reconnect_count = reconnects.load(Ordering::Relaxed);
        if reconnect_count > reported_reconnects {
            reported_reconnects = reconnect_count;
            let dropped_count = dropped_events.load(Ordering::Relaxed);
            send_event(
                &event_sender,
                &dropped_events,
                &args.site,
                &node_id,
                factory.build(
                    Signal::HealthEvent {
                        severity: Severity::Info,
                        code: "NATS_RECONNECTED".to_owned(),
                        message: "NATS connection was restored".to_owned(),
                        attributes: BTreeMap::new(),
                    },
                    Duration::ZERO,
                    Duration::from_mins(1),
                ),
                print_stdout,
                publish_events,
            )?;
            if dropped_count > reported_dropped_events {
                reported_dropped_events = dropped_count;
                send_event(
                    &event_sender,
                    &dropped_events,
                    &args.site,
                    &node_id,
                    factory.build(
                        Signal::HealthEvent {
                            severity: Severity::Warning,
                            code: "EVENTS_DROPPED".to_owned(),
                            message:
                                "health events were dropped while the publication queue was full"
                                    .to_owned(),
                            attributes: BTreeMap::new(),
                        },
                        Duration::ZERO,
                        Duration::from_mins(1),
                    ),
                    print_stdout,
                    publish_events,
                )?;
            }
        }
        let mut system_points = linux.collect_core();
        let mut network_points = linux.collect_network_points();
        let mut storage_points = if medium_due {
            linux.collect_storage()
        } else {
            Vec::new()
        };
        let mut nvidia_points = nvidia.collect();
        let current_nvidia_inventory = nvidia.inventory();
        if current_nvidia_inventory != last_nvidia_inventory {
            last_nvidia_inventory = current_nvidia_inventory;
            let mut inventory = host_inventory();
            inventory.extend(last_nvidia_inventory.clone());
            publish_state(
                &state_sender,
                &args.site,
                &node_id,
                "inventory",
                factory.build(
                    Signal::Inventory {
                        attributes: inventory,
                    },
                    Duration::ZERO,
                    Duration::from_mins(2),
                ),
                print_stdout,
            )?;
        }
        for xid in nvidia_points.iter().filter(|point| {
            point.name == "nvidia.gpu.xid.count" && point.value.is_some_and(|value| value > 0.0)
        }) {
            send_event(
                &event_sender,
                &dropped_events,
                &args.site,
                &node_id,
                factory.build(
                    Signal::HealthEvent {
                        severity: Severity::Critical,
                        code: "NVIDIA_XID".to_owned(),
                        message: "NVIDIA reported a critical Xid event".to_owned(),
                        attributes: xid.attributes.clone(),
                    },
                    Duration::ZERO,
                    Duration::from_mins(1),
                ),
                print_stdout,
                publish_events,
            )?;
        }
        detect_system_events(
            &system_points,
            &mut system_health_states,
            &event_sender,
            &dropped_events,
            &args.site,
            &node_id,
            &mut factory,
            print_stdout,
            publish_events,
        )?;
        detect_system_events(
            &network_points,
            &mut system_health_states,
            &event_sender,
            &dropped_events,
            &args.site,
            &node_id,
            &mut factory,
            print_stdout,
            publish_events,
        )?;
        if medium_due {
            detect_system_events(
                &storage_points,
                &mut system_health_states,
                &event_sender,
                &dropped_events,
                &args.site,
                &node_id,
                &mut factory,
                print_stdout,
                publish_events,
            )?;
        }
        let error_count = system_points
            .iter()
            .chain(&network_points)
            .chain(&storage_points)
            .chain(&nvidia_points)
            .filter(|point| point.quality == Quality::Error)
            .count();
        let system_age = collector_ages.observe("system", &system_points);
        let network_age = collector_ages.observe("network", &network_points);
        let storage_age = medium_due.then(|| collector_ages.observe("storage", &storage_points));
        let nvidia_age = collector_ages.observe("nvidia", &nvidia_points);
        system_points.extend([
            MetricPoint::gauge(
                "spark.agent.collection.duration",
                started.elapsed().as_secs_f64(),
                "s",
                Quality::Measured,
                "agent",
            ),
            MetricPoint::gauge(
                "spark.agent.collection.errors",
                error_count as f64,
                "{error}",
                Quality::Measured,
                "agent",
            ),
            MetricPoint::gauge(
                "spark.agent.nats.reconnects",
                reconnects.load(Ordering::Relaxed) as f64,
                "{reconnect}",
                Quality::Measured,
                "agent",
            ),
            MetricPoint::gauge(
                "spark.agent.events.dropped",
                dropped_events.load(Ordering::Relaxed) as f64,
                "{event}",
                Quality::Measured,
                "agent",
            ),
            system_age,
        ]);
        network_points.push(network_age);
        if let Some(age) = storage_age {
            storage_points.push(age);
        }
        nvidia_points.push(nvidia_age);
        let duration = started.elapsed();
        publish_state(
            &state_sender,
            &args.site,
            &node_id,
            "sample.system",
            factory.build(
                Signal::MetricBatch {
                    points: system_points,
                },
                duration,
                interval.saturating_mul(3),
            ),
            print_stdout,
        )?;
        publish_state(
            &state_sender,
            &args.site,
            &node_id,
            "sample.network",
            factory.build(
                Signal::MetricBatch {
                    points: network_points,
                },
                duration,
                interval.saturating_mul(3),
            ),
            print_stdout,
        )?;
        if medium_due {
            publish_state(
                &state_sender,
                &args.site,
                &node_id,
                "sample.storage",
                factory.build(
                    Signal::MetricBatch {
                        points: storage_points,
                    },
                    duration,
                    Duration::from_secs(args.medium_interval_seconds).saturating_mul(3),
                ),
                print_stdout,
            )?;
        }
        publish_state(
            &state_sender,
            &args.site,
            &node_id,
            "sample.nvidia",
            factory.build(
                Signal::MetricBatch {
                    points: nvidia_points,
                },
                duration,
                interval.saturating_mul(3),
            ),
            print_stdout,
        )?;

        let has_errors = error_count > 0;
        if has_errors != collector_had_errors {
            collector_had_errors = has_errors;
            send_event(
                &event_sender,
                &dropped_events,
                &args.site,
                &node_id,
                factory.build(
                    Signal::HealthEvent {
                        severity: if has_errors {
                            Severity::Warning
                        } else {
                            Severity::Info
                        },
                        code: if has_errors {
                            "COLLECTOR_DEGRADED"
                        } else {
                            "COLLECTOR_RECOVERED"
                        }
                        .to_owned(),
                        message: if has_errors {
                            "one or more collectors reported errors"
                        } else {
                            "collectors recovered"
                        }
                        .to_owned(),
                        attributes: BTreeMap::new(),
                    },
                    Duration::ZERO,
                    Duration::from_mins(1),
                ),
                print_stdout,
                publish_events,
            )?;
        }

        if args.once || medium_due {
            let mut service_points = services.collect(&config.service);
            for point in &service_points {
                if point.name == "spark.service.active"
                    && let Some(value) = point.value
                    && let Some(unit) = point.attributes.get("systemd.unit")
                {
                    let active = value > 0.5;
                    if let Some(previous) = service_states
                        .insert(unit.clone(), active)
                        .filter(|old| *old != active)
                    {
                        let _ = previous;
                        let mut attributes = BTreeMap::new();
                        attributes.insert("systemd.unit".to_owned(), unit.clone());
                        send_event(
                            &event_sender,
                            &dropped_events,
                            &args.site,
                            &node_id,
                            factory.build(
                                Signal::HealthEvent {
                                    severity: if active {
                                        Severity::Info
                                    } else {
                                        Severity::Error
                                    },
                                    code: "SERVICE_STATE_CHANGED".to_owned(),
                                    message: if active {
                                        "configured service became active"
                                    } else {
                                        "configured service became inactive"
                                    }
                                    .to_owned(),
                                    attributes,
                                },
                                Duration::ZERO,
                                Duration::from_mins(1),
                            ),
                            print_stdout,
                            publish_events,
                        )?;
                    }
                }
            }
            let service_age = collector_ages.observe("service", &service_points);
            service_points.push(service_age);
            publish_state(
                &state_sender,
                &args.site,
                &node_id,
                "sample.service",
                factory.build(
                    Signal::MetricBatch {
                        points: service_points,
                    },
                    Duration::ZERO,
                    Duration::from_secs(30),
                ),
                print_stdout,
            )?;
            let llm_started = Instant::now();
            let mut llm_points = llm.collect().await;
            let llm_age = collector_ages.observe("llm", &llm_points);
            llm_points.push(llm_age);
            publish_state(
                &state_sender,
                &args.site,
                &node_id,
                "sample.llm",
                factory.build(
                    Signal::MetricBatch { points: llm_points },
                    llm_started.elapsed(),
                    Duration::from_secs(30),
                ),
                print_stdout,
            )?;
        }

        if tick_count.is_multiple_of(slow_ticks) {
            let mut inventory = host_inventory();
            inventory.extend(nvidia.inventory());
            publish_state(
                &state_sender,
                &args.site,
                &node_id,
                "inventory",
                factory.build(
                    Signal::Inventory {
                        attributes: inventory,
                    },
                    Duration::ZERO,
                    Duration::from_mins(2),
                ),
                print_stdout,
            )?;
        }
        if args.once {
            break;
        }
    }
    Ok(())
}

#[allow(clippy::needless_pass_by_value)]
fn publish_state(
    sender: &watch::Sender<BTreeMap<String, Publication>>,
    site: &str,
    node: &str,
    suffix: &str,
    envelope: Envelope,
    stdout: bool,
) -> Result<()> {
    let publication = publication(site, node, suffix, &envelope, stdout)?;
    sender.send_modify(|state| {
        state.insert(publication.subject.clone(), publication);
    });
    Ok(())
}

#[allow(clippy::needless_pass_by_value)]
fn send_event(
    sender: &mpsc::Sender<Publication>,
    dropped: &AtomicU64,
    site: &str,
    node: &str,
    envelope: Envelope,
    stdout: bool,
    publish: bool,
) -> Result<()> {
    let publication = publication(site, node, "event.health", &envelope, stdout)?;
    if publish && sender.try_send(publication).is_err() {
        dropped.fetch_add(1, Ordering::Relaxed);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn detect_system_events(
    points: &[MetricPoint],
    states: &mut HashMap<String, u8>,
    sender: &mpsc::Sender<Publication>,
    dropped: &AtomicU64,
    site: &str,
    node: &str,
    factory: &mut EnvelopeFactory,
    stdout: bool,
    publish: bool,
) -> Result<()> {
    for point in points {
        let Some(value) = point.value else { continue };
        if point.name == "system.memory.oom_kills" && value > 0.0 {
            send_event(
                sender,
                dropped,
                site,
                node,
                factory.build(
                    Signal::HealthEvent {
                        severity: Severity::Critical,
                        code: "MEMORY_OOM_KILL".to_owned(),
                        message: "one or more OOM kills were observed".to_owned(),
                        attributes: point.attributes.clone(),
                    },
                    Duration::ZERO,
                    Duration::from_mins(1),
                ),
                stdout,
                publish,
            )?;
            continue;
        }

        let transition = if point.name == "system.network.link.up" {
            let interface = point
                .attributes
                .get("network.interface.name")
                .cloned()
                .unwrap_or_default();
            Some((
                format!("network:{interface}"),
                u8::from(value < 0.5),
                "NETWORK_CARRIER_LOST",
                "NETWORK_CARRIER_RESTORED",
                "physical network carrier was lost",
                "physical network carrier was restored",
            ))
        } else if point.name == "system.filesystem.read_only" {
            let mount = point
                .attributes
                .get("mountpoint")
                .cloned()
                .unwrap_or_default();
            Some((
                format!("filesystem:{mount}"),
                if value > 0.5 { 2 } else { 0 },
                "FILESYSTEM_READ_ONLY",
                "FILESYSTEM_WRITABLE",
                "filesystem became read-only",
                "filesystem is writable again",
            ))
        } else if point.name == "system.temperature" && point.quality == Quality::Measured {
            let critical = point
                .attributes
                .get("temperature.limit.critical_celsius")
                .and_then(|limit| limit.parse::<f64>().ok());
            let maximum = point
                .attributes
                .get("temperature.limit.max_celsius")
                .and_then(|limit| limit.parse::<f64>().ok());
            let level = if critical.is_some_and(|limit| value >= limit) {
                2
            } else {
                u8::from(maximum.is_some_and(|limit| value >= limit))
            };
            let sensor = point.attributes.get("sensor").cloned().unwrap_or_default();
            let channel = point.attributes.get("channel").cloned().unwrap_or_default();
            Some((
                format!("temperature:{sensor}:{channel}"),
                level,
                if level == 2 {
                    "THERMAL_CRITICAL"
                } else {
                    "THERMAL_WARNING"
                },
                "THERMAL_RECOVERED",
                if level == 2 {
                    "temperature reached its critical limit"
                } else {
                    "temperature reached its maximum limit"
                },
                "temperature returned below its configured limits",
            ))
        } else {
            None
        };
        let Some((key, level, bad_code, good_code, bad_message, good_message)) = transition else {
            continue;
        };
        let previous = states.insert(key, level);
        if previous == Some(level) || previous.is_none() && level == 0 {
            continue;
        }
        send_event(
            sender,
            dropped,
            site,
            node,
            factory.build(
                Signal::HealthEvent {
                    severity: match level {
                        0 => Severity::Info,
                        1 => Severity::Warning,
                        _ => Severity::Critical,
                    },
                    code: if level == 0 { good_code } else { bad_code }.to_owned(),
                    message: if level == 0 {
                        good_message
                    } else {
                        bad_message
                    }
                    .to_owned(),
                    attributes: point.attributes.clone(),
                },
                Duration::ZERO,
                Duration::from_mins(1),
            ),
            stdout,
            publish,
        )?;
    }
    Ok(())
}

fn publication(
    site: &str,
    node: &str,
    suffix: &str,
    envelope: &Envelope,
    stdout: bool,
) -> Result<Publication> {
    let sequence = envelope.sequence;
    let payload = Arc::new(serde_json::to_vec(&envelope).context("serializing observation")?);
    if stdout {
        println!("{}", String::from_utf8_lossy(&payload));
    }
    Ok(Publication {
        sequence,
        subject: format!("spark.v1.{site}.{node}.{suffix}"),
        payload,
    })
}

#[allow(clippy::too_many_arguments)]
async fn nats_publisher(
    url: String,
    credentials: Option<PathBuf>,
    user: Option<String>,
    password: Option<String>,
    ca: Option<PathBuf>,
    mut state: watch::Receiver<BTreeMap<String, Publication>>,
    mut events: mpsc::Receiver<Publication>,
    reconnects: Arc<AtomicU64>,
) {
    loop {
        let (nats_event_sender, mut nats_events) = mpsc::channel(32);
        let mut options = async_nats::ConnectOptions::new()
            .name("spark-agent")
            .max_reconnects(None)
            .event_callback(move |event| {
                let sender = nats_event_sender.clone();
                async move {
                    let _ = sender.try_send(event);
                }
            });
        if let (Some(user), Some(password)) = (&user, &password) {
            options = options.user_and_password(user.clone(), password.clone());
        }
        if let Some(path) = &credentials {
            match options.credentials_file(path).await {
                Ok(configured) => options = configured,
                Err(error) => {
                    eprintln!("NATS credentials unavailable: {error}");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            }
        }
        if let Some(path) = &ca {
            options = options
                .add_root_certificates(path.clone())
                .require_tls(true);
        }
        let connection = tokio::time::timeout(Duration::from_secs(5), options.connect(&url)).await;
        let Ok(Ok(client)) = connection else {
            eprintln!("NATS connection unavailable; collection continues");
            tokio::time::sleep(Duration::from_secs(5)).await;
            continue;
        };
        let mut sent = HashMap::<String, u64>::new();
        let snapshot = state.borrow().clone();
        if publish_latest(&client, &snapshot, &mut sent).await.is_err() {
            continue;
        }
        let mut connected_events = 0_u64;
        loop {
            tokio::select! {
                changed = state.changed() => {
                    if changed.is_err() { return; }
                    let snapshot = state.borrow().clone();
                    if publish_latest(&client, &snapshot, &mut sent).await.is_err() { break; }
                }
                event = events.recv() => {
                    let Some(event) = event else { return; };
                    if client.publish(event.subject, event.payload.as_ref().clone().into()).await.is_err() { break; }
                }
                event = nats_events.recv() => {
                    match event {
                        Some(async_nats::Event::Connected) => {
                            connected_events = connected_events.saturating_add(1);
                            if connected_events > 1 { reconnects.fetch_add(1, Ordering::Relaxed); }
                            sent.clear();
                            let snapshot = state.borrow().clone();
                            if publish_latest(&client, &snapshot, &mut sent).await.is_err() { break; }
                        }
                        Some(async_nats::Event::SlowConsumer(_)) => eprintln!("NATS slow consumer event"),
                        Some(_) => {}
                        None => break,
                    }
                }
            }
        }
    }
}

async fn publish_latest(
    client: &async_nats::Client,
    state: &BTreeMap<String, Publication>,
    sent: &mut HashMap<String, u64>,
) -> Result<()> {
    let mut publications: Vec<_> = state.values().collect();
    publications.sort_by_key(|publication| publication.sequence);
    for publication in publications {
        if sent
            .get(&publication.subject)
            .is_some_and(|sequence| *sequence >= publication.sequence)
        {
            continue;
        }
        client
            .publish(
                publication.subject.clone(),
                publication.payload.as_ref().clone().into(),
            )
            .await?;
        sent.insert(publication.subject.clone(), publication.sequence);
    }
    client.flush().await?;
    Ok(())
}

fn host_inventory() -> BTreeMap<String, String> {
    let mut inventory = linux_inventory();
    for (key, path) in [
        ("host.kernel.version", "/proc/sys/kernel/osrelease"),
        ("host.product.name", "/sys/class/dmi/id/product_name"),
        ("host.board.name", "/sys/class/dmi/id/board_name"),
    ] {
        if let Ok(value) = read_trimmed(path) {
            inventory.insert(key.to_owned(), value);
        }
    }
    if let Ok(raw) = fs::read_to_string("/proc/cpuinfo")
        && let Some(model) = raw
            .lines()
            .find_map(|line| line.strip_prefix("model name\t: "))
    {
        inventory.insert("host.cpu.model".to_owned(), model.to_owned());
    }
    inventory.insert(
        "host.cpu.logical.count".to_owned(),
        std::thread::available_parallelism()
            .map_or(0, std::num::NonZero::get)
            .to_string(),
    );
    inventory
}

fn read_trimmed(path: &str) -> Result<String> {
    Ok(fs::read_to_string(path)
        .with_context(|| format!("reading {path}"))?
        .trim()
        .to_owned())
}

fn valid_subject_component(value: &str) -> Result<String, String> {
    if !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        Ok(value.to_owned())
    } else {
        Err("must contain only ASCII letters, digits, '_' or '-'".to_owned())
    }
}

fn sanitize_component(value: &str) -> String {
    let sanitized: String = value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '_' | '-') {
                character
            } else {
                '-'
            }
        })
        .collect();
    if sanitized.is_empty() {
        "unknown".to_owned()
    } else {
        sanitized
    }
}

fn stable_jitter(node: &str, interval: Duration) -> Duration {
    let hash = node
        .bytes()
        .fold(1_469_598_103_934_665_603_u64, |hash, byte| {
            hash.wrapping_mul(1_099_511_628_211)
                .wrapping_add(u64::from(byte))
        });
    let ceiling = u64::try_from(interval.as_millis() / 4)
        .unwrap_or(u64::MAX)
        .clamp(1, 500);
    Duration::from_millis(hash % ceiling)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_subject_components() {
        assert!(valid_subject_component("spark-885a").is_ok());
        assert!(valid_subject_component("home.wildcard").is_err());
        assert!(valid_subject_component(">").is_err());
    }

    #[test]
    fn sampling_jitter_is_stable_and_bounded() {
        let interval = Duration::from_secs(2);
        assert_eq!(
            stable_jitter("spark-885a", interval),
            stable_jitter("spark-885a", interval)
        );
        assert!(stable_jitter("spark-885a", interval) < Duration::from_millis(500));
    }
}
