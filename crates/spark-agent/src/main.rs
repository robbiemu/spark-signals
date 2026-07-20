use std::{fs, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use chrono::{SecondsFormat, Utc};
use clap::Parser;
use spark_linux::SystemCollector;
use spark_schema::{Envelope, Node, SCHEMA_V1, Signal};
use tokio::{sync::watch, time::Instant};

#[derive(Debug, Parser)]
#[command(about = "DGX Spark host telemetry agent")]
struct Args {
    #[arg(long, env = "SPARK_SITE", default_value = "home", value_parser = valid_subject_component)]
    site: String,
    #[arg(long, env = "SPARK_NODE", value_parser = valid_subject_component)]
    node: Option<String>,
    #[arg(long, env = "NATS_URL")]
    nats_url: Option<String>,
    #[arg(long)]
    stdout: bool,
    #[arg(long)]
    once: bool,
    #[arg(long, default_value_t = 2, value_parser = clap::value_parser!(u64).range(1..=300))]
    interval_seconds: u64,
}

#[derive(Debug, Clone)]
struct Publication {
    subject: String,
    payload: Arc<Vec<u8>>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let host_name = read_trimmed("/etc/hostname").unwrap_or_else(|_| "unknown".to_owned());
    let node_id = args
        .node
        .clone()
        .unwrap_or_else(|| sanitize_component(&host_name));
    let boot_id =
        read_trimmed("/proc/sys/kernel/random/boot_id").unwrap_or_else(|_| "unknown".to_owned());
    let node = Node {
        site: args.site.clone(),
        id: node_id.clone(),
        host_name,
    };
    let subject = format!("spark.v1.{}.{}.sample.system", args.site, node_id);
    let interval = Duration::from_secs(args.interval_seconds);
    let process_start = Instant::now();
    let print_stdout = args.stdout || args.nats_url.is_none();
    let (sender, receiver) = watch::channel::<Option<Publication>>(None);

    if let Some(url) = args.nats_url.clone() {
        tokio::spawn(nats_publisher(url, receiver));
    }

    let mut collector = SystemCollector::default();
    let mut sequence = 0_u64;
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        ticker.tick().await;
        sequence = sequence.saturating_add(1);
        let collection_start = Instant::now();
        let points = collector.collect();
        let envelope = Envelope {
            schema: SCHEMA_V1.to_owned(),
            node: node.clone(),
            boot_id: boot_id.clone(),
            sequence,
            observed_at: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
            monotonic_ns: u64::try_from(process_start.elapsed().as_nanos()).unwrap_or(u64::MAX),
            collection_duration_ms: u64::try_from(collection_start.elapsed().as_millis())
                .unwrap_or(u64::MAX),
            valid_for_ms: u64::try_from(interval.as_millis().saturating_mul(3)).unwrap_or(u64::MAX),
            signal: Signal::MetricBatch { points },
        };
        let payload = Arc::new(serde_json::to_vec(&envelope).context("serializing observation")?);
        if print_stdout {
            println!("{}", String::from_utf8_lossy(&payload));
        }
        if args.nats_url.is_some() {
            sender.send_replace(Some(Publication {
                subject: subject.clone(),
                payload,
            }));
        }
        if args.once {
            break;
        }
    }
    Ok(())
}

async fn nats_publisher(url: String, mut receiver: watch::Receiver<Option<Publication>>) {
    loop {
        let connection =
            tokio::time::timeout(Duration::from_secs(5), async_nats::connect(&url)).await;
        let Ok(Ok(client)) = connection else {
            eprintln!("NATS connection unavailable at {url}; collection continues");
            tokio::time::sleep(Duration::from_secs(5)).await;
            continue;
        };
        loop {
            if receiver.changed().await.is_err() {
                return;
            }
            let publication = receiver.borrow_and_update().clone();
            let Some(publication) = publication else {
                continue;
            };
            if client
                .publish(
                    publication.subject,
                    publication.payload.as_ref().clone().into(),
                )
                .await
                .is_err()
            {
                eprintln!("NATS publish failed; reconnecting while collection continues");
                break;
            }
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_subject_components() {
        assert!(valid_subject_component("spark-885a").is_ok());
        assert!(valid_subject_component("home.wildcard").is_err());
        assert!(valid_subject_component(">").is_err());
    }
}
