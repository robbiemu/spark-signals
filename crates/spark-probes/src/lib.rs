#![allow(clippy::cast_precision_loss)]

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use reqwest::{
    Client,
    header::{HeaderName, HeaderValue},
};
use serde::Deserialize;
use spark_schema::{MetricPoint, Quality};

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentConfig {
    #[serde(default)]
    pub hardware_capabilities_dir: Option<PathBuf>,
    #[serde(default)]
    pub signal_policies_dir: Option<PathBuf>,
    #[serde(default)]
    pub service: Vec<ServiceConfig>,
    #[serde(default)]
    pub llm: Vec<LlmEndpointConfig>,
}

impl AgentConfig {
    /// Loads and parses a strict agent TOML configuration.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be read or contains invalid TOML.
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let raw =
            fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        toml::from_str(&raw).with_context(|| format!("parsing {}", path.display()))
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceConfig {
    pub name: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LlmEndpointConfig {
    pub id: String,
    pub backend: Backend,
    pub base_url: String,
    #[serde(default)]
    pub served_model_id: Option<String>,
    #[serde(default)]
    pub context_capacity: Option<u64>,
    #[serde(default)]
    pub auth_header_file: Option<PathBuf>,
    #[serde(default)]
    pub tls_ca_file: Option<PathBuf>,
    #[serde(default = "default_metrics_path")]
    pub metrics_path: String,
    #[serde(default)]
    pub metric_names: MetricNames,
}

fn default_metrics_path() -> String {
    "/metrics".to_owned()
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Backend {
    Sglang,
    Vllm,
    LlamaCpp,
    Openai,
    Custom,
}

impl Backend {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Sglang => "sglang",
            Self::Vllm => "vllm",
            Self::LlamaCpp => "llama_cpp",
            Self::Openai => "openai",
            Self::Custom => "custom",
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetricNames {
    pub running: Option<String>,
    pub queued: Option<String>,
    pub input_tokens: Option<String>,
    pub output_tokens: Option<String>,
    pub generation_rate: Option<String>,
    pub uptime: Option<String>,
}

struct Endpoint {
    config: LlmEndpointConfig,
    client: Client,
    auth: Option<(HeaderName, HeaderValue)>,
}

#[derive(Debug, Clone, Copy)]
struct Baseline {
    value: f64,
    observed: Instant,
}

pub struct LlmProbe {
    endpoints: Vec<Endpoint>,
    baselines: BTreeMap<(String, String), Baseline>,
    started: Instant,
    last_success: BTreeMap<String, Instant>,
}

impl LlmProbe {
    /// Builds clients for all configured model-server endpoints.
    ///
    /// # Errors
    ///
    /// Returns an error for an unreadable auth/CA file, invalid header, invalid
    /// CA certificate, or failure to construct an HTTP client.
    pub fn new(configs: Vec<LlmEndpointConfig>) -> Result<Self> {
        let endpoints = configs
            .into_iter()
            .map(build_endpoint)
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            endpoints,
            baselines: BTreeMap::new(),
            started: Instant::now(),
            last_success: BTreeMap::new(),
        })
    }

    #[allow(clippy::too_many_lines)]
    pub async fn collect(&mut self) -> Vec<MetricPoint> {
        let mut points = Vec::new();
        for index in 0..self.endpoints.len() {
            let result = fetch_endpoint(&self.endpoints[index]).await;
            let id = self.endpoints[index].config.id.clone();
            let backend = self.endpoints[index].config.backend;
            let model = self.endpoints[index].config.served_model_id.clone();
            let decorate = |point: MetricPoint| {
                let point = point
                    .with_attribute("llm.endpoint.id", &id)
                    .with_attribute("llm.backend", backend.as_str());
                model.as_ref().map_or(point.clone(), |model| {
                    point.with_attribute("llm.model.id", model)
                })
            };
            points.push(decorate(
                self.endpoints[index].config.context_capacity.map_or_else(
                    || {
                        MetricPoint::unavailable(
                            "spark.llm.context.capacity",
                            "{token}",
                            Quality::Unsupported,
                            "config",
                            "CONTEXT_CAPACITY_NOT_CONFIGURED",
                        )
                    },
                    |capacity| {
                        MetricPoint::gauge(
                            "spark.llm.context.capacity",
                            capacity as f64,
                            "{token}",
                            Quality::Measured,
                            "config",
                        )
                    },
                ),
            ));
            match result {
                Ok(metrics) => {
                    let now = Instant::now();
                    self.last_success.insert(id.clone(), now);
                    points.push(decorate(MetricPoint::gauge(
                        "spark.llm.response.age",
                        0.0,
                        "s",
                        Quality::Derived,
                        "http",
                    )));
                    points.push(decorate(MetricPoint::gauge(
                        "spark.llm.collection.errors",
                        0.0,
                        "{error}",
                        Quality::Measured,
                        "http",
                    )));
                    points.push(decorate(MetricPoint::gauge(
                        "spark.llm.available",
                        1.0,
                        "1",
                        Quality::Measured,
                        "http",
                    )));
                    let names = resolved_names(&self.endpoints[index].config);
                    points.push(decorate(metric_value(
                        &metrics,
                        names.uptime.as_deref(),
                        "spark.llm.uptime",
                        "s",
                    )));
                    for (metric_name, source_name) in [
                        ("spark.llm.requests.running", names.running),
                        ("spark.llm.requests.queued", names.queued),
                    ] {
                        points.push(decorate(metric_value(
                            &metrics,
                            source_name.as_deref(),
                            metric_name,
                            "{request}",
                        )));
                    }
                    for (metric_name, rate_name, source_name) in [
                        (
                            "spark.llm.tokens.input",
                            "spark.llm.tokens.prefill.rate",
                            names.input_tokens,
                        ),
                        (
                            "spark.llm.tokens.output",
                            "spark.llm.tokens.generation.rate",
                            names.output_tokens,
                        ),
                    ] {
                        let value = source_name
                            .as_deref()
                            .and_then(|name| metrics.get(name))
                            .copied();
                        let Some(value) = value else {
                            points.push(decorate(MetricPoint::unavailable(
                                metric_name,
                                "{token}",
                                Quality::Unsupported,
                                "prometheus",
                                "METRIC_NOT_EXPOSED",
                            )));
                            points.push(decorate(MetricPoint::unavailable(
                                rate_name,
                                "{token}/s",
                                Quality::Unsupported,
                                "prometheus",
                                "METRIC_NOT_EXPOSED",
                            )));
                            continue;
                        };
                        let key = (id.clone(), metric_name.to_owned());
                        let now = Instant::now();
                        let previous = self.baselines.insert(
                            key,
                            Baseline {
                                value,
                                observed: now,
                            },
                        );
                        if let Some(previous) = previous.filter(|old| value >= old.value) {
                            let elapsed = now.duration_since(previous.observed).as_secs_f64();
                            let delta = value - previous.value;
                            points.push(decorate(MetricPoint::counter_delta(
                                metric_name,
                                delta,
                                "{token}",
                                "prometheus",
                            )));
                            points.push(decorate(MetricPoint::gauge(
                                rate_name,
                                if elapsed > 0.0 { delta / elapsed } else { 0.0 },
                                "{token}/s",
                                Quality::Derived,
                                "prometheus",
                            )));
                        } else {
                            points.push(decorate(MetricPoint::unavailable(
                                metric_name,
                                "{token}",
                                Quality::Stale,
                                "prometheus",
                                "BASELINE_INITIALIZING",
                            )));
                            points.push(decorate(MetricPoint::unavailable(
                                rate_name,
                                "{token}/s",
                                Quality::Stale,
                                "prometheus",
                                "BASELINE_INITIALIZING",
                            )));
                        }
                    }
                    if let Some(name) = names.generation_rate
                        && let Some(value) = metrics.get(&name)
                    {
                        points.push(decorate(MetricPoint::gauge(
                            "spark.llm.tokens.generation.rate",
                            *value,
                            "{token}/s",
                            Quality::Measured,
                            "prometheus",
                        )));
                    }
                }
                Err(code) => {
                    let now = Instant::now();
                    let age = self.last_success.get(&id).map_or_else(
                        || now.duration_since(self.started),
                        |last| now.duration_since(*last),
                    );
                    points.push(decorate(MetricPoint::gauge(
                        "spark.llm.response.age",
                        age.as_secs_f64(),
                        "s",
                        Quality::Derived,
                        "http",
                    )));
                    points.push(decorate(MetricPoint::gauge(
                        "spark.llm.collection.errors",
                        1.0,
                        "{error}",
                        Quality::Measured,
                        "http",
                    )));
                    points.push(decorate(MetricPoint::unavailable(
                        "spark.llm.available",
                        "1",
                        Quality::Error,
                        "http",
                        code,
                    )));
                }
            }
        }
        points
    }
}

#[derive(Default)]
pub struct ServiceProbe {
    cgroup_baselines: BTreeMap<(String, String), u64>,
}

impl ServiceProbe {
    #[must_use]
    pub fn collect(&mut self, configs: &[ServiceConfig]) -> Vec<MetricPoint> {
        let mut points = Vec::new();
        for config in configs {
            let output = Command::new("systemctl")
                .args([
                    "show",
                    &config.name,
                    "--property=ActiveState",
                    "--property=SubState",
                    "--property=NRestarts",
                    "--property=MainPID",
                    "--property=ActiveEnterTimestampMonotonic",
                    "--property=ControlGroup",
                ])
                .output();
            let attrs = |point: MetricPoint| point.with_attribute("systemd.unit", &config.name);
            match output {
                Ok(output) if output.status.success() => {
                    let raw = String::from_utf8_lossy(&output.stdout);
                    let values = parse_properties(&raw);
                    let active = values
                        .get("ActiveState")
                        .is_some_and(|value| *value == "active");
                    let mut active_point = attrs(MetricPoint::gauge(
                        "spark.service.active",
                        f64::from(active),
                        "1",
                        Quality::Measured,
                        "systemd",
                    ));
                    if let Some(substate) = values.get("SubState") {
                        active_point = active_point.with_attribute("systemd.substate", *substate);
                    }
                    for (property, attribute) in [
                        ("MainPID", "process.pid"),
                        (
                            "ActiveEnterTimestampMonotonic",
                            "systemd.active_enter_timestamp_monotonic_us",
                        ),
                    ] {
                        if let Some(value) = values.get(property).filter(|value| **value != "0") {
                            active_point = active_point.with_attribute(attribute, *value);
                        }
                    }
                    let restarts = values
                        .get("NRestarts")
                        .and_then(|value| value.parse::<f64>().ok())
                        .unwrap_or(0.0);
                    points.extend([
                        active_point,
                        attrs(MetricPoint::gauge(
                            "spark.service.restarts",
                            restarts,
                            "{restart}",
                            Quality::Measured,
                            "systemd",
                        )),
                    ]);
                    if let Some(control_group) = values.get("ControlGroup")
                        && !control_group.is_empty()
                    {
                        points.extend(self.collect_cgroup(&config.name, control_group));
                    }
                }
                _ => points.push(attrs(MetricPoint::unavailable(
                    "spark.service.active",
                    "1",
                    Quality::Error,
                    "systemd",
                    "SYSTEMD_QUERY_FAILED",
                ))),
            }
        }
        points
    }

    fn collect_cgroup(&mut self, unit: &str, control_group: &str) -> Vec<MetricPoint> {
        let root = Path::new("/sys/fs/cgroup").join(control_group.trim_start_matches('/'));
        let mut points = Vec::new();
        if let Ok(raw) = fs::read_to_string(root.join("memory.events")) {
            for (event, value) in parse_u64_properties(&raw) {
                let (name, unit_name) = if event == "oom_kill" {
                    ("system.memory.oom_kills", "{event}")
                } else {
                    ("system.memory.cgroup.events", "{event}")
                };
                points.push(
                    self.cgroup_delta(unit, event, value, name, unit_name)
                        .with_attribute("cgroup.memory.event", event),
                );
            }
        }
        if let Ok(raw) = fs::read_to_string(root.join("memory.stat"))
            && let Some(value) = parse_u64_properties(&raw).get("pgscan").copied()
        {
            points.push(
                self.cgroup_delta(unit, "pgscan", value, "system.memory.reclaim", "{page}")
                    .with_attribute("cgroup.memory.event", "pgscan"),
            );
        }
        points
    }

    fn cgroup_delta(
        &mut self,
        unit: &str,
        event: &str,
        value: u64,
        metric: &str,
        metric_unit: &str,
    ) -> MetricPoint {
        let key = (unit.to_owned(), event.to_owned());
        let previous = self.cgroup_baselines.insert(key, value);
        previous
            .map_or_else(
                || {
                    MetricPoint::unavailable(
                        metric,
                        metric_unit,
                        Quality::Stale,
                        "cgroupfs",
                        "BASELINE_INITIALIZING",
                    )
                },
                |old| {
                    MetricPoint::counter_delta(
                        metric,
                        value.saturating_sub(old) as f64,
                        metric_unit,
                        "cgroupfs",
                    )
                },
            )
            .with_attribute("systemd.unit", unit)
            .with_attribute("cgroup.scope", "service")
    }
}

fn parse_u64_properties(raw: &str) -> BTreeMap<&str, u64> {
    raw.lines()
        .filter_map(|line| {
            let (key, value) = line.split_once(char::is_whitespace)?;
            Some((key, value.trim().parse().ok()?))
        })
        .collect()
}

fn build_endpoint(config: LlmEndpointConfig) -> Result<Endpoint> {
    let mut builder = Client::builder().timeout(Duration::from_secs(3));
    if let Some(path) = &config.tls_ca_file {
        let bytes = fs::read(path).with_context(|| format!("reading TLS CA {}", path.display()))?;
        builder = builder.add_root_certificate(
            reqwest::Certificate::from_pem(&bytes).context("parsing TLS CA")?,
        );
    }
    let auth = config
        .auth_header_file
        .as_ref()
        .map(|path| read_auth_header(path))
        .transpose()?;
    Ok(Endpoint {
        config,
        client: builder.build().context("building LLM HTTP client")?,
        auth,
    })
}

fn read_auth_header(path: &Path) -> Result<(HeaderName, HeaderValue)> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("reading auth header {}", path.display()))?;
    let (name, value) = raw
        .trim()
        .split_once(':')
        .context("auth header must be 'Name: value'")?;
    Ok((
        HeaderName::from_bytes(name.trim().as_bytes()).context("invalid auth header name")?,
        HeaderValue::from_str(value.trim()).context("invalid auth header value")?,
    ))
}

async fn fetch_endpoint(endpoint: &Endpoint) -> Result<BTreeMap<String, f64>, &'static str> {
    let path = if matches!(endpoint.config.backend, Backend::Openai) {
        "/v1/models"
    } else {
        &endpoint.config.metrics_path
    };
    let url = format!("{}{}", endpoint.config.base_url.trim_end_matches('/'), path);
    let mut request = endpoint.client.get(url);
    if let Some((name, value)) = &endpoint.auth {
        request = request.header(name, value);
    }
    let response = request.send().await.map_err(|_| "ENDPOINT_UNREACHABLE")?;
    if !response.status().is_success() {
        return Err("ENDPOINT_HTTP_ERROR");
    }
    if matches!(endpoint.config.backend, Backend::Openai) {
        return Ok(BTreeMap::new());
    }
    let body = response.text().await.map_err(|_| "ENDPOINT_BODY_ERROR")?;
    Ok(parse_prometheus(&body))
}

fn resolved_names(config: &LlmEndpointConfig) -> MetricNames {
    let some = |value: &str| Some(value.to_owned());
    let defaults = match config.backend {
        Backend::Sglang => MetricNames {
            running: some("sglang:num_running_reqs"),
            queued: some("sglang:num_queue_reqs"),
            input_tokens: some("sglang:prompt_tokens_total"),
            output_tokens: some("sglang:generation_tokens_total"),
            generation_rate: some("sglang:gen_throughput"),
            uptime: None,
        },
        Backend::Vllm => MetricNames {
            running: some("vllm:num_requests_running"),
            queued: some("vllm:num_requests_waiting"),
            input_tokens: some("vllm:prompt_tokens_total"),
            output_tokens: some("vllm:generation_tokens_total"),
            generation_rate: None,
            uptime: None,
        },
        Backend::LlamaCpp => MetricNames {
            running: some("llamacpp:requests_processing"),
            queued: some("llamacpp:requests_deferred"),
            input_tokens: some("llamacpp:prompt_tokens_total"),
            output_tokens: some("llamacpp:tokens_predicted_total"),
            generation_rate: some("llamacpp:predicted_tokens_seconds"),
            uptime: None,
        },
        Backend::Openai | Backend::Custom => MetricNames::default(),
    };
    MetricNames {
        running: config.metric_names.running.clone().or(defaults.running),
        queued: config.metric_names.queued.clone().or(defaults.queued),
        input_tokens: config
            .metric_names
            .input_tokens
            .clone()
            .or(defaults.input_tokens),
        output_tokens: config
            .metric_names
            .output_tokens
            .clone()
            .or(defaults.output_tokens),
        generation_rate: config
            .metric_names
            .generation_rate
            .clone()
            .or(defaults.generation_rate),
        uptime: config.metric_names.uptime.clone().or(defaults.uptime),
    }
}

fn metric_value(
    metrics: &BTreeMap<String, f64>,
    source_name: Option<&str>,
    output_name: &str,
    unit: &str,
) -> MetricPoint {
    source_name.and_then(|name| metrics.get(name)).map_or_else(
        || {
            MetricPoint::unavailable(
                output_name,
                unit,
                Quality::Unsupported,
                "prometheus",
                "METRIC_NOT_EXPOSED",
            )
        },
        |value| MetricPoint::gauge(output_name, *value, unit, Quality::Measured, "prometheus"),
    )
}

fn parse_prometheus(raw: &str) -> BTreeMap<String, f64> {
    let mut metrics = BTreeMap::new();
    for line in raw
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
    {
        let mut fields = line.split_whitespace();
        let Some(raw_name) = fields.next() else {
            continue;
        };
        let Some(value) = fields.next().and_then(|value| value.parse::<f64>().ok()) else {
            continue;
        };
        let name = raw_name.split_once('{').map_or(raw_name, |(name, _)| name);
        *metrics.entry(name.to_owned()).or_insert(0.0) += value;
    }
    metrics
}

fn parse_properties(raw: &str) -> BTreeMap<&str, &str> {
    raw.lines()
        .filter_map(|line| line.split_once('='))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_sums_prometheus_series() {
        let values = parse_prometheus(
            "# HELP ignored\nsglang:num_running_reqs{model_name=\"a\"} 2\nsglang:num_running_reqs{model_name=\"b\"} 3\nsglang:prompt_tokens_total 1.2e3\n",
        );
        assert!((values["sglang:num_running_reqs"] - 5.0).abs() < f64::EPSILON);
        assert!((values["sglang:prompt_tokens_total"] - 1200.0).abs() < f64::EPSILON);
    }

    #[test]
    fn custom_metric_names_override_defaults() {
        let config = LlmEndpointConfig {
            id: "x".to_owned(),
            backend: Backend::Sglang,
            base_url: "http://localhost".to_owned(),
            served_model_id: None,
            context_capacity: None,
            auth_header_file: None,
            tls_ca_file: None,
            metrics_path: "/metrics".to_owned(),
            metric_names: MetricNames {
                running: Some("custom_running".to_owned()),
                ..MetricNames::default()
            },
        };
        assert_eq!(
            resolved_names(&config).running.as_deref(),
            Some("custom_running")
        );
    }

    #[test]
    fn parses_cgroup_event_counters() {
        let values = parse_u64_properties("low 1\nhigh 2\noom_kill 3\n");
        assert_eq!(values.get("high"), Some(&2));
        assert_eq!(values.get("oom_kill"), Some(&3));
    }
}
