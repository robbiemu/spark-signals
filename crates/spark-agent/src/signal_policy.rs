use std::{
    collections::{BTreeMap, HashMap},
    fs,
    path::Path,
};

use anyhow::{Context, Result};
use serde::Deserialize;
use spark_schema::{
    MAX_ATTRIBUTE_VALUE_BYTES, MetricPoint, Quality, attribute_name_allowed, metric_name_allowed,
};

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum SignalPolicy {
    Enabled,
    Disabled,
    StateChange,
    AvailabilityChange,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SignalSelector {
    source: String,
    metric: String,
    attributes: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
struct CompiledRule {
    selector: SignalSelector,
    policy: SignalPolicy,
    reason: String,
}

#[derive(Debug, Clone, Default)]
struct SignalPolicySet {
    rules: Vec<CompiledRule>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PointKey {
    source: String,
    metric: String,
    attributes: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ObservationState {
    value_bits: Option<u64>,
    quality: Quality,
    error_code: Option<String>,
}

pub(crate) struct SignalEmitter {
    policies: SignalPolicySet,
    states: HashMap<PointKey, ObservationState>,
    availability: HashMap<PointKey, bool>,
}

impl SignalEmitter {
    pub(crate) fn load(directory: Option<&Path>) -> Result<Self> {
        Ok(Self {
            policies: directory
                .map_or_else(|| Ok(SignalPolicySet::default()), load_policy_directory)?,
            states: HashMap::new(),
            availability: HashMap::new(),
        })
    }

    pub(crate) fn filter(&mut self, points: Vec<MetricPoint>) -> Vec<MetricPoint> {
        points
            .into_iter()
            .filter(|point| self.should_emit(point))
            .collect()
    }

    fn should_emit(&mut self, point: &MetricPoint) -> bool {
        let policy = self.policies.policy_for(point);
        let key = PointKey::from(point);
        match policy {
            SignalPolicy::Enabled => true,
            SignalPolicy::Disabled => false,
            SignalPolicy::StateChange => {
                let next = ObservationState::from(point);
                self.states.insert(key, next.clone()).as_ref() != Some(&next)
            }
            SignalPolicy::AvailabilityChange => {
                let available = point.value.is_some();
                let previous = self.availability.insert(key, available);
                available || previous != Some(false)
            }
        }
    }
}

impl SignalPolicySet {
    fn policy_for(&self, point: &MetricPoint) -> SignalPolicy {
        self.rules
            .iter()
            .filter(|rule| rule.selector.matches(point))
            .max_by_key(|rule| rule.selector.attributes.len())
            .map_or(SignalPolicy::Enabled, |rule| rule.policy)
    }
}

impl SignalSelector {
    fn matches(&self, point: &MetricPoint) -> bool {
        self.source == point.source
            && self.metric == point.name
            && self
                .attributes
                .iter()
                .all(|(key, value)| point.attributes.get(key) == Some(value))
    }

    fn can_overlap(&self, other: &Self) -> bool {
        self.source == other.source
            && self.metric == other.metric
            && self.attributes.len() == other.attributes.len()
            && self
                .attributes
                .iter()
                .all(|(key, value)| other.attributes.get(key).is_none_or(|other| other == value))
    }
}

impl From<&MetricPoint> for PointKey {
    fn from(point: &MetricPoint) -> Self {
        Self {
            source: point.source.clone(),
            metric: point.name.clone(),
            attributes: point.attributes.clone(),
        }
    }
}

impl From<&MetricPoint> for ObservationState {
    fn from(point: &MetricPoint) -> Self {
        Self {
            value_bits: point.value.map(f64::to_bits),
            quality: point.quality.clone(),
            error_code: point.error_code.clone(),
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct PolicyFile {
    #[serde(default)]
    signal_policy: Vec<PolicyEntry>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PolicyEntry {
    source: String,
    metric: String,
    #[serde(default)]
    attributes: BTreeMap<String, String>,
    policy: SignalPolicy,
    reason: String,
}

impl PolicyEntry {
    fn validate(&self) -> Result<()> {
        if !metric_name_allowed(&self.metric) {
            anyhow::bail!("unknown v1 metric in signal policy: {}", self.metric);
        }
        if !source_allowed(&self.source) {
            anyhow::bail!("unknown signal source in signal policy: {}", self.source);
        }
        if self.attributes.iter().any(|(key, value)| {
            !attribute_name_allowed(key)
                || value.is_empty()
                || value.len() > MAX_ATTRIBUTE_VALUE_BYTES
        }) {
            anyhow::bail!("unknown or invalid v1 attribute in signal policy");
        }
        if self.reason.is_empty() || self.reason.len() > MAX_ATTRIBUTE_VALUE_BYTES {
            anyhow::bail!("signal policy reason is empty or too long");
        }
        Ok(())
    }

    fn into_rule(self) -> CompiledRule {
        CompiledRule {
            selector: SignalSelector {
                source: self.source,
                metric: self.metric,
                attributes: self.attributes,
            },
            policy: self.policy,
            reason: self.reason,
        }
    }
}

fn load_policy_directory(directory: &Path) -> Result<SignalPolicySet> {
    let mut paths = fs::read_dir(directory)
        .with_context(|| format!("reading signal policy directory {}", directory.display()))?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<std::io::Result<Vec<_>>>()?;
    paths.retain(|path| {
        path.extension()
            .is_some_and(|extension| extension == "toml")
    });
    paths.sort();

    let mut rules: Vec<CompiledRule> = Vec::new();
    for path in paths {
        let raw = fs::read_to_string(&path)
            .with_context(|| format!("reading signal policy file {}", path.display()))?;
        let file: PolicyFile = toml::from_str(&raw)
            .with_context(|| format!("parsing signal policy file {}", path.display()))?;
        for entry in file.signal_policy {
            entry.validate()?;
            let rule = entry.into_rule();
            if let Some(existing) = rules
                .iter()
                .find(|existing| existing.selector.can_overlap(&rule.selector))
            {
                anyhow::bail!(
                    "ambiguous equal-specificity signal policies for {} from {}: {} and {}",
                    rule.selector.metric,
                    rule.selector.source,
                    existing.reason,
                    rule.reason
                );
            }
            rules.push(rule);
        }
    }
    Ok(SignalPolicySet { rules })
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

    fn point(value: Option<f64>, quality: &Quality) -> MetricPoint {
        let mut point = value.map_or_else(
            || {
                MetricPoint::unavailable(
                    "nvidia.gpu.clock.frequency",
                    "MHz",
                    quality.clone(),
                    "nvml",
                    "NVML_NOTSUPPORTED",
                )
            },
            |value| {
                MetricPoint::gauge(
                    "nvidia.gpu.clock.frequency",
                    value,
                    "MHz",
                    quality.clone(),
                    "nvml",
                )
            },
        );
        point
            .attributes
            .insert("clock.domain".to_owned(), "memory".to_owned());
        point
    }

    fn temp_directory() -> std::path::PathBuf {
        let id = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("spark-signal-policy-{}-{id}", std::process::id()));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir(&path).unwrap();
        path
    }

    fn emitter(policy: &str, attributes: &str) -> (SignalEmitter, std::path::PathBuf) {
        let directory = temp_directory();
        fs::write(
            directory.join("policy.toml"),
            format!(
                r#"[[signal_policy]]
source = "nvml"
metric = "nvidia.gpu.clock.frequency"
attributes = {attributes}
policy = "{policy}"
reason = "TEST_POLICY"
"#
            ),
        )
        .unwrap();
        (SignalEmitter::load(Some(&directory)).unwrap(), directory)
    }

    #[test]
    fn disabled_suppresses_matching_points_only() {
        let (mut emitter, directory) = emitter("disabled", r#"{ "clock.domain" = "memory" }"#);
        let memory = point(Some(100.0), &Quality::Measured);
        let mut graphics = point(Some(200.0), &Quality::Measured);
        graphics
            .attributes
            .insert("clock.domain".to_owned(), "graphics".to_owned());
        assert_eq!(emitter.filter(vec![memory, graphics]).len(), 1);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn state_change_emits_first_and_changed_observations() {
        let (mut emitter, directory) = emitter("state-change", "{}");
        assert_eq!(
            emitter
                .filter(vec![point(Some(100.0), &Quality::Measured)])
                .len(),
            1
        );
        assert!(
            emitter
                .filter(vec![point(Some(100.0), &Quality::Measured)])
                .is_empty()
        );
        assert_eq!(
            emitter
                .filter(vec![point(Some(101.0), &Quality::Measured)])
                .len(),
            1
        );
        assert_eq!(
            emitter
                .filter(vec![point(None, &Quality::Unsupported)])
                .len(),
            1
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn availability_change_suppresses_only_repeated_unavailable_observations() {
        let (mut emitter, directory) = emitter("availability-change", "{}");
        assert_eq!(
            emitter
                .filter(vec![point(Some(100.0), &Quality::Measured)])
                .len(),
            1
        );
        assert_eq!(
            emitter
                .filter(vec![point(Some(100.0), &Quality::Measured)])
                .len(),
            1
        );
        assert_eq!(
            emitter
                .filter(vec![point(None, &Quality::Unsupported)])
                .len(),
            1
        );
        assert!(
            emitter
                .filter(vec![point(None, &Quality::Unsupported)])
                .is_empty()
        );
        assert_eq!(
            emitter
                .filter(vec![point(Some(100.0), &Quality::Measured)])
                .len(),
            1
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn specific_rule_overrides_metric_wide_rule() {
        let directory = temp_directory();
        fs::write(
            directory.join("policy.toml"),
            r#"[[signal_policy]]
source = "nvml"
metric = "nvidia.gpu.clock.frequency"
policy = "disabled"
reason = "ALL_CLOCKS"

[[signal_policy]]
source = "nvml"
metric = "nvidia.gpu.clock.frequency"
attributes = { "clock.domain" = "graphics" }
policy = "enabled"
reason = "GRAPHICS_REQUIRED"
"#,
        )
        .unwrap();
        let mut emitter = SignalEmitter::load(Some(&directory)).unwrap();
        let mut graphics = point(Some(200.0), &Quality::Measured);
        graphics
            .attributes
            .insert("clock.domain".to_owned(), "graphics".to_owned());
        assert_eq!(
            emitter
                .filter(vec![point(Some(100.0), &Quality::Measured), graphics])
                .len(),
            1
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn invalid_reload_preserves_active_policy() {
        let (mut emitter, directory) = emitter("disabled", "{}");
        fs::write(directory.join("invalid.toml"), "unknown = true").unwrap();
        assert!(SignalEmitter::load(Some(&directory)).is_err());
        assert!(
            emitter
                .filter(vec![point(Some(100.0), &Quality::Measured)])
                .is_empty()
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn rejects_unknown_and_ambiguous_rules() {
        let directory = temp_directory();
        fs::write(
            directory.join("unknown.toml"),
            r#"[[signal_policy]]
source = "nvml"
metric = "unknown.metric"
policy = "disabled"
reason = "UNKNOWN"
"#,
        )
        .unwrap();
        assert!(SignalEmitter::load(Some(&directory)).is_err());
        fs::remove_file(directory.join("unknown.toml")).unwrap();
        for name in ["a.toml", "b.toml"] {
            fs::write(
                directory.join(name),
                format!(
                    r#"[[signal_policy]]
source = "nvml"
metric = "nvidia.gpu.clock.frequency"
policy = "disabled"
reason = "{name}"
"#
                ),
            )
            .unwrap();
        }
        assert!(SignalEmitter::load(Some(&directory)).is_err());
        fs::remove_dir_all(directory).unwrap();
    }
}
