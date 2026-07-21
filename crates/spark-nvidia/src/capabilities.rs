use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};

use anyhow::{Context, Result};
use serde::Deserialize;
use spark_schema::{MAX_ATTRIBUTE_VALUE_BYTES, attribute_name_allowed, metric_name_allowed};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct CapabilityKey {
    pub source: String,
    pub metric: String,
    pub attributes: BTreeMap<String, String>,
}

impl CapabilityKey {
    #[must_use]
    pub fn new(
        source: impl Into<String>,
        metric: impl Into<String>,
        attributes: BTreeMap<String, String>,
    ) -> Self {
        Self {
            source: source.into(),
            metric: metric.into(),
            attributes,
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityPolicy {
    Auto,
    Enabled,
    Disabled,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HardwareIdentity {
    pub gpu_products: BTreeSet<String>,
    pub pci_device_ids: BTreeSet<String>,
    pub driver_version: Option<String>,
}

impl HardwareIdentity {
    #[must_use]
    pub fn lifecycle_key(&self) -> String {
        format!(
            "products={:?};pci={:?};driver={}",
            self.gpu_products,
            self.pci_device_ids,
            self.driver_version.as_deref().unwrap_or("")
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapabilityRule {
    pub policy: CapabilityPolicy,
    pub reason: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResolvedCapabilities {
    rules: BTreeMap<CapabilityKey, CapabilityRule>,
}

impl ResolvedCapabilities {
    #[must_use]
    pub fn rule(&self, key: &CapabilityKey) -> Option<&CapabilityRule> {
        self.rules.get(key)
    }

    pub fn inventory_attributes(
        &self,
        auto_unsupported: &BTreeSet<CapabilityKey>,
    ) -> BTreeMap<String, String> {
        let mut inventory = BTreeMap::new();
        for (index, (key, rule)) in self
            .rules
            .iter()
            .filter(|(_, rule)| rule.policy == CapabilityPolicy::Disabled)
            .enumerate()
        {
            insert_inventory_capability(&mut inventory, index, key, "disabled", &rule.reason);
        }
        let offset = inventory.len() / 5;
        for (index, key) in auto_unsupported.iter().enumerate() {
            insert_inventory_capability(
                &mut inventory,
                offset + index,
                key,
                "unsupported",
                "NVML_NOTSUPPORTED",
            );
        }
        inventory
    }
}

fn insert_inventory_capability(
    inventory: &mut BTreeMap<String, String>,
    index: usize,
    key: &CapabilityKey,
    state: &str,
    reason: &str,
) {
    let prefix = format!("spark.capability.{index}");
    inventory.insert(format!("{prefix}.source"), key.source.clone());
    inventory.insert(format!("{prefix}.metric"), key.metric.clone());
    inventory.insert(
        format!("{prefix}.attributes"),
        key.attributes
            .iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>()
            .join(","),
    );
    inventory.insert(format!("{prefix}.state"), state.to_owned());
    inventory.insert(format!("{prefix}.reason"), reason.to_owned());
}

#[derive(Debug)]
pub struct CapabilityRuntime {
    identity: HardwareIdentity,
    resolved: ResolvedCapabilities,
    automatic_unsupported: BTreeSet<CapabilityKey>,
}

impl CapabilityRuntime {
    pub fn load(directory: Option<&Path>, identity: HardwareIdentity) -> Result<Self> {
        let resolved = directory.map_or_else(
            || Ok(ResolvedCapabilities::default()),
            |directory| load_directory(directory, &identity),
        )?;
        Ok(Self {
            identity,
            resolved,
            automatic_unsupported: BTreeSet::new(),
        })
    }

    pub fn reload(&mut self, directory: Option<&Path>, identity: HardwareIdentity) -> Result<()> {
        let next = directory.map_or_else(
            || Ok(ResolvedCapabilities::default()),
            |directory| load_directory(directory, &identity),
        )?;
        self.identity = identity;
        self.resolved = next;
        self.automatic_unsupported.clear();
        Ok(())
    }

    pub fn update_identity(&mut self, identity: HardwareIdentity) {
        if identity.lifecycle_key() != self.identity.lifecycle_key() {
            self.identity = identity;
            self.automatic_unsupported.clear();
        }
    }

    #[must_use]
    pub fn policy(&self, key: &CapabilityKey) -> CapabilityPolicy {
        self.resolved
            .rule(key)
            .map_or(CapabilityPolicy::Auto, |rule| rule.policy)
    }

    #[must_use]
    pub fn should_query(&self, key: &CapabilityKey) -> bool {
        match self.policy(key) {
            CapabilityPolicy::Disabled => false,
            CapabilityPolicy::Auto => !self.automatic_unsupported.contains(key),
            CapabilityPolicy::Enabled => true,
        }
    }

    pub fn observe_definitive_unsupported(&mut self, key: &CapabilityKey) {
        if self.policy(key) == CapabilityPolicy::Auto {
            self.automatic_unsupported.insert(key.clone());
        }
    }

    #[must_use]
    pub fn inventory_attributes(&self) -> BTreeMap<String, String> {
        self.resolved
            .inventory_attributes(&self.automatic_unsupported)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProfileFile {
    hardware_profile: Vec<HardwareProfile>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct HardwareProfile {
    id: String,
    #[serde(rename = "match")]
    selector: HardwareSelector,
    capability: Vec<ProfileCapability>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct HardwareSelector {
    gpu_product: Option<String>,
    pci_device_id: Option<String>,
}

impl HardwareSelector {
    fn validate(&self) -> Result<()> {
        if self.gpu_product.is_none() && self.pci_device_id.is_none() {
            anyhow::bail!("hardware selector must contain gpu_product or pci_device_id");
        }
        if self.gpu_product.as_deref().is_some_and(str::is_empty)
            || self.pci_device_id.as_deref().is_some_and(str::is_empty)
        {
            anyhow::bail!("hardware selector values must not be empty");
        }
        Ok(())
    }

    fn matches(&self, identity: &HardwareIdentity) -> bool {
        self.gpu_product
            .as_ref()
            .is_none_or(|product| identity.gpu_products.contains(product))
            && self
                .pci_device_id
                .as_ref()
                .is_none_or(|id| identity.pci_device_ids.contains(&id.to_ascii_lowercase()))
    }

    fn specificity(&self) -> u8 {
        u8::from(self.gpu_product.is_some()) + u8::from(self.pci_device_id.is_some())
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProfileCapability {
    metric: String,
    source: String,
    #[serde(default)]
    attributes: BTreeMap<String, String>,
    policy: CapabilityPolicy,
    reason: String,
}

impl ProfileCapability {
    fn validate(&self) -> Result<()> {
        if !metric_name_allowed(&self.metric) {
            anyhow::bail!("unknown v1 metric in hardware capability: {}", self.metric);
        }
        if self.source != "nvml" {
            anyhow::bail!("unknown hardware capability source: {}", self.source);
        }
        if self.attributes.iter().any(|(key, value)| {
            !attribute_name_allowed(key)
                || value.is_empty()
                || value.len() > MAX_ATTRIBUTE_VALUE_BYTES
        }) {
            anyhow::bail!("unknown or invalid v1 attribute in hardware capability");
        }
        if self.reason.is_empty() || self.reason.len() > MAX_ATTRIBUTE_VALUE_BYTES {
            anyhow::bail!("hardware capability reason is empty or too long");
        }
        Ok(())
    }

    fn key(&self) -> CapabilityKey {
        CapabilityKey::new(&self.source, &self.metric, self.attributes.clone())
    }
}

pub fn load_directory(
    directory: &Path,
    identity: &HardwareIdentity,
) -> Result<ResolvedCapabilities> {
    let mut paths = fs::read_dir(directory)
        .with_context(|| {
            format!(
                "reading hardware capability directory {}",
                directory.display()
            )
        })?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<std::io::Result<Vec<_>>>()?;
    paths.retain(|path| {
        path.extension()
            .is_some_and(|extension| extension == "toml")
    });
    paths.sort();

    let mut profile_ids = BTreeSet::new();
    let mut selected = BTreeMap::<CapabilityKey, (u8, CapabilityRule, String)>::new();
    for path in paths {
        let raw = fs::read_to_string(&path)
            .with_context(|| format!("reading hardware capability profile {}", path.display()))?;
        let file: ProfileFile = toml::from_str(&raw)
            .with_context(|| format!("parsing hardware capability profile {}", path.display()))?;
        if file.hardware_profile.is_empty() {
            anyhow::bail!(
                "hardware capability file has no profiles: {}",
                path.display()
            );
        }
        for profile in file.hardware_profile {
            if profile.id.is_empty() || !profile_ids.insert(profile.id.clone()) {
                anyhow::bail!(
                    "hardware capability profile id is empty or duplicated: {}",
                    profile.id
                );
            }
            profile.selector.validate()?;
            if profile.capability.is_empty() {
                anyhow::bail!(
                    "hardware capability profile has no capabilities: {}",
                    profile.id
                );
            }
            for capability in &profile.capability {
                capability.validate()?;
            }
            if !profile.selector.matches(identity) {
                continue;
            }
            let specificity = profile.selector.specificity();
            for capability in profile.capability {
                let key = capability.key();
                let rule = CapabilityRule {
                    policy: capability.policy,
                    reason: capability.reason,
                };
                match selected.get(&key) {
                    Some((existing_specificity, _, existing_profile))
                        if *existing_specificity == specificity =>
                    {
                        anyhow::bail!(
                            "ambiguous equal-specificity capability {} in profiles {} and {}",
                            key.metric,
                            existing_profile,
                            profile.id
                        );
                    }
                    Some((existing_specificity, _, _)) if *existing_specificity > specificity => {}
                    _ => {
                        selected.insert(key, (specificity, rule, profile.id.clone()));
                    }
                }
            }
        }
    }
    Ok(ResolvedCapabilities {
        rules: selected
            .into_iter()
            .map(|(key, (_, rule, _))| (key, rule))
            .collect(),
    })
}

#[cfg(test)]
mod tests {
    use std::{
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };

    use super::*;

    fn gb10_identity() -> HardwareIdentity {
        HardwareIdentity {
            gpu_products: BTreeSet::from(["NVIDIA GB10".to_owned()]),
            pci_device_ids: BTreeSet::new(),
            driver_version: Some("fixture-driver".to_owned()),
        }
    }

    fn memory_clock_key() -> CapabilityKey {
        CapabilityKey::new(
            "nvml",
            "nvidia.gpu.clock.frequency",
            BTreeMap::from([("clock.domain".to_owned(), "memory".to_owned())]),
        )
    }

    fn graphics_clock_key() -> CapabilityKey {
        CapabilityKey::new(
            "nvml",
            "nvidia.gpu.clock.frequency",
            BTreeMap::from([("clock.domain".to_owned(), "graphics".to_owned())]),
        )
    }

    fn profile_directory() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../deploy/hardware-capabilities")
    }

    #[test]
    fn gb10_profile_matches_and_disables_only_memory_clock() {
        let runtime = CapabilityRuntime::load(Some(&profile_directory()), gb10_identity()).unwrap();
        assert_eq!(
            runtime.policy(&memory_clock_key()),
            CapabilityPolicy::Disabled
        );
        assert_eq!(
            runtime.policy(&graphics_clock_key()),
            CapabilityPolicy::Auto
        );
    }

    #[test]
    fn disabled_call_is_not_invoked_and_graphics_remains_measured() {
        let runtime = CapabilityRuntime::load(Some(&profile_directory()), gb10_identity()).unwrap();
        let calls = AtomicU64::new(0);
        if runtime.should_query(&memory_clock_key()) {
            calls.fetch_add(1, Ordering::Relaxed);
        }
        let graphics_measured = if runtime.should_query(&graphics_clock_key()) {
            calls.fetch_add(1, Ordering::Relaxed);
            Some(1_000_u32)
        } else {
            None
        };
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert_eq!(graphics_measured, Some(1_000));
    }

    #[test]
    fn auto_definitive_unsupported_is_probed_once_but_transient_errors_are_not_cached() {
        let mut runtime = CapabilityRuntime::load(None, gb10_identity()).unwrap();
        let key = memory_clock_key();
        assert!(runtime.should_query(&key));
        runtime.observe_definitive_unsupported(&key);
        assert!(!runtime.should_query(&key));

        let transient = graphics_clock_key();
        assert!(runtime.should_query(&transient));
        assert!(runtime.should_query(&transient));
    }

    #[test]
    fn invalid_reload_preserves_previous_configuration_and_valid_reload_changes_it() {
        let mut runtime =
            CapabilityRuntime::load(Some(&profile_directory()), gb10_identity()).unwrap();
        let missing = profile_directory().join("missing");
        assert!(runtime.reload(Some(&missing), gb10_identity()).is_err());
        assert_eq!(
            runtime.policy(&memory_clock_key()),
            CapabilityPolicy::Disabled
        );
        runtime.reload(None, gb10_identity()).unwrap();
        assert_eq!(runtime.policy(&memory_clock_key()), CapabilityPolicy::Auto);
    }

    #[test]
    fn rejects_unknown_and_ambiguous_entries() {
        let root = std::env::temp_dir().join(format!("spark-capabilities-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir(&root).unwrap();
        fs::write(
            root.join("unknown.toml"),
            r#"[[hardware_profile]]
id = "bad"
[hardware_profile.match]
gpu_product = "NVIDIA GB10"
[[hardware_profile.capability]]
metric = "nvidia.gpu.clock.frequency"
source = "nvml"
attributes = { "arbitrary.label" = "memory" }
policy = "disabled"
reason = "fixture"
"#,
        )
        .unwrap();
        assert!(load_directory(&root, &gb10_identity()).is_err());
        fs::remove_file(root.join("unknown.toml")).unwrap();
        for name in ["a.toml", "b.toml"] {
            fs::write(
                root.join(name),
                format!(
                    r#"[[hardware_profile]]
id = "{name}"
[hardware_profile.match]
gpu_product = "NVIDIA GB10"
[[hardware_profile.capability]]
metric = "nvidia.gpu.clock.frequency"
source = "nvml"
attributes = {{ "clock.domain" = "memory" }}
policy = "disabled"
reason = "fixture"
"#
                ),
            )
            .unwrap();
        }
        assert!(load_directory(&root, &gb10_identity()).is_err());
        fs::remove_dir_all(root).unwrap();
    }
}
