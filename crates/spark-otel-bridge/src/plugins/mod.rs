use std::collections::{BTreeMap, HashMap};

use anyhow::Result;
use clap::Args;

mod maple;
mod standard;

pub use maple::MapleOptions;

#[derive(Args, Debug, Default)]
pub struct TargetOptions {
    #[command(flatten)]
    pub maple: MapleOptions,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OtlpProtocol {
    HttpProtobuf,
}

pub struct PreparedOtlpTarget {
    pub plugin_name: &'static str,
    pub protocol: OtlpProtocol,
    pub metrics_endpoint: Option<String>,
    pub logs_endpoint: Option<String>,
    pub headers: HashMap<String, String>,
    pub diagnostics: BTreeMap<String, String>,
}

pub trait OtlpTargetPlugin {
    fn name(&self) -> &'static str;
    fn prepare(&self) -> Result<PreparedOtlpTarget>;
}

pub fn select<'a>(
    name: &str,
    options: &'a TargetOptions,
) -> Result<Box<dyn OtlpTargetPlugin + 'a>> {
    match name {
        "standard" => Ok(Box::new(standard::StandardPlugin::new(options)?)),
        "maple" => Ok(Box::new(maple::MaplePlugin::new(&options.maple)?)),
        _ => anyhow::bail!("unknown OTLP target plugin: {name}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_unknown_plugin() {
        let options = TargetOptions::default();
        assert!(select("unknown", &options).is_err());
    }

    #[test]
    fn selects_standard_without_backend_configuration() {
        let options = TargetOptions::default();
        let target = select("standard", &options).unwrap().prepare().unwrap();
        assert_eq!(target.plugin_name, "standard");
        assert_eq!(target.protocol, OtlpProtocol::HttpProtobuf);
        assert!(target.metrics_endpoint.is_none());
        assert!(target.logs_endpoint.is_none());
        assert!(target.headers.is_empty());
    }

    #[test]
    fn maple_selection_rejects_missing_settings() {
        let options = TargetOptions::default();
        assert!(select("maple", &options).is_err());
    }
}
