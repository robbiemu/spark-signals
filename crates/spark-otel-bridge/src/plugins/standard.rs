use std::collections::{BTreeMap, HashMap};

use anyhow::Result;

use super::{OtlpProtocol, OtlpTargetPlugin, PreparedOtlpTarget, TargetOptions};

pub struct StandardPlugin;

impl StandardPlugin {
    pub fn new(options: &TargetOptions) -> Result<Self> {
        if options.maple.is_configured() {
            anyhow::bail!("Maple settings require SPARK_OTEL_TARGET=maple");
        }
        Ok(Self)
    }
}

impl OtlpTargetPlugin for StandardPlugin {
    fn name(&self) -> &'static str {
        "standard"
    }

    fn prepare(&self) -> Result<PreparedOtlpTarget> {
        Ok(PreparedOtlpTarget {
            plugin_name: self.name(),
            protocol: OtlpProtocol::HttpProtobuf,
            metrics_endpoint: None,
            logs_endpoint: None,
            headers: HashMap::new(),
            diagnostics: BTreeMap::new(),
        })
    }
}
