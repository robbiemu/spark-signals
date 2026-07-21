use std::{
    collections::{BTreeMap, HashMap},
    env,
    fs::File,
    io::Read,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use clap::Args;
use nix::{
    fcntl::{OFlag, open},
    sys::stat::Mode,
};
use serde::Deserialize;
use zeroize::{Zeroize, Zeroizing};

use super::{OtlpProtocol, OtlpTargetPlugin, PreparedOtlpTarget};

const MAPLE_PROTOCOL: &str = "http/protobuf";
const MAX_MAPLE_CREDENTIAL_BYTES: u64 = 16 * 1024;

#[derive(Args, Debug, Default)]
pub struct MapleOptions {
    #[arg(long = "maple-credential", env = "SPARK_OTEL_MAPLE_CREDENTIAL")]
    credential: Option<PathBuf>,
    #[arg(
        long = "maple-credential-schema",
        env = "SPARK_OTEL_MAPLE_CREDENTIAL_SCHEMA"
    )]
    credential_schema: Option<String>,
    #[arg(long = "maple-producer", env = "SPARK_OTEL_MAPLE_PRODUCER")]
    producer: Option<String>,
}

impl MapleOptions {
    pub fn is_configured(&self) -> bool {
        self.credential.is_some() || self.credential_schema.is_some() || self.producer.is_some()
    }
}

pub struct MaplePlugin<'a> {
    credential: &'a Path,
    schema: &'a str,
    producer: &'a str,
}

impl<'a> MaplePlugin<'a> {
    pub fn new(options: &'a MapleOptions) -> Result<Self> {
        Ok(Self {
            credential: options
                .credential
                .as_deref()
                .context("SPARK_OTEL_MAPLE_CREDENTIAL is required for the Maple target")?,
            schema: options
                .credential_schema
                .as_deref()
                .context("SPARK_OTEL_MAPLE_CREDENTIAL_SCHEMA is required for the Maple target")?,
            producer: options
                .producer
                .as_deref()
                .context("SPARK_OTEL_MAPLE_PRODUCER is required for the Maple target")?,
        })
    }
}

impl OtlpTargetPlugin for MaplePlugin<'_> {
    fn name(&self) -> &'static str {
        "maple"
    }

    fn prepare(&self) -> Result<PreparedOtlpTarget> {
        reject_otel_override_environment()?;
        let credential = load_credential(self.credential, self.schema, self.producer)?;
        Ok(exporter_config(&credential))
    }
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

fn load_credential(
    path: &Path,
    expected_schema: &str,
    expected_producer: &str,
) -> Result<MapleCredential> {
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
    validate_credential_metadata(
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
    let credential = serde_json::from_slice(&bytes).context("decoding Maple credential")?;
    validate_credential(&credential, expected_schema, expected_producer)?;
    Ok(credential)
}

fn exporter_config(credential: &MapleCredential) -> PreparedOtlpTarget {
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
    let endpoint = credential.endpoint.trim_end_matches('/');
    PreparedOtlpTarget {
        plugin_name: "maple",
        protocol: OtlpProtocol::HttpProtobuf,
        metrics_endpoint: Some(format!("{endpoint}/v1/metrics")),
        logs_endpoint: Some(format!("{endpoint}/v1/logs")),
        headers,
        diagnostics: BTreeMap::from([("producer".to_owned(), credential.producer.clone())]),
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
        validate_parent_metadata(
            metadata.file_type().is_symlink(),
            metadata.is_dir(),
            metadata.uid(),
            metadata.mode(),
        )?;
    }
    Ok(())
}

fn validate_parent_metadata(symlink: bool, directory: bool, owner: u32, mode: u32) -> Result<()> {
    if symlink || !directory {
        anyhow::bail!("Maple credential parent is not a real directory");
    }
    if owner != 0 || mode & 0o022 != 0 {
        anyhow::bail!("Maple credential parent is not root-controlled");
    }
    Ok(())
}

fn validate_credential_metadata(
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

fn validate_credential(
    credential: &MapleCredential,
    expected_schema: &str,
    expected_producer: &str,
) -> Result<()> {
    if expected_schema.is_empty() || credential.schema != expected_schema {
        anyhow::bail!("Maple credential schema is invalid");
    }
    if credential.producer != expected_producer {
        anyhow::bail!("Maple credential producer is invalid");
    }
    let authority = credential
        .endpoint
        .strip_prefix("http://")
        .or_else(|| credential.endpoint.strip_prefix("https://"));
    if !authority.is_some_and(|value| {
        !value.is_empty()
            && !value.contains(['/', '?', '#'])
            && !value.chars().any(char::is_whitespace)
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
    if credential.password.is_empty() || credential.password.contains(['\r', '\n']) {
        anyhow::bail!("Maple credential password is invalid");
    }
    Ok(())
}

fn reject_otel_override_environment() -> Result<()> {
    reject_otel_overrides_with(|key| env::var_os(key).is_some())
}

fn reject_otel_overrides_with(is_set: impl Fn(&str) -> bool) -> Result<()> {
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
        if is_set(key) {
            anyhow::bail!(
                "OTLP endpoint, protocol, and headers must come from the Maple credential file"
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_ENV: &str = include_str!("../../../../.env.test");

    fn test_env(name: &str) -> &'static str {
        TEST_ENV
            .lines()
            .filter_map(|line| line.split_once('='))
            .find_map(|(key, value)| (key == name).then_some(value))
            .unwrap_or_else(|| panic!("missing test environment value: {name}"))
    }

    fn fixture() -> MapleCredential {
        MapleCredential {
            schema: test_env("SPARK_OTEL_MAPLE_TEST_SCHEMA").to_owned(),
            endpoint: test_env("SPARK_OTEL_MAPLE_TEST_ENDPOINT").to_owned(),
            password: test_env("SPARK_OTEL_MAPLE_TEST_AUTH_INPUT").to_owned(),
            producer: test_env("SPARK_OTEL_MAPLE_TEST_PRODUCER").to_owned(),
            protocol: MAPLE_PROTOCOL.to_owned(),
            username: test_env("SPARK_OTEL_MAPLE_TEST_USERNAME").to_owned(),
        }
    }

    #[test]
    fn validates_contract_and_builds_signal_endpoints_and_authentication() {
        let credential = fixture();
        validate_credential(
            &credential,
            test_env("SPARK_OTEL_MAPLE_TEST_SCHEMA"),
            test_env("SPARK_OTEL_MAPLE_TEST_PRODUCER"),
        )
        .unwrap();
        let config = exporter_config(&credential);
        assert_eq!(
            config.metrics_endpoint.as_deref(),
            Some("http://maple.invalid:4318/v1/metrics")
        );
        assert_eq!(
            config.logs_endpoint.as_deref(),
            Some("http://maple.invalid:4318/v1/logs")
        );
        let encoded = config.headers["authorization"]
            .strip_prefix("Basic ")
            .unwrap();
        assert_eq!(
            BASE64_STANDARD.decode(encoded).unwrap(),
            format!("{}:{}", credential.username, credential.password).as_bytes()
        );
    }

    #[test]
    fn rejects_schema_producer_endpoint_protocol_username_and_password_errors() {
        let schema = test_env("SPARK_OTEL_MAPLE_TEST_SCHEMA");
        let producer = test_env("SPARK_OTEL_MAPLE_TEST_PRODUCER");
        let mut credential = fixture();
        credential.schema.clear();
        assert!(validate_credential(&credential, schema, producer).is_err());
        credential = fixture();
        credential.producer = "other".to_owned();
        assert!(validate_credential(&credential, schema, producer).is_err());
        credential = fixture();
        credential.endpoint = "file:///tmp/invalid".to_owned();
        assert!(validate_credential(&credential, schema, producer).is_err());
        credential = fixture();
        credential.protocol = "grpc".to_owned();
        assert!(validate_credential(&credential, schema, producer).is_err());
        credential = fixture();
        credential.username.push(':');
        assert!(validate_credential(&credential, schema, producer).is_err());
        credential = fixture();
        credential.password.push('\n');
        assert!(validate_credential(&credential, schema, producer).is_err());
    }

    #[test]
    fn enforces_file_and_parent_metadata() {
        assert!(validate_credential_metadata(true, 0, 0o100_600, 512).is_ok());
        assert!(validate_credential_metadata(true, 1000, 0o100_600, 512).is_err());
        assert!(validate_credential_metadata(true, 0, 0o100_640, 512).is_err());
        assert!(validate_credential_metadata(false, 0, 0o100_600, 512).is_err());
        assert!(validate_credential_metadata(true, 0, 0o100_600, 0).is_err());
        assert!(
            validate_credential_metadata(true, 0, 0o100_600, MAX_MAPLE_CREDENTIAL_BYTES + 1)
                .is_err()
        );
        assert!(validate_parent_metadata(false, true, 0, 0o040_755).is_ok());
        assert!(validate_parent_metadata(true, true, 0, 0o040_755).is_err());
        assert!(validate_parent_metadata(false, false, 0, 0o100_600).is_err());
        assert!(validate_parent_metadata(false, true, 1000, 0o040_755).is_err());
        assert!(validate_parent_metadata(false, true, 0, 0o040_775).is_err());
    }

    #[test]
    fn denies_unknown_credential_fields() {
        let fixture = fixture();
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

    #[test]
    fn rejects_all_conflicting_otlp_override_classes() {
        for key in [
            "OTEL_EXPORTER_OTLP_ENDPOINT",
            "OTEL_EXPORTER_OTLP_METRICS_PROTOCOL",
            "OTEL_EXPORTER_OTLP_LOGS_HEADERS",
        ] {
            assert!(reject_otel_overrides_with(|candidate| candidate == key).is_err());
        }
        assert!(reject_otel_overrides_with(|_| false).is_ok());
    }
}
