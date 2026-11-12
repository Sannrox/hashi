//! `hashi run` configuration (TOML).
//!
//! A config declares the checkpoint directory and connector instances, each
//! with one source section (named by family) and one sink section. Each
//! family adds its own source section; credentials are file paths, never
//! values. Unknown fields are refused, and every validation error names the
//! field it is about. Relative paths resolve against the config file's
//! directory.
//!
//! ```toml
//! checkpoint_dir = "state"
//! # Optional. Default is a local file slot under checkpoint_dir.
//! # lease = { backend = "file", ttl_ms = 15000, members = 1 }
//!
//! [[instance]]
//! name = "line-1"
//! source.fixture = { records = 3, credential_file = "secrets/line-1", accept_sha256 = ["<hex>"] }
//! sink.jsonl = { dir = "out/line-1" }
//! # Optional store-and-forward buffer in front of the sink (#47).
//! buffer = { dir = "buffer/line-1", max_bytes = 1073741824, max_age_secs = 86400, when_full = "stop" }
//! ```

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::Error;

/// The config schema version this release reads.
pub const CONFIG_VERSION: u32 = 1;

/// A config file's declared schema version. Omitted means
/// [`CONFIG_VERSION`]; any other value is refused, so a file written for a
/// newer schema never runs with its new settings ignored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ConfigVersion;

impl<'de> Deserialize<'de> for ConfigVersion {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        match u32::deserialize(deserializer)? {
            CONFIG_VERSION => Ok(Self),
            other => Err(serde::de::Error::custom(format!(
                "config version {other} is not supported; this release reads version {CONFIG_VERSION}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Schema version (`version = 1`). Omitted means the current version.
    #[serde(default)]
    version: Option<ConfigVersion>,
    pub checkpoint_dir: PathBuf,
    /// Lock store for instance leases. Absent: a file slot under `checkpoint_dir`.
    #[serde(default)]
    pub lease: LeaseConfig,
    #[serde(rename = "instance")]
    pub instances: Vec<InstanceConfig>,
}

/// `[lease]` section. The lock store is independent of checkpoint files.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LeaseConfig {
    #[serde(default)]
    pub backend: LeaseBackend,
    /// File-backend directory. Absent: `checkpoint_dir`.
    #[serde(default)]
    pub dir: Option<PathBuf>,
    /// Hold time in milliseconds. Absent: 15000.
    #[serde(default)]
    pub ttl_ms: Option<u64>,
    /// Site size for automatic promotion. Absent: 1. Two nodes need this
    /// exclusive lock (or a witness); the configured backend is that lock.
    #[serde(default)]
    pub members: Option<u32>,
    /// PostgreSQL URL or Kubernetes bearer token. Never a value in the config.
    #[serde(default)]
    pub credential_file: Option<PathBuf>,
    /// Kubernetes namespace.
    #[serde(default)]
    pub namespace: Option<String>,
    /// Kubernetes API base URL.
    #[serde(default)]
    pub api: Option<String>,
}

impl Default for LeaseConfig {
    fn default() -> Self {
        Self {
            backend: LeaseBackend::File,
            dir: None,
            ttl_ms: None,
            members: None,
            credential_file: None,
            namespace: None,
            api: None,
        }
    }
}

impl LeaseConfig {
    pub fn ttl(&self) -> std::time::Duration {
        std::time::Duration::from_millis(self.ttl_ms.unwrap_or(15_000))
    }

    pub fn members(&self) -> u32 {
        self.members.unwrap_or(1)
    }
}

/// `lease.backend`: `file` (default), `postgres`, or `kubernetes`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LeaseBackend {
    #[default]
    File,
    Postgres,
    Kubernetes,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstanceConfig {
    pub name: String,
    pub source: SourceConfig,
    pub sink: SinkConfig,
    /// A local disk buffer between the source and the sink. Absent: the
    /// sink is written directly.
    #[serde(default)]
    pub buffer: Option<BufferConfig>,
}

/// `buffer = { dir, max_bytes, max_age_secs?, when_full? }`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BufferConfig {
    pub dir: PathBuf,
    pub max_bytes: u64,
    /// The oldest buffered record may be at most this old. Absent: no age cap.
    #[serde(default)]
    pub max_age_secs: Option<u64>,
    /// `stop` (default) or `drop_oldest`.
    #[serde(default)]
    pub when_full: crate::buffer::WhenFull,
}

impl BufferConfig {
    pub fn limits(&self) -> crate::buffer::BufferLimits {
        crate::buffer::BufferLimits {
            max_bytes: self.max_bytes,
            max_age_ms: self
                .max_age_secs
                .map(|secs| i64::try_from(secs.saturating_mul(1000)).unwrap_or(i64::MAX)),
            when_full: self.when_full,
        }
    }
}

/// One section per source family.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum SourceConfig {
    /// The in-process fixture source: `records` programmed records, readable
    /// only while the credential file holds a value whose SHA-256 is listed.
    Fixture(FixtureSourceConfig),
    #[serde(rename = "opcua")]
    OpcUa(OpcUaSourceConfig),
    Mqtt(MqttSourceConfig),
    Modbus(ModbusSourceConfig),
    Mysql(MysqlSourceConfig),
    Mssql(MssqlSourceConfig),
    Postgres(PostgresSourceConfig),
    Historian(HistorianSourceConfig),
}

impl SourceConfig {
    pub fn family(&self) -> &'static str {
        match self {
            Self::Fixture(_) => "fixture",
            Self::OpcUa(_) => "opcua",
            Self::Mqtt(_) => "mqtt",
            Self::Modbus(_) => "modbus",
            Self::Mysql(_) => "mysql",
            Self::Mssql(_) => "mssql",
            Self::Postgres(_) => "postgres",
            Self::Historian(_) => "historian",
        }
    }

    fn resolve_paths(&mut self, base: &Path) {
        match self {
            Self::Fixture(fixture) => {
                fixture.credential_file = base.join(&fixture.credential_file);
            }
            Self::OpcUa(opcua) => opcua.resolve_paths(base),
            Self::Mqtt(mqtt) => {
                join_opt(base, &mut mqtt.credential_file);
                join_opt(base, &mut mqtt.ca_file);
                join_opt(base, &mut mqtt.client_certificate);
                join_opt(base, &mut mqtt.client_private_key);
                join_opt(base, &mut mqtt.sparkplug_state_dir);
            }
            Self::Modbus(modbus) => {
                modbus.state_dir = base.join(&modbus.state_dir);
                for device in &mut modbus.devices {
                    if let ModbusEndpointConfig::Rtu { path, .. } = &mut device.endpoint {
                        *path = base.join(&*path).to_string_lossy().into_owned();
                    }
                }
            }
            Self::Mysql(mysql) => {
                mysql.credential_file = base.join(&mysql.credential_file);
                mysql.state_dir = base.join(&mysql.state_dir);
            }
            Self::Mssql(mssql) => {
                mssql.credential_file = base.join(&mssql.credential_file);
                mssql.state_dir = base.join(&mssql.state_dir);
            }
            Self::Postgres(postgres) => {
                postgres.credential_file = base.join(&postgres.credential_file);
                postgres.state_dir = base.join(&postgres.state_dir);
            }
            Self::Historian(historian) => {
                historian.state_dir = base.join(&historian.state_dir);
                match &mut historian.adapter {
                    HistorianAdapterConfig::OpcUa(opcua) => opcua.resolve_paths(base),
                    HistorianAdapterConfig::Mysql(mysql) => {
                        mysql.credential_file = base.join(&mysql.credential_file);
                    }
                    HistorianAdapterConfig::Mssql(mssql) => {
                        mssql.credential_file = base.join(&mssql.credential_file);
                    }
                }
            }
        }
    }

    fn validate(&self, field: &impl Fn(&str) -> String) -> Result<(), String> {
        let invalid =
            |name: &str, problem: &str| Err(format!("invalid config: {}: {problem}", field(name)));
        match self {
            Self::Fixture(fixture) => {
                if fixture.records == 0 {
                    return invalid("source.fixture.records", "must be at least 1");
                }
                if fixture.credential_file.as_os_str().is_empty() {
                    return invalid("source.fixture.credential_file", "must not be empty");
                }
                if fixture.accept_sha256.is_empty()
                    || fixture.accept_sha256.iter().any(|digest| {
                        digest.len() != 64
                            || !digest
                                .bytes()
                                .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
                    })
                {
                    return invalid(
                        "source.fixture.accept_sha256",
                        "list lowercase hex SHA-256 digests",
                    );
                }
            }
            Self::OpcUa(opcua) => opcua.validate(field, "source.opcua")?,
            Self::Mqtt(mqtt) => {
                if mqtt.host.trim().is_empty() {
                    return invalid("source.mqtt.host", "must not be empty");
                }
                if mqtt.port == 0 {
                    return invalid("source.mqtt.port", "must be at least 1");
                }
                if !matches!(mqtt.protocol.as_str(), "v311" | "v5") {
                    return invalid("source.mqtt.protocol", "use v311 or v5");
                }
                if mqtt.client_id.trim().is_empty() {
                    return invalid("source.mqtt.client_id", "must not be empty");
                }
                if mqtt.routes.is_empty() {
                    return invalid("source.mqtt.routes", "list at least one route");
                }
                for route in &mqtt.routes {
                    if route.filter.trim().is_empty() || route.kind.trim().is_empty() {
                        return invalid("source.mqtt.routes", "filter and kind must not be empty");
                    }
                    if !matches!(route.decoder.as_str(), "json" | "raw" | "sparkplug_b") {
                        return invalid(
                            "source.mqtt.routes",
                            "decoder is json, raw, or sparkplug_b",
                        );
                    }
                }
                if mqtt.timeout_ms == 0 {
                    return invalid("source.mqtt.timeout_ms", "must be at least 1");
                }
                if mqtt.max_packet_bytes == 0 {
                    return invalid("source.mqtt.max_packet_bytes", "must be at least 1");
                }
                if mqtt.schema_version == 0 {
                    return invalid("source.mqtt.schema_version", "must be at least 1");
                }
                if mqtt.session_expiry_secs == Some(0) {
                    return invalid("source.mqtt.session_expiry_secs", "must be at least 1");
                }
                if mqtt.keep_alive_secs == Some(0) {
                    return invalid("source.mqtt.keep_alive_secs", "must be at least 1");
                }
                let tls = mqtt
                    .ca_file
                    .as_ref()
                    .is_some_and(|path| !path.as_os_str().is_empty());
                if !tls && !mqtt.allow_insecure_lab {
                    return invalid(
                        "source.mqtt.ca_file",
                        "required unless allow_insecure_lab is set",
                    );
                }
                let cert = mqtt
                    .client_certificate
                    .as_ref()
                    .is_some_and(|path| !path.as_os_str().is_empty());
                let key = mqtt
                    .client_private_key
                    .as_ref()
                    .is_some_and(|path| !path.as_os_str().is_empty());
                if cert != key {
                    return invalid(
                        "source.mqtt.client_certificate",
                        "set both client_certificate and client_private_key",
                    );
                }
                if mqtt
                    .routes
                    .iter()
                    .any(|route| route.decoder == "sparkplug_b")
                    && mqtt
                        .sparkplug_state_dir
                        .as_ref()
                        .is_none_or(|path| path.as_os_str().is_empty())
                {
                    return invalid(
                        "source.mqtt.sparkplug_state_dir",
                        "required by sparkplug_b routes",
                    );
                }
            }
            Self::Modbus(modbus) => {
                if modbus.state_dir.as_os_str().is_empty() {
                    return invalid("source.modbus.state_dir", "must not be empty");
                }
                if modbus.devices.is_empty() {
                    return invalid("source.modbus.devices", "list at least one device");
                }
                for device in &modbus.devices {
                    match &device.endpoint {
                        ModbusEndpointConfig::Tcp { host, port } => {
                            if host.trim().is_empty() {
                                return invalid(
                                    "source.modbus.devices",
                                    "tcp host must not be empty",
                                );
                            }
                            if *port == 0 {
                                return invalid(
                                    "source.modbus.devices",
                                    "tcp port must be at least 1",
                                );
                            }
                        }
                        ModbusEndpointConfig::Rtu { path, baud, parity } => {
                            if path.trim().is_empty() {
                                return invalid(
                                    "source.modbus.devices",
                                    "rtu path must not be empty",
                                );
                            }
                            if *baud == 0 {
                                return invalid(
                                    "source.modbus.devices",
                                    "rtu baud must be at least 1",
                                );
                            }
                            if !matches!(parity.as_str(), "none" | "even" | "odd") {
                                return invalid(
                                    "source.modbus.devices",
                                    "rtu parity is none, even, or odd",
                                );
                            }
                        }
                    }
                    if device.timeout_ms == 0
                        || device.min_request_interval_ms == 0
                        || device.max_backoff_ms < device.min_request_interval_ms
                        || device.groups.is_empty()
                    {
                        return invalid("source.modbus.devices", "invalid device limits");
                    }
                    for group in &device.groups {
                        if group.kind.trim().is_empty()
                            || group.record_key.trim().is_empty()
                            || group.schema_version == 0
                            || group.interval_ms == 0
                            || group.registers.is_empty()
                        {
                            return invalid("source.modbus.devices", "invalid poll group");
                        }
                        if !matches!(
                            group.table.as_str(),
                            "coils" | "discrete_inputs" | "holding_registers" | "input_registers"
                        ) {
                            return invalid(
                                "source.modbus.devices",
                                "table is coils, discrete_inputs, holding_registers, or input_registers",
                            );
                        }
                        for register in &group.registers {
                            if register.name.trim().is_empty() {
                                return invalid(
                                    "source.modbus.devices",
                                    "register name must not be empty",
                                );
                            }
                            if !matches!(
                                register.value_type.as_str(),
                                "bool" | "i16" | "u16" | "i32" | "u32" | "f32" | "f64"
                            ) {
                                return invalid(
                                    "source.modbus.devices",
                                    "unknown register value_type",
                                );
                            }
                            if !matches!(register.word_order.as_str(), "big" | "little")
                                || !matches!(register.byte_order.as_str(), "big" | "little")
                            {
                                return invalid(
                                    "source.modbus.devices",
                                    "word_order and byte_order are big or little",
                                );
                            }
                            if !register.scale.is_finite() || !register.offset.is_finite() {
                                return invalid(
                                    "source.modbus.devices",
                                    "scale and offset must be finite",
                                );
                            }
                        }
                    }
                }
            }
            Self::Mysql(mysql) => {
                if mysql.url.trim().is_empty() {
                    return invalid("source.mysql.url", "must not be empty");
                }
                if mysql.credential_file.as_os_str().is_empty() {
                    return invalid("source.mysql.credential_file", "must not be empty");
                }
                if mysql.tables.is_empty() {
                    return invalid("source.mysql.tables", "list at least one table");
                }
                if mysql.state_dir.as_os_str().is_empty() {
                    return invalid("source.mysql.state_dir", "must not be empty");
                }
                if mysql.chunk_rows == 0 {
                    return invalid("source.mysql.chunk_rows", "must be at least 1");
                }
                if mysql.max_batch_bytes == 0 {
                    return invalid("source.mysql.max_batch_bytes", "must be at least 1");
                }
                if mysql.server_id == 0 {
                    return invalid("source.mysql.server_id", "must be at least 1");
                }
                if mysql.schema_version == 0 {
                    return invalid("source.mysql.schema_version", "must be at least 1");
                }
                if mysql.timeout_ms == 0 {
                    return invalid("source.mysql.timeout_ms", "must be at least 1");
                }
            }
            Self::Mssql(mssql) => {
                if mssql.host.trim().is_empty() {
                    return invalid("source.mssql.host", "must not be empty");
                }
                if mssql.port == 0 {
                    return invalid("source.mssql.port", "must be at least 1");
                }
                if mssql.user.trim().is_empty() {
                    return invalid("source.mssql.user", "must not be empty");
                }
                if mssql.database.trim().is_empty() {
                    return invalid("source.mssql.database", "must not be empty");
                }
                if mssql.credential_file.as_os_str().is_empty() {
                    return invalid("source.mssql.credential_file", "must not be empty");
                }
                if mssql.tables.is_empty() {
                    return invalid("source.mssql.tables", "list at least one table");
                }
                if mssql.state_dir.as_os_str().is_empty() {
                    return invalid("source.mssql.state_dir", "must not be empty");
                }
                if mssql.chunk_rows == 0 {
                    return invalid("source.mssql.chunk_rows", "must be at least 1");
                }
                if mssql.max_batch_bytes == 0 {
                    return invalid("source.mssql.max_batch_bytes", "must be at least 1");
                }
                if mssql.schema_version == 0 {
                    return invalid("source.mssql.schema_version", "must be at least 1");
                }
                if mssql.timeout_ms == 0 {
                    return invalid("source.mssql.timeout_ms", "must be at least 1");
                }
                if mssql.max_lag_ms == 0 {
                    return invalid("source.mssql.max_lag_ms", "must be at least 1");
                }
            }
            Self::Postgres(postgres) => {
                if postgres.host.trim().is_empty() {
                    return invalid("source.postgres.host", "must not be empty");
                }
                if postgres.port == 0 {
                    return invalid("source.postgres.port", "must be at least 1");
                }
                if postgres.user.trim().is_empty() {
                    return invalid("source.postgres.user", "must not be empty");
                }
                if postgres.database.trim().is_empty() {
                    return invalid("source.postgres.database", "must not be empty");
                }
                if postgres.credential_file.as_os_str().is_empty() {
                    return invalid("source.postgres.credential_file", "must not be empty");
                }
                if postgres.tables.is_empty() {
                    return invalid("source.postgres.tables", "list at least one table");
                }
                if postgres.state_dir.as_os_str().is_empty() {
                    return invalid("source.postgres.state_dir", "must not be empty");
                }
                if postgres.chunk_rows == 0 {
                    return invalid("source.postgres.chunk_rows", "must be at least 1");
                }
                if postgres.max_batch_bytes == 0 {
                    return invalid("source.postgres.max_batch_bytes", "must be at least 1");
                }
                if postgres.schema_version == 0 {
                    return invalid("source.postgres.schema_version", "must be at least 1");
                }
                if postgres.timeout_ms == 0 {
                    return invalid("source.postgres.timeout_ms", "must be at least 1");
                }
                if postgres.max_lag_ms == 0 {
                    return invalid("source.postgres.max_lag_ms", "must be at least 1");
                }
            }
            Self::Historian(historian) => {
                if historian.chunk_ms <= 0 {
                    return invalid("source.historian.chunk_ms", "must be at least 1");
                }
                if historian.page_limit == 0 {
                    return invalid("source.historian.page_limit", "must be at least 1");
                }
                if historian.settle_ms < 0 {
                    return invalid("source.historian.settle_ms", "must be 0 or more");
                }
                if historian.state_dir.as_os_str().is_empty() {
                    return invalid("source.historian.state_dir", "must not be empty");
                }
                match &historian.adapter {
                    HistorianAdapterConfig::OpcUa(opcua) => {
                        opcua.validate(field, "source.historian.adapter.opcua")?;
                    }
                    HistorianAdapterConfig::Mysql(mysql) => {
                        if mysql.url.trim().is_empty() {
                            return invalid(
                                "source.historian.adapter.mysql.url",
                                "must not be empty",
                            );
                        }
                        if mysql.credential_file.as_os_str().is_empty() {
                            return invalid(
                                "source.historian.adapter.mysql.credential_file",
                                "must not be empty",
                            );
                        }
                        if mysql.timeout_ms == 0 {
                            return invalid(
                                "source.historian.adapter.mysql.timeout_ms",
                                "must be at least 1",
                            );
                        }
                    }
                    HistorianAdapterConfig::Mssql(mssql) => {
                        if mssql.host.trim().is_empty()
                            || mssql.user.trim().is_empty()
                            || mssql.database.trim().is_empty()
                        {
                            return invalid(
                                "source.historian.adapter.mssql.host",
                                "host, user, and database must not be empty",
                            );
                        }
                        if mssql.port == 0 || mssql.timeout_ms == 0 {
                            return invalid(
                                "source.historian.adapter.mssql.port",
                                "port and timeout_ms must be at least 1",
                            );
                        }
                        if mssql.credential_file.as_os_str().is_empty() {
                            return invalid(
                                "source.historian.adapter.mssql.credential_file",
                                "must not be empty",
                            );
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

fn join_opt(base: &Path, path: &mut Option<PathBuf>) {
    if let Some(value) = path {
        *value = base.join(&*value);
    }
}

impl OpcUaSourceConfig {
    fn resolve_paths(&mut self, base: &Path) {
        self.pki_dir = base.join(&self.pki_dir);
        join_opt(base, &mut self.credential_file);
        join_opt(base, &mut self.user_certificate);
        join_opt(base, &mut self.user_private_key);
        join_opt(base, &mut self.state_dir);
    }

    fn validate(&self, field: &impl Fn(&str) -> String, prefix: &str) -> Result<(), String> {
        let invalid = |name: &str, problem: &str| {
            Err(format!(
                "invalid config: {}: {problem}",
                field(&format!("{prefix}.{name}"))
            ))
        };
        if !self.endpoint.starts_with("opc.tcp://") {
            return invalid("endpoint", "must start with opc.tcp://");
        }
        if self.application_uri.trim().is_empty() {
            return invalid("application_uri", "must not be empty");
        }
        if self.pki_dir.as_os_str().is_empty() {
            return invalid("pki_dir", "must not be empty");
        }
        if !matches!(
            self.policy.as_str(),
            "basic256sha256" | "aes128sha256rsaoaep" | "aes256sha256rsapss" | "none"
        ) {
            return invalid(
                "policy",
                "use basic256sha256, aes128sha256rsaoaep, aes256sha256rsapss, or none",
            );
        }
        if self.policy == "none" && !self.allow_insecure_lab {
            return invalid("allow_insecure_lab", "required for policy none");
        }
        let user = self
            .credential_file
            .as_ref()
            .is_some_and(|path| !path.as_os_str().is_empty());
        let x509 = self
            .user_certificate
            .as_ref()
            .is_some_and(|path| !path.as_os_str().is_empty())
            && self
                .user_private_key
                .as_ref()
                .is_some_and(|path| !path.as_os_str().is_empty());
        if user == x509 {
            return invalid(
                "credential_file",
                "set credential_file or both user_certificate and user_private_key",
            );
        }
        if self.roots.is_empty() || self.roots.iter().any(|root| root.trim().is_empty()) {
            return invalid("roots", "list at least one node id");
        }
        if self.max_nodes == 0 || self.max_nodes > 10_000 {
            return invalid("max_nodes", "must be between 1 and 10000");
        }
        if self.timeout_ms == 0 {
            return invalid("timeout_ms", "must be at least 1");
        }
        if self.schema_version == 0 {
            return invalid("schema_version", "must be at least 1");
        }
        if self.nodes.iter().any(|node| node.trim().is_empty()) {
            return invalid("nodes", "node ids must not be empty");
        }
        if !self.nodes.is_empty()
            && self
                .state_dir
                .as_ref()
                .is_none_or(|path| path.as_os_str().is_empty())
        {
            return invalid("state_dir", "required to subscribe");
        }
        if self.publishing_interval_ms == Some(0) {
            return invalid("publishing_interval_ms", "must be at least 1");
        }
        if self.sampling_interval_ms == Some(0) {
            return invalid("sampling_interval_ms", "must be at least 1");
        }
        if self
            .deadband
            .is_some_and(|value| !value.is_finite() || value < 0.0)
        {
            return invalid("deadband", "must be zero or more");
        }
        if self.queue_size == Some(0) {
            return invalid("queue_size", "must be at least 1");
        }
        if self.max_pending_bytes == Some(0) {
            return invalid("max_pending_bytes", "must be at least 1");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FixtureSourceConfig {
    pub records: u64,
    pub credential_file: PathBuf,
    pub accept_sha256: Vec<String>,
}

/// `source.opcua = { endpoint, application_uri, pki_dir, policy, ... }`.
///
/// Username authentication reads `credential_file` as user then password,
/// one per line. X.509 authentication names certificate and key files.
/// `hashi run` also needs `nodes` and `state_dir` to subscribe.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpcUaSourceConfig {
    pub endpoint: String,
    pub application_uri: String,
    pub pki_dir: PathBuf,
    pub policy: String,
    #[serde(default)]
    pub allow_insecure_lab: bool,
    #[serde(default)]
    pub credential_file: Option<PathBuf>,
    #[serde(default)]
    pub user_certificate: Option<PathBuf>,
    #[serde(default)]
    pub user_private_key: Option<PathBuf>,
    pub roots: Vec<String>,
    pub max_nodes: u64,
    pub timeout_ms: u64,
    pub schema_version: u64,
    #[serde(default)]
    pub nodes: Vec<String>,
    #[serde(default)]
    pub publishing_interval_ms: Option<u64>,
    #[serde(default)]
    pub sampling_interval_ms: Option<u64>,
    #[serde(default)]
    pub deadband: Option<f64>,
    #[serde(default)]
    pub queue_size: Option<u32>,
    #[serde(default)]
    pub state_dir: Option<PathBuf>,
    #[serde(default)]
    pub max_pending_bytes: Option<u64>,
}

/// `source.mqtt = { host, port, protocol, client_id, routes, ... }`.
///
/// Optional `credential_file` is user then password, one per line.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MqttSourceConfig {
    pub host: String,
    pub port: u16,
    pub protocol: String,
    pub client_id: String,
    pub routes: Vec<MqttRouteConfig>,
    #[serde(default)]
    pub allow_insecure_lab: bool,
    #[serde(default)]
    pub credential_file: Option<PathBuf>,
    #[serde(default)]
    pub ca_file: Option<PathBuf>,
    #[serde(default)]
    pub client_certificate: Option<PathBuf>,
    #[serde(default)]
    pub client_private_key: Option<PathBuf>,
    #[serde(default)]
    pub shared_group: Option<String>,
    #[serde(default)]
    pub session_expiry_secs: Option<u64>,
    #[serde(default)]
    pub keep_alive_secs: Option<u64>,
    pub timeout_ms: u64,
    pub max_packet_bytes: u64,
    pub schema_version: u64,
    #[serde(default)]
    pub sparkplug_state_dir: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MqttRouteConfig {
    pub filter: String,
    pub kind: String,
    pub decoder: String,
}

/// `source.modbus = { state_dir, devices = [...] }`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModbusSourceConfig {
    pub state_dir: PathBuf,
    pub devices: Vec<ModbusDeviceConfig>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModbusDeviceConfig {
    pub endpoint: ModbusEndpointConfig,
    pub unit: u8,
    pub timeout_ms: u64,
    pub min_request_interval_ms: u64,
    pub max_backoff_ms: u64,
    pub groups: Vec<ModbusGroupConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ModbusEndpointConfig {
    Tcp {
        host: String,
        port: u16,
    },
    Rtu {
        path: String,
        baud: u32,
        parity: String,
    },
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModbusGroupConfig {
    pub kind: String,
    pub record_key: String,
    pub schema_version: u64,
    pub table: String,
    pub interval_ms: u64,
    pub registers: Vec<ModbusRegisterConfig>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModbusRegisterConfig {
    pub name: String,
    pub address: u16,
    pub value_type: String,
    pub word_order: String,
    pub byte_order: String,
    pub scale: f64,
    pub offset: f64,
    #[serde(default)]
    pub unit: Option<String>,
}

/// `source.mysql = { url, credential_file, tables, state_dir, ... }`.
///
/// `url` is `mysql://user@host:port`; the password is the credential file.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MysqlSourceConfig {
    pub url: String,
    pub credential_file: PathBuf,
    pub tables: Vec<MysqlTableConfig>,
    pub state_dir: PathBuf,
    pub chunk_rows: u64,
    pub max_batch_bytes: u64,
    pub server_id: u32,
    pub schema_version: u64,
    pub timeout_ms: u64,
    #[serde(default)]
    pub allow_insecure_lab: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MysqlTableConfig {
    pub database: String,
    pub table: String,
    #[serde(default)]
    pub key: Vec<String>,
}

/// `source.mssql = { host, port, user, database, credential_file, tables, ... }`.
///
/// The credential file is the password only.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MssqlSourceConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub database: String,
    pub credential_file: PathBuf,
    pub tables: Vec<MssqlTableConfig>,
    pub state_dir: PathBuf,
    pub chunk_rows: u64,
    pub max_batch_bytes: u64,
    pub schema_version: u64,
    pub timeout_ms: u64,
    pub max_lag_ms: u64,
    #[serde(default)]
    pub allow_insecure_lab: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MssqlTableConfig {
    pub schema: String,
    pub table: String,
    #[serde(default)]
    pub key: Vec<String>,
}

/// `source.postgres = { host, port, user, database, credential_file, tables, ... }`.
///
/// The credential file is the password only. Distinct from `lease.backend = "postgres"`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresSourceConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub database: String,
    pub credential_file: PathBuf,
    pub tables: Vec<PostgresTableConfig>,
    pub state_dir: PathBuf,
    pub chunk_rows: u64,
    pub max_batch_bytes: u64,
    pub schema_version: u64,
    pub timeout_ms: u64,
    pub max_lag_ms: u64,
    #[serde(default)]
    pub allow_insecure_lab: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresTableConfig {
    pub schema: String,
    pub table: String,
    #[serde(default)]
    pub key: Vec<String>,
}

/// `source.historian = { start_ms, chunk_ms, ..., adapter.<family> = { ... } }`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistorianSourceConfig {
    pub start_ms: i64,
    pub chunk_ms: i64,
    pub page_limit: u64,
    pub settle_ms: i64,
    #[serde(default)]
    pub max_reads_per_second: u32,
    pub state_dir: PathBuf,
    pub adapter: HistorianAdapterConfig,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum HistorianAdapterConfig {
    #[serde(rename = "opcua")]
    OpcUa(OpcUaSourceConfig),
    Mysql(MysqlHistorianConfig),
    Mssql(MssqlHistorianConfig),
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MysqlHistorianConfig {
    pub url: String,
    pub credential_file: PathBuf,
    pub timeout_ms: u64,
    #[serde(default)]
    pub allow_insecure_lab: bool,
    pub mapping_database: String,
    pub tag_table: String,
    pub tag_id_column: String,
    pub tag_name_column: String,
    pub value_table: String,
    pub value_tag_column: String,
    pub time_column: String,
    pub value_column: String,
    pub quality_column: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MssqlHistorianConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub database: String,
    pub credential_file: PathBuf,
    pub timeout_ms: u64,
    #[serde(default)]
    pub allow_insecure_lab: bool,
    pub mapping_database: String,
    pub tag_table: String,
    pub tag_id_column: String,
    pub tag_name_column: String,
    pub value_table: String,
    pub value_tag_column: String,
    pub time_column: String,
    pub value_column: String,
    pub quality_column: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum SinkConfig {
    Jsonl(JsonlSinkConfig),
    Parquet(ParquetSinkConfig),
    Kafka(KafkaSinkConfig),
}

impl SinkConfig {
    pub fn family(&self) -> &'static str {
        match self {
            Self::Jsonl(_) => "jsonl",
            Self::Parquet(_) => "parquet",
            Self::Kafka(_) => "kafka",
        }
    }

    pub fn dir(&self) -> &Path {
        match self {
            Self::Jsonl(jsonl) => &jsonl.dir,
            Self::Parquet(parquet) => &parquet.dir,
            Self::Kafka(kafka) => &kafka.dir,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JsonlSinkConfig {
    pub dir: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParquetSinkConfig {
    pub dir: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KafkaSinkConfig {
    pub dir: PathBuf,
    pub topic: String,
}

impl Config {
    /// Read, parse, validate, and resolve relative paths against the file's
    /// directory.
    pub fn load(path: &Path) -> Result<Self, Error> {
        let text = std::fs::read_to_string(path)
            .map_err(|_| Error::new(format!("config {} is unreadable", path.display())))?;
        let base = path.parent().unwrap_or(Path::new("."));
        Self::parse(&text, base)
    }

    pub fn parse(text: &str, base: &Path) -> Result<Self, Error> {
        let mut config: Config = toml::from_str(text)
            .map_err(|error| Error::new(format!("invalid config: {}", error.message())))?;
        config.validate()?;
        config.checkpoint_dir = base.join(&config.checkpoint_dir);
        if let Some(dir) = &mut config.lease.dir {
            *dir = base.join(&*dir);
        }
        if let Some(path) = &mut config.lease.credential_file {
            *path = base.join(&*path);
        }
        if config.lease.dir.is_none() {
            config.lease.dir = Some(config.checkpoint_dir.clone());
        }
        for instance in &mut config.instances {
            instance.source.resolve_paths(base);
            match &mut instance.sink {
                SinkConfig::Jsonl(jsonl) => jsonl.dir = base.join(&jsonl.dir),
                SinkConfig::Parquet(parquet) => parquet.dir = base.join(&parquet.dir),
                SinkConfig::Kafka(kafka) => kafka.dir = base.join(&kafka.dir),
            }
            if let Some(buffer) = &mut instance.buffer {
                buffer.dir = base.join(&buffer.dir);
            }
        }
        Ok(config)
    }

    fn validate(&self) -> Result<(), Error> {
        let invalid = |field: String, problem: &str| {
            Err(Error::new(format!("invalid config: {field}: {problem}")))
        };
        if self.checkpoint_dir.as_os_str().is_empty() {
            return invalid("checkpoint_dir".into(), "must not be empty");
        }
        if self.lease.ttl_ms == Some(0) {
            return invalid("lease.ttl_ms".into(), "must be at least 1");
        }
        if self.lease.members == Some(0) {
            return invalid("lease.members".into(), "must be at least 1");
        }
        match self.lease.backend {
            LeaseBackend::File => {}
            LeaseBackend::Postgres => {
                if self
                    .lease
                    .credential_file
                    .as_ref()
                    .is_none_or(|path| path.as_os_str().is_empty())
                {
                    return invalid(
                        "lease.credential_file".into(),
                        "required for the postgres backend",
                    );
                }
            }
            LeaseBackend::Kubernetes => {
                if self
                    .lease
                    .namespace
                    .as_ref()
                    .is_none_or(|value| value.is_empty())
                {
                    return invalid(
                        "lease.namespace".into(),
                        "required for the kubernetes backend",
                    );
                }
                if self.lease.api.as_ref().is_none_or(|value| value.is_empty()) {
                    return invalid("lease.api".into(), "required for the kubernetes backend");
                }
                if self
                    .lease
                    .credential_file
                    .as_ref()
                    .is_none_or(|path| path.as_os_str().is_empty())
                {
                    return invalid(
                        "lease.credential_file".into(),
                        "required for the kubernetes backend",
                    );
                }
            }
        }
        if self.instances.is_empty() {
            return invalid("instance".into(), "declare at least one instance");
        }
        let mut names = BTreeSet::new();
        // Leases fence one instance's checkpoint, not a shared sink, so two
        // instances never write one directory.
        let mut sink_dirs = BTreeSet::new();
        for (index, instance) in self.instances.iter().enumerate() {
            let field = |name: &str| format!("instance[{index}].{name}");
            // The name is a checkpoint directory component.
            if instance.name.is_empty()
                || !instance
                    .name
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
            {
                return invalid(field("name"), "use 1 or more of A-Z a-z 0-9 - _");
            }
            if !names.insert(instance.name.as_str()) {
                return invalid(field("name"), "duplicates another instance");
            }
            instance.source.validate(&field).map_err(Error::new)?;
            let sink_dir = instance.sink.dir();
            if sink_dir.as_os_str().is_empty() {
                return invalid(
                    field(&format!("sink.{}.dir", instance.sink.family())),
                    "must not be empty",
                );
            }
            if let SinkConfig::Kafka(kafka) = &instance.sink
                && kafka.topic.trim().is_empty()
            {
                return invalid(field("sink.kafka.topic"), "must not be empty");
            }
            if !sink_dirs.insert(
                sink_dir
                    .components()
                    .filter(|part| !matches!(part, std::path::Component::CurDir))
                    .collect::<PathBuf>(),
            ) {
                return invalid(
                    field(&format!("sink.{}.dir", instance.sink.family())),
                    "is used by another instance",
                );
            }
            if let Some(buffer) = &instance.buffer {
                if buffer.dir.as_os_str().is_empty() {
                    return invalid(field("buffer.dir"), "must not be empty");
                }
                // One directory, one buffer: shared with a sink or another
                // buffer, two writers would interleave its log.
                if !sink_dirs.insert(
                    buffer
                        .dir
                        .components()
                        .filter(|part| !matches!(part, std::path::Component::CurDir))
                        .collect::<PathBuf>(),
                ) {
                    return invalid(field("buffer.dir"), "is used by a sink or another buffer");
                }
                if buffer.max_bytes == 0 {
                    return invalid(field("buffer.max_bytes"), "must be at least 1");
                }
                if buffer.max_age_secs == Some(0) {
                    return invalid(field("buffer.max_age_secs"), "must be at least 1");
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DIGEST: &str = "0000000000000000000000000000000000000000000000000000000000000000";

    fn config(instance: &str) -> String {
        format!("checkpoint_dir = \"state\"\n\n[[instance]]\n{instance}\n")
    }

    fn valid_instance() -> String {
        format!(
            "name = \"line-1\"\nsource.fixture = {{ records = 2, credential_file = \"secret\", accept_sha256 = [\"{DIGEST}\"] }}\nsink.jsonl = {{ dir = \"out\" }}"
        )
    }

    #[test]
    fn a_valid_config_resolves_paths_against_its_directory() {
        let parsed = Config::parse(&config(&valid_instance()), Path::new("/etc/hashi")).unwrap();
        assert_eq!(parsed.checkpoint_dir, Path::new("/etc/hashi/state"));
        assert_eq!(parsed.lease.backend, LeaseBackend::File);
        assert_eq!(
            parsed.lease.dir.as_deref(),
            Some(Path::new("/etc/hashi/state"))
        );
        assert_eq!(parsed.lease.ttl(), std::time::Duration::from_millis(15_000));
        let SourceConfig::Fixture(fixture) = &parsed.instances[0].source else {
            panic!("expected fixture");
        };
        assert_eq!(fixture.credential_file, Path::new("/etc/hashi/secret"));
    }

    #[test]
    fn an_opcua_source_resolves_pki_and_credential_paths() {
        let instance = "name = \"line-1\"\n\
             source.opcua = { endpoint = \"opc.tcp://127.0.0.1:4840/\", application_uri = \"urn:hashi:line-1\", \
             pki_dir = \"pki\", policy = \"basic256sha256\", credential_file = \"secrets/opcua\", \
             roots = [\"nsu=urn:hashi:fixture;s=Equipment\"], max_nodes = 100, timeout_ms = 3000, schema_version = 1 }\n\
             sink.jsonl = { dir = \"out\" }";
        let parsed = Config::parse(&config(instance), Path::new("/etc/hashi")).unwrap();
        let SourceConfig::OpcUa(opcua) = &parsed.instances[0].source else {
            panic!("expected opcua");
        };
        assert_eq!(opcua.pki_dir, Path::new("/etc/hashi/pki"));
        assert_eq!(
            opcua.credential_file.as_deref(),
            Some(Path::new("/etc/hashi/secrets/opcua"))
        );
        let missing = instance.replace("credential_file = \"secrets/opcua\", ", "");
        let error = Config::parse(&config(&missing), Path::new("."))
            .unwrap_err()
            .message;
        assert!(error.contains("source.opcua.credential_file"), "{error}");
    }

    /// A buffer section resolves its directory like the sink's, defaults to
    /// `stop`, and is refused when it shares a directory or has no room.
    #[test]
    fn a_buffer_section_is_parsed_and_validated() {
        let buffered =
            |buffer: &str| config(&format!("{}\nbuffer = {{ {buffer} }}", valid_instance()));
        let parsed = Config::parse(
            &buffered("dir = \"buf\", max_bytes = 1024, max_age_secs = 60"),
            Path::new("/etc/hashi"),
        )
        .unwrap();
        let buffer = parsed.instances[0].buffer.as_ref().unwrap();
        assert_eq!(buffer.dir, Path::new("/etc/hashi/buf"));
        assert_eq!(
            buffer.limits(),
            crate::buffer::BufferLimits {
                max_bytes: 1024,
                max_age_ms: Some(60_000),
                when_full: crate::buffer::WhenFull::Stop,
            }
        );
        let dropping = Config::parse(
            &buffered("dir = \"buf\", max_bytes = 1, when_full = \"drop_oldest\""),
            Path::new("."),
        )
        .unwrap();
        assert_eq!(
            dropping.instances[0].buffer.as_ref().unwrap().when_full,
            crate::buffer::WhenFull::DropOldest
        );
        for (buffer, field) in [
            ("dir = \"out\", max_bytes = 1", "instance[0].buffer.dir"),
            (
                "dir = \"buf\", max_bytes = 0",
                "instance[0].buffer.max_bytes",
            ),
            (
                "dir = \"buf\", max_bytes = 1, max_age_secs = 0",
                "instance[0].buffer.max_age_secs",
            ),
            (
                "dir = \"buf\", max_bytes = 1, when_full = \"block\"",
                "drop_oldest",
            ),
        ] {
            let error = Config::parse(&buffered(buffer), Path::new("."))
                .unwrap_err()
                .message;
            assert!(error.contains(field), "{buffer}: {error}");
        }
    }

    #[test]
    fn invalid_configs_are_refused_naming_the_field() {
        let cases = [
            (
                valid_instance().replace("line-1", "line 1"),
                "instance[0].name",
            ),
            (
                valid_instance().replace("records = 2", "records = 0"),
                "instance[0].source.fixture.records",
            ),
            (
                valid_instance().replace(DIGEST, "ABC"),
                "instance[0].source.fixture.accept_sha256",
            ),
            (
                valid_instance().replace("dir = \"out\"", "dir = \"out\", mode = \"x\""),
                "mode",
            ),
            (
                valid_instance().replace("source.fixture", "source.oracle"),
                "oracle",
            ),
            (valid_instance().replace("name = \"line-1\"\n", ""), "name"),
        ];
        for (instance, field) in cases {
            let error = Config::parse(&config(&instance), Path::new("."))
                .unwrap_err()
                .message;
            assert!(error.contains(field), "{field}: {error}");
        }
        let twice = format!(
            "{}\n\n[[instance]]\n{}",
            config(&valid_instance()),
            valid_instance()
        );
        let error = Config::parse(&twice, Path::new(".")).unwrap_err().message;
        assert!(error.contains("instance[1].name"), "{error}");
        // `./out` and `out` are one directory.
        let shared = format!(
            "{}\n\n[[instance]]\n{}",
            config(&valid_instance()),
            valid_instance()
                .replace("line-1", "line-2")
                .replace("\"out\"", "\"./out\"")
        );
        let error = Config::parse(&shared, Path::new(".")).unwrap_err().message;
        assert!(error.contains("instance[1].sink.jsonl.dir"), "{error}");
    }

    #[test]
    fn a_lease_section_is_parsed_and_validated() {
        let text = format!(
            "checkpoint_dir = \"state\"\nlease = {{ backend = \"file\", dir = \"locks\", ttl_ms = 2000, members = 2 }}\n\n[[instance]]\n{}\n",
            valid_instance()
        );
        let parsed = Config::parse(&text, Path::new("/etc/hashi")).unwrap();
        assert_eq!(
            parsed.lease.dir.as_deref(),
            Some(Path::new("/etc/hashi/locks"))
        );
        assert_eq!(parsed.lease.ttl(), std::time::Duration::from_millis(2000));
        assert_eq!(parsed.lease.members(), 2);
        for (lease, field) in [
            ("backend = \"file\", ttl_ms = 0", "lease.ttl_ms"),
            ("backend = \"postgres\"", "lease.credential_file"),
            ("backend = \"kubernetes\", namespace = \"ns\"", "lease.api"),
        ] {
            let text = format!(
                "checkpoint_dir = \"state\"\nlease = {{ {lease} }}\n\n[[instance]]\n{}\n",
                valid_instance()
            );
            let error = Config::parse(&text, Path::new(".")).unwrap_err().message;
            assert!(error.contains(field), "{lease}: {error}");
        }
    }

    #[test]
    fn config_version_defaults_to_current_and_unknown_versions_are_refused() {
        for accepted in ["", "version = 1\n"] {
            let text = format!("{accepted}{}", config(&valid_instance()));
            Config::parse(&text, Path::new("/etc/hashi"))
                .unwrap_or_else(|error| panic!("{accepted:?}: {error}"));
        }
        for refused in ["version = 2\n", "version = 0\n"] {
            let text = format!("{refused}{}", config(&valid_instance()));
            let error = Config::parse(&text, Path::new("/etc/hashi"))
                .unwrap_err()
                .to_string();
            assert!(error.contains("is not supported"), "{refused:?}: {error}");
        }
    }

    #[test]
    fn protocol_sources_and_file_sinks_parse_and_unknown_kinds_are_refused() {
        let mqtt = "name = \"line-1\"\n\
             source.mqtt = { host = \"broker\", port = 8883, protocol = \"v5\", client_id = \"line-1\", \
             ca_file = \"ca.pem\", timeout_ms = 3000, max_packet_bytes = 65536, schema_version = 1, \
             routes = [{ filter = \"plant/#\", kind = \"reading\", decoder = \"json\" }] }\n\
             sink.parquet = { dir = \"out\" }";
        let parsed = Config::parse(&config(mqtt), Path::new("/etc/hashi")).unwrap();
        assert_eq!(parsed.instances[0].source.family(), "mqtt");
        assert_eq!(parsed.instances[0].sink.family(), "parquet");
        let SourceConfig::Mqtt(parsed_mqtt) = &parsed.instances[0].source else {
            panic!("expected mqtt");
        };
        assert_eq!(
            parsed_mqtt.ca_file.as_deref(),
            Some(Path::new("/etc/hashi/ca.pem"))
        );

        let modbus = "name = \"line-1\"\n\
             source.modbus = { state_dir = \"modbus\", devices = [{ \
             endpoint = { type = \"tcp\", host = \"10.0.0.1\", port = 502 }, unit = 1, timeout_ms = 500, \
             min_request_interval_ms = 10, max_backoff_ms = 1000, groups = [{ kind = \"samples\", \
             record_key = \"d1\", schema_version = 1, table = \"holding_registers\", interval_ms = 100, \
             registers = [{ name = \"temp\", address = 0, value_type = \"f32\", word_order = \"big\", \
             byte_order = \"big\", scale = 1.0, offset = 0.0 }] }] }] }\n\
             sink.kafka = { dir = \"out\", topic = \"line-1\" }";
        let parsed = Config::parse(&config(modbus), Path::new("/etc/hashi")).unwrap();
        assert_eq!(parsed.instances[0].source.family(), "modbus");
        assert_eq!(parsed.instances[0].sink.family(), "kafka");

        let mysql = "name = \"line-1\"\n\
             source.mysql = { url = \"mysql://user@db:3306\", credential_file = \"secrets/mysql\", \
             tables = [{ database = \"plant\", table = \"items\" }], state_dir = \"mysql\", \
             chunk_rows = 100, max_batch_bytes = 1048576, server_id = 1024, schema_version = 1, timeout_ms = 20000 }\n\
             sink.jsonl = { dir = \"out\" }";
        let parsed = Config::parse(&config(mysql), Path::new("/etc/hashi")).unwrap();
        let SourceConfig::Mysql(mysql) = &parsed.instances[0].source else {
            panic!("expected mysql");
        };
        assert_eq!(mysql.credential_file, Path::new("/etc/hashi/secrets/mysql"));

        let historian = "name = \"line-1\"\n\
             source.historian = { start_ms = 0, chunk_ms = 60000, page_limit = 100, settle_ms = 1000, \
             state_dir = \"hist\", adapter.mysql = { url = \"mysql://user@db:3306\", \
             credential_file = \"secrets/mysql\", timeout_ms = 20000, mapping_database = \"hist\", \
             tag_table = \"tags\", tag_id_column = \"id\", tag_name_column = \"name\", \
             value_table = \"samples\", value_tag_column = \"tag_id\", time_column = \"ts\", \
             value_column = \"value\", quality_column = \"quality\" } }\n\
             sink.jsonl = { dir = \"out\" }";
        let parsed = Config::parse(&config(historian), Path::new("/etc/hashi")).unwrap();
        assert_eq!(parsed.instances[0].source.family(), "historian");

        let mssql = "name = \"line-1\"\n\
             source.mssql = { host = \"db\", port = 1433, user = \"sa\", database = \"plant\", \
             credential_file = \"secrets/mssql\", tables = [{ schema = \"dbo\", table = \"items\" }], \
             state_dir = \"mssql\", chunk_rows = 100, max_batch_bytes = 1048576, schema_version = 1, \
             timeout_ms = 20000, max_lag_ms = 60000 }\n\
             sink.jsonl = { dir = \"out\" }";
        let parsed = Config::parse(&config(mssql), Path::new("/etc/hashi")).unwrap();
        assert_eq!(parsed.instances[0].source.family(), "mssql");

        let postgres = "name = \"line-1\"\n\
             source.postgres = { host = \"db\", port = 5432, user = \"hashi\", database = \"plant\", \
             credential_file = \"secrets/postgres\", tables = [{ schema = \"public\", table = \"items\" }], \
             state_dir = \"postgres\", chunk_rows = 100, max_batch_bytes = 1048576, schema_version = 1, \
             timeout_ms = 20000, max_lag_ms = 60000 }\n\
             sink.jsonl = { dir = \"out\" }";
        let parsed = Config::parse(&config(postgres), Path::new("/etc/hashi")).unwrap();
        assert_eq!(parsed.instances[0].source.family(), "postgres");

        let error = Config::parse(
            &config(&valid_instance().replace("sink.jsonl", "sink.platform")),
            Path::new("."),
        )
        .unwrap_err()
        .message;
        assert!(error.contains("platform"), "{error}");
        let missing_ca = mqtt.replace("ca_file = \"ca.pem\", ", "");
        let error = Config::parse(&config(&missing_ca), Path::new("."))
            .unwrap_err()
            .message;
        assert!(error.contains("source.mqtt.ca_file"), "{error}");
    }
}
