//! Operator commands: discover a configured source and review OPC UA trust.
//!
//! The CLI is a thin wrapper. Discover never trusts a certificate. Trust
//! list/approve/revoke hold the same exclusive PKI lock `connect` holds, keep
//! PEM and keys out of output, and leave no certificate in both `trusted/`
//! and `rejected/` after a crash.

use std::path::Path;

use serde::Serialize;

#[cfg(feature = "modbus")]
use crate::config::ModbusSourceConfig;
#[cfg(feature = "mqtt")]
use crate::config::MqttSourceConfig;
#[cfg(feature = "mssql")]
use crate::config::MssqlSourceConfig;
#[cfg(feature = "mysql")]
use crate::config::MysqlSourceConfig;
#[cfg(feature = "postgres")]
use crate::config::PostgresSourceConfig;
use crate::config::{Config, HistorianAdapterConfig, InstanceConfig, SourceConfig, WriterTarget};
use crate::contract::DiscoveredKind;
use crate::credential::FileCredential;
use crate::fake::{FakeSource, record};
#[cfg(any(
    feature = "mqtt",
    feature = "modbus",
    feature = "mysql",
    feature = "mssql",
    feature = "postgres"
))]
use crate::schema::FieldType;
use crate::schema::Schema;
use crate::{Connector, Error};

/// JSON schema for discover and trust documents.
pub const OPERATOR_SCHEMA_VERSION: u32 = 1;

/// Discover output: versioned kinds keyed `(kind, key)`.
#[derive(Debug, Serialize)]
pub struct DiscoverDocument {
    pub schema_version: u32,
    pub instance: String,
    pub source: String,
    pub kinds: Vec<DiscoverKind>,
}

#[derive(Debug, Serialize)]
pub struct DiscoverKind {
    pub kind: String,
    /// Same identity `run` uses for the kind; discover does not list record keys.
    pub key: String,
    pub schema: crate::schema::Schema,
    pub metadata: std::collections::BTreeMap<String, String>,
}

/// Printed when discover is refused because the OPC UA server is untrusted.
#[derive(Debug, Serialize)]
pub struct UntrustedDocument {
    pub schema_version: u32,
    pub error: &'static str,
    pub message: String,
    pub rejected: Vec<CertificateSummary>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CertificateSummary {
    pub sha256: String,
    pub subject: String,
    pub issuer: String,
    pub not_before: String,
    pub not_after: String,
}

#[derive(Debug, Serialize)]
pub struct TrustDocument {
    pub schema_version: u32,
    pub instance: String,
    pub rejected: Vec<CertificateSummary>,
    pub trusted: Vec<CertificateSummary>,
}

/// Discover failed in a way the CLI should print as JSON or as a message.
pub enum DiscoverOutcome {
    Ok(DiscoverDocument),
    Untrusted(UntrustedDocument),
}

/// Pick the instance `discover` and `trust` run against.
pub fn select_instance<'a>(
    config: &'a Config,
    instance: Option<&str>,
) -> Result<&'a InstanceConfig, Error> {
    match instance {
        None if config.instances.len() == 1 => Ok(&config.instances[0]),
        None => Err(Error::new(
            "pass --instance <name>; this config declares more than one instance",
        )),
        Some(name) => config
            .instances
            .iter()
            .find(|instance| instance.name == name)
            .ok_or_else(|| Error::new(format!("instance {name} is not in the config"))),
    }
}

/// Discover the instance's source. Never trusts a certificate.
pub fn discover(config: &Config, instance: Option<&str>) -> Result<DiscoverOutcome, Error> {
    let instance = select_instance(config, instance)?;
    match &instance.source {
        SourceConfig::Fixture(fixture) => {
            let records = (1..=fixture.records)
                .map(|position| {
                    record(
                        "reading",
                        &format!("r{position}"),
                        position,
                        &format!("value-{position}"),
                    )
                })
                .collect();
            let source = FakeSource::with_live_credential(
                crate::service::connector_id(instance),
                records,
                FileCredential::new(&fixture.credential_file),
                fixture.accept_sha256.clone(),
            );
            document(instance, "fixture", source.discover()?)
        }
        SourceConfig::OpcUa(_) => {
            #[cfg(feature = "opcua")]
            {
                discover_opcua(instance)
            }
            #[cfg(not(feature = "opcua"))]
            {
                Err(Error::new("hashi discover needs the opcua feature"))
            }
        }
        SourceConfig::Mqtt(mqtt) => {
            #[cfg(feature = "mqtt")]
            {
                document(instance, "mqtt", mqtt_discovered(mqtt))
            }
            #[cfg(not(feature = "mqtt"))]
            {
                let _ = mqtt;
                Err(Error::new("hashi discover needs the mqtt feature"))
            }
        }
        SourceConfig::Modbus(modbus) => {
            #[cfg(feature = "modbus")]
            {
                document(instance, "modbus", modbus_discovered(modbus))
            }
            #[cfg(not(feature = "modbus"))]
            {
                let _ = modbus;
                Err(Error::new("hashi discover needs the modbus feature"))
            }
        }
        SourceConfig::Mysql(mysql) => {
            #[cfg(feature = "mysql")]
            {
                document(instance, "mysql", mysql_discovered(mysql))
            }
            #[cfg(not(feature = "mysql"))]
            {
                let _ = mysql;
                Err(Error::new("hashi discover needs the mysql feature"))
            }
        }
        SourceConfig::Mssql(mssql) => {
            #[cfg(feature = "mssql")]
            {
                document(instance, "mssql", mssql_discovered(mssql))
            }
            #[cfg(not(feature = "mssql"))]
            {
                let _ = mssql;
                Err(Error::new("hashi discover needs the mssql feature"))
            }
        }
        SourceConfig::Postgres(postgres) => {
            #[cfg(feature = "postgres")]
            {
                document(instance, "postgres", postgres_discovered(postgres))
            }
            #[cfg(not(feature = "postgres"))]
            {
                let _ = postgres;
                Err(Error::new("hashi discover needs the postgres feature"))
            }
        }
        SourceConfig::Historian(_) => document(instance, "historian", historian_discovered()),
    }
}

#[cfg(feature = "mqtt")]
fn mqtt_discovered(mqtt: &MqttSourceConfig) -> Vec<DiscoveredKind> {
    let mut kinds: std::collections::BTreeMap<&str, (&str, Vec<&str>)> =
        std::collections::BTreeMap::new();
    for route in &mqtt.routes {
        let entry = kinds
            .entry(route.kind.as_str())
            .or_insert((route.decoder.as_str(), Vec::new()));
        entry.1.push(route.filter.as_str());
    }
    kinds
        .into_iter()
        .map(|(kind, (decoder, filters))| DiscoveredKind {
            kind: kind.to_string(),
            schema: Schema {
                version: mqtt.schema_version,
                key_fields: vec![],
                fields: std::collections::BTreeMap::from([(
                    "payload".into(),
                    match decoder {
                        "raw" => FieldType::Bytes,
                        _ => FieldType::Text,
                    },
                )]),
            },
            metadata: std::collections::BTreeMap::from([(
                "topic_filters".into(),
                filters.join(" "),
            )]),
        })
        .collect()
}

#[cfg(feature = "modbus")]
fn modbus_discovered(modbus: &ModbusSourceConfig) -> Vec<DiscoveredKind> {
    modbus
        .devices
        .iter()
        .flat_map(|device| &device.groups)
        .map(|group| DiscoveredKind {
            kind: group.kind.clone(),
            schema: Schema {
                version: group.schema_version,
                key_fields: vec![],
                fields: group
                    .registers
                    .iter()
                    .flat_map(|register| {
                        let mut fields = vec![(
                            register.name.clone(),
                            if register.value_type == "bool" {
                                FieldType::Bool
                            } else {
                                FieldType::F64
                            },
                        )];
                        if register.unit.is_some() {
                            fields.push((format!("{}_unit", register.name), FieldType::Text));
                        }
                        fields
                    })
                    .collect(),
            },
            metadata: Default::default(),
        })
        .collect()
}

#[cfg(feature = "mysql")]
fn mysql_discovered(mysql: &MysqlSourceConfig) -> Vec<DiscoveredKind> {
    let mut kinds: Vec<_> = mysql
        .tables
        .iter()
        .map(|table| {
            sql_table_kind(
                format!("{}.{}", table.database, table.table),
                mysql.schema_version,
                &table.key,
            )
        })
        .collect();
    kinds.push(sql_transaction_kind("__mysql_transaction"));
    kinds
}

#[cfg(feature = "mssql")]
fn mssql_discovered(mssql: &MssqlSourceConfig) -> Vec<DiscoveredKind> {
    let mut kinds: Vec<_> = mssql
        .tables
        .iter()
        .map(|table| {
            sql_table_kind(
                format!("{}.{}", table.schema, table.table),
                mssql.schema_version,
                &table.key,
            )
        })
        .collect();
    kinds.push(sql_transaction_kind("__mssql_transaction"));
    kinds
}

#[cfg(feature = "postgres")]
fn postgres_discovered(postgres: &PostgresSourceConfig) -> Vec<DiscoveredKind> {
    let mut kinds: Vec<_> = postgres
        .tables
        .iter()
        .map(|table| {
            sql_table_kind(
                format!("{}.{}", table.schema, table.table),
                postgres.schema_version,
                &table.key,
            )
        })
        .collect();
    kinds.push(sql_transaction_kind("__postgres_transaction"));
    kinds
}

#[cfg(any(feature = "mysql", feature = "mssql", feature = "postgres"))]
fn sql_table_kind(kind: String, version: u64, keys: &[String]) -> DiscoveredKind {
    let mut fields = std::collections::BTreeMap::from([("payload".into(), FieldType::Text)]);
    for key in keys {
        fields.insert(key.clone(), FieldType::Text);
    }
    DiscoveredKind {
        kind,
        schema: Schema {
            version,
            key_fields: keys.to_vec(),
            fields,
        },
        metadata: Default::default(),
    }
}

#[cfg(any(feature = "mysql", feature = "mssql", feature = "postgres"))]
fn sql_transaction_kind(kind: &str) -> DiscoveredKind {
    DiscoveredKind {
        kind: kind.into(),
        schema: Schema {
            version: 1,
            key_fields: vec![],
            fields: std::collections::BTreeMap::from([("transaction_id".into(), FieldType::Text)]),
        },
        metadata: Default::default(),
    }
}

fn historian_discovered() -> Vec<DiscoveredKind> {
    vec![DiscoveredKind {
        kind: crate::historian::SAMPLE_KIND.into(),
        schema: Schema::text(),
        metadata: Default::default(),
    }]
}

fn document(
    instance: &InstanceConfig,
    source: &str,
    kinds: Vec<DiscoveredKind>,
) -> Result<DiscoverOutcome, Error> {
    Ok(DiscoverOutcome::Ok(DiscoverDocument {
        schema_version: OPERATOR_SCHEMA_VERSION,
        instance: instance.name.clone(),
        source: source.to_string(),
        kinds: kinds
            .into_iter()
            .map(|kind| DiscoverKind {
                key: kind.kind.clone(),
                kind: kind.kind,
                schema: kind.schema,
                metadata: kind.metadata,
            })
            .collect(),
    }))
}

/// List rejected and trusted certificates for an OPC UA instance.
pub fn trust_list(config: &Config, instance: Option<&str>) -> Result<TrustDocument, Error> {
    let (name, pki) = trust_target(config, instance)?;
    #[cfg(feature = "opcua")]
    {
        let store = trust::list(pki)?;
        Ok(TrustDocument {
            schema_version: OPERATOR_SCHEMA_VERSION,
            instance: name.to_string(),
            rejected: store.rejected,
            trusted: store.trusted,
        })
    }
    #[cfg(not(feature = "opcua"))]
    {
        let _ = (name, pki);
        Err(Error::new("hashi trust needs the opcua feature"))
    }
}

/// Move one rejected certificate into `trusted/` by SHA-256 fingerprint.
pub fn trust_approve(
    config: &Config,
    instance: Option<&str>,
    sha256: &str,
) -> Result<TrustDocument, Error> {
    let (name, pki) = trust_target(config, instance)?;
    #[cfg(feature = "opcua")]
    {
        trust::approve(pki, sha256)?;
        trust_list(config, Some(name))
    }
    #[cfg(not(feature = "opcua"))]
    {
        let _ = (name, pki, sha256);
        Err(Error::new("hashi trust needs the opcua feature"))
    }
}

/// Move one trusted certificate into `rejected/` by SHA-256 fingerprint.
pub fn trust_revoke(
    config: &Config,
    instance: Option<&str>,
    sha256: &str,
) -> Result<TrustDocument, Error> {
    let (name, pki) = trust_target(config, instance)?;
    #[cfg(feature = "opcua")]
    {
        trust::revoke(pki, sha256)?;
        trust_list(config, Some(name))
    }
    #[cfg(not(feature = "opcua"))]
    {
        let _ = (name, pki, sha256);
        Err(Error::new("hashi trust needs the opcua feature"))
    }
}

/// The PKI store `trust` manages and the name it reports: a writer's target
/// when `name` names a writer, otherwise the selected instance's source.
fn trust_target<'a>(config: &'a Config, name: Option<&str>) -> Result<(&'a str, &'a Path), Error> {
    if let Some(name) = name
        && let Some(writer) = config.writers.iter().find(|writer| writer.name == name)
    {
        return match &writer.target {
            WriterTarget::OpcUa(opcua) => Ok((&writer.name, &opcua.pki_dir)),
            WriterTarget::Mqtt(_) => Err(Error::new(
                "trust does not apply to MQTT writers; set ca_file instead",
            )),
        };
    }
    let instance = select_instance(config, name)?;
    Ok((&instance.name, pki_dir(instance)?))
}

fn pki_dir(instance: &InstanceConfig) -> Result<&Path, Error> {
    match &instance.source {
        SourceConfig::OpcUa(opcua) => Ok(&opcua.pki_dir),
        SourceConfig::Historian(historian) => match &historian.adapter {
            HistorianAdapterConfig::OpcUa(opcua) => Ok(&opcua.pki_dir),
            _ => Err(Error::new("trust does not apply to this historian adapter")),
        },
        other => Err(Error::new(format!(
            "trust does not apply to {}",
            other.family()
        ))),
    }
}

/// Replay a crashed approve/revoke and fail-closed sweep. The caller holds
/// the PKI lease (`connect` and `trust` share it).
#[cfg(feature = "opcua")]
pub(crate) fn prepare_opcua_pki(pki: &Path) -> Result<(), Error> {
    trust::prepare(pki)
}

#[cfg(feature = "opcua")]
impl crate::config::OpcUaSourceConfig {
    pub(crate) fn connector_config(&self, id: &str) -> Result<crate::opcua::Config, Error> {
        let authentication = if let Some(path) = &self.credential_file {
            let (user, password) = FileCredential::new(path).user_password()?;
            crate::opcua::Authentication::UserName {
                user: user.expose().to_string(),
                password: password.expose().to_string(),
            }
        } else {
            crate::opcua::Authentication::X509 {
                certificate: self
                    .user_certificate
                    .clone()
                    .ok_or_else(|| Error::new("OPC UA user certificate is required"))?,
                private_key: self
                    .user_private_key
                    .clone()
                    .ok_or_else(|| Error::new("OPC UA user private key is required"))?,
            }
        };
        Ok(crate::opcua::Config {
            id: id.to_string(),
            endpoint: self.endpoint.clone(),
            application_uri: self.application_uri.clone(),
            pki_dir: self.pki_dir.clone(),
            policy: crate::opcua::Policy::from_config(&self.policy)?,
            allow_insecure_lab: self.allow_insecure_lab,
            authentication,
            roots: self
                .roots
                .iter()
                .map(|root| crate::opcua::parse_root(root))
                .collect::<Result<Vec<_>, _>>()?,
            max_nodes: usize::try_from(self.max_nodes).unwrap_or(usize::MAX),
            timeout: std::time::Duration::from_millis(self.timeout_ms),
            schema_version: self.schema_version,
        })
    }

    pub(crate) fn subscription_config(&self) -> Result<crate::opcua::SubscriptionConfig, Error> {
        let state_dir = self
            .state_dir
            .clone()
            .ok_or_else(|| Error::new("OPC UA subscription state_dir is required"))?;
        Ok(crate::opcua::SubscriptionConfig {
            nodes: self
                .nodes
                .iter()
                .map(|node| crate::opcua::subscription_node(node))
                .collect::<Result<Vec<_>, Error>>()?,
            publishing_interval: std::time::Duration::from_millis(
                self.publishing_interval_ms.unwrap_or(1000),
            ),
            sampling_interval: std::time::Duration::from_millis(
                self.sampling_interval_ms.unwrap_or(500),
            ),
            deadband: self.deadband,
            queue_size: self.queue_size.unwrap_or(10),
            state_dir,
            max_pending_bytes: usize::try_from(self.max_pending_bytes.unwrap_or(16 * 1024 * 1024))
                .unwrap_or(usize::MAX),
        })
    }
}

#[cfg(feature = "opcua")]
fn discover_opcua(instance: &InstanceConfig) -> Result<DiscoverOutcome, Error> {
    let SourceConfig::OpcUa(opcua) = &instance.source else {
        return Err(Error::new("instance is not OPC UA"));
    };
    let config = opcua.connector_config(&instance.name)?;
    match crate::opcua::OpcUaConnector::connect(config) {
        Ok(source) => document(instance, "opcua", source.discover()?),
        Err(error) if error.message.contains("review the PKI rejected store") => {
            let store = trust::list(&opcua.pki_dir)?;
            if store.rejected.is_empty() {
                return Err(Error::new("OPC UA secure session failed"));
            }
            Ok(DiscoverOutcome::Untrusted(UntrustedDocument {
                schema_version: OPERATOR_SCHEMA_VERSION,
                error: "untrusted_certificate",
                message: error.message,
                rejected: store.rejected,
            }))
        }
        Err(error) => Err(error),
    }
}

#[cfg(feature = "opcua")]
mod trust {
    use std::fs::{self, File, OpenOptions};
    use std::io::{Read, Write};
    use std::os::unix::fs::OpenOptionsExt;
    use std::path::{Path, PathBuf};

    use serde::{Deserialize, Serialize};
    use sha2::{Digest, Sha256};

    use super::CertificateSummary;
    use crate::Error;
    use crate::lease;

    const JOURNAL: &str = ".hashi-trust-journal";
    const REJECTED: &str = "rejected";
    const TRUSTED: &str = "trusted";

    #[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(rename_all = "snake_case")]
    enum Op {
        Approve,
        Revoke,
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct Journal {
        op: Op,
        sha256: String,
    }

    pub struct Store {
        pub rejected: Vec<CertificateSummary>,
        pub trusted: Vec<CertificateSummary>,
    }

    pub fn list(pki: &Path) -> Result<Store, Error> {
        with_store(pki, |_, rejected, trusted| {
            Ok(Store {
                rejected: summaries(rejected)?,
                trusted: summaries(trusted)?,
            })
        })
    }

    pub fn approve(pki: &Path, sha256: &str) -> Result<(), Error> {
        move_by_fingerprint(pki, sha256, Op::Approve)
    }

    pub fn revoke(pki: &Path, sha256: &str) -> Result<(), Error> {
        move_by_fingerprint(pki, sha256, Op::Revoke)
    }

    fn move_by_fingerprint(pki: &Path, sha256: &str, op: Op) -> Result<(), Error> {
        let sha256 = normalize_fingerprint(sha256)?;
        with_store(pki, |lock, rejected, trusted| {
            let (source, dest) = match op {
                Op::Approve => (rejected, trusted),
                Op::Revoke => (trusted, rejected),
            };
            let source_hits = find(source, &sha256);
            let dest_hits = find(dest, &sha256);
            match (source_hits.len(), dest_hits.len()) {
                (0, 1) => Ok(()),
                (1, _) => {
                    lock.renew()?;
                    replay(pki, op, &sha256, source, dest)
                }
                (0, 0) => Err(Error::new(format!(
                    "certificate {sha256} is not in the PKI store"
                ))),
                _ => Err(Error::new(format!("certificate {sha256} is ambiguous"))),
            }
        })
    }

    fn with_store<T>(
        pki: &Path,
        f: impl FnOnce(&lease::DropRelease, &[Entry], &[Entry]) -> Result<T, Error>,
    ) -> Result<T, Error> {
        // Same TTL lease connect holds. A pause past the TTL is takeover;
        // renew before recover, sweep, and each move so a live holder keeps
        // the fence.
        let lock = lease::hold(
            pki,
            "hashi",
            lease::hold_ttl(),
            "OPC UA PKI store is already owned",
        )?;
        lock.renew()?;
        prepare(pki)?;
        lock.renew()?;
        let rejected = load(&pki.join(REJECTED))?;
        let trusted = load(&pki.join(TRUSTED))?;
        f(&lock, &rejected, &trusted)
    }

    pub(super) fn prepare(pki: &Path) -> Result<(), Error> {
        recover(pki)?;
        let rejected = load(&pki.join(REJECTED))?;
        let trusted = load(&pki.join(TRUSTED))?;
        sweep_duplicates(pki, &rejected, &trusted)
    }

    fn recover(pki: &Path) -> Result<(), Error> {
        let path = pki.join(JOURNAL);
        let text = match fs::read_to_string(&path) {
            Ok(text) => text,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        if text.trim().is_empty() {
            let _ = fs::remove_file(&path);
            return Ok(());
        }
        let journal: Journal = serde_json::from_str(&text)
            .map_err(|_| Error::new("OPC UA trust journal is unreadable"))?;
        let rejected = load(&pki.join(REJECTED))?;
        let trusted = load(&pki.join(TRUSTED))?;
        let (source, dest) = match journal.op {
            Op::Approve => (&rejected, &trusted),
            Op::Revoke => (&trusted, &rejected),
        };
        replay(pki, journal.op, &journal.sha256, source, dest)?;
        Ok(())
    }

    /// A crash without a journal can leave the same fingerprint in both
    /// directories. Fail closed: drop the trusted copy so the server stays
    /// untrusted until an operator approves again.
    fn sweep_duplicates(pki: &Path, rejected: &[Entry], trusted: &[Entry]) -> Result<(), Error> {
        for trusted_entry in trusted {
            if rejected
                .iter()
                .any(|entry| entry.sha256 == trusted_entry.sha256)
            {
                fs::remove_file(&trusted_entry.path)?;
                sync_dir(&pki.join(TRUSTED))?;
            }
        }
        Ok(())
    }

    fn replay(
        pki: &Path,
        op: Op,
        sha256: &str,
        source: &[Entry],
        dest: &[Entry],
    ) -> Result<(), Error> {
        let Some(entry) = source.iter().find(|entry| entry.sha256 == sha256) else {
            if dest.iter().any(|entry| entry.sha256 == sha256) {
                let _ = fs::remove_file(pki.join(JOURNAL));
                return Ok(());
            }
            let _ = fs::remove_file(pki.join(JOURNAL));
            return Err(Error::new(format!(
                "certificate {sha256} is not in the PKI store"
            )));
        };
        write_journal(pki, op, sha256)?;
        let dest_dir = match op {
            Op::Approve => pki.join(TRUSTED),
            Op::Revoke => pki.join(REJECTED),
        };
        fs::create_dir_all(&dest_dir)?;
        let dest_name = ::opcua::crypto::CertificateStore::cert_file_name(&entry.cert);
        let dest_path = dest_dir.join(&dest_name);
        if dest
            .iter()
            .any(|existing| existing.path != dest_path && existing.sha256 == sha256)
        {
            return Err(Error::new(format!("certificate {sha256} is ambiguous")));
        }
        if !dest_path.exists() {
            atomic_write(&dest_path, &entry.der)?;
        }
        fs::remove_file(&entry.path)?;
        sync_dir(entry.path.parent().unwrap_or(pki))?;
        let _ = fs::remove_file(pki.join(JOURNAL));
        sync_dir(pki)?;
        Ok(())
    }

    fn write_journal(pki: &Path, op: Op, sha256: &str) -> Result<(), Error> {
        let path = pki.join(JOURNAL);
        let body = serde_json::to_vec(&Journal {
            op,
            sha256: sha256.to_string(),
        })
        .map_err(|error| Error::new(error.to_string()))?;
        atomic_write(&path, &body)
    }

    fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), Error> {
        let dir = path.parent().unwrap_or(Path::new("."));
        fs::create_dir_all(dir)?;
        let tmp = dir.join(format!(
            ".{}.tmp",
            path.file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("trust")
        ));
        {
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .mode(0o600)
                .open(&tmp)?;
            file.write_all(bytes)?;
            file.sync_all()?;
        }
        fs::rename(&tmp, path)?;
        sync_dir(dir)
    }

    fn sync_dir(dir: &Path) -> Result<(), Error> {
        File::open(dir)?.sync_all()?;
        Ok(())
    }

    struct Entry {
        path: PathBuf,
        der: Vec<u8>,
        sha256: String,
        cert: ::opcua::crypto::X509,
        issuer: String,
    }

    fn issuer_dn(der: &[u8]) -> String {
        use x509_cert::der::Decode;
        x509_cert::Certificate::from_der(der)
            .map(|cert| cert.tbs_certificate.issuer.to_string().replace(';', "/"))
            .unwrap_or_default()
    }

    fn load(dir: &Path) -> Result<Vec<Entry>, Error> {
        let mut entries = Vec::new();
        let listing = match fs::read_dir(dir) {
            Ok(listing) => listing,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(entries),
            Err(error) => return Err(error.into()),
        };
        for item in listing {
            let item = item?;
            let path = item.path();
            let name = item.file_name();
            if name.to_string_lossy().starts_with('.') {
                continue;
            }
            let mut file = File::open(&path)?;
            let mut der = Vec::new();
            file.read_to_end(&mut der)?;
            let cert = ::opcua::crypto::X509::from_der(&der)
                .map_err(|_| Error::new(format!("certificate {} is unreadable", path.display())))?;
            let issuer = issuer_dn(&der);
            entries.push(Entry {
                sha256: fingerprint(&der),
                der,
                cert,
                issuer,
                path,
            });
        }
        entries.sort_by(|a, b| a.sha256.cmp(&b.sha256));
        Ok(entries)
    }

    fn find<'a>(entries: &'a [Entry], sha256: &str) -> Vec<&'a Entry> {
        entries
            .iter()
            .filter(|entry| entry.sha256 == sha256)
            .collect()
    }

    fn summaries(entries: &[Entry]) -> Result<Vec<CertificateSummary>, Error> {
        entries
            .iter()
            .map(|entry| {
                Ok(CertificateSummary {
                    sha256: entry.sha256.clone(),
                    issuer: entry.issuer.clone(),
                    subject: entry.cert.subject_name(),
                    not_before: entry
                        .cert
                        .not_before()
                        .map(|time| time.to_string())
                        .unwrap_or_default(),
                    not_after: entry
                        .cert
                        .not_after()
                        .map(|time| time.to_string())
                        .unwrap_or_default(),
                })
            })
            .collect()
    }

    fn fingerprint(der: &[u8]) -> String {
        format!("{:x}", Sha256::digest(der))
    }

    fn normalize_fingerprint(value: &str) -> Result<String, Error> {
        let value = value.trim().to_ascii_lowercase();
        if value.len() != 64
            || !value
                .bytes()
                .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
        {
            return Err(Error::new("sha256 must be 64 lowercase hex characters"));
        }
        Ok(value)
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::sync::atomic::{AtomicU64, Ordering};

        static SEQ: AtomicU64 = AtomicU64::new(0);

        fn dir() -> PathBuf {
            let path = std::env::temp_dir().join(format!(
                "hashi-trust-{}-{}",
                std::process::id(),
                SEQ.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir_all(path.join(REJECTED)).unwrap();
            fs::create_dir_all(path.join(TRUSTED)).unwrap();
            path
        }

        fn mint(dir: &Path, uri: &str) -> (PathBuf, Vec<u8>) {
            fs::create_dir_all(dir.join("own")).unwrap();
            fs::create_dir_all(dir.join("private")).unwrap();
            let data = ::opcua::crypto::X509Data {
                key_size: 2048,
                common_name: "Hashi fixture".into(),
                organization: "Hashi".into(),
                organizational_unit: "Test".into(),
                country: "DE".into(),
                state: "Test".into(),
                alt_host_names: vec![uri.into(), "127.0.0.1".into()].into(),
                certificate_duration_days: 1,
            };
            ::opcua::crypto::CertificateStore::create_certificate_and_key(
                &data,
                true,
                &dir.join("own/cert.der"),
                &dir.join("private/private.pem"),
            )
            .unwrap();
            let der = fs::read(dir.join("own/cert.der")).unwrap();
            (dir.join("own/cert.der"), der)
        }

        #[test]
        fn approve_by_fingerprint_then_revoke_is_idempotent() {
            let pki = dir();
            let (_path, der) = mint(&pki.join("server"), "urn:hashi:server");
            let sha = fingerprint(&der);
            let cert = ::opcua::crypto::X509::from_der(&der).unwrap();
            let name = ::opcua::crypto::CertificateStore::cert_file_name(&cert);
            fs::write(pki.join(REJECTED).join(&name), &der).unwrap();
            approve(&pki, &sha).unwrap();
            let store = list(&pki).unwrap();
            assert_eq!(store.trusted.len(), 1);
            assert!(
                !store.trusted[0].issuer.is_empty(),
                "issuer is the TBS issuer DN"
            );
            assert_eq!(store.trusted[0].issuer, store.trusted[0].subject);
            assert!(pki.join(TRUSTED).join(&name).is_file());
            assert!(fs::read_dir(pki.join(REJECTED)).unwrap().next().is_none());
            approve(&pki, &sha).unwrap();
            revoke(&pki, &sha).unwrap();
            assert!(pki.join(REJECTED).join(&name).is_file());
            assert!(fs::read_dir(pki.join(TRUSTED)).unwrap().next().is_none());
            revoke(&pki, &sha).unwrap();
            let _ = fs::remove_dir_all(&pki);
        }

        #[test]
        fn a_crash_between_file_operations_leaves_no_certificate_in_both_stores() {
            let pki = dir();
            let (_path, der) = mint(&pki.join("server"), "urn:hashi:server");
            let sha = fingerprint(&der);
            let cert = ::opcua::crypto::X509::from_der(&der).unwrap();
            let name = ::opcua::crypto::CertificateStore::cert_file_name(&cert);
            fs::write(pki.join(REJECTED).join(&name), &der).unwrap();
            fs::write(pki.join(TRUSTED).join(&name), &der).unwrap();
            write_journal(&pki, Op::Approve, &sha).unwrap();
            let store = list(&pki).unwrap();
            assert_eq!(store.trusted.len(), 1, "approve journal completes");
            assert!(store.rejected.is_empty(), "{:?}", store.rejected);
            fs::write(pki.join(REJECTED).join(&name), &der).unwrap();
            fs::write(pki.join(TRUSTED).join(&name), &der).unwrap();
            let store = list(&pki).unwrap();
            assert!(
                store.trusted.is_empty(),
                "without a journal, both copies fail closed"
            );
            assert_eq!(store.rejected.len(), 1);
            let _ = fs::remove_dir_all(&pki);
        }

        #[test]
        fn a_pending_revoke_journal_is_recovered_before_the_store_is_used() {
            let pki = dir();
            let (_path, der) = mint(&pki.join("server"), "urn:hashi:server");
            let sha = fingerprint(&der);
            let cert = ::opcua::crypto::X509::from_der(&der).unwrap();
            let name = ::opcua::crypto::CertificateStore::cert_file_name(&cert);
            fs::write(pki.join(TRUSTED).join(&name), &der).unwrap();
            write_journal(&pki, Op::Revoke, &sha).unwrap();
            prepare(&pki).unwrap();
            let store = list(&pki).unwrap();
            assert!(store.trusted.is_empty(), "{:?}", store.trusted);
            assert_eq!(store.rejected.len(), 1);
            let _ = fs::remove_dir_all(&pki);
        }

        #[test]
        fn unknown_and_ambiguous_fingerprints_are_refused() {
            let pki = dir();
            let err = approve(&pki, "abcd").unwrap_err();
            assert!(err.message.contains("64 lowercase hex"), "{}", err.message);
            let missing = "0".repeat(64);
            let err = approve(&pki, &missing).unwrap_err();
            assert!(
                err.message.contains("not in the PKI store"),
                "{}",
                err.message
            );
            let _ = fs::remove_dir_all(&pki);
        }
    }
}
