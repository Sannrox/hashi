//! `hashi diagnostics --config <file>`: a support bundle without secrets or payloads.
//!
//! The bundle is one JSON document built from the config and durable state:
//! version, instances with their source and sink families, committed
//! positions, quarantine counts by reason, and schema decisions. It never
//! reads credential files and leaves out credential references, source
//! cursors, record keys, payloads and sink rejection texts, so it can leave
//! an air-gapped site.

use std::collections::BTreeMap;

use serde::Serialize;

use crate::config::{Config, InstanceConfig, SinkConfig, SourceConfig};
use crate::{CheckpointStore, Error, FileCheckpoint};

#[derive(Debug, Serialize)]
pub struct Bundle {
    pub hashi_version: &'static str,
    pub contract_version: u32,
    pub instances: Vec<InstanceDiagnostics>,
}

#[derive(Debug, Serialize)]
pub struct InstanceDiagnostics {
    pub name: String,
    pub source: &'static str,
    pub sink: &'static str,
    /// None when no checkpoint has been committed yet.
    pub checkpoint: Option<CheckpointSummary>,
    pub quarantined_by_reason: BTreeMap<String, u64>,
    pub schema_kinds: BTreeMap<String, SchemaSummary>,
    /// Set when state could not be read. A fixed text, never a parser message.
    pub state_error: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct CheckpointSummary {
    pub position: u64,
    pub contract_version: u32,
    pub has_source_cursor: bool,
}

#[derive(Debug, Serialize)]
pub struct SchemaSummary {
    pub version: u64,
    pub fields: usize,
    pub quarantined: bool,
    pub control_pending: bool,
}

/// Read-only: never creates directories, takes leases, or opens sources.
pub fn collect(config: &Config) -> Bundle {
    Bundle {
        hashi_version: env!("CARGO_PKG_VERSION"),
        contract_version: crate::CONTRACT_VERSION,
        instances: config
            .instances
            .iter()
            .map(|instance| instance_diagnostics(config, instance))
            .collect(),
    }
}

fn instance_diagnostics(config: &Config, instance: &InstanceConfig) -> InstanceDiagnostics {
    let SourceConfig::Fixture(_) = &instance.source;
    let SinkConfig::Jsonl(_) = &instance.sink;
    let mut diagnostics = InstanceDiagnostics {
        name: instance.name.clone(),
        source: "fixture",
        sink: "jsonl",
        checkpoint: None,
        quarantined_by_reason: BTreeMap::new(),
        schema_kinds: BTreeMap::new(),
        state_error: None,
    };
    let dir = config.checkpoint_dir.join(&instance.name);
    // Only a missing directory means "no state yet"; a permission error must show.
    let readable = match std::fs::metadata(&dir) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return diagnostics,
        Ok(metadata) => metadata.is_dir() && read_state(&dir, instance, &mut diagnostics).is_ok(),
        Err(_) => false,
    };
    // Parser messages can quote state contents, so only the fact is reported.
    if !readable {
        diagnostics.state_error = Some("checkpoint state is unreadable".into());
    }
    diagnostics
}

fn read_state(
    dir: &std::path::Path,
    instance: &InstanceConfig,
    diagnostics: &mut InstanceDiagnostics,
) -> Result<(), Error> {
    let store = FileCheckpoint::open(dir)?;
    let connector = crate::service::connector_id(instance);
    diagnostics.checkpoint =
        store
            .load(&connector, &instance.name)?
            .map(|checkpoint| CheckpointSummary {
                position: checkpoint.position,
                contract_version: checkpoint.contract_version,
                has_source_cursor: checkpoint.source_cursor.is_some(),
            });
    let schemas = store.load_schemas(&connector, &instance.name)?;
    for row in store.quarantined(&connector, &instance.name)? {
        let schema_reason = schemas
            .kinds
            .values()
            .any(|state| state.quarantine.as_deref() == Some(row.reason.as_str()));
        let reason = if schema_reason || RUNTIME_REASONS.contains(&row.reason.as_str()) {
            row.reason
        } else {
            SINK_REJECTED.to_string()
        };
        *diagnostics.quarantined_by_reason.entry(reason).or_default() += 1;
    }
    for (kind, state) in schemas.kinds {
        diagnostics.schema_kinds.insert(
            kind,
            SchemaSummary {
                version: state.schema.version,
                fields: state.schema.fields.len(),
                quarantined: state.quarantine.is_some(),
                control_pending: state.pending,
            },
        );
    }
    Ok(())
}

/// Quarantine reasons the runtime writes itself. Schema drift reasons come
/// from schema state. Anything else is sink text, which may quote the record.
const RUNTIME_REASONS: &[&str] = &[
    "record kind absent from discovery",
    "missing kind or key",
    "payload exceeds max_record_bytes",
];
const SINK_REJECTED: &str = "rejected by the sink";
