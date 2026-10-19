//! Versioned discovery and durable per-kind drift decisions.
use crate::Error;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const CONTROL_KIND: &str = "__hashi_schema";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldType {
    Bool,
    I32,
    I64,
    F32,
    F64,
    Text,
    Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Schema {
    pub version: u64,
    pub key_fields: Vec<String>,
    pub fields: BTreeMap<String, FieldType>,
}
impl Schema {
    pub fn text() -> Self {
        Self {
            version: 1,
            key_fields: vec![],
            fields: BTreeMap::from([("payload".into(), FieldType::Text)]),
        }
    }
    pub fn validate(&self) -> Result<(), Error> {
        if self.version == 0
            || self.fields.is_empty()
            || self.fields.keys().any(|k| k.trim().is_empty())
            || self.key_fields.iter().any(|k| !self.fields.contains_key(k))
            || self
                .key_fields
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != self.key_fields.len()
        {
            return Err(Error::new("invalid discovered schema"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DriftClass {
    Initial,
    Additive,
    Widening,
    Breaking,
    Resolved,
}

/// Version changes alone are recorded. Only lossless numeric widenings are compatible.
pub fn classify(old: &Schema, new: &Schema) -> (DriftClass, String) {
    if new.version <= old.version {
        return (
            DriftClass::Breaking,
            "schema version did not increase".into(),
        );
    }
    if old.key_fields != new.key_fields {
        return (DriftClass::Breaking, "record key changed".into());
    }
    let mut widened = false;
    for (name, ty) in &old.fields {
        let Some(next) = new.fields.get(name) else {
            return (DriftClass::Breaking, format!("field removed: {name}"));
        };
        if ty == next {
            continue;
        }
        if old.key_fields.contains(name)
            || !matches!(
                (ty, next),
                (FieldType::I32, FieldType::I64 | FieldType::F64)
                    | (FieldType::F32, FieldType::F64)
            )
        {
            return (DriftClass::Breaking, format!("field type changed: {name}"));
        }
        widened = true;
    }
    (
        if widened {
            DriftClass::Widening
        } else {
            DriftClass::Additive
        },
        "compatible schema change".into(),
    )
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DriftEvent {
    pub kind: String,
    pub previous: Option<Schema>,
    pub current: Schema,
    pub class: DriftClass,
    pub reason: String,
    /// Durable sequence, independent of source positions and schema revisions.
    pub sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KindState {
    pub schema: Schema,
    pub quarantine: Option<String>,
    pub event: DriftEvent,
    pub pending: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaState {
    pub kinds: BTreeMap<String, KindState>,
}

impl SchemaState {
    pub fn observe(&mut self, kind: &str, schema: &Schema) -> Result<bool, Error> {
        schema.validate()?;
        if kind.trim().is_empty() || kind == CONTROL_KIND {
            return Err(Error::new("invalid or reserved record kind"));
        }
        if let Some(old) = self.kinds.get(kind) {
            if old.schema == *schema {
                return Ok(false);
            }
            if old.pending {
                return Err(Error::new(
                    "pending schema event must be acknowledged before discovery changes",
                ));
            }
            if old.quarantine.is_some() {
                return Err(Error::new(format!(
                    "resolve quarantined schema for {kind} before changing discovery"
                )));
            }
        }
        let old = self.kinds.get(kind);
        let pending = old.is_some();
        let (class, reason) = old
            .map(|old| classify(&old.schema, schema))
            .unwrap_or((DriftClass::Initial, "initial discovery".into()));
        let sequence = old
            .map(|old| old.event.sequence)
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| Error::new("schema event sequence exhausted"))?;
        let event = DriftEvent {
            kind: kind.into(),
            previous: old.map(|old| old.schema.clone()),
            current: schema.clone(),
            class: class.clone(),
            reason: reason.clone(),
            sequence,
        };
        self.kinds.insert(
            kind.into(),
            KindState {
                schema: schema.clone(),
                quarantine: (class == DriftClass::Breaking).then_some(reason),
                event,
                pending,
            },
        );
        Ok(true)
    }

    /// Explicitly accept the exact quarantined revision after consumer remediation.
    pub fn resolve(&mut self, kind: &str, schema: &Schema) -> Result<(), Error> {
        let state = self
            .kinds
            .get_mut(kind)
            .ok_or_else(|| Error::new("unknown schema kind"))?;
        if state.schema != *schema || state.quarantine.is_none() || state.pending {
            return Err(Error::new(
                "schema resolution requires the acknowledged quarantined revision",
            ));
        }
        state.event.sequence = state
            .event
            .sequence
            .checked_add(1)
            .ok_or_else(|| Error::new("schema event sequence exhausted"))?;
        state.event.class = DriftClass::Resolved;
        state.event.reason = "explicitly accepted after remediation".into();
        state.quarantine = None;
        state.pending = true;
        Ok(())
    }
}
