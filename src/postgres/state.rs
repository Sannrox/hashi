use super::*;
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Column {
    pub name: String,
    pub type_oid: u32,
    pub field: FieldType,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Layout {
    pub schema: String,
    pub table: String,
    pub keys: Vec<String>,
    pub columns: Vec<Column>,
    pub version: u64,
}

impl Layout {
    pub fn kind(&self) -> String {
        format!("{}.{}", self.schema, self.table)
    }
    pub fn discovered(&self) -> DiscoveredKind {
        DiscoveredKind {
            kind: self.kind(),
            schema: Schema {
                version: self.version,
                key_fields: self.keys.clone(),
                fields: self
                    .columns
                    .iter()
                    .map(|c| (c.name.clone(), c.field.clone()))
                    .collect(),
            },
            metadata: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Batch {
    pub first: Position,
    pub last: Position,
    pub cursor_before: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct State {
    pub version: u32,
    pub fingerprint: String,
    pub high_water: Position,
    pub lsn: String,
    pub slot: String,
    pub publication: String,
    pub layouts: Vec<Layout>,
    pub batch: Option<Batch>,
}

pub(super) fn state_path(dir: &Path) -> PathBuf {
    dir.join("postgres.state.v1.json")
}

pub(super) fn batch_path(dir: &Path, batch: &Batch) -> PathBuf {
    dir.join(format!("postgres.batch-{}.v1", batch.first))
}

pub(super) fn save(dir: &Path, state: &State) -> Result<(), Error> {
    let text =
        serde_json::to_string(state).map_err(|_| Error::new("PostgreSQL state encoding failed"))?;
    crate::checkpoint::atomic_write(&state_path(dir), &text)
}

pub(super) fn load(dir: &Path, fingerprint: &str) -> Result<Option<State>, Error> {
    let bytes = match std::fs::read(state_path(dir)) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let state: State =
        serde_json::from_slice(&bytes).map_err(|_| Error::new("PostgreSQL state is unreadable"))?;
    if state.version != 1 || state.fingerprint != fingerprint || state.layouts.is_empty() {
        return Err(Error::new(
            "PostgreSQL state version or source configuration mismatch",
        ));
    }
    replication::parse_lsn(&state.lsn)?;
    if state.slot.is_empty() || state.publication.is_empty() {
        return Err(Error::new("PostgreSQL state is missing slot identity"));
    }
    Ok(Some(state))
}

pub(super) fn recover_batch(
    dir: &Path,
    state: &State,
    max_bytes: usize,
) -> Result<VecDeque<Record>, Error> {
    let Some(batch) = &state.batch else {
        return Ok(VecDeque::new());
    };
    if batch.first == 0 || batch.first > batch.last || batch.last != state.high_water {
        return Err(Error::new("PostgreSQL batch positions are invalid"));
    }
    let path = batch_path(dir, batch);
    if std::fs::metadata(&path)?.len() > max_bytes as u64 {
        return Err(Error::new("PostgreSQL batch exceeds configured bound"));
    }
    let text = std::fs::read_to_string(path)?;
    if !text.ends_with('\n') {
        return Err(Error::new("PostgreSQL batch is incomplete"));
    }
    let mut rows = VecDeque::new();
    let mut expected = batch.first;
    for line in text.lines() {
        let (record, id) = crate::delivery::decode(line)?;
        if record.position != expected
            || id != record.idempotency_key("postgres", "spool")
            || record
                .source
                .as_ref()
                .and_then(|s| s.cursor.as_ref())
                .is_none()
        {
            return Err(Error::new("PostgreSQL batch record is invalid"));
        }
        expected = expected
            .checked_add(1)
            .ok_or_else(|| Error::new("PostgreSQL position exhausted"))?;
        rows.push_back(record);
    }
    if rows.back().is_none_or(|r| r.position != batch.last) {
        return Err(Error::new("PostgreSQL batch high-water mismatch"));
    }
    Ok(rows)
}
