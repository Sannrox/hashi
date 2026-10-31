use super::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Mode {
    Cdc,
    ChangeTracking,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Column {
    pub name: String,
    pub sql_type: String,
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
    pub capture_instance: Option<String>,
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
    pub cursor: String,
    pub mode: Mode,
    pub layouts: Vec<Layout>,
    pub batch: Option<Batch>,
}

pub(super) fn state_path(dir: &Path) -> PathBuf {
    dir.join("mssql.state.v1.json")
}
pub(super) fn batch_path(dir: &Path, batch: &Batch) -> PathBuf {
    dir.join(format!("mssql.batch-{}.v1", batch.first))
}
pub(super) fn save(dir: &Path, state: &State) -> Result<(), Error> {
    let text =
        serde_json::to_string(state).map_err(|_| Error::new("SQL Server state encoding failed"))?;
    crate::checkpoint::atomic_write(&state_path(dir), &text)
}
pub(super) fn load(dir: &Path, fingerprint: &str) -> Result<Option<State>, Error> {
    let bytes = match std::fs::read(state_path(dir)) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let state: State =
        serde_json::from_slice(&bytes).map_err(|_| Error::new("SQL Server state is unreadable"))?;
    if state.version != 1 || state.fingerprint != fingerprint || state.layouts.is_empty() {
        return Err(Error::new(
            "SQL Server state version or source configuration mismatch",
        ));
    }
    match state.mode {
        Mode::Cdc => {
            parse_lsn(&state.cursor)?;
        }
        Mode::ChangeTracking => {
            parse_version(&state.cursor)?;
        }
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
        return Err(Error::new("SQL Server batch positions are invalid"));
    }
    let path = batch_path(dir, batch);
    if std::fs::metadata(&path)?.len() > max_bytes as u64 {
        return Err(Error::new("SQL Server batch exceeds configured bound"));
    }
    let text = std::fs::read_to_string(path)?;
    if !text.ends_with('\n') {
        return Err(Error::new("SQL Server batch is incomplete"));
    }
    let mut rows = VecDeque::new();
    let mut expected = batch.first;
    for line in text.lines() {
        let (record, id) = crate::delivery::decode(line)?;
        if record.position != expected
            || id != record.idempotency_key("mssql", "spool")
            || record
                .source
                .as_ref()
                .and_then(|s| s.cursor.as_ref())
                .is_none()
        {
            return Err(Error::new("SQL Server batch record is invalid"));
        }
        expected = expected
            .checked_add(1)
            .ok_or_else(|| Error::new("SQL Server position overflow"))?;
        rows.push_back(record);
    }
    if expected != batch.last.saturating_add(1) {
        return Err(Error::new("SQL Server batch is incomplete"));
    }
    Ok(rows)
}

pub(super) fn parse_lsn(text: &str) -> Result<[u8; 10], Error> {
    if text.len() != 20 || !text.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(Error::new("SQL Server LSN cursor is unreadable"));
    }
    let mut out = [0u8; 10];
    for (i, slot) in out.iter_mut().enumerate() {
        *slot = u8::from_str_radix(&text[i * 2..i * 2 + 2], 16)
            .map_err(|_| Error::new("SQL Server LSN cursor is unreadable"))?;
    }
    Ok(out)
}
pub(super) fn format_lsn(bytes: &[u8]) -> Result<String, Error> {
    if bytes.len() != 10 {
        return Err(Error::new("SQL Server LSN must be 10 bytes"));
    }
    Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
}
pub(super) fn parse_version(text: &str) -> Result<i64, Error> {
    text.parse()
        .map_err(|_| Error::new("SQL Server change tracking version is unreadable"))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CaptureInstance {
    pub name: String,
    pub start_lsn: [u8; 10],
    pub create_date_ms: i64,
}

pub(super) fn select_instance<'a>(
    instances: &'a [CaptureInstance],
    from: &[u8; 10],
) -> Result<&'a CaptureInstance, Error> {
    if instances.is_empty() {
        return Err(Error::new("SQL Server table has no capture instance"));
    }
    let mut ordered: Vec<_> = instances.iter().collect();
    ordered.sort_by_key(|instance| (instance.create_date_ms, instance.name.as_str()));
    if from < &ordered[0].start_lsn {
        return Err(Error::new(
            "SQL Server CDC retention expired; an explicit new snapshot and consumer recovery are required",
        ));
    }
    let mut chosen = ordered[0];
    for instance in ordered.into_iter().skip(1) {
        if from >= &instance.start_lsn {
            chosen = instance;
        }
    }
    Ok(chosen)
}

/// Exclusive CDC `from_lsn` for one capture instance. Newer instances start at
/// their own min LSN; a range that is empty after that clamp is skipped.
pub(super) fn cdc_query_from(from: [u8; 10], min: [u8; 10], to: [u8; 10]) -> Option<[u8; 10]> {
    let start = if from < min { min } else { from };
    (start < to).then_some(start)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lsn_round_trips_and_rejects_short_values() {
        let bytes = [0x00, 0x00, 0x00, 0x1d, 0x00, 0x00, 0x00, 0x4b, 0x00, 0x01];
        let text = format_lsn(&bytes).unwrap();
        assert_eq!(text, "0000001d0000004b0001");
        assert_eq!(parse_lsn(&text).unwrap(), bytes);
        assert!(parse_lsn("00").is_err());
        assert!(format_lsn(&[0, 1]).is_err());
    }

    #[test]
    fn capture_instance_follows_the_newest_covering_lsn() {
        let first = CaptureInstance {
            name: "dbo_items".into(),
            start_lsn: parse_lsn("00000000000000000001").unwrap(),
            create_date_ms: 1,
        };
        let second = CaptureInstance {
            name: "dbo_items_v2".into(),
            start_lsn: parse_lsn("00000000000000000010").unwrap(),
            create_date_ms: 2,
        };
        let instances = [first.clone(), second.clone()];
        assert_eq!(
            select_instance(&instances, &parse_lsn("00000000000000000005").unwrap())
                .unwrap()
                .name,
            "dbo_items"
        );
        assert_eq!(
            select_instance(&instances, &parse_lsn("00000000000000000010").unwrap())
                .unwrap()
                .name,
            "dbo_items_v2"
        );
        assert!(select_instance(&instances, &parse_lsn("00000000000000000000").unwrap()).is_err());
    }

    #[test]
    fn cdc_query_from_clamps_to_instance_min_and_skips_empty_ranges() {
        let min = parse_lsn("00000000000000000010").unwrap();
        let to = parse_lsn("00000000000000000020").unwrap();
        assert_eq!(
            cdc_query_from(parse_lsn("00000000000000000001").unwrap(), min, to),
            Some(min)
        );
        assert_eq!(
            cdc_query_from(parse_lsn("00000000000000000015").unwrap(), min, to),
            Some(parse_lsn("00000000000000000015").unwrap())
        );
        assert_eq!(
            cdc_query_from(parse_lsn("00000000000000000020").unwrap(), min, to),
            None
        );
        assert_eq!(
            cdc_query_from(parse_lsn("00000000000000000001").unwrap(), to, to),
            None
        );
    }
}
