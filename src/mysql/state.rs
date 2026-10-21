use super::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Column {
    pub name: String,
    pub sql_type: String,
    pub collation: Option<u16>,
    pub field: FieldType,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Layout {
    pub database: String,
    pub table: String,
    pub keys: Vec<String>,
    pub primary_key: Vec<String>,
    pub columns: Vec<Column>,
    pub version: u64,
}
impl Layout {
    pub fn kind(&self) -> String {
        format!("{}.{}", self.database, self.table)
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
    pub gtids: String,
    pub layouts: Vec<Layout>,
    pub batch: Option<Batch>,
}
pub(super) fn state_path(dir: &Path) -> PathBuf {
    dir.join("mysql.state.v1.json")
}
pub(super) fn batch_path(dir: &Path, batch: &Batch) -> PathBuf {
    dir.join(format!("mysql.batch-{}.v1", batch.first))
}
pub(super) fn save(dir: &Path, state: &State) -> Result<(), Error> {
    let text =
        serde_json::to_string(state).map_err(|_| Error::new("MySQL state encoding failed"))?;
    crate::checkpoint::atomic_write(&state_path(dir), &text)
}
pub(super) fn load(dir: &Path, fingerprint: &str) -> Result<Option<State>, Error> {
    let bytes = match std::fs::read(state_path(dir)) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let state: State =
        serde_json::from_slice(&bytes).map_err(|_| Error::new("MySQL state is unreadable"))?;
    if state.version != 1 || state.fingerprint != fingerprint || state.layouts.is_empty() {
        return Err(Error::new(
            "MySQL state version or source configuration mismatch",
        ));
    }
    parse_gtids(&state.gtids)?;
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
        return Err(Error::new("MySQL batch positions are invalid"));
    }
    let path = batch_path(dir, batch);
    if std::fs::metadata(&path)?.len() > max_bytes as u64 {
        return Err(Error::new("MySQL batch exceeds configured bound"));
    }
    let text = std::fs::read_to_string(path)?;
    if !text.ends_with('\n') {
        return Err(Error::new("MySQL batch is incomplete"));
    }
    let mut rows = VecDeque::new();
    let mut expected = batch.first;
    for line in text.lines() {
        let (record, id) = crate::delivery::decode(line)?;
        if record.position != expected
            || id != record.idempotency_key("mysql", "spool")
            || record
                .source
                .as_ref()
                .and_then(|s| s.cursor.as_ref())
                .is_none()
        {
            return Err(Error::new("MySQL batch record is invalid"));
        }
        expected = expected
            .checked_add(1)
            .ok_or_else(|| Error::new("MySQL position exhausted"))?;
        rows.push_back(record);
    }
    if rows.back().is_none_or(|r| r.position != batch.last) {
        return Err(Error::new("MySQL batch high-water mismatch"));
    }
    Ok(rows)
}

/// Reject overflowing intervals before calling the protocol parser, which assumes valid GNOs.
pub(super) fn parse_gtids(text: &str) -> Result<Vec<mysql_common::packets::Sid<'static>>, Error> {
    if text.len() > 4096 {
        return Err(Error::new("MySQL GTID set exceeds cursor bounds"));
    }
    if text.trim().is_empty() {
        return Ok(Vec::new());
    }
    text.split(',')
        .map(|entry| {
            for component in entry.trim().split(':').skip(1) {
                if component.as_bytes().first().is_some_and(u8::is_ascii_digit) {
                    for number in component.split('-') {
                        let gno = number
                            .parse::<u64>()
                            .map_err(|_| Error::new("MySQL GTID interval is invalid"))?;
                        if gno == 0 || gno >= i64::MAX as u64 {
                            return Err(Error::new("MySQL GTID interval is out of range"));
                        }
                    }
                }
            }
            entry
                .trim()
                .parse()
                .map_err(|_| Error::new("MySQL GTID set is invalid"))
        })
        .collect()
}
pub(super) fn add_gtid(
    set: &str,
    event: &mysql_async::binlog::events::GtidEvent,
) -> Result<String, Error> {
    use mysql_common::packets::{GnoInterval, Sid};
    let mut sids = parse_gtids(set)?;
    if event.gno() == 0 || event.gno() >= i64::MAX as u64 {
        return Err(Error::new("MySQL GTID sequence is out of range"));
    }
    let mut new = Sid::new(event.sid());
    if let Some(tag) = event.tag() {
        new = new.with_tag(tag.clone());
    }
    let mut intervals = Vec::new();
    if let Some(index) = sids
        .iter()
        .position(|sid| sid.uuid() == new.uuid() && sid.tag() == new.tag())
    {
        intervals.extend(
            sids.remove(index)
                .intervals()
                .iter()
                .map(|i| (i.start(), i.end())),
        );
    }
    intervals.push((event.gno(), event.gno() + 1));
    intervals.sort_unstable();
    let mut merged: Vec<(u64, u64)> = Vec::new();
    for (start, end) in intervals {
        if let Some(last) = merged.last_mut()
            && start <= last.1
        {
            last.1 = last.1.max(end);
        } else {
            merged.push((start, end));
        }
    }
    for (start, end) in merged {
        new = new.with_interval(
            GnoInterval::check_and_new(start, end)
                .map_err(|_| Error::new("MySQL GTID interval is invalid"))?,
        );
    }
    sids.push(new);
    let mut strings: Vec<_> = sids.iter().map(ToString::to_string).collect();
    strings.sort();
    let result = strings.join(",");
    parse_gtids(&result)?;
    Ok(result)
}
