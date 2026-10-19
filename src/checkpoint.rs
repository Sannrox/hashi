//! Durable checkpoints owned by the runtime. A connector never writes these.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::Error;
use crate::contract::{CONTRACT_VERSION, Position};

static TMP_SEQ: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checkpoint {
    pub contract_version: u32,
    pub connector: String,
    pub instance: String,
    pub position: Position,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuarantineRow {
    pub connector: String,
    pub instance: String,
    pub kind: String,
    pub key: String,
    pub position: Position,
    pub reason: String,
    pub payload: String,
    pub source_ts_ms: i64,
    pub ingest_ts_ms: i64,
    pub quality: Option<String>,
}

pub trait CheckpointStore {
    /// Hold this exclusive lease for the entire run, including sink acknowledgements.
    fn acquire(&self, connector: &str, instance: &str) -> Result<File, Error>;
    fn load_schemas(
        &self,
        connector: &str,
        instance: &str,
    ) -> Result<crate::schema::SchemaState, Error>;
    fn save_schemas(
        &self,
        connector: &str,
        instance: &str,
        state: &crate::schema::SchemaState,
    ) -> Result<(), Error>;
    fn load(&self, connector: &str, instance: &str) -> Result<Option<Checkpoint>, Error>;
    fn commit(&self, checkpoint: &Checkpoint) -> Result<(), Error>;
    fn quarantine(&self, row: &QuarantineRow) -> Result<(), Error>;
    fn quarantined(&self, connector: &str, instance: &str) -> Result<Vec<QuarantineRow>, Error>;
}

/// Directory of one checkpoint file plus an append-only quarantine log.
///
/// Writes fsync the temp file before rename so a kill mid-commit cannot leave
/// a half-written checkpoint as the live file.
#[derive(Debug, Clone)]
pub struct FileCheckpoint {
    dir: PathBuf,
}

impl FileCheckpoint {
    pub fn open(dir: impl Into<PathBuf>) -> Result<Self, Error> {
        let dir = dir.into();
        fs::create_dir_all(&dir)?;
        Ok(Self { dir })
    }

    fn checkpoint_path(&self, connector: &str, instance: &str) -> PathBuf {
        self.dir.join(format!(
            "checkpoint-{}.{}.v{CONTRACT_VERSION}",
            path_token(connector),
            path_token(instance)
        ))
    }

    fn quarantine_path(&self, connector: &str, instance: &str) -> PathBuf {
        self.dir.join(format!(
            "quarantine-{}.{}.v{CONTRACT_VERSION}.log",
            path_token(connector),
            path_token(instance)
        ))
    }
}

impl CheckpointStore for FileCheckpoint {
    fn acquire(&self, connector: &str, instance: &str) -> Result<File, Error> {
        let path = self.dir.join(format!(
            "lease-{}.{}",
            path_token(connector),
            path_token(instance)
        ));
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;
        file.try_lock()
            .map_err(|error| Error::new(format!("connector instance already running: {error}")))?;
        Ok(file)
    }

    fn load_schemas(
        &self,
        connector: &str,
        instance: &str,
    ) -> Result<crate::schema::SchemaState, Error> {
        let path = self.dir.join(format!(
            "schema-{}.{}.v1.json",
            path_token(connector),
            path_token(instance)
        ));
        match fs::read(path) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map_err(|error| Error::new(format!("invalid schema state: {error}"))),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Ok(crate::schema::SchemaState::default())
            }
            Err(error) => Err(error.into()),
        }
    }

    fn save_schemas(
        &self,
        connector: &str,
        instance: &str,
        state: &crate::schema::SchemaState,
    ) -> Result<(), Error> {
        let path = self.dir.join(format!(
            "schema-{}.{}.v1.json",
            path_token(connector),
            path_token(instance)
        ));
        let body = serde_json::to_string(state).map_err(|error| Error::new(error.to_string()))?;
        atomic_write(&path, &body)
    }

    fn load(&self, connector: &str, instance: &str) -> Result<Option<Checkpoint>, Error> {
        let path = self.checkpoint_path(connector, instance);
        if !path.exists() {
            return Ok(None);
        }
        let mut text = String::new();
        File::open(&path)?.read_to_string(&mut text)?;
        let Some(loaded) = parse_checkpoint(&text)? else {
            return Ok(None);
        };
        if loaded.connector != connector || loaded.instance != instance {
            return Err(Error::new("checkpoint identity does not match path"));
        }
        Ok(Some(loaded))
    }

    fn commit(&self, checkpoint: &Checkpoint) -> Result<(), Error> {
        if checkpoint.contract_version != CONTRACT_VERSION {
            return Err(Error::new(format!(
                "unsupported checkpoint contract {}",
                checkpoint.contract_version
            )));
        }
        let path = self.checkpoint_path(&checkpoint.connector, &checkpoint.instance);
        atomic_write(&path, &encode_checkpoint(checkpoint))?;
        Ok(())
    }

    fn quarantine(&self, row: &QuarantineRow) -> Result<(), Error> {
        let path = self.quarantine_path(&row.connector, &row.instance);
        repair_log_tail(&path)?;
        let mut file = OpenOptions::new().create(true).append(true).open(&path)?;
        file.write_all(encode_quarantine(row).as_bytes())?;
        file.sync_all()?;
        drop(file);
        sync_dir(&self.dir)?;
        Ok(())
    }

    fn quarantined(&self, connector: &str, instance: &str) -> Result<Vec<QuarantineRow>, Error> {
        let path = self.quarantine_path(connector, instance);
        if !path.exists() {
            return Ok(Vec::new());
        }
        let mut buf = Vec::new();
        File::open(&path)?.read_to_end(&mut buf)?;
        let complete = match buf.iter().rposition(|&byte| byte == b'\n') {
            Some(index) => &buf[..=index],
            None => return Ok(Vec::new()),
        };
        let text = std::str::from_utf8(complete).map_err(|error| Error::new(error.to_string()))?;
        let mut rows = Vec::new();
        for line in text.lines() {
            if line.trim().is_empty() {
                continue;
            }
            rows.push(parse_quarantine(line)?);
        }
        Ok(rows)
    }
}

fn encode_checkpoint(checkpoint: &Checkpoint) -> String {
    format!(
        "{}\n{}\n{}\n{}\n",
        checkpoint.contract_version,
        escape_field(&checkpoint.connector),
        escape_field(&checkpoint.instance),
        checkpoint.position
    )
}

fn parse_checkpoint(text: &str) -> Result<Option<Checkpoint>, Error> {
    let mut lines = text.lines();
    let version = lines
        .next()
        .ok_or_else(|| Error::new("checkpoint missing version"))?
        .parse::<u32>()
        .map_err(|error| Error::new(error.to_string()))?;
    if version != CONTRACT_VERSION {
        return Err(Error::new(format!(
            "unsupported checkpoint contract {version}"
        )));
    }
    let connector = unescape_field(
        lines
            .next()
            .ok_or_else(|| Error::new("checkpoint missing connector"))?,
    )?;
    let instance = unescape_field(
        lines
            .next()
            .ok_or_else(|| Error::new("checkpoint missing instance"))?,
    )?;
    let position = lines
        .next()
        .ok_or_else(|| Error::new("checkpoint missing position"))?
        .parse::<Position>()
        .map_err(|error| Error::new(error.to_string()))?;
    Ok(Some(Checkpoint {
        contract_version: version,
        connector,
        instance,
        position,
    }))
}

fn encode_quarantine(row: &QuarantineRow) -> String {
    format!(
        "v{CONTRACT_VERSION}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
        escape_field(&row.connector),
        escape_field(&row.instance),
        escape_field(&row.kind),
        escape_field(&row.key),
        row.position,
        escape_field(&row.reason),
        escape_field(&row.payload),
        row.source_ts_ms,
        row.ingest_ts_ms,
        escape_field(row.quality.as_deref().unwrap_or(""))
    )
}

fn parse_quarantine(line: &str) -> Result<QuarantineRow, Error> {
    let parts: Vec<&str> = line.split('\t').collect();
    if parts.len() != 11 || parts[0] != format!("v{CONTRACT_VERSION}") {
        return Err(Error::new("quarantine row is unreadable"));
    }
    let quality = unescape_field(parts[10])?;
    Ok(QuarantineRow {
        connector: unescape_field(parts[1])?,
        instance: unescape_field(parts[2])?,
        kind: unescape_field(parts[3])?,
        key: unescape_field(parts[4])?,
        position: parts[5]
            .parse()
            .map_err(|error: std::num::ParseIntError| Error::new(error.to_string()))?,
        reason: unescape_field(parts[6])?,
        payload: unescape_field(parts[7])?,
        source_ts_ms: parts[8]
            .parse()
            .map_err(|error: std::num::ParseIntError| Error::new(error.to_string()))?,
        ingest_ts_ms: parts[9]
            .parse()
            .map_err(|error: std::num::ParseIntError| Error::new(error.to_string()))?,
        quality: if quality.is_empty() {
            None
        } else {
            Some(quality)
        },
    })
}

pub(crate) fn repair_log_tail(path: &Path) -> Result<(), Error> {
    if !path.exists() {
        return Ok(());
    }
    let mut file = OpenOptions::new().read(true).write(true).open(path)?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)?;
    if buf.is_empty() || buf.last() == Some(&b'\n') {
        return Ok(());
    }
    let keep = buf
        .iter()
        .rposition(|&byte| byte == b'\n')
        .map(|index| index + 1)
        .unwrap_or(0);
    file.set_len(keep as u64)?;
    file.sync_all()?;
    drop(file);
    if let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }
    Ok(())
}

pub(crate) fn path_token(value: &str) -> String {
    let mut out = String::with_capacity(value.len() * 2);
    for byte in value.bytes() {
        out.push_str(&format!("{byte:02X}"));
    }
    out
}

pub(crate) fn escape_field(value: &str) -> String {
    let mut out = String::new();
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            other => out.push(other),
        }
    }
    out
}

pub(crate) fn unescape_field(value: &str) -> Result<String, Error> {
    let mut out = String::new();
    let mut chars = value.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }
        match chars.next() {
            Some('\\') => out.push('\\'),
            Some('t') => out.push('\t'),
            Some('n') => out.push('\n'),
            Some('r') => out.push('\r'),
            _ => return Err(Error::new("quarantine row is unreadable")),
        }
    }
    Ok(out)
}

pub(crate) fn atomic_write(path: &Path, body: &str) -> Result<(), Error> {
    let tmp = path.with_extension(format!(
        "tmp-{}-{}",
        std::process::id(),
        TMP_SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let mut file = File::create(&tmp)?;
    file.write_all(body.as_bytes())?;
    file.sync_all()?;
    drop(file);
    fs::rename(tmp, path)?;
    if let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }
    Ok(())
}

pub(crate) fn sync_dir(dir: &Path) -> Result<(), Error> {
    File::open(dir)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(1);

    fn dir() -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "hashi-ckpt-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn commit_then_load_round_trips_the_position() {
        let store = FileCheckpoint::open(dir()).unwrap();
        store
            .commit(&Checkpoint {
                contract_version: CONTRACT_VERSION,
                connector: "fake".into(),
                instance: "line-a".into(),
                position: 9,
            })
            .unwrap();
        let loaded = store.load("fake", "line-a").unwrap().unwrap();
        assert_eq!(loaded.position, 9);
    }

    #[test]
    fn quarantine_appends_a_reasoned_row() {
        let store = FileCheckpoint::open(dir()).unwrap();
        store
            .quarantine(&QuarantineRow {
                connector: "fake".into(),
                instance: "line-a".into(),
                kind: "tag".into(),
                key: "bad".into(),
                position: 3,
                reason: "poison".into(),
                payload: "1.0".into(),
                source_ts_ms: 30,
                ingest_ts_ms: 31,
                quality: Some("bad".into()),
            })
            .unwrap();
        let rows = store.quarantined("fake", "line-a").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].reason, "poison");
        assert_eq!(rows[0].payload, "1.0");
        assert_eq!(rows[0].quality.as_deref(), Some("bad"));
    }

    #[test]
    fn hyphenated_identities_do_not_share_a_checkpoint_file() {
        let store = FileCheckpoint::open(dir()).unwrap();
        store
            .commit(&Checkpoint {
                contract_version: CONTRACT_VERSION,
                connector: "a-b".into(),
                instance: "c".into(),
                position: 4,
            })
            .unwrap();
        store
            .commit(&Checkpoint {
                contract_version: CONTRACT_VERSION,
                connector: "a".into(),
                instance: "b-c".into(),
                position: 9,
            })
            .unwrap();
        assert_eq!(store.load("a-b", "c").unwrap().unwrap().position, 4);
        assert_eq!(store.load("a", "b-c").unwrap().unwrap().position, 9);
    }

    #[test]
    fn case_differing_identities_do_not_share_a_checkpoint_file() {
        let store = FileCheckpoint::open(dir()).unwrap();
        store
            .commit(&Checkpoint {
                contract_version: CONTRACT_VERSION,
                connector: "fake".into(),
                instance: "line-a".into(),
                position: 4,
            })
            .unwrap();
        store
            .commit(&Checkpoint {
                contract_version: CONTRACT_VERSION,
                connector: "fake".into(),
                instance: "Line-a".into(),
                position: 9,
            })
            .unwrap();
        assert_eq!(store.load("fake", "line-a").unwrap().unwrap().position, 4);
        assert_eq!(store.load("fake", "Line-a").unwrap().unwrap().position, 9);
    }

    #[test]
    fn empty_and_underscore_identities_do_not_share_a_checkpoint_file() {
        let store = FileCheckpoint::open(dir()).unwrap();
        store
            .commit(&Checkpoint {
                contract_version: CONTRACT_VERSION,
                connector: String::new(),
                instance: "x".into(),
                position: 4,
            })
            .unwrap();
        store
            .commit(&Checkpoint {
                contract_version: CONTRACT_VERSION,
                connector: "_".into(),
                instance: "x".into(),
                position: 9,
            })
            .unwrap();
        assert_eq!(store.load("", "x").unwrap().unwrap().position, 4);
        assert_eq!(store.load("_", "x").unwrap().unwrap().position, 9);
    }

    #[test]
    fn checkpoint_round_trips_newlines_in_identities() {
        let store = FileCheckpoint::open(dir()).unwrap();
        store
            .commit(&Checkpoint {
                contract_version: CONTRACT_VERSION,
                connector: "mod\nbus".into(),
                instance: "line\na".into(),
                position: 11,
            })
            .unwrap();
        let loaded = store.load("mod\nbus", "line\na").unwrap().unwrap();
        assert_eq!(loaded.position, 11);
        assert_eq!(loaded.connector, "mod\nbus");
        assert_eq!(loaded.instance, "line\na");
    }

    #[test]
    fn quarantine_round_trips_tabs_and_newlines() {
        let store = FileCheckpoint::open(dir()).unwrap();
        store
            .quarantine(&QuarantineRow {
                connector: "fake".into(),
                instance: "line-a".into(),
                kind: "tag".into(),
                key: "k\t1".into(),
                position: 1,
                reason: "bad\nline".into(),
                payload: "p\tay".into(),
                source_ts_ms: 10,
                ingest_ts_ms: 11,
                quality: None,
            })
            .unwrap();
        let rows = store.quarantined("fake", "line-a").unwrap();
        assert_eq!(rows[0].key, "k\t1");
        assert_eq!(rows[0].reason, "bad\nline");
        assert_eq!(rows[0].payload, "p\tay");
        assert_eq!(rows[0].quality, None);
    }

    #[test]
    fn a_torn_quarantine_tail_is_repaired_before_the_next_append() {
        let dir = dir();
        let store = FileCheckpoint::open(&dir).unwrap();
        let complete = QuarantineRow {
            connector: "fake".into(),
            instance: "line-a".into(),
            kind: "tag".into(),
            key: "ok".into(),
            position: 1,
            reason: "poison".into(),
            payload: "1".into(),
            source_ts_ms: 1,
            ingest_ts_ms: 1,
            quality: None,
        };
        store.quarantine(&complete).unwrap();
        let path = store.quarantine_path("fake", "line-a");
        {
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            file.write_all(b"v1\ttorn").unwrap();
        }
        store
            .quarantine(&QuarantineRow {
                connector: "fake".into(),
                instance: "line-a".into(),
                kind: "tag".into(),
                key: "next".into(),
                position: 2,
                reason: "poison".into(),
                payload: "2".into(),
                source_ts_ms: 2,
                ingest_ts_ms: 2,
                quality: None,
            })
            .unwrap();
        let rows = store.quarantined("fake", "line-a").unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].key, "ok");
        assert_eq!(rows[1].key, "next");
    }

    #[test]
    fn a_torn_utf8_tail_does_not_hide_complete_quarantine_rows() {
        let dir = dir();
        let store = FileCheckpoint::open(&dir).unwrap();
        store
            .quarantine(&QuarantineRow {
                connector: "fake".into(),
                instance: "line-a".into(),
                kind: "tag".into(),
                key: "ok".into(),
                position: 1,
                reason: "poison".into(),
                payload: "ä".into(),
                source_ts_ms: 1,
                ingest_ts_ms: 1,
                quality: None,
            })
            .unwrap();
        let path = store.quarantine_path("fake", "line-a");
        {
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            file.write_all(&[0xC3]).unwrap();
        }
        let rows = store.quarantined("fake", "line-a").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].payload, "ä");
    }
}
