//! Durable checkpoints owned by the runtime. A connector never writes these.
//!
//! The lock store is pluggable and independent of these files. Writers carry a
//! fencing token so a stale node is refused after takeover.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use fencing_lease::{Fence, HolderId};

use crate::Error;
use crate::contract::{CONTRACT_VERSION, Position};
use crate::lease::{self, DEFAULT_TTL, InstanceLease, SharedLeaseStore, lease_err, next_holder};

static TMP_SEQ: AtomicU64 = AtomicU64::new(1);

/// Checkpoint file encoding. Distinct from [`CONTRACT_VERSION`] (records and quarantine).
pub const CHECKPOINT_VERSION: u32 = 3;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checkpoint {
    pub source_cursor: Option<String>,
    pub contract_version: u32,
    pub connector: String,
    pub instance: String,
    pub position: Position,
    /// Zero on a v1/v2 file that has not yet been rewritten.
    pub fencing_token: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuarantineRow {
    pub source: Option<crate::contract::SourceMetadata>,
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
    /// Dropping the lease does not release it; a standby waits for the TTL.
    fn acquire(&self, connector: &str, instance: &str) -> Result<InstanceLease, Error>;
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
        token: u64,
    ) -> Result<(), Error>;
    fn load(&self, connector: &str, instance: &str) -> Result<Option<Checkpoint>, Error>;
    fn commit(&self, checkpoint: &Checkpoint) -> Result<(), Error>;
    fn quarantine(&self, row: &QuarantineRow, token: u64) -> Result<(), Error>;
    fn quarantined(&self, connector: &str, instance: &str) -> Result<Vec<QuarantineRow>, Error>;
}

/// Directory of one checkpoint file plus an append-only quarantine log.
///
/// Writes fsync the temp file before rename so a kill mid-commit cannot leave
/// a half-written checkpoint as the live file. The exclusive hold is a
/// fencing-lease slot, not a process-lifetime flock, so a paused holder can
/// be taken over after its TTL.
#[derive(Clone)]
pub struct FileCheckpoint {
    dir: PathBuf,
    leases: SharedLeaseStore,
    holder: HolderId,
    ttl: Duration,
}

impl std::fmt::Debug for FileCheckpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileCheckpoint")
            .field("dir", &self.dir)
            .field("holder", &self.holder.as_str())
            .field("ttl", &self.ttl)
            .finish_non_exhaustive()
    }
}

impl FileCheckpoint {
    pub fn open(dir: impl Into<PathBuf>) -> Result<Self, Error> {
        Self::open_with_ttl(dir, DEFAULT_TTL)
    }

    pub fn open_with_ttl(dir: impl Into<PathBuf>, ttl: Duration) -> Result<Self, Error> {
        let dir = dir.into();
        fs::create_dir_all(&dir)?;
        let leases: SharedLeaseStore =
            std::sync::Arc::new(fencing_lease::FileStore::new(&dir).map_err(lease_err)?);
        Self::with_store(dir, leases, ttl)
    }

    pub fn with_store(
        dir: impl Into<PathBuf>,
        leases: SharedLeaseStore,
        ttl: Duration,
    ) -> Result<Self, Error> {
        let dir = dir.into();
        fs::create_dir_all(&dir)?;
        Ok(Self {
            dir,
            leases,
            holder: next_holder()?,
            ttl,
        })
    }

    fn checkpoint_path(&self, connector: &str, instance: &str) -> PathBuf {
        self.dir.join(format!(
            "checkpoint-{}.{}.v1",
            path_token(connector),
            path_token(instance)
        ))
    }

    fn quarantine_path(&self, connector: &str, instance: &str) -> PathBuf {
        self.dir.join(format!(
            "quarantine-{}.{}.v1.log",
            path_token(connector),
            path_token(instance)
        ))
    }

    fn fence_path(&self, connector: &str, instance: &str) -> PathBuf {
        self.dir.join(format!(
            "fence-{}.{}.v1",
            path_token(connector),
            path_token(instance)
        ))
    }
}

impl CheckpointStore for FileCheckpoint {
    fn acquire(&self, connector: &str, instance: &str) -> Result<InstanceLease, Error> {
        lease::acquire(&self.leases, connector, instance, &self.holder, self.ttl)
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
        token: u64,
    ) -> Result<(), Error> {
        let path = self.dir.join(format!(
            "schema-{}.{}.v1.json",
            path_token(connector),
            path_token(instance)
        ));
        let body = serde_json::to_string(state).map_err(|error| Error::new(error.to_string()))?;
        with_admitted(&self.fence_path(connector, instance), token, || {
            atomic_write(&path, &body)
        })
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
        if checkpoint.contract_version != CHECKPOINT_VERSION {
            return Err(Error::new(format!(
                "unsupported checkpoint contract {}",
                checkpoint.contract_version
            )));
        }
        let path = self.checkpoint_path(&checkpoint.connector, &checkpoint.instance);
        with_admitted(
            &self.fence_path(&checkpoint.connector, &checkpoint.instance),
            checkpoint.fencing_token,
            || atomic_write(&path, &encode_checkpoint(checkpoint)),
        )
    }

    fn quarantine(&self, row: &QuarantineRow, token: u64) -> Result<(), Error> {
        let path = self.quarantine_path(&row.connector, &row.instance);
        with_admitted(
            &self.fence_path(&row.connector, &row.instance),
            token,
            || {
                repair_log_tail(&path)?;
                let mut file = OpenOptions::new().create(true).append(true).open(&path)?;
                file.write_all(encode_quarantine(row).as_bytes())?;
                file.sync_all()?;
                drop(file);
                sync_dir(&self.dir)?;
                Ok(())
            },
        )
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
        "{}\n{}\n{}\n{}\n{}\n{}\n",
        checkpoint.contract_version,
        escape_field(&checkpoint.connector),
        escape_field(&checkpoint.instance),
        checkpoint.position,
        serde_json::json!(checkpoint.source_cursor),
        checkpoint.fencing_token
    )
}

fn parse_checkpoint(text: &str) -> Result<Option<Checkpoint>, Error> {
    let mut lines = text.lines();
    let version = lines
        .next()
        .ok_or_else(|| Error::new("checkpoint missing version"))?
        .parse::<u32>()
        .map_err(|error| Error::new(error.to_string()))?;
    if version != 1 && version != 2 && version != CHECKPOINT_VERSION {
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
    let source_cursor = if version == 1 {
        None
    } else {
        serde_json::from_str(
            lines
                .next()
                .ok_or_else(|| Error::new("checkpoint missing source cursor"))?,
        )
        .map_err(|_| Error::new("invalid source cursor"))?
    };
    let fencing_token = if version >= CHECKPOINT_VERSION {
        lines
            .next()
            .ok_or_else(|| Error::new("checkpoint missing fencing token"))?
            .parse::<u64>()
            .map_err(|error| Error::new(error.to_string()))?
    } else {
        0
    };
    if lines.next().is_some() {
        return Err(Error::new("checkpoint has extra fields"));
    }
    Ok(Some(Checkpoint {
        source_cursor,
        contract_version: version,
        connector,
        instance,
        position,
        fencing_token,
    }))
}

/// Exclusive lock on a fence file. Dropping it releases the lock.
pub(crate) struct FenceLock {
    _file: File,
}

/// Lock `path`, refuse an older token, persist the highest, and hold the lock
/// until the guard drops so the protected write cannot race a successor.
pub(crate) fn lock_and_admit(path: &Path, token: u64) -> Result<FenceLock, Error> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)?;
    file.lock()?;
    file.seek(SeekFrom::Start(0))?;
    let mut text = String::new();
    file.read_to_string(&mut text)?;
    let current = {
        let text = text.trim();
        if text.is_empty() {
            fencing_lease::FencingToken::ZERO
        } else {
            let raw = text
                .parse::<u64>()
                .map_err(|_| Error::new("fence token is unreadable"))?;
            lease::fencing_token(raw)
        }
    };
    let mut fence = Fence::new(current);
    fence
        .admit(lease::fencing_token(token))
        .map_err(lease_err)?;
    if current.get() != fence.highest().get() {
        let body = format!("{}\n", fence.highest().get());
        file.seek(SeekFrom::Start(0))?;
        file.write_all(body.as_bytes())?;
        file.set_len(body.len() as u64)?;
        file.sync_all()?;
    }
    Ok(FenceLock { _file: file })
}

/// Persist the highest admitted token at `path`. A presented token older than
/// the stored highest is refused.
pub(crate) fn with_admitted<T>(
    path: &Path,
    token: u64,
    body: impl FnOnce() -> Result<T, Error>,
) -> Result<T, Error> {
    let _lock = lock_and_admit(path, token)?;
    body()
}

fn encode_quarantine(row: &QuarantineRow) -> String {
    let mut line = format!(
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
    );
    line.pop();
    line.push('\t');
    line.push_str(&escape_field(&serde_json::json!(row.source).to_string()));
    line.push('\n');
    line
}

fn parse_quarantine(line: &str) -> Result<QuarantineRow, Error> {
    let parts: Vec<&str> = line.split('\t').collect();
    if !((parts.len() == 11 && parts[0] == "v1") || (parts.len() == 12 && parts[0] == "v2")) {
        return Err(Error::new("quarantine row is unreadable"));
    }
    let quality = unescape_field(parts[10])?;
    Ok(QuarantineRow {
        source: if parts[0] == "v2" {
            serde_json::from_str(&unescape_field(parts[11])?)
                .map_err(|_| Error::new("invalid quarantined source metadata"))?
        } else {
            None
        },
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
                source_cursor: None,
                contract_version: CHECKPOINT_VERSION,
                connector: "fake".into(),
                instance: "line-a".into(),
                position: 9,
                fencing_token: 1,
            })
            .unwrap();
        let loaded = store.load("fake", "line-a").unwrap().unwrap();
        assert_eq!(loaded.position, 9);
    }

    #[test]
    fn quarantine_appends_a_reasoned_row() {
        let store = FileCheckpoint::open(dir()).unwrap();
        store
            .quarantine(
                &QuarantineRow {
                    source: None,
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
                },
                1,
            )
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
                source_cursor: None,
                contract_version: CHECKPOINT_VERSION,
                connector: "a-b".into(),
                instance: "c".into(),
                position: 4,
                fencing_token: 1,
            })
            .unwrap();
        store
            .commit(&Checkpoint {
                source_cursor: None,
                contract_version: CHECKPOINT_VERSION,
                connector: "a".into(),
                instance: "b-c".into(),
                position: 9,
                fencing_token: 1,
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
                source_cursor: None,
                contract_version: CHECKPOINT_VERSION,
                connector: "fake".into(),
                instance: "line-a".into(),
                position: 4,
                fencing_token: 1,
            })
            .unwrap();
        store
            .commit(&Checkpoint {
                source_cursor: None,
                contract_version: CHECKPOINT_VERSION,
                connector: "fake".into(),
                instance: "Line-a".into(),
                position: 9,
                fencing_token: 1,
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
                source_cursor: None,
                contract_version: CHECKPOINT_VERSION,
                connector: String::new(),
                instance: "x".into(),
                position: 4,
                fencing_token: 1,
            })
            .unwrap();
        store
            .commit(&Checkpoint {
                source_cursor: None,
                contract_version: CHECKPOINT_VERSION,
                connector: "_".into(),
                instance: "x".into(),
                position: 9,
                fencing_token: 1,
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
                source_cursor: None,
                contract_version: CHECKPOINT_VERSION,
                connector: "mod\nbus".into(),
                instance: "line\na".into(),
                position: 11,
                fencing_token: 1,
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
            .quarantine(
                &QuarantineRow {
                    source: None,
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
                },
                1,
            )
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
            source: None,
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
        store.quarantine(&complete, 1).unwrap();
        let path = store.quarantine_path("fake", "line-a");
        {
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            file.write_all(b"v1\ttorn").unwrap();
        }
        store
            .quarantine(
                &QuarantineRow {
                    source: None,
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
                },
                1,
            )
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
            .quarantine(
                &QuarantineRow {
                    source: None,
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
                },
                1,
            )
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

    #[test]
    fn v1_and_v2_checkpoints_load_with_a_zero_token_and_rewrite_as_v3() {
        let dir = dir();
        let store = FileCheckpoint::open(&dir).unwrap();
        let path = store.checkpoint_path("fake", "line-a");
        fs::write(&path, "1\nfake\nline-a\n4\n").unwrap();
        let loaded = store.load("fake", "line-a").unwrap().unwrap();
        assert_eq!(loaded.position, 4);
        assert_eq!(loaded.fencing_token, 0);
        assert_eq!(loaded.contract_version, 1);
        fs::write(&path, "2\nfake\nline-a\n5\nnull\n").unwrap();
        let loaded = store.load("fake", "line-a").unwrap().unwrap();
        assert_eq!(loaded.position, 5);
        assert_eq!(loaded.source_cursor, None);
        assert_eq!(loaded.fencing_token, 0);
        store
            .commit(&Checkpoint {
                source_cursor: Some("gtid".into()),
                contract_version: CHECKPOINT_VERSION,
                connector: "fake".into(),
                instance: "line-a".into(),
                position: 6,
                fencing_token: 2,
            })
            .unwrap();
        let text = fs::read_to_string(&path).unwrap();
        assert!(text.starts_with("3\n"), "{text}");
        assert!(text.ends_with("\n2\n") || text.contains("\n2\n"), "{text}");
        let err = parse_checkpoint("3\nfake\nline-a\n1\nnull\n1\nextra\n").unwrap_err();
        assert!(err.message.contains("extra fields"), "{}", err.message);
    }

    #[test]
    fn a_stale_token_cannot_replace_the_checkpoint() {
        let store = FileCheckpoint::open(dir()).unwrap();
        store
            .commit(&Checkpoint {
                source_cursor: None,
                contract_version: CHECKPOINT_VERSION,
                connector: "fake".into(),
                instance: "line-a".into(),
                position: 1,
                fencing_token: 2,
            })
            .unwrap();
        let err = store
            .commit(&Checkpoint {
                source_cursor: None,
                contract_version: CHECKPOINT_VERSION,
                connector: "fake".into(),
                instance: "line-a".into(),
                position: 2,
                fencing_token: 1,
            })
            .unwrap_err();
        assert!(
            err.message.contains("stale writer refused"),
            "{}",
            err.message
        );
        assert_eq!(store.load("fake", "line-a").unwrap().unwrap().position, 1);
    }

    #[test]
    fn a_stale_token_cannot_replace_schema_or_quarantine() {
        let store = FileCheckpoint::open(dir()).unwrap();
        store
            .commit(&Checkpoint {
                source_cursor: None,
                contract_version: CHECKPOINT_VERSION,
                connector: "fake".into(),
                instance: "line-a".into(),
                position: 1,
                fencing_token: 2,
            })
            .unwrap();
        let err = store
            .save_schemas("fake", "line-a", &crate::schema::SchemaState::default(), 1)
            .unwrap_err();
        assert!(
            err.message.contains("stale writer refused"),
            "{}",
            err.message
        );
        let err = store
            .quarantine(
                &QuarantineRow {
                    source: None,
                    connector: "fake".into(),
                    instance: "line-a".into(),
                    kind: "tag".into(),
                    key: "bad".into(),
                    position: 1,
                    reason: "poison".into(),
                    payload: "1".into(),
                    source_ts_ms: 1,
                    ingest_ts_ms: 1,
                    quality: None,
                },
                1,
            )
            .unwrap_err();
        assert!(
            err.message.contains("stale writer refused"),
            "{}",
            err.message
        );
        assert!(store.quarantined("fake", "line-a").unwrap().is_empty());
    }

    #[test]
    fn acquire_contends_until_the_holder_releases() {
        let dir = dir();
        let store = FileCheckpoint::open(&dir).unwrap();
        let lease = store.acquire("fake", "line-a").unwrap();
        let other = FileCheckpoint::open(&dir).unwrap();
        let err = other.acquire("fake", "line-a").unwrap_err();
        assert!(err.message.contains("already running"), "{}", err.message);
        lease.release().unwrap();
        other.acquire("fake", "line-a").unwrap();
    }
}
