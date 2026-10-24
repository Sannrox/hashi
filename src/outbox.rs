//! Outbox sink and one-shot run mode for sandboxed hosts (#63).
//!
//! A host that runs connectors with no network route to the governance plane
//! gives the process a read-only file with the last committed cursor
//! (`CONNECTOR_CHECKPOINT`, empty when never committed), a credential file
//! (`CONNECTOR_CREDENTIAL`), a configuration file (`CONNECTOR_CONFIG`) and a
//! writable outbox directory (`CONNECTOR_OUTBOX`). One run delivers from the
//! committed cursor, writes pages to the outbox and exits; the host applies
//! them afterwards.
//!
//! Each page is one `sekai.source-batch/v1` document, `000001.json`,
//! `000002.json`, …, each followed by an empty `000001.ok` once it is fully
//! written and synced, so a run killed mid-page leaves at most one unmarked
//! page. The first page's `current_cursor` is the committed cursor; each later
//! page's is the previous page's `proposed_next_cursor`. Pages carry no
//! delivery metadata.
//!
//! The connector keeps no checkpoint of its own across runs: the host's
//! committed cursor is authoritative, and a page it refused is emitted again,
//! from the same cursor and with the same idempotency key, on the next run.
//! A cursor is the last stream boundary (a transaction end, or the end of the
//! initial snapshot) plus how many records past it were already paged; a run
//! replays the stream from the boundary and skips those, so its records and
//! positions repeat exactly.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::Error;
use crate::contract::{Operation, Record};

pub const ENV_CHECKPOINT: &str = "CONNECTOR_CHECKPOINT";
pub const ENV_CREDENTIAL: &str = "CONNECTOR_CREDENTIAL";
pub const ENV_CONFIG: &str = "CONNECTOR_CONFIG";
pub const ENV_OUTBOX: &str = "CONNECTOR_OUTBOX";

pub const DEFAULT_MAX_PAGES: usize = 64;
pub const DEFAULT_MAX_PAGE_BYTES: usize = 8 * 1024 * 1024;
/// The contract's record cap per batch.
pub const MAX_PAGE_RECORDS: usize = 500;

const SOURCE_BATCH_VERSION: &str = "sekai.source-batch/v1";
const FAMILY: &str = "source_control.object_sync";
const ADAPTER_ID: &str = "adapter.registered.object_sync";
const ADAPTER_VERSION: &str = "1.0.0";
/// Transaction and snapshot boundary markers; they move the cursor and are
/// never paged.
const MARKER_KIND: &str = "__mysql_transaction";
const MAX_PROPERTIES: usize = 64;
const MAX_PROPERTY_VALUE_BYTES: usize = 4 * 1024;
const MAX_DISPLAY_NAME_BYTES: usize = 1024;
const MAX_IDENTIFIER_BYTES: usize = 512;

/// What the pages are filed under; from `CONNECTOR_CONFIG`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Envelope {
    pub namespace: String,
    pub producer_identity: String,
    pub source: String,
    pub source_instance: String,
    pub type_digest: String,
}

/// The caps the host advertises.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Caps {
    #[serde(default = "default_max_pages")]
    pub max_pages: usize,
    #[serde(default = "default_max_page_bytes")]
    pub max_page_bytes: usize,
    #[serde(default = "default_max_page_records")]
    pub max_page_records: usize,
}

fn default_max_pages() -> usize {
    DEFAULT_MAX_PAGES
}
fn default_max_page_bytes() -> usize {
    DEFAULT_MAX_PAGE_BYTES
}
fn default_max_page_records() -> usize {
    MAX_PAGE_RECORDS
}

impl Default for Caps {
    fn default() -> Self {
        Self {
            max_pages: DEFAULT_MAX_PAGES,
            max_page_bytes: DEFAULT_MAX_PAGE_BYTES,
            max_page_records: MAX_PAGE_RECORDS,
        }
    }
}

/// A committed cursor: the last stream boundary and the records already
/// paged past it. Opaque to the host.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cursor {
    pub v: u32,
    /// The boundary's position; 0 before the initial snapshot completed.
    pub position: u64,
    /// The boundary's GTID set; empty before the initial snapshot completed.
    pub gtids: String,
    /// Positions past the boundary already paged.
    pub skip: u64,
    /// The capture configuration the positions were assigned under; a run
    /// under another refuses the cursor.
    pub capture: String,
}

impl Cursor {
    fn encode(&self) -> String {
        serde_json::to_string(self).expect("a cursor serializes")
    }

    /// The committed cursor; `None` when never committed.
    pub fn decode(text: &str) -> Result<Option<Self>, Error> {
        let text = text.trim();
        if text.is_empty() {
            return Ok(None);
        }
        let cursor: Self = serde_json::from_str(text)
            .map_err(|_| Error::new("committed cursor is not an outbox cursor"))?;
        if cursor.v != 1 {
            return Err(Error::new("committed cursor version is not supported"));
        }
        if cursor.gtids.is_empty() && (cursor.position != 0 || cursor.skip != 0) {
            // Part of the initial snapshot was applied and the rest refused:
            // a new snapshot cannot continue it without losing what changed
            // since, so the binding must start over.
            return Err(Error::new(
                "committed cursor is inside the initial snapshot; reset the binding's cursor to start over",
            ));
        }
        Ok(Some(cursor))
    }
}

/// One `sekai.source-batch/v1` record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PageRecord {
    pub source: String,
    pub source_instance: String,
    pub external_id: String,
    pub source_version: String,
    pub type_name: String,
    pub display_name: String,
    pub payload_digest: String,
    pub properties: BTreeMap<String, String>,
    pub deleted: bool,
    pub observed_at_ms: i64,
}

/// One `sekai.source-batch/v1` document, fields in contract order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Page {
    pub contract_version: String,
    pub namespace: String,
    pub producer_identity: String,
    pub source: String,
    pub source_instance: String,
    pub family: String,
    pub adapter_id: String,
    pub adapter_version: String,
    pub type_digest: String,
    pub current_cursor: String,
    pub proposed_next_cursor: String,
    pub idempotency_key: String,
    pub batch_digest: String,
    pub collected_at_ms: i64,
    pub records: Vec<PageRecord>,
}

/// The digest input, as the plane canonicalizes a v1 batch: everything but
/// the digest itself and the collection time.
#[derive(Serialize)]
struct Canonical<'a> {
    contract_version: &'a str,
    namespace: &'a str,
    producer_identity: &'a str,
    source: &'a str,
    source_instance: &'a str,
    family: &'a str,
    adapter_id: &'a str,
    adapter_version: &'a str,
    type_digest: &'a str,
    current_cursor: &'a str,
    proposed_next_cursor: &'a str,
    idempotency_key: &'a str,
    records: &'a [PageRecord],
}

fn sha256(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

impl Page {
    fn seal(
        envelope: &Envelope,
        current_cursor: String,
        proposed_next_cursor: String,
        records: Vec<PageRecord>,
        collected_at_ms: i64,
    ) -> Self {
        // The key follows from where the page starts and ends and what it
        // holds: a rerun from the same committed cursor names the same pages
        // the same, and a page that differs since (a last page that reaches
        // further, a snapshot taken again) gets a new name rather than a
        // conflicting one.
        let contents = sha256(&serde_json::to_vec(&records).expect("records serialize"));
        let idempotency_key = format!(
            "hashi-outbox-{}",
            &sha256(
                format!(
                    "{}\n{}\n{current_cursor}\n{proposed_next_cursor}\n{contents}",
                    envelope.source, envelope.source_instance
                )
                .as_bytes()
            )[7..39]
        );
        let mut page = Self {
            contract_version: SOURCE_BATCH_VERSION.into(),
            namespace: envelope.namespace.clone(),
            producer_identity: envelope.producer_identity.clone(),
            source: envelope.source.clone(),
            source_instance: envelope.source_instance.clone(),
            family: FAMILY.into(),
            adapter_id: ADAPTER_ID.into(),
            adapter_version: ADAPTER_VERSION.into(),
            type_digest: envelope.type_digest.clone(),
            current_cursor,
            proposed_next_cursor,
            idempotency_key,
            batch_digest: String::new(),
            collected_at_ms,
            records,
        };
        page.batch_digest = page.canonical_digest();
        page
    }

    pub fn canonical_digest(&self) -> String {
        let canonical = Canonical {
            contract_version: &self.contract_version,
            namespace: &self.namespace,
            producer_identity: &self.producer_identity,
            source: &self.source,
            source_instance: &self.source_instance,
            family: &self.family,
            adapter_id: &self.adapter_id,
            adapter_version: &self.adapter_version,
            type_digest: &self.type_digest,
            current_cursor: &self.current_cursor,
            proposed_next_cursor: &self.proposed_next_cursor,
            idempotency_key: &self.idempotency_key,
            records: &self.records,
        };
        sha256(&serde_json::to_vec(&canonical).expect("a page serializes"))
    }
}

/// A registered token: no `:`, `#` or `/`, so those (and `%`) are
/// percent-encoded.
fn token(value: &str) -> String {
    let mut out = String::new();
    for character in value.chars() {
        match character {
            ':' | '#' | '/' | '%' => out.push_str(&format!("%{:02X}", character as u32)),
            other => out.push(other),
        }
    }
    out
}

/// A property key as the contract normalizes it: lowercase ASCII letters,
/// digits, `_`, `-` and `.`; anything else becomes `_`.
fn property_key(name: &str) -> String {
    name.chars()
        .map(|character| match character.to_ascii_lowercase() {
            lower @ ('a'..='z' | '0'..='9' | '_' | '-' | '.') => lower,
            _ => '_',
        })
        .collect()
}

fn page_record(envelope: &Envelope, record: &Record, now_ms: i64) -> Result<PageRecord, Error> {
    let external_id = token(&record.key);
    let type_name = token(&record.kind);
    if external_id.is_empty()
        || external_id.len() > MAX_IDENTIFIER_BYTES
        || type_name.is_empty()
        || type_name.len() > MAX_IDENTIFIER_BYTES
        || matches!(type_name.as_str(), "Issue" | "PullRequest")
    {
        return Err(Error::new(format!(
            "record at position {} has no pageable identity",
            record.position
        )));
    }
    let mut properties = BTreeMap::new();
    if let Ok(serde_json::Value::Object(fields)) = serde_json::from_str(&record.payload) {
        for (name, value) in fields {
            let key = property_key(&name);
            let value = match value {
                serde_json::Value::String(text) => text,
                serde_json::Value::Null => String::new(),
                other => other.to_string(),
            };
            if key.is_empty() || value.len() > MAX_PROPERTY_VALUE_BYTES {
                return Err(Error::new(format!(
                    "record at position {} has a property the contract cannot carry",
                    record.position
                )));
            }
            if properties.insert(key, value).is_some() {
                return Err(Error::new(format!(
                    "record at position {} has two properties that normalize alike",
                    record.position
                )));
            }
        }
    }
    if properties.len() > MAX_PROPERTIES {
        return Err(Error::new(format!(
            "record at position {} has more than {MAX_PROPERTIES} properties",
            record.position
        )));
    }
    let mut display_name = external_id.clone();
    while display_name.len() > MAX_DISPLAY_NAME_BYTES {
        display_name.pop();
    }
    let deleted = record
        .source
        .as_ref()
        .and_then(|source| source.change.as_ref())
        .is_some_and(|change| change.operation == Operation::Delete);
    Ok(PageRecord {
        source: envelope.source.clone(),
        source_instance: envelope.source_instance.clone(),
        external_id,
        source_version: record.position.to_string(),
        type_name,
        display_name,
        payload_digest: sha256(record.payload.as_bytes()),
        properties,
        deleted,
        observed_at_ms: if record.source_ts_ms > 0 {
            record.source_ts_ms
        } else {
            now_ms
        },
    })
}

/// The rows `record` pages as: itself, preceded by the deletion of its old
/// identity when an update moved its key (`keys` names each kind's key
/// fields, in key order).
fn page_rows(
    envelope: &Envelope,
    keys: &BTreeMap<String, Vec<String>>,
    record: &Record,
    now_ms: i64,
) -> Result<Vec<PageRecord>, Error> {
    let row = page_record(envelope, record, now_ms)?;
    let change = record
        .source
        .as_ref()
        .and_then(|source| source.change.as_ref());
    let Some(before) = change
        .filter(|change| change.operation == Operation::Update)
        .and_then(|change| change.before_image.as_deref())
    else {
        return Ok(vec![row]);
    };
    let fields = keys
        .get(&record.kind)
        .filter(|fields| !fields.is_empty())
        .ok_or_else(|| {
            Error::new(format!(
                "record at position {} has no known key fields",
                record.position
            ))
        })?;
    let before: serde_json::Value = serde_json::from_str(before).map_err(|_| {
        Error::new(format!(
            "record at position {} has an unreadable before image",
            record.position
        ))
    })?;
    let parts: Vec<&serde_json::Value> =
        fields.iter().map(|field| &before[field.as_str()]).collect();
    let old_key = serde_json::to_string(&parts).map_err(|error| Error::new(error.to_string()))?;
    if old_key == record.key {
        return Ok(vec![row]);
    }
    let gone = Record {
        key: old_key,
        payload: "null".into(),
        ..record.clone()
    };
    let deleted = PageRecord {
        deleted: true,
        payload_digest: sha256(before.to_string().as_bytes()),
        ..page_record(envelope, &gone, now_ms)?
    };
    Ok(vec![deleted, row])
}

/// What pages are written under: their envelope, each kind's key fields, and
/// the capture fingerprint every cursor carries.
pub struct Scope<'a> {
    pub envelope: &'a Envelope,
    pub keys: &'a BTreeMap<String, Vec<String>>,
    pub capture: &'a str,
}

/// Turns the records a source yields into pages. `next` returns the next
/// record after the previous one, or `None` when caught up. `snapshot_end`
/// is the last position of an initial snapshot this run took, if any.
pub fn paginate(
    scope: &Scope<'_>,
    caps: Caps,
    committed: Option<&Cursor>,
    snapshot_end: Option<u64>,
    now_ms: i64,
    mut next: impl FnMut() -> Result<Option<Record>, Error>,
) -> Result<Vec<Page>, Error> {
    let Scope {
        envelope,
        keys,
        capture,
    } = *scope;
    if caps.max_pages == 0 || caps.max_page_bytes == 0 || caps.max_page_records == 0 {
        return Err(Error::new("outbox caps must be positive"));
    }
    let max_records = caps.max_page_records.min(MAX_PAGE_RECORDS);
    let start = committed.cloned().unwrap_or(Cursor {
        v: 1,
        position: 0,
        gtids: String::new(),
        skip: 0,
        capture: capture.to_owned(),
    });
    let committed_text = committed.map(Cursor::encode).unwrap_or_default();
    let skip_through = start.position + start.skip;
    // The last boundary seen and the cursor after the last record handled.
    let (mut boundary, mut boundary_gtids) = (start.position, start.gtids.clone());
    let mut cursor = start.clone();
    let mut pages = Vec::new();
    let mut open: Vec<PageRecord> = Vec::new();
    let (mut open_bytes, mut open_ids) = (0_usize, BTreeSet::new());
    // The GTID-set length the open page's size check assumed.
    let mut reserved = 0_usize;
    let mut page_start = committed_text.clone();
    while let Some(record) = next()? {
        if record.position <= skip_through {
            continue;
        }
        let record_cursor = record
            .source
            .as_ref()
            .and_then(|source| source.cursor.clone());
        let at_boundary = record.kind == MARKER_KIND
            && record
                .source
                .as_ref()
                .and_then(|source| source.change.as_ref())
                .is_some_and(|change| change.operation == Operation::TransactionEnd)
            || snapshot_end == Some(record.position);
        if record.kind != MARKER_KIND {
            // One record pages as one row, or two for an update that moved
            // its key: the old identity's deletion, then the new row. They
            // stay together on one page.
            let rows = page_rows(envelope, keys, &record, now_ms)?;
            let bytes: usize = rows
                .iter()
                .map(|row| serde_json::to_vec(row).map_or(0, |bytes| bytes.len()) + 1)
                .sum();
            // The page as written: its envelope with the longest cursor it
            // could propose, and its records.
            let longest = record_cursor
                .as_ref()
                .map_or(0, String::len)
                .max(boundary_gtids.len());
            // Too large even for a page of its own: a refusal, whatever the
            // run's page limit, never an empty success.
            if page_overhead(envelope, &cursor.encode(), longest, capture, now_ms) + bytes
                > caps.max_page_bytes
                || rows.len() > max_records
            {
                return Err(Error::new(format!(
                    "record at position {} exceeds the page size cap",
                    record.position
                )));
            }
            let overhead = page_overhead(envelope, &page_start, longest, capture, now_ms);
            let fits = open.len() + rows.len() <= max_records
                && overhead + open_bytes + bytes <= caps.max_page_bytes
                && open
                    .first()
                    .is_none_or(|first| first.type_name == rows[0].type_name)
                && rows.iter().all(|row| !open_ids.contains(&row.external_id));
            if !fits {
                // The run's last page is full: stop, and leave the record for
                // the next run.
                if pages.len() + 1 >= caps.max_pages {
                    break;
                }
                let proposed = cursor.encode();
                pages.push(Page::seal(
                    envelope,
                    std::mem::replace(&mut page_start, proposed.clone()),
                    proposed,
                    std::mem::take(&mut open),
                    now_ms,
                ));
                open_bytes = 0;
                open_ids.clear();
                reserved = 0;
            }
            reserved = reserved.max(longest);
            open_bytes += bytes;
            for row in rows {
                open_ids.insert(row.external_id.clone());
                open.push(row);
            }
        }
        if at_boundary {
            let gtids = record_cursor.unwrap_or_default();
            // A boundary whose GTID set outgrows what the open page reserved
            // for its proposed cursor ends that page first, so it stays
            // within the byte cap.
            if record.kind == MARKER_KIND
                && !open.is_empty()
                && gtids.len() > reserved
                && page_overhead(envelope, &page_start, gtids.len(), capture, now_ms) + open_bytes
                    > caps.max_page_bytes
            {
                if pages.len() + 1 >= caps.max_pages {
                    break;
                }
                let proposed = cursor.encode();
                pages.push(Page::seal(
                    envelope,
                    std::mem::replace(&mut page_start, proposed.clone()),
                    proposed,
                    std::mem::take(&mut open),
                    now_ms,
                ));
                open_bytes = 0;
                open_ids.clear();
                reserved = 0;
            }
            reserved = reserved.max(gtids.len());
            boundary = record.position;
            boundary_gtids = gtids;
        }
        cursor = Cursor {
            v: 1,
            position: boundary,
            gtids: boundary_gtids.clone(),
            skip: record.position - boundary,
            capture: capture.to_owned(),
        };
    }
    let snapshot_open = snapshot_end.is_some_and(|end| end > 0 && boundary < end);
    if snapshot_open {
        return Err(Error::new(
            "the initial snapshot does not fit one run's outbox caps; raise them",
        ));
    }
    if !open.is_empty() {
        pages.push(Page::seal(
            envelope,
            page_start,
            cursor.encode(),
            open,
            now_ms,
        ));
    }
    Ok(pages)
}

/// The bytes of a page with no records that starts at `current_cursor` and
/// proposes a cursor whose GTID set is at most `gtids_bytes` long.
fn page_overhead(
    envelope: &Envelope,
    current_cursor: &str,
    gtids_bytes: usize,
    capture: &str,
    now_ms: i64,
) -> usize {
    let widest = Cursor {
        v: 1,
        position: u64::MAX,
        gtids: "x".repeat(gtids_bytes),
        skip: u64::MAX,
        capture: capture.to_owned(),
    }
    .encode();
    serde_json::to_vec(&Page::seal(
        envelope,
        current_cursor.to_owned(),
        widest,
        Vec::new(),
        now_ms,
    ))
    .map_or(usize::MAX, |bytes| bytes.len())
}

/// Writes `pages` as `000001.json` with an `000001.ok` marker once synced,
/// in order. The outbox must hold no earlier pages.
pub fn write_pages(outbox: &Path, pages: &[Page]) -> Result<(), Error> {
    let existing = std::fs::read_dir(outbox)?
        .filter_map(Result::ok)
        .any(|entry| {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            name.ends_with(".json") || name.ends_with(".ok")
        });
    if existing {
        return Err(Error::new("the outbox already holds pages"));
    }
    for (index, page) in pages.iter().enumerate() {
        let stem = format!("{:06}", index + 1);
        let mut file = File::create_new(outbox.join(format!("{stem}.json")))?;
        file.write_all(&serde_json::to_vec(page).map_err(|error| Error::new(error.to_string()))?)?;
        file.sync_all()?;
        // The page's name is durable before its marker is.
        File::open(outbox)?.sync_all()?;
        File::create_new(outbox.join(format!("{stem}.ok")))?.sync_all()?;
        File::open(outbox)?.sync_all()?;
    }
    Ok(())
}

/// What `CONNECTOR_CONFIG` holds for a MySQL source.
#[cfg(feature = "mysql")]
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MySqlOutboxConfig {
    pub envelope: Envelope,
    #[serde(default)]
    pub caps: Caps,
    /// `mysql://user@host:port`; the password comes from the credential file.
    pub url: String,
    pub tables: Vec<OutboxTable>,
    pub server_id: u32,
    #[serde(default = "default_schema_version")]
    pub schema_version: u64,
    #[serde(default)]
    pub allow_insecure_lab: bool,
}

#[cfg(feature = "mysql")]
fn default_schema_version() -> u64 {
    1
}

#[cfg(feature = "mysql")]
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutboxTable {
    pub database: String,
    pub table: String,
    #[serde(default)]
    pub key: Vec<String>,
}

/// The files a host hands a one-shot run.
#[derive(Debug, Clone)]
pub struct HostFiles {
    pub checkpoint: PathBuf,
    pub credential: PathBuf,
    pub config: PathBuf,
    pub outbox: PathBuf,
}

impl HostFiles {
    pub fn from_env() -> Result<Self, Error> {
        let var = |name: &str| {
            std::env::var_os(name)
                .map(PathBuf::from)
                .ok_or_else(|| Error::new(format!("{name} is not set")))
        };
        Ok(Self {
            checkpoint: var(ENV_CHECKPOINT)?,
            credential: var(ENV_CREDENTIAL)?,
            config: var(ENV_CONFIG)?,
            outbox: var(ENV_OUTBOX)?,
        })
    }
}

#[cfg(feature = "mysql")]
const SPOOL_PREFIX: &str = "hashi-outbox-spool-";
#[cfg(feature = "mysql")]
const SPOOL_LOCK: &str = "outbox.lock";

/// A spool for this run only, readable by its owner alone (it holds source
/// rows). Spools that exited runs left behind are removed first.
#[cfg(feature = "mysql")]
fn private_spool() -> Result<(PathBuf, File), Error> {
    let temp = std::env::temp_dir();
    // A published spool always holds its run's lock (an OS file lock, taken
    // before the spool got its name); a lock that can be taken means its run
    // has exited, and it stays held through the removal. A published spool
    // without a lock file is left alone unless it is an hour old.
    if let Ok(entries) = std::fs::read_dir(&temp) {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            let old = || {
                entry
                    .metadata()
                    .and_then(|metadata| metadata.modified())
                    .is_ok_and(|modified| {
                        modified
                            .elapsed()
                            .is_ok_and(|age| age > std::time::Duration::from_secs(3600))
                    })
            };
            // A run killed before publishing leaves an unnamed spool.
            if name.starts_with(&format!(".{SPOOL_PREFIX}")) && old() {
                let _ = std::fs::remove_dir_all(entry.path());
                continue;
            }
            if !name.starts_with(SPOOL_PREFIX) {
                continue;
            }
            match File::options()
                .read(true)
                .write(true)
                .open(entry.path().join(SPOOL_LOCK))
            {
                Ok(lock) => {
                    if lock.try_lock().is_ok() {
                        let _ = std::fs::remove_dir_all(entry.path());
                    }
                    drop(lock);
                }
                Err(_) => {
                    if old() {
                        let _ = std::fs::remove_dir_all(entry.path());
                    }
                }
            }
        }
    }
    let stem = format!(
        "{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_nanos())
    );
    // Built under a name no reclaimer looks at, locked, then published.
    let hidden = temp.join(format!(".{SPOOL_PREFIX}{stem}"));
    let mut builder = std::fs::DirBuilder::new();
    #[cfg(unix)]
    std::os::unix::fs::DirBuilderExt::mode(&mut builder, 0o700);
    builder.create(&hidden)?;
    let lock = File::create_new(hidden.join(SPOOL_LOCK))?;
    lock.try_lock()
        .map_err(|_| Error::new("outbox spool lock is unavailable"))?;
    let path = temp.join(format!("{SPOOL_PREFIX}{stem}"));
    std::fs::rename(&hidden, &path)?;
    Ok((path, lock))
}

/// One run: MySQL change capture from the committed cursor into the outbox.
/// Returns how many pages it wrote.
#[cfg(feature = "mysql")]
pub fn run_mysql_once(files: &HostFiles) -> Result<usize, Error> {
    use crate::Connector;
    use crate::mysql::{Config, MySqlConnector, Table};

    let config: MySqlOutboxConfig = serde_json::from_slice(&std::fs::read(&files.config)?)
        .map_err(|error| Error::new(format!("{ENV_CONFIG}: {error}")))?;
    let committed = Cursor::decode(&std::fs::read_to_string(&files.checkpoint)?)?;
    let password = std::fs::read_to_string(&files.credential)?;
    let options = mysql_async::OptsBuilder::from_opts(
        mysql_async::Opts::from_url(&config.url)
            .map_err(|_| Error::new(format!("{ENV_CONFIG}: url is not a MySQL URL")))?,
    )
    .pass(Some(password.trim_end_matches(['\r', '\n'])));
    // A spool for this run only; the committed cursor is the state.
    // The lock is held, and the spool kept, until the run ends.
    let (state_dir, _lock) = private_spool()?;
    struct Remove(PathBuf);
    impl Drop for Remove {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let _spool = Remove(state_dir.clone());
    let connector_config = Config {
        id: config.envelope.source_instance.clone(),
        options: options.into(),
        tables: config
            .tables
            .iter()
            .map(|table| Table {
                database: table.database.clone(),
                table: table.table.clone(),
                key: table.key.clone(),
            })
            .collect(),
        state_dir,
        chunk_rows: 1000,
        // The snapshot is staged whole: allow what the run may write.
        max_batch_bytes: config
            .caps
            .max_pages
            .saturating_mul(config.caps.max_page_bytes)
            .max(16 * 1024 * 1024),
        server_id: config.server_id,
        schema_version: config.schema_version,
        timeout: std::time::Duration::from_secs(20),
        allow_insecure_lab: config.allow_insecure_lab,
    };
    let (mut connector, snapshot_end) = match &committed {
        None => {
            let connector = MySqlConnector::connect(connector_config)?;
            let end = connector.opened_at();
            (connector, Some(end))
        }
        Some(cursor) => (
            MySqlConnector::resume(connector_config, cursor.position, &cursor.gtids)?,
            None,
        ),
    };
    // Read up to what the server had executed when the run opened; a read
    // that finds nothing buffered yet is not caught up.
    let target = connector.executed_gtids()?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    let mut done = false;
    let mut last: Option<(u64, Option<String>)> = committed
        .as_ref()
        .map(|cursor| (cursor.position, Some(cursor.gtids.clone())));
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(1, |elapsed| elapsed.as_millis() as i64);
    let keys: BTreeMap<String, Vec<String>> = connector
        .discover()?
        .into_iter()
        .map(|kind| (kind.kind, kind.schema.key_fields))
        .collect();
    // What the positions and identities in a cursor depend on: the source,
    // the tables captured and the keys they resolve to now, never the
    // credential.
    let capture = sha256(
        serde_json::json!({
            "envelope": [
                &config.envelope.source,
                &config.envelope.source_instance,
                &config.envelope.type_digest,
            ],
            "url": config.url.split('@').next_back().unwrap_or_default(),
            "tables": config
                .tables
                .iter()
                .map(|table| [table.database.clone(), table.table.clone()])
                .collect::<Vec<_>>(),
            "keys": &keys,
            "schema_version": config.schema_version,
        })
        .to_string()
        .as_bytes(),
    );
    if committed
        .as_ref()
        .is_some_and(|cursor| cursor.capture != capture)
    {
        return Err(Error::new(
            "the committed cursor was taken under another capture configuration; reset the binding's cursor to start over",
        ));
    }
    let pages = paginate(
        &Scope {
            envelope: &config.envelope,
            keys: &keys,
            capture: &capture,
        },
        config.caps,
        committed.as_ref(),
        snapshot_end,
        now_ms,
        || loop {
            if done {
                return Ok(None);
            }
            if std::time::Instant::now() > deadline {
                return Err(Error::new("MySQL stream did not catch up within a minute"));
            }
            let (from, cursor) = match &last {
                Some((position, cursor)) => (Some(*position), cursor.clone()),
                None => (None, None),
            };
            if let Some(record) = connector.read(from, cursor.as_deref())? {
                let record_cursor = record
                    .source
                    .as_ref()
                    .and_then(|source| source.cursor.clone());
                // A boundary at or past the target ends the run, however busy
                // the server is since.
                let boundary = record.kind == MARKER_KIND
                    && record
                        .source
                        .as_ref()
                        .and_then(|source| source.change.as_ref())
                        .is_some_and(|change| change.operation == Operation::TransactionEnd)
                    || snapshot_end == Some(record.position);
                if boundary {
                    done =
                        connector.reached(record_cursor.as_deref().unwrap_or_default(), &target)?;
                }
                last = Some((record.position, record_cursor));
                return Ok(Some(record));
            }
            if connector.reached(cursor.as_deref().unwrap_or_default(), &target)? {
                return Ok(None);
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        },
    )?;
    write_pages(&files.outbox, &pages)?;
    Ok(pages.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    const CAPTURE: &str = "sha256:capture";
    use crate::contract::{Change, SourceMetadata};

    fn envelope() -> Envelope {
        Envelope {
            namespace: "orders".into(),
            producer_identity: "connector.hashi".into(),
            source: "mysql".into(),
            source_instance: "shop".into(),
            type_digest: format!("sha256:{}", "a".repeat(64)),
        }
    }

    fn keys() -> BTreeMap<String, Vec<String>> {
        BTreeMap::from([("shop.items".to_owned(), vec!["id".to_owned()])])
    }

    fn row(position: u64, key: &str, operation: Operation, gtids: &str) -> Record {
        Record {
            source: Some(SourceMetadata {
                cursor: Some(gtids.into()),
                change: Some(Change {
                    operation,
                    before_image: None,
                    transaction_id: None,
                    commit_ts_ms: None,
                }),
            }),
            kind: "shop.items".into(),
            key: key.into(),
            source_ts_ms: 1_700_000_000_000,
            ingest_ts_ms: 0,
            quality: None,
            position,
            payload: format!(r#"{{"id":"{key}","Status":"open"}}"#),
        }
    }

    fn end(position: u64, gtids: &str) -> Record {
        Record {
            kind: MARKER_KIND.into(),
            key: format!("t{position}"),
            payload: "{}".into(),
            ..row(position, "x", Operation::TransactionEnd, gtids)
        }
    }

    /// A snapshot of two rows (positions 1-2, GTID `g:1`), then one
    /// transaction with an update and a delete (3-5, `g:2`).
    fn stream() -> Vec<Record> {
        vec![
            row(1, "1", Operation::Insert, "g:1"),
            row(2, "2", Operation::Insert, "g:1"),
            row(3, "1", Operation::Update, "g:2"),
            row(4, "2", Operation::Delete, "g:2"),
            end(5, "g:2"),
        ]
    }

    fn run(
        records: &[Record],
        caps: Caps,
        committed: Option<&Cursor>,
        snapshot: Option<u64>,
    ) -> Vec<Page> {
        let mut iter = records.iter().cloned();
        paginate(
            &Scope {
                envelope: &envelope(),
                keys: &keys(),
                capture: CAPTURE,
            },
            caps,
            committed,
            snapshot,
            1_700_000_000_500,
            || Ok(iter.next()),
        )
        .unwrap()
    }

    #[test]
    fn pages_chain_cursor_to_cursor_and_split_on_a_repeated_identity() {
        let pages = run(&stream(), Caps::default(), None, Some(2));
        // The update repeats row 1: a new page.
        assert_eq!(pages.len(), 2);
        assert_eq!(pages[0].current_cursor, "");
        assert_eq!(pages[1].current_cursor, pages[0].proposed_next_cursor);
        assert_eq!(
            Cursor::decode(&pages[0].proposed_next_cursor)
                .unwrap()
                .unwrap(),
            Cursor {
                v: 1,
                position: 2,
                gtids: "g:1".into(),
                skip: 0,
                capture: CAPTURE.into(),
            }
        );
        assert_eq!(
            Cursor::decode(&pages[1].proposed_next_cursor)
                .unwrap()
                .unwrap(),
            Cursor {
                v: 1,
                position: 5,
                gtids: "g:2".into(),
                skip: 0,
                capture: CAPTURE.into(),
            }
        );
        let ops: Vec<bool> = pages[1]
            .records
            .iter()
            .map(|record| record.deleted)
            .collect();
        assert_eq!(ops, [false, true]);
        assert_eq!(pages[0].records[0].properties["status"], "open");
        for page in &pages {
            assert_eq!(page.batch_digest, page.canonical_digest());
            assert!(page.idempotency_key.starts_with("hashi-outbox-"));
        }
    }

    #[test]
    fn a_rerun_from_the_same_cursor_emits_identical_pages_and_keys() {
        let first = run(&stream(), Caps::default(), None, Some(2));
        let committed = Cursor::decode(&first[0].proposed_next_cursor)
            .unwrap()
            .unwrap();
        // The host refused page 2: the next run replays from the boundary.
        let replay: Vec<Record> = stream()
            .into_iter()
            .filter(|record| record.position > 2)
            .collect();
        let again = run(&replay, Caps::default(), Some(&committed), None);
        assert_eq!(again.len(), 1);
        assert_eq!(again[0].idempotency_key, first[1].idempotency_key);
        assert_eq!(again[0].batch_digest, first[1].batch_digest);
    }

    #[test]
    fn an_update_that_moves_a_key_deletes_the_old_identity() {
        let mut moved = row(3, "[2]", Operation::Update, "g:2");
        moved.payload = r#"{"id":2,"Status":"open"}"#.into();
        if let Some(change) = moved
            .source
            .as_mut()
            .and_then(|source| source.change.as_mut())
        {
            change.before_image = Some(r#"{"id":1,"Status":"open"}"#.into());
        }
        let mut same = row(4, "[3]", Operation::Update, "g:2");
        same.payload = r#"{"id":3,"Status":"paid"}"#.into();
        if let Some(change) = same
            .source
            .as_mut()
            .and_then(|source| source.change.as_mut())
        {
            change.before_image = Some(r#"{"id":3,"Status":"open"}"#.into());
        }
        let committed = Cursor {
            v: 1,
            position: 2,
            gtids: "g:1".into(),
            skip: 0,
            capture: CAPTURE.into(),
        };
        let pages = run(
            &[moved, same, end(5, "g:2")],
            Caps::default(),
            Some(&committed),
            None,
        );
        let rows: Vec<(&str, bool)> = pages[0]
            .records
            .iter()
            .map(|record| (record.external_id.as_str(), record.deleted))
            .collect();
        assert_eq!(rows, [("[1]", true), ("[2]", false), ("[3]", false)]);
    }

    #[test]
    fn a_row_too_large_for_any_page_is_refused_even_at_the_page_limit() {
        let mut big = row(1, "1", Operation::Insert, "g:1");
        big.payload = format!(r#"{{"id":"1","note":"{}"}}"#, "x".repeat(3000));
        let caps = Caps {
            max_pages: 1,
            max_page_bytes: 2048,
            ..Caps::default()
        };
        let mut iter = vec![big].into_iter();
        assert!(
            paginate(
                &Scope {
                    envelope: &envelope(),
                    keys: &keys(),
                    capture: CAPTURE
                },
                caps,
                None,
                Some(1),
                1,
                || Ok(iter.next())
            )
            .is_err()
        );
    }

    #[test]
    fn a_last_page_that_grew_since_gets_a_new_key() {
        let first = run(&stream(), Caps::default(), None, Some(2));
        let committed = Cursor::decode(&first[0].proposed_next_cursor)
            .unwrap()
            .unwrap();
        let mut grown: Vec<Record> = stream()
            .into_iter()
            .filter(|record| record.position > 2)
            .collect();
        grown.push(row(6, "3", Operation::Insert, "g:3"));
        grown.push(end(7, "g:3"));
        let again = run(&grown, Caps::default(), Some(&committed), None);
        assert_eq!(again.len(), 1);
        assert_eq!(again[0].current_cursor, first[1].current_cursor);
        assert_ne!(again[0].idempotency_key, first[1].idempotency_key);
    }

    #[test]
    fn a_page_cut_inside_a_transaction_resumes_past_what_it_paged() {
        let records = vec![
            row(3, "a", Operation::Insert, "g:2"),
            row(4, "b", Operation::Insert, "g:2"),
            row(5, "c", Operation::Insert, "g:2"),
            end(6, "g:2"),
        ];
        let committed = Cursor {
            v: 1,
            position: 2,
            gtids: "g:1".into(),
            skip: 0,
            capture: CAPTURE.into(),
        };
        let caps = Caps {
            max_page_records: 2,
            ..Caps::default()
        };
        let pages = run(&records, caps, Some(&committed), None);
        assert_eq!(pages.len(), 2);
        let middle = Cursor::decode(&pages[0].proposed_next_cursor)
            .unwrap()
            .unwrap();
        assert_eq!(
            middle,
            Cursor {
                v: 1,
                position: 2,
                gtids: "g:1".into(),
                skip: 2,
                capture: CAPTURE.into(),
            }
        );
        // Rerun from the middle: the stream replays from the boundary and the
        // two paged records are skipped.
        let again = run(&records, caps, Some(&middle), None);
        assert_eq!(again.len(), 1);
        assert_eq!(again[0].records[0].external_id, "c");
        assert_eq!(again[0].idempotency_key, pages[1].idempotency_key);
    }

    #[test]
    fn caps_bound_the_run_and_an_unfinished_snapshot_writes_nothing() {
        let records: Vec<Record> = (1..=6)
            .map(|n| row(n, &n.to_string(), Operation::Insert, "g:1"))
            .collect();
        let caps = Caps {
            max_pages: 2,
            max_page_records: 2,
            ..Caps::default()
        };
        let mut iter = records.iter().cloned();
        let snapshot = paginate(
            &Scope {
                envelope: &envelope(),
                keys: &keys(),
                capture: CAPTURE,
            },
            caps,
            None,
            Some(6),
            1,
            || Ok(iter.next()),
        );
        assert!(snapshot.is_err());
        let committed = Cursor {
            v: 1,
            position: 10,
            gtids: "g:0".into(),
            skip: 0,
            capture: CAPTURE.into(),
        };
        let streamed: Vec<Record> = (11..=16)
            .map(|n| row(n, &n.to_string(), Operation::Insert, "g:1"))
            .collect();
        let pages = run(&streamed, caps, Some(&committed), None);
        assert_eq!(pages.len(), 2);
        assert_eq!(
            pages.iter().map(|page| page.records.len()).sum::<usize>(),
            4
        );
    }

    #[test]
    fn a_cursor_inside_the_snapshot_is_refused() {
        let inside = Cursor {
            v: 1,
            position: 0,
            gtids: String::new(),
            skip: 3,
            capture: CAPTURE.into(),
        }
        .encode();
        assert!(Cursor::decode(&inside).is_err());
        assert_eq!(Cursor::decode("  ").unwrap(), None);
        assert!(Cursor::decode("not json").is_err());
    }

    #[test]
    fn identities_become_registered_tokens() {
        assert_eq!(token("a:b#c/d%e"), "a%3Ab%23c%2Fd%25e");
        assert_eq!(property_key("Order Total"), "order_total");
    }

    #[test]
    fn pages_are_written_with_markers_and_never_over_earlier_ones() {
        let dir = std::env::temp_dir().join(format!(
            "hashi-outbox-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let pages = run(&stream(), Caps::default(), None, Some(2));
        write_pages(&dir, &pages).unwrap();
        let mut names: Vec<String> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        assert_eq!(
            names,
            ["000001.json", "000001.ok", "000002.json", "000002.ok"]
        );
        let written: serde_json::Value =
            serde_json::from_slice(&std::fs::read(dir.join("000001.json")).unwrap()).unwrap();
        assert!(written.get("delivery").is_none());
        assert_eq!(written["contract_version"], "sekai.source-batch/v1");
        assert!(write_pages(&dir, &pages).is_err());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
