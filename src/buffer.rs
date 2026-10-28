//! Store-and-forward disk buffer. Acks after durable enqueue so the connector
//! keeps reading while the uplink is down, and the checkpoint advances once a
//! record is buffered. Drains in order with each record's original
//! idempotency key.
//!
//! Bounded by size and, optionally, by the age of the oldest buffered record
//! (its `ingest_ts_ms`; a row without one does not age). When full, [`WhenFull::Stop`] refuses the record as a
//! transient error, so the source stops advancing; [`WhenFull::DropOldest`]
//! drops the oldest records instead, logs each one's key and ingest time to
//! `gaps.log` and counts it in `dropped` before the cursor moves past it, so a
//! gap is always reported. A crash in between reports a dropped record twice,
//! never zero times.
//!
//! The buffer is local to the node: plain files under one directory, with no
//! lease or database.

use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use crate::Error;
use crate::checkpoint::{atomic_write, repair_log_tail, sync_dir};
use crate::contract::{IdempotencyKey, Record};
use crate::delivery;
use crate::sink::{DeliverError, Sink};

/// What a full buffer does with the next record.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WhenFull {
    /// Refuse it; the source stops advancing until the buffer drains.
    #[default]
    Stop,
    /// Drop the oldest buffered records, counted and logged as a gap.
    DropOldest,
}

/// The buffer's bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BufferLimits {
    pub max_bytes: u64,
    /// The oldest buffered record may be at most this old; `None` is no age cap.
    pub max_age_ms: Option<i64>,
    pub when_full: WhenFull,
}

impl BufferLimits {
    pub fn bytes(max_bytes: u64) -> Self {
        Self {
            max_bytes,
            max_age_ms: None,
            when_full: WhenFull::Stop,
        }
    }
}

/// Fill level and counters for metrics. `drained` counts this process's
/// deliveries out of the buffer; `dropped` counts every gap record ever
/// logged in this directory.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BufferStats {
    pub bytes: u64,
    pub records: u64,
    pub oldest_ingest_ts_ms: Option<i64>,
    pub drained: u64,
    pub dropped: u64,
}

/// Key and ingest time of each dropped record, newest last.
const GAP_LOG: &str = "gaps.log";
/// The gap log before its last rotation.
const GAP_LOG_PREVIOUS: &str = "gaps.1.log";
const GAP_LOG_MAX_BYTES: u64 = 1024 * 1024;
/// Total records ever dropped in this directory.
const DROPPED_FILE: &str = "dropped";

#[derive(Debug)]
pub struct StoreAndForward<S> {
    dir: PathBuf,
    limits: BufferLimits,
    now_ms: fn() -> i64,
    inner: S,
    drained: u64,
    dropped: u64,
    pub filled: bool,
}

fn system_now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as i64)
}

impl<S> StoreAndForward<S> {
    pub fn open(dir: impl Into<PathBuf>, max_bytes: u64, inner: S) -> Result<Self, Error> {
        Self::with_limits(dir, BufferLimits::bytes(max_bytes), inner)
    }

    pub fn with_limits(
        dir: impl Into<PathBuf>,
        limits: BufferLimits,
        inner: S,
    ) -> Result<Self, Error> {
        Self::with_clock(dir, limits, inner, system_now_ms)
    }

    /// [`Self::with_limits`] with the clock the age cap reads.
    pub fn with_clock(
        dir: impl Into<PathBuf>,
        limits: BufferLimits,
        inner: S,
        now_ms: fn() -> i64,
    ) -> Result<Self, Error> {
        let dir = dir.into();
        fs::create_dir_all(&dir)?;
        let dropped = match fs::read_to_string(dir.join(DROPPED_FILE)) {
            Ok(text) => text
                .trim()
                .parse()
                .map_err(|_| Error::new("store-and-forward dropped count is unreadable"))?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
            Err(error) => return Err(error.into()),
        };
        Ok(Self {
            dir,
            limits,
            now_ms,
            inner,
            drained: 0,
            dropped,
            filled: false,
        })
    }

    pub fn into_inner(self) -> S {
        self.inner
    }

    /// Fill level and counters, read from the buffer files. Streams the log:
    /// it counts rows and decodes only the oldest.
    pub fn stats(&self) -> Result<BufferStats, Error> {
        let (generation, offset) = self.load_cursor()?;
        let oldest = self.next_row(generation, offset)?;
        let path = self.log_path(generation);
        let mut records = 0;
        if path.exists() {
            let mut reader = BufReader::new(File::open(&path)?);
            reader.seek(SeekFrom::Start(offset))?;
            loop {
                let chunk = reader.fill_buf()?;
                if chunk.is_empty() {
                    break;
                }
                records += chunk.iter().filter(|&&byte| byte == b'\n').count() as u64;
                let consumed = chunk.len();
                reader.consume(consumed);
            }
        }
        Ok(BufferStats {
            bytes: self.pending_bytes()?,
            records,
            oldest_ingest_ts_ms: oldest
                .map(|(record, _, _)| record.ingest_ts_ms)
                .filter(|at| *at > 0),
            drained: self.drained,
            dropped: self.dropped,
        })
    }

    /// Whether `incoming` more bytes would break a bound.
    fn over_limit(&self, incoming: u64) -> Result<bool, Error> {
        let used = self.pending_bytes()?;
        if used.saturating_add(incoming) > self.limits.max_bytes {
            return Ok(true);
        }
        let Some(max_age_ms) = self.limits.max_age_ms else {
            return Ok(false);
        };
        let (generation, offset) = self.load_cursor()?;
        // A row with no ingest time (a schema control buffered by an older
        // version) does not age.
        Ok(self
            .next_row(generation, offset)?
            .is_some_and(|(record, _, _)| {
                record.ingest_ts_ms > 0 && (self.now_ms)() - record.ingest_ts_ms > max_age_ms
            }))
    }

    /// Drop the oldest buffered record: log its key and ingest time as a
    /// gap, count it, then move the cursor past it. False when nothing is
    /// buffered, or when the oldest is a schema control, which later records
    /// depend on and is never dropped. The gap log holds no payloads and
    /// rotates at [`GAP_LOG_MAX_BYTES`] into one previous file, so dropping
    /// bounds disk use.
    fn drop_oldest(&mut self) -> Result<bool, Error> {
        let (generation, offset) = self.load_cursor()?;
        let Some((record, idk, len)) = self.next_row(generation, offset)? else {
            return Ok(false);
        };
        if record.kind == crate::schema::CONTROL_KIND {
            return Ok(false);
        }
        let path = self.dir.join(GAP_LOG);
        if fs::metadata(&path).is_ok_and(|meta| meta.len() >= GAP_LOG_MAX_BYTES) {
            fs::rename(&path, self.dir.join(GAP_LOG_PREVIOUS))?;
        }
        repair_log_tail(&path)?;
        let mut file = OpenOptions::new().create(true).append(true).open(&path)?;
        let line = format!(
            "{}\t{}\n",
            crate::checkpoint::escape_field(&idk.encode()),
            record.ingest_ts_ms
        );
        file.write_all(line.as_bytes())?;
        file.sync_all()?;
        drop(file);
        sync_dir(&self.dir)?;
        atomic_write(
            &self.dir.join(DROPPED_FILE),
            &format!("{}\n", self.dropped + 1),
        )?;
        self.dropped += 1;
        self.store_cursor(generation, offset + len)?;
        Ok(true)
    }

    fn log_path(&self, generation: u64) -> PathBuf {
        self.dir.join(format!("pending.{generation}.log"))
    }

    fn cursor_path(&self) -> PathBuf {
        self.dir.join("cursor")
    }

    fn load_cursor(&self) -> Result<(u64, u64), Error> {
        let path = self.cursor_path();
        if !path.exists() {
            return Ok((0, 0));
        }
        let text = fs::read_to_string(&path)?;
        let mut parts = text.split_whitespace();
        let generation = parts
            .next()
            .unwrap_or("0")
            .parse()
            .map_err(|error: std::num::ParseIntError| Error::new(error.to_string()))?;
        let offset = parts
            .next()
            .unwrap_or("0")
            .parse()
            .map_err(|error: std::num::ParseIntError| Error::new(error.to_string()))?;
        Ok((generation, offset))
    }

    fn store_cursor(&self, generation: u64, offset: u64) -> Result<(), Error> {
        atomic_write(&self.cursor_path(), &format!("{generation} {offset}\n"))
    }

    fn pending_bytes(&self) -> Result<u64, Error> {
        let (generation, offset) = self.load_cursor()?;
        let path = self.log_path(generation);
        if !path.exists() {
            return Ok(0);
        }
        let size = fs::metadata(&path)?.len();
        Ok(size.saturating_sub(offset))
    }

    pub fn pending(&self) -> Result<Vec<(Record, IdempotencyKey)>, Error> {
        let (generation, offset) = self.load_cursor()?;
        let path = self.log_path(generation);
        if !path.exists() {
            return Ok(Vec::new());
        }
        let mut buf = Vec::new();
        File::open(&path)?.read_to_end(&mut buf)?;
        let cursor = offset as usize;
        if cursor > buf.len() {
            return Ok(Vec::new());
        }
        let slice = &buf[cursor..];
        let complete = match slice.iter().rposition(|&byte| byte == b'\n') {
            Some(index) => &slice[..=index],
            None => return Ok(Vec::new()),
        };
        let text = std::str::from_utf8(complete).map_err(|error| Error::new(error.to_string()))?;
        let mut rows = Vec::new();
        for line in text.lines() {
            if line.trim().is_empty() {
                continue;
            }
            rows.push(delivery::decode(line)?);
        }
        Ok(rows)
    }

    fn compact(&self, generation: u64, offset: u64) -> Result<(u64, u64), Error> {
        if offset == 0 {
            return Ok((generation, offset));
        }
        let path = self.log_path(generation);
        let rest = if path.exists() {
            let mut buf = Vec::new();
            File::open(&path)?.read_to_end(&mut buf)?;
            if (offset as usize) > buf.len() {
                Vec::new()
            } else {
                buf[offset as usize..].to_vec()
            }
        } else {
            Vec::new()
        };
        let next = generation + 1;
        let new_path = self.log_path(next);
        let mut file = File::create(&new_path)?;
        file.write_all(&rest)?;
        file.sync_all()?;
        drop(file);
        sync_dir(&self.dir)?;
        self.store_cursor(next, 0)?;
        let _ = fs::remove_file(path);
        Ok((next, 0))
    }

    fn next_row(
        &self,
        generation: u64,
        offset: u64,
    ) -> Result<Option<(Record, IdempotencyKey, u64)>, Error> {
        let path = self.log_path(generation);
        if !path.exists() {
            return Ok(None);
        }
        let mut reader = BufReader::new(File::open(&path)?);
        reader.seek(SeekFrom::Start(offset))?;
        let mut line = String::new();
        let n = reader.read_line(&mut line)?;
        if n == 0 || !line.ends_with('\n') {
            return Ok(None);
        }
        let decoded = delivery::decode(line.strip_suffix('\n').unwrap_or(line.as_str()))?;
        Ok(Some((decoded.0, decoded.1, n as u64)))
    }
}

impl<S: Sink> StoreAndForward<S> {
    pub fn drain(&mut self) -> Result<(), DeliverError> {
        let (generation, mut offset) = self.load_cursor().map_err(io_err)?;
        loop {
            let Some((record, idk, len)) = self.next_row(generation, offset).map_err(io_err)?
            else {
                self.compact(generation, offset).map_err(io_err)?;
                self.filled = false;
                return Ok(());
            };
            let delivered = match self.inner.deliver(&record, &idk) {
                Ok(()) => true,
                Err(error @ DeliverError::Blocked(_)) => return Err(error),
                Err(DeliverError::Transient(reason)) => {
                    self.compact(generation, offset).map_err(io_err)?;
                    return Err(DeliverError::Transient(reason));
                }
                Err(DeliverError::Poison(reason)) => {
                    if record.kind == crate::schema::CONTROL_KIND {
                        // A rejected transition fences every following record. Keep its cursor
                        // unchanged so remediation can retry the exact control after restart.
                        return Err(DeliverError::Blocked(format!(
                            "schema control rejected: {reason}"
                        )));
                    }
                    // Already acked to the runtime on enqueue. Skip so a
                    // later record's deliver cannot inherit this poison.
                    self.record_poison(&record, &idk, &reason).map_err(io_err)?;
                    false
                }
            };
            offset += len;
            self.store_cursor(generation, offset).map_err(io_err)?;
            self.drained += u64::from(delivered);
        }
    }

    fn record_poison(
        &self,
        record: &Record,
        idk: &IdempotencyKey,
        reason: &str,
    ) -> Result<(), Error> {
        let path = self.dir.join("poison.log");
        repair_log_tail(&path)?;
        let mut file = OpenOptions::new().create(true).append(true).open(&path)?;
        let encoded = delivery::encode(record, idk);
        let body = encoded.strip_suffix('\n').unwrap_or(encoded.as_str());
        let line = format!("{}\t{}\n", body, crate::checkpoint::escape_field(reason));
        file.write_all(line.as_bytes())?;
        file.sync_all()?;
        drop(file);
        sync_dir(&self.dir)?;
        Ok(())
    }
}

impl<S: Sink> Sink for StoreAndForward<S> {
    fn recover(&mut self) -> Result<(), DeliverError> {
        self.drain()
    }

    fn deliver(&mut self, record: &Record, idk: &IdempotencyKey) -> Result<(), DeliverError> {
        match self.drain() {
            Ok(()) | Err(DeliverError::Transient(_)) => {}
            Err(other) => return Err(other),
        }
        let (generation, _) = self.load_cursor().map_err(io_err)?;
        let path = self.log_path(generation);
        repair_log_tail(&path).map_err(io_err)?;
        let encoded = delivery::encode(record, idk);
        if encoded.len() as u64 > self.limits.max_bytes {
            self.filled = true;
            return Err(DeliverError::Transient(
                "store-and-forward buffer full: record exceeds max_bytes".into(),
            ));
        }
        while self.over_limit(encoded.len() as u64).map_err(io_err)? {
            self.filled = true;
            match self.limits.when_full {
                WhenFull::Stop => {
                    return Err(DeliverError::Transient(
                        "store-and-forward buffer full".into(),
                    ));
                }
                WhenFull::DropOldest => {
                    if !self.drop_oldest().map_err(io_err)? {
                        return Err(DeliverError::Transient(
                            "store-and-forward buffer full behind a schema control".into(),
                        ));
                    }
                }
            }
        }
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(io_err)?;
        file.write_all(encoded.as_bytes()).map_err(io_err)?;
        file.sync_all().map_err(io_err)?;
        drop(file);
        sync_dir(&self.dir).map_err(io_err)?;
        match self.drain() {
            Ok(()) => Ok(()),
            Err(DeliverError::Transient(_)) => Ok(()),
            Err(other) => Err(other),
        }
    }
}

fn io_err(error: impl Into<Error>) -> DeliverError {
    DeliverError::Transient(error.into().message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fake::record;
    use crate::sink::MemorySink;
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(1);

    fn dir() -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "hashi-buf-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn full_buffer_refuses_and_sets_the_alert() {
        let rec = record("tag", "k1", 1, "1.0");
        let idk = rec.idempotency_key("opc", "line-a");
        let line_len = delivery::encode(&rec, &idk).len() as u64;
        let mut sink =
            StoreAndForward::open(dir(), line_len.saturating_sub(1), MemorySink::new()).unwrap();
        let err = sink.deliver(&rec, &idk).unwrap_err();
        assert!(matches!(err, DeliverError::Transient(message) if message.contains("full")));
        assert!(sink.filled);
    }

    struct FlipSink {
        down: bool,
        inner: MemorySink,
    }

    impl Sink for FlipSink {
        fn deliver(&mut self, record: &Record, idk: &IdempotencyKey) -> Result<(), DeliverError> {
            if self.down {
                return Err(DeliverError::Transient("uplink down".into()));
            }
            self.inner.deliver(record, idk)
        }
    }

    #[test]
    fn drain_runs_before_a_capacity_reject_so_an_uplink_recovery_unblocks() {
        let rec = record("tag", "k1", 1, "1.0");
        let idk = rec.idempotency_key("opc", "line-a");
        let line_len = delivery::encode(&rec, &idk).len() as u64;
        let mut sink = StoreAndForward::open(
            dir(),
            line_len,
            FlipSink {
                down: true,
                inner: MemorySink::new(),
            },
        )
        .unwrap();
        sink.deliver(&rec, &idk).unwrap();
        assert_eq!(sink.pending().unwrap().len(), 1);
        sink.inner.down = false;
        let extra = record("tag", "k2", 2, "2.0");
        let extra_idk = extra.idempotency_key("opc", "line-a");
        sink.deliver(&extra, &extra_idk).unwrap();
        assert!(sink.pending().unwrap().is_empty());
        let delivered = sink.into_inner().inner.delivered;
        assert_eq!(delivered.len(), 2);
        assert_eq!(delivered[0].0.key, "k1");
        assert_eq!(delivered[1].0.key, "k2");
    }

    #[test]
    fn buffered_poison_does_not_attach_to_a_later_record() {
        let poison = record("tag", "bad", 1, "x");
        let poison_idk = poison.idempotency_key("opc", "line-a");
        let ok = record("tag", "k2", 2, "2.0");
        let ok_idk = ok.idempotency_key("opc", "line-a");
        let mut uplink = MemorySink::new();
        uplink.poison_keys.push("bad".into());
        let mut sink = StoreAndForward::open(dir(), 64 * 1024, uplink).unwrap();
        sink.deliver(&poison, &poison_idk).unwrap();
        sink.deliver(&ok, &ok_idk).unwrap();
        // A poisoned record is skipped, not delivered.
        assert_eq!(sink.stats().unwrap().drained, 1);
        let inner = sink.into_inner();
        assert_eq!(inner.delivered.len(), 1);
        assert_eq!(inner.delivered[0].0.key, "k2");
    }

    #[test]
    fn torn_tail_is_repaired_before_capacity_is_checked() {
        let rec = record("tag", "k1", 1, "1.0");
        let idk = rec.idempotency_key("opc", "line-a");
        let line_len = delivery::encode(&rec, &idk).len() as u64;
        let dir = dir();
        fs::write(dir.join("pending.0.log"), vec![b'x'; line_len as usize]).unwrap();
        let mut sink = StoreAndForward::open(&dir, line_len, MemorySink::new()).unwrap();
        sink.deliver(&rec, &idk).unwrap();
        assert_eq!(sink.into_inner().delivered.len(), 1);
    }

    #[test]
    fn poison_log_keeps_trailing_payload_whitespace() {
        let rec = record("tag", "bad", 1, "x ");
        let idk = rec.idempotency_key("opc", "line-a");
        let mut uplink = MemorySink::new();
        uplink.poison_keys.push("bad".into());
        let dir = dir();
        let mut sink = StoreAndForward::open(&dir, 64 * 1024, uplink).unwrap();
        sink.deliver(&rec, &idk).unwrap();
        let text = fs::read_to_string(dir.join("poison.log")).unwrap();
        assert!(
            text.contains("x \t"),
            "poison log dropped trailing payload space: {text:?}"
        );
    }

    thread_local! {
        static NOW_MS: std::cell::Cell<i64> = const { std::cell::Cell::new(0) };
    }

    fn now_ms() -> i64 {
        NOW_MS.with(std::cell::Cell::get)
    }

    fn set_now(ms: i64) {
        NOW_MS.with(|now| now.set(ms));
    }

    fn at(key: &str, position: u64, ingest_ts_ms: i64) -> (Record, IdempotencyKey) {
        let mut rec = record("tag", key, position, "1.0");
        rec.ingest_ts_ms = ingest_ts_ms;
        let idk = rec.idempotency_key("opc", "line-a");
        (rec, idk)
    }

    fn down() -> FlipSink {
        FlipSink {
            down: true,
            inner: MemorySink::new(),
        }
    }

    fn limits(max_bytes: u64, max_age_ms: Option<i64>, when_full: WhenFull) -> BufferLimits {
        BufferLimits {
            max_bytes,
            max_age_ms,
            when_full,
        }
    }

    /// The sink is cut for an hour of records, one every ten seconds, then
    /// restored: every record arrives once, in order, with its key (#47).
    #[test]
    fn an_hour_long_outage_drains_in_order_without_loss_or_duplicates() {
        let dir = dir();
        let mut sink = StoreAndForward::with_clock(
            &dir,
            limits(64 * 1024 * 1024, Some(2 * 3_600_000), WhenFull::Stop),
            down(),
            now_ms,
        )
        .unwrap();
        let sent: Vec<_> = (0..360)
            .map(|second| {
                at(
                    &format!("k{}", second % 7),
                    second + 1,
                    1 + second as i64 * 10_000,
                )
            })
            .collect();
        for (rec, idk) in &sent {
            set_now(rec.ingest_ts_ms);
            sink.deliver(rec, idk).unwrap();
        }
        let stats = sink.stats().unwrap();
        assert_eq!((stats.records, stats.oldest_ingest_ts_ms), (360, Some(1)));
        sink.inner.down = false;
        sink.recover().unwrap();
        let stats = sink.stats().unwrap();
        assert_eq!((stats.records, stats.drained, stats.dropped), (0, 360, 0));
        let delivered = sink.into_inner().inner.delivered;
        assert_eq!(delivered, sent);
    }

    /// A crash after delivering but before the cursor moved redelivers from
    /// the last stored cursor with the same keys: nothing is lost.
    #[test]
    fn a_crash_during_drain_resumes_without_loss() {
        let dir = dir();
        let mut sink = StoreAndForward::open(&dir, 64 * 1024, down()).unwrap();
        let sent: Vec<_> = (1..=3).map(|n| at(&format!("k{n}"), n, 0)).collect();
        for (rec, idk) in &sent {
            sink.deliver(rec, idk).unwrap();
        }
        let cursor_before_drain = fs::read(dir.join("cursor")).ok();
        let log_before_drain = fs::read(dir.join("pending.0.log")).unwrap();
        sink.inner.down = false;
        sink.recover().unwrap();
        // The process died before its cursor and compaction reached disk.
        let _ = fs::remove_file(dir.join("cursor"));
        if let Some(cursor) = cursor_before_drain {
            fs::write(dir.join("cursor"), cursor).unwrap();
        }
        for entry in fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("pending.")
            {
                fs::remove_file(path).unwrap();
            }
        }
        fs::write(dir.join("pending.0.log"), log_before_drain).unwrap();
        let mut restarted = StoreAndForward::open(&dir, 64 * 1024, MemorySink::new()).unwrap();
        restarted.recover().unwrap();
        assert_eq!(restarted.into_inner().delivered, sent);
    }

    /// `drop_oldest` makes room by dropping the oldest records, logs each as
    /// a gap and counts it across restarts; the rest drain in order.
    #[test]
    fn drop_oldest_logs_a_counted_gap_and_keeps_order() {
        let dir = dir();
        let (first, first_idk) = at("k1", 1, 0);
        let line = delivery::encode(&first, &first_idk).len() as u64;
        let mut sink = StoreAndForward::with_limits(
            &dir,
            limits(line * 2, None, WhenFull::DropOldest),
            down(),
        )
        .unwrap();
        let sent: Vec<_> = (1..=3).map(|n| at(&format!("k{n}"), n, 0)).collect();
        for (rec, idk) in &sent {
            sink.deliver(rec, idk).unwrap();
        }
        assert_eq!(sink.stats().unwrap().dropped, 1);
        let gaps = fs::read_to_string(dir.join("gaps.log")).unwrap();
        assert_eq!(gaps, format!("{}\t0\n", sent[0].1.encode()));
        sink.inner.down = false;
        sink.recover().unwrap();
        assert_eq!(sink.into_inner().inner.delivered, sent[1..]);
        let reopened = StoreAndForward::open(&dir, line * 2, MemorySink::new()).unwrap();
        assert_eq!(reopened.stats().unwrap().dropped, 1);
    }

    /// Past the age cap, `stop` refuses the next record and `drop_oldest`
    /// drops the stale ones.
    #[test]
    fn the_age_cap_stops_or_drops_by_policy() {
        let (old, old_idk) = at("k1", 1, 1);
        let (new, new_idk) = at("k2", 2, 120_001);
        set_now(120_001);
        let mut stop = StoreAndForward::with_clock(
            dir(),
            limits(64 * 1024, Some(60_000), WhenFull::Stop),
            down(),
            now_ms,
        )
        .unwrap();
        stop.deliver(&old, &old_idk).unwrap();
        let error = stop.deliver(&new, &new_idk).unwrap_err();
        assert!(matches!(error, DeliverError::Transient(message) if message.contains("full")));
        assert_eq!(stop.stats().unwrap().records, 1);

        let mut drop = StoreAndForward::with_clock(
            dir(),
            limits(64 * 1024, Some(60_000), WhenFull::DropOldest),
            down(),
            now_ms,
        )
        .unwrap();
        drop.deliver(&old, &old_idk).unwrap();
        drop.deliver(&new, &new_idk).unwrap();
        let stats = drop.stats().unwrap();
        assert_eq!((stats.records, stats.dropped), (1, 1));
        assert_eq!(
            drop.pending().unwrap(),
            vec![(new.clone(), new_idk.clone())]
        );

        // A control buffered by an older version has no ingest time: it does
        // not age, so it never stops the buffer by itself.
        let mut control = record(crate::schema::CONTROL_KIND, "schema", 1, "{}");
        control.ingest_ts_ms = 0;
        let control_idk = control.idempotency_key("opc", "line-a");
        let mut behind_control = StoreAndForward::with_clock(
            dir(),
            limits(64 * 1024, Some(60_000), WhenFull::Stop),
            down(),
            now_ms,
        )
        .unwrap();
        behind_control.deliver(&control, &control_idk).unwrap();
        behind_control.deliver(&new, &new_idk).unwrap();
        let stats = behind_control.stats().unwrap();
        assert_eq!((stats.records, stats.oldest_ingest_ts_ms), (2, None));
    }

    /// A schema control is never dropped: later records depend on it, so a
    /// full buffer behind one stops instead.
    #[test]
    fn drop_oldest_never_drops_a_schema_control() {
        let mut control = record(crate::schema::CONTROL_KIND, "schema", 1, "{}");
        control.ingest_ts_ms = 0;
        let control_idk = control.idempotency_key("opc", "line-a");
        let line = delivery::encode(&control, &control_idk).len() as u64;
        let mut sink =
            StoreAndForward::with_limits(dir(), limits(line, None, WhenFull::DropOldest), down())
                .unwrap();
        sink.deliver(&control, &control_idk).unwrap();
        let (rec, idk) = at("k1", 2, 0);
        assert!(sink.deliver(&rec, &idk).is_err());
        assert_eq!(sink.stats().unwrap().dropped, 0);
        assert_eq!(sink.pending().unwrap(), vec![(control, control_idk)]);
    }
}
