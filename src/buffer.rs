//! Store-and-forward disk buffer. Acks after durable enqueue so the connector
//! keeps reading while the uplink is down. Bounded; full is a transient error.

use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use crate::Error;
use crate::checkpoint::{atomic_write, repair_log_tail, sync_dir};
use crate::contract::{IdempotencyKey, Record};
use crate::delivery;
use crate::sink::{DeliverError, Sink};

#[derive(Debug)]
pub struct StoreAndForward<S> {
    dir: PathBuf,
    max_bytes: u64,
    inner: S,
    pub filled: bool,
}

impl<S> StoreAndForward<S> {
    pub fn open(dir: impl Into<PathBuf>, max_bytes: u64, inner: S) -> Result<Self, Error> {
        let dir = dir.into();
        fs::create_dir_all(&dir)?;
        Ok(Self {
            dir,
            max_bytes,
            inner,
            filled: false,
        })
    }

    pub fn into_inner(self) -> S {
        self.inner
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
            match self.inner.deliver(&record, &idk) {
                Ok(()) => {}
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
                }
            }
            offset += len;
            self.store_cursor(generation, offset).map_err(io_err)?;
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
        let used = self.pending_bytes().map_err(io_err)?;
        if used.saturating_add(encoded.len() as u64) > self.max_bytes {
            self.filled = true;
            return Err(DeliverError::Transient(
                "store-and-forward buffer full".into(),
            ));
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
}
