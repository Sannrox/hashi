//! File-backed Kafka-compatible topic log.
//!
//! Idempotent producer (key = record key, duplicates dropped by idempotency
//! key reconstructed from the durable log). A live broker client is out of
//! scope for offline `cargo test`.

use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::Error;
use crate::checkpoint::{repair_log_tail, sync_dir};
use crate::contract::{IdempotencyKey, Record};
use crate::delivery;
use crate::sink::{DeliverError, Sink};

#[derive(Debug, Clone)]
pub struct KafkaCompatibleSink {
    dir: PathBuf,
    topic: String,
    seen: HashSet<String>,
}

impl KafkaCompatibleSink {
    pub fn open(dir: impl Into<PathBuf>, topic: impl Into<String>) -> Result<Self, Error> {
        let topic = topic.into();
        validate_topic(&topic)?;
        let dir = dir.into();
        fs::create_dir_all(&dir)?;
        let mut sink = Self {
            dir,
            topic,
            seen: HashSet::new(),
        };
        sink.seen = sink.load_seen()?;
        Ok(sink)
    }

    fn log_path(&self) -> PathBuf {
        self.dir.join(format!("{}.partition-0.log", self.topic))
    }

    fn load_seen(&self) -> Result<HashSet<String>, Error> {
        let mut seen = HashSet::new();
        for (_, idk) in read_log(&self.log_path())? {
            seen.insert(idk.encode());
        }
        Ok(seen)
    }

    pub fn replay(&self) -> Result<Vec<(Record, IdempotencyKey)>, Error> {
        read_log(&self.log_path())
    }

    fn sync_log(&self) -> Result<(), Error> {
        let path = self.log_path();
        if path.exists() {
            File::open(&path)?.sync_all()?;
        }
        sync_dir(&self.dir)
    }
}

impl Sink for KafkaCompatibleSink {
    fn deliver(&mut self, record: &Record, idk: &IdempotencyKey) -> Result<(), DeliverError> {
        let encoded = idk.encode();
        if self.seen.contains(&encoded) {
            self.sync_log().map_err(io_err)?;
            return Ok(());
        }
        let path = self.log_path();
        repair_log_tail(&path).map_err(io_err)?;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(io_err)?;
        // Kafka record: key = record key, value = payload, producer idk.
        file.write_all(delivery::encode(record, idk).as_bytes())
            .map_err(io_err)?;
        drop(file);
        self.seen.insert(encoded);
        self.sync_log().map_err(io_err)?;
        Ok(())
    }
}

fn validate_topic(topic: &str) -> Result<(), Error> {
    if topic.is_empty() {
        return Err(Error::new("kafka topic is empty"));
    }
    if topic.contains(['/', '\\', '\0']) {
        return Err(Error::new("kafka topic must not contain a path separator"));
    }
    if topic == "." || topic == ".." {
        return Err(Error::new("kafka topic is not a valid filename"));
    }
    Ok(())
}

fn read_log(path: &Path) -> Result<Vec<(Record, IdempotencyKey)>, Error> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let mut buf = Vec::new();
    File::open(path)?.read_to_end(&mut buf)?;
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
        rows.push(delivery::decode(line)?);
    }
    Ok(rows)
}

fn io_err(error: impl Into<Error>) -> DeliverError {
    DeliverError::Transient(error.into().message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fake::record;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(1);

    fn dir() -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "hashi-kafka-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn idempotent_produce_drops_a_duplicate_idk() {
        let mut sink = KafkaCompatibleSink::open(dir(), "plant").unwrap();
        let rec = record("tag", "k1", 1, "1.0");
        let idk = rec.idempotency_key("opc", "line-a");
        sink.deliver(&rec, &idk).unwrap();
        sink.deliver(&rec, &idk).unwrap();
        assert_eq!(sink.replay().unwrap().len(), 1);
    }

    #[test]
    fn producer_key_is_the_record_key() {
        let mut sink = KafkaCompatibleSink::open(dir(), "plant").unwrap();
        let rec = record("tag", "pump-1", 4, "9.0");
        let idk = rec.idempotency_key("opc", "line-a");
        sink.deliver(&rec, &idk).unwrap();
        assert_eq!(sink.replay().unwrap()[0].0.key, "pump-1");
    }

    #[test]
    fn open_rejects_a_topic_that_escapes_the_sink_directory() {
        let root = dir();
        for topic in ["../outside", "/abs", "..", ""] {
            let err = KafkaCompatibleSink::open(&root, topic).unwrap_err();
            assert!(
                err.message.contains("topic"),
                "expected a topic error for {topic:?}, got {}",
                err.message
            );
        }
    }
}
