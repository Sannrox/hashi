//! Consumer of records. The runtime commits a checkpoint only after `Ok`.

use crate::Error;
use crate::contract::{IdempotencyKey, Record};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeliverError {
    Transient(String),
    Poison(String),
}

impl From<DeliverError> for Error {
    fn from(error: DeliverError) -> Self {
        match error {
            DeliverError::Transient(message) | DeliverError::Poison(message) => Error::new(message),
        }
    }
}

pub trait Sink {
    fn deliver(&mut self, record: &Record, idk: &IdempotencyKey) -> Result<(), DeliverError>;

    /// Drain work that does not need a new source record. Store-and-forward
    /// uses this on runtime start so a recovered uplink can empty the queue
    /// after the source has already been checkpointed.
    fn recover(&mut self) -> Result<(), DeliverError> {
        Ok(())
    }
}

/// In-memory sink for tests and local proof. Records every attempt, including retries.
#[derive(Debug, Default)]
pub struct MemorySink {
    pub delivered: Vec<(Record, IdempotencyKey)>,
    pub poison_keys: Vec<String>,
    pub fail_times: u32,
    remaining_failures: u32,
}

impl MemorySink {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn fail_next(&mut self, times: u32) {
        self.fail_times = times;
        self.remaining_failures = times;
    }
}

impl Sink for MemorySink {
    fn deliver(&mut self, record: &Record, idk: &IdempotencyKey) -> Result<(), DeliverError> {
        if self.poison_keys.iter().any(|key| key == &record.key) {
            return Err(DeliverError::Poison(format!(
                "poison record {}",
                record.key
            )));
        }
        if self.remaining_failures > 0 {
            self.remaining_failures -= 1;
            return Err(DeliverError::Transient("injected sink failure".into()));
        }
        self.delivered.push((record.clone(), idk.clone()));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::StoreAndForward;
    use crate::checkpoint::FileCheckpoint;
    use crate::fake::{FakeSource, record};
    use crate::jsonl::JsonLinesSink;
    use crate::kafka::KafkaCompatibleSink;
    use crate::parquet::ParquetSink;
    use crate::platform::{CapturePeer, PlatformSdkSink};
    use crate::runtime::{NullClock, Runtime, RuntimeConfig};
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(1);

    fn dir() -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "hashi-sink-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&path).unwrap();
        path
    }

    fn sample() -> Vec<Record> {
        vec![
            record("tag", "k1", 1, "1.0"),
            record("tag", "k2", 2, "2.0"),
            record("tag", "k1", 3, "1.1"),
        ]
    }

    fn assert_per_key_order(rows: &[(Record, IdempotencyKey)]) {
        let mut last_k1 = 0;
        let mut last_k2 = 0;
        for (record, _) in rows {
            if record.key == "k1" {
                assert!(record.position > last_k1);
                last_k1 = record.position;
            }
            if record.key == "k2" {
                assert!(record.position > last_k2);
                last_k2 = record.position;
            }
        }
        assert_eq!(last_k1, 3);
        assert_eq!(last_k2, 2);
    }

    #[test]
    fn jsonl_ack_resume_and_per_key_order() {
        let dir = dir();
        let mut sink = JsonLinesSink::open(&dir).unwrap();
        for rec in sample() {
            let idk = rec.idempotency_key("opc", "line-a");
            sink.deliver(&rec, &idk).unwrap();
        }
        assert_eq!(sink.replay().unwrap().len(), 3);
        let mut sink = JsonLinesSink::open(&dir).unwrap();
        let extra = record("tag", "k1", 4, "1.2");
        sink.deliver(&extra, &extra.idempotency_key("opc", "line-a"))
            .unwrap();
        let rows = sink.replay().unwrap();
        assert_eq!(rows.len(), 4);
        assert_per_key_order(&rows[..3]);
        assert_eq!(rows[3].0.position, 4);
    }

    #[test]
    fn parquet_ack_resume_and_per_key_order() {
        let dir = dir();
        let mut sink = ParquetSink::open(&dir).unwrap();
        for rec in sample() {
            let idk = rec.idempotency_key("opc", "line-a");
            sink.deliver(&rec, &idk).unwrap();
        }
        let mut sink = ParquetSink::open(&dir).unwrap();
        let extra = record("tag", "k2", 5, "2.1");
        sink.deliver(&extra, &extra.idempotency_key("opc", "line-a"))
            .unwrap();
        let rows = sink.replay().unwrap();
        assert_eq!(rows.len(), 4);
        assert_per_key_order(&rows[..3]);
    }

    #[test]
    fn kafka_ack_resume_and_per_key_order() {
        let dir = dir();
        let mut sink = KafkaCompatibleSink::open(&dir, "plant").unwrap();
        for rec in sample() {
            let idk = rec.idempotency_key("opc", "line-a");
            sink.deliver(&rec, &idk).unwrap();
        }
        let mut sink = KafkaCompatibleSink::open(&dir, "plant").unwrap();
        sink.deliver(&sample()[0], &sample()[0].idempotency_key("opc", "line-a"))
            .unwrap();
        let rows = sink.replay().unwrap();
        assert_eq!(rows.len(), 3);
        assert_per_key_order(&rows);
    }

    #[test]
    fn platform_sdk_ack_and_per_key_order() {
        let mut sink = PlatformSdkSink::new(CapturePeer::new());
        for rec in sample() {
            let idk = rec.idempotency_key("opc", "line-a");
            sink.deliver(&rec, &idk).unwrap();
        }
        let rows = sink.into_peer().decoded().unwrap();
        assert_eq!(rows.len(), 3);
        assert_per_key_order(&rows);
    }

    struct OutageSink {
        clock: Arc<AtomicI64>,
        down_until_ms: i64,
        inner: MemorySink,
    }

    impl Sink for OutageSink {
        fn deliver(&mut self, record: &Record, idk: &IdempotencyKey) -> Result<(), DeliverError> {
            if self.clock.load(Ordering::SeqCst) < self.down_until_ms {
                return Err(DeliverError::Transient("uplink down".into()));
            }
            self.inner.deliver(record, idk)
        }
    }

    #[test]
    fn thirty_minute_uplink_outage_loses_no_records_and_drains_in_order() {
        const THIRTY_MIN_MS: i64 = 30 * 60 * 1000;
        let dir = dir();
        let ckpt = dir.join("ckpt");
        let buf = dir.join("buf");
        let clock = Arc::new(AtomicI64::new(0));
        let uplink = OutageSink {
            clock: clock.clone(),
            down_until_ms: THIRTY_MIN_MS,
            inner: MemorySink::new(),
        };
        let source = FakeSource::new("opc", sample());
        let mut rt = Runtime::new(
            source,
            StoreAndForward::open(&buf, 64 * 1024, uplink).unwrap(),
            FileCheckpoint::open(&ckpt).unwrap(),
            NullClock { now_ms: 1 },
            RuntimeConfig::new("line-a"),
        );
        let report = rt.run().unwrap();
        assert_eq!(report.delivered, 3);
        assert_eq!(report.checkpoint, 3);
        let (_, buffer, _) = rt.into_parts();
        assert_eq!(buffer.pending().unwrap().len(), 3);
        assert!(buffer.into_inner().inner.delivered.is_empty());

        clock.store(THIRTY_MIN_MS, Ordering::SeqCst);
        let uplink = OutageSink {
            clock: clock.clone(),
            down_until_ms: THIRTY_MIN_MS,
            inner: MemorySink::new(),
        };
        let mut rt = Runtime::new(
            FakeSource::new("opc", sample()),
            StoreAndForward::open(&buf, 64 * 1024, uplink).unwrap(),
            FileCheckpoint::open(&ckpt).unwrap(),
            NullClock { now_ms: 1 },
            RuntimeConfig::new("line-a"),
        );
        let recovered = rt.run().unwrap();
        assert_eq!(recovered.delivered, 0);
        assert_eq!(recovered.checkpoint, 3);
        let (_, buffer, _) = rt.into_parts();
        assert!(buffer.pending().unwrap().is_empty());
        let delivered = buffer.into_inner().inner.delivered;
        assert_eq!(
            delivered
                .iter()
                .map(|(record, _)| (record.key.as_str(), record.position))
                .collect::<Vec<_>>(),
            vec![("k1", 1), ("k2", 2), ("k1", 3)]
        );
    }
}
