//! Drive one connector: check, discover, read, deliver, then commit.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::Error;
use crate::checkpoint::{Checkpoint, CheckpointStore, QuarantineRow};
use crate::connector::Connector;
use crate::contract::{CONTRACT_VERSION, IdempotencyKey, Position, Record};
use crate::sink::{DeliverError, Sink};

pub trait Clock {
    fn now_ms(&self) -> i64;
    fn sleep_ms(&self, ms: u64);
}

pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64
    }

    fn sleep_ms(&self, ms: u64) {
        if ms > 0 {
            std::thread::sleep(Duration::from_millis(ms));
        }
    }
}

/// Test clock. Sleep is a no-op so retry jitter does not stall the suite.
#[derive(Debug, Clone)]
pub struct NullClock {
    pub now_ms: i64,
}

impl Clock for NullClock {
    fn now_ms(&self) -> i64 {
        self.now_ms
    }

    fn sleep_ms(&self, _ms: u64) {}
}

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub instance: String,
    pub in_flight_limit: usize,
    pub max_attempts: u32,
    pub max_record_bytes: usize,
}

impl RuntimeConfig {
    pub fn new(instance: impl Into<String>) -> Self {
        Self {
            instance: instance.into(),
            in_flight_limit: 16,
            max_attempts: 3,
            max_record_bytes: 64 * 1024,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RunReport {
    pub discovered: usize,
    pub delivered: u64,
    pub quarantined: u64,
    pub checkpoint: Position,
}

pub struct Runtime<C, S, K, T> {
    connector: C,
    sink: S,
    store: K,
    clock: T,
    config: RuntimeConfig,
}

impl<C, S, K, T> Runtime<C, S, K, T>
where
    C: Connector,
    S: Sink,
    K: CheckpointStore,
    T: Clock,
{
    pub fn new(connector: C, sink: S, store: K, clock: T, config: RuntimeConfig) -> Self {
        Self {
            connector,
            sink,
            store,
            clock,
            config,
        }
    }

    pub fn into_parts(self) -> (C, S, K) {
        (self.connector, self.sink, self.store)
    }

    pub fn run(&mut self) -> Result<RunReport, Error> {
        self.run_while(|_| true)
    }

    /// `keep_going` is checked after each checkpoint so a kill/restart test
    /// can stop the loop without losing the last committed position.
    pub fn run_while(
        &mut self,
        mut keep_going: impl FnMut(&RunReport) -> bool,
    ) -> Result<RunReport, Error> {
        let check = self.connector.check()?;
        if !check.ok {
            return Err(Error::new(format!("check failed: {}", check.message)));
        }
        let discovered = self.connector.discover()?;
        let connector_id = self.connector.id().to_string();
        let loaded = self.store.load(&connector_id, &self.config.instance)?;
        let mut committed: Option<Position> = loaded.map(|checkpoint| checkpoint.position);
        let mut report = RunReport {
            discovered: discovered.len(),
            delivered: 0,
            quarantined: 0,
            checkpoint: committed.unwrap_or(0),
        };
        if self.config.in_flight_limit == 0 {
            return Err(Error::new("in_flight_limit must be >= 1"));
        }
        // Sequential deliver-then-commit is a buffer of one. The next read
        // waits for the sink ack; `in_flight_limit` is the configured bound.
        loop {
            if !keep_going(&report) {
                break;
            }
            let Some(mut record) = self.connector.read(committed)? else {
                break;
            };
            if let Some(previous) = committed
                && record.position <= previous
            {
                return Err(Error::new("connector emitted a non-monotonic position"));
            }
            record.ingest_ts_ms = self.clock.now_ms();
            match self.dispatch(&connector_id, &record)? {
                Dispatch::Delivered => {
                    self.commit(&connector_id, record.position)?;
                    committed = Some(record.position);
                    report.delivered += 1;
                    report.checkpoint = record.position;
                }
                Dispatch::Quarantined => {
                    self.commit(&connector_id, record.position)?;
                    committed = Some(record.position);
                    report.quarantined += 1;
                    report.checkpoint = record.position;
                }
            }
        }
        Ok(report)
    }

    fn dispatch(&mut self, connector_id: &str, record: &Record) -> Result<Dispatch, Error> {
        if record.key.trim().is_empty() || record.kind.trim().is_empty() {
            self.quarantine(connector_id, record, "missing kind or key")?;
            return Ok(Dispatch::Quarantined);
        }
        if record.payload.len() > self.config.max_record_bytes {
            self.quarantine(connector_id, record, "payload exceeds max_record_bytes")?;
            return Ok(Dispatch::Quarantined);
        }
        let idk = record.idempotency_key(connector_id, &self.config.instance);
        let mut last_error = String::new();
        for attempt in 0..self.config.max_attempts.max(1) {
            match self.sink.deliver(record, &idk) {
                Ok(()) => return Ok(Dispatch::Delivered),
                Err(DeliverError::Poison(reason)) => {
                    self.quarantine(connector_id, record, &reason)?;
                    return Ok(Dispatch::Quarantined);
                }
                Err(DeliverError::Transient(reason)) => {
                    last_error = reason;
                    self.clock.sleep_ms(backoff_ms(attempt, record.position));
                }
            }
        }
        Err(Error::new(format!(
            "retries exhausted at position {}: {last_error}",
            record.position
        )))
    }

    fn commit(&self, connector_id: &str, position: Position) -> Result<(), Error> {
        self.store.commit(&Checkpoint {
            contract_version: CONTRACT_VERSION,
            connector: connector_id.to_string(),
            instance: self.config.instance.clone(),
            position,
        })
    }

    fn quarantine(&self, connector_id: &str, record: &Record, reason: &str) -> Result<(), Error> {
        self.store.quarantine(&QuarantineRow {
            connector: connector_id.to_string(),
            instance: self.config.instance.clone(),
            kind: record.kind.clone(),
            key: record.key.clone(),
            position: record.position,
            reason: reason.to_string(),
            payload: record.payload.clone(),
            source_ts_ms: record.source_ts_ms,
            ingest_ts_ms: record.ingest_ts_ms,
            quality: record.quality.clone(),
        })
    }
}

enum Dispatch {
    Delivered,
    Quarantined,
}

fn backoff_ms(attempt: u32, position: Position) -> u64 {
    let base = 1u64 << attempt.min(4);
    let jitter = (position.wrapping_mul(11400714819323198485) >> 48) % (base + 1);
    base + jitter
}

pub fn idempotency(record: &Record, connector: &str, instance: &str) -> IdempotencyKey {
    record.idempotency_key(connector, instance)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::FileCheckpoint;
    use crate::fake::{FakeSource, record};
    use crate::sink::MemorySink;
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};

    static NEXT: AtomicU64 = AtomicU64::new(1);

    fn dir() -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "hashi-run-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&path).unwrap();
        path
    }

    fn records(n: u64) -> Vec<Record> {
        (1..=n)
            .map(|i| record("tag", &format!("k{i}"), i, &format!("{i}.0")))
            .collect()
    }

    fn runtime(
        source: FakeSource,
        sink: MemorySink,
        dir: &std::path::Path,
    ) -> Runtime<FakeSource, MemorySink, FileCheckpoint, NullClock> {
        Runtime::new(
            source,
            sink,
            FileCheckpoint::open(dir).unwrap(),
            NullClock { now_ms: 1_000 },
            RuntimeConfig::new("line-a"),
        )
    }

    #[test]
    fn a_record_at_position_zero_is_delivered() {
        let dir = dir();
        let source = FakeSource::new(
            "opc",
            vec![record("tag", "k0", 0, "0.0"), record("tag", "k1", 1, "1.0")],
        );
        let mut rt = runtime(source, MemorySink::new(), &dir);
        let report = rt.run().unwrap();
        assert_eq!(report.delivered, 2);
        assert_eq!(report.checkpoint, 1);
        let keys: Vec<u64> = rt
            .into_parts()
            .1
            .delivered
            .iter()
            .map(|(record, _)| record.position)
            .collect();
        assert_eq!(keys, vec![0, 1]);
    }

    #[test]
    fn check_discover_snapshot_and_incremental() {
        let dir = dir();
        let mut source = FakeSource::new("opc", records(2));
        let mut rt = runtime(source.clone(), MemorySink::new(), &dir);
        let first = rt.run().unwrap();
        assert_eq!(first.discovered, 1);
        assert_eq!(first.delivered, 2);
        assert_eq!(first.checkpoint, 2);
        source.append(record("tag", "k3", 3, "3.0"));
        let mut rt = runtime(source, MemorySink::new(), &dir);
        let second = rt.run().unwrap();
        assert_eq!(second.delivered, 1);
        assert_eq!(second.checkpoint, 3);
        let (_, sink, _) = rt.into_parts();
        assert_eq!(sink.delivered[0].1.encode(), "3:opc/6:line-a/3:tag/2:k3/3");
    }

    #[test]
    fn kill_mid_read_resumes_from_the_last_commit_without_gaps() {
        let dir = dir();
        let source = FakeSource::new("opc", records(8));
        let mut rt = runtime(source.clone(), MemorySink::new(), &dir);
        let stopped = rt.run_while(|report| report.delivered < 3).unwrap();
        assert_eq!(stopped.checkpoint, 3);
        let mut rt = runtime(source, MemorySink::new(), &dir);
        let resumed = rt.run().unwrap();
        assert_eq!(resumed.delivered, 5);
        assert_eq!(resumed.checkpoint, 8);
        let keys: Vec<u64> = rt
            .into_parts()
            .1
            .delivered
            .iter()
            .map(|(record, _)| record.position)
            .collect();
        assert_eq!(keys, vec![4, 5, 6, 7, 8]);
    }

    #[test]
    fn a_crash_after_deliver_replays_the_same_idempotency_key() {
        let dir = dir();
        let source = FakeSource::new("opc", records(3));
        let delivered = Arc::new(Mutex::new(Vec::new()));
        let store = CrashAfter {
            inner: FileCheckpoint::open(&dir).unwrap(),
            delivered: delivered.clone(),
        };
        let mut rt = Runtime::new(
            source.clone(),
            CountingSink {
                inner: MemorySink::new(),
                delivered: delivered.clone(),
            },
            store,
            NullClock { now_ms: 1 },
            RuntimeConfig::new("line-a"),
        );
        let err = rt.run().unwrap_err();
        assert!(err.message.contains("injected checkpoint crash"));
        let first_keys: Vec<String> = delivered
            .lock()
            .unwrap()
            .iter()
            .map(|idk| idk.encode())
            .collect();
        assert_eq!(first_keys, vec!["3:opc/6:line-a/3:tag/2:k1/1"]);
        let mut rt = runtime(source, MemorySink::new(), &dir);
        let report = rt.run().unwrap();
        assert_eq!(report.delivered, 3);
        let replayed = &rt.into_parts().1.delivered[0].1;
        assert_eq!(replayed.encode(), "3:opc/6:line-a/3:tag/2:k1/1");
    }

    #[test]
    fn poison_records_go_to_quarantine_and_the_checkpoint_still_advances() {
        let dir = dir();
        let mut sink = MemorySink::new();
        sink.poison_keys.push("k2".into());
        let mut rt = runtime(FakeSource::new("opc", records(3)), sink, &dir);
        let report = rt.run().unwrap();
        assert_eq!(report.delivered, 2);
        assert_eq!(report.quarantined, 1);
        assert_eq!(report.checkpoint, 3);
        let rows = FileCheckpoint::open(&dir)
            .unwrap()
            .quarantined("opc", "line-a")
            .unwrap();
        assert_eq!(rows[0].key, "k2");
        assert_eq!(rows[0].payload, "2.0");
        assert_eq!(rows[0].reason, "poison record k2");
    }

    #[test]
    fn transient_sink_errors_retry_then_succeed() {
        let dir = dir();
        let mut sink = MemorySink::new();
        sink.fail_next(2);
        let mut rt = runtime(FakeSource::new("opc", records(1)), sink, &dir);
        let report = rt.run().unwrap();
        assert_eq!(report.delivered, 1);
        assert_eq!(report.quarantined, 0);
    }

    #[test]
    fn exhausted_transient_retries_leave_the_checkpoint_unmoved() {
        let dir = dir();
        let mut sink = MemorySink::new();
        sink.fail_next(8);
        let mut rt = runtime(FakeSource::new("opc", records(3)), sink, &dir);
        let error = rt.run().unwrap_err();
        assert!(error.message.contains("retries exhausted"));
        assert!(
            FileCheckpoint::open(&dir)
                .unwrap()
                .load("opc", "line-a")
                .unwrap()
                .is_none()
        );
        let sink = MemorySink::new();
        let mut rt = runtime(FakeSource::new("opc", records(3)), sink, &dir);
        let report = rt.run().unwrap();
        assert_eq!(report.delivered, 3);
        assert_eq!(report.checkpoint, 3);
    }

    struct CountingSink {
        inner: MemorySink,
        delivered: Arc<Mutex<Vec<IdempotencyKey>>>,
    }

    impl Sink for CountingSink {
        fn deliver(&mut self, record: &Record, idk: &IdempotencyKey) -> Result<(), DeliverError> {
            self.inner.deliver(record, idk)?;
            self.delivered.lock().unwrap().push(idk.clone());
            Ok(())
        }
    }

    struct CrashAfter<K> {
        inner: K,
        delivered: Arc<Mutex<Vec<IdempotencyKey>>>,
    }

    impl<K: CheckpointStore> CheckpointStore for CrashAfter<K> {
        fn load(&self, connector: &str, instance: &str) -> Result<Option<Checkpoint>, Error> {
            self.inner.load(connector, instance)
        }

        fn commit(&self, checkpoint: &Checkpoint) -> Result<(), Error> {
            let already = self
                .inner
                .load(&checkpoint.connector, &checkpoint.instance)?;
            if already.is_none() && !self.delivered.lock().unwrap().is_empty() {
                return Err(Error::new("injected checkpoint crash"));
            }
            self.inner.commit(checkpoint)
        }

        fn quarantine(&self, row: &QuarantineRow) -> Result<(), Error> {
            self.inner.quarantine(row)
        }

        fn quarantined(
            &self,
            connector: &str,
            instance: &str,
        ) -> Result<Vec<QuarantineRow>, Error> {
            self.inner.quarantined(connector, instance)
        }
    }
}
