use hashi::contract::{CheckReport, DiscoveredKind, Position};
use hashi::{
    Change, Checkpoint, CheckpointStore, Connector, DeliverError, Error, FileCheckpoint,
    IdempotencyKey, InstanceLease, MemorySink, NullClock, Operation, QuarantineRow, Record,
    Runtime, RuntimeConfig, Sink, SourceMetadata,
};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
static NEXT: AtomicU64 = AtomicU64::new(0);
fn dir() -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "hashi-cursor-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&path).unwrap();
    path
}
fn records() -> Vec<Record> {
    [Operation::Insert, Operation::Update, Operation::Delete]
        .into_iter()
        .enumerate()
        .map(|(i, operation)| {
            let mut record = hashi::fake::record("row", "k", i as u64 + 1, "after");
            record.source = Some(SourceMetadata {
                cursor: Some(format!("opaque:α\t{}\n", i)),
                change: Some(Change {
                    operation,
                    before_image: Some("before\t\n".into()),
                    transaction_id: Some("transaction".into()),
                    commit_ts_ms: Some(1000),
                }),
            });
            record
        })
        .collect()
}
#[derive(Clone)]
struct Source(Vec<Record>);
impl Connector for Source {
    fn id(&self) -> &str {
        "cdc"
    }
    fn check(&self) -> Result<CheckReport, Error> {
        Ok(CheckReport {
            ok: true,
            message: String::new(),
        })
    }
    fn discover(&self) -> Result<Vec<DiscoveredKind>, Error> {
        Ok(vec![DiscoveredKind {
            kind: "row".into(),
            schema: hashi::schema::Schema::text(),
            metadata: Default::default(),
        }])
    }
    fn read(
        &mut self,
        from: Option<Position>,
        cursor: Option<&str>,
    ) -> Result<Option<Record>, Error> {
        if let Some(cursor) = cursor {
            let expected = self
                .0
                .iter()
                .find(|record| Some(record.position) == from)
                .and_then(|record| record.source.as_ref())
                .and_then(|source| source.cursor.as_deref());
            if expected != Some(cursor) {
                return Err(Error::new(
                    "runtime did not return the committed opaque cursor",
                ));
            }
        }
        Ok(self
            .0
            .iter()
            .find(|record| from.is_none_or(|position| record.position > position))
            .cloned())
    }
}
fn runtime(path: &Path, source: Source) -> Runtime<Source, MemorySink, FileCheckpoint, NullClock> {
    Runtime::new(
        source,
        MemorySink::new(),
        FileCheckpoint::open(path).unwrap(),
        NullClock { now_ms: 5 },
        RuntimeConfig::new("instance"),
    )
}
struct FailCommit(FileCheckpoint);
impl CheckpointStore for FailCommit {
    fn acquire(&self, c: &str, i: &str) -> Result<InstanceLease, Error> {
        self.0.acquire(c, i)
    }
    fn load_schemas(&self, c: &str, i: &str) -> Result<hashi::schema::SchemaState, Error> {
        self.0.load_schemas(c, i)
    }
    fn save_schemas(
        &self,
        c: &str,
        i: &str,
        s: &hashi::schema::SchemaState,
        token: u64,
    ) -> Result<(), Error> {
        self.0.save_schemas(c, i, s, token)
    }
    fn load(&self, c: &str, i: &str) -> Result<Option<Checkpoint>, Error> {
        self.0.load(c, i)
    }
    fn commit(&self, _: &Checkpoint) -> Result<(), Error> {
        Err(Error::new("injected crash before checkpoint replacement"))
    }
    fn quarantine(&self, r: &QuarantineRow, token: u64) -> Result<(), Error> {
        self.0.quarantine(r, token)
    }
    fn quarantined(&self, c: &str, i: &str) -> Result<Vec<QuarantineRow>, Error> {
        self.0.quarantined(c, i)
    }
}
struct Capture(Arc<Mutex<Vec<IdempotencyKey>>>);
impl Sink for Capture {
    fn deliver(&mut self, _: &Record, key: &IdempotencyKey) -> Result<(), DeliverError> {
        self.0.lock().unwrap().push(key.clone());
        Ok(())
    }
}
#[test]
fn position_and_cursor_commit_together_and_replay_after_sink_ack_before_checkpoint() {
    let path = dir();
    let source = Source(records());
    assert_eq!(
        runtime(&path, source.clone())
            .run_while(|r| r.delivered < 1)
            .unwrap()
            .checkpoint,
        1
    );
    let keys = Arc::new(Mutex::new(vec![]));
    let mut failed = Runtime::new(
        source.clone(),
        Capture(keys.clone()),
        FailCommit(FileCheckpoint::open(&path).unwrap()),
        NullClock { now_ms: 5 },
        RuntimeConfig::new("instance"),
    );
    assert!(failed.run().unwrap_err().message.contains("injected crash"));
    drop(failed);
    let checkpoint = FileCheckpoint::open(&path)
        .unwrap()
        .load("cdc", "instance")
        .unwrap()
        .unwrap();
    assert_eq!(checkpoint.position, 1);
    assert_eq!(
        checkpoint.source_cursor,
        source.0[0].source.as_ref().unwrap().cursor
    );
    let mut resumed = runtime(&path, source.clone());
    assert_eq!(resumed.run().unwrap().delivered, 2);
    let (_, sink, store) = resumed.into_parts();
    assert_eq!(sink.delivered[0].1, keys.lock().unwrap()[0]);
    let checkpoint = store.load("cdc", "instance").unwrap().unwrap();
    assert_eq!(checkpoint.position, 3);
    assert_eq!(
        checkpoint.source_cursor,
        source.0[2].source.as_ref().unwrap().cursor
    );
    std::fs::remove_dir_all(path).unwrap();
}
#[test]
fn version_one_checkpoint_and_quarantine_upgrade_in_the_same_durable_slots() {
    let path = dir();
    std::fs::write(
        path.join("checkpoint-636463.696E7374616E6365.v1"),
        "1\ncdc\ninstance\n1\n",
    )
    .unwrap();
    std::fs::write(
        path.join("quarantine-636463.696E7374616E6365.v1.log"),
        "v1\tcdc\tinstance\trow\tlegacy\t1\tpoison\told\t10\t11\t\n",
    )
    .unwrap();
    let store = FileCheckpoint::open(&path).unwrap();
    assert_eq!(
        store
            .load("cdc", "instance")
            .unwrap()
            .unwrap()
            .source_cursor,
        None
    );
    let mut source = Source(records());
    source.0[1].key = String::new();
    assert_eq!(runtime(&path, source.clone()).run().unwrap().quarantined, 1);
    let checkpoint = store.load("cdc", "instance").unwrap().unwrap();
    assert_eq!(checkpoint.contract_version, hashi::CHECKPOINT_VERSION);
    assert_eq!(checkpoint.position, 3);
    let rows = store.quarantined("cdc", "instance").unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].source, None);
    assert_eq!(rows[1].source, source.0[1].source);
    assert!(
        std::fs::read_to_string(path.join("checkpoint-636463.696E7374616E6365.v1"))
            .unwrap()
            .starts_with("3\n")
    );
    std::fs::remove_dir_all(path).unwrap();
}
#[test]
fn source_metadata_round_trips_through_every_sink_and_buffer_encoding() {
    let path = dir();
    let mut sample = records();
    for operation in [Operation::TransactionBegin, Operation::TransactionEnd] {
        let mut record = sample[0].clone();
        record.position = sample.len() as u64 + 1;
        record
            .source
            .as_mut()
            .unwrap()
            .change
            .as_mut()
            .unwrap()
            .operation = operation;
        sample.push(record);
    }
    let legacy = hashi::fake::record("row", "k", 7, "plain");
    let key = legacy.idempotency_key("cdc", "instance");
    assert_eq!(
        hashi::delivery::encode(&legacy, &key),
        "v1\t3:cdc/8:instance/3:row/1:k/7\trow\tk\t70\t0\t\t7\tplain\n"
    );
    let frame = hashi::platform::encode_frame(&legacy, &key);
    assert_eq!(&frame[4..8], &1u32.to_le_bytes());
    sample.push(legacy);
    let mut json = hashi::JsonLinesSink::open(path.join("json")).unwrap();
    #[cfg(feature = "parquet")]
    let mut parquet = hashi::ParquetSink::open(path.join("parquet")).unwrap();
    let mut kafka = hashi::KafkaCompatibleSink::open(path.join("kafka"), "records").unwrap();
    let mut platform = hashi::PlatformSdkSink::new(hashi::CapturePeer::new());
    for record in &sample {
        let key = record.idempotency_key("cdc", "instance");
        let line = hashi::delivery::encode(record, &key);
        assert_eq!(
            hashi::delivery::decode(line.trim_end_matches('\n'))
                .unwrap()
                .0,
            *record
        );
        json.deliver(record, &key).unwrap();
        #[cfg(feature = "parquet")]
        parquet.deliver(record, &key).unwrap();
        kafka.deliver(record, &key).unwrap();
        platform.deliver(record, &key).unwrap();
    }
    let decoded = |rows: Vec<(Record, IdempotencyKey)>| {
        rows.into_iter()
            .map(|(record, _)| record)
            .collect::<Vec<_>>()
    };
    assert_eq!(decoded(json.replay().unwrap()), sample);
    #[cfg(feature = "parquet")]
    assert_eq!(decoded(parquet.replay().unwrap()), sample);
    assert_eq!(decoded(kafka.replay().unwrap()), sample);
    assert_eq!(decoded(platform.into_peer().decoded().unwrap()), sample);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn missing_or_oversized_cursor_never_reaches_the_sink_or_advances_checkpoint() {
    for missing in [
        None,
        Some(SourceMetadata {
            cursor: None,
            change: None,
        }),
    ] {
        let path = dir();
        let mut records = records();
        records[1].source = missing;
        let mut rt = runtime(&path, Source(records));
        assert!(rt.run().unwrap_err().message.contains("omitted"));
        let (_, sink, store) = rt.into_parts();
        assert_eq!(sink.delivered.len(), 1);
        assert_eq!(store.load("cdc", "instance").unwrap().unwrap().position, 1);
        std::fs::remove_dir_all(path).unwrap();
    }
    let path = dir();
    let mut records = records();
    records[0].source.as_mut().unwrap().cursor = Some("x".repeat(4097));
    let mut rt = runtime(&path, Source(records));
    assert!(rt.run().unwrap_err().message.contains("bounds"));
    let (_, sink, store) = rt.into_parts();
    assert!(sink.delivered.is_empty());
    assert!(store.load("cdc", "instance").unwrap().is_none());
    std::fs::remove_dir_all(path).unwrap();
}
