use hashi::contract::{CheckReport, DiscoveredKind, Position, Record};
use hashi::fake::record;
use hashi::schema::{CONTROL_KIND, DriftClass, DriftEvent, FieldType, Schema};
use hashi::{
    CheckpointStore, Connector, Error, FileCheckpoint, MemorySink, NullClock, Runtime,
    RuntimeConfig,
};
use std::sync::atomic::{AtomicU64, Ordering};
static NEXT: AtomicU64 = AtomicU64::new(0);

struct Fixture {
    schema: Schema,
    records: Vec<Record>,
}
impl Connector for Fixture {
    fn id(&self) -> &str {
        "fixture"
    }
    fn check(&self) -> Result<CheckReport, Error> {
        Ok(CheckReport {
            ok: true,
            message: String::new(),
        })
    }
    fn discover(&self) -> Result<Vec<DiscoveredKind>, Error> {
        Ok(vec![
            DiscoveredKind {
                kind: "row".into(),
                schema: self.schema.clone(),
                metadata: Default::default(),
            },
            DiscoveredKind {
                kind: "other".into(),
                schema: Schema::text(),
                metadata: Default::default(),
            },
        ])
    }
    fn read(
        &mut self,
        from: Option<Position>,
        _source_cursor: Option<&str>,
    ) -> Result<Option<Record>, Error> {
        Ok(self
            .records
            .iter()
            .find(|r| from.is_none_or(|p| r.position > p))
            .cloned())
    }
}
fn dir() -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!(
        "hashi-drift-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&p).unwrap();
    p
}
fn baseline() -> Schema {
    Schema {
        version: 1,
        key_fields: vec!["id".into()],
        fields: [
            ("id".into(), FieldType::Text),
            ("value".into(), FieldType::I32),
        ]
        .into(),
    }
}
fn runtime(
    path: &std::path::Path,
    schema: Schema,
    records: Vec<Record>,
    sink: MemorySink,
) -> Runtime<Fixture, MemorySink, FileCheckpoint, NullClock> {
    Runtime::new(
        Fixture { schema, records },
        sink,
        FileCheckpoint::open(path).unwrap(),
        NullClock { now_ms: 1 },
        RuntimeConfig::new("instance"),
    )
}

#[test]
fn drift_fixtures_survive_restart_and_isolate_breaking_kinds() {
    let old = baseline();
    let mut additive = old.clone();
    additive.fields.insert("extra".into(), FieldType::Bool);
    let mut widening = old.clone();
    widening.fields.insert("value".into(), FieldType::I64);
    let mut type_change = old.clone();
    type_change.fields.insert("value".into(), FieldType::Text);
    let mut removal = old.clone();
    removal.fields.remove("value");
    let mut key_change = old.clone();
    key_change.key_fields.clear();
    for (mut new, class) in [
        (additive, DriftClass::Additive),
        (widening, DriftClass::Widening),
        (type_change, DriftClass::Breaking),
        (removal, DriftClass::Breaking),
        (key_change, DriftClass::Breaking),
    ] {
        new.version = 2;
        let path = dir();
        runtime(
            &path,
            old.clone(),
            vec![record("row", "a", 1, "1")],
            MemorySink::new(),
        )
        .run()
        .unwrap();
        let mut rt = runtime(
            &path,
            new.clone(),
            vec![
                record("row", "b", 2, "changed"),
                record("other", "c", 3, "ok"),
            ],
            MemorySink::new(),
        );
        let report = rt.run().unwrap();
        let (_, sink, store) = rt.into_parts();
        let event: DriftEvent = serde_json::from_str(&sink.delivered[0].0.payload).unwrap();
        assert_eq!(sink.delivered[0].0.kind, CONTROL_KIND);
        assert_eq!(event.class, class);
        assert_eq!(event.previous, Some(old.clone()));
        assert_eq!(event.current, new);
        let breaking = class == DriftClass::Breaking;
        assert_eq!(report.quarantined, u64::from(breaking));
        assert_eq!(report.delivered, if breaking { 1 } else { 2 });
        assert!(sink.delivered.iter().any(|(r, _)| r.kind == "other"));
        if breaking {
            assert!(!sink.delivered.iter().any(|(r, _)| r.kind == "row"));
            assert!(
                !store.quarantined("fixture", "instance").unwrap()[0]
                    .reason
                    .is_empty()
            );
            let mut rt = runtime(
                &path,
                new.clone(),
                vec![record("row", "d", 4, "changed")],
                MemorySink::new(),
            );
            assert_eq!(rt.run().unwrap().quarantined, 1);
            rt.resolve_schema("row", &old).unwrap_err();
            rt.resolve_schema("row", &new).unwrap();
            rt.release_lease().expect("release resolved runtime lease");
            drop(rt);
            let mut rt = runtime(
                &path,
                new,
                vec![record("row", "e", 5, "changed")],
                MemorySink::new(),
            );
            assert_eq!(rt.run().unwrap().delivered, 1);
            let (_, sink, _) = rt.into_parts();
            let event: DriftEvent = serde_json::from_str(&sink.delivered[0].0.payload).unwrap();
            assert_eq!(event.class, DriftClass::Resolved);
            assert_eq!(sink.delivered[1].0.kind, "row");
        }
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn failed_control_delivery_remains_pending_and_blocks_data_until_restart() {
    let path = dir();
    let old = baseline();
    runtime(&path, old.clone(), vec![], MemorySink::new())
        .run()
        .unwrap();
    let mut new = old;
    new.version = 2;
    new.fields.insert("value".into(), FieldType::I64);
    let mut sink = MemorySink::new();
    sink.fail_next(9);
    let mut rt = runtime(&path, new.clone(), vec![record("row", "a", 1, "1")], sink);
    assert!(rt.run().unwrap_err().message.contains("control retries"));
    let (_, sink, store) = rt.into_parts();
    assert!(sink.delivered.is_empty());
    assert!(store.load("fixture", "instance").unwrap().is_none());
    let event = store.load_schemas("fixture", "instance").unwrap().kinds["row"]
        .event
        .clone();
    let mut rt = runtime(
        &path,
        new,
        vec![record("row", "a", 1, "1")],
        MemorySink::new(),
    );
    assert_eq!(rt.run().unwrap().delivered, 1);
    let (_, sink, _) = rt.into_parts();
    let replay: DriftEvent = serde_json::from_str(&sink.delivered[0].0.payload).unwrap();
    assert_eq!(replay, event);
    assert_eq!(sink.delivered[0].1.position, event.sequence);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn instance_lease_fails_immediately_and_releases_for_takeover() {
    let path = dir();
    let store = FileCheckpoint::open(&path).unwrap();
    let lease = store.acquire("fixture", "instance").unwrap();
    let mut rt = runtime(&path, baseline(), vec![], MemorySink::new());
    assert!(rt.run().unwrap_err().message.contains("already running"));
    lease.release().unwrap();
    rt.run().unwrap();
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn newer_discovery_replays_pending_control_before_recording_another_transition() {
    let path = dir();
    runtime(&path, baseline(), vec![], MemorySink::new())
        .run()
        .unwrap();
    let mut v2 = baseline();
    v2.version = 2;
    v2.fields.insert("value".into(), FieldType::I64);
    let mut sink = MemorySink::new();
    sink.fail_next(9);
    runtime(&path, v2, vec![], sink).run().unwrap_err();
    let mut v3 = baseline();
    v3.version = 3;
    v3.fields.insert("value".into(), FieldType::I64);
    v3.fields.insert("extra".into(), FieldType::Text);
    let mut rt = runtime(
        &path,
        v3,
        vec![record("row", "a", 1, "1")],
        MemorySink::new(),
    );
    rt.run().unwrap();
    let (_, sink, _) = rt.into_parts();
    let versions: Vec<u64> = sink
        .delivered
        .iter()
        .filter(|(r, _)| r.kind == CONTROL_KIND)
        .map(|(r, _)| {
            serde_json::from_str::<DriftEvent>(&r.payload)
                .unwrap()
                .current
                .version
        })
        .collect();
    assert_eq!(versions, [2, 3]);
    assert_eq!(sink.delivered.last().unwrap().0.kind, "row");
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn rejected_buffered_control_fences_data_across_restart_and_nested_buffers() {
    use hashi::{DeliverError, Sink, StoreAndForward};
    for offline in [false, true] {
        let path = dir();
        let control = record(CONTROL_KIND, "row", 2, "transition");
        let idk = control.idempotency_key("fixture", "instance");
        let mut peer = MemorySink::new();
        if offline {
            peer.fail_next(9);
        } else {
            peer.poison_keys.push("row".into());
        }
        let mut buffer = StoreAndForward::open(&path, 64 * 1024, peer).unwrap();
        let result = buffer.deliver(&control, &idk);
        if offline {
            result.unwrap();
        } else {
            assert!(matches!(result, Err(DeliverError::Blocked(_))));
        }
        drop(buffer);
        let mut peer = MemorySink::new();
        peer.poison_keys.push("row".into());
        let inner = StoreAndForward::open(&path, 64 * 1024, peer).unwrap();
        let mut outer = StoreAndForward::open(path.join("outer"), 64 * 1024, inner).unwrap();
        let data = record("row", "data", 1, "1");
        assert!(matches!(
            outer.deliver(&data, &data.idempotency_key("fixture", "instance")),
            Err(DeliverError::Blocked(_))
        ));
        let inner = outer.into_inner();
        assert_eq!(inner.pending().unwrap()[0].0, control);
        assert!(inner.into_inner().delivered.is_empty());
        let mut recovered = StoreAndForward::open(&path, 64 * 1024, MemorySink::new()).unwrap();
        recovered.recover().unwrap();
        assert_eq!(recovered.into_inner().delivered[0].0, control);
        std::fs::remove_dir_all(path).unwrap();
    }
}
