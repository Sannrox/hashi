//! OPC UA historical access through the historian trait (#23): the #22
//! backfill, kill, resume and tail handover against the simulator, and a
//! server without history.
#![cfg(feature = "opcua-simulator")]
#[path = "../examples/support/opcua_simulator.rs"]
mod simulator;

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::fs::File;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use hashi::checkpoint::{Checkpoint, CheckpointStore, QuarantineRow};
use hashi::historian::{Historian, HistorianConfig, HistorianConnector};
use hashi::opcua::historian::OpcUaHistorian;
use hashi::opcua::{Authentication, Config, Policy};
use hashi::runtime::Clock;
use hashi::{Connector, Error, MemorySink, Runtime, RuntimeConfig};
use opcua::types::{DataValue, DateTime, NodeId};

const START_MS: i64 = 1_700_000_000_000;
const UNIX_EPOCH_TICKS: i64 = 116_444_736_000_000_000;

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "hashi-opcua-historian-{name}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ))
}

/// A sample `offset_ms` after the backfill start, half a millisecond in, so
/// tick-to-millisecond rounding is exercised.
fn sample(offset_ms: i64) -> DataValue {
    let ticks = (START_MS + offset_ms) * 10_000 + UNIX_EPOCH_TICKS + 5_000;
    DataValue::new_at(offset_ms as f64, DateTime::from(ticks))
}

fn pki(path: &std::path::Path) {
    let server_pki = path.join("server");
    let client_pki = path.join("client");
    simulator::certificate(&server_pki, simulator::SERVER_URI);
    simulator::certificate(&client_pki, simulator::CLIENT_URI);
    std::fs::copy(client_pki.join("own/cert.der"), server_pki.join("user.der")).unwrap();
    simulator::trust(&server_pki, &client_pki.join("own/cert.der"));
    simulator::trust(&client_pki, &server_pki.join("own/cert.der"));
}

fn config(server: &simulator::Simulator, pki: &std::path::Path, root: NodeId) -> Config {
    Config {
        id: "historian".into(),
        endpoint: server.endpoint.clone(),
        application_uri: simulator::CLIENT_URI.into(),
        pki_dir: pki.into(),
        policy: Policy::Basic256Sha256,
        allow_insecure_lab: false,
        authentication: Authentication::UserName {
            user: "reader".into(),
            password: "fixture-password".into(),
        },
        roots: vec![root.into()],
        max_nodes: 100,
        timeout: Duration::from_secs(10),
        schema_version: 1,
    }
}

#[derive(Clone)]
struct FixtureClock(Rc<Cell<i64>>);

impl Clock for FixtureClock {
    fn now_ms(&self) -> i64 {
        self.0.get()
    }

    fn sleep_ms(&self, _ms: u64) {}
}

#[derive(Clone, Default)]
struct MemoryStore(Rc<RefCell<Option<Checkpoint>>>);

impl CheckpointStore for MemoryStore {
    fn acquire(&self, _connector: &str, _instance: &str) -> Result<File, Error> {
        Ok(File::create(scratch("lease"))?)
    }

    fn load_schemas(&self, _: &str, _: &str) -> Result<hashi::schema::SchemaState, Error> {
        Ok(Default::default())
    }

    fn save_schemas(&self, _: &str, _: &str, _: &hashi::schema::SchemaState) -> Result<(), Error> {
        Ok(())
    }

    fn load(&self, _: &str, _: &str) -> Result<Option<Checkpoint>, Error> {
        Ok(self.0.borrow().clone())
    }

    fn commit(&self, checkpoint: &Checkpoint) -> Result<(), Error> {
        *self.0.borrow_mut() = Some(checkpoint.clone());
        Ok(())
    }

    fn quarantine(&self, _: &QuarantineRow) -> Result<(), Error> {
        Err(Error::new("no quarantine expected"))
    }

    fn quarantined(&self, _: &str, _: &str) -> Result<Vec<QuarantineRow>, Error> {
        Ok(Vec::new())
    }
}

type Source = HistorianConnector<OpcUaHistorian, FixtureClock>;

fn connector(historian: OpcUaHistorian, now: &Rc<Cell<i64>>, state: &std::path::Path) -> Source {
    HistorianConnector::new(
        "historian",
        historian,
        FixtureClock(now.clone()),
        HistorianConfig {
            start_ms: START_MS,
            chunk_ms: 4_000,
            page_limit: 2,
            settle_ms: 0,
            max_reads_per_second: 0,
            state_dir: state.into(),
        },
    )
    .unwrap()
}

fn runtime(
    source: Source,
    now: &Rc<Cell<i64>>,
    store: &MemoryStore,
) -> Runtime<Source, MemorySink, MemoryStore, FixtureClock> {
    Runtime::new(
        source,
        MemorySink::new(),
        store.clone(),
        FixtureClock(now.clone()),
        RuntimeConfig::new("plant"),
    )
}

/// Delivered sample timestamps, counting repeats.
fn delivered(sinks: &[MemorySink]) -> BTreeMap<i64, usize> {
    let mut seen = BTreeMap::new();
    for sink in sinks {
        for (record, _) in &sink.delivered {
            let (_, ts) = record.key.split_once('@').unwrap();
            *seen
                .entry(ts.parse::<i64>().unwrap() - START_MS)
                .or_insert(0) += 1;
        }
    }
    seen
}

#[test]
fn opcua_history_backfills_resumes_after_a_kill_and_hands_over_to_the_tail() {
    let path = scratch("backfill");
    pki(&path);
    // A sample at the start itself is before the backfill window.
    let samples = Arc::new(Mutex::new(
        (0..=10).map(|second| sample(second * 1_000)).collect(),
    ));
    let server = simulator::Simulator::start_with_history(
        &path.join("server"),
        "127.0.0.1",
        0,
        samples.clone(),
    );
    let open = |now: &Rc<Cell<i64>>| {
        let historian =
            OpcUaHistorian::connect(config(&server, &path.join("client"), server.history_root()))
                .unwrap();
        connector(historian, now, &path.join("state"))
    };
    let now = Rc::new(Cell::new(START_MS + 20_000));
    let store = MemoryStore::default();

    let source = open(&now);
    assert!(source.check().unwrap().ok);
    let mut first = runtime(source, &now, &store);
    let report = first.run_while(|report| report.delivered < 4).unwrap();
    assert_eq!(report.delivered, 4);
    let (_, first_sink, _) = first.into_parts();

    // A new process resumes from the committed cursor.
    let mut second = runtime(open(&now), &now, &store);
    assert_eq!(second.run().unwrap().delivered, 6);
    let (_, second_sink, _) = second.into_parts();
    let backfilled = delivered(&[first_sink, second_sink]);
    assert!(backfilled.values().all(|count| *count == 1), "overlap");
    assert_eq!(
        backfilled.keys().copied().collect::<Vec<_>>(),
        (1..=10).map(|second| second * 1_000).collect::<Vec<_>>(),
        "gap"
    );

    // New history after the backfill reaches the tail exactly once.
    samples
        .lock()
        .unwrap()
        .extend([sample(21_000), sample(22_000), sample(23_000)]);
    now.set(START_MS + 30_000);
    let mut third = runtime(open(&now), &now, &store);
    assert_eq!(third.run().unwrap().delivered, 3);
    let (_, tail_sink, _) = third.into_parts();
    assert_eq!(
        delivered(&[tail_sink]),
        BTreeMap::from([(21_000, 1), (22_000, 1), (23_000, 1)])
    );
    assert_eq!(
        runtime(open(&now), &now, &store).run().unwrap().delivered,
        0
    );

    // A page boundary inside one millisecond cannot be resumed, so it is
    // refused rather than skipping the second sample.
    let late = sample(40_000);
    let mut same_millisecond = late.clone();
    same_millisecond.source_timestamp = late
        .source_timestamp
        .map(|time| DateTime::from(time.ticks() + 1_000));
    samples.lock().unwrap().extend([late, same_millisecond]);
    let mut historian =
        OpcUaHistorian::connect(config(&server, &path.join("client"), server.history_root()))
            .unwrap();
    let tag = historian.tags().unwrap().remove(0).name;
    assert_eq!(tag, "nsu=urn:hashi:history-fixture;s=HistoricalValue");
    assert_eq!(
        historian
            .read_raw(&tag, START_MS + 30_000, START_MS + 50_000, 2)
            .unwrap()
            .len(),
        2
    );
    assert!(
        historian
            .read_raw(&tag, START_MS + 30_000, START_MS + 50_000, 1)
            .is_err()
    );

    // A server restart fails one read; the next read opens a new session.
    let port = url::Url::parse(&server.endpoint).unwrap().port().unwrap();
    drop(server);
    assert!(
        historian
            .read_raw(&tag, START_MS + 30_000, START_MS + 50_000, 2)
            .is_err()
    );
    let _server =
        simulator::Simulator::start_with_history(&path.join("server"), "127.0.0.1", port, samples);
    assert_eq!(
        historian
            .read_raw(&tag, START_MS + 30_000, START_MS + 50_000, 2)
            .unwrap()
            .len(),
        2
    );
}

#[test]
fn a_server_without_history_is_reported_and_the_connector_still_runs() {
    let path = scratch("none");
    pki(&path);
    let server = simulator::Simulator::start(&path.join("server"), "127.0.0.1", 0);
    let historian =
        OpcUaHistorian::connect(config(&server, &path.join("client"), server.root.clone()))
            .unwrap();
    let now = Rc::new(Cell::new(START_MS + 20_000));
    let source = connector(historian, &now, &path.join("state"));
    let report = source.check().unwrap();
    assert!(report.ok);
    assert!(
        report.message.contains("without history"),
        "{}",
        report.message
    );
    let mut runtime = runtime(source, &now, &MemoryStore::default());
    assert_eq!(runtime.run().unwrap().delivered, 0);
}
