#![cfg(feature = "opcua-simulator")]
#[path = "../examples/support/opcua_simulator.rs"]
mod simulator;
use hashi::{
    Connector,
    opcua::{Authentication, Config, OpcUaConnector, Policy, SubscriptionConfig},
};
use opcua::types::{DateTime, NodeId, StatusCode};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
static SEQ: AtomicU64 = AtomicU64::new(0);
fn config(server: &simulator::Simulator, pki: &std::path::Path) -> Config {
    Config {
        id: "subscription".into(),
        endpoint: server.endpoint.clone(),
        application_uri: simulator::CLIENT_URI.into(),
        pki_dir: pki.into(),
        policy: Policy::Basic256Sha256,
        allow_insecure_lab: false,
        authentication: Authentication::UserName {
            user: "reader".into(),
            password: "fixture-password".into(),
        },
        roots: vec![server.root.clone().into()],
        max_nodes: 100,
        timeout: Duration::from_secs(10),
        schema_version: 1,
    }
}
fn subscription(nodes: Vec<NodeId>, path: &std::path::Path) -> SubscriptionConfig {
    SubscriptionConfig {
        nodes,
        state_dir: path.into(),
        publishing_interval: Duration::from_millis(50),
        sampling_interval: Duration::from_millis(20),
        deadband: Some(1.0),
        queue_size: 10,
        max_pending_bytes: 16 * 1024 * 1024,
    }
}
fn fixture(count: usize) -> (std::path::PathBuf, simulator::Simulator) {
    let path = std::env::temp_dir().join(format!(
        "hashi-subscription-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let server_pki = path.join("server");
    let client_pki = path.join("client");
    simulator::certificate(&server_pki, simulator::SERVER_URI);
    simulator::certificate(&client_pki, simulator::CLIENT_URI);
    std::fs::copy(client_pki.join("own/cert.der"), server_pki.join("user.der")).unwrap();
    simulator::trust(&server_pki, &client_pki.join("own/cert.der"));
    simulator::trust(&client_pki, &server_pki.join("own/cert.der"));
    let server = simulator::Simulator::start_with_nodes(&server_pki, "127.0.0.1", 0, count);
    (path, server)
}
#[test]
fn subscription_replays_durable_samples_and_recreates_after_server_restart() {
    let (path, server) = fixture(0);
    let state = path.join("state");
    let mut source = OpcUaConnector::subscribe(
        config(&server, &path.join("client")),
        subscription(vec![server.value.clone()], &state),
    )
    .unwrap();
    let first = source.read(None, None).unwrap().unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&first.payload).unwrap()["value"],
        21.5
    );
    assert!(first.source_ts_ms > 0);
    let timestamp = DateTime::now();
    server.set_value(24.0, StatusCode::Uncertain, timestamp);
    let next = source.read(Some(first.position), None).unwrap().unwrap();
    assert_eq!(next.source_ts_ms, timestamp.as_chrono().timestamp_millis());
    assert!(next.quality.as_ref().unwrap().contains("Uncertain"));
    server.set_value(24.5, StatusCode::Uncertain, DateTime::now());
    std::thread::sleep(Duration::from_millis(150));
    server.set_value(27.0, StatusCode::Uncertain, DateTime::now());
    let next = source.read(Some(next.position), None).unwrap().unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&next.payload).unwrap()["value"],
        27.0,
        "absolute deadband must suppress the smaller value change"
    );
    let committed = next.position - 1;
    drop(source);
    let mut source = OpcUaConnector::subscribe(
        config(&server, &path.join("client")),
        subscription(vec![server.value.clone()], &state),
    )
    .unwrap();
    let replay = source.read(Some(committed), None).unwrap().unwrap();
    assert_eq!(replay, next);
    assert_eq!(
        replay.idempotency_key("subscription", "one"),
        next.idempotency_key("subscription", "one")
    );
    // Consume the newly created subscription's initial sample before restarting its server.
    let initial = source.read(Some(replay.position), None).unwrap().unwrap();
    let port = url::Url::parse(&server.endpoint).unwrap().port().unwrap();
    drop(server);
    std::thread::sleep(Duration::from_millis(300));
    let restarted = simulator::Simulator::start(&path.join("server"), "127.0.0.1", port);
    let resumed = source.read(Some(initial.position), None).unwrap().unwrap();
    assert!(resumed.position > initial.position);
    assert_eq!(resumed.kind, restarted.value.to_string());
    drop(source);
    drop(restarted);
    std::fs::remove_dir_all(path).unwrap();
}
#[test]
fn ten_thousand_monitored_items_have_bounded_batches_and_measured_latency() {
    let (path, server) = fixture(10_000);
    let nodes: Vec<_> = (0..10_000)
        .map(|i| NodeId::new(server.value.namespace, format!("Sample{i}")))
        .collect();
    let setup = Instant::now();
    let mut source = OpcUaConnector::subscribe(
        config(&server, &path.join("client")),
        subscription(nodes, &path.join("state")),
    )
    .unwrap();
    let setup_elapsed = setup.elapsed();
    assert_eq!(source.discover().unwrap().len(), 10_000);
    let started = Instant::now();
    let mut from = None;
    let mut seen = std::collections::BTreeSet::new();
    let mut latencies = Vec::new();
    for _ in 0..10_000 {
        let read = Instant::now();
        let record = source.read(from, None).unwrap().unwrap();
        latencies.push(read.elapsed());
        assert!(from.is_none_or(|p| record.position > p));
        from = Some(record.position);
        seen.insert(record.key);
    }
    assert_eq!(seen.len(), 10_000);
    latencies.sort();
    eprintln!(
        "10k setup={setup_elapsed:?}, drain={:?}, records/s={:.0}, read p50={:?}, p99={:?}",
        started.elapsed(),
        10_000.0 / started.elapsed().as_secs_f64(),
        latencies[5000],
        latencies[9900]
    );
    let burst = Instant::now();
    server.update_samples(10_000);
    let mut ages = Vec::new();
    for _ in 0..10_000 {
        let record = source.read(from, None).unwrap().unwrap();
        assert!(
            serde_json::from_str::<serde_json::Value>(&record.payload).unwrap()["value"]
                .as_f64()
                .unwrap()
                >= 100_000.0
        );
        from = Some(record.position);
        ages.push(DateTime::now().as_chrono().timestamp_millis() - record.source_ts_ms);
    }
    ages.sort();
    eprintln!(
        "10k change burst={:?}, records/s={:.0}, source-to-read age p50={}ms, p99={}ms",
        burst.elapsed(),
        10_000.0 / burst.elapsed().as_secs_f64(),
        ages[5000],
        ages[9900]
    );
    drop(source);
    drop(server);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn rejected_batch_never_becomes_a_durable_receipt_and_reports_unhealthy() {
    let (path, server) = fixture(0);
    let state = path.join("state");
    let mut bounded = subscription(vec![server.value.clone()], &state);
    bounded.max_pending_bytes = 1;
    let mut source =
        OpcUaConnector::subscribe(config(&server, &path.join("client")), bounded).unwrap();
    assert!(
        source
            .read(None, None)
            .unwrap_err()
            .message
            .contains("bound")
    );
    assert!(!source.check().unwrap().ok);
    assert!(!state.join("opcua.pending.v1").exists());
    drop(source);
    let mut source = OpcUaConnector::subscribe(
        config(&server, &path.join("client")),
        subscription(vec![server.value.clone()], &state),
    )
    .unwrap();
    assert_eq!(source.read(None, None).unwrap().unwrap().position, 1);
    let receipt = std::fs::read(state.join("opcua.pending.v1")).unwrap();
    server.set_value(f64::NAN, StatusCode::BadOutOfRange, DateTime::now());
    assert!(
        source
            .read(Some(1), None)
            .unwrap_err()
            .message
            .contains("non-finite")
    );
    assert_eq!(
        std::fs::read(state.join("opcua.pending.v1")).unwrap(),
        receipt,
        "a nonfinite value must not replace the last durable receipt"
    );
    assert!(!source.check().unwrap().ok);
    drop(source);
    drop(server);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn subscription_payload_matches_discovered_text_for_numeric_subtypes_and_uint64() {
    let (path, server) = fixture(0);
    for (name, expected) in [("Duration", "5"), ("Counter", "18446744073709551615")] {
        let node = NodeId::new(server.value.namespace, name);
        let mut source = OpcUaConnector::subscribe(
            config(&server, &path.join("client")),
            subscription(vec![node], &path.join(name)),
        )
        .unwrap();
        assert_eq!(
            source.discover().unwrap()[0].schema.fields["value"],
            hashi::schema::FieldType::Text
        );
        let record = source.read(None, None).unwrap().unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&record.payload).unwrap()["value"],
            expected
        );
    }
    drop(server);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn historical_pages_replay_durably_and_fill_server_restart_before_subscription_tail() {
    use hashi::opcua::HistoryConfig;
    use opcua::types::DataValue;
    use std::sync::{Arc, Mutex};
    let (path, server) = fixture(0);
    drop(server);
    let start = DateTime::now().ticks() - 10_000_000;
    let sample = |value: f64, tick: i64| DataValue::new_at(value, DateTime::from(tick));
    let samples = Arc::new(Mutex::new(vec![sample(1.0, start), sample(2.0, start + 1)]));
    let server = simulator::Simulator::start_with_history(
        &path.join("server"),
        "127.0.0.1",
        0,
        samples.clone(),
    );
    let node = server.history_value();
    let state = path.join("history-state");
    let open = |server: &simulator::Simulator| {
        let mut settings = subscription(vec![node.clone()], &state);
        settings.deadband = None;
        OpcUaConnector::subscribe_with_history(
            config(server, &path.join("client")),
            settings,
            HistoryConfig {
                start: DateTime::from(start),
                page_rows: 1,
            },
        )
        .unwrap()
    };
    let payload = |record: &hashi::Record| {
        serde_json::from_str::<serde_json::Value>(&record.payload).unwrap()["value"]
            .as_f64()
            .unwrap()
    };
    let mut source = open(&server);
    let first = source.read(None, None).unwrap().unwrap();
    assert_eq!(payload(&first), 1.0);
    let second = source.read(Some(first.position), None).unwrap().unwrap();
    assert_eq!(payload(&second), 2.0);
    drop(source);
    let mut source = open(&server);
    assert_eq!(
        source.read(Some(first.position), None).unwrap().unwrap(),
        second
    );
    let port = url::Url::parse(&server.endpoint).unwrap().port().unwrap();
    drop(server);
    samples
        .lock()
        .unwrap()
        .extend([sample(3.0, start + 2), sample(4.0, start + 3)]);
    let server = simulator::Simulator::start_with_history(
        &path.join("server"),
        "127.0.0.1",
        port,
        samples.clone(),
    );
    let third = source.read(Some(second.position), None).unwrap().unwrap();
    let fourth = source.read(Some(third.position), None).unwrap().unwrap();
    assert_eq!([payload(&third), payload(&fourth)], [3.0, 4.0]);
    assert_eq!(
        [third.position, fourth.position],
        [second.position + 1, second.position + 2]
    );
    server.set_history_value(sample(5.0, DateTime::now().ticks()));
    let fifth = source.read(Some(fourth.position), None).unwrap().unwrap();
    assert_eq!(
        payload(&fifth),
        5.0,
        "subscription initial overlap must not become a duplicate"
    );
    let caught_up = source.read(Some(fifth.position), None).unwrap();
    assert!(caught_up.is_none());
    assert!(source.check().unwrap().ok);
    drop(source);
    let overflow_state = path.join("overflow-history");
    let overflow_settings = || {
        let mut settings = subscription(vec![node.clone()], &overflow_state);
        settings.queue_size = 2;
        settings.deadband = None;
        settings
    };
    let mut overflow_source = OpcUaConnector::subscribe_with_history(
        config(&server, &path.join("client")),
        overflow_settings(),
        HistoryConfig {
            start: DateTime::from(start),
            page_rows: 1,
        },
    )
    .unwrap();
    let mut overflow_last = overflow_source.read(None, None).unwrap().unwrap();
    for _ in 1..5 {
        overflow_last = overflow_source
            .read(Some(overflow_last.position), None)
            .unwrap()
            .unwrap();
    }
    let burst_start = DateTime::now().ticks();
    for number in 6..=25 {
        server.set_history_value(sample(
            number as f64,
            burst_start + (number - 6) * 1_000_000,
        ));
    }
    assert!(
        overflow_source
            .read(Some(overflow_last.position), None)
            .unwrap_err()
            .message
            .contains("queue Overflow")
    );
    assert!(!overflow_source.check().unwrap().ok);
    drop(overflow_source);
    std::thread::sleep(Duration::from_secs(2));
    let mut overflow_source = OpcUaConnector::subscribe_with_history(
        config(&server, &path.join("client")),
        overflow_settings(),
        HistoryConfig {
            start: DateTime::from(start),
            page_rows: 1,
        },
    )
    .unwrap();
    for number in 6..=25 {
        let record = overflow_source
            .read(Some(overflow_last.position), None)
            .unwrap()
            .unwrap();
        assert_eq!(
            payload(&record),
            number as f64,
            "restart must recover values dropped before the overflow marker"
        );
        overflow_last = record;
    }
    drop(overflow_source);
    let fifth_time = samples
        .lock()
        .unwrap()
        .iter()
        .find(|value| value.value == Some(opcua::types::Variant::Double(5.0)))
        .unwrap()
        .source_timestamp
        .unwrap();
    samples
        .lock()
        .unwrap()
        .retain(|sample| sample.source_timestamp != Some(fifth_time));
    samples
        .lock()
        .unwrap()
        .push(sample(6.0, DateTime::now().ticks()));
    let mut source = open(&server);
    assert!(
        source
            .read(Some(fifth.position), None)
            .unwrap_err()
            .message
            .contains("retained historical boundary")
    );
    drop(source);
    let mut bounded = subscription(vec![node.clone()], &path.join("bounded-history"));
    bounded.max_pending_bytes = 1;
    let mut source = OpcUaConnector::subscribe_with_history(
        config(&server, &path.join("client")),
        bounded,
        HistoryConfig {
            start: DateTime::from(start),
            page_rows: 1,
        },
    )
    .unwrap();
    assert!(
        source
            .read(None, None)
            .unwrap_err()
            .message
            .contains("batch bound")
    );
    assert!(!source.check().unwrap().ok);
    assert!(!path.join("bounded-history/opcua.pending.v1").exists());
    drop(source);
    let mut source = OpcUaConnector::subscribe_with_history(
        config(&server, &path.join("client")),
        subscription(vec![node.clone()], &path.join("empty-history")),
        HistoryConfig {
            start: DateTime::now(),
            page_rows: 1,
        },
    )
    .unwrap();
    assert!(source.read(None, None).unwrap().is_none());
    assert!(source.check().unwrap().ok);
    drop(source);
    samples.lock().unwrap().push(DataValue::new_at_status(
        6.0,
        DateTime::now(),
        StatusCode::BadDataUnavailable.set_overflow(true),
    ));
    let mut source = OpcUaConnector::subscribe_with_history(
        config(&server, &path.join("client")),
        subscription(vec![node.clone()], &path.join("unavailable-history")),
        HistoryConfig {
            start: DateTime::from(start),
            page_rows: 1,
        },
    )
    .unwrap();
    assert!(
        source
            .read(None, None)
            .unwrap_err()
            .message
            .contains("historical range is incomplete")
    );
    assert!(!path.join("unavailable-history/opcua.pending.v1").exists());
    drop(source);
    drop(server);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn history_unsupported_is_reported_while_subscription_keeps_running() {
    let (path, server) = fixture(0);
    let history_start = DateTime::from(DateTime::now().ticks() - 10_000_000);
    let mut source = OpcUaConnector::subscribe_with_history(
        config(&server, &path.join("client")),
        subscription(vec![server.value.clone()], &path.join("state")),
        hashi::opcua::HistoryConfig {
            start: history_start,
            page_rows: 2,
        },
    )
    .unwrap();
    let first = source.read(None, None).unwrap().unwrap();
    let report = source.check().unwrap();
    assert!(report.ok);
    assert!(report.message.contains("history unsupported for 1"));
    server.set_value(30.0, StatusCode::Good, DateTime::now());
    let next = source.read(Some(first.position), None).unwrap().unwrap();
    assert!(next.payload.contains("30.0"));
    drop(source);
    let mut source = OpcUaConnector::subscribe_with_history(
        config(&server, &path.join("client")),
        subscription(vec![server.value.clone()], &path.join("state")),
        hashi::opcua::HistoryConfig {
            start: history_start,
            page_rows: 2,
        },
    )
    .unwrap();
    assert!(
        source
            .read(Some(next.position), None)
            .unwrap_err()
            .message
            .contains("nodes without history")
    );
    drop(source);
    drop(server);
    std::fs::remove_dir_all(path).unwrap();
}

/// Conformance kit (#28) driver: one monitored node whose value changes past the
/// deadband; drift is a schema revision.
struct OpcUaFamily {
    server: simulator::Simulator,
    path: std::path::PathBuf,
    value: f64,
    schema_version: u64,
}

impl hashi::conformance::Family for OpcUaFamily {
    type Source = OpcUaConnector;

    fn connect(&mut self) -> Result<OpcUaConnector, hashi::Error> {
        let mut config = config(&self.server, &self.path.join("client"));
        config.schema_version = self.schema_version;
        OpcUaConnector::subscribe(
            config,
            subscription(vec![self.server.value.clone()], &self.path.join("state")),
        )
    }

    /// Each change writes a new value past the deadband; the value tells it apart
    /// from the initial sample and from earlier changes.
    fn produce(&mut self, keys: &[&str]) -> Result<Vec<hashi::conformance::Change>, hashi::Error> {
        let mut changes = Vec::new();
        for _ in keys {
            self.value += 2.0;
            self.server
                .set_value(self.value, StatusCode::Good, DateTime::now());
            changes.push(hashi::conformance::Change::new(
                self.server.value.to_string(),
                "value",
                self.value,
            ));
            // Past the sampling interval, so every change is its own sample.
            std::thread::sleep(Duration::from_millis(100));
        }
        Ok(changes)
    }

    /// A schema revision on an existing spool fails closed until an operator
    /// recovers its state (docs/opcua.md), so the kit checks the refusal.
    fn drift(&mut self) -> Result<hashi::conformance::Drift, hashi::Error> {
        self.schema_version += 1;
        Ok(hashi::conformance::Drift::Refused)
    }
}

#[test]
fn opcua_passes_the_conformance_kit() {
    let (path, server) = fixture(0);
    let mut family = OpcUaFamily {
        server,
        path: path.clone(),
        value: 21.5,
        schema_version: 1,
    };
    let passed = hashi::conformance::run(&mut family, &path.join("checkpoints")).unwrap();
    assert_eq!(passed, hashi::conformance::CHECKS);
    drop(family);
    std::fs::remove_dir_all(path).unwrap();
}
