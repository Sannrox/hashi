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
