#![cfg(feature = "mqtt")]
//! Broker proofs. Run through scripts/test-mqtt-container.sh, which owns the broker.
use hashi::mqtt::{Config, Decoder, MqttConnector, Protocol, Route, Transport};
use hashi::{
    Connector, DeliverError, FileCheckpoint, IdempotencyKey, JsonLinesSink, MemorySink, Record,
    Runtime, RuntimeConfig, Sink, SystemClock,
};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

static NEXT: AtomicU64 = AtomicU64::new(1);

/// Unique per process and call, so parallel agents and tests never share a topic or session.
fn unique(label: &str) -> String {
    format!(
        "{label}-{}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn port(var: &str) -> u16 {
    assert_eq!(std::env::var("HASHI_MQTT_DISPOSABLE").as_deref(), Ok("1"));
    std::env::var(var).unwrap().parse().unwrap()
}

fn config(protocol: Protocol, client_id: &str, prefix: &str) -> Config {
    Config {
        id: "mqtt-fixture".into(),
        host: "127.0.0.1".into(),
        port: port("HASHI_MQTT_PORT"),
        protocol,
        transport: Transport::Plain,
        allow_insecure_lab: true,
        user_name: None,
        client_id: client_id.into(),
        shared_group: None,
        routes: vec![
            Route {
                filter: format!("{prefix}/json/+"),
                kind: "reading".into(),
                decoder: Decoder::Json,
            },
            Route {
                filter: format!("{prefix}/raw/#"),
                kind: "frame".into(),
                decoder: Decoder::Raw,
            },
        ],
        session_expiry: Duration::from_secs(300),
        keep_alive: Duration::from_secs(30),
        timeout: Duration::from_secs(3),
        max_packet_bytes: 64 * 1024,
        schema_version: 1,
    }
}

fn temp_dir(label: &str) -> PathBuf {
    let path = std::env::temp_dir().join(unique(label));
    std::fs::create_dir_all(&path).unwrap();
    path
}

/// Publishes at QoS 1 and returns once the broker acknowledged every message.
fn publish(topic: &str, payloads: impl IntoIterator<Item = String>) {
    let payloads: Vec<String> = payloads.into_iter().collect();
    let mut options =
        rumqttc::MqttOptions::new(unique("publisher"), "127.0.0.1", port("HASHI_MQTT_PORT"));
    options.set_keep_alive(Duration::from_secs(30));
    let (client, mut connection) = rumqttc::Client::new(options, payloads.len() + 1);
    for payload in &payloads {
        client
            .publish(topic, rumqttc::QoS::AtLeastOnce, false, payload.as_bytes())
            .unwrap();
    }
    let mut acked = 0;
    while acked < payloads.len() {
        match connection.recv_timeout(Duration::from_secs(10)) {
            Ok(Ok(rumqttc::Event::Incoming(rumqttc::Packet::PubAck(_)))) => acked += 1,
            Ok(Ok(_)) => {}
            other => panic!("publisher failed: {other:?}"),
        }
    }
    client.disconnect().unwrap();
}

/// Runs until `read` reports caught up and returns the delivered records.
fn drain(connector: MqttConnector, state: &Path) -> (MqttConnector, Vec<Record>) {
    let mut runtime = Runtime::new(
        connector,
        MemorySink::new(),
        FileCheckpoint::open(state).unwrap(),
        SystemClock,
        RuntimeConfig::new("line-a"),
    );
    runtime.run().unwrap();
    let (connector, sink, _) = runtime.into_parts();
    (
        connector,
        sink.delivered
            .into_iter()
            .map(|(record, _)| record)
            .collect(),
    )
}

fn payloads(records: &[Record]) -> Vec<String> {
    records
        .iter()
        .map(|record| record.payload.clone())
        .collect()
}

fn numbered(range: std::ops::Range<u32>) -> Vec<String> {
    range.map(|i| format!("{{\"n\":{i}}}")).collect()
}

#[test]
#[ignore = "requires the disposable broker created by scripts/test-mqtt-container.sh"]
fn reconnect_at_qos1_loses_nothing_on_both_protocols() {
    for protocol in [Protocol::V311, Protocol::V5] {
        let prefix = unique("hashi/reconnect");
        let client_id = unique("reconnect");
        let state = temp_dir("hashi-mqtt-reconnect");
        let connector = MqttConnector::connect(config(protocol, &client_id, &prefix)).unwrap();
        publish(&format!("{prefix}/json/a"), numbered(0..5));
        publish(&format!("{prefix}/raw/x/y"), ["\u{1}AB".to_string()]);
        let (connector, first) = drain(connector, &state);
        assert_eq!(first.len(), 6, "{protocol:?}");
        assert_eq!(first[5].kind, "frame");
        assert_eq!(first[5].key, format!("{prefix}/raw/x/y"));
        assert_eq!(first[5].payload, "014142");
        drop(connector);

        // Offline: the persistent session queues these for the same client id.
        publish(&format!("{prefix}/json/a"), numbered(5..20));
        let connector = MqttConnector::connect(config(protocol, &client_id, &prefix)).unwrap();
        let (_, second) = drain(connector, &state);
        assert_eq!(payloads(&second), numbered(5..20), "{protocol:?}");
        assert!(second.iter().all(|record| record.kind == "reading"));
        assert_eq!(second[0].position, first[5].position + 1);
    }
}

/// Sink that kills the process right after the inner sink acknowledged the
/// `abort_at`-th record, before the runtime commits it or the broker hears an ack.
struct AbortAfterAck {
    inner: JsonLinesSink,
    seen: usize,
    abort_at: usize,
}

impl Sink for AbortAfterAck {
    fn deliver(&mut self, record: &Record, idk: &IdempotencyKey) -> Result<(), DeliverError> {
        self.inner.deliver(record, idk)?;
        self.seen += 1;
        if self.seen == self.abort_at {
            std::process::abort();
        }
        Ok(())
    }
}

#[test]
#[ignore = "child process of kill_between_sink_ack_and_broker_ack_redelivers"]
fn kill_child() {
    let Ok(dir) = std::env::var("HASHI_MQTT_KILL_DIR") else {
        return;
    };
    let dir = PathBuf::from(dir);
    let prefix = std::env::var("HASHI_MQTT_KILL_PREFIX").unwrap();
    let client_id = std::env::var("HASHI_MQTT_KILL_CLIENT").unwrap();
    let connector = MqttConnector::connect(config(Protocol::V311, &client_id, &prefix)).unwrap();
    std::fs::write(dir.join("subscribed"), b"").unwrap();
    while !dir.join("published").exists() {
        std::thread::sleep(Duration::from_millis(20));
    }
    let mut runtime = Runtime::new(
        connector,
        AbortAfterAck {
            inner: JsonLinesSink::open(dir.join("out")).unwrap(),
            seen: 0,
            abort_at: 4,
        },
        FileCheckpoint::open(dir.join("state")).unwrap(),
        SystemClock,
        RuntimeConfig::new("line-a"),
    );
    runtime.run().unwrap();
    panic!("the sink should have killed the process");
}

#[test]
#[ignore = "requires the disposable broker created by scripts/test-mqtt-container.sh"]
fn kill_between_sink_ack_and_broker_ack_redelivers() {
    let dir = temp_dir("hashi-mqtt-kill");
    let prefix = unique("hashi/kill");
    let client_id = unique("kill");
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "kill_child", "--ignored", "--nocapture"])
        .env("HASHI_MQTT_KILL_DIR", &dir)
        .env("HASHI_MQTT_KILL_PREFIX", &prefix)
        .env("HASHI_MQTT_KILL_CLIENT", &client_id)
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while !dir.join("subscribed").exists() {
        assert!(
            std::time::Instant::now() < deadline,
            "child never subscribed"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    let sent = numbered(0..10);
    publish(&format!("{prefix}/json/a"), sent.clone());
    std::fs::write(dir.join("published"), b"").unwrap();
    let status = child.wait().unwrap();
    assert!(!status.success(), "the child must die inside the sink");

    let connector = MqttConnector::connect(config(Protocol::V311, &client_id, &prefix)).unwrap();
    let mut runtime = Runtime::new(
        connector,
        JsonLinesSink::open(dir.join("out")).unwrap(),
        FileCheckpoint::open(dir.join("state")).unwrap(),
        SystemClock,
        RuntimeConfig::new("line-a"),
    );
    runtime.run().unwrap();
    let delivered: Vec<String> = JsonLinesSink::open(dir.join("out"))
        .unwrap()
        .replay()
        .unwrap()
        .into_iter()
        .map(|(record, _)| record.payload)
        .collect();
    let distinct: BTreeSet<&String> = delivered.iter().collect();
    assert_eq!(distinct, sent.iter().collect(), "no message may be lost");
    let killed = &sent[3];
    assert!(
        delivered.iter().filter(|p| *p == killed).count() >= 2,
        "the record acknowledged by the sink but not by the broker is redelivered"
    );
}

#[test]
#[ignore = "requires the disposable broker created by scripts/test-mqtt-container.sh"]
fn two_instances_on_a_shared_subscription_split_the_load_without_loss() {
    let prefix = unique("hashi/shared");
    let group = unique("group");
    let instances: Vec<_> = (0..2)
        .map(|_| {
            let mut config = config(Protocol::V5, &unique("shared"), &prefix);
            config.shared_group = Some(group.clone());
            MqttConnector::connect(config).unwrap()
        })
        .collect();
    let sent = numbered(0..200);
    let workers: Vec<_> = instances
        .into_iter()
        .map(|connector| {
            let state = temp_dir("hashi-mqtt-shared");
            std::thread::spawn(move || drain(connector, &state).1)
        })
        .collect();
    publish(&format!("{prefix}/json/a"), sent.clone());
    let split: Vec<Vec<String>> = workers
        .into_iter()
        .map(|worker| payloads(&worker.join().unwrap()))
        .collect();
    assert!(
        split.iter().all(|part| !part.is_empty()),
        "both instances must receive a share: {:?}",
        split.iter().map(Vec::len).collect::<Vec<_>>()
    );
    let union: BTreeSet<&String> = split.iter().flatten().collect();
    assert_eq!(union, sent.iter().collect());
}

#[test]
#[ignore = "requires the disposable broker created by scripts/test-mqtt-container.sh"]
fn tls_requires_a_trusted_client_certificate() {
    let certs = PathBuf::from(std::env::var("HASHI_MQTT_CERTS").unwrap());
    let prefix = unique("hashi/tls");
    let tls = |client_certificate| {
        let mut config = config(Protocol::V5, &unique("tls"), &prefix);
        config.host = "localhost".into();
        config.port = port("HASHI_MQTT_TLS_PORT");
        config.allow_insecure_lab = false;
        config.transport = Transport::Tls {
            ca_file: certs.join("ca.pem"),
            client_certificate,
        };
        config
    };
    assert!(MqttConnector::connect(tls(None)).is_err());
    let connector = MqttConnector::connect(tls(Some((
        certs.join("client.pem"),
        certs.join("client.key"),
    ))))
    .unwrap();
    assert!(connector.check().unwrap().ok);
    publish(&format!("{prefix}/json/a"), numbered(0..3));
    let (_, records) = drain(connector, &temp_dir("hashi-mqtt-tls"));
    assert_eq!(payloads(&records), numbered(0..3));
}
