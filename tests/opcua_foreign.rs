//! Browse, subscribe, and raw Historical Access against a server this
//! repository does not compile. Off the default `cargo test` path; the
//! container harness sets `HASHI_OPCUA_FOREIGN=1`.
#![cfg(feature = "opcua-simulator")]
#[allow(dead_code)]
#[path = "../examples/support/opcua_simulator.rs"]
mod simulator;

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use hashi::historian::{Historian, HistorianConfig, HistorianConnector};
use hashi::opcua::historian::OpcUaHistorian;
use hashi::opcua::{Authentication, Config, OpcUaConnector, Policy, SubscriptionConfig};
use hashi::{Connector, DiscoverOutcome};
use opcua::types::NodeId;

const START_MS: i64 = 1_700_000_000_000;
const SERVER_URI: &str = "urn:hashi:foreign-server";
const NS_URI: &str = "urn:hashi:foreign";

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "hashi-opcua-foreign-{name}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&path).unwrap();
    path
}

fn script() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/opcua/foreign_server.py")
}

fn python() -> Command {
    let exe = std::env::var("HASHI_OPCUA_FOREIGN_PYTHON").unwrap_or_else(|_| "python3".into());
    Command::new(exe)
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn pki(path: &Path) {
    for dir in [
        path.join("server/own"),
        path.join("server/private"),
        path.join("server/trusted-clients"),
        path.join("client/own"),
        path.join("client/private"),
    ] {
        std::fs::create_dir_all(dir).unwrap();
    }
}

fn root() -> opcua::types::ExpandedNodeId {
    opcua::types::ExpandedNodeId {
        node_id: NodeId::new(0, "Equipment"),
        namespace_uri: NS_URI.into(),
        server_index: 0,
    }
}

fn config(endpoint: &str, pki: &Path) -> Config {
    Config {
        id: "foreign".into(),
        endpoint: endpoint.into(),
        application_uri: simulator::CLIENT_URI.into(),
        pki_dir: pki.into(),
        policy: Policy::Basic256Sha256,
        allow_insecure_lab: false,
        authentication: Authentication::UserName {
            user: "reader".into(),
            password: "fixture-password".into(),
        },
        roots: vec![root()],
        max_nodes: 100,
        timeout: Duration::from_secs(15),
        schema_version: 1,
    }
}

struct Foreign {
    child: Child,
    endpoint: String,
    port: u16,
    dir: PathBuf,
}

impl Foreign {
    fn start(dir: PathBuf, extra_ns: bool) -> Self {
        Self::start_on(dir, free_port(), extra_ns)
    }

    fn start_on(dir: PathBuf, port: u16, extra_ns: bool) -> Self {
        let server_pki = dir.join("server");
        let mut command = python();
        command
            .arg(script())
            .env("HASHI_OPCUA_FOREIGN_PORT", port.to_string())
            .env("HASHI_OPCUA_FOREIGN_CERT", server_pki.join("own/cert.der"))
            .env(
                "HASHI_OPCUA_FOREIGN_KEY",
                server_pki.join("private/private.pem"),
            )
            .env(
                "HASHI_OPCUA_FOREIGN_TRUST_DIR",
                server_pki.join("trusted-clients"),
            )
            .env(
                "HASHI_OPCUA_FOREIGN_CLIENT_CERT",
                dir.join("client/own/cert.der"),
            )
            .env(
                "HASHI_OPCUA_FOREIGN_CLIENT_KEY",
                dir.join("client/private/private.pem"),
            )
            .env("HASHI_OPCUA_FOREIGN_CLIENT_URI", simulator::CLIENT_URI)
            .env("HASHI_OPCUA_FOREIGN_APP_URI", SERVER_URI)
            .env("HASHI_OPCUA_FOREIGN_PAGE", "2")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if extra_ns {
            command.env("HASHI_OPCUA_FOREIGN_EXTRA_NS", "1");
        }
        let mut child = command.spawn().unwrap_or_else(|error| {
            panic!("start foreign OPC UA server: {error}");
        });
        let stdout = child.stdout.take().expect("foreign server stdout");
        let stderr = child.stderr.take().expect("foreign server stderr");
        std::thread::spawn(move || for _ in BufReader::new(stderr).lines() {});
        let ready = std::thread::spawn(move || {
            BufReader::new(stdout)
                .lines()
                .map_while(Result::ok)
                .find(|line| line.starts_with("READY "))
        });
        let line = match ready.join() {
            Ok(Some(line)) => line,
            Ok(None) => {
                let status = child.wait().ok();
                panic!("foreign OPC UA server exited before ready ({status:?})")
            }
            Err(_) => panic!("foreign OPC UA server ready thread panicked"),
        };
        assert!(
            line.contains(&port.to_string()),
            "ready line {line} does not name port {port}"
        );
        Self {
            child,
            endpoint: format!("opc.tcp://127.0.0.1:{port}/"),
            port,
            dir,
        }
    }

    fn restart(mut self, extra_ns: bool) -> Self {
        let port = self.port;
        let dir = self.dir.clone();
        self.stop();
        Self::start_on(dir, port, extra_ns)
    }

    fn stop(&mut self) {
        drop(self.child.stdin.take());
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            match self.child.try_wait() {
                Ok(Some(_)) => return,
                Ok(None) if std::time::Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(20));
                }
                _ => {
                    let _ = self.child.kill();
                    let _ = self.child.wait();
                    return;
                }
            }
        }
    }
}

impl Drop for Foreign {
    fn drop(&mut self) {
        self.stop();
    }
}

fn require_foreign() {
    assert_eq!(
        std::env::var("HASHI_OPCUA_FOREIGN").as_deref(),
        Ok("1"),
        "set HASHI_OPCUA_FOREIGN=1 to run this fixture"
    );
}

/// One Publish can be a keepalive with no notification. Poll until a later sample.
fn next_record(
    source: &mut impl Connector,
    after: hashi::contract::Position,
) -> Option<hashi::contract::Record> {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        match source.read(Some(after), None).unwrap() {
            Some(record) if record.position > after => return Some(record),
            _ => {}
        }
    }
    None
}

fn temperature(kinds: &[hashi::contract::DiscoveredKind]) -> &hashi::contract::DiscoveredKind {
    kinds
        .iter()
        .find(|kind| {
            kind.metadata
                .get("browse_path")
                .is_some_and(|path| path.contains("Temperature"))
                || kind.kind.contains("Temperature")
        })
        .expect("Temperature node")
}

#[test]
#[ignore]
fn untrusted_certificate_is_refused_until_trust_approve() {
    require_foreign();
    let path = scratch("trust");
    pki(&path);
    std::fs::create_dir_all(path.join("secrets")).unwrap();
    let server = Foreign::start(path.clone(), false);
    let client = path.join("client");
    let rejection = OpcUaConnector::connect(config(&server.endpoint, &client))
        .err()
        .expect("untrusted server");
    assert!(
        rejection.message.contains("secure session failed"),
        "{}",
        rejection.message
    );
    let rejected: Vec<_> = std::fs::read_dir(client.join("rejected"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "der"))
        .collect();
    assert!(
        !rejected.is_empty(),
        "untrusted server certificate must land in rejected/"
    );
    let rejected_bytes = std::fs::read(&rejected[0]).unwrap();
    assert!(
        !rejected_bytes.starts_with(b"-----BEGIN "),
        "rejected store must keep DER, not PEM"
    );

    std::fs::write(path.join("secrets/opcua"), "reader\nfixture-password").unwrap();
    let config_path = path.join("hashi.toml");
    std::fs::write(
        &config_path,
        format!(
            "checkpoint_dir = \"state\"\n\n[[instance]]\nname = \"line-1\"\n\
             source.opcua = {{ endpoint = \"{}\", application_uri = \"{}\", pki_dir = \"client\", \
             policy = \"basic256sha256\", credential_file = \"secrets/opcua\", \
             roots = [\"nsu={NS_URI};s=Equipment\"], max_nodes = 100, timeout_ms = 15000, schema_version = 1 }}\n\
             sink.jsonl = {{ dir = \"out\" }}\n",
            server.endpoint, simulator::CLIENT_URI
        ),
    )
    .unwrap();
    let loaded = hashi::Config::load(&config_path).unwrap();
    let DiscoverOutcome::Untrusted(untrusted) = hashi::discover(&loaded, None).unwrap() else {
        panic!("untrusted server must not yield an address space");
    };
    assert_eq!(untrusted.error, "untrusted_certificate");
    let json = serde_json::to_string(&untrusted).unwrap();
    assert!(!json.contains("BEGIN "), "{json}");
    let sha256 = untrusted.rejected[0].sha256.clone();

    let mut none = config(&server.endpoint, &client);
    none.policy = Policy::None;
    let none_err = OpcUaConnector::connect(none).err().expect("policy None");
    assert!(
        none_err
            .message
            .contains("OPC UA policy None requires allow_insecure_lab"),
        "{}",
        none_err.message
    );

    hashi::trust_approve(&loaded, None, &sha256).unwrap();
    let source = OpcUaConnector::connect(config(&server.endpoint, &client)).unwrap();
    assert!(source.check().unwrap().ok);
    let kinds = source.discover().unwrap();
    let node = temperature(&kinds);
    assert_eq!(node.metadata["engineering_units"], "C");
    assert!(node.metadata["browse_path"].contains("Temperature"));
    assert_eq!(node.schema.fields["value"], hashi::schema::FieldType::F64);
    assert!(kinds.iter().any(|kind| {
        kind.metadata
            .get("browse_path")
            .is_some_and(|path| path.contains("NoHistory"))
    }));
    drop(source);

    let mut variable_root = config(&server.endpoint, &client);
    variable_root.roots = vec![opcua::types::ExpandedNodeId {
        node_id: NodeId::new(0, "Temperature"),
        namespace_uri: NS_URI.into(),
        server_index: 0,
    }];
    let source = OpcUaConnector::connect(variable_root).unwrap();
    assert!(
        source
            .discover()
            .unwrap_err()
            .message
            .contains("Object containers")
    );
    drop(source);

    let trusted = std::fs::read_dir(client.join("trusted"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    std::fs::copy(&trusted, client.join("rejected/duplicate.der")).unwrap();
    assert!(
        OpcUaConnector::connect(config(&server.endpoint, &client)).is_err(),
        "a copy left in rejected/ must keep denying"
    );
}

#[test]
#[ignore]
fn subscribe_recreates_after_the_foreign_server_restarts() {
    require_foreign();
    let path = scratch("subscribe");
    pki(&path);
    let server = Foreign::start(path.clone(), false);
    simulator::trust(&path.join("client"), &path.join("server/own/cert.der"));
    let client = path.join("client");
    let source = OpcUaConnector::connect(config(&server.endpoint, &client)).unwrap();
    let kinds = source.discover().unwrap();
    let node: NodeId = temperature(&kinds).kind.parse().unwrap();
    drop(source);

    let mut source = OpcUaConnector::subscribe(
        config(&server.endpoint, &client),
        SubscriptionConfig {
            nodes: vec![node.clone()],
            state_dir: path.join("state"),
            publishing_interval: Duration::from_millis(100),
            sampling_interval: Duration::from_millis(50),
            deadband: None,
            queue_size: 10,
            max_pending_bytes: 1024 * 1024,
        },
    )
    .unwrap();
    let first = source.read(None, None).unwrap().unwrap();
    assert!(first.source_ts_ms > 0, "source timestamp is required");
    assert!(
        first
            .quality
            .as_ref()
            .is_some_and(|q| q.contains("Good") || q.contains("good")),
        "{:?}",
        first.quality
    );
    let value = serde_json::from_str::<serde_json::Value>(&first.payload).unwrap()["value"]
        .as_f64()
        .unwrap();
    let next =
        next_record(&mut source, first.position).expect("live sample after the initial value");
    let later = serde_json::from_str::<serde_json::Value>(&next.payload).unwrap()["value"]
        .as_f64()
        .unwrap();
    assert!(later >= value, "live values must advance or hold");
    let committed = next.position;
    let server = server.restart(false);
    let resumed =
        next_record(&mut source, committed).expect("recreated subscription after restart");
    assert!(resumed.position > committed);
    assert_eq!(resumed.kind, node.to_string());
    drop(source);
    drop(server);
}

#[test]
#[ignore]
fn historian_pages_raw_history_and_refuses_a_changed_namespace_table() {
    require_foreign();
    let path = scratch("history");
    pki(&path);
    let server = Foreign::start(path.clone(), false);
    simulator::trust(&path.join("client"), &path.join("server/own/cert.der"));
    let client = path.join("client");
    let mut historian = OpcUaHistorian::connect(config(&server.endpoint, &client)).unwrap();
    let without = historian.without_history();
    assert!(
        without.iter().any(|tag| tag.contains("NoHistory")),
        "{without:?}"
    );
    let tags = historian.tags().unwrap();
    let tag = tags
        .iter()
        .find(|tag| tag.name.contains("Temperature"))
        .unwrap()
        .name
        .clone();

    let page = historian
        .read_raw(&tag, START_MS, START_MS + 5_000, 2)
        .unwrap();
    assert_eq!(page.len(), 2, "{page:?}");
    assert!(page[0].ts_ms > START_MS);
    assert!(page[0].ts_ms < page[1].ts_ms);
    let rest = historian
        .read_raw(&tag, page[1].ts_ms, START_MS + 20_000, 20)
        .unwrap();
    assert!(
        rest.len() > 2,
        "continuation pages must yield the rest of the seeded range, got {}",
        rest.len()
    );
    let empty = historian
        .read_raw(&tag, START_MS + 60_000, START_MS + 70_000, 8)
        .unwrap();
    assert!(empty.is_empty(), "a range with no samples is an empty page");

    let same_ms = historian.read_raw(&tag, START_MS + 24_999, START_MS + 25_000, 2);
    assert!(
        same_ms
            .unwrap_err()
            .message
            .contains("more samples in one millisecond than a page holds"),
        "same-millisecond page must fail closed"
    );

    let now = std::rc::Rc::new(std::cell::Cell::new(START_MS + 30_000));
    let source = HistorianConnector::new(
        "historian",
        historian,
        FixtureClock(now.clone()),
        HistorianConfig {
            start_ms: START_MS,
            chunk_ms: 10_000,
            page_limit: 2,
            settle_ms: 0,
            max_reads_per_second: 0,
            state_dir: path.join("state"),
        },
    )
    .unwrap();
    let report = source.check().unwrap();
    assert!(report.ok);
    assert!(
        report.message.contains("without history"),
        "{}",
        report.message
    );
    drop(source);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let err = runtime
        .block_on(async { OpcUaConnector::connect(config(&server.endpoint, &client)).err() });
    assert!(
        err.expect("runtime-attached connect")
            .message
            .contains("call OPC UA synchronous API outside an async runtime")
    );

    let mut historian = OpcUaHistorian::connect(config(&server.endpoint, &client)).unwrap();
    let _server = server.restart(true);
    let first = historian.read_raw(&tag, START_MS, START_MS + 5_000, 2);
    let changed = match first {
        Err(error)
            if error
                .message
                .contains("OPC UA namespace table changed; rediscovery required") =>
        {
            Err(error)
        }
        _ => historian.read_raw(&tag, START_MS, START_MS + 5_000, 2),
    };
    assert!(
        changed
            .unwrap_err()
            .message
            .contains("OPC UA namespace table changed; rediscovery required"),
        "renumbered namespaces must fail closed"
    );
}

#[derive(Clone)]
struct FixtureClock(std::rc::Rc<std::cell::Cell<i64>>);

impl hashi::runtime::Clock for FixtureClock {
    fn now_ms(&self) -> i64 {
        self.0.get()
    }

    fn sleep_ms(&self, _ms: u64) {}
}
