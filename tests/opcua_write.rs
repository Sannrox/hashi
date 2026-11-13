#![cfg(feature = "opcua-simulator")]
#[path = "../examples/support/opcua_simulator.rs"]
mod simulator;
use hashi::opcua::{Authentication, Config as SessionConfig, OpcUaConnector, Policy};
use hashi::write::{Command, CommandFile, Outcome, Target};
use opcua::types::Variant;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

static SEQ: AtomicU64 = AtomicU64::new(0);

fn fixture() -> (PathBuf, simulator::Simulator) {
    let path = std::env::temp_dir().join(format!(
        "hashi-write-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let server_pki = path.join("server");
    let client_pki = path.join("client");
    simulator::certificate(&server_pki, simulator::SERVER_URI);
    simulator::certificate(&client_pki, simulator::CLIENT_URI);
    simulator::trust(&server_pki, &client_pki.join("own/cert.der"));
    simulator::trust(&client_pki, &server_pki.join("own/cert.der"));
    std::fs::write(path.join("writer-credential"), "reader\nfixture-password\n").unwrap();
    let server = simulator::Simulator::start(&server_pki, "127.0.0.1", 0);
    (path, server)
}

fn config(path: &Path, server: &simulator::Simulator) -> hashi::Config {
    let text = format!(
        r#"checkpoint_dir = "state"

[[instance]]
name = "line-1"
source.fixture = {{ records = 1, credential_file = "unused", accept_sha256 = ["{digest}"] }}
sink.jsonl = {{ dir = "out" }}

[[writer]]
name = "setpoints"
target.opcua = {{ endpoint = "{endpoint}", application_uri = "{uri}", pki_dir = "client", policy = "basic256sha256", credential_file = "writer-credential", timeout_ms = 10000, allow = ["{setpoint}", "{temperature}"] }}
"#,
        digest = "0".repeat(64),
        endpoint = server.endpoint,
        uri = simulator::CLIENT_URI,
        setpoint = server.setpoint(),
        temperature = server.value,
    );
    hashi::Config::parse(&text, path).unwrap()
}

fn commands(list: &[(&str, String, serde_json::Value)]) -> CommandFile {
    CommandFile {
        version: 1,
        commands: list
            .iter()
            .map(|(id, target, value)| Command {
                id: (*id).into(),
                target: target.clone(),
                value: value.clone(),
            })
            .collect(),
    }
}

#[test]
fn an_allowed_write_is_applied_once_and_refusals_say_why() {
    let (path, server) = fixture();
    let config = config(&path, &server);
    let setpoint = server.setpoint().to_string();
    let first = commands(&[
        ("c1", setpoint.clone(), 42.5.into()),
        // Allowed by configuration, read-only on the server.
        ("c2", server.value.to_string(), 1.0.into()),
        ("c3", "ns=2;s=NotListed".into(), 1.0.into()),
    ]);
    let results = hashi::write::run(&config, "setpoints", &first).unwrap();
    assert_eq!(results.results[0].outcome, Outcome::Applied);
    assert!(
        matches!(&results.results[1].outcome, Outcome::Refused { reason } if reason.contains("BadNotWritable") || reason.contains("BadUserAccessDenied")),
        "{:?}",
        results.results[1]
    );
    assert!(matches!(
        &results.results[2].outcome,
        Outcome::Refused { reason } if reason.contains("allow list")
    ));
    assert_eq!(server.read(&server.setpoint()), Some(Variant::Double(42.5)));

    let moved = commands(&[("c4", setpoint.clone(), 7.0.into())]);
    hashi::write::run(&config, "setpoints", &moved).unwrap();
    // Rerunning c1 answers from the ledger; a resend would restore 42.5.
    let again = hashi::write::run(&config, "setpoints", &first).unwrap();
    assert_eq!(again.results[0].outcome, Outcome::Applied);
    assert_eq!(server.read(&server.setpoint()), Some(Variant::Double(7.0)));
}

#[test]
fn losing_the_session_before_the_response_reports_unknown() {
    let (path, server) = fixture();
    let mut session = OpcUaConnector::connect(SessionConfig {
        id: "writer".into(),
        endpoint: server.endpoint.clone(),
        application_uri: simulator::CLIENT_URI.into(),
        pki_dir: path.join("client"),
        policy: Policy::Basic256Sha256,
        allow_insecure_lab: false,
        authentication: Authentication::UserName {
            user: "reader".into(),
            password: "fixture-password".into(),
        },
        roots: vec![server.setpoint().into()],
        max_nodes: 1,
        timeout: Duration::from_secs(2),
        schema_version: 1,
    })
    .unwrap();
    let prepared = session
        .prepare(&Command {
            id: "c1".into(),
            target: server.setpoint().to_string(),
            value: 3.0.into(),
        })
        .unwrap();
    drop(server);
    assert!(matches!(session.send(prepared), Outcome::Unknown { .. }));
    // The transport was stopped, so nothing queued can leave later and the
    // session refuses further commands.
    assert!(
        session
            .prepare(&Command {
                id: "c2".into(),
                target: "ns=2;s=Setpoint".into(),
                value: 4.0.into(),
            })
            .is_err()
    );
}

#[test]
fn trust_commands_select_the_writer_store_by_name() {
    let (path, server) = fixture();
    let store = hashi::trust_list(&config(&path, &server), Some("setpoints")).unwrap();
    assert_eq!(store.instance, "setpoints");
    assert_eq!(store.trusted.len(), 1);
}
