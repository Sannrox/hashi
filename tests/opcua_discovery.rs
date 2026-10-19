#![cfg(feature = "opcua-simulator")]
#[path = "../examples/support/opcua_simulator.rs"]
mod simulator;
use hashi::Connector;
use hashi::opcua::{Authentication, Config, OpcUaConnector, Policy};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
static SEQ: AtomicU64 = AtomicU64::new(0);
fn dir() -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!(
        "hashi-opcua-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&path).unwrap();
    path
}
fn config(server: &simulator::Simulator, pki: std::path::PathBuf, policy: Policy) -> Config {
    Config {
        id: "fixture".into(),
        endpoint: server.endpoint.clone(),
        application_uri: simulator::CLIENT_URI.into(),
        pki_dir: pki,
        policy,
        allow_insecure_lab: false,
        authentication: Authentication::UserName {
            user: "reader".into(),
            password: "fixture-password".into(),
        },
        roots: vec![opcua::types::ExpandedNodeId {
            node_id: {
                let mut root = server.root.clone();
                root.namespace = 0;
                root
            },
            namespace_uri: "urn:hashi:fixture".into(),
            server_index: 0,
        }],
        max_nodes: 100,
        timeout: Duration::from_secs(3),
        schema_version: 1,
    }
}
#[test]
fn secure_sessions_reject_untrusted_certs_and_discover_typed_nodes_after_explicit_trust() {
    let path = dir();
    let server_pki = path.join("server");
    let client_pki = path.join("client");
    let user_pki = path.join("user");
    simulator::certificate(&server_pki, simulator::SERVER_URI);
    simulator::certificate(&client_pki, simulator::CLIENT_URI);
    simulator::certificate(&user_pki, "urn:hashi:user");
    std::fs::copy(user_pki.join("own/cert.der"), server_pki.join("user.der")).unwrap();
    simulator::trust(&server_pki, &client_pki.join("own/cert.der"));
    let server = simulator::Simulator::start(&server_pki, "127.0.0.1", 0);
    let rejection =
        OpcUaConnector::connect(config(&server, client_pki.clone(), Policy::Basic256Sha256))
            .err()
            .unwrap();
    assert!(rejection.message.contains("secure session failed"));
    let rejected = std::fs::read_dir(client_pki.join("rejected"))
        .unwrap()
        .count();
    assert!(
        rejected > 0,
        "untrusted certificates must remain reviewable"
    );
    simulator::trust(&client_pki, &server_pki.join("own/cert.der"));
    // Explicit trust approval moves the rejected certificate out of the deny directory.
    for item in std::fs::read_dir(client_pki.join("rejected")).unwrap() {
        std::fs::remove_file(item.unwrap().path()).unwrap();
    }
    for policy in [
        Policy::Basic256Sha256,
        Policy::Aes128Sha256RsaOaep,
        Policy::Aes256Sha256RsaPss,
    ] {
        let source = OpcUaConnector::connect(config(&server, client_pki.clone(), policy)).unwrap();
        assert!(source.check().unwrap().ok);
        let nodes = source.discover().unwrap();
        let node = nodes
            .iter()
            .find(|n| n.kind == server.value.to_string())
            .unwrap();
        assert_eq!(node.metadata["engineering_units"], "C");
        assert!(node.metadata["browse_path"].contains("Temperature"));
        assert_eq!(node.metadata["node_id"], server.value.to_string());
        assert_eq!(node.schema.fields["value"], hashi::schema::FieldType::F64);
    }
    let mut lab = config(&server, client_pki.clone(), Policy::None);
    assert!(OpcUaConnector::connect(lab).is_err());
    lab = config(&server, client_pki.clone(), Policy::None);
    lab.allow_insecure_lab = true;
    let source = OpcUaConnector::connect(lab).unwrap();
    assert!(source.check().unwrap().ok);
    drop(source);
    let mut x509 = config(&server, client_pki, Policy::Basic256Sha256);
    x509.authentication = Authentication::X509 {
        certificate: user_pki.join("own/cert.der"),
        private_key: user_pki.join("private/private.pem"),
    };
    let source = OpcUaConnector::connect(x509).unwrap();
    assert!(source.check().unwrap().ok);
    drop(source);
    let mut bounded = config(&server, path.join("client"), Policy::Basic256Sha256);
    bounded.max_nodes = 1;
    let source = OpcUaConnector::connect(bounded).unwrap();
    assert!(source.discover().unwrap_err().message.contains("limit"));
    assert!(
        !source.check().unwrap().ok,
        "failed discovery closes its session and releases continuation resources"
    );
    drop(source);
    let mut variable_root = config(&server, path.join("client"), Policy::Basic256Sha256);
    variable_root.roots = vec![server.value.clone().into()];
    let source = OpcUaConnector::connect(variable_root).unwrap();
    assert!(
        source
            .discover()
            .unwrap_err()
            .message
            .contains("Object containers")
    );
    drop(source);
    drop(server);
    std::fs::remove_dir_all(path).unwrap();
}
