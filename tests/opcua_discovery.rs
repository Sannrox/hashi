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

#[test]
fn operator_discover_is_refused_until_trust_approve_and_revoke_denies_again() {
    let path = dir();
    let server_pki = path.join("server");
    let client_pki = path.join("client");
    let user_pki = path.join("user");
    simulator::certificate(&server_pki, simulator::SERVER_URI);
    simulator::certificate(&client_pki, simulator::CLIENT_URI);
    simulator::certificate(&user_pki, "urn:hashi:user");
    std::fs::copy(user_pki.join("own/cert.der"), server_pki.join("user.der")).unwrap();
    std::fs::create_dir_all(path.join("secrets")).unwrap();
    std::fs::write(path.join("secrets/opcua"), "reader\nfixture-password").unwrap();
    simulator::trust(&server_pki, &client_pki.join("own/cert.der"));
    let server = simulator::Simulator::start(&server_pki, "127.0.0.1", 0);
    let config_path = path.join("hashi.toml");
    std::fs::write(
        &config_path,
        format!(
            "checkpoint_dir = \"state\"\n\n[[instance]]\nname = \"line-1\"\n\
             source.opcua = {{ endpoint = \"{}\", application_uri = \"{}\", pki_dir = \"client\", \
             policy = \"basic256sha256\", credential_file = \"secrets/opcua\", \
             roots = [\"nsu=urn:hashi:fixture;s=Equipment\"], max_nodes = 100, timeout_ms = 3000, schema_version = 1 }}\n\
             sink.jsonl = {{ dir = \"out\" }}\n",
            server.endpoint, simulator::CLIENT_URI
        ),
    )
    .unwrap();
    let config = hashi::Config::load(&config_path).unwrap();
    let hashi::DiscoverOutcome::Untrusted(untrusted) = hashi::discover(&config, None).unwrap()
    else {
        panic!("untrusted server must not yield an address space");
    };
    assert_eq!(untrusted.error, "untrusted_certificate");
    assert!(!untrusted.rejected.is_empty());
    assert!(
        !serde_json::to_string(&untrusted)
            .unwrap()
            .contains("BEGIN "),
        "discover JSON must not carry PEM"
    );
    let sha256 = untrusted.rejected[0].sha256.clone();
    hashi::trust_approve(&config, None, &sha256).unwrap();
    let hashi::DiscoverOutcome::Ok(found) = hashi::discover(&config, None).unwrap() else {
        panic!("discover must succeed after approve");
    };
    assert_eq!(found.schema_version, 1);
    assert_eq!(found.source, "opcua");
    assert!(found.kinds.iter().any(|kind| {
        kind.kind.contains("Temperature")
            || kind
                .metadata
                .get("browse_path")
                .is_some_and(|path| path.contains("Temperature"))
    }));
    std::fs::write(path.join("secrets/opcua"), "reader\nwrong-password").unwrap();
    match hashi::discover(&config, None) {
        Err(err) => {
            assert!(
                err.message.contains("secure session failed"),
                "{}",
                err.message
            );
            assert!(
                !err.message.contains("untrusted_certificate"),
                "{}",
                err.message
            );
        }
        Ok(_) => panic!("trusted server with a bad password is a session error"),
    }
    std::fs::write(path.join("secrets/opcua"), "reader\nfixture-password").unwrap();
    hashi::trust_revoke(&config, None, &sha256).unwrap();
    let hashi::DiscoverOutcome::Untrusted(_) = hashi::discover(&config, None).unwrap() else {
        panic!("revoke must deny discover again");
    };
    drop(server);
    std::fs::remove_dir_all(path).unwrap();
}
