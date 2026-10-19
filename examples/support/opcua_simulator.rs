use opcua::crypto::SecurityPolicy;
use opcua::crypto::{CertificateStore, X509Data};
use opcua::nodes::AccessLevel;
use opcua::server::{
    ServerBuilder, ServerHandle, ServerUserToken,
    address_space::VariableBuilder,
    diagnostics::NamespaceMetadata,
    node_manager::memory::{SimpleNodeManager, simple_node_manager},
};
use opcua::types::{
    DataTypeId, EUInformation, ExtensionObject, MessageSecurityMode, NodeId, ObjectId, Variant,
};
use std::path::Path;
use std::time::Duration;

pub const SERVER_URI: &str = "urn:hashi:simulator";
pub const CLIENT_URI: &str = "urn:hashi:fixture-client";

pub fn certificate(dir: &Path, uri: &str) {
    std::fs::create_dir_all(dir.join("own")).unwrap();
    std::fs::create_dir_all(dir.join("private")).unwrap();
    std::fs::create_dir_all(dir.join("trusted")).unwrap();
    let data = X509Data {
        key_size: 2048,
        common_name: "Hashi fixture".into(),
        organization: "Hashi".into(),
        organizational_unit: "Test".into(),
        country: "DE".into(),
        state: "Test".into(),
        alt_host_names: vec![uri.into(), "127.0.0.1".into()].into(),
        certificate_duration_days: 1,
    };
    CertificateStore::create_certificate_and_key(
        &data,
        true,
        &dir.join("own/cert.der"),
        &dir.join("private/private.pem"),
    )
    .unwrap();
}
pub fn trust(dir: &Path, certificate: &Path) {
    let bytes = std::fs::read(certificate).unwrap();
    let cert = opcua::crypto::X509::from_der(&bytes).unwrap();
    std::fs::create_dir_all(dir.join("trusted")).unwrap();
    std::fs::write(
        dir.join("trusted")
            .join(CertificateStore::cert_file_name(&cert)),
        bytes,
    )
    .unwrap();
}
pub struct Simulator {
    pub endpoint: String,
    pub root: NodeId,
    pub value: NodeId,
    handle: ServerHandle,
    _runtime: tokio::runtime::Runtime,
}
impl Simulator {
    pub fn start(dir: &Path, host: &str, port: u16) -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let listener = runtime
            .block_on(tokio::net::TcpListener::bind((host, port)))
            .unwrap();
        let port = listener.local_addr().unwrap().port();
        let endpoint = format!("opc.tcp://127.0.0.1:{port}/");
        let entered = runtime.enter();
        let mut builder = ServerBuilder::new()
            .application_name("Hashi simulator")
            .application_uri(SERVER_URI)
            .product_uri("urn:hashi:simulator-product")
            .pki_dir(dir)
            .certificate_path("own/cert.der")
            .private_key_path("private/private.pem")
            .create_sample_keypair(false)
            .trust_client_certs(false)
            .host("127.0.0.1")
            .port(port)
            .discovery_urls(vec![endpoint.clone()])
            .add_user_token(
                "user",
                ServerUserToken::user_pass("reader", "fixture-password"),
            )
            .add_user_token(
                "cert",
                ServerUserToken::x509("certificate-reader", &dir.join("user.der")),
            )
            .with_node_manager(simple_node_manager(
                NamespaceMetadata {
                    namespace_uri: "urn:hashi:fixture".into(),
                    ..Default::default()
                },
                "fixture",
            ));
        for (name, policy, mode) in [
            (
                "basic",
                SecurityPolicy::Basic256Sha256,
                MessageSecurityMode::SignAndEncrypt,
            ),
            (
                "aes128",
                SecurityPolicy::Aes128Sha256RsaOaep,
                MessageSecurityMode::SignAndEncrypt,
            ),
            (
                "aes256",
                SecurityPolicy::Aes256Sha256RsaPss,
                MessageSecurityMode::SignAndEncrypt,
            ),
            ("lab", SecurityPolicy::None, MessageSecurityMode::None),
        ] {
            builder = builder.add_endpoint(name, ("/", policy, mode, &["user", "cert"] as &[&str]));
        }
        let (server, handle) = builder.build().unwrap();
        let ns = handle.get_namespace_index("urn:hashi:fixture").unwrap();
        let root = NodeId::new(ns, "Equipment");
        let value = NodeId::new(ns, "Temperature");
        let manager = handle
            .node_managers()
            .get_of_type::<SimpleNodeManager>()
            .unwrap();
        {
            let mut space = manager.address_space().write();
            assert!(space.add_folder(
                &root,
                opcua::types::QualifiedName::new(ns, "Equipment"),
                "Equipment",
                &ObjectId::ObjectsFolder.into()
            ));
            assert!(
                VariableBuilder::new(
                    &value,
                    opcua::types::QualifiedName::new(ns, "Temperature"),
                    "Temperature"
                )
                .data_type(DataTypeId::Double)
                .value(21.5f64)
                .access_level(AccessLevel::CURRENT_READ)
                .user_access_level(AccessLevel::CURRENT_READ)
                .organized_by(root.clone())
                .insert(&mut *space)
            );
            assert!(
                VariableBuilder::new(
                    &NodeId::new(ns, "Units"),
                    opcua::types::QualifiedName::new(0, "EngineeringUnits"),
                    "EngineeringUnits"
                )
                .data_type(DataTypeId::EUInformation)
                .value(Variant::ExtensionObject(ExtensionObject::from_message(
                    EUInformation {
                        namespace_uri: "urn:hashi:units".into(),
                        unit_id: 1,
                        display_name: "C".into(),
                        description: "temperature".into()
                    }
                )))
                .access_level(AccessLevel::CURRENT_READ)
                .user_access_level(AccessLevel::CURRENT_READ)
                .property_of(value.clone())
                .insert(&mut *space)
            );
        }
        {
            let mut space = manager.address_space().write();
            // These are ordinary application nodes, not the standard unit property.
            assert!(
                VariableBuilder::new(
                    &NodeId::new(ns, "ApplicationUnits"),
                    opcua::types::QualifiedName::new(ns, "EngineeringUnits"),
                    "Application units"
                )
                .data_type(DataTypeId::String)
                .value("custom")
                .access_level(AccessLevel::CURRENT_READ)
                .property_of(value.clone())
                .insert(&mut *space)
            );
            assert!(
                VariableBuilder::new(
                    &NodeId::new(ns, "ComponentUnits"),
                    opcua::types::QualifiedName::new(0, "EngineeringUnits"),
                    "Component units"
                )
                .data_type(DataTypeId::String)
                .value("component")
                .access_level(AccessLevel::CURRENT_READ)
                .component_of(value.clone())
                .insert(&mut *space)
            );
        }
        runtime.spawn(server.run_with(listener));
        drop(entered);
        Self {
            endpoint,
            root,
            value,
            handle,
            _runtime: runtime,
        }
    }
}
impl Drop for Simulator {
    fn drop(&mut self) {
        self.handle.cancel();
        std::thread::sleep(Duration::from_millis(20));
    }
}
