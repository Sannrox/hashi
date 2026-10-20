//! Secure OPC UA sessions, bounded discovery, and durable subscription reads.
mod subscription;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
pub use subscription::SubscriptionConfig;

use crate::contract::{CheckReport, DiscoveredKind, Position, Record};
use crate::schema::{FieldType, Schema};
use crate::{Connector, Error};
use ::opcua::client::{ClientBuilder, IdentityToken, Session};
use ::opcua::crypto::SecurityPolicy;
use ::opcua::types::{
    AttributeId, BrowseDescription, BrowseDirection, BrowseResultMask, DataTypeId, EUInformation,
    ExpandedNodeId, MessageSecurityMode, NodeClass, NodeId, ReadValueId, ReferenceTypeId,
    StatusCode, TimestampsToReturn, Variant,
};

#[derive(Debug, Clone, Copy)]
pub enum Policy {
    Basic256Sha256,
    Aes128Sha256RsaOaep,
    Aes256Sha256RsaPss,
    None,
}
impl Policy {
    fn security(self) -> SecurityPolicy {
        match self {
            Self::Basic256Sha256 => SecurityPolicy::Basic256Sha256,
            Self::Aes128Sha256RsaOaep => SecurityPolicy::Aes128Sha256RsaOaep,
            Self::Aes256Sha256RsaPss => SecurityPolicy::Aes256Sha256RsaPss,
            Self::None => SecurityPolicy::None,
        }
    }
}
/// Never format credential contents into diagnostics.
pub enum Authentication {
    UserName {
        user: String,
        password: String,
    },
    X509 {
        certificate: PathBuf,
        private_key: PathBuf,
    },
}
pub struct Config {
    pub id: String,
    pub endpoint: String,
    pub application_uri: String,
    /// Explicit SDK PKI store; application certificate/key must already exist.
    pub pki_dir: PathBuf,
    pub policy: Policy,
    pub allow_insecure_lab: bool,
    pub authentication: Authentication,
    pub roots: Vec<ExpandedNodeId>,
    pub max_nodes: usize,
    pub timeout: Duration,
    pub schema_version: u64,
}

pub struct OpcUaConnector {
    id: String,
    session: Arc<Session>,
    event_loop: tokio::task::JoinHandle<StatusCode>,
    runtime: Option<tokio::runtime::Runtime>,
    roots: Vec<ExpandedNodeId>,
    max_nodes: usize,
    timeout: Duration,
    schema_version: u64,
    subscription: Option<subscription::Reader>,
    _pki_lease: std::fs::File,
}
impl OpcUaConnector {
    /// Synchronous embedding API. Use a dedicated OS thread when called by an async host.
    /// Runtime-attached calls are rejected before IO to keep nested runtimes out of this adapter.
    pub fn connect(config: Config) -> Result<Self, Error> {
        Self::connect_inner(config, false)
    }

    /// Starts a source with an explicit monitored-node set and a durable replay spool.
    pub fn subscribe(config: Config, subscription: SubscriptionConfig) -> Result<Self, Error> {
        subscription.validate()?;
        let mut connector = Self::connect_inner(config, true)?;
        let runtime = connector
            .runtime
            .as_ref()
            .ok_or_else(|| Error::new("OPC UA runtime closed"))?;
        let reader = runtime.block_on(subscription::Reader::open(&connector, subscription))?;
        connector.subscription = Some(reader);
        Ok(connector)
    }

    fn connect_inner(config: Config, reconnect: bool) -> Result<Self, Error> {
        if matches!(config.policy, Policy::None) && !config.allow_insecure_lab {
            return Err(Error::new("OPC UA policy None requires allow_insecure_lab"));
        }
        if config.id.trim().is_empty()
            || config.application_uri.trim().is_empty()
            || !config.endpoint.starts_with("opc.tcp://")
            || config.timeout.is_zero()
            || config.roots.is_empty()
            || config.roots.len() > config.max_nodes
            || config.max_nodes == 0
            || config.max_nodes > 10000
            || config.schema_version == 0
        {
            return Err(Error::new("invalid OPC UA configuration"));
        }
        let configured_endpoint = endpoint_url(&config.endpoint)?;
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err(Error::new(
                "call OPC UA synchronous API outside an async runtime",
            ));
        }
        if !config.pki_dir.join("own/cert.der").is_file()
            || !config.pki_dir.join("private/private.pem").is_file()
        {
            return Err(Error::new(
                "OPC UA application certificate and private key are required",
            ));
        }
        let lease = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(config.pki_dir.join("hashi.lease"))?;
        lease
            .try_lock()
            .map_err(|_| Error::new("OPC UA PKI store is already owned"))?;
        let token = match config.authentication {
            Authentication::UserName { user, password } => {
                IdentityToken::UserName(user, password.into())
            }
            Authentication::X509 {
                certificate,
                private_key,
            } => IdentityToken::new_x509_path(certificate, private_key)
                .map_err(|_| Error::new("invalid OPC UA user certificate or key"))?,
        };
        let mut client = ClientBuilder::new()
            .application_name("Hashi")
            .application_uri(config.application_uri)
            .pki_dir(config.pki_dir)
            .create_sample_keypair(false)
            .trust_server_certs(false)
            .verify_server_certs(true)
            .session_retry_limit(if reconnect { -1 } else { 0 })
            .recreate_subscriptions(false)
            .request_timeout(config.timeout)
            .max_array_length(10000)
            .max_string_length(65536)
            .max_byte_string_length(1024 * 1024)
            .max_message_size(1024 * 1024)
            .max_incoming_chunk_size(65536)
            .max_chunk_count(32)
            .client()
            .map_err(|_| Error::new("invalid OPC UA client configuration"))?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()?;
        let mode = if matches!(config.policy, Policy::None) {
            MessageSecurityMode::None
        } else {
            MessageSecurityMode::SignAndEncrypt
        };
        let policy = config.policy.security();
        let endpoints = runtime
            .block_on(async {
                tokio::time::timeout(
                    config.timeout,
                    client.get_server_endpoints_from_url(config.endpoint.as_str()),
                )
                .await
            })
            .map_err(|_| Error::new("OPC UA endpoint discovery timed out"))?
            .map_err(|_| Error::new("OPC UA endpoint discovery failed"))?;
        let endpoint = select_endpoint(endpoints, &configured_endpoint, policy, mode)?;
        let (session, event_loop) = client
            .connect_to_endpoint_directly(endpoint, token)
            .map_err(|_| Error::new("OPC UA session configuration failed"))?;
        if !reconnect {
            session.disable_reconnects();
        }
        let event_loop = {
            let _entered = runtime.enter();
            event_loop.spawn()
        };
        let connected = runtime.block_on(async {
            tokio::time::timeout(config.timeout, session.wait_for_connection()).await
        });
        if !matches!(connected, Ok(true)) {
            event_loop.abort();
            return Err(Error::new(
                "OPC UA secure session failed; review the PKI rejected store",
            ));
        }
        Ok(Self {
            subscription: None,
            id: config.id,
            session,
            event_loop,
            runtime: Some(runtime),
            roots: config.roots,
            max_nodes: config.max_nodes,
            timeout: config.timeout,
            schema_version: config.schema_version,
            _pki_lease: lease,
        })
    }

    async fn local_node_id(&self, expanded: &ExpandedNodeId) -> Result<NodeId, Error> {
        if expanded.server_index != 0 {
            return Err(Error::new(
                "OPC UA remote server references are unsupported",
            ));
        }
        let mut node = expanded.node_id.clone();
        if !expanded.namespace_uri.is_null() {
            node.namespace = self
                .session
                .get_namespace_index(expanded.namespace_uri.as_ref())
                .await
                .map_err(|_| Error::new("OPC UA namespace URI is unavailable"))?;
        }
        Ok(node)
    }

    async fn references(
        &self,
        node: &NodeId,
    ) -> Result<Vec<::opcua::types::ReferenceDescription>, Error> {
        let description = BrowseDescription {
            node_id: node.clone(),
            browse_direction: BrowseDirection::Forward,
            reference_type_id: ReferenceTypeId::HierarchicalReferences.into(),
            include_subtypes: true,
            node_class_mask: 0,
            result_mask: BrowseResultMask::All as u32,
        };
        let mut results = self
            .session
            .browse(&[description], 128, None)
            .await
            .map_err(|_| Error::new("OPC UA browse failed"))?;
        let mut refs = Vec::new();
        loop {
            if results.len() != 1 {
                return Err(Error::new("OPC UA browse response incomplete"));
            }
            let result = results.remove(0);
            if result.status_code.is_bad() {
                return Err(Error::new("OPC UA browse was denied"));
            }
            refs.extend(result.references.unwrap_or_default());
            if refs.len() > self.max_nodes {
                return Err(Error::new("OPC UA discovery node limit exceeded"));
            }
            if result.continuation_point.is_null_or_empty() {
                break;
            }
            results = self
                .session
                .browse_next(false, &[result.continuation_point])
                .await
                .map_err(|_| Error::new("OPC UA browse continuation failed"))?;
        }
        Ok(refs)
    }
    async fn discover_inner(&self) -> Result<Vec<DiscoveredKind>, Error> {
        let mut roots = Vec::with_capacity(self.roots.len());
        for expanded in &self.roots {
            let root = self.local_node_id(expanded).await?;
            let values = self
                .session
                .read(
                    &[ReadValueId {
                        node_id: root.clone(),
                        attribute_id: AttributeId::NodeClass as u32,
                        ..Default::default()
                    }],
                    TimestampsToReturn::Neither,
                    0.0,
                )
                .await
                .map_err(|_| Error::new("OPC UA discovery root read failed"))?;
            if !matches!(values.as_slice(), [value] if value.status.is_none_or(|s| !s.is_bad()) && matches!(value.value, Some(Variant::Int32(class)) if class == NodeClass::Object as i32))
            {
                return Err(Error::new(
                    "OPC UA discovery roots must be Object containers",
                ));
            }
            roots.push(root);
        }
        let mut queue: VecDeque<_> = roots.iter().map(|r| (r.clone(), r.to_string())).collect();
        let mut scheduled: BTreeSet<_> = roots.iter().map(ToString::to_string).collect();
        let mut visited = BTreeSet::new();
        let mut kinds = BTreeMap::new();
        while let Some((node, path)) = queue.pop_front() {
            if !visited.insert(node.to_string()) {
                continue;
            }
            if visited.len() > self.max_nodes {
                return Err(Error::new("OPC UA discovery node limit exceeded"));
            }
            let mut refs = self.references(&node).await?;
            refs.sort_by_key(|reference| reference.node_id.node_id.to_string());
            for reference in refs {
                let child = self.local_node_id(&reference.node_id).await?;
                let child_path = format!("{path}/{}", reference.browse_name);
                if child_path.len() > 4096 {
                    return Err(Error::new("OPC UA browse path exceeds limit"));
                }
                if reference.node_class == NodeClass::Variable
                    && !kinds.contains_key(&child.to_string())
                {
                    let values = self
                        .session
                        .read(
                            &[
                                ReadValueId {
                                    node_id: child.clone(),
                                    attribute_id: AttributeId::DataType as u32,
                                    ..Default::default()
                                },
                                ReadValueId {
                                    node_id: child.clone(),
                                    attribute_id: AttributeId::ValueRank as u32,
                                    ..Default::default()
                                },
                            ],
                            TimestampsToReturn::Neither,
                            0.0,
                        )
                        .await
                        .map_err(|_| Error::new("OPC UA data type read failed"))?;
                    let (data_type, value_rank) = match values.as_slice() {
                        [data_type, rank]
                            if data_type.status.is_none_or(|s| !s.is_bad())
                                && rank.status.is_none_or(|s| !s.is_bad()) =>
                        {
                            match (&data_type.value, &rank.value) {
                                (Some(Variant::NodeId(id)), Some(Variant::Int32(rank))) => {
                                    (id.clone(), *rank)
                                }
                                _ => {
                                    return Err(Error::new(
                                        "OPC UA data type or value rank missing",
                                    ));
                                }
                            }
                        }
                        _ => return Err(Error::new("OPC UA type attributes unavailable")),
                    };
                    let mut metadata = BTreeMap::from([
                        ("node_id".into(), child.to_string()),
                        ("browse_path".into(), child_path.clone()),
                        ("data_type".into(), data_type.to_string()),
                        ("value_rank".into(), value_rank.to_string()),
                    ]);
                    for property in self.references(&child).await? {
                        if property.browse_name.name.as_ref() != "EngineeringUnits"
                            || property.browse_name.namespace_index != 0
                            || property.reference_type_id != ReferenceTypeId::HasProperty
                        {
                            continue;
                        }
                        let property_id = self.local_node_id(&property.node_id).await?;
                        let values = self
                            .session
                            .read(
                                &[ReadValueId {
                                    node_id: property_id,
                                    attribute_id: AttributeId::Value as u32,
                                    ..Default::default()
                                }],
                                TimestampsToReturn::Neither,
                                0.0,
                            )
                            .await
                            .map_err(|_| Error::new("OPC UA engineering units read failed"))?;
                        let unit = match values.as_slice() {
                            [value] if value.status.is_none_or(|s| !s.is_bad()) => {
                                match &value.value {
                                    Some(Variant::ExtensionObject(object)) => {
                                        object.inner_as::<EUInformation>()
                                    }
                                    _ => None,
                                }
                            }
                            _ => None,
                        }
                        .ok_or_else(|| Error::new("OPC UA engineering units unavailable"))?;
                        metadata.insert(
                            "engineering_units".into(),
                            unit.display_name.text.to_string(),
                        );
                    }
                    kinds.insert(
                        child.to_string(),
                        DiscoveredKind {
                            kind: child.to_string(),
                            schema: Schema {
                                version: self.schema_version,
                                key_fields: vec![],
                                fields: BTreeMap::from([(
                                    "value".into(),
                                    if value_rank == -1 {
                                        field_type(&data_type)
                                    } else {
                                        FieldType::Bytes
                                    },
                                )]),
                            },
                            metadata,
                        },
                    );
                }
                if matches!(
                    reference.node_class,
                    NodeClass::Object | NodeClass::Variable
                ) && scheduled.insert(child.to_string())
                {
                    if scheduled.len() > self.max_nodes {
                        return Err(Error::new("OPC UA discovery node limit exceeded"));
                    }
                    queue.push_back((child, child_path));
                }
            }
        }
        Ok(kinds.into_values().collect())
    }
}
fn select_endpoint(
    endpoints: Vec<::opcua::types::EndpointDescription>,
    configured: &url::Url,
    policy: SecurityPolicy,
    mode: MessageSecurityMode,
) -> Result<::opcua::types::EndpointDescription, Error> {
    endpoints
        .into_iter()
        .find(|endpoint| {
            endpoint.security_mode == mode
                && SecurityPolicy::from_uri(endpoint.security_policy_uri.as_ref()) == policy
                && endpoint_url(endpoint.endpoint_url.as_ref()).is_ok_and(|url| url == *configured)
        })
        .ok_or_else(|| Error::new("OPC UA no endpoint matches configured address and security"))
}

fn endpoint_url(text: &str) -> Result<url::Url, Error> {
    let mut url = url::Url::parse(text).map_err(|_| Error::new("invalid OPC UA endpoint URL"))?;
    if url.scheme() != "opc.tcp"
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(Error::new("invalid OPC UA endpoint URL"));
    }
    if url.path().is_empty() {
        url.set_path("/");
    }
    url.set_port(Some(url.port().unwrap_or(4840)))
        .map_err(|_| Error::new("invalid OPC UA endpoint port"))?;
    Ok(url)
}

fn field_type(id: &NodeId) -> FieldType {
    if *id == DataTypeId::Boolean {
        FieldType::Bool
    } else if [
        DataTypeId::SByte,
        DataTypeId::Byte,
        DataTypeId::Int16,
        DataTypeId::UInt16,
        DataTypeId::Int32,
    ]
    .iter()
    .any(|t| *id == *t)
    {
        FieldType::I32
    } else if *id == DataTypeId::Int64 || *id == DataTypeId::UInt32 {
        FieldType::I64
    } else if *id == DataTypeId::Float {
        FieldType::F32
    } else if *id == DataTypeId::Double {
        FieldType::F64
    } else if *id == DataTypeId::ByteString {
        FieldType::Bytes
    } else {
        FieldType::Text
    }
}
impl Connector for OpcUaConnector {
    fn id(&self) -> &str {
        &self.id
    }
    fn check(&self) -> Result<CheckReport, Error> {
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err(Error::new(
                "call OPC UA synchronous API outside an async runtime",
            ));
        }
        let runtime = self
            .runtime
            .as_ref()
            .ok_or_else(|| Error::new("OPC UA runtime closed"))?;
        let good = self
            .subscription
            .as_ref()
            .is_none_or(|reader| reader.healthy())
            && !self.event_loop.is_finished()
            && runtime
                .block_on(async {
                    tokio::time::timeout(self.timeout, self.session.wait_for_connection()).await
                })
                .is_ok_and(|connected| connected);
        Ok(CheckReport {
            ok: good,
            message: if good {
                "session connected"
            } else {
                "session disconnected"
            }
            .into(),
        })
    }
    fn discover(&self) -> Result<Vec<DiscoveredKind>, Error> {
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err(Error::new(
                "call OPC UA synchronous API outside an async runtime",
            ));
        }
        let runtime = self
            .runtime
            .as_ref()
            .ok_or_else(|| Error::new("OPC UA runtime closed"))?;
        if let Some(reader) = &self.subscription {
            return Ok(reader.kinds.clone());
        }
        runtime.block_on(async {
            let result = tokio::time::timeout(self.timeout, self.discover_inner())
                .await
                .map_err(|_| Error::new("OPC UA discovery timed out"))
                .and_then(|result| result);
            if result.is_err() {
                // Closing the session releases every continuation, including a token whose
                // response was cancelled before it reached the caller.
                self.session.disable_reconnects();
                let _ = tokio::time::timeout(self.timeout, self.session.disconnect()).await;
                self.event_loop.abort();
            }
            result
        })
    }
    fn read(&mut self, from: Option<Position>, _: Option<&str>) -> Result<Option<Record>, Error> {
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err(Error::new(
                "call OPC UA synchronous API outside an async runtime",
            ));
        }
        let Some(reader) = self.subscription.as_mut() else {
            return Ok(None);
        };
        let runtime = self
            .runtime
            .as_ref()
            .ok_or_else(|| Error::new("OPC UA runtime closed"))?;
        runtime.block_on(reader.read(&self.session, from, self.timeout))
    }
}
impl Drop for OpcUaConnector {
    fn drop(&mut self) {
        self.session.disable_reconnects();
        if let Some(runtime) = self.runtime.take() {
            if tokio::runtime::Handle::try_current().is_ok() {
                self.event_loop.abort();
                runtime.shutdown_background();
            } else {
                let _ = runtime.block_on(async {
                    tokio::time::timeout(self.timeout, self.session.disconnect()).await
                });
                self.event_loop.abort();
                runtime.shutdown_timeout(self.timeout);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::endpoint_url;
    #[test]
    fn endpoint_scope_keeps_explicit_ports_and_rejects_embedded_credentials() {
        let configured = endpoint_url("opc.tcp://127.0.0.1:4841/").unwrap();
        assert_ne!(
            configured,
            endpoint_url("opc.tcp://127.0.0.1:4842/").unwrap()
        );
        assert_eq!(
            endpoint_url("opc.tcp://127.0.0.1").unwrap(),
            endpoint_url("opc.tcp://127.0.0.1:4840/").unwrap()
        );
        for endpoint in [
            "opc.tcp://user:secret@127.0.0.1:4841/",
            "opc.tcp://127.0.0.1:4841/?token=secret",
            "opc.tcp://127.0.0.1:4841/#fragment",
            "https://127.0.0.1/",
        ] {
            assert!(endpoint_url(endpoint).is_err());
        }
    }
    #[test]
    fn endpoint_selection_checks_original_addresses_and_can_choose_a_later_exact_match() {
        use ::opcua::crypto::SecurityPolicy;
        use ::opcua::types::{EndpointDescription, MessageSecurityMode};
        let endpoint = |url: &str, mode| EndpointDescription {
            endpoint_url: url.into(),
            security_policy_uri: SecurityPolicy::Basic256Sha256.to_uri().into(),
            security_mode: mode,
            ..Default::default()
        };
        let configured = endpoint_url("opc.tcp://127.0.0.1:4841/").unwrap();
        let mode = MessageSecurityMode::SignAndEncrypt;
        let candidates = vec![
            endpoint("opc.tcp://127.0.0.1:4842/", mode),
            endpoint("opc.tcp://127.0.0.2:4841/", mode),
            endpoint("opc.tcp://127.0.0.1:4841/", MessageSecurityMode::None),
            endpoint("opc.tcp://127.0.0.1:4841/", mode),
        ];
        let selected = super::select_endpoint(
            candidates.clone(),
            &configured,
            SecurityPolicy::Basic256Sha256,
            mode,
        )
        .unwrap();
        assert_eq!(selected.endpoint_url.as_ref(), "opc.tcp://127.0.0.1:4841/");
        assert_eq!(selected.security_mode, mode);
        assert!(
            super::select_endpoint(
                candidates[..3].to_vec(),
                &configured,
                SecurityPolicy::Basic256Sha256,
                mode
            )
            .is_err()
        );
    }
}
