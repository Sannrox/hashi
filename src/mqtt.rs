//! MQTT 3.1.1 and 5 subscriber that acknowledges a message only after the runtime commits it.
//!
//! Each message is held until the runtime's next `read(from)` proves that the
//! checkpoint reached its position. Only then is the QoS 1 acknowledgment sent,
//! so a crash between the sink ack and the broker ack makes the broker redeliver.
//! Delivery is at least once: a redelivered message gets a new position.
//! One message can yield several records (Sparkplug B); it is acknowledged
//! after the last of them is committed.
pub mod sparkplug;

use std::collections::{BTreeMap, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rumqttc::{TlsConfiguration, Transport as MqttTransport, v5};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

use crate::contract::{CheckReport, DiscoveredKind, Position, Record};
use crate::schema::{FieldType, Schema};
use crate::{Connector, Error};

/// Kind of records whose topic matches no route. The runtime quarantines them,
/// commits, and the message is acknowledged, so a stale subscription cannot stall.
pub const UNROUTED_KIND: &str = "__mqtt_unrouted";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    V311,
    V5,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decoder {
    /// Payload must be JSON. It is emitted compacted; invalid JSON is emitted
    /// as hex with a `bad:` quality so nothing is dropped.
    Json,
    /// Payload is emitted as lowercase hex.
    Raw,
    /// Sparkplug B: one record per metric plus birth and death records. Needs
    /// `sparkplug_state_dir` and cannot share a subscription; see [`sparkplug`].
    SparkplugB,
}

/// Messages on topics matching `filter` become records of `kind`. The first
/// matching route wins when filters overlap.
#[derive(Debug, Clone)]
pub struct Route {
    pub filter: String,
    pub kind: String,
    pub decoder: Decoder,
}

pub enum Transport {
    /// Verified TLS against `ca_file`, optionally with a client certificate (PEM files).
    Tls {
        ca_file: PathBuf,
        client_certificate: Option<(PathBuf, PathBuf)>,
    },
    /// Plaintext. Refused unless `allow_insecure_lab` is set.
    Plain,
}

/// Never format credential contents into diagnostics.
pub struct Config {
    pub id: String,
    pub host: String,
    pub port: u16,
    pub protocol: Protocol,
    pub transport: Transport,
    pub allow_insecure_lab: bool,
    pub user_name: Option<(String, String)>,
    /// Stable per instance: the broker keeps the persistent session under it.
    pub client_id: String,
    /// Subscribe as `$share/<group>/<filter>` so instances split the load.
    pub shared_group: Option<String>,
    pub routes: Vec<Route>,
    /// MQTT 5 session expiry, at least one second: a zero expiry would drop
    /// unacknowledged messages with the connection. MQTT 3.1.1 sessions never expire client-side.
    pub session_expiry: Duration,
    pub keep_alive: Duration,
    /// Connect and subscribe deadline, and how long `read` waits for a message.
    pub timeout: Duration,
    pub max_packet_bytes: usize,
    pub schema_version: u64,
    /// Durable Sparkplug B birth state, owned by one instance. Required by Sparkplug B routes.
    pub sparkplug_state_dir: Option<PathBuf>,
}

pub struct MqttConnector {
    id: String,
    routes: Vec<Route>,
    filters: Vec<String>,
    schema_version: u64,
    timeout: Duration,
    session: Session,
    /// Bumped on every ConnAck and disconnect. An ack is only valid on the connection that received the message.
    generation: u64,
    queue: VecDeque<Message>,
    held: Option<Held>,
    pending_subacks: usize,
    connected: bool,
    ack_sent: Option<u16>,
    sparkplug: Option<sparkplug::Hosts>,
}

/// A message whose records are being handed to the runtime.
struct Held {
    message: Message,
    pending: VecDeque<Record>,
    /// The last record handed out and its position, until the runtime commits it.
    emitted: Option<(Position, Record)>,
}

struct Message {
    generation: u64,
    publish: Publish,
    received_ms: i64,
}

enum Publish {
    V311(rumqttc::Publish),
    V5(v5::mqttbytes::v5::Publish),
}

enum Session {
    V311(rumqttc::Client, Box<rumqttc::Connection>),
    V5(v5::Client, Box<v5::Connection>),
}

/// Protocol-neutral view of the events this connector acts on.
enum Event {
    ConnAck { session_present: bool },
    SubAck { granted: Vec<bool> },
    Publish(Publish),
    AckSent(u16),
    Disconnected,
    Other,
}

impl MqttConnector {
    /// Connects, subscribes every route at QoS 1, and waits for the acknowledgments.
    /// Synchronous: call it from a dedicated OS thread, never inside an async runtime.
    pub fn connect(config: Config) -> Result<Self, Error> {
        validate(&config)?;
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err(Error::new(
                "call MQTT synchronous API outside an async runtime",
            ));
        }
        let transport = match &config.transport {
            Transport::Plain => MqttTransport::Tcp,
            Transport::Tls {
                ca_file,
                client_certificate,
            } => MqttTransport::Tls(TlsConfiguration::Rustls(Arc::new(tls(
                ca_file,
                client_certificate.as_ref(),
            )?))),
        };
        let session = match config.protocol {
            Protocol::V311 => {
                let mut options =
                    rumqttc::MqttOptions::new(&config.client_id, &config.host, config.port);
                options
                    .set_clean_session(false)
                    .set_manual_acks(true)
                    .set_keep_alive(config.keep_alive)
                    .set_max_packet_size(config.max_packet_bytes, config.max_packet_bytes)
                    .set_transport(transport);
                if let Some((user, password)) = &config.user_name {
                    options.set_credentials(user, password);
                }
                let (client, connection) = rumqttc::Client::new(options, 64);
                Session::V311(client, Box::new(connection))
            }
            Protocol::V5 => {
                let mut options =
                    v5::MqttOptions::new(&config.client_id, &config.host, config.port);
                let expiry = u32::try_from(config.session_expiry.as_secs()).unwrap_or(u32::MAX);
                options
                    .set_clean_start(false)
                    .set_session_expiry_interval(Some(expiry))
                    .set_manual_acks(true)
                    .set_keep_alive(config.keep_alive)
                    .set_max_packet_size(Some(
                        u32::try_from(config.max_packet_bytes).unwrap_or(u32::MAX),
                    ))
                    .set_transport(transport);
                if let Some((user, password)) = &config.user_name {
                    options.set_credentials(user, password);
                }
                let (client, connection) = v5::Client::new(options, 64);
                Session::V5(client, Box::new(connection))
            }
        };
        let filters = config
            .routes
            .iter()
            .map(|route| match &config.shared_group {
                Some(group) => format!("$share/{group}/{}", route.filter),
                None => route.filter.clone(),
            })
            .collect();
        let sparkplug = match &config.sparkplug_state_dir {
            Some(dir) => Some(sparkplug::Hosts::open(
                dir,
                i64::try_from(config.timeout.as_millis()).unwrap_or(i64::MAX),
            )?),
            None => None,
        };
        let mut connector = Self {
            id: config.id,
            routes: config.routes,
            filters,
            schema_version: config.schema_version,
            timeout: config.timeout,
            session,
            generation: 0,
            queue: VecDeque::new(),
            held: None,
            pending_subacks: 0,
            connected: false,
            ack_sent: None,
            sparkplug,
        };
        let deadline = Instant::now() + config.timeout;
        while !connector.connected || connector.pending_subacks > 0 {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(Error::new("MQTT connect and subscribe timed out"));
            }
            connector.poll(remaining)?;
        }
        Ok(connector)
    }

    /// Drives the connection once: queues publishes, resubscribes after every
    /// ConnAck, and refuses subscriptions the broker did not grant at QoS 1.
    fn poll(&mut self, wait: Duration) -> Result<(), Error> {
        let Some(event) = self.session.recv(wait)? else {
            return Ok(());
        };
        match event {
            Event::ConnAck { session_present } => {
                self.generation += 1;
                self.connected = true;
                // A present session redelivers every unacknowledged QoS 1 message, so
                // local copies would duplicate. QoS 0 and fresh-session copies are the last ones.
                if session_present {
                    self.queue
                        .retain(|message| !message.publish.redelivered_on_resume());
                }
                // One SUBSCRIBE for all filters: a request per route could fill the
                // bounded request channel that only `poll` drains.
                self.session.subscribe(&self.filters)?;
                self.pending_subacks = 1;
            }
            Event::SubAck { granted } => {
                if granted.iter().any(|ok| !ok) {
                    return Err(Error::new("MQTT broker refused a QoS 1 subscription"));
                }
                self.pending_subacks = self.pending_subacks.saturating_sub(1);
            }
            Event::Publish(publish) => self.queue.push_back(Message {
                generation: self.generation,
                publish,
                received_ms: now_ms(),
            }),
            Event::AckSent(pkid) => self.ack_sent = Some(pkid),
            Event::Disconnected => {
                // Fence now, not at the next ConnAck: rumqttc drops queued requests on
                // disconnect, and an ack queued after that would reach the new session.
                self.generation += 1;
                self.connected = false;
            }
            Event::Other => {}
        }
        Ok(())
    }

    /// Sends the ack and drives the loop until it is on the wire, so at most one
    /// ack waits in the bounded request channel. A reconnect ends the wait:
    /// that connection's broker redelivers the message instead.
    fn acknowledge(&mut self, message: &Message) -> Result<(), Error> {
        let Some(pkid) = message.publish.ack_pkid() else {
            return Ok(());
        };
        if message.generation != self.generation {
            return Ok(());
        }
        self.session.ack(&message.publish)?;
        self.ack_sent = None;
        let deadline = Instant::now() + self.timeout;
        while self.ack_sent != Some(pkid) && self.generation == message.generation {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(Error::new("MQTT acknowledgment was not sent"));
            }
            self.poll(remaining)?;
        }
        Ok(())
    }

    /// The records of one message, positions unset. A Sparkplug B message may
    /// yield none (commands, stale deaths) or several, and may publish a rebirth request.
    fn records(&mut self, message: &Message) -> Result<Vec<Record>, Error> {
        let (topic, payload) = message.publish.topic_and_payload();
        let route = self
            .routes
            .iter()
            .find(|route| topic_matches(&route.filter, &topic));
        let (kind, payload, quality) = match route {
            None => (UNROUTED_KIND.to_string(), hex(payload), None),
            Some(route) if route.decoder == Decoder::SparkplugB => {
                let hosts = self
                    .sparkplug
                    .as_mut()
                    .ok_or_else(|| Error::new("Sparkplug B route without state"))?;
                let (records, rebirth) =
                    hosts.decode(&route.kind, &topic, payload, message.received_ms)?;
                // Best effort: a full request channel skips it, and the next
                // unresolved message after the interval asks again.
                if let Some(rebirth) = rebirth {
                    self.session.try_publish(rebirth.topic, rebirth.payload);
                }
                return Ok(records);
            }
            Some(route) => {
                let (payload, quality) = decode(route.decoder, payload);
                (route.kind.clone(), payload, quality)
            }
        };
        Ok(vec![Record {
            source: None,
            kind,
            key: topic,
            source_ts_ms: message.received_ms,
            ingest_ts_ms: 0,
            quality,
            position: 0,
            payload,
        }])
    }
}

impl Connector for MqttConnector {
    fn id(&self) -> &str {
        &self.id
    }

    fn check(&self) -> Result<CheckReport, Error> {
        Ok(CheckReport {
            ok: true,
            message: format!("subscribed to {} topic filters", self.filters.len()),
        })
    }

    fn discover(&self) -> Result<Vec<DiscoveredKind>, Error> {
        let mut kinds: BTreeMap<&str, (Decoder, Vec<&str>)> = BTreeMap::new();
        for route in &self.routes {
            let entry = kinds
                .entry(&route.kind)
                .or_insert((route.decoder, Vec::new()));
            entry.1.push(&route.filter);
        }
        Ok(kinds
            .into_iter()
            .map(|(kind, (decoder, filters))| DiscoveredKind {
                kind: kind.to_string(),
                schema: Schema {
                    version: self.schema_version,
                    key_fields: vec![],
                    fields: BTreeMap::from([(
                        "payload".into(),
                        match decoder {
                            Decoder::Json | Decoder::SparkplugB => FieldType::Text,
                            Decoder::Raw => FieldType::Bytes,
                        },
                    )]),
                },
                metadata: BTreeMap::from([("topic_filters".into(), filters.join(" "))]),
            })
            .collect())
    }

    /// `from` at or past the last handed-out position is the runtime's commit
    /// proof. A message is acknowledged once its last record is committed.
    fn read(
        &mut self,
        from: Option<Position>,
        _source_cursor: Option<&str>,
    ) -> Result<Option<Record>, Error> {
        let position = match from {
            None => 0,
            Some(p) => p
                .checked_add(1)
                .ok_or_else(|| Error::new("MQTT position exhausted"))?,
        };
        let deadline = Instant::now() + self.timeout;
        loop {
            if let Some(mut held) = self.held.take() {
                let committed = held
                    .emitted
                    .as_ref()
                    .is_none_or(|(at, _)| from.is_some_and(|committed| committed >= *at));
                // Not committed (a failed run left it): emit it again at the resumed position.
                let next = if committed {
                    held.pending.pop_front()
                } else {
                    held.emitted.take().map(|(_, record)| record)
                };
                if let Some(mut record) = next {
                    record.position = position;
                    held.emitted = Some((position, record.clone()));
                    self.held = Some(held);
                    return Ok(Some(record));
                }
                self.acknowledge(&held.message)?;
            }
            while self.queue.is_empty() || self.pending_subacks > 0 {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    // Caught up only counts on a live session; an outage must stay visible.
                    return if self.connected {
                        Ok(None)
                    } else {
                        Err(Error::new("MQTT broker unreachable"))
                    };
                }
                self.poll(remaining)?;
            }
            let Some(message) = self.queue.pop_front() else {
                continue;
            };
            match self.records(&message) {
                Ok(records) => {
                    self.held = Some(Held {
                        message,
                        pending: records.into(),
                        emitted: None,
                    })
                }
                Err(error) => {
                    self.queue.push_front(message);
                    return Err(error);
                }
            }
        }
    }
}

impl Session {
    /// `None` on timeout or a connection error; the next call reconnects.
    fn recv(&mut self, wait: Duration) -> Result<Option<Event>, Error> {
        let closed = || Error::new("MQTT client closed");
        match self {
            Self::V311(_, connection) => match connection.recv_timeout(wait) {
                Err(rumqttc::RecvTimeoutError::Timeout) => Ok(None),
                Err(rumqttc::RecvTimeoutError::Disconnected) => Err(closed()),
                Ok(Err(_)) => disconnected(),
                Ok(Ok(rumqttc::Event::Incoming(packet))) => Ok(Some(match packet {
                    rumqttc::Packet::ConnAck(ack) => Event::ConnAck {
                        session_present: ack.session_present,
                    },
                    rumqttc::Packet::SubAck(ack) => Event::SubAck {
                        granted: ack
                            .return_codes
                            .iter()
                            .map(|code| {
                                matches!(
                                    code,
                                    rumqttc::SubscribeReasonCode::Success(
                                        rumqttc::QoS::AtLeastOnce
                                    )
                                )
                            })
                            .collect(),
                    },
                    rumqttc::Packet::Publish(publish) => Event::Publish(Publish::V311(publish)),
                    _ => Event::Other,
                })),
                Ok(Ok(rumqttc::Event::Outgoing(rumqttc::Outgoing::PubAck(pkid)))) => {
                    Ok(Some(Event::AckSent(pkid)))
                }
                Ok(Ok(rumqttc::Event::Outgoing(_))) => Ok(Some(Event::Other)),
            },
            Self::V5(_, connection) => match connection.recv_timeout(wait) {
                Err(v5::RecvTimeoutError::Timeout) => Ok(None),
                Err(v5::RecvTimeoutError::Disconnected) => Err(closed()),
                Ok(Err(_)) => disconnected(),
                Ok(Ok(v5::Event::Incoming(packet))) => Ok(Some(match packet {
                    v5::Incoming::ConnAck(ack) => Event::ConnAck {
                        session_present: ack.session_present,
                    },
                    v5::Incoming::SubAck(ack) => Event::SubAck {
                        granted: ack
                            .return_codes
                            .iter()
                            .map(|code| {
                                matches!(
                                    code,
                                    v5::mqttbytes::v5::SubscribeReasonCode::Success(
                                        v5::mqttbytes::QoS::AtLeastOnce
                                    )
                                )
                            })
                            .collect(),
                    },
                    v5::Incoming::Publish(publish) => Event::Publish(Publish::V5(publish)),
                    _ => Event::Other,
                })),
                Ok(Ok(v5::Event::Outgoing(rumqttc::Outgoing::PubAck(pkid)))) => {
                    Ok(Some(Event::AckSent(pkid)))
                }
                Ok(Ok(v5::Event::Outgoing(_))) => Ok(Some(Event::Other)),
            },
        }
    }

    fn subscribe(&self, filters: &[String]) -> Result<(), Error> {
        let result = match self {
            Self::V311(client, _) => client
                .subscribe_many(filters.iter().map(|filter| {
                    rumqttc::SubscribeFilter::new(filter.clone(), rumqttc::QoS::AtLeastOnce)
                }))
                .map_err(|_| ()),
            Self::V5(client, _) => client
                .subscribe_many(filters.iter().map(|filter| {
                    v5::mqttbytes::v5::Filter::new(filter, v5::mqttbytes::QoS::AtLeastOnce)
                }))
                .map_err(|_| ()),
        };
        result.map_err(|()| Error::new("MQTT client closed"))
    }

    /// QoS 0 and nonblocking: never waits on the request channel that only `poll`
    /// drains. A full or closed channel drops the request; callers retry later.
    fn try_publish(&self, topic: String, payload: Vec<u8>) {
        let _ = match self {
            Self::V311(client, _) => client
                .try_publish(topic, rumqttc::QoS::AtMostOnce, false, payload)
                .map_err(|_| ()),
            Self::V5(client, _) => client
                .try_publish(topic, v5::mqttbytes::QoS::AtMostOnce, false, payload)
                .map_err(|_| ()),
        };
    }

    fn ack(&self, publish: &Publish) -> Result<(), Error> {
        let result = match (self, publish) {
            (Self::V311(client, _), Publish::V311(publish)) => client.ack(publish).map_err(|_| ()),
            (Self::V5(client, _), Publish::V5(publish)) => client.ack(publish).map_err(|_| ()),
            _ => Err(()),
        };
        result.map_err(|()| Error::new("MQTT client closed"))
    }
}

/// Connection errors are retried by the next poll. A short pause keeps a
/// refused or unreachable broker from spinning the loop; protocol details stay out of errors.
fn disconnected() -> Result<Option<Event>, Error> {
    std::thread::sleep(Duration::from_millis(200));
    Ok(Some(Event::Disconnected))
}

impl Publish {
    fn redelivered_on_resume(&self) -> bool {
        self.ack_pkid().is_some()
    }

    /// The packet id the broker waits on. Subscriptions are QoS 1, so nothing arrives at QoS 2.
    fn ack_pkid(&self) -> Option<u16> {
        match self {
            Self::V311(publish) => {
                (publish.qos == rumqttc::QoS::AtLeastOnce).then_some(publish.pkid)
            }
            Self::V5(publish) => {
                (publish.qos == v5::mqttbytes::QoS::AtLeastOnce).then_some(publish.pkid)
            }
        }
    }

    fn topic_and_payload(&self) -> (String, &[u8]) {
        match self {
            Self::V311(publish) => (publish.topic.clone(), &publish.payload),
            Self::V5(publish) => (
                String::from_utf8_lossy(&publish.topic).into_owned(),
                &publish.payload,
            ),
        }
    }
}

fn tls(
    ca_file: &std::path::Path,
    client_certificate: Option<&(PathBuf, PathBuf)>,
) -> Result<rustls::ClientConfig, Error> {
    let mut roots = rustls::RootCertStore::empty();
    for certificate in CertificateDer::pem_file_iter(ca_file)
        .map_err(|_| Error::new("MQTT CA file is unreadable"))?
    {
        let certificate = certificate.map_err(|_| Error::new("MQTT CA file is unreadable"))?;
        roots
            .add(certificate)
            .map_err(|_| Error::new("MQTT CA certificate is invalid"))?;
    }
    if roots.is_empty() {
        return Err(Error::new("MQTT CA file holds no certificate"));
    }
    let builder = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|_| Error::new("MQTT TLS configuration is invalid"))?
    .with_root_certificates(roots);
    match client_certificate {
        None => Ok(builder.with_no_client_auth()),
        Some((certificate, key)) => {
            let chain = CertificateDer::pem_file_iter(certificate)
                .and_then(|certs| certs.collect::<Result<Vec<_>, _>>())
                .map_err(|_| Error::new("MQTT client certificate is unreadable"))?;
            let key = PrivateKeyDer::from_pem_file(key)
                .map_err(|_| Error::new("MQTT client key is unreadable"))?;
            builder
                .with_client_auth_cert(chain, key)
                .map_err(|_| Error::new("MQTT client certificate or key is invalid"))
        }
    }
}

fn validate(config: &Config) -> Result<(), Error> {
    if matches!(config.transport, Transport::Plain) && !config.allow_insecure_lab {
        return Err(Error::new("MQTT without TLS requires allow_insecure_lab"));
    }
    let group_ok = config.shared_group.as_ref().is_none_or(|group| {
        !group.is_empty() && !group.contains(['/', '+', '#']) && valid_text(group)
    });
    if config.id.trim().is_empty()
        || config.host.trim().is_empty()
        || config.client_id.trim().is_empty()
        || !valid_text(&config.client_id)
        || !group_ok
        || config.routes.is_empty()
        || config.timeout.is_zero()
        || (config.protocol == Protocol::V5 && config.session_expiry < Duration::from_secs(1))
        || config.keep_alive < Duration::from_secs(5)
        || config.max_packet_bytes == 0
        || config.schema_version == 0
    {
        return Err(Error::new("invalid MQTT configuration"));
    }
    let mut decoders = BTreeMap::new();
    for route in &config.routes {
        if route.kind.trim().is_empty()
            || route.kind == UNROUTED_KIND
            || route.kind == sparkplug::UNRESOLVED_KIND
            || route.filter.starts_with("$share/")
            || !valid_filter(&route.filter)
        {
            return Err(Error::new(format!(
                "invalid MQTT route for filter {:?}",
                route.filter
            )));
        }
        if route.decoder == Decoder::SparkplugB
            && (config.sparkplug_state_dir.is_none() || config.shared_group.is_some())
        {
            return Err(Error::new(
                "Sparkplug B routes need sparkplug_state_dir and no shared_group",
            ));
        }
        if *decoders.entry(&route.kind).or_insert(route.decoder) != route.decoder {
            return Err(Error::new(format!(
                "MQTT kind {:?} has routes with different decoders",
                route.kind
            )));
        }
    }
    Ok(())
}

fn valid_text(value: &str) -> bool {
    !value.contains('\0') && value.len() <= 65535
}

/// MQTT filter rules: `#` only as the whole last level, `+` only as a whole level.
fn valid_filter(filter: &str) -> bool {
    let levels: Vec<&str> = filter.split('/').collect();
    !filter.is_empty()
        && valid_text(filter)
        && levels.iter().enumerate().all(|(i, level)| {
            (*level == "#" && i == levels.len() - 1) || *level == "+" || !level.contains(['#', '+'])
        })
}

/// Wildcards never match a first level starting with `$` (MQTT 4.7.2).
fn topic_matches(filter: &str, topic: &str) -> bool {
    if topic.starts_with('$') && filter.starts_with(['+', '#']) {
        return false;
    }
    let mut topic_levels = topic.split('/');
    for level in filter.split('/') {
        match (level, topic_levels.next()) {
            ("#", _) => return true,
            ("+", Some(_)) => {}
            (expected, Some(actual)) if expected == actual => {}
            _ => return false,
        }
    }
    topic_levels.next().is_none()
}

fn decode(decoder: Decoder, payload: &[u8]) -> (String, Option<String>) {
    match decoder {
        // Sparkplug B is decoded with its birth state in `MqttConnector::records`.
        Decoder::Raw | Decoder::SparkplugB => (hex(payload), None),
        Decoder::Json => match serde_json::from_slice::<serde_json::Value>(payload) {
            Ok(value) => (value.to_string(), None),
            Err(_) => (hex(payload), Some("bad: payload is not JSON".into())),
        },
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filters_match_like_the_broker() {
        assert!(topic_matches("plant/+/temp", "plant/a/temp"));
        assert!(!topic_matches("plant/+/temp", "plant/a/b/temp"));
        assert!(topic_matches("plant/#", "plant"));
        assert!(topic_matches("plant/#", "plant/a/b"));
        assert!(!topic_matches("plant/a", "plant/a/b"));
        assert!(!topic_matches("#", "$SYS/uptime"));
        assert!(topic_matches("$SYS/#", "$SYS/uptime"));
        assert!(valid_filter("a/+/#"));
        assert!(!valid_filter("a/#/b"));
        assert!(!valid_filter("a/b+"));
        assert!(!valid_filter(""));
    }

    #[test]
    fn plaintext_shared_prefixes_and_mixed_decoders_are_refused() {
        let route = |filter: &str, decoder| Route {
            filter: filter.into(),
            kind: "reading".into(),
            decoder,
        };
        let config = |allow_insecure_lab, routes| Config {
            id: "mqtt".into(),
            host: "127.0.0.1".into(),
            port: 1883,
            protocol: Protocol::V311,
            transport: Transport::Plain,
            allow_insecure_lab,
            user_name: None,
            client_id: "hashi".into(),
            shared_group: None,
            routes,
            session_expiry: Duration::from_secs(60),
            keep_alive: Duration::from_secs(30),
            timeout: Duration::from_secs(1),
            max_packet_bytes: 1024,
            schema_version: 1,
            sparkplug_state_dir: None,
        };
        assert!(validate(&config(true, vec![route("a/+", Decoder::Json)])).is_ok());
        assert!(validate(&config(false, vec![route("a/+", Decoder::Json)])).is_err());
        assert!(validate(&config(true, vec![route("$share/g/a", Decoder::Json)])).is_err());
        let mixed = vec![route("a", Decoder::Json), route("b", Decoder::Raw)];
        assert!(validate(&config(true, mixed)).is_err());
    }

    #[test]
    fn decoders_never_drop_a_payload() {
        assert_eq!(
            decode(Decoder::Json, br#"{ "v": 1 }"#),
            (r#"{"v":1}"#.into(), None)
        );
        assert_eq!(
            decode(Decoder::Json, b"\xff"),
            ("ff".into(), Some("bad: payload is not JSON".into()))
        );
        assert_eq!(decode(Decoder::Raw, b"\x00\xab"), ("00ab".into(), None));
    }
}
