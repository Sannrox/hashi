//! MQTT write target for [`crate::write`]: one QoS 2 publish per command.
//!
//! `applied` means the broker completed the QoS 2 exchange (PUBCOMP). That is
//! broker delivery, not device confirmation. The session is clean and never
//! reconnects: a resumed session would resend an unconfirmed publish, so once
//! an exchange ends without PUBCOMP the session is dropped, the command is
//! `unknown`, and later commands in the run are refused before sending.

use std::time::{Duration, Instant};

use super::{Config, Event, Session, validate};
use crate::Error;
use crate::write::{Command, Outcome, Target};

/// Room for the fixed header, topic length, packet id, and MQTT 5 properties.
const PACKET_OVERHEAD: usize = 16;

pub struct MqttWriter {
    /// `None` once an exchange ended without PUBCOMP.
    session: Option<Session>,
    timeout: Duration,
    max_packet_bytes: usize,
}

/// A topic and the command value as compact JSON text.
pub struct PreparedPublish {
    topic: String,
    payload: Vec<u8>,
}

impl MqttWriter {
    /// Opens a clean session and waits for the broker's ConnAck. Synchronous:
    /// call it outside an async runtime. `config.routes` are the allowed topics.
    pub fn connect(config: Config) -> Result<Self, Error> {
        validate(&config)?;
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err(Error::new(
                "call MQTT synchronous API outside an async runtime",
            ));
        }
        let mut session = Session::open(&config, false)?;
        let deadline = Instant::now() + config.timeout;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(Error::new("MQTT connect timed out"));
            }
            match session.recv(remaining)? {
                Some(Event::ConnAck { .. }) => break,
                // Fail at the first error: a retry would only reconnect into the same refusal.
                Some(Event::Disconnected) => {
                    return Err(Error::new("MQTT broker unreachable or refused the session"));
                }
                _ => {}
            }
        }
        Ok(Self {
            session: Some(session),
            timeout: config.timeout,
            max_packet_bytes: config.max_packet_bytes,
        })
    }
}

impl Target for MqttWriter {
    type Prepared = PreparedPublish;

    fn prepare(&mut self, command: &Command) -> Result<PreparedPublish, String> {
        if self.session.is_none() {
            return Err("MQTT session closed after an earlier unconfirmed write".into());
        }
        let payload = command.value.to_string().into_bytes();
        if payload.len() + command.target.len() + PACKET_OVERHEAD > self.max_packet_bytes {
            return Err(format!(
                "payload exceeds the {} byte packet limit",
                self.max_packet_bytes
            ));
        }
        Ok(PreparedPublish {
            topic: command.target.clone(),
            payload,
        })
    }

    fn send(&mut self, prepared: PreparedPublish) -> Outcome {
        let Some(session) = self.session.as_mut() else {
            return Outcome::Refused {
                reason: "MQTT session closed after an earlier unconfirmed write".into(),
            };
        };
        let outcome = exchange(session, prepared, self.timeout);
        if outcome != Outcome::Applied {
            // Dropping the session stops its event loop, so nothing queued is
            // ever sent or resent; an MQTT 5 refusal also leaves an inflight slot taken.
            self.session = None;
        }
        outcome
    }
}

/// Publishes once and drives the session until PUBCOMP, a refusal, a lost
/// connection, or the timeout. Never polls after a disconnect, which would reconnect.
fn exchange(session: &mut Session, prepared: PreparedPublish, timeout: Duration) -> Outcome {
    let unknown = |reason: &str| Outcome::Unknown {
        reason: reason.into(),
    };
    if !session.try_publish_exactly_once(&prepared.topic, prepared.payload) {
        return Outcome::Refused {
            reason: "MQTT client closed".into(),
        };
    }
    let deadline = Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return unknown("no PUBCOMP within timeout_ms");
        }
        match session.recv(remaining) {
            Err(_) | Ok(Some(Event::Disconnected)) => {
                return unknown("MQTT session lost before PUBCOMP");
            }
            Ok(Some(Event::PubRec { refused: true })) => {
                return Outcome::Refused {
                    reason: "MQTT broker refused the publish".into(),
                };
            }
            Ok(Some(Event::PubComp { ok: true })) => return Outcome::Applied,
            Ok(Some(Event::PubComp { ok: false })) => {
                return unknown("MQTT broker ended the exchange with a failure reason");
            }
            Ok(_) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mqtt::{Decoder, Protocol, Route, Transport};
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::mpsc;

    const TOPIC: &str = "plant/line-1/setpoint";

    /// What the scripted broker does after it receives the PUBLISH.
    #[derive(Clone, Copy)]
    enum Broker {
        Silent,
        Close,
        RefuseV5,
        Complete,
    }

    fn read_packet(stream: &mut TcpStream) -> Option<(u8, Vec<u8>)> {
        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte).ok()?;
        let kind = byte[0];
        let (mut length, mut shift) = (0usize, 0);
        loop {
            stream.read_exact(&mut byte).ok()?;
            length |= usize::from(byte[0] & 0x7f) << shift;
            shift += 7;
            if byte[0] & 0x80 == 0 {
                break;
            }
        }
        let mut body = vec![0; length];
        stream.read_exact(&mut body).ok()?;
        Some((kind, body))
    }

    /// Serves one session on an ephemeral port and reports every PUBLISH
    /// (first header byte, body) and whether the client ever connected again.
    fn broker(protocol: Protocol, script: Broker) -> (u16, mpsc::Receiver<(u8, Vec<u8>)>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            read_packet(&mut stream).unwrap();
            let connack: &[u8] = match protocol {
                Protocol::V311 => &[0x20, 2, 0, 0],
                Protocol::V5 => &[0x20, 3, 0, 0, 0],
            };
            stream.write_all(connack).unwrap();
            while let Some((kind, body)) = read_packet(&mut stream) {
                if kind >> 4 != 3 {
                    continue;
                }
                let topic_len = usize::from(u16::from_be_bytes([body[0], body[1]]));
                let pkid = [body[2 + topic_len], body[3 + topic_len]];
                let _ = tx.send((kind, body.clone()));
                match script {
                    Broker::Silent => {}
                    Broker::Close => break,
                    Broker::RefuseV5 => {
                        stream
                            .write_all(&[0x50, 3, pkid[0], pkid[1], 0x87])
                            .unwrap();
                    }
                    Broker::Complete => {
                        stream.write_all(&[0x50, 2, pkid[0], pkid[1]]).unwrap();
                        read_packet(&mut stream).unwrap();
                        stream.write_all(&[0x70, 2, pkid[0], pkid[1]]).unwrap();
                    }
                }
            }
            drop(stream);
            // A reconnect would be a resend path; report it as a second session.
            listener.set_nonblocking(true).unwrap();
            let until = Instant::now() + Duration::from_millis(800);
            while Instant::now() < until {
                if listener.accept().is_ok() {
                    let _ = tx.send((0, b"reconnected".to_vec()));
                    return;
                }
                std::thread::sleep(Duration::from_millis(20));
            }
        });
        (port, rx)
    }

    fn writer(protocol: Protocol, port: u16) -> MqttWriter {
        MqttWriter::connect(Config {
            id: "writer".into(),
            host: "127.0.0.1".into(),
            port,
            protocol,
            transport: Transport::Plain,
            allow_insecure_lab: true,
            user_name: None,
            client_id: "hashi-writer".into(),
            shared_group: None,
            routes: vec![Route {
                filter: TOPIC.into(),
                kind: "write".into(),
                decoder: Decoder::Raw,
            }],
            session_expiry: Duration::from_secs(1),
            keep_alive: Duration::from_secs(30),
            timeout: Duration::from_millis(300),
            max_packet_bytes: 1024,
            schema_version: 1,
            sparkplug_state_dir: None,
        })
        .unwrap()
    }

    fn command(id: &str, value: serde_json::Value) -> Command {
        Command {
            id: id.into(),
            target: TOPIC.into(),
            value,
        }
    }

    fn publish_once(
        protocol: Protocol,
        script: Broker,
    ) -> (MqttWriter, Outcome, Vec<(u8, Vec<u8>)>) {
        let (port, published) = broker(protocol, script);
        let mut writer = writer(protocol, port);
        let prepared = writer
            .prepare(&command("c1", serde_json::json!({"sp": 42})))
            .unwrap();
        let outcome = writer.send(prepared);
        std::thread::sleep(Duration::from_millis(900));
        (writer, outcome, published.try_iter().collect())
    }

    #[test]
    fn a_completed_exchange_is_applied_with_one_qos2_publish() {
        for protocol in [Protocol::V311, Protocol::V5] {
            let (mut writer, outcome, published) = publish_once(protocol, Broker::Complete);
            assert_eq!(outcome, Outcome::Applied);
            assert_eq!(published.len(), 1);
            let (kind, body) = &published[0];
            assert_eq!(kind & 0x06, 0x04, "QoS 2");
            assert!(body.ends_with(br#"{"sp":42}"#));
            assert!(writer.prepare(&command("c2", 1.into())).is_ok());
        }
    }

    #[test]
    fn a_publish_without_pubcomp_is_unknown_and_never_resent() {
        for protocol in [Protocol::V311, Protocol::V5] {
            for script in [Broker::Silent, Broker::Close] {
                let (mut writer, outcome, published) = publish_once(protocol, script);
                assert!(matches!(outcome, Outcome::Unknown { .. }), "{outcome:?}");
                assert_eq!(published.len(), 1, "no resend and no reconnect");
                assert!(writer.prepare(&command("c2", 1.into())).is_err());
            }
        }
    }

    #[test]
    fn an_mqtt5_failure_reason_is_a_refusal() {
        let (_, outcome, published) = publish_once(Protocol::V5, Broker::RefuseV5);
        assert!(matches!(outcome, Outcome::Refused { .. }), "{outcome:?}");
        assert_eq!(published.len(), 1);
    }

    #[test]
    fn an_oversized_payload_is_refused_before_sending() {
        let (port, published) = broker(Protocol::V311, Broker::Complete);
        let mut writer = writer(Protocol::V311, port);
        assert!(
            writer
                .prepare(&command("big", "x".repeat(2048).into()))
                .is_err()
        );
        assert!(published.try_iter().next().is_none());
    }
}
