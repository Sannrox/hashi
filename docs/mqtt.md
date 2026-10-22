# MQTT subscriptions

Enable the `mqtt` Cargo feature to use `mqtt::MqttConnector::connect(Config)`.
The rumqttc 0.25 client supplies MQTT 3.1.1 and 5 framing; rustls with the ring
provider supplies TLS. Hashi owns acknowledgment timing, routing, decoding, and
the connector contract. Call the synchronous API from a dedicated OS thread
outside a Tokio runtime.

## Configure a subscription

```rust,ignore
let config = Config {
    id: "plant-mqtt".into(),
    host: "broker.example".into(),
    port: 8883,
    protocol: Protocol::V5,
    transport: Transport::Tls { ca_file, client_certificate: Some((cert_pem, key_pem)) },
    allow_insecure_lab: false,
    user_name: None,
    client_id: "hashi-line-1".into(),
    shared_group: Some("hashi".into()),
    routes: vec![Route { filter: "plant/+/telemetry".into(), kind: "telemetry".into(), decoder: Decoder::Json }],
    session_expiry: Duration::from_secs(24 * 3600),
    keep_alive: Duration::from_secs(30),
    timeout: Duration::from_secs(10),
    max_packet_bytes: 1024 * 1024,
    schema_version: 1,
};
```

TLS verifies the broker certificate against `ca_file` and the configured host
name. A client certificate and key (PEM) are optional, as is a user name and
password; obtain both from the embedding application's credential custody.
Plaintext requires `allow_insecure_lab`. Diagnostics never carry credentials or
raw protocol errors.

## Routes and decoders

Every route is subscribed at QoS 1; a broker that grants a lower QoS fails the
connect. A message takes the kind of the first route whose filter matches its
topic, and its topic becomes the record key. `Decoder::Json` emits the payload
as compact JSON text; a payload that is not JSON is emitted as hex with quality
`bad: payload is not JSON`, so it is never dropped. `Decoder::Raw` emits hex.
Routes of one kind must share a decoder. A message on a topic no route matches
(for example from a subscription left in the persistent session by an older
config) gets the kind `__mqtt_unrouted`; the runtime quarantines and commits it.

## Acknowledgment after the sink

Sessions are persistent: MQTT 3.1.1 connects with a clean session off, MQTT 5
with clean start off and `session_expiry`. Keep `client_id` stable per
instance and unique across instances.

A message is acknowledged to the broker only when the runtime's next read
starts from its position, which happens after the sink acknowledged it and the
checkpoint committed. A crash at any point before that leaves the message
unacknowledged, and the broker redelivers it on reconnect. Delivery is at least
once: a redelivered message can be delivered again under a new position, so a
consumer that needs exactly-once must deduplicate on payload content.

On reconnect the connector resubscribes. When the broker resumed the session,
QoS 1 messages received on the old connection are discarded locally because the
broker redelivers them; QoS 0 messages, and every message when the session is
new, are kept. A publisher that sends at QoS 0 gets at most once delivery from
the broker regardless of the subscription: such a message is lost if the
connector dies before the runtime commits it. An ack is never
sent on a connection other than the one that received the message.

If the broker stays unreachable for a whole read wait, `read` fails instead of
reporting caught up.

## Scale-out

Set the same `shared_group` on several instances, each with its own
`client_id` and checkpoint state. Each subscribes to `$share/<group>/<filter>`
and the broker splits the messages between them; an unacknowledged message of a
dead instance is redelivered by the broker to the group.

## Fixture

`scripts/test-mqtt-container.sh` starts a disposable broker with generated
certificates on ephemeral loopback ports and runs `tests/mqtt.rs`: a process
killed between the sink ack and the broker ack gets the message redelivered
with nothing lost, a reconnect at QoS 1 on both protocols loses nothing, two
instances on a shared subscription split the load without loss, and TLS refuses
a client without a trusted certificate.
