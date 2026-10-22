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
    sparkplug_state_dir: None,
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

## Sparkplug B

A route with `Decoder::SparkplugB` reads the `spBv1.0/<group>/<type>/<edge
node>[/<device>]` namespace, for example `spBv1.0/plant-1/#`. It needs
`sparkplug_state_dir` and cannot use `shared_group`: births and the data that
refers to them must reach the same instance.

- Each metric becomes a record of the route's kind keyed
  `<group>/<edge node>[/<device>]/<metric>`, with JSON `type` (message type),
  `seq`, `name`, `datatype` and `value`. Signed integers are decoded from their
  two's complement encoding; bytes and files as hex. Data sets and templates
  are emitted with a `bad:` quality and a null value.
- NBIRTH, DBIRTH, DDEATH and NDEATH also emit a record keyed by the node or
  device path. An NDEATH whose `bdSeq` does not match the current NBIRTH comes
  from an older session and is ignored; a redelivered one emits its record
  again. An NDEATH invalidates the node's certificates and a DDEATH the
  device's, until the next birth. A data metric whose datatype differs from
  its birth certificate gets a null value and a `bad:` quality.
  NCMD, DCMD and STATE are ignored.
- Births register each metric's name, alias and datatype per edge node, so
  data may carry only the alias. Birth state is written atomically to
  `sparkplug_state_dir` on every birth and death and read back on connect, so a
  restarted connector resolves aliases of messages queued while it was away.
  An exclusive lease on the directory fences a second instance.
- A data message from a node without a birth, one whose sequence number skips,
  or one with a metric missing from the certificates makes the connector
  publish an NCMD `Node Control/Rebirth` request (QoS 0, at most once per
  `timeout` per node). Known aliases keep resolving.
- An NBIRTH without `seq` or `bdSeq`, a topic whose shape does not match its
  message type, or a `seq` above 255 changes no state. A birth that declares an
  alias or a name twice leaves that identity unresolved.
- A metric whose alias or name is not in a birth certificate (a name and
  datatype on a data message do not count; a name and alias must match the
  same certificate), or a message that
  is not Sparkplug B, becomes a `__sparkplug_unresolved` record: the runtime
  quarantines it with its alias and reason. It is never guessed.

A message is acknowledged after the last of its records is committed.

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
The Sparkplug B proofs use an edge node simulator that answers rebirth
requests: birth, alias resolution and device lifecycle, a sequence gap that
triggers one rebirth with an unknown alias quarantined, and a reconnect at
QoS 1 that resolves queued alias-only data from durable birth state.
