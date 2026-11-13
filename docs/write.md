# Write-back

`hashi write` applies write commands that the host has already authorized. It
does not decide whether a write is allowed; it only refuses targets (OPC UA
node ids or MQTT topics) that the writer's configuration does not list. One run reads a command file, applies
each command in order, prints the results as JSON, and exits.

```sh
hashi write --config hashi.toml --writer line-1-setpoints commands.json
```

The exit code is 0 when every command was applied, 1 when any was refused or
unknown, and 2 for a usage, configuration, lease, or ledger error. Hashi
connects only when a command needs the target, so ids already in the ledger
are answered even while the server or broker is unreachable.

## OPC UA writer

```toml
[[writer]]
name = "line-1-setpoints"
target.opcua = { endpoint = "opc.tcp://plc:4840", application_uri = "urn:site:hashi-writer", pki_dir = "pki/writer", policy = "basic256sha256", credential_file = "secrets/writer", timeout_ms = 5000, allow = ["ns=2;s=Setpoint"] }
```

The connection fields mean what they mean on `source.opcua`
([OPC UA](opcua.md)). Review the server certificate for the writer's store
with `hashi trust list|approve --config hashi.toml --instance <writer name>`. Give the
writer its own `pki_dir`, because a running reader holds its PKI store
exclusively. Prefer a separate OPC UA user that can write only the nodes you
list. `allow` lists the node ids that a command may name. They are compared as
exact strings, so write them in the same form the host uses. A writer name
shares the checkpoint directory namespace with instances.

## MQTT writer

```toml
[[writer]]
name = "line-1-commands"
target.mqtt = { host = "broker", port = 8883, protocol = "v5", client_id = "hashi-line-1-writer", ca_file = "pki/broker-ca.pem", timeout_ms = 5000, allow = ["plant/line-1/setpoint"] }
```

The connection fields mean what they mean on `source.mqtt`
([MQTT](mqtt.md)): TLS through `ca_file` (and optionally
`client_certificate` with `client_private_key`), `credential_file` with the user
and password, and `allow_insecure_lab` for plaintext. Give the writer its own
`client_id`, because a broker drops the older of two sessions that share one.
`allow` lists the topic names that a command may name. They are compared as
exact strings, and wildcards are refused. Limit the writer's broker user to
those topics as well.

Each command is one QoS 2 publish, not retained, whose payload is the command
value as compact JSON text: `42.5`, `true`, `"on"` (with the quotes), or
`{"sp":42}`. A packet over 256 KiB is refused before sending.

`applied` means the broker completed the QoS 2 exchange (PUBCOMP). That is
broker delivery, not confirmation by a device or any subscriber. An MQTT 3.1.1
broker may also complete the exchange for a publish its access rules drop. An
MQTT 5 broker that answers with a failure reason gives `refused`. Session loss
or no PUBCOMP within `timeout_ms` gives `unknown`.

The session is clean and never reconnects, because a resumed session would
resend an unconfirmed publish. After a publish ends `refused` or `unknown`, the
session is closed and the remaining commands in that run are refused before
sending. Rerun them with the same ids. `hashi trust` does not apply to MQTT
writers.

## Commands and results

```json
{"version": 1, "commands": [
  {"id": "batch-17-setpoint", "target": "ns=2;s=Setpoint", "value": 42.5}
]}
```

`id` is chosen by the host and identifies one intended write for the lifetime
of the writer's ledger. For MQTT, `target` is a topic and `value` is any JSON
value (see above). For OPC UA, `value` is a JSON scalar. Hashi reads the node's
DataType and converts the value: Boolean, the signed and unsigned integer types
(the value must fit), Float, Double, Duration, and String. Other types are
refused.

```json
{"version": 1, "results": [
  {"id": "batch-17-setpoint", "outcome": "applied"}
]}
```

| Outcome | Meaning | Same id again |
| --- | --- | --- |
| `applied` | OPC UA: the server returned Good for the write. MQTT: the broker sent PUBCOMP. | Answers `applied` from the ledger without sending. |
| `refused` | The write was not applied. The `reason` field says why: not in `allow`, no connection, value does not fit, node unreadable, the server returned a Bad status, or an MQTT 5 broker returned a failure reason. | Pre-send refusals are not recorded and may be retried. A server refusal is recorded and answers again. |
| `unknown` | The request was sent but no clear answer arrived (lost session, timeout), or an earlier run crashed after recording intent. | Answers `unknown` without sending. Check the node and use a new id if the write is still wanted. |

An id reused with a different target or value is refused without sending.

## At most once

Each writer keeps a ledger, `checkpoint_dir/<writer>/writes.v1.log`. Before
sending, a run records the command's intent there, synced to disk. After the
response, it records the outcome. A run holds the writer's lease (the `[lease]`
store that instances use), and every ledger append is admitted at a fence
with the lease's fencing token. The fence stays locked from the intent
through the send to the outcome, so a successor never acts while a
predecessor can still send. A second `hashi write` for the same writer
fails until the first releases the lease or its hold expires. A process that takes over after a crash admits
its token before reading the ledger, so a paused predecessor cannot record a
new intent afterwards. A command whose intent was recorded without an outcome
is reported as `unknown` and is never sent again.

A run that loses its lease or fence mid-file exits 2 without printing
results, including for commands it already applied. Rerun the same file to
read them from the ledger. A successor that finds a predecessor paused inside
a send waits for the fence; if its own hold lapsed meanwhile, it exits 2 and a
rerun continues.

The OPC UA session is opened without automatic reconnects, and the write
request is never retried. After a write ends `unknown`, the session is closed,
and the remaining commands in that run are refused. Each command waits up to
`timeout_ms` for the server, so a stalled server makes a long file slow to
refuse.

The ledger grows with every sent command. It is never compacted in this
release.
