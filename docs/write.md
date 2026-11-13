# OPC UA write-back

`hashi write` applies write commands that the host has already authorized. It
does not decide whether a write is allowed; it only refuses node ids that the
writer's configuration does not list. One run reads a command file, applies
each command in order, prints the results as JSON, and exits.

```sh
hashi write --config hashi.toml --writer line-1-setpoints commands.json
```

The exit code is 0 when every command was applied, 1 when any was refused or
unknown, and 2 for a usage, configuration, lease, or ledger error. Hashi
connects only when a command needs the server, so ids already in the ledger
are answered even while the server is unreachable.

## Writer

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

## Commands and results

```json
{"version": 1, "commands": [
  {"id": "batch-17-setpoint", "target": "ns=2;s=Setpoint", "value": 42.5}
]}
```

`id` is chosen by the host and identifies one intended write for the lifetime
of the writer's ledger. `value` is a JSON scalar. Hashi reads the node's
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
| `applied` | The server returned Good for the write. | Answers `applied` from the ledger without sending. |
| `refused` | The write was not applied. The `reason` field says why: not in `allow`, no connection, value does not fit, node unreadable, or the server returned a Bad status. | Pre-send refusals are not recorded and may be retried. A server refusal is recorded and answers again. |
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

The session is opened without automatic reconnects, and the write request is
never retried. After a write ends `unknown`, the session is closed, and the
remaining commands in that run are refused. Each command waits up to
`timeout_ms` for the server, so a stalled server makes a long file slow to
refuse.

The ledger grows with every sent command. It is never compacted in this
release.
