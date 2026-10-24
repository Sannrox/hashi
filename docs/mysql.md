# MySQL snapshot and change capture

Enable the `mysql` Cargo feature and construct `mysql::MySqlConnector` with a
stable source id, connection options, selected tables, dedicated state directory,
chunk size, staged-batch byte limit, unique replication server id, schema revision,
and timeout. Call the synchronous API from a dedicated OS thread outside a Tokio
runtime. Supply credentials through the embedding application's credential custody.
Diagnostics and durable state omit credentials and raw protocol errors.

Verified TLS is required unless `allow_insecure_lab` explicitly permits a disposable
plaintext fixture. Invalid certificates, disabled domain checks, and TLS hostname
overrides are refused outside that fixture mode. The adapter does not create trust
or weaken connection verification.

## Prerequisites

The source requires GTID mode and GTID consistency, ROW binlogs, FULL row images,
and FULL row metadata. Cascading foreign keys are refused because their child
changes are absent from the row stream. Selected tables must use InnoDB and have a primary key or
an explicitly configured, complete, non-null unique key. Prefix and functional
indexes cannot establish identity. ENUM/SET/BIT identity columns are refused because
bound snapshot values do not reliably share their source ordering semantics. Text columns use UTF-8 or ASCII; virtual
generated columns and TIME(1)/TIME(2) are refused. The latter avoids unsafe
negative-fraction decoding in the locked row decoder. Unsupported row images and binlog events fail
visibly before their transaction is exposed.

`check` verifies the connection, source modes, SELECT access to each selected
table, and effective global REPLICATION SLAVE, REPLICATION CLIENT, and RELOAD or
FLUSH_TABLES privileges, including enabled roles. Replication authorization is
also enforced by the server when the stream opens. Reserve a unique `server_id`
for this connector so another replication client cannot displace its session.

## Snapshot and handoff

A short global read lock brackets creation of a repeatable-read snapshot and
capture of its GTID set. Metadata locks are acquired on selected tables before
the global lock is released. Reads use primary/unique-key pagination in configured
chunks; DDL waits for the snapshot transaction to finish. The timeout bounds the
whole snapshot. A complete snapshot must fit `max_batch_bytes`; no partial snapshot
is delivered when it exceeds that bound.

The source stages the complete snapshot into an atomically replaced, fsynced
batch file, then publishes its durable manifest. A crash before the manifest
restarts an unpublished snapshot. A crash after publication replays its remaining
records with the same order positions and idempotency identities. Binlog reading
starts from the captured GTID set, so writes committed during snapshot construction
follow the snapshot instead of disappearing between the two paths.

Each source transaction is fully staged before its first record is returned.
Insert, update, and delete records include change metadata; updates and deletes
retain their before image. Transaction begin/end markers use the discovered
`__mysql_transaction` kind. GTIDs advance on the transaction-end cursor. A key-changing
update retains the old key in its before image; consumers own identity remapping
and transaction application. Markers do not promise atomic application by a sink.

The runtime owns checkpoint commits after sink acknowledgement. The source manifest
owns received batches, not runtime delivery. Preserve both stores. Missing,
regressed, or mismatched checkpoint/cursor state fails closed. A per-directory
exclusive lease fences concurrent local spool writers and releases on process death.
This does not implement a cross-node ownership protocol.

## Types and schema changes

Integers and finite floating-point values use JSON numbers when their discovered
type can represent them. Unsigned BIGINT uses lossless text. Decimal values use
text; dates and times use stable textual forms. TIMESTAMP uses UTC epoch seconds
with six fractional digits in both snapshot and binlog paths. Binary fields retain
byte arrays. JSON values use canonical JSON text. ENUM and SET labels are decoded
from each historical table map, rather than from the latest catalog definition.

DDL emits a transaction boundary before changed table discovery becomes visible.
The runtime's existing drift handling then announces or quarantines the changed
kind before subsequent data. Historical row maps must match column names, wire
types, signedness, primary-key identity, and binary/text classification. If multiple DDLs overtake the
reader and the historical layout cannot be established, it stops before decoding
rows under the wrong schema. Operator schema recovery is required; it does not
guess a migration from the newest catalog. Sources with explicitly configured keys
stop at ALTER statements because historical table maps do not prove secondary
unique-index continuity.

CREATE statements are refused because their binlog query can precede row events
in the same GTID transaction. Selected tables must already exist at snapshot time.

DROP, table RENAME, TRUNCATE, partition/table-space operations, and data-transforming
ALTER operations (including column DROP/RENAME, MODIFY, CHANGE, CONVERT, ENGINE, FORCE,
and foreign-key additions) are
refused before GTID advancement because catalog existence cannot establish
continuity and row-deletion events may be absent. Keywords are recognized across
whitespace and ordinary comments; executable comments and ambiguous SQL escapes
require explicit recovery. Dropped or incompatible tables,
compressed transaction payloads, and unsupported statement events also stop visibly.
These cases require an explicit consumer recovery plan.

## Restart and re-snapshot

Restart with the same source configuration, manifest, batch files, and runtime
checkpoint. Connection credentials may rotate without changing source identity.
A read failure marks the reader unhealthy; restart retries from durable state.
Requested source heartbeats keep quiet streams serviced within the per-event
timeout without inventing records or advancing GTIDs. A heartbeat between
transactions returns `None` when caught up. The timeout also bounds stream setup,
a complete transaction read, and catalog refresh; an incomplete timed-out read
requires durable restart.

A missing retained binlog or incompatible GTID set is reported as a recovery
requirement. Re-snapshot is an explicit new source generation/consumer recovery,
not an automatic checkpoint reset. Do not remove a spool or replace a checkpoint
with older state to bypass the check. Generation fencing and consumer remapping
remain outside this adapter.

## Sandboxed hosts

A host with no network route to the plane can run change capture once per
start with `hashi outbox`, from a committed cursor file into an outbox
directory. See [the outbox run mode](outbox.md).

## Disposable conformance fixture

```sh
bash scripts/test-mysql-container.sh
```

The harness owns a pinned database image, isolated container, ephemeral loopback
port, memory-backed fixture data directory, and test process. Connector manifests,
batches, and checkpoints remain on the test filesystem. Cleanup removes only that
container and its volumes. Parallel invocations have distinct containers, ports,
state paths, and build directories per worktree.

The fixture exercises process kill and takeover, writes committed during a 10,000-row
snapshot, transaction ordering/before images/deletes, unsigned and temporal values,
ENUM/SET/JSON/binary normalization, DDL drift and historical mismatch, prefix-index
rejection, required metadata, expired retention, and TRUNCATE refusal. Snapshot
readiness is observed through transaction-duration metadata locks, including
read-only transactions that need not appear in the storage transaction view.
