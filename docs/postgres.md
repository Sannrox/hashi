# PostgreSQL snapshot and change capture

Enable the `postgres` Cargo feature and construct `postgres::PostgresConnector`
with a stable source id, host, port, user, password, database, selected tables,
dedicated state directory, chunk size, staged-batch byte limit, schema revision,
timeout, and maximum lag. Call the synchronous API from a dedicated OS thread
outside a Tokio runtime. Supply credentials through the embedding application's
credential custody. Diagnostics and durable state omit credentials and raw
protocol errors.

Verified TLS is required unless `allow_insecure_lab` explicitly permits a
disposable plaintext fixture. The adapter does not create trust or weaken
connection verification outside that fixture mode.

`source.postgres` is the run-file section. It is not `lease.backend = "postgres"`;
that lock store stays the `lease-postgres` feature.

## Prerequisites

The source requires `wal_level=logical`, a primary that is not in recovery, and
selected tables that already exist. Each selected table must be an ordinary
table (a partitioned parent is refused; a partition may be selected on its
own). Snapshot and publication use `ONLY`, so inheritance children are not
captured unless selected themselves. Slot and publication names include the
source id and database so two databases on one cluster do not share a slot.
Each needs a non-null primary key or an explicitly configured complete
non-null unique key that matches an unconditional unique index, and replica
identity `FULL` (or `DEFAULT` when the selected key is that primary key).
`FULL` is what supplies a before image for updates and deletes. Generated
columns are omitted from snapshot and stream so layouts stay aligned with
`pgoutput`.

`check` verifies the connection, logical WAL, SELECT access to each selected
table, that the replication slot still exists with a restart LSN, and that
the connected walsender flush lag stays within `max_lag`. A dropped slot or WAL recycled past the
checkpoint fails closed and requires an explicit new snapshot.

The connector creates a publication and a logical replication slot named from
the source id (`pgoutput`). Extra decoding plugins are not used.

## Snapshot and handoff

A replication-protocol `CREATE_REPLICATION_SLOT ... EXPORT_SNAPSHOT` exports a
consistent snapshot and its LSN. A repeatable-read transaction on a second
connection reads selected tables from that snapshot in primary-key pages, then
publishes an atomically replaced, fsynced batch and the snapshot LSN. Stream
reads start at that LSN. A crash before the manifest restarts the snapshot
(dropping a leftover slot). A crash after publication replays remaining records
with the same order positions and idempotency identities.

Each logical transaction is fully staged before its first record is returned.
Insert, update, and delete records include change metadata; updates and deletes
retain a before image when replica identity supplies one. An unchanged TOAST
marker on an update is filled from that before image; without one, capture
fails closed. Transaction begin/end markers use the discovered
`__postgres_transaction` kind. Cursors advance on the
commit LSN. Markers do not promise atomic application by a sink.

The runtime owns checkpoint commits after sink acknowledgement. The source
manifest owns received batches, not runtime delivery. Preserve both stores.
Missing, regressed, or mismatched checkpoint/cursor state fails closed. A
per-directory fencing lease fences concurrent spool writers.

## Schema changes

A relation message whose columns differ from the stored layout is a schema
event: the connector keeps the WAL relation layout, revalidates unique keys
against the catalog, and bumps the kind version. A unique-key change, schema
change mixed into a data transaction, TRUNCATE, or unsupported column type
fails closed. Operator recovery is a new snapshot when continuity cannot be
proved.

## Restart

Restart with the same source configuration, manifest, batch files, and runtime
checkpoint. Connection credentials may rotate without changing source identity.
A read failure marks the reader unhealthy; restart retries from durable state.

## Disposable conformance fixture

```sh
bash scripts/test-postgres-container.sh
```
