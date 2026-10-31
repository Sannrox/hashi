# SQL Server snapshot and change capture

Enable the `mssql` Cargo feature and construct `mssql::MssqlConnector` with a
stable source id, host, port, user, password, database, selected tables, dedicated
state directory, chunk size, staged-batch byte limit, schema revision, timeout,
and maximum lag. Call the synchronous API from a dedicated OS thread outside a
Tokio runtime. Supply credentials through the embedding application's credential
custody. Diagnostics and durable state omit credentials and raw protocol errors.

Verified TLS is required unless `allow_insecure_lab` explicitly permits a disposable
fixture with a server-selected certificate. The adapter does not create trust or
weaken connection verification outside that fixture mode.

## Prerequisites

Change data capture is the primary path. The database must have CDC enabled, each
selected table must have a capture instance, SQL Server Agent must be running, and
cleanup retention must cover `max_lag`. Selected tables need a non-null primary
key. Snapshot isolation must be allowed so a consistent staged snapshot can be
taken without blocking writers.

When CDC is not enabled, change tracking on the database and each selected table
is the fallback. Change tracking has no before image. Writing to SQL Server is
out of scope.

`check` verifies the connection, CDC or change tracking, Agent (for CDC), SELECT
access, and that retention covers the configured lag. A checkpoint LSN below the
retained minimum fails closed and requires an explicit new snapshot.

## Snapshot and handoff

A snapshot waits until change data capture has a max LSN that covers each
selected capture instance. A snapshot-isolation transaction then stages complete
selected tables in primary-key pages, then publishes an atomically replaced,
fsynced batch and the LSN (or change-tracking version) captured with that snapshot. Stream reads start after
that cursor. A crash before the manifest restarts the snapshot. A crash after
publication replays remaining records with the same order positions and
idempotency identities.

Each CDC transaction (`__$start_lsn`) is fully staged before its first record is
returned. Insert, update, and delete records include change metadata; CDC updates
and deletes retain a before image when the capture instance supplies it.
Transaction begin/end markers use the discovered `__mssql_transaction` kind.
Cursors advance on the transaction-end LSN. Markers do not promise atomic
application by a sink.

The runtime owns checkpoint commits after sink acknowledgement. The source
manifest owns received batches, not runtime delivery. Preserve both stores.
Missing, regressed, or mismatched checkpoint/cursor state fails closed. A
per-directory fencing lease fences concurrent spool writers.

## Capture instances and schema changes

After DDL, enable a new capture instance for the table. The connector follows the
newest instance whose start LSN covers the checkpoint. Earlier instances remain
readable until that LSN. A newer instance is queried from its own min LSN, not
from a checkpoint that predates it. Dropped retention below the checkpoint, or a cursor the
cleanup job has purged, fails visibly. Operator recovery is a new snapshot.

## Restart

Restart with the same source configuration, manifest, batch files, and runtime
checkpoint. Connection credentials may rotate without changing source identity.
A read failure marks the reader unhealthy; restart retries from durable state.
`resume` opens at a committed LSN or change-tracking version in an empty state
directory for one-shot outbox hosts.

A mapped SQL historian (`mssql::historian::SqlHistorian`) uses the same driver
and disposable fixture; see [historian.md](historian.md).
