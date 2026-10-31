# Historian backfill and live tail

`historian::Historian` is the one trait a historian adapter implements: tag
discovery, raw and interpolated time-range reads with paging, and a live tail
(by default the raw read; a subscribing historian serves it from samples it
keeps durably). `historian::HistorianConnector` turns any historian into a
connector.

## Backfill and handover

The connector backfills every tag from `start_ms`, then follows the tail.
Work runs in rounds over a window `(lower, upper]`, tag by tag in name order:

- The first round is the backfill, up to `now - settle_ms` when it opens. It
  reads each tag in `chunk_ms` time chunks, `page_limit` samples per read, at
  most `max_reads_per_second` reads.
- Each later round is a tail round from the previous `upper` to
  `now - settle_ms`. `settle_ms` leaves time for late writes to land.

Each tag reads strictly after its watermark, so the backfill hands over to the
tail with no gap and no overlap.

A round's bounds are written to the connector's `state_dir` (fsynced, then
renamed into place) before any of its records is delivered. A restart reuses
the saved round, so a record re-emitted after a crash has the same content
and position. A round that turns out empty is widened in place to the next
horizon instead of being followed by a new one, so the saved round always
starts where the last checkpointed round ended. The state directory is leased: a second connector on it is contended while
the hold is unexpired. A standby resumes from the last checkpointed round;
historian reads replay from their watermarks.

## Checkpoints

The checkpoint per tag lives in the connector cursor, which the runtime commits
after the sink acknowledges each record. The cursor holds the round, the tag
index, that tag's watermark and a fingerprint of the tag set; that implies
every tag's checkpoint (earlier tags are at `upper`, later ones at `lower`), so
it stays a few bytes for any number of tags. Discovery order does not matter;
a changed tag set or backfill start refuses the old cursor rather than
misapply it. A restart resumes from the committed cursor and re-emits the
same records at the same positions, so a redelivered record keeps its
idempotency key. A cursor from another configuration, or a position without a
cursor, is refused.

## Contract

`tail` must be replayable: called again with the same arguments, for example
after a restart, it returns the same samples. A subscribing adapter keeps
received samples durably until the watermark passes them.

Timestamps of one tag strictly increase; a historian that returns a sample at
or before the watermark, or outside the requested range, is refused. Records
use kind `historian.sample`, key `<tag>@<ts_ms>`, and a JSON payload with
`tag`, `ts_ms` and `value` (a number, or `"NaN"`, `"Infinity"` or
`"-Infinity"` for non-finite readings); the sample quality maps to the record quality.

## SQL mapping (MySQL)

Enable the `mysql` Cargo feature and construct
`mysql::historian::SqlHistorian` with connection options and a mapping: the
tag table (identity and name columns), the value table (tag, time, value, and
quality columns), and the database that holds them. `connect` checks that
mapping against the catalog and refuses a missing table, a missing column, a
time/value/name column of the wrong type, a nullable identity, time, value, or
name column, a tag identity or name that is not unique, or identity columns
that do not share a type and collation, before any sample is read. Raw reads select by the tag identity
column so a case-insensitive name collation cannot mix two tags. Time bounds
are millisecond watermarks even when the time column stores microseconds.
Call the synchronous API from a dedicated OS thread outside a Tokio runtime.
Verified TLS is required unless `allow_insecure_lab` explicitly permits a
disposable plaintext fixture.

Raw reads are `SELECT`s over `(after_ms, until_ms]`, ordered by the time
column, so a restart with the same window is replayable. Interpolated reads
are not offered.

## SQL mapping (SQL Server)

Enable the `mssql` Cargo feature and construct
`mssql::historian::SqlHistorian` with host, port, user, password, database,
and a mapping: the tag table (identity and name columns), the value table
(tag, time, value, and quality columns), and the schema that holds them
(`mapping.database`). `connect` checks that mapping against the catalog and
refuses a missing table, a missing column, a time/value/name column of the
wrong type, a nullable identity, time, value, or name column, a tag identity
or name that is not unique, or identity columns that do not share a type and
collation, before any sample is read. Raw reads select by the tag identity
column so a case-insensitive name collation cannot mix two tags. Time bounds
are millisecond watermarks even when the time column stores fractional
seconds. Call the synchronous API from a dedicated OS thread outside a Tokio
runtime. Verified TLS is required unless `allow_insecure_lab` explicitly
permits a disposable fixture with a server-selected certificate.

Raw reads are `SELECT`s over `(after_ms, until_ms]`, ordered by the time
column, so a restart with the same window is replayable. Interpolated reads
are not offered.

## OPC UA Historical Access

Enable the `opcua` Cargo feature. `opcua::historian::OpcUaHistorian` discovers
variables under the configured roots and reads paged raw history through the
historian trait. Interpolated reads are not offered. Discovered variables
without the history access bit are reported by `check`; the connector still
runs over the rest. Session, trust, and PKI setup are the same as
[opcua.md](opcua.md).

## Acceptance coverage

The parent historian scope in #6 is delivered by #22 (one trait, chunked
backfill, per-tag checkpoints, tail handover), #23 (OPC UA Historical Access),
#24 (MySQL mapping), and #68 (SQL Server mapping). The REST historian adapter
stays later.

```sh
CARGO_TARGET_DIR="$PWD/target" cargo test --locked --test historian_backfill
CARGO_TARGET_DIR="$PWD/target" cargo test --locked --features opcua-simulator --test opcua_historian
bash scripts/test-mysql-container.sh
bash scripts/test-mssql-container.sh
```

| Capability | Evidence |
| --- | --- |
| One trait: tag discovery, paged raw and interpolated reads, live tail by poll or subscription | [`Historian`](../src/historian.rs) |
| One-year backfill of 1k tags, checkpoints, resume after a kill, handover to the tail with no gap or overlap beyond the idempotency key | [Backfill fixture](../tests/historian_backfill.rs) |
| OPC UA Historical Access adapter: paged raw history, resume after a kill, tail handover, same-millisecond page refusal, session restart | [OPC UA historian fixture](../tests/opcua_historian.rs) |
| Variables without history reported by `check`; connector still runs | [OPC UA historian fixture](../tests/opcua_historian.rs) |
| Generic SQL historian adapter configured by mapping (tag table, value table, time column, quality column) on MySQL | [MySQL historian fixture](../tests/mysql_historian.rs) |
| Wrong mapping refused at connect; interpolated SQL reads not offered | [MySQL historian fixture](../tests/mysql_historian.rs) |
| Generic SQL historian adapter configured by mapping (tag table, value table, time column, quality column) on SQL Server | [SQL Server historian fixture](../tests/mssql_historian.rs) |
| Wrong SQL Server mapping refused at connect; interpolated SQL reads not offered | [SQL Server historian fixture](../tests/mssql_historian.rs) |

These checks establish fixture and simulator conformance. They do not certify
arbitrary plant historians. Vendor client libraries stay out of the default
build.
