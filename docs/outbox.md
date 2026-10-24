# Outbox one-shot run mode

Some hosts run connectors sandboxed, with no network route to the
governance plane. Such a host gives the connector three files and a
directory, starts it, and applies what it wrote after it exits:

| Variable | What it names |
| --- | --- |
| `CONNECTOR_CHECKPOINT` | Read-only file with the last committed cursor; empty when never committed. |
| `CONNECTOR_CREDENTIAL` | File with the source credential (the MySQL password). |
| `CONNECTOR_CONFIG` | JSON configuration, below. |
| `CONNECTOR_OUTBOX` | Writable directory for the pages. |

`hashi outbox` (built with the `mysql` feature) reads MySQL change capture
from the committed cursor up to what the server had executed when the run
opened, writes the pages and exits: 0 when it wrote them (zero pages when
nothing changed), 1 when the run failed and wrote nothing usable.

## Pages

Each page is one `sekai.source-batch/v1` document named by sequence,
`000001.json`, `000002.json`, …, followed by an empty `000001.ok` once the
page file is fully written and synced. A run killed mid-page leaves at most
one unmarked page; apply only marked pages, in order. The outbox must hold
no earlier pages.

- The first page's `current_cursor` is the committed cursor; each later
  page's is the previous page's `proposed_next_cursor`.
- Pages carry no delivery metadata. They use the registered object-sync
  adapter (`adapter.registered.object_sync` 1.0.0) under the configured
  envelope.
- One page holds one table's rows and each row at most once, so a page ends
  where the table changes or a row repeats.
- Caps default to 64 pages a run and 8 MiB and 500 records a page; the
  configuration may lower them. The byte cap holds for the page as written
  (compact JSON).

## Cursor

The connector keeps no checkpoint of its own across runs; the host's
committed cursor is authoritative. A cursor names the last stream boundary
(a transaction end, or the end of the initial snapshot) and how many records
past it were already paged. A run replays the stream from that boundary and
skips those, so a page the host refused is written again on the next run
from the same cursor, with the same idempotency key and digest. A page's key
follows from where it starts and ends and what it holds: when the source
changed since (the run's last page reaches further, or a snapshot is taken
again), it is a new page with a new key, never a conflicting one.

The run keeps a spool for itself only, readable by its owner alone, and
removes it on exit; a run that was killed leaves one, which the next run
removes.

A run reads up to what the server had executed when it opened and stops at
the first transaction boundary at or past that, however busy the server is;
it fails if it cannot catch up within a minute.

A page holds at least one row, so the cursor moves with rows: transactions
that touch only other tables are read through, and the progress past them is
committed with the next row of a captured table. Keep the binlog retained
longer than the longest stretch without one.

A cursor also fingerprints the capture configuration (the source, the tables
and their keys, the schema revision; never the credential). A run under
another configuration refuses it: reset the binding's cursor to start over,
which takes a new snapshot.

The first run, from an empty cursor, takes the initial snapshot. The whole
snapshot must fit one run's caps, or the run fails and writes nothing. A
committed cursor inside the snapshot (some snapshot pages applied, a later
one refused) is refused: reset the binding's cursor to start over.

## Configuration

```json
{
  "envelope": {
    "namespace": "orders",
    "producer_identity": "connector.hashi",
    "source": "mysql",
    "source_instance": "shop",
    "type_digest": "sha256:<64 hex>"
  },
  "caps": { "max_pages": 64, "max_page_bytes": 8388608, "max_page_records": 500 },
  "url": "mysql://capture@db.internal:3306",
  "tables": [{ "database": "shop", "table": "orders" }],
  "server_id": 2048,
  "allow_insecure_lab": false
}
```

The MySQL prerequisites are those of [change capture](mysql.md). Plaintext
connections need `allow_insecure_lab: true`, for lab use only.
