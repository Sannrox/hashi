# Source cursors and change records

Contract v2 adds `Record.source: Option<SourceMetadata>`. Its cursor is an
opaque UTF-8 string beside the runtime's monotonic u64 position. Optional
change information contains insert, update, delete, transaction-begin, or
transaction-end; an optional before image; transaction id; and commit time.
Transaction markers describe source boundaries, not a promise that Hashi
commits a multi-record transaction atomically at the consumer.

`Connector::read(from, source_cursor)` receives the last committed order
position and opaque source cursor together. The runtime neither parses nor
compares the cursor. A source may use its own cursor comparison rules, but
must return increasing order positions and stable idempotency identities.
Once a cursor is emitted, every subsequent record must include its next
cursor. Missing cursors fail before delivery or checkpoint advancement.
Cursors are at most 4096 bytes and transaction ids at most 1024 bytes; an empty
cursor is distinct from absence. Before images count against the configured
record-payload byte limit.

A sink acknowledgement precedes one atomic checkpoint replacement containing
both position and cursor. A failure after acknowledgement replays with the
same idempotency key. Poison records preserve source metadata in quarantine
before the same atomic checkpoint advancement. Consumers own exactly-once
application and any decision to replay quarantined records.

## Encoding compatibility

| Surface | Legacy records | Records with source metadata |
| --- | --- | --- |
| Buffer/topic delivery lines | Exact v1 encoding | v2 with one appended escaped JSON field |
| JSON Lines | Existing envelope | Optional `source` JSON member |
| Parquet | Existing eight columns | Optional ninth `source_metadata` text column |
| Platform frames | Protocol v1 and existing bytes | Protocol v2 and appended source-metadata JSON |
| Quarantine | Readers accept v1 rows | Writers append v2 rows with metadata |
| Checkpoints | Readers accept four-line v1 and five-line v2 state | Writers store six-line v3 state including optional cursor and fencing token |

The checkpoint and quarantine filenames retain their historical `.v1` slot
names. This is deliberate: v2 must replace the authoritative checkpoint in the
same slot, rather than leaving an older file that an older binary could resume
from. File and directory fsync plus atomic replacement remain the durability
boundary. Quarantine logs can contain complete v1 and v2 rows. Unknown versions
and unreadable metadata fail closed without exposing cursor contents in errors.
The separate schema-decision file keeps its existing format.

## Upgrade and rollback

Stop the older instance, retain its full state, and start the new binary against
that state. The per-instance fencing lease prevents overlapping writers. A v1
checkpoint loads with no cursor and token 0; a v2 checkpoint loads with its
cursor and token 0. The connector must support that position-only resume or
explicitly refuse it. The first successful v3 checkpoint upgrades its contents
in place, including the fencing token. Buffered legacy records still load and
drain. Extra checkpoint fields still fail closed.

Older binaries cannot read a v3 checkpoint. They fail closed on the upgraded
file instead of resuming stale state. Rollback therefore requires an explicit
compatible state/consumer recovery plan; copying an old checkpoint over
acknowledged work is unsafe. Mixed v1/v2 consumers remain compatible only for
records without source metadata. Upgrade consumers before enabling
source-aware records.
