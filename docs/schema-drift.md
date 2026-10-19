# Schema discovery and drift

`DiscoveredKind.schema` is a typed `Schema`: a nonzero source revision, field
names/types, and ordered key fields. Revisions increase whenever a schema
changes. Connectors must expose a change through discovery before returning
records that use it. The runtime discovers before each read; adapters own
payload normalization and must buffer a source-side schema transition until
that discovery boundary. Payload text remains adapter-defined.

New fields are additive. I32 to I64 or F64 and F32 to F64 are lossless
widenings. Other type changes, field removals, changed key fields, or a changed
schema without an increasing revision are breaking. Key-field type changes
are breaking even when the value type would otherwise widen.

The first discovery establishes a durable baseline. Subsequent changes emit
JSON `DriftEvent` payloads with record kind `__hashi_schema`, key equal to the
source record kind, and position equal to a durable per-kind event sequence.
That kind is reserved and rejected in source discovery. The event includes
previous/current schemas, classification, and reason. Controls share normal
sink delivery and store-and-forward ordering, but never advance source
checkpoints. Consumers must handle this reserved kind before interpreting
ordinary payloads. Delivery remains at least once; deduplicate by the normal
idempotency key. A rejected or unacknowledged control blocks reads and remains
pending on disk for retry. No automatic consumer migration occurs.

Breaking changes quarantine records of that kind with their original payload
and a reason; unrelated kinds continue. Those records advance the source
checkpoint after durable quarantine, so resolving does not replay them.
Keep quarantine data for a separate consumer remediation/replay workflow.
`Runtime::resolve_schema(kind, &schema)` explicitly accepts the exact recorded
quarantined schema after its control event has been acknowledged. The next run
emits a resolution control before delivering new records. A different revision
cannot accidentally clear quarantine. While quarantined, a further discovered
schema change stops the run until the recorded decision is resolved.

Schema state uses atomic replacement, file and directory fsync, and a separate
`schema-*.v1.json` file beside unchanged v1 checkpoints and quarantine logs.
Malformed or unknown state fails closed. Pending events survive restart and
replay with their original sequence. Instance state must travel together with
its source checkpoint during backup, restore, or takeover.

A run and explicit resolution hold an OS file lock per connector/instance.
Another process using the same state directory fails immediately; process
exit releases the lock. This is local durable-state ownership, not a cluster
lease. Independent directories for the same source are not coordinated. Do
not share an instance across nodes without storage that enforces the lock.

The discovery Rust API changes from opaque text to typed schemas. Sink data,
checkpoint, and quarantine encodings remain v1. Consumers need reserved-control
support before upgrading the producer. Older binaries do not enforce schema
quarantine; drain the instance before rollback and retain its complete state.
