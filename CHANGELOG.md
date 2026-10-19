# Changelog

## Unreleased

- Versioned typed schema discovery, additive and lossless widening detection,
  and durable per-kind quarantine for breaking changes. Drift control records
  use the existing sinks and replay safely after interruption. Explicit schema
  resolution resumes the kind; other kinds continue while it is quarantined.
  An exclusive instance lease prevents concurrent local checkpoint writers (#10).

- Pluggable sinks: JSON Lines, partitioned Apache Parquet, a file-backed
  Kafka-compatible idempotent topic log, and a versioned out-of-process
  platform SDK protocol.
  A bounded store-and-forward disk buffer acks after durable enqueue so a
  connector keeps reading while the uplink is down, then drains in order.
  Shared conformance covers ack-before-commit, resume, and per-key order.
  A thirty-minute uplink outage loses no records (#2).

- Connector runtime: `check`, `discover`, and `read` against a versioned
  record contract. Checkpoints live in the runtime and commit only after the
  sink acks. Delivery is at-least-once; each record carries an idempotency
  key. Poison records go to a quarantine log. A fake source covers snapshot,
  incremental, kill/resume, and quarantine (#1).
