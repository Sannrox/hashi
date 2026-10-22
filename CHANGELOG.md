# Changelog

## Unreleased

- A connector conformance kit (`hashi::conformance`) checks every v0.1 family
  against a live fixture: check, discover, snapshot, incremental, interruption
  and resume, duplicate keys and schema drift. Modbus and OPC UA run it on
  every pull request; MQTT and MySQL run it in their container jobs (#28).

- Sparkplug B decoding on the MQTT connector: births, deaths, metric
  aliases and sequence numbers, with NCMD rebirth requests on a gap or an
  unknown node. Birth state is durable, so a restart still resolves aliases.
  A metric that cannot be resolved is quarantined, never guessed (#21).

- `hashi run --listen <addr>` serves `/healthz` and Prometheus `/metrics` per
  instance: delivered and quarantined counts, errors, records per second,
  checkpoint position and age, and source lag. `hashi diagnostics` prints a
  support bundle without secrets, keys or payloads. A `Dockerfile` builds a
  static binary on an empty, digest-pinned image that runs with no network
  beyond configured sources and sinks (#27).

- Optional MQTT 3.1.1 and 5 connector with verified TLS, client certificates or
  user name and password, persistent QoS 1 sessions, shared subscriptions for
  scale-out, and topic filters mapped to record kinds with JSON or raw
  decoders. A message is acknowledged to the broker only after the runtime
  committed it, so a crash redelivers instead of losing it (#20).

- OPC UA servers with history support backfill through the historian trait
  (`opcua::historian::OpcUaHistorian`). Discovered variables without the
  history access bit are reported by `check`, and the connector still runs
  over the rest (#23).

- One historian trait (tag discovery, paged raw and interpolated reads, live
  tail) and a connector that backfills in rate-limited time chunks with a
  checkpoint per tag in the cursor, then hands over to the live tail with no
  gap or overlap (#22).

- `hashi run --config <file>` runs connector instances from a TOML config
  that declares sources, sinks and the checkpoint directory, and resumes
  after a restart. Credentials are file paths read on every use, so a rotated
  file applies without a restart; their values never reach logs or errors.
  Invalid configs are refused naming the field (#26).

- CI refuses copyleft (GPL, LGPL, AGPL) and proprietary or unlicensed
  dependencies in the default build with a `cargo deny` license gate
  (`deny.toml`); optional features are reported separately and do not block
  it (#25).

- Add optional paged OPC UA raw-history backfill and durable reconnect gap recovery before monitored tail delivery.

- Add optional MySQL consistent snapshots and GTID change capture with durable
  staged batches, before images, transaction markers, schema drift boundaries,
  strict source/key validation, and disposable crash/retention fixtures (#7).

- Add durable OPC UA monitored-item reads with sampling, absolute deadband, status quality, reconnect recovery, and a 10,000-item simulator measurement.

- Contract v2 commits an opaque source cursor with each order position and
  carries optional change operations, before images, transaction ids, and
  commit times through every sink and quarantine. V1 state remains readable;
  records without source metadata retain their previous encodings (#19).

- Optional OPC UA secure session and bounded address-space discovery. Explicit
  application trust stores reject and retain unknown certificates; username
  and X.509 user authentication support the three modern security policies.
  Policy None requires an explicit lab flag. Discovery includes node ids,
  browse paths, raw data types, value rank, and engineering units. A shared
  simulator fixture runs natively and in an isolated container (#16).

- Read-only Modbus TCP and serial RTU polling with declarative maps, all four
  data tables, integer/float decoding, independent word/byte order, scaling,
  units, bounded batches, per-device rate limits, and failure backoff. Samples
  spool durably until acknowledged and read failures produce bad quality (#5).

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
