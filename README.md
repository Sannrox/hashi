# Hashi

Hashi is a standalone connector engine written in Rust. It brings industrial and operational data into the rest of the stack.

- **Industrial:** OPC UA (browse, subscriptions, historical access), MQTT 3.1.1 and 5 (including Sparkplug B payloads), Modbus TCP and RTU, and process historians.
- **Change data capture:** MySQL snapshot and GTID capture, and SQL Server snapshot with LSN change data capture (change tracking fallback), are implemented. Oracle and PostgreSQL follow the same contract on the roadmap.
- **One connector contract:** check, discover and read, with server-owned checkpoints, at-least-once delivery and idempotency keys, so a consumer can apply records exactly once.

The v0 runtime is in-tree: a `Connector` trait, a file-backed checkpoint store, and a sequential run loop that commits after the sink acks. Sinks: JSON Lines, partitioned Apache Parquet, a file-backed Kafka-compatible topic log (idempotent producer, key = record key; no live broker in `cargo test`), and a versioned platform source-adapter protocol. A store-and-forward disk buffer keeps reading while the uplink is down. Modbus polling, optional OPC UA secure discovery/subscriptions, and optional MySQL snapshot/GTID capture are implemented. Other source families follow the roadmap. Tests use fixture sources and isolated simulators.

Default Cargo features are `parquet` and `modbus`. `mqtt`, `mysql`, `mssql`, `opcua`, and `opcua-simulator` stay off until selected. `cargo check --no-default-features` builds the runtime without Parquet or serialport. The crate graph stays one package ([docs/decisions/0001-single-package-crate-graph.md](docs/decisions/0001-single-package-crate-graph.md)).

Hashi doesn't make governance decisions. It authenticates to sources, reads, normalizes and delivers. Mapping records to objects, markings and Actions belongs to the consumer. It runs as a single binary or as one process per connector, with no runtime network calls besides the configured sources and sinks.

Status: v0.1 MVP families are in tree. SQL Server CDC and SQL Server historian mapping are in tree; Oracle CDC stays on the v0.2 roadmap. See [VISION.md](VISION.md).

License: Apache-2.0. The default build admits only permissive dependency
licenses; `cargo deny check licenses` enforces it (`deny.toml`).

## Connector contracts

- [Running from a config file](docs/run.md)
- [Health, metrics, diagnostics and offline packaging](docs/operations.md)
- [Source cursors and change records](docs/source-cursors.md)
- [Schema drift and quarantine](docs/schema-drift.md)
- [Modbus polling](docs/modbus.md)
- [OPC UA sessions, discovery, and subscriptions](docs/opcua.md)
- [MySQL snapshot and change capture](docs/mysql.md)
- [SQL Server snapshot and change capture](docs/mssql.md)
- [MQTT subscriptions](docs/mqtt.md)
- [Historian backfill and live tail](docs/historian.md)
- [Connector conformance kit](docs/conformance.md)
- [Soak harness](docs/soak.md)
