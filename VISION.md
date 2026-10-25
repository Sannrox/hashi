# Hashi vision

Hashi (橋, bridge) is a standalone connector engine. It bridges plant floors, devices and operational databases to the systems that govern and analyze that data.

## Vision
- One connector contract (check, discover, read) for every source family, with typed records, source timestamps, quality codes and stable record keys.
- Industrial protocols as first-class sources: OPC UA, MQTT (including Sparkplug B), Modbus and process historians, with backfill and live tail.
- Change data capture for MySQL, SQL Server and Oracle: snapshot-to-stream handoff, transaction boundaries, deletes and schema change events.
- Durable checkpoints, at-least-once delivery with idempotency keys, and resume after interruption without gaps.
- Pluggable sinks: the governed source SDK of the platform, Kafka-compatible topics, Parquet files and JSON Lines.
- No governance logic: credentials are scoped per connector, and the consumer owns identity mapping, markings and Actions.
- Suitable for edge and air-gapped sites: a single static binary, offline packaging, and a store-and-forward buffer when the uplink drops.

## Roadmap
1. **v0.1 MVP:** connector runtime and contract; pluggable sinks and store-and-forward; OPC UA, MQTT and Modbus; historian backfill and live tail (OPC UA Historical Access and MySQL mapping); MySQL CDC; schema discovery and drift quarantine; conformance and soak harness against containerized simulators; health, metrics, diagnostics, offline packaging and the default-build license gate.
2. **v0.2:** SQL Server and Oracle CDC; SQL Server historian mapping; write-back to OPC UA and MQTT behind caller approval.
3. **Later:** REST historian adapter; more historians and fieldbus gateways; connector hot reload; horizontal partitioning of large sources.

## v0.1 acceptance coverage

The parent MVP in #13 is delivered by #1 (runtime), #2 (sinks and store-and-forward), #3 (OPC UA), #4 (MQTT), #5 (Modbus), #6 (historian), #7 (MySQL CDC), #10 (schema drift), #11 (conformance and soak harness), and #12 (operations). SQL Server CDC (#8), Oracle CDC (#9), and SQL Server historian mapping (#68) stay in v0.2. 24-hour soak evidence (#29) does not block v0.1.

| Capability | Evidence |
| --- | --- |
| One connector contract: check, discover, read, durable checkpoints, at-least-once delivery | [`Connector`](src/connector.rs), [`Runtime`](src/runtime.rs), [run.md](docs/run.md) |
| Pluggable sinks (JSON Lines, Parquet, Kafka-compatible, platform SDK) and store-and-forward | [`Sink`](src/sink.rs), [`StoreAndForward`](src/buffer.rs) |
| OPC UA browse, subscriptions, historical access, certificate trust | [opcua.md](docs/opcua.md), [discovery fixture](tests/opcua_discovery.rs), [subscription fixture](tests/opcua_subscription.rs) |
| MQTT 3.1.1 and 5 with TLS, topic filters and Sparkplug B | [mqtt.md](docs/mqtt.md), [MQTT fixture](tests/mqtt.rs) |
| Modbus TCP and RTU polling | [modbus.md](docs/modbus.md), [Modbus fixture](tests/modbus_poll.rs) |
| Historian backfill and live tail | [historian.md](docs/historian.md) |
| MySQL snapshot and GTID change capture | [mysql.md](docs/mysql.md), [MySQL CDC fixture](tests/mysql_cdc.rs) |
| Schema discovery, drift detection and quarantine | [schema-drift.md](docs/schema-drift.md), [schema fixture](tests/schema_drift.rs) |
| Conformance kit per v0.1 family | [conformance.md](docs/conformance.md) |
| Soak harness with fault injection | [soak.md](docs/soak.md), [Modbus soak fixture](tests/soak_modbus.rs) |
| Health, metrics, diagnostics, offline packaging and license gate | [operations.md](docs/operations.md) |

These checks establish fixture and simulator conformance. They do not certify arbitrary plant sources. Vendor client libraries stay out of the default build.
