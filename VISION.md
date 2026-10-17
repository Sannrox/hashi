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
1. **v0.1 MVP:** connector runtime and contract; OPC UA, MQTT and Modbus; MySQL CDC; schema discovery and drift quarantine; conformance and soak harness against containerized simulators; metrics and diagnostics.
2. **v0.2:** SQL Server and Oracle CDC; historian backfill; store-and-forward at the edge; write-back to OPC UA and MQTT behind caller approval.
3. **Later:** more historians and fieldbus gateways, connector hot reload, and horizontal partitioning of large sources.
