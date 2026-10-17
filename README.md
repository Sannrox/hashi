# Hashi

Hashi is a standalone connector engine written in Rust. It brings industrial and operational data into the rest of the stack.

- **Industrial:** OPC UA (browse, subscriptions, historical access), MQTT 3.1.1 and 5 (including Sparkplug B payloads), Modbus TCP and RTU, and process historians.
- **Change data capture:** MySQL, SQL Server and Oracle, each with a consistent snapshot followed by a resumable change feed. PostgreSQL follows the same contract.
- **One connector contract:** check, discover and read, with server-owned checkpoints, at-least-once delivery and idempotency keys, so a consumer can apply records exactly once.

Hashi doesn't make governance decisions. It authenticates to sources, reads, normalizes and delivers. Mapping records to objects, markings and Actions belongs to the consumer. It runs as a single binary or as one process per connector, with no runtime network calls besides the configured sources and sinks.

Status: early. See [VISION.md](VISION.md) for the roadmap and GitHub Issues for planned work.

License: Apache-2.0.
