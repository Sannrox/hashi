# 0001. Single package crate graph

Status: accepted
Source: #65

## Context

Hashi is one Cargo package: the runtime, sinks, and protocol adapters share
`src/`. Protocol SDKs (`mysql_async`, `rumqttc`, `async-opcua`, `tokio`) were
already optional. `parquet` and `serialport` were always compiled. A crate-graph
survey of the tree (one member, ~20k Rust lines, 90-day hot set covering almost
every module) asked whether to cut workspace crates.

Incremental compile from a warm cache, after a one-line edit in `src/runtime.rs`:

| Command | Duration |
| --- | --- |
| `cargo check --locked` | 0.33s |
| `cargo check --locked --all-features --all-targets` | 1.31s |
| `cargo test --locked --all-features --no-run` | 5.28s |

A leaf integration-test edit (`tests/modbus_poll.rs`) checked in 0.29s. Those
times already reuse the `parquet` and protocol crates; rustc only rechecks the
hashi package.

## Decision

Stay one package. Do not cut `hashi-core`, protocol crates, or a Parquet crate
until a later measurement on this tree shows a compile-cache win that pays for
the workspace.

Isolate weight that the hot modules do not call:

- default features `parquet` and `modbus` keep the product graph (license gate,
  `Dockerfile`, `cargo test`) the same
- `cargo check --no-default-features` skips `parquet` and `serialport`
- `hashi::modbus::Parity` replaces `serialport::Parity` on `Endpoint::Rtu`

A cut is earned when a hot-module `cargo check --locked --all-features --all-targets`
from a warm cache exceeds about five seconds, or when a protocol-local
`cargo test -p <crate> --no-run` would skip relinking the other families by
enough to matter.

## Consequences

Callers that named `serialport::Parity` in `Endpoint::Rtu` use
`hashi::modbus::Parity`. Default builds are unchanged. Protocol families stay
modules behind the existing optional features.
