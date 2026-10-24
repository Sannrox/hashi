//! Connector runtime: check, discover, read, durable checkpoints, at-least-once delivery.
//!
//! Protocol connectors and diagnostics are added independently.
//! This crate owns the versioned record contract, the runtime, and pluggable
//! sinks (JSON Lines, Parquet, Kafka-compatible, platform SDK) plus a
//! store-and-forward buffer.

pub mod buffer;
pub mod checkpoint;
pub mod config;
pub mod conformance;
pub mod connector;
pub mod contract;
pub mod credential;
pub mod delivery;
pub mod diagnostics;
pub mod fake;
pub mod historian;
pub mod jsonl;
pub mod kafka;
pub mod modbus;
#[cfg(feature = "mqtt")]
pub mod mqtt;
#[cfg(feature = "mysql")]
pub mod mysql;
pub mod observe;
#[cfg(feature = "opcua")]
pub mod opcua;
pub mod outbox;
pub mod parquet;
pub mod platform;
pub mod runtime;
pub mod schema;
pub mod service;
pub mod sink;
pub mod soak;

pub use buffer::StoreAndForward;
pub use checkpoint::{Checkpoint, CheckpointStore, FileCheckpoint, QuarantineRow};
pub use config::Config;
pub use connector::Connector;
pub use contract::{
    CONTRACT_VERSION, Change, CheckReport, DiscoveredKind, IdempotencyKey, Operation, Position,
    Record, SourceMetadata,
};
pub use credential::{FileCredential, Secret};
pub use fake::FakeSource;
pub use jsonl::JsonLinesSink;
pub use kafka::KafkaCompatibleSink;
pub use parquet::ParquetSink;
pub use platform::{CapturePeer, PLATFORM_SINK_PROTOCOL, PlatformPeer, PlatformSdkSink};
pub use runtime::{NullClock, RunReport, Runtime, RuntimeConfig, SystemClock};
pub use service::Service;
pub use sink::{DeliverError, MemorySink, Sink};

/// Runtime and persistence errors. Connectors map source failures into this type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error {
    pub message: String,
}

impl Error {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::new(error.to_string())
    }
}
