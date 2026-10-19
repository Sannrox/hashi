//! Connector runtime: check, discover, read, durable checkpoints, at-least-once delivery.
//!
//! Protocol connectors and diagnostics are added independently.
//! This crate owns the versioned record contract, the runtime, and pluggable
//! sinks (JSON Lines, Parquet, Kafka-compatible, platform SDK) plus a
//! store-and-forward buffer.

pub mod buffer;
pub mod checkpoint;
pub mod connector;
pub mod contract;
pub mod delivery;
pub mod fake;
pub mod jsonl;
pub mod kafka;
pub mod parquet;
pub mod platform;
pub mod runtime;
pub mod schema;
pub mod sink;

pub use buffer::StoreAndForward;
pub use checkpoint::{Checkpoint, CheckpointStore, FileCheckpoint, QuarantineRow};
pub use connector::Connector;
pub use contract::{
    CONTRACT_VERSION, CheckReport, DiscoveredKind, IdempotencyKey, Position, Record,
};
pub use fake::FakeSource;
pub use jsonl::JsonLinesSink;
pub use kafka::KafkaCompatibleSink;
pub use parquet::ParquetSink;
pub use platform::{CapturePeer, PLATFORM_SINK_PROTOCOL, PlatformPeer, PlatformSdkSink};
pub use runtime::{NullClock, RunReport, Runtime, RuntimeConfig, SystemClock};
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
