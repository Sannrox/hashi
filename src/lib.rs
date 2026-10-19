//! Connector runtime: check, discover, read, durable checkpoints, at-least-once delivery.
//!
//! Protocol connectors, pluggable production sinks, schema drift, and ops
//! surfaces live in later Issues. This crate owns the versioned record
//! contract and the runtime that drives a connector against a sink.

pub mod checkpoint;
pub mod connector;
pub mod contract;
pub mod fake;
pub mod runtime;
pub mod sink;

pub use checkpoint::{Checkpoint, CheckpointStore, FileCheckpoint, QuarantineRow};
pub use connector::Connector;
pub use contract::{
    CONTRACT_VERSION, CheckReport, DiscoveredKind, IdempotencyKey, Position, Record,
};
pub use fake::FakeSource;
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
