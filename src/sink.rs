//! Consumer of records. The runtime commits a checkpoint only after `Ok`.

use crate::Error;
use crate::contract::{IdempotencyKey, Record};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeliverError {
    Transient(String),
    Poison(String),
}

impl From<DeliverError> for Error {
    fn from(error: DeliverError) -> Self {
        match error {
            DeliverError::Transient(message) | DeliverError::Poison(message) => Error::new(message),
        }
    }
}

pub trait Sink {
    fn deliver(&mut self, record: &Record, idk: &IdempotencyKey) -> Result<(), DeliverError>;
}

/// In-memory sink for tests and local proof. Records every attempt, including retries.
#[derive(Debug, Default)]
pub struct MemorySink {
    pub delivered: Vec<(Record, IdempotencyKey)>,
    pub poison_keys: Vec<String>,
    pub fail_times: u32,
    remaining_failures: u32,
}

impl MemorySink {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn fail_next(&mut self, times: u32) {
        self.fail_times = times;
        self.remaining_failures = times;
    }
}

impl Sink for MemorySink {
    fn deliver(&mut self, record: &Record, idk: &IdempotencyKey) -> Result<(), DeliverError> {
        if self.poison_keys.iter().any(|key| key == &record.key) {
            return Err(DeliverError::Poison(format!(
                "poison record {}",
                record.key
            )));
        }
        if self.remaining_failures > 0 {
            self.remaining_failures -= 1;
            return Err(DeliverError::Transient("injected sink failure".into()));
        }
        self.delivered.push((record.clone(), idk.clone()));
        Ok(())
    }
}
