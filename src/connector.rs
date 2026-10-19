//! Connector lifecycle. Implementations talk to a source; the runtime owns checkpoints.

use crate::Error;
use crate::contract::{CheckReport, DiscoveredKind, Position, Record};

/// Discovery must describe the next read: a connector must not return data under a
/// changed schema between `discover` and `read`. A source adapter owns payload
/// normalization and buffers source-side schema changes until the next discovery.
/// One source family. `read` yields the next record after `from`, or `None` when caught up.
pub trait Connector {
    fn id(&self) -> &str;
    fn check(&self) -> Result<CheckReport, Error>;
    fn discover(&self) -> Result<Vec<DiscoveredKind>, Error>;
    fn read(&mut self, from: Option<Position>) -> Result<Option<Record>, Error>;
}
