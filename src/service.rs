//! `hashi run`: every configured instance driven through the runtime.
//!
//! Each pass runs every instance until its source is caught up. An
//! instance's failure is reported and does not stop the others, so a
//! credential that is missing on one pass is picked up on the next once the
//! file is rotated, and an instance whose directories cannot open yet is
//! retried. Checkpoints live under `checkpoint_dir/<instance>`, so a
//! restarted process resumes where the last one committed.

use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};

use crate::buffer::{BufferStats, StoreAndForward};
use crate::config::{Config, InstanceConfig, SinkConfig, SourceConfig};
use crate::contract::{IdempotencyKey, Record};
use crate::credential::FileCredential;
use crate::fake::{FakeSource, record};
use crate::sink::{DeliverError, Sink};
use crate::{Error, FileCheckpoint, JsonLinesSink, RunReport, Runtime, RuntimeConfig, SystemClock};

type FixtureRuntime = Runtime<FakeSource, InstanceSink, FileCheckpoint, SystemClock>;

/// An instance's sink, written directly or through its store-and-forward
/// buffer.
enum InstanceSink {
    Direct(JsonLinesSink),
    Buffered(StoreAndForward<LazySink>),
}

/// The sink behind a buffer, opened (and its directory locked) on first
/// use. A sink that cannot open yet, such as an unmounted destination, is a
/// transient error, so records keep buffering across a restart during an
/// outage.
struct LazySink {
    dir: PathBuf,
    open: Option<(JsonLinesSink, File)>,
}

impl LazySink {
    fn sink(&mut self) -> Result<&mut JsonLinesSink, DeliverError> {
        if self.open.is_none() {
            let lock =
                lock_sink_dir(&self.dir).map_err(|error| DeliverError::Transient(error.message))?;
            let sink = JsonLinesSink::open(&self.dir)
                .map_err(|error| DeliverError::Transient(error.message))?;
            self.open = Some((sink, lock));
        }
        Ok(&mut self.open.as_mut().expect("opened above").0)
    }
}

impl Sink for LazySink {
    fn recover(&mut self) -> Result<(), DeliverError> {
        self.sink()?.recover()
    }

    fn deliver(&mut self, record: &Record, idk: &IdempotencyKey) -> Result<(), DeliverError> {
        self.sink()?.deliver(record, idk)
    }
}

impl Sink for InstanceSink {
    fn recover(&mut self) -> Result<(), DeliverError> {
        match self {
            Self::Direct(sink) => sink.recover(),
            Self::Buffered(sink) => sink.recover(),
        }
    }

    fn deliver(&mut self, record: &Record, idk: &IdempotencyKey) -> Result<(), DeliverError> {
        match self {
            Self::Direct(sink) => sink.deliver(record, idk),
            Self::Buffered(sink) => sink.deliver(record, idk),
        }
    }
}

/// A running instance and the locks that fence its sink and buffer
/// directories.
struct Running {
    runtime: FixtureRuntime,
    _locks: Vec<File>,
}

impl Running {
    /// Run the instance. A pass that fails, for example because the source
    /// is unavailable, still drains the buffer: those records were already
    /// acknowledged and checkpointed, so their delivery must not wait on the
    /// source.
    fn run(&mut self) -> Result<RunReport, Error> {
        let outcome = self.runtime.run();
        if outcome.is_err()
            && let InstanceSink::Buffered(buffer) = self.runtime.sink_mut()
            && let Err(error @ (DeliverError::Blocked(_) | DeliverError::Poison(_))) =
                buffer.recover()
        {
            eprintln!("buffer drain stopped: {error:?}");
        }
        outcome
    }

    fn buffer(&self) -> Option<BufferStats> {
        match self.runtime.sink() {
            InstanceSink::Direct(_) => None,
            InstanceSink::Buffered(buffer) => buffer
                .stats()
                .inspect_err(|error| eprintln!("buffer stats unreadable: {error}"))
                .ok(),
        }
    }
}

/// One instance's pass. `progress` counts what was committed even when `outcome` is an error.
pub struct Pass {
    pub instance: String,
    pub outcome: Result<RunReport, Error>,
    pub progress: RunReport,
    /// The buffer's fill level and counters after the pass, when it has one.
    pub buffer: Option<BufferStats>,
}

pub struct Service {
    checkpoint_dir: PathBuf,
    instances: Vec<(InstanceConfig, Option<Running>)>,
}

impl Service {
    /// Instances open lazily on their first pass, so one unavailable
    /// directory does not stop the others; it is retried on later passes.
    pub fn from_config(config: &Config) -> Result<Self, Error> {
        Ok(Self {
            checkpoint_dir: config.checkpoint_dir.clone(),
            instances: config
                .instances
                .iter()
                .map(|instance| (instance.clone(), None))
                .collect(),
        })
    }

    /// Run every instance until caught up, in config order.
    pub fn pass(&mut self) -> Vec<(String, Result<RunReport, Error>)> {
        self.pass_with_progress()
            .into_iter()
            .map(|pass| (pass.instance, pass.outcome))
            .collect()
    }

    /// Like [`Service::pass`], plus what each instance committed before an error.
    pub fn pass_with_progress(&mut self) -> Vec<Pass> {
        let checkpoint_dir = &self.checkpoint_dir;
        self.instances
            .iter_mut()
            .map(|(instance, runtime)| {
                let (outcome, progress, buffer) = match runtime {
                    Some(running) => (
                        running.run(),
                        running.runtime.progress().clone(),
                        running.buffer(),
                    ),
                    None => match open(checkpoint_dir, instance) {
                        Ok(opened) => {
                            let running = runtime.insert(opened);
                            (
                                running.run(),
                                running.runtime.progress().clone(),
                                running.buffer(),
                            )
                        }
                        Err(error) => (Err(error), RunReport::default(), None),
                    },
                };
                Pass {
                    instance: instance.name.clone(),
                    outcome,
                    progress,
                    buffer,
                }
            })
            .collect()
    }
}

/// Checkpoint leases fence one instance, not a sink directory that another
/// instance or process reaches by a different path; this lock does, for as
/// long as the instance runs.
fn lock_sink_dir(dir: &Path) -> Result<File, Error> {
    lock_dir(dir, ".hashi-sink.lock", "sink")
}

fn lock_dir(dir: &Path, file: &str, what: &str) -> Result<File, Error> {
    fs::create_dir_all(dir)?;
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(dir.join(file))?;
    lock.try_lock().map_err(|_| {
        Error::new(format!(
            "{what} directory {} is in use by another instance",
            dir.display()
        ))
    })?;
    Ok(lock)
}

/// The connector id an instance checkpoints under.
pub(crate) fn connector_id(instance: &InstanceConfig) -> String {
    let SourceConfig::Fixture(_) = &instance.source;
    format!("fixture-{}", instance.name)
}

fn open(checkpoint_dir: &Path, instance: &InstanceConfig) -> Result<Running, Error> {
    let SourceConfig::Fixture(fixture) = &instance.source;
    let records = (1..=fixture.records)
        .map(|position| {
            record(
                "reading",
                &format!("r{position}"),
                position,
                &format!("value-{position}"),
            )
        })
        .collect();
    let source = FakeSource::with_live_credential(
        connector_id(instance),
        records,
        FileCredential::new(&fixture.credential_file),
        fixture.accept_sha256.clone(),
    );
    let SinkConfig::Jsonl(jsonl) = &instance.sink;
    let mut locks = Vec::new();
    let sink = match &instance.buffer {
        None => {
            locks.push(lock_sink_dir(&jsonl.dir)?);
            InstanceSink::Direct(JsonLinesSink::open(&jsonl.dir)?)
        }
        Some(buffer) => {
            // Two processes draining one buffer would deliver twice.
            locks.push(lock_dir(&buffer.dir, ".hashi-buffer.lock", "buffer")?);
            InstanceSink::Buffered(StoreAndForward::with_limits(
                &buffer.dir,
                buffer.limits(),
                LazySink {
                    dir: jsonl.dir.clone(),
                    open: None,
                },
            )?)
        }
    };
    Ok(Running {
        runtime: Runtime::new(
            source,
            sink,
            FileCheckpoint::open(checkpoint_dir.join(&instance.name))?,
            SystemClock,
            RuntimeConfig::new(&instance.name),
        ),
        _locks: locks,
    })
}
