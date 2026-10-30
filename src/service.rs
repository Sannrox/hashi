//! `hashi run`: every configured instance driven through the runtime.
//!
//! Each pass runs every instance until its source is caught up. An
//! instance's failure is reported and does not stop the others, so a
//! credential that is missing on one pass is picked up on the next once the
//! file is rotated, and an instance whose directories cannot open yet is
//! retried. Checkpoints live under `checkpoint_dir/<instance>`, so a
//! restarted process resumes where the last one committed.

use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::buffer::{BufferStats, StoreAndForward};
use crate::config::{Config, InstanceConfig, SinkConfig, SourceConfig};
use crate::contract::{IdempotencyKey, Record};
use crate::credential::FileCredential;
use crate::fake::{FakeSource, record};
use crate::lease::{self, DropRelease, SharedLeaseStore};
use crate::runtime::Clock;
use crate::sink::{DeliverError, Sink};
use crate::{Error, FileCheckpoint, JsonLinesSink, RunReport, Runtime, RuntimeConfig, SystemClock};

type FixtureRuntime = Runtime<FakeSource, InstanceSink, FileCheckpoint, SystemClock>;

/// An instance's sink, written directly or through its store-and-forward
/// buffer.
struct Locked<S> {
    sink: S,
    lock: DropRelease,
}

impl<S: Sink> Sink for Locked<S> {
    fn admit(&mut self, token: u64) -> Result<(), DeliverError> {
        self.lock.renew().map_err(dir_lock_err)?;
        self.sink.admit(token)
    }

    fn recover(&mut self) -> Result<(), DeliverError> {
        self.lock.renew().map_err(dir_lock_err)?;
        self.sink.recover()
    }

    fn deliver(&mut self, record: &Record, idk: &IdempotencyKey) -> Result<(), DeliverError> {
        self.lock.renew().map_err(dir_lock_err)?;
        self.sink.deliver(record, idk)
    }
}

fn dir_lock_err(error: Error) -> DeliverError {
    if lease::is_fence_lost(&error)
        || error.message.contains("already running")
        || error.message.contains("in use by another instance")
    {
        DeliverError::Blocked(error.message)
    } else {
        DeliverError::Transient(error.message)
    }
}

enum InstanceSink {
    Direct(Locked<JsonLinesSink>),
    Buffered(Box<Locked<StoreAndForward<LazySink>>>),
}

/// The sink behind a buffer, opened (and its directory locked) on first
/// use. A sink that cannot open yet, such as an unmounted destination, is a
/// transient error, so records keep buffering across a restart during an
/// outage.
struct LazySink {
    dir: PathBuf,
    ttl: Duration,
    token: u64,
    open: Option<(JsonLinesSink, DropRelease)>,
}

impl LazySink {
    fn sink(&mut self) -> Result<&mut JsonLinesSink, DeliverError> {
        if self.open.is_none() {
            let lock = lock_sink_dir(&self.dir, self.ttl)
                .map_err(|error| DeliverError::Transient(error.message))?;
            let mut sink = JsonLinesSink::open(&self.dir)
                .map_err(|error| DeliverError::Transient(error.message))?;
            if self.token > 0 {
                sink.admit(self.token)?;
            }
            self.open = Some((sink, lock));
        }
        let open = self.open.as_mut().expect("opened above");
        open.1.renew().map_err(dir_lock_err)?;
        Ok(&mut open.0)
    }
}

impl Sink for LazySink {
    fn admit(&mut self, token: u64) -> Result<(), DeliverError> {
        self.token = token;
        if self.open.is_some() {
            self.sink()?.admit(token)?;
        }
        Ok(())
    }

    fn recover(&mut self) -> Result<(), DeliverError> {
        self.sink()?.recover()
    }

    fn deliver(&mut self, record: &Record, idk: &IdempotencyKey) -> Result<(), DeliverError> {
        self.sink()?.deliver(record, idk)
    }
}

impl Sink for InstanceSink {
    fn admit(&mut self, token: u64) -> Result<(), DeliverError> {
        match self {
            Self::Direct(sink) => sink.admit(token),
            Self::Buffered(sink) => sink.admit(token),
        }
    }

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
/// directories. Those locks release on drop so a clean stop frees the path.
struct Running {
    runtime: FixtureRuntime,
    takeovers: u64,
    acquired_ms: i64,
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
            InstanceSink::Buffered(locked) => locked
                .sink
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
    /// 0 while waiting for the lease, 1 while this process holds it.
    pub role: u8,
    pub fencing_token: u64,
    pub takeovers: u64,
    pub lease_acquired_ms: Option<i64>,
}

pub struct Service {
    checkpoint_dir: PathBuf,
    leases: SharedLeaseStore,
    ttl: Duration,
    instances: Vec<(InstanceConfig, Option<Running>)>,
}

impl Service {
    /// Instances open lazily on their first pass, so one unavailable
    /// directory does not stop the others; it is retried on later passes.
    pub fn from_config(config: &Config) -> Result<Self, Error> {
        Ok(Self {
            checkpoint_dir: config.checkpoint_dir.clone(),
            leases: lease::open_store(&config.lease)?,
            ttl: config.lease.ttl(),
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
        let leases = &self.leases;
        let ttl = self.ttl;
        self.instances
            .iter_mut()
            .map(|(instance, runtime)| match runtime {
                Some(running) => {
                    let outcome = running.run();
                    finish_pass(&instance.name, running, outcome)
                }
                None => match open(checkpoint_dir, leases.clone(), ttl, instance) {
                    Ok(opened) => {
                        let running = runtime.insert(opened);
                        let outcome = running.run();
                        finish_pass(&instance.name, running, outcome)
                    }
                    Err(error) => {
                        let standby = is_standby(&error);
                        Pass {
                            instance: instance.name.clone(),
                            outcome: Err(error),
                            progress: RunReport::default(),
                            buffer: None,
                            role: u8::from(!standby),
                            fencing_token: 0,
                            takeovers: 0,
                            lease_acquired_ms: None,
                        }
                    }
                },
            })
            .collect()
    }
}

fn is_standby(error: &Error) -> bool {
    error.message.contains("already running")
        || error.message.contains("stale writer")
        || error.message.contains("in use by another instance")
}

fn finish_pass(name: &str, running: &mut Running, outcome: Result<RunReport, Error>) -> Pass {
    let token = running.runtime.fencing_token().unwrap_or(0);
    let standby = outcome.as_ref().is_err_and(is_standby);
    if !standby && token > 1 && running.takeovers == 0 {
        running.takeovers = 1;
    }
    Pass {
        instance: name.to_string(),
        outcome,
        progress: running.runtime.progress().clone(),
        buffer: running.buffer(),
        role: u8::from(!standby),
        fencing_token: token,
        takeovers: running.takeovers,
        lease_acquired_ms: Some(running.acquired_ms),
    }
}

/// Checkpoint leases fence one instance, not a sink directory that another
/// instance or process reaches by a different path; this lock does, and
/// releases on a clean stop.
fn lock_sink_dir(dir: &Path, ttl: Duration) -> Result<DropRelease, Error> {
    lease::lock_dir(dir, "sink", "sink", ttl)
}

/// The connector id an instance checkpoints under.
pub(crate) fn connector_id(instance: &InstanceConfig) -> String {
    let SourceConfig::Fixture(_) = &instance.source;
    format!("fixture-{}", instance.name)
}

fn open(
    checkpoint_dir: &Path,
    leases: SharedLeaseStore,
    ttl: Duration,
    instance: &InstanceConfig,
) -> Result<Running, Error> {
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
    let sink = match &instance.buffer {
        None => InstanceSink::Direct(Locked {
            sink: JsonLinesSink::open(&jsonl.dir)?,
            lock: lock_sink_dir(&jsonl.dir, ttl)?,
        }),
        Some(buffer) => {
            // Two processes draining one buffer would deliver twice.
            InstanceSink::Buffered(Box::new(Locked {
                sink: StoreAndForward::with_limits(
                    &buffer.dir,
                    buffer.limits(),
                    LazySink {
                        dir: jsonl.dir.clone(),
                        ttl,
                        token: 0,
                        open: None,
                    },
                )?,
                lock: lease::lock_dir(&buffer.dir, "buffer", "buffer", ttl)?,
            }))
        }
    };
    Ok(Running {
        runtime: Runtime::new(
            source,
            sink,
            FileCheckpoint::with_store(checkpoint_dir.join(&instance.name), leases, ttl)?,
            SystemClock,
            RuntimeConfig::new(&instance.name),
        ),
        takeovers: 0,
        acquired_ms: SystemClock.now_ms(),
    })
}
