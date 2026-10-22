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

use crate::config::{Config, InstanceConfig, SinkConfig, SourceConfig};
use crate::credential::FileCredential;
use crate::fake::{FakeSource, record};
use crate::{Error, FileCheckpoint, JsonLinesSink, RunReport, Runtime, RuntimeConfig, SystemClock};

type FixtureRuntime = Runtime<FakeSource, JsonLinesSink, FileCheckpoint, SystemClock>;

/// A running instance and the lock that fences its sink directory.
struct Running {
    runtime: FixtureRuntime,
    _sink_lock: File,
}

/// One instance's pass. `progress` counts what was committed even when `outcome` is an error.
pub struct Pass {
    pub instance: String,
    pub outcome: Result<RunReport, Error>,
    pub progress: RunReport,
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
                let (outcome, progress) = match runtime {
                    Some(running) => (running.runtime.run(), running.runtime.progress().clone()),
                    None => match open(checkpoint_dir, instance) {
                        Ok(opened) => {
                            let running = runtime.insert(opened);
                            (running.runtime.run(), running.runtime.progress().clone())
                        }
                        Err(error) => (Err(error), RunReport::default()),
                    },
                };
                Pass {
                    instance: instance.name.clone(),
                    outcome,
                    progress,
                }
            })
            .collect()
    }
}

/// Checkpoint leases fence one instance, not a sink directory that another
/// instance or process reaches by a different path; this lock does, for as
/// long as the instance runs.
fn lock_sink_dir(dir: &Path) -> Result<File, Error> {
    fs::create_dir_all(dir)?;
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(dir.join(".hashi-sink.lock"))?;
    lock.try_lock().map_err(|_| {
        Error::new(format!(
            "sink directory {} is in use by another instance",
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
    let sink_lock = lock_sink_dir(&jsonl.dir)?;
    Ok(Running {
        runtime: Runtime::new(
            source,
            JsonLinesSink::open(&jsonl.dir)?,
            FileCheckpoint::open(checkpoint_dir.join(&instance.name))?,
            SystemClock,
            RuntimeConfig::new(&instance.name),
        ),
        _sink_lock: sink_lock,
    })
}
