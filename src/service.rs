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
        let checkpoint_dir = &self.checkpoint_dir;
        self.instances
            .iter_mut()
            .map(|(instance, runtime)| {
                let outcome = match runtime {
                    Some(running) => running.runtime.run(),
                    None => open(checkpoint_dir, instance)
                        .and_then(|opened| runtime.insert(opened).runtime.run()),
                };
                (instance.name.clone(), outcome)
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
        format!("fixture-{}", instance.name),
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
