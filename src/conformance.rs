//! Connector conformance kit (#28): one contract every source family must prove.
//!
//! A family supplies a [`Family`] driver over a live fixture (an in-process
//! simulator or a disposable container). [`run`] drives the family's connector
//! through the real [`Runtime`] with a durable checkpoint store and checks, in
//! order: check, discover, snapshot, incremental, interruption and resume,
//! duplicate keys, kill-active takeover, paused-active fence, and schema drift.
//! It returns the names of the passed checks,
//! or the first contract violation as an error naming its check.
//!
//! Every check uses only the connector contract, so a family that passes here
//! behaves like every other family toward sinks and consumers.

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::checkpoint::{CheckpointStore, FileCheckpoint};
use crate::connector::Connector;
use crate::contract::{IdempotencyKey, Position, Record};
use crate::runtime::{Runtime, RuntimeConfig, SystemClock};
use crate::schema::CONTROL_KIND;
use crate::sink::{DeliverError, Sink};
use crate::{Error, RunReport};

/// The checks [`run`] performs, in order.
pub const CHECKS: [&str; 9] = [
    "check",
    "discover",
    "snapshot",
    "incremental",
    "interruption_and_resume",
    "duplicate_keys",
    "kill_active",
    "paused_active",
    "drift",
];
/// Lease TTL for takeover checks. Short so the kit does not wait on the
/// operator default.
const TAKEOVER_TTL: std::time::Duration = std::time::Duration::from_millis(500);

/// Runtime instance name the kit checkpoints under.
pub const INSTANCE: &str = "conformance";
/// Longest wait for expected deliveries; a sampling source never reports caught up.
const DRAIN_DEADLINE: std::time::Duration = std::time::Duration::from_secs(60);
/// Longest whole run. A connector read can block past [`DRAIN_DEADLINE`], so a
/// watchdog ends the process naming the stuck check instead of letting CI hang.
const RUN_DEADLINE: std::time::Duration = std::time::Duration::from_secs(600);

/// Levers the kit pulls on one family's fixture.
pub trait Family {
    type Source: Connector;

    /// A connector over the fixture's current state. The kit calls it again after
    /// a simulated process kill and after drift, with the same durable state.
    fn connect(&mut self) -> Result<Self::Source, Error>;

    /// Source state that exists before the first connect, which a snapshot must
    /// deliver. A stream with no history (a broker subscription) has none.
    fn seed(&mut self) -> Result<Vec<Change>, Error> {
        Ok(Vec::new())
    }

    /// Makes source changes for `keys` visible and returns the records the connector
    /// must deliver for them, at least once each. Repeating a key changes the same
    /// row, tag or topic again. A sampling source may return fewer changes than
    /// keys, since it reports current state.
    fn produce(&mut self, keys: &[&str]) -> Result<Vec<Change>, Error>;

    /// Changes the schema of one discovered kind, in the source or in the
    /// family's configuration, and says how the family handles it. The kit has
    /// stopped the connector; the next [`Family::connect`] sees the change.
    fn drift(&mut self) -> Result<Drift, Error>;
}

/// One record a change must produce: its key, and a payload field holding the value
/// the change wrote, so an older record for the same key cannot stand in for it.
/// Numbers compare numerically, also when the payload encodes them as strings.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct Change {
    pub key: String,
    pub field: String,
    pub value: serde_json::Value,
}

impl Change {
    pub fn new(
        key: impl Into<String>,
        field: impl Into<String>,
        value: impl Into<serde_json::Value>,
    ) -> Self {
        Self {
            key: key.into(),
            field: field.into(),
            value: value.into(),
        }
    }

    pub(crate) fn matches(&self, record: &Record) -> bool {
        if record.key != self.key || record.kind == CONTROL_KIND {
            return false;
        }
        let Ok(payload) = serde_json::from_str::<serde_json::Value>(&record.payload) else {
            return false;
        };
        let text = |value: &serde_json::Value| match value {
            serde_json::Value::Number(number) => Some(number.to_string()),
            serde_json::Value::String(text) => Some(text.clone()),
            _ => None,
        };
        let found = &payload[self.field.as_str()];
        let (Some(found_text), Some(wanted_text)) = (text(found), text(&self.value)) else {
            return *found == self.value;
        };
        // Integers compare exactly, so large values never collapse onto a neighbor.
        if let (Ok(found), Ok(wanted)) = (found_text.parse::<i128>(), wanted_text.parse::<i128>()) {
            return found == wanted;
        }
        match (found_text.parse::<f64>(), wanted_text.parse::<f64>()) {
            (Ok(found), Ok(wanted)) => found == wanted,
            _ => *found == self.value,
        }
    }
}

/// A family's documented answer to a schema change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Drift {
    /// The runtime records the change and delivers a schema control record for this kind.
    Reported(String),
    /// The connector refuses to start until an operator recovers its state, so the
    /// next connect must fail and nothing may be delivered or committed.
    Refused,
}

/// Runs every check in [`CHECKS`] against `family`, keeping checkpoints in `state_dir`.
pub fn run<F: Family>(family: &mut F, state_dir: &Path) -> Result<Vec<&'static str>, Error> {
    let current = Arc::new(Mutex::new(Some("seed")));
    let watched = current.clone();
    std::thread::spawn(move || {
        std::thread::sleep(RUN_DEADLINE);
        if let Some(check) = *lock(&watched) {
            eprintln!("conformance: check {check:?} did not finish within {RUN_DEADLINE:?}");
            std::process::exit(101);
        }
    });
    let outcome =
        crate::lease::with_ttl_override(TAKEOVER_TTL, || run_checks(family, state_dir, &current));
    *lock(&current) = None;
    outcome
}

fn run_checks<F: Family>(
    family: &mut F,
    state_dir: &Path,
    current: &Mutex<Option<&'static str>>,
) -> Result<Vec<&'static str>, Error> {
    let seeded = family
        .seed()
        .map_err(|error| Error::new(format!("seed: {}", error.message)))?;
    let delivered = Arc::new(Mutex::new(Vec::new()));
    let mut kit = Kit {
        family,
        state_dir,
        delivered: delivered.clone(),
        runtime: None,
        id: String::new(),
        checkpoint: None,
        seeded,
    };
    let mut passed = Vec::new();
    for check in CHECKS {
        *lock(current) = Some(check);
        let outcome = match check {
            "check" => kit.check(),
            "discover" => kit.discover(),
            "snapshot" => kit.snapshot(),
            "incremental" => kit.incremental(),
            "interruption_and_resume" => kit.interruption_and_resume(),
            "duplicate_keys" => kit.duplicate_keys(),
            "kill_active" => kit.kill_active(),
            "paused_active" => kit.paused_active(),
            _ => kit.drift(),
        };
        outcome.map_err(|error| Error::new(format!("{check}: {}", error.message)))?;
        passed.push(check);
    }
    Ok(passed)
}

type Delivered = Arc<Mutex<Vec<(Record, IdempotencyKey)>>>;

/// Records every acknowledged delivery, surviving a runtime that is dropped as if killed.
struct Recorder(Delivered);

impl Sink for Recorder {
    fn deliver(&mut self, record: &Record, idk: &IdempotencyKey) -> Result<(), DeliverError> {
        lock(&self.0).push((record.clone(), idk.clone()));
        Ok(())
    }
}

struct Kit<'a, F: Family> {
    family: &'a mut F,
    state_dir: &'a Path,
    delivered: Delivered,
    runtime: Option<Runtime<F::Source, Recorder, FileCheckpoint, SystemClock>>,
    /// Connector id, kept across a simulated kill to read the durable checkpoint.
    id: String,
    /// Last committed position, read back from the durable store after each run.
    checkpoint: Option<Position>,
    /// State created before the first connect, due in the snapshot.
    seeded: Vec<Change>,
}

impl<F: Family> Kit<'_, F> {
    fn check(&mut self) -> Result<(), Error> {
        let source = self.family.connect()?;
        let report = source.check()?;
        if !report.ok {
            return Err(Error::new(format!(
                "check reported failure: {}",
                report.message
            )));
        }
        self.start(source)
    }

    fn discover(&mut self) -> Result<(), Error> {
        let kinds = self.runtime()?.connector().discover()?;
        if kinds.is_empty() {
            return Err(Error::new("discovery is empty"));
        }
        let mut seen = BTreeSet::new();
        for kind in &kinds {
            kind.schema.validate()?;
            if kind.kind.trim().is_empty() || kind.kind == CONTROL_KIND || !seen.insert(&kind.kind)
            {
                return Err(Error::new(format!(
                    "discovered kind {:?} is empty, reserved or duplicated",
                    kind.kind
                )));
            }
        }
        Ok(())
    }

    fn snapshot(&mut self) -> Result<(), Error> {
        let mut expected = std::mem::take(&mut self.seeded);
        expected.extend(self.family.produce(&["k1", "k2", "k3"])?);
        let records = self.drain(&expected, None)?;
        require_keys(&records, &expected)
    }

    fn incremental(&mut self) -> Result<(), Error> {
        let before = self.checkpoint;
        let expected = self.family.produce(&["k4"])?;
        let records = self.drain(&expected, None)?;
        require_keys(&records, &expected)?;
        if let (Some(before), Some(first)) = (before, records.first())
            && first.position <= before
        {
            return Err(Error::new(format!(
                "incremental read replayed position {} at or before checkpoint {before}",
                first.position
            )));
        }
        Ok(())
    }

    /// Stops after one delivery, drops the runtime and connector as a kill would,
    /// then resumes from the durable checkpoint with a fresh connector.
    fn interruption_and_resume(&mut self) -> Result<(), Error> {
        let expected = self.family.produce(&["k5", "k6", "k7"])?;
        let mark = lock(&self.delivered).len();
        self.runtime_mut()?
            .run_while(|report: &RunReport| report.delivered == 0)?;
        if let Some(runtime) = &mut self.runtime {
            runtime.release_lease()?;
        }
        self.runtime = None;
        self.checkpoint = self.committed()?;
        let killed_at = self.checkpoint;
        let source = self.family.connect()?;
        self.start(source)?;
        let delivered_before_kill: Vec<Record> = lock(&self.delivered)[mark..]
            .iter()
            .map(|(record, _)| record.clone())
            .collect();
        if delivered_before_kill.is_empty() {
            return Err(Error::new("nothing was delivered before the kill"));
        }
        // One more change after the restart, so the resumed connector must read.
        let mut remaining = missing(&delivered_before_kill, &expected);
        let after_restart = self.family.produce(&["k8"])?;
        remaining.extend(after_restart.iter().cloned());
        let resumed = self.drain(&remaining, None)?;
        require_keys(&resumed, &after_restart)?;
        let all: Vec<Record> = lock(&self.delivered)[mark..]
            .iter()
            .map(|(record, _)| record.clone())
            .filter(|record| record.kind != CONTROL_KIND)
            .collect();
        require_keys(&all, &expected)?;
        if let (Some(killed_at), Some(first)) = (killed_at, resumed.first())
            && first.position <= killed_at
        {
            return Err(Error::new(format!(
                "resume replayed committed position {} (checkpoint {killed_at})",
                first.position
            )));
        }
        Ok(())
    }

    /// Two changes to one key are two records with distinct identities; neither is dropped.
    fn duplicate_keys(&mut self) -> Result<(), Error> {
        let mut identities = Vec::new();
        for _ in 0..2 {
            let expected = self.family.produce(&["dup"])?;
            let mark = lock(&self.delivered).len();
            let records = self.drain(&expected, None)?;
            require_keys(&records, &expected)?;
            let key = &expected
                .first()
                .ok_or_else(|| Error::new("produce returned no change"))?
                .key;
            identities.extend(
                lock(&self.delivered)[mark..]
                    .iter()
                    .filter(|(record, _)| &record.key == key)
                    .map(|(_, idk)| idk.encode()),
            );
        }
        let distinct: BTreeSet<&String> = identities.iter().collect();
        if distinct.len() < 2 {
            return Err(Error::new(
                "two changes to one key did not yield two record identities",
            ));
        }
        Ok(())
    }

    /// A schema change yields a durable schema control record for the drifted kind,
    /// or, for a family that fails closed, a refused start that changes nothing.
    fn drift(&mut self) -> Result<(), Error> {
        // Stop the connector first, as an operator does before changing a source schema.
        if let Some(runtime) = &mut self.runtime {
            runtime.release_lease()?;
        }
        self.runtime = None;
        let before = self.committed()?;
        let kind = match self.family.drift()? {
            Drift::Reported(kind) => kind,
            Drift::Refused => {
                if self.family.connect().is_ok() {
                    return Err(Error::new("a refused schema change still connected"));
                }
                if self.committed()? != before {
                    return Err(Error::new("a refused schema change moved the checkpoint"));
                }
                return Ok(());
            }
        };
        let source = self.family.connect()?;
        self.start(source)?;
        let mark = lock(&self.delivered).len();
        let expected = self.family.produce(&["k9"])?;
        let records = self.drain(&expected, Some(&kind))?;
        require_keys(&records, &expected)?;
        let controls = lock(&self.delivered)[mark..]
            .iter()
            .filter(|(record, _)| record.kind == CONTROL_KIND && record.key == kind)
            .count();
        if controls == 0 {
            return Err(Error::new(format!(
                "no schema control record for drifted kind {kind:?}"
            )));
        }
        Ok(())
    }

    /// Drops the active runtime without releasing the lease (a kill), waits
    /// for the TTL, and resumes from the durable checkpoint.
    fn kill_active(&mut self) -> Result<(), Error> {
        let expected = self.family.produce(&["ka1", "ka2"])?;
        let mark = lock(&self.delivered).len();
        self.runtime_mut()?
            .run_while(|report: &RunReport| report.delivered == 0)?;
        let acked: Vec<Record> = lock(&self.delivered)[mark..]
            .iter()
            .map(|(record, _)| record.clone())
            .filter(|record| record.kind != CONTROL_KIND)
            .collect();
        if acked.is_empty() {
            return Err(Error::new("nothing was delivered before the kill"));
        }
        let acked_keys: Vec<String> = acked
            .iter()
            .map(|record| record.idempotency_key(&self.id, INSTANCE).encode())
            .collect();
        if let Some(runtime) = &mut self.runtime {
            runtime.abandon_lease();
        }
        self.runtime = None;
        std::thread::sleep(TAKEOVER_TTL + std::time::Duration::from_millis(50));
        let source = self.family.connect()?;
        self.start(source)?;
        let mut remaining = missing(&acked, &expected);
        let after = self.family.produce(&["ka3"])?;
        remaining.extend(after.iter().cloned());
        let resumed = self.drain(&remaining, None)?;
        require_keys(&resumed, &after)?;
        let all: Vec<Record> = lock(&self.delivered)[mark..]
            .iter()
            .map(|(record, _)| record.clone())
            .filter(|record| record.kind != CONTROL_KIND)
            .collect();
        require_keys(&all, &expected)?;
        let mut seen = BTreeSet::new();
        for (_, idk) in lock(&self.delivered)[mark..].iter() {
            let encoded = idk.encode();
            if !seen.insert(encoded.clone()) && !acked_keys.contains(&encoded) {
                return Err(Error::new(
                    "takeover delivered a duplicate with a new idempotency key",
                ));
            }
        }
        Ok(())
    }

    /// Keeps the old runtime after takeover; its next write must be refused.
    fn paused_active(&mut self) -> Result<(), Error> {
        let expected = self.family.produce(&["pa1"])?;
        self.drain(&expected, None)?;
        let mut paused = self
            .runtime
            .take()
            .ok_or_else(|| Error::new("no running connector"))?;
        std::thread::sleep(TAKEOVER_TTL + std::time::Duration::from_millis(50));
        let source = self.family.connect()?;
        self.start(source)?;
        let next = self.family.produce(&["pa2"])?;
        self.drain(&next, None)?;
        let err = paused
            .run()
            .err()
            .ok_or_else(|| Error::new("paused writer was not refused after takeover"))?;
        if !(err.message.contains("already running")
            || err.message.contains("stale writer")
            || err.message.contains("fence"))
        {
            return Err(Error::new(format!(
                "paused writer failed for the wrong reason: {}",
                err.message
            )));
        }
        Ok(())
    }

    fn start(&mut self, source: F::Source) -> Result<(), Error> {
        self.id = source.id().to_string();
        let store = FileCheckpoint::open_with_ttl(self.state_dir, TAKEOVER_TTL)?;
        self.runtime = Some(Runtime::new(
            source,
            Recorder(self.delivered.clone()),
            store,
            SystemClock,
            RuntimeConfig::new(INSTANCE),
        ));
        Ok(())
    }

    fn runtime(&self) -> Result<&Runtime<F::Source, Recorder, FileCheckpoint, SystemClock>, Error> {
        self.runtime
            .as_ref()
            .ok_or_else(|| Error::new("no running connector"))
    }

    fn runtime_mut(
        &mut self,
    ) -> Result<&mut Runtime<F::Source, Recorder, FileCheckpoint, SystemClock>, Error> {
        self.runtime
            .as_mut()
            .ok_or_else(|| Error::new("no running connector"))
    }

    /// Runs until caught up, or until `expected` keys (and a schema control record
    /// for `control`) arrived, whichever is first. Returns the data records delivered,
    /// checking that positions only grow and that the durable checkpoint matches.
    fn drain(&mut self, expected: &[Change], control: Option<&str>) -> Result<Vec<Record>, Error> {
        let mark = lock(&self.delivered).len();
        let delivered = self.delivered.clone();
        let started = std::time::Instant::now();
        let done = || {
            let since = &lock(&delivered)[mark..];
            let records: Vec<Record> = since.iter().map(|(record, _)| record.clone()).collect();
            let controlled = control.is_none_or(|kind| {
                since
                    .iter()
                    .any(|(record, _)| record.kind == CONTROL_KIND && record.key == kind)
            });
            missing(&records, expected).is_empty() && controlled
        };
        // A source may report caught up before a change it already accepted is
        // readable (a replication stream), so caught up alone does not end the drain.
        let mut report = RunReport::default();
        loop {
            let run = self
                .runtime_mut()?
                .run_while(|_| !done() && started.elapsed() < DRAIN_DEADLINE)?;
            if run.delivered + run.quarantined > 0 {
                report.checkpoint = run.checkpoint;
            }
            report.delivered += run.delivered;
            report.quarantined += run.quarantined;
            if done() || started.elapsed() >= DRAIN_DEADLINE {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        let records: Vec<Record> = lock(&self.delivered)[mark..]
            .iter()
            .map(|(record, _)| record.clone())
            .collect();
        let mut previous = self.checkpoint;
        for record in records.iter().filter(|record| record.kind != CONTROL_KIND) {
            if previous.is_some_and(|previous| record.position <= previous) {
                return Err(Error::new(format!(
                    "position {} does not follow {previous:?}",
                    record.position
                )));
            }
            previous = Some(record.position);
        }
        let committed = self.committed()?;
        if report.delivered + report.quarantined > 0 && committed != Some(report.checkpoint) {
            return Err(Error::new(format!(
                "durable checkpoint {committed:?} differs from the run's {}",
                report.checkpoint
            )));
        }
        self.checkpoint = committed;
        Ok(records
            .into_iter()
            .filter(|record| record.kind != CONTROL_KIND)
            .collect())
    }

    fn committed(&self) -> Result<Option<Position>, Error> {
        Ok(FileCheckpoint::open(self.state_dir)?
            .load(&self.id, INSTANCE)?
            .map(|checkpoint| checkpoint.position))
    }
}

/// Expected changes no delivered record covers; each record covers at most one change.
fn missing(records: &[Record], expected: &[Change]) -> Vec<Change> {
    let mut used = vec![false; records.len()];
    expected
        .iter()
        .filter(|change| {
            match records
                .iter()
                .enumerate()
                .position(|(index, record)| !used[index] && change.matches(record))
            {
                Some(index) => {
                    used[index] = true;
                    false
                }
                None => true,
            }
        })
        .cloned()
        .collect()
}

/// Every expected change has its own delivered record.
fn require_keys(records: &[Record], expected: &[Change]) -> Result<(), Error> {
    match missing(records, expected).first() {
        None => Ok(()),
        Some(change) => Err(Error::new(format!(
            "no record for key {:?} with {} = {}",
            change.key, change.field, change.value
        ))),
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poison| poison.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contract::{CheckReport, DiscoveredKind};
    use crate::schema::Schema;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(0);

    fn state_dir() -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "hashi-conformance-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ))
    }

    /// A change log with a schema revision, shared between restarts like a real source.
    #[derive(Clone, Default)]
    struct Log(Arc<Mutex<(Vec<String>, u64)>>);

    struct LogSource {
        log: Log,
        /// Replays committed positions, breaking the contract on purpose.
        replay: bool,
    }

    impl Connector for LogSource {
        fn id(&self) -> &str {
            "log"
        }
        fn check(&self) -> Result<CheckReport, Error> {
            Ok(CheckReport {
                ok: true,
                message: "ok".into(),
            })
        }
        fn discover(&self) -> Result<Vec<DiscoveredKind>, Error> {
            let mut schema = Schema::text();
            schema.version = lock(&self.log.0).1;
            Ok(vec![DiscoveredKind {
                kind: "entry".into(),
                schema,
                metadata: std::collections::BTreeMap::new(),
            }])
        }
        fn read(
            &mut self,
            from: Option<Position>,
            _: Option<&str>,
        ) -> Result<Option<Record>, Error> {
            let next = match (from, self.replay) {
                (Some(from), false) => from + 1,
                (Some(from), true) => from.saturating_sub(1),
                (None, _) => 0,
            };
            let log = lock(&self.log.0);
            Ok(log.0.get(next as usize).map(|key| Record {
                source: None,
                kind: "entry".into(),
                key: key.clone(),
                source_ts_ms: 0,
                ingest_ts_ms: 0,
                quality: None,
                position: next,
                payload: format!("{{\"seq\":{next}}}"),
            }))
        }
    }

    struct LogFamily {
        log: Log,
        replay: bool,
    }

    impl Family for LogFamily {
        type Source = LogSource;
        fn connect(&mut self) -> Result<LogSource, Error> {
            Ok(LogSource {
                log: self.log.clone(),
                replay: self.replay,
            })
        }
        fn produce(&mut self, keys: &[&str]) -> Result<Vec<Change>, Error> {
            let mut log = lock(&self.log.0);
            Ok(keys
                .iter()
                .map(|key| {
                    log.0.push(key.to_string());
                    Change::new(*key, "seq", log.0.len() - 1)
                })
                .collect())
        }
        fn drift(&mut self) -> Result<Drift, Error> {
            lock(&self.log.0).1 += 1;
            Ok(Drift::Reported("entry".into()))
        }
    }

    #[test]
    fn a_conforming_source_passes_every_check() {
        let log = Log::default();
        lock(&log.0).1 = 1;
        let mut family = LogFamily { log, replay: false };
        assert_eq!(run(&mut family, &state_dir()).unwrap(), CHECKS);
    }

    #[test]
    fn a_source_that_replays_committed_positions_fails() {
        let log = Log::default();
        lock(&log.0).1 = 1;
        let mut family = LogFamily { log, replay: true };
        let error = run(&mut family, &state_dir()).unwrap_err();
        assert!(error.message.starts_with("snapshot: "), "{}", error.message);
    }
}
