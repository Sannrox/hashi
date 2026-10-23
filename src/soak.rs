//! Soak harness (#48): loops produce and drain over a conformance [`Family`] for a
//! configured duration, injects faults through the family's [`Faults`] hooks, and
//! reports lag percentiles, throughput and gaps per family.
//!
//! Every change a family produces must be delivered by the end of the run. One
//! that never is, is a gap: the report names it and the run fails. A driver
//! calls [`soak`] and writes the [`SoakReport`] as JSON.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::Serialize;

use crate::Error;
use crate::checkpoint::FileCheckpoint;
use crate::conformance::{Change, Family};
use crate::contract::{IdempotencyKey, Record};
use crate::runtime::{Runtime, RuntimeConfig, SystemClock};
use crate::schema::CONTROL_KIND;
use crate::sink::{DeliverError, Sink};

pub const REPORT_SCHEMA: &str = "hashi.soak-report/v1";
/// Runtime instance name the harness checkpoints under.
pub const INSTANCE: &str = "soak";
/// Each runtime call reads for at most this long before the harness matches deliveries.
const SLICE: Duration = Duration::from_millis(200);
/// Consecutive failed reconnects that end the run.
const MAX_RECONNECT_FAILURES: u32 = 5;
/// Extra time past the configured duration before a watchdog ends a stuck run.
/// A connector read can block, so only the process exit can end it.
const WATCHDOG_MARGIN: Duration = Duration::from_secs(600);

/// A fault the harness injects through a family's fixture.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Fault {
    /// The fixture stops answering (a paused container) and then resumes. The
    /// connector keeps reading through it and must carry on afterwards.
    NetworkDrop,
    /// The source restarts under the live connector. Its session is gone, so the
    /// harness rebuilds the connector once the source is back.
    SourceRestart,
    /// The source credential changes under the live connector. The harness
    /// rebuilds the connector with the new one.
    CredentialRotation,
}

impl Fault {
    /// A restarted source or a rotated credential ends the live session, so the
    /// harness rebuilds the connector from the durable checkpoint after healing.
    fn reconnects(self) -> bool {
        !matches!(self, Self::NetworkDrop)
    }
}

/// Fault hooks over a family's fixture.
pub trait Faults: Family {
    /// The faults this fixture can inject. The harness rotates through them.
    fn faults(&self) -> Vec<Fault>;

    /// Starts `fault`: pauses the fixture, restarts the source, or rotates the
    /// credential that the next [`Family::connect`] must use.
    fn inject(&mut self, fault: Fault) -> Result<(), Error>;

    /// Ends `fault` and returns once the fixture serves again.
    fn heal(&mut self, fault: Fault) -> Result<(), Error>;
}

/// How long to soak and how often to inject a fault.
#[derive(Debug, Clone)]
pub struct SoakConfig {
    /// Produce-and-drain time, after which the run drains what is outstanding.
    pub duration: Duration,
    /// Time between faults. `None` spreads one round of the family's faults
    /// evenly over the duration.
    pub fault_interval: Option<Duration>,
    /// How long a fault lasts before it is healed.
    pub fault_window: Duration,
    /// Longest wait for one change, and for the final drain.
    pub drain_deadline: Duration,
}

impl SoakConfig {
    pub fn new(duration: Duration) -> Self {
        Self {
            duration,
            fault_interval: None,
            fault_window: Duration::from_secs(2),
            drain_deadline: Duration::from_secs(60),
        }
    }
}

/// One family's soak result. `passed` holds only with no gap and no error.
#[derive(Debug, Clone, Serialize)]
pub struct SoakReport {
    pub schema: &'static str,
    pub family: String,
    pub duration_ms: u64,
    pub cycles: u64,
    /// Changes produced, including seeded state.
    pub produced: u64,
    /// Data records delivered, counting redeliveries.
    pub delivered: u64,
    pub throughput_per_sec: f64,
    /// Produce-to-delivery lag of each change's first delivery.
    pub lag_ms: Lag,
    pub faults: BTreeMap<Fault, u64>,
    pub reconnects: u64,
    /// Reads that failed and rebuilt the connector, and the last such failure.
    pub read_errors: u64,
    pub last_read_error: Option<String>,
    /// Changes never delivered.
    pub gaps: Vec<Change>,
    /// Why the run stopped early, if it did.
    pub error: Option<String>,
    pub passed: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct Lag {
    pub p50: u64,
    pub p95: u64,
    pub p99: u64,
    pub max: u64,
}

impl Lag {
    fn of(mut samples: Vec<u64>) -> Self {
        samples.sort_unstable();
        let at = |quantile: f64| {
            let index = ((samples.len() as f64 * quantile).ceil() as usize).saturating_sub(1);
            samples.get(index).copied().unwrap_or(0)
        };
        Self {
            p50: at(0.50),
            p95: at(0.95),
            p99: at(0.99),
            max: samples.last().copied().unwrap_or(0),
        }
    }
}

impl SoakReport {
    /// Writes the report as pretty JSON to `path`.
    pub fn write(&self, path: &Path) -> Result<(), Error> {
        let json = serde_json::to_vec_pretty(self)
            .map_err(|error| Error::new(format!("soak report: {error}")))?;
        std::fs::write(path, json)
            .map_err(|error| Error::new(format!("soak report {}: {error}", path.display())))
    }
}

/// Soaks `family` for `config.duration`, keeping checkpoints in `state_dir`.
/// Failures are part of the report, never a panic.
pub fn soak<F: Faults>(
    family: &mut F,
    name: &str,
    state_dir: &Path,
    config: &SoakConfig,
) -> SoakReport {
    let watchdog = config.duration + config.drain_deadline + WATCHDOG_MARGIN;
    let running = Arc::new(Mutex::new(true));
    let watched = running.clone();
    let family_name = name.to_string();
    std::thread::spawn(move || {
        std::thread::sleep(watchdog);
        if *lock(&watched) {
            eprintln!("soak: {family_name} did not finish within {watchdog:?}");
            std::process::exit(101);
        }
    });
    let mut run = Soak::new(family, state_dir, config);
    let started = Instant::now();
    let error = run.run().err().map(|error| error.message);
    *lock(&running) = false;
    run.report(name, started.elapsed(), error)
}

/// Soaks for `HASHI_SOAK_SECS` (else `default`), injecting a fault every
/// `HASHI_SOAK_FAULT_SECS` when set, and writes the report to
/// `$HASHI_SOAK_REPORT_DIR/<name>.json` when that is set. Family drivers call it
/// so one test serves the short CI run and the nightly run.
pub fn soak_from_env<F: Faults>(
    family: &mut F,
    name: &str,
    state_dir: &Path,
    default: Duration,
) -> SoakReport {
    let secs = |var: &str| {
        std::env::var(var)
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .map(Duration::from_secs)
    };
    let mut config = SoakConfig::new(secs("HASHI_SOAK_SECS").unwrap_or(default));
    config.fault_interval = secs("HASHI_SOAK_FAULT_SECS");
    let mut report = soak(family, name, state_dir, &config);
    if let Some(dir) = std::env::var_os("HASHI_SOAK_REPORT_DIR") {
        let dir = Path::new(&dir);
        let written = std::fs::create_dir_all(dir)
            .map_err(|error| Error::new(format!("soak report {}: {error}", dir.display())))
            .and_then(|()| report.write(&dir.join(format!("{name}.json"))));
        // A nightly run without its report is not evidence.
        if let Err(error) = written {
            report.error.get_or_insert(error.message);
            report.passed = false;
        }
    }
    report
}

type Delivered = Arc<Mutex<Vec<(Record, Instant)>>>;

/// Records every acknowledged delivery with its arrival time.
struct Recorder(Delivered);

impl Sink for Recorder {
    fn deliver(&mut self, record: &Record, _: &IdempotencyKey) -> Result<(), DeliverError> {
        lock(&self.0).push((record.clone(), Instant::now()));
        Ok(())
    }
}

struct Soak<'a, F: Faults> {
    family: &'a mut F,
    state_dir: &'a Path,
    config: &'a SoakConfig,
    runtime: Option<Runtime<F::Source, Recorder, FileCheckpoint, SystemClock>>,
    /// Deliveries not yet matched; drained by each match, so a long run stays bounded.
    delivered: Delivered,
    /// Data records delivered so far, counting redeliveries.
    delivered_count: u64,
    /// Produced changes not yet delivered, with their produce time.
    outstanding: Vec<(Change, Instant)>,
    lags: Vec<u64>,
    cycles: u64,
    produced: u64,
    faults: BTreeMap<Fault, u64>,
    reconnects: u64,
    read_errors: u64,
    last_read_error: Option<String>,
}

impl<'a, F: Faults> Soak<'a, F> {
    fn new(family: &'a mut F, state_dir: &'a Path, config: &'a SoakConfig) -> Self {
        Self {
            family,
            state_dir,
            config,
            runtime: None,
            delivered: Arc::new(Mutex::new(Vec::new())),
            delivered_count: 0,
            outstanding: Vec::new(),
            lags: Vec::new(),
            cycles: 0,
            produced: 0,
            faults: BTreeMap::new(),
            reconnects: 0,
            read_errors: 0,
            last_read_error: None,
        }
    }

    fn run(&mut self) -> Result<(), Error> {
        let seeded = self.family.seed()?;
        self.expect(seeded);
        self.connect()?;
        let faults = self.family.faults();
        let interval = self
            .config
            .fault_interval
            .unwrap_or_else(|| self.config.duration / (faults.len() as u32 + 1));
        let started = Instant::now();
        let mut next_fault = interval;
        let mut rotation = faults.iter().copied().cycle();
        while started.elapsed() < self.config.duration {
            if !faults.is_empty()
                && started.elapsed() >= next_fault
                && let Some(fault) = rotation.next()
            {
                self.fault(fault)?;
                next_fault += interval;
            }
            self.produce()?;
            // One change at a time, so a sampling source never overwrites one
            // before it is read.
            self.pump(self.config.drain_deadline)?;
        }
        self.pump(self.config.drain_deadline)
    }

    fn expect(&mut self, changes: Vec<Change>) {
        let now = Instant::now();
        self.produced += changes.len() as u64;
        self.outstanding
            .extend(changes.into_iter().map(|change| (change, now)));
    }

    /// Produces a change, injects `fault` with the connector live, keeps reading
    /// through the fault window, heals it, and rebuilds the connector when the
    /// fault ends its session. The change in flight must still arrive.
    fn fault(&mut self, fault: Fault) -> Result<(), Error> {
        self.produce()?;
        self.family.inject(fault)?;
        self.read_through(self.config.fault_window);
        self.family.heal(fault)?;
        *self.faults.entry(fault).or_default() += 1;
        if fault.reconnects() {
            self.runtime = None;
            self.reconnect()?;
        }
        // Drain the in-flight change before the next one, so a sampling source
        // that missed it during the outage reads it now instead of a newer value.
        self.pump(self.config.drain_deadline)
    }

    /// Keeps the connector reading while a fault is held, so it meets the
    /// outage. Failed reads, and reconnects that fail while the source is
    /// away, are expected here: they are counted, never fatal.
    fn read_through(&mut self, window: Duration) {
        let started = Instant::now();
        while started.elapsed() < window {
            match self.runtime.as_mut() {
                Some(runtime) => {
                    let slice = Instant::now();
                    if let Err(error) = runtime.run_while(|_| slice.elapsed() < SLICE) {
                        self.read_errors += 1;
                        self.last_read_error = Some(error.message);
                        self.runtime = None;
                    }
                }
                None => {
                    if self.connect().is_ok() {
                        self.reconnects += 1;
                    }
                }
            }
            self.match_deliveries();
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    fn produce(&mut self) -> Result<(), Error> {
        let key = format!("s{}", self.cycles);
        let changes = self.family.produce(&[key.as_str()])?;
        self.expect(changes);
        self.cycles += 1;
        Ok(())
    }

    /// Reads until every outstanding change is delivered or `deadline` passes.
    /// A failed read rebuilds the connector from the durable checkpoint.
    fn pump(&mut self, deadline: Duration) -> Result<(), Error> {
        let started = Instant::now();
        while !self.outstanding.is_empty() && started.elapsed() < deadline {
            let slice = Instant::now();
            // Stop reading once every outstanding change has arrived: a read on
            // a quiet source may block until the next change, which never comes.
            let delivered = self.delivered.clone();
            let wanted: Vec<Change> = self
                .outstanding
                .iter()
                .map(|(change, _)| change.clone())
                .collect();
            let arrived = move || {
                let delivered = lock(&delivered);
                wanted
                    .iter()
                    .all(|change| delivered.iter().any(|(record, _)| change.matches(record)))
            };
            let runtime = match self.runtime.as_mut() {
                Some(runtime) => runtime,
                None => {
                    self.reconnect()?;
                    continue;
                }
            };
            if let Err(error) = runtime.run_while(|_| !arrived() && slice.elapsed() < SLICE) {
                self.read_errors += 1;
                self.last_read_error = Some(error.message);
                self.runtime = None;
                // A source that fails every read must not spin reconnects.
                std::thread::sleep(Duration::from_millis(500));
                self.reconnect()?;
            }
            self.match_deliveries();
            if !self.outstanding.is_empty() {
                std::thread::sleep(Duration::from_millis(20));
            }
        }
        Ok(())
    }

    fn match_deliveries(&mut self) {
        let delivered = std::mem::take(&mut *lock(&self.delivered));
        for (record, at) in &delivered {
            if record.kind == CONTROL_KIND {
                continue;
            }
            self.delivered_count += 1;
            if let Some(index) = self
                .outstanding
                .iter()
                .position(|(change, _)| change.matches(record))
            {
                let (_, produced) = self.outstanding.remove(index);
                self.lags
                    .push(at.saturating_duration_since(produced).as_millis() as u64);
            }
        }
    }

    fn connect(&mut self) -> Result<(), Error> {
        let source = self.family.connect()?;
        let store = FileCheckpoint::open(self.state_dir)?;
        self.runtime = Some(Runtime::new(
            source,
            Recorder(self.delivered.clone()),
            store,
            SystemClock,
            RuntimeConfig::new(INSTANCE),
        ));
        Ok(())
    }

    fn reconnect(&mut self) -> Result<(), Error> {
        let mut failures = 0;
        loop {
            match self.connect() {
                Ok(()) => {
                    self.reconnects += 1;
                    return Ok(());
                }
                Err(error) if failures + 1 >= MAX_RECONNECT_FAILURES => {
                    return Err(Error::new(format!("reconnect: {}", error.message)));
                }
                Err(_) => {
                    failures += 1;
                    std::thread::sleep(Duration::from_secs(1));
                }
            }
        }
    }

    fn report(mut self, name: &str, elapsed: Duration, error: Option<String>) -> SoakReport {
        self.match_deliveries();
        let delivered = self.delivered_count;
        let gaps: Vec<Change> = self
            .outstanding
            .into_iter()
            .map(|(change, _)| change)
            .collect();
        SoakReport {
            schema: REPORT_SCHEMA,
            family: name.to_string(),
            duration_ms: elapsed.as_millis() as u64,
            cycles: self.cycles,
            produced: self.produced,
            delivered,
            throughput_per_sec: delivered as f64 / elapsed.as_secs_f64().max(f64::EPSILON),
            lag_ms: Lag::of(self.lags),
            faults: self.faults,
            reconnects: self.reconnects,
            read_errors: self.read_errors,
            last_read_error: self.last_read_error,
            passed: gaps.is_empty() && error.is_none(),
            gaps,
            error,
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poison| poison.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conformance::Drift;
    use crate::connector::Connector;
    use crate::contract::{CheckReport, DiscoveredKind, Position};
    use crate::schema::Schema;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(0);

    fn state_dir() -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "hashi-soak-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ))
    }

    /// A change log that can go down, restart and rotate its credential.
    #[derive(Default)]
    struct Fixture {
        log: Vec<String>,
        down: bool,
        generation: u64,
        credential: u64,
    }

    #[derive(Clone, Default)]
    struct Shared(Arc<Mutex<Fixture>>);

    struct LogSource {
        fixture: Shared,
        generation: u64,
        credential: u64,
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
            Ok(vec![DiscoveredKind {
                kind: "entry".into(),
                schema: Schema::text(),
                metadata: BTreeMap::new(),
            }])
        }
        fn read(
            &mut self,
            from: Option<Position>,
            _: Option<&str>,
        ) -> Result<Option<Record>, Error> {
            let fixture = lock(&self.fixture.0);
            if fixture.down {
                return Err(Error::new("unreachable"));
            }
            if fixture.generation != self.generation || fixture.credential != self.credential {
                return Err(Error::new("session ended"));
            }
            let next = from.map_or(0, |from| from + 1);
            Ok(fixture.log.get(next as usize).map(|key| Record {
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
        fixture: Shared,
        /// The produce call whose change is lost before it reaches the log.
        drop_at: Option<usize>,
        produced: usize,
    }

    impl Family for LogFamily {
        type Source = LogSource;
        fn connect(&mut self) -> Result<LogSource, Error> {
            let fixture = lock(&self.fixture.0);
            if fixture.down {
                return Err(Error::new("unreachable"));
            }
            Ok(LogSource {
                fixture: self.fixture.clone(),
                generation: fixture.generation,
                credential: fixture.credential,
            })
        }
        fn produce(&mut self, keys: &[&str]) -> Result<Vec<Change>, Error> {
            let mut fixture = lock(&self.fixture.0);
            let mut changes = Vec::new();
            for key in keys {
                changes.push(Change::new(*key, "seq", fixture.log.len()));
                // A dropped change never reaches the log, so no record carries its key.
                if self.drop_at != Some(self.produced) {
                    fixture.log.push(key.to_string());
                }
                self.produced += 1;
            }
            Ok(changes)
        }
        fn drift(&mut self) -> Result<Drift, Error> {
            Ok(Drift::Reported("entry".into()))
        }
    }

    impl Faults for LogFamily {
        fn faults(&self) -> Vec<Fault> {
            vec![
                Fault::NetworkDrop,
                Fault::SourceRestart,
                Fault::CredentialRotation,
            ]
        }
        fn inject(&mut self, fault: Fault) -> Result<(), Error> {
            let mut fixture = lock(&self.fixture.0);
            match fault {
                Fault::NetworkDrop => fixture.down = true,
                Fault::SourceRestart => fixture.generation += 1,
                Fault::CredentialRotation => fixture.credential += 1,
            }
            Ok(())
        }
        fn heal(&mut self, _: Fault) -> Result<(), Error> {
            lock(&self.fixture.0).down = false;
            Ok(())
        }
    }

    fn config() -> SoakConfig {
        SoakConfig {
            duration: Duration::from_millis(1500),
            fault_interval: Some(Duration::from_millis(300)),
            fault_window: Duration::from_millis(50),
            drain_deadline: Duration::from_secs(5),
        }
    }

    #[test]
    fn a_short_run_injects_every_fault_and_passes() {
        let mut family = LogFamily {
            fixture: Shared::default(),
            drop_at: None,
            produced: 0,
        };
        let dir = state_dir();
        let report = soak(&mut family, "log", &dir, &config());
        assert!(report.passed, "{report:?}");
        assert!(report.gaps.is_empty());
        for fault in family.faults() {
            assert!(
                report.faults.get(&fault).copied().unwrap_or(0) >= 1,
                "{fault:?}: {report:?}"
            );
        }
        assert!(report.reconnects >= 2, "{report:?}");
        assert_eq!(report.produced, report.cycles);
        assert!(report.delivered >= report.produced);
        assert!(report.lag_ms.max >= report.lag_ms.p50);

        let path = dir.join("report.json");
        report.write(&path).unwrap();
        let json: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(json["schema"], REPORT_SCHEMA);
        assert_eq!(
            json["faults"]["network_drop"].as_u64().map(|n| n >= 1),
            Some(true)
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_dropped_change_fails_with_the_gap_named() {
        let mut family = LogFamily {
            fixture: Shared::default(),
            drop_at: Some(2),
            produced: 0,
        };
        let mut config = config();
        config.drain_deadline = Duration::from_millis(300);
        let dir = state_dir();
        let report = soak(&mut family, "log", &dir, &config);
        assert!(!report.passed);
        assert_eq!(report.error, None);
        assert_eq!(report.gaps.len(), 1, "{report:?}");
        assert_eq!(report.gaps[0].key, "s2");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn lag_percentiles_use_nearest_rank() {
        let lag = Lag::of((1..=100).collect());
        assert_eq!(
            lag,
            Lag {
                p50: 50,
                p95: 95,
                p99: 99,
                max: 100
            }
        );
        assert_eq!(Lag::of(Vec::new()), Lag::default());
    }
}
