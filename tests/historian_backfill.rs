//! Historian backfill, restart and handover to the live tail (#22).

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};

use hashi::checkpoint::{Checkpoint, CheckpointStore, QuarantineRow};
use hashi::historian::{Historian, HistorianConfig, HistorianConnector, Sample, Tag};
use hashi::runtime::Clock;
use hashi::{Error, InstanceLease, MemorySink, Runtime, RuntimeConfig};

const DAY_MS: i64 = 86_400_000;
const START_MS: i64 = 1_700_000_000_000;
const TAGS: usize = 1_000;
const DAYS: i64 = 365;

static LEASE_SEQ: AtomicU64 = AtomicU64::new(0);

/// A fixture historian: one sample per tag per day, appended as time passes.
struct Fixture {
    now: Rc<Cell<i64>>,
    reads: Rc<Cell<u64>>,
}

impl Historian for Fixture {
    fn tags(&mut self) -> Result<Vec<Tag>, Error> {
        Ok((0..TAGS)
            .map(|index| Tag {
                name: format!("tag-{index:04}"),
            })
            .collect())
    }

    fn read_raw(
        &mut self,
        tag: &str,
        after_ms: i64,
        until_ms: i64,
        limit: usize,
    ) -> Result<Vec<Sample>, Error> {
        self.reads.set(self.reads.get() + 1);
        // Only samples that exist by now; the first is a day after START_MS.
        let until = until_ms.min(self.now.get());
        let first = (after_ms - START_MS).div_euclid(DAY_MS) + 1;
        Ok((first.max(1)..)
            .map(|day| START_MS + day * DAY_MS)
            .take_while(|ts| *ts <= until)
            .take(limit)
            .map(|ts_ms| Sample {
                ts_ms,
                value: (ts_ms / DAY_MS) as f64 + tag.len() as f64,
                quality: Some("good".into()),
            })
            .collect())
    }

    fn read_interpolated(
        &mut self,
        _tag: &str,
        start_ms: i64,
        end_ms: i64,
        step_ms: i64,
    ) -> Result<Vec<Sample>, Error> {
        Ok((start_ms..=end_ms)
            .step_by(step_ms as usize)
            .map(|ts_ms| Sample {
                ts_ms,
                value: 0.0,
                quality: None,
            })
            .collect())
    }
}

#[derive(Clone)]
struct FixtureClock(Rc<Cell<i64>>);

impl Clock for FixtureClock {
    fn now_ms(&self) -> i64 {
        self.0.get()
    }

    fn sleep_ms(&self, _ms: u64) {}
}

/// An in-memory checkpoint store, so a year of commits stays fast.
#[derive(Clone, Default)]
struct MemoryStore(Rc<RefCell<Option<Checkpoint>>>);

impl CheckpointStore for MemoryStore {
    fn acquire(&self, _connector: &str, _instance: &str) -> Result<InstanceLease, Error> {
        Ok(InstanceLease::unfenced(1))
    }

    fn load_schemas(&self, _: &str, _: &str) -> Result<hashi::schema::SchemaState, Error> {
        Ok(Default::default())
    }

    fn save_schemas(
        &self,
        _: &str,
        _: &str,
        _: &hashi::schema::SchemaState,
        _: u64,
    ) -> Result<(), Error> {
        Ok(())
    }

    fn load(&self, _: &str, _: &str) -> Result<Option<Checkpoint>, Error> {
        Ok(self.0.borrow().clone())
    }

    fn commit(&self, checkpoint: &Checkpoint) -> Result<(), Error> {
        *self.0.borrow_mut() = Some(checkpoint.clone());
        Ok(())
    }

    fn quarantine(&self, _: &QuarantineRow, _: u64) -> Result<(), Error> {
        Err(Error::new("no quarantine expected"))
    }

    fn quarantined(&self, _: &str, _: &str) -> Result<Vec<QuarantineRow>, Error> {
        Ok(Vec::new())
    }
}

fn config(state_dir: &std::path::Path) -> HistorianConfig {
    HistorianConfig {
        start_ms: START_MS,
        chunk_ms: 30 * DAY_MS,
        page_limit: 64,
        settle_ms: 0,
        max_reads_per_second: 0,
        state_dir: state_dir.to_path_buf(),
    }
}

fn runtime(
    now: &Rc<Cell<i64>>,
    reads: &Rc<Cell<u64>>,
    store: &MemoryStore,
    state: &std::path::Path,
) -> Runtime<HistorianConnector<Fixture, FixtureClock>, MemorySink, MemoryStore, FixtureClock> {
    let historian = Fixture {
        now: now.clone(),
        reads: reads.clone(),
    };
    let connector = HistorianConnector::new(
        "historian",
        historian,
        FixtureClock(now.clone()),
        config(state),
    )
    .unwrap();
    Runtime::new(
        connector,
        MemorySink::new(),
        store.clone(),
        FixtureClock(now.clone()),
        RuntimeConfig::new("plant"),
    )
}

/// `(tag, ts)` of every delivered sample, counting repeats.
fn delivered(sinks: &[MemorySink]) -> BTreeMap<(String, i64), usize> {
    let mut seen = BTreeMap::new();
    for sink in sinks {
        for (record, _) in &sink.delivered {
            let (tag, ts) = record.key.split_once('@').unwrap();
            *seen
                .entry((tag.to_string(), ts.parse().unwrap()))
                .or_insert(0) += 1;
        }
    }
    seen
}

fn expected(days: i64) -> BTreeSet<(String, i64)> {
    (0..TAGS)
        .flat_map(|index| {
            (1..=days).map(move |day| (format!("tag-{index:04}"), START_MS + day * DAY_MS))
        })
        .collect()
}

#[test]
fn a_year_of_backfill_resumes_after_a_kill_and_hands_over_to_the_tail() {
    let now = Rc::new(Cell::new(START_MS + DAYS * DAY_MS));
    let reads = Rc::new(Cell::new(0));
    let store = MemoryStore::default();
    let state = std::env::temp_dir().join(format!(
        "hashi-historian-state-{}-{}",
        std::process::id(),
        LEASE_SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let total = TAGS * DAYS as usize;

    // Kill the first process part way through the backfill.
    let mut first = runtime(&now, &reads, &store, &state);
    let report = first
        .run_while(|report| report.delivered < (total / 3) as u64)
        .unwrap();
    assert_eq!(report.delivered, (total / 3) as u64);
    let (_, first_sink, _) = first.into_parts();
    let checkpoint = store.0.borrow().clone().unwrap();
    assert!(checkpoint.source_cursor.as_ref().unwrap().len() < 4096);

    // A new process resumes from the committed cursor and completes it.
    let mut second = runtime(&now, &reads, &store, &state);
    let report = second.run().unwrap();
    assert_eq!(report.delivered, (total - total / 3) as u64);
    let (_, second_sink, _) = second.into_parts();
    let backfilled = delivered(&[first_sink, second_sink]);
    assert!(backfilled.values().all(|count| *count == 1), "overlap");
    assert_eq!(
        backfilled.keys().cloned().collect::<BTreeSet<_>>(),
        expected(DAYS),
        "gap"
    );
    // Chunked reads: about one per tag per 30-day chunk, not one per sample.
    assert!(reads.get() < (TAGS as u64) * 20, "{} reads", reads.get());

    // Three live days later, the tail delivers exactly the new samples.
    now.set(now.get() + 3 * DAY_MS);
    let mut third = runtime(&now, &reads, &store, &state);
    let report = third.run().unwrap();
    assert_eq!(report.delivered, (TAGS * 3) as u64);
    let (_, tail_sink, _) = third.into_parts();
    let tail = delivered(&[tail_sink]);
    assert!(tail.values().all(|count| *count == 1));
    assert_eq!(
        tail.keys().cloned().collect::<BTreeSet<_>>(),
        expected(DAYS + 3)
            .difference(&expected(DAYS))
            .cloned()
            .collect::<BTreeSet<_>>()
    );
    // Caught up: another pass delivers nothing.
    assert_eq!(
        runtime(&now, &reads, &store, &state)
            .run()
            .unwrap()
            .delivered,
        0
    );
}
