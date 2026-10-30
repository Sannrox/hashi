//! One historian trait, chunked backfill and a gap-free live tail.
//!
//! A [`Historian`] discovers tags and reads samples by time range, raw or
//! interpolated, in pages. [`HistorianConnector`] turns any historian into a
//! [`Connector`]: it backfills every tag from `start_ms` in time chunks, then
//! follows the live tail, and checkpoints per tag in the connector cursor.
//!
//! Work runs in rounds over the window `(lower, upper]`, tag by tag in name
//! order. The first round is the backfill, up to `now - settle_ms` when it
//! opens; each later round is a tail round from the previous `upper` to
//! `now - settle_ms`. A round's bounds are written to the connector's leased
//! state directory before any of its records is delivered, so a restart
//! reuses them and re-emits the same records at the same positions, and a
//! redelivered record keeps its idempotency key. An empty round is widened in
//! place rather than followed by another, so the saved round always starts
//! where the last checkpointed round ended. The cursor holds the round,
//! the tag index, that tag's watermark and a fingerprint of the tag set,
//! which implies every tag's checkpoint (earlier tags are at `upper`, later
//! ones at `lower`) in a few bytes. Each tag reads strictly after its
//! watermark, so backfill hands over to the tail with no gap and no overlap.
//! A changed tag set or backfill start refuses the old cursor. `tail` must be
//! replayable from the watermark.
//!
//! Timestamps of one tag must strictly increase; a historian that returns a
//! sample at or before the watermark is refused. Samples written later than
//! `settle_ms` behind real time are outside this contract.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::Error;
use crate::checkpoint::atomic_write;
use crate::connector::Connector;
use crate::contract::{CheckReport, DiscoveredKind, Position, Record, SourceMetadata};
use crate::runtime::Clock;

pub const SAMPLE_KIND: &str = "historian.sample";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tag {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Sample {
    pub ts_ms: i64,
    pub value: f64,
    pub quality: Option<String>,
}

/// One historian. Implementations wrap a historian's own API.
pub trait Historian {
    fn tags(&mut self) -> Result<Vec<Tag>, Error>;
    /// Raw samples of `tag` with `after_ms < ts <= until_ms`, ascending, at
    /// most `limit`. A full page means more may follow.
    fn read_raw(
        &mut self,
        tag: &str,
        after_ms: i64,
        until_ms: i64,
        limit: usize,
    ) -> Result<Vec<Sample>, Error>;
    /// Interpolated samples of `tag` every `step_ms` from `start_ms` to
    /// `end_ms` inclusive.
    fn read_interpolated(
        &mut self,
        tag: &str,
        start_ms: i64,
        end_ms: i64,
        step_ms: i64,
    ) -> Result<Vec<Sample>, Error>;
    /// Live samples of `tag` with `after_ms < ts <= until_ms`. Must be
    /// replayable: called again with the same arguments, for example after a
    /// restart, it returns the same samples. Polling historians use the raw
    /// read; a subscribing adapter keeps received samples durably until the
    /// watermark passes them, so a crash never drops what it drained.
    fn tail(
        &mut self,
        tag: &str,
        after_ms: i64,
        until_ms: i64,
        limit: usize,
    ) -> Result<Vec<Sample>, Error> {
        self.read_raw(tag, after_ms, until_ms, limit)
    }
    /// Series the source knows but cannot read history for. `check` reports
    /// them; the connector still runs over the tags it has.
    fn without_history(&self) -> &[String] {
        &[]
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistorianConfig {
    /// Backfill start; samples at or before it are not read.
    pub start_ms: i64,
    /// Backfill time chunk per read.
    pub chunk_ms: i64,
    /// Samples per read.
    pub page_limit: usize,
    /// How far behind real time the tail stays, so late writes still land.
    pub settle_ms: i64,
    /// Historian reads per second, at millisecond resolution; 0 is unlimited.
    pub max_reads_per_second: u32,
    /// Dedicated, leased directory for the open round's bounds.
    pub state_dir: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    v: u8,
    backfill: bool,
    lower: i64,
    upper: i64,
    tag: usize,
    watermark: i64,
    /// SHA-256 prefix of the backfill start and the sorted tag names the
    /// indexes refer to.
    tags: String,
}

/// A round's bounds, durable before its first record is delivered.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Round {
    v: u8,
    backfill: bool,
    lower: i64,
    upper: i64,
    tags: String,
}

/// The last page read, so consecutive records do not re-read the historian.
struct Page {
    tag: usize,
    after_ms: i64,
    until_ms: i64,
    samples: Vec<Sample>,
}

pub struct HistorianConnector<H, T> {
    id: String,
    historian: H,
    clock: T,
    config: HistorianConfig,
    tags: Vec<Tag>,
    tag_set: String,
    page: Option<Page>,
    last_read_ms: Option<i64>,
    lease: crate::lease::DropRelease,
}

impl<H: Historian, T: Clock> HistorianConnector<H, T> {
    pub fn new(
        id: impl Into<String>,
        mut historian: H,
        clock: T,
        config: HistorianConfig,
    ) -> Result<Self, Error> {
        if config.chunk_ms <= 0 || config.page_limit == 0 || config.settle_ms < 0 {
            return Err(Error::new("invalid historian configuration"));
        }
        std::fs::create_dir_all(&config.state_dir)?;
        let lease = crate::lease::hold(
            &config.state_dir,
            "historian",
            crate::lease::hold_ttl(),
            "historian state directory is already owned",
        )?;
        // Name order, so cursor indexes do not depend on discovery order.
        let mut tags = historian.tags()?;
        tags.sort_by(|left, right| left.name.cmp(&right.name));
        if tags.windows(2).any(|pair| pair[0].name == pair[1].name) {
            return Err(Error::new("historian reports a tag twice"));
        }
        // Binds cursors and saved rounds to the tag set and backfill start.
        let tag_set = {
            use sha2::{Digest, Sha256};
            let mut hasher = Sha256::new();
            hasher.update(config.start_ms.to_be_bytes());
            for tag in &tags {
                hasher.update(tag.name.as_bytes());
                hasher.update([0]);
            }
            format!("{:x}", hasher.finalize())[..16].to_string()
        };
        Ok(Self {
            id: id.into(),
            historian,
            clock,
            config,
            tags,
            tag_set,
            page: None,
            last_read_ms: None,
            lease,
        })
    }

    pub fn into_historian(self) -> H {
        self.historian
    }

    fn horizon(&self) -> i64 {
        self.clock.now_ms() - self.config.settle_ms
    }

    fn round_path(&self) -> PathBuf {
        self.config.state_dir.join("round.json")
    }

    /// The round that starts at `lower`: the saved one when a crash
    /// interrupted it before its first checkpoint, or a new one up to
    /// `horizon`, saved before use. `None` when nothing new is ready.
    fn open_round(
        &self,
        backfill: bool,
        lower: i64,
        horizon: i64,
    ) -> Result<Option<Cursor>, Error> {
        let saved = match std::fs::read_to_string(self.round_path()) {
            Ok(text) => Some(
                serde_json::from_str::<Round>(&text)
                    .map_err(|_| Error::new("historian round state is unreadable"))?,
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
        let round = match saved {
            Some(round)
                if round.v == 1
                    && round.lower == lower
                    && round.backfill == backfill
                    && round.tags == self.tag_set =>
            {
                round
            }
            _ => {
                let upper = if backfill {
                    horizon.max(lower)
                } else {
                    horizon
                };
                if !backfill && upper <= lower {
                    return Ok(None);
                }
                let round = Round {
                    v: 1,
                    backfill,
                    lower,
                    upper,
                    tags: self.tag_set.clone(),
                };
                write_round(&self.round_path(), &round)?;
                round
            }
        };
        Ok(Some(Cursor {
            v: 1,
            backfill: round.backfill,
            lower: round.lower,
            upper: round.upper,
            tag: 0,
            watermark: round.lower,
            tags: round.tags,
        }))
    }

    /// The empty round `cursor` widened to `horizon`, saved before use.
    fn widen_round(&self, cursor: &Cursor, horizon: i64) -> Result<Option<Cursor>, Error> {
        if horizon <= cursor.upper {
            return Ok(None);
        }
        let round = Round {
            v: 1,
            backfill: cursor.backfill,
            lower: cursor.lower,
            upper: horizon,
            tags: self.tag_set.clone(),
        };
        write_round(&self.round_path(), &round)?;
        Ok(Some(Cursor {
            v: 1,
            backfill: round.backfill,
            lower: round.lower,
            upper: round.upper,
            tag: 0,
            watermark: round.lower,
            tags: round.tags,
        }))
    }

    fn parse(&self, raw: &str) -> Result<Cursor, Error> {
        let cursor: Cursor =
            serde_json::from_str(raw).map_err(|_| Error::new("historian cursor is unreadable"))?;
        if cursor.v != 1
            || cursor.lower > cursor.upper
            || cursor.watermark < cursor.lower
            || cursor.watermark > cursor.upper
            || cursor.tag > self.tags.len()
            || cursor.lower < self.config.start_ms
            || cursor.tags != self.tag_set
        {
            return Err(Error::new(
                "historian cursor does not match this configuration",
            ));
        }
        Ok(cursor)
    }

    /// Wait out the configured read rate before the next historian call.
    fn pace(&mut self) {
        if self.config.max_reads_per_second == 0 {
            return;
        }
        // Rounded up, so pacing never exceeds the configured rate; the
        // resolution is one millisecond.
        let rate = i64::from(self.config.max_reads_per_second);
        let interval = (1000 + rate - 1) / rate;
        if let Some(last) = self.last_read_ms {
            let wait = last + interval - self.clock.now_ms();
            if wait > 0 {
                self.clock.sleep_ms(wait as u64);
            }
        }
        self.last_read_ms = Some(self.clock.now_ms());
    }

    /// The first sample of `tag` in `(after, until]`, from the cached page
    /// when it covers the range.
    fn first_after(
        &mut self,
        tag: usize,
        after: i64,
        until: i64,
        backfill: bool,
    ) -> Result<Option<Sample>, Error> {
        if let Some(page) = &self.page
            && page.tag == tag
            && page.until_ms == until
            && page.after_ms <= after
        {
            let position = page.samples.partition_point(|sample| sample.ts_ms <= after);
            // Past the end of a full page, more may follow: read again.
            if position < page.samples.len() {
                return Ok(Some(page.samples[position].clone()));
            }
            if page.samples.len() < self.config.page_limit {
                return Ok(None);
            }
        }
        self.pace();
        let name = self.tags[tag].name.clone();
        let samples = if backfill {
            self.historian
                .read_raw(&name, after, until, self.config.page_limit)?
        } else {
            self.historian
                .tail(&name, after, until, self.config.page_limit)?
        };
        let mut previous = after;
        for sample in &samples {
            if sample.ts_ms <= previous || sample.ts_ms > until {
                return Err(Error::new(format!(
                    "historian returned tag {name} samples out of order or range"
                )));
            }
            previous = sample.ts_ms;
        }
        let first = samples.first().cloned();
        self.page = Some(Page {
            tag,
            after_ms: after,
            until_ms: until,
            samples,
        });
        Ok(first)
    }
}

impl<H: Historian, T: Clock> Connector for HistorianConnector<H, T> {
    fn id(&self) -> &str {
        &self.id
    }

    fn check(&self) -> Result<CheckReport, Error> {
        self.lease.renew()?;
        let mut message = if self.tags.is_empty() {
            "historian reports no tags".to_string()
        } else {
            format!("{} tags", self.tags.len())
        };
        let without = self.historian.without_history();
        if !without.is_empty() {
            message.push_str(&format!("; {} without history", without.len()));
        }
        // Series without history are a reported condition, not a failure:
        // the source answered, and the connector runs over what it has.
        Ok(CheckReport {
            ok: !self.tags.is_empty() || !without.is_empty(),
            message,
        })
    }

    fn discover(&self) -> Result<Vec<DiscoveredKind>, Error> {
        self.lease.renew()?;
        Ok(vec![DiscoveredKind {
            kind: SAMPLE_KIND.into(),
            schema: crate::schema::Schema::text(),
            metadata: Default::default(),
        }])
    }

    fn read(
        &mut self,
        from: Option<Position>,
        source_cursor: Option<&str>,
    ) -> Result<Option<Record>, Error> {
        self.lease.renew()?;
        // One horizon per read, so catching up always ends.
        let horizon = self.horizon();
        // Whether the current round opened in this call and has emitted
        // nothing: then it is empty, and nothing from it was ever delivered.
        let mut fresh = source_cursor.is_none();
        let mut cursor = match source_cursor {
            Some(raw) => self.parse(raw)?,
            None if from.is_some() => {
                return Err(Error::new(
                    "historian resume needs its cursor; position-only state is refused",
                ));
            }
            None => match self.open_round(true, self.config.start_ms, horizon)? {
                Some(cursor) => cursor,
                None => return Ok(None),
            },
        };
        loop {
            if cursor.tag >= self.tags.len() {
                // An empty round is widened in place rather than followed by
                // another, so the saved round always starts at the committed
                // round's end and a crash replays in the same order.
                let next = if fresh {
                    self.widen_round(&cursor, horizon)?
                } else {
                    self.open_round(false, cursor.upper, horizon)?
                };
                match next {
                    Some(next) => cursor = next,
                    None => return Ok(None),
                }
                fresh = true;
                continue;
            }
            // Backfill reads in chunks on a fixed grid from the round's lower
            // bound, so consecutive records hit the same cached page; a tail
            // round is one window.
            let until = if cursor.backfill {
                let chunk = (cursor.watermark - cursor.lower) / self.config.chunk_ms + 1;
                cursor
                    .lower
                    .saturating_add(chunk.saturating_mul(self.config.chunk_ms))
                    .min(cursor.upper)
            } else {
                cursor.upper
            };
            match self.first_after(cursor.tag, cursor.watermark, until, cursor.backfill)? {
                Some(sample) => {
                    let tag = self.tags[cursor.tag].name.clone();
                    cursor.watermark = sample.ts_ms;
                    let payload = serde_json::json!({
                        "tag": tag,
                        "ts_ms": sample.ts_ms,
                        "value": json_value(sample.value),
                    })
                    .to_string();
                    return Ok(Some(Record {
                        source: Some(SourceMetadata {
                            cursor: Some(
                                serde_json::to_string(&cursor)
                                    .map_err(|error| Error::new(error.to_string()))?,
                            ),
                            change: None,
                        }),
                        kind: SAMPLE_KIND.into(),
                        key: format!("{tag}@{}", sample.ts_ms),
                        source_ts_ms: sample.ts_ms,
                        ingest_ts_ms: 0,
                        quality: sample.quality,
                        position: from.map_or(1, |position| position + 1),
                        payload,
                    }));
                }
                None if until < cursor.upper => cursor.watermark = until,
                None => {
                    cursor.tag += 1;
                    cursor.watermark = cursor.lower;
                }
            }
        }
    }
}

/// A finite value as a JSON number; NaN and the infinities, which JSON has
/// no number for, as the strings "NaN", "Infinity" and "-Infinity".
fn json_value(value: f64) -> serde_json::Value {
    if value.is_nan() {
        "NaN".into()
    } else if value.is_infinite() {
        if value > 0.0 { "Infinity" } else { "-Infinity" }.into()
    } else {
        value.into()
    }
}

fn write_round(path: &Path, round: &Round) -> Result<(), Error> {
    let body = serde_json::to_string(round).map_err(|error| Error::new(error.to_string()))?;
    atomic_write(path, &body)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static DIR_SEQ: AtomicU64 = AtomicU64::new(0);

    fn state_dir() -> PathBuf {
        std::env::temp_dir().join(format!(
            "hashi-historian-{}-{}",
            std::process::id(),
            DIR_SEQ.fetch_add(1, Ordering::Relaxed)
        ))
    }
    use std::cell::Cell;
    use std::rc::Rc;

    struct Clockwork {
        now: Rc<Cell<i64>>,
        slept: Rc<Cell<u64>>,
    }

    impl Clock for Clockwork {
        fn now_ms(&self) -> i64 {
            self.now.get()
        }

        fn sleep_ms(&self, ms: u64) {
            self.slept.set(self.slept.get() + ms);
            self.now.set(self.now.get() + ms as i64);
        }
    }

    struct Steady;

    impl Historian for Steady {
        fn tags(&mut self) -> Result<Vec<Tag>, Error> {
            Ok(vec![Tag { name: "t".into() }])
        }

        fn read_raw(
            &mut self,
            _tag: &str,
            after_ms: i64,
            until_ms: i64,
            limit: usize,
        ) -> Result<Vec<Sample>, Error> {
            Ok((after_ms + 1..=until_ms)
                .filter(|ts| ts % 10 == 0)
                .take(limit)
                .map(|ts_ms| Sample {
                    ts_ms,
                    value: ts_ms as f64,
                    quality: None,
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
                    value: ts_ms as f64,
                    quality: None,
                })
                .collect())
        }
    }

    #[test]
    fn reads_are_paced_to_the_configured_rate() {
        let now = Rc::new(Cell::new(10_000));
        let slept = Rc::new(Cell::new(0));
        let clock = Clockwork {
            now: now.clone(),
            slept: slept.clone(),
        };
        let mut connector = HistorianConnector::new(
            "steady",
            Steady,
            clock,
            HistorianConfig {
                start_ms: 0,
                chunk_ms: 100,
                page_limit: 4,
                settle_ms: 0,
                max_reads_per_second: 10,
                state_dir: state_dir(),
            },
        )
        .unwrap();
        let mut from = None;
        let mut cursor = None;
        for _ in 0..20 {
            let record = connector.read(from, cursor.as_deref()).unwrap().unwrap();
            from = Some(record.position);
            cursor = record.source.unwrap().cursor;
        }
        // Twenty samples at four per page are at least five reads, 100 ms apart.
        assert!(slept.get() >= 400, "slept {}", slept.get());
    }

    #[test]
    fn a_cursor_from_another_configuration_or_a_position_alone_is_refused() {
        let clock = Clockwork {
            now: Rc::new(Cell::new(1_000)),
            slept: Rc::new(Cell::new(0)),
        };
        let config = HistorianConfig {
            start_ms: 500,
            chunk_ms: 100,
            page_limit: 4,
            settle_ms: 0,
            max_reads_per_second: 0,
            state_dir: state_dir(),
        };
        let mut connector = HistorianConnector::new("steady", Steady, clock, config).unwrap();
        let foreign =
            r#"{"v":1,"backfill":true,"lower":0,"upper":900,"tag":0,"watermark":0,"tags":"x"}"#;
        assert!(connector.read(Some(3), Some(foreign)).is_err());
        assert!(connector.read(Some(3), None).is_err());
        let interpolated = Steady.read_interpolated("t", 0, 30, 10).unwrap();
        assert_eq!(interpolated.len(), 4);
    }

    /// Tags in a configurable order, one sample every 10 ms each.
    struct Named(Vec<&'static str>);

    impl Historian for Named {
        fn tags(&mut self) -> Result<Vec<Tag>, Error> {
            Ok(self
                .0
                .iter()
                .map(|name| Tag {
                    name: (*name).into(),
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
            Steady.read_raw(tag, after_ms, until_ms, limit)
        }

        fn read_interpolated(
            &mut self,
            tag: &str,
            start_ms: i64,
            end_ms: i64,
            step_ms: i64,
        ) -> Result<Vec<Sample>, Error> {
            Steady.read_interpolated(tag, start_ms, end_ms, step_ms)
        }
    }

    fn named(
        dir: &Path,
        tags: Vec<&'static str>,
        now: i64,
    ) -> HistorianConnector<Named, Clockwork> {
        let clock = Clockwork {
            now: Rc::new(Cell::new(now)),
            slept: Rc::new(Cell::new(0)),
        };
        let config = HistorianConfig {
            start_ms: 0,
            chunk_ms: 100,
            page_limit: 8,
            settle_ms: 0,
            max_reads_per_second: 0,
            state_dir: dir.to_path_buf(),
        };
        HistorianConnector::new("named", Named(tags), clock, config).unwrap()
    }

    /// Read until caught up, returning `(key, position)` and the last cursor.
    fn drain(
        connector: &mut HistorianConnector<Named, Clockwork>,
        mut from: Option<Position>,
        mut cursor: Option<String>,
    ) -> (Vec<(String, Position)>, Option<Position>, Option<String>) {
        let mut seen = Vec::new();
        while let Some(record) = connector.read(from, cursor.as_deref()).unwrap() {
            seen.push((record.key.clone(), record.position));
            from = Some(record.position);
            cursor = record.source.unwrap().cursor;
        }
        (seen, from, cursor)
    }

    #[test]
    fn cursors_follow_tag_names_not_discovery_order() {
        let dir = state_dir();
        let (_, from, cursor) = drain(&mut named(&dir, vec!["a", "b"], 300), None, None);
        // The same tags discovered in another order resume the same cursor.
        let (more, _, _) = drain(&mut named(&dir, vec!["b", "a"], 500), from, cursor.clone());
        assert_eq!(more.len(), 40, "two hundred ms of samples for two tags");
        // A different tag set refuses it.
        let mut changed = named(&dir, vec!["a", "c"], 500);
        assert!(changed.read(from, cursor.as_deref()).is_err());
        drop(changed);
        // So does a different backfill start.
        let clock = Clockwork {
            now: Rc::new(Cell::new(500)),
            slept: Rc::new(Cell::new(0)),
        };
        let mut earlier = HistorianConnector::new(
            "named",
            Named(vec!["a", "b"]),
            clock,
            HistorianConfig {
                start_ms: -1_000,
                chunk_ms: 100,
                page_limit: 8,
                settle_ms: 0,
                max_reads_per_second: 0,
                state_dir: dir.clone(),
            },
        )
        .unwrap();
        assert!(earlier.read(from, cursor.as_deref()).is_err());
    }

    #[test]
    fn a_replayed_round_keeps_its_records_whatever_the_clock() {
        let dir = state_dir();
        let (_, from, cursor) = drain(&mut named(&dir, vec!["a", "b"], 300), None, None);
        // Nothing new at the same horizon, and a read always ends.
        assert!(
            named(&dir, vec!["a", "b"], 300)
                .read(from, cursor.as_deref())
                .unwrap()
                .is_none()
        );
        // The first tail record is delivered, then the process dies before
        // its checkpoint; a restart much later re-emits the same record.
        let early = named(&dir, vec!["a", "b"], 400)
            .read(from, cursor.as_deref())
            .unwrap()
            .unwrap();
        let late = named(&dir, vec!["a", "b"], 950)
            .read(from, cursor.as_deref())
            .unwrap()
            .unwrap();
        assert_eq!(early, late);
        // A second owner of the state directory is refused.
        let _owner = named(&dir, vec!["a", "b"], 950);
        let clock = Clockwork {
            now: Rc::new(Cell::new(950)),
            slept: Rc::new(Cell::new(0)),
        };
        let config = HistorianConfig {
            start_ms: 0,
            chunk_ms: 100,
            page_limit: 8,
            settle_ms: 0,
            max_reads_per_second: 0,
            state_dir: dir.clone(),
        };
        assert!(HistorianConnector::new("named", Named(vec!["a"]), clock, config).is_err());
    }

    #[test]
    fn an_empty_tail_round_is_widened_so_a_crash_replays_in_order() {
        /// Tag `a` writes at 500 only; tag `b` at 300 only.
        struct Sparse;
        impl Historian for Sparse {
            fn tags(&mut self) -> Result<Vec<Tag>, Error> {
                Ok(vec![Tag { name: "a".into() }, Tag { name: "b".into() }])
            }
            fn read_raw(
                &mut self,
                tag: &str,
                after_ms: i64,
                until_ms: i64,
                _limit: usize,
            ) -> Result<Vec<Sample>, Error> {
                let at = if tag == "a" { 500 } else { 300 };
                Ok(if after_ms < at && at <= until_ms {
                    vec![Sample {
                        ts_ms: at,
                        value: 1.0,
                        quality: None,
                    }]
                } else {
                    Vec::new()
                })
            }
            fn read_interpolated(
                &mut self,
                _: &str,
                _: i64,
                _: i64,
                _: i64,
            ) -> Result<Vec<Sample>, Error> {
                Ok(Vec::new())
            }
        }
        let dir = state_dir();
        let open = |now: i64| {
            let clock = Clockwork {
                now: Rc::new(Cell::new(now)),
                slept: Rc::new(Cell::new(0)),
            };
            HistorianConnector::new(
                "sparse",
                Sparse,
                clock,
                HistorianConfig {
                    start_ms: 0,
                    chunk_ms: 1_000,
                    page_limit: 8,
                    settle_ms: 0,
                    max_reads_per_second: 0,
                    state_dir: dir.clone(),
                },
            )
            .unwrap()
        };
        // Backfill to 100 holds nothing; idle polls at 200 and 400, then b@300
        // is delivered and the process dies before its checkpoint.
        assert!(open(100).read(None, None).unwrap().is_none());
        assert!(open(200).read(None, None).unwrap().is_none());
        let delivered = open(400).read(None, None).unwrap().unwrap();
        assert_eq!(delivered.key, "b@300");
        // Restarting after a@500 exists re-emits b@300 first, at its position.
        let replayed = open(600).read(None, None).unwrap().unwrap();
        assert_eq!(replayed, delivered);
    }

    #[test]
    fn non_finite_values_keep_an_explicit_representation() {
        assert_eq!(json_value(1.5), serde_json::json!(1.5));
        assert_eq!(json_value(f64::NAN), serde_json::json!("NaN"));
        assert_eq!(json_value(f64::INFINITY), serde_json::json!("Infinity"));
        assert_eq!(
            json_value(f64::NEG_INFINITY),
            serde_json::json!("-Infinity")
        );
    }
}
