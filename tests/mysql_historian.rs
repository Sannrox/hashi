//! MySQL mapping historian (#24): catalog check, backfill, and live tail.
#![cfg(feature = "mysql")]

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::fs::File;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use hashi::checkpoint::{Checkpoint, CheckpointStore, QuarantineRow};
use hashi::historian::{Historian, HistorianConfig, HistorianConnector};
use hashi::mysql::historian::{Config, Mapping, SqlHistorian};
use hashi::runtime::Clock;
use hashi::{Connector, Error, MemorySink, Runtime, RuntimeConfig};
use mysql_async::{Conn, Opts, OptsBuilder, prelude::Queryable};

const START_MS: i64 = 1_700_000_000_000;

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "hashi-mysql-historian-{name}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ))
}

fn opts() -> Opts {
    assert_eq!(std::env::var("HASHI_MYSQL_DISPOSABLE").as_deref(), Ok("1"));
    let port = std::env::var("HASHI_MYSQL_TEST_PORT")
        .unwrap()
        .parse::<u16>()
        .unwrap();
    OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .tcp_port(port)
        .user(Some("root"))
        .pass(Some("hashi-fixture-only"))
        .into()
}

fn mapping(database: &str) -> Mapping {
    Mapping {
        database: database.into(),
        tag_table: "tags".into(),
        tag_id_column: "id".into(),
        tag_name_column: "name".into(),
        value_table: "samples".into(),
        value_tag_column: "tag_id".into(),
        time_column: "ts".into(),
        value_column: "value".into(),
        quality_column: "quality".into(),
    }
}

fn config(database: &str) -> Config {
    Config {
        options: opts(),
        mapping: mapping(database),
        timeout: Duration::from_secs(20),
        allow_insecure_lab: true,
    }
}

fn database(label: &str) -> String {
    format!(
        "hashi_hist_{}_{}_{}",
        label,
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    )
}

fn runtime_block<T>(task: impl std::future::Future<Output = T>) -> T {
    tokio::runtime::Runtime::new().unwrap().block_on(task)
}

fn seed(database: &str, extra: &str) {
    runtime_block(async {
        let mut conn = Conn::new(opts()).await.unwrap();
        conn.query_drop("SET SESSION time_zone='+00:00'")
            .await
            .unwrap();
        conn.query_drop(format!("CREATE DATABASE `{database}`"))
            .await
            .unwrap();
        conn.query_drop(format!(
            "CREATE TABLE `{database}`.tags (id INT PRIMARY KEY, name VARCHAR(64) NOT NULL UNIQUE) ENGINE=InnoDB"
        ))
        .await
        .unwrap();
        conn.query_drop(format!(
            "CREATE TABLE `{database}`.samples (tag_id INT NOT NULL, tag_ref VARCHAR(16) NOT NULL DEFAULT '1', ts TIMESTAMP(6) NOT NULL, value DOUBLE NOT NULL, quality VARCHAR(32) NULL, PRIMARY KEY (tag_id, ts)) ENGINE=InnoDB"
        ))
        .await
        .unwrap();
        conn.query_drop(format!(
            "INSERT INTO `{database}`.tags (id, name) VALUES (1, 'line.temp'), (2, 'line.pressure')"
        ))
        .await
        .unwrap();
        conn.query_drop(format!(
            "INSERT INTO `{database}`.samples (tag_id, ts, value, quality) VALUES (1, FROM_UNIXTIME(1700000001), 1.0, 'good'), (1, FROM_UNIXTIME(1700000002), 2.0, 'good'), (1, FROM_UNIXTIME(1700000003), 3.0, 'good'), (2, FROM_UNIXTIME(1700000001), 10.0, 'good'), (2, FROM_UNIXTIME(1700000002), 20.0, 'good')"
        ))
        .await
        .unwrap();
        if !extra.is_empty() {
            conn.query_drop(extra.replace("{db}", database))
                .await
                .unwrap();
        }
        conn.disconnect().await.unwrap();
    });
}

fn insert_tail(database: &str) {
    runtime_block(async {
        let mut conn = Conn::new(opts()).await.unwrap();
        conn.query_drop("SET SESSION time_zone='+00:00'")
            .await
            .unwrap();
        conn.query_drop(format!(
            "INSERT INTO `{database}`.samples (tag_id, ts, value, quality) VALUES (1, FROM_UNIXTIME(1700000021.0005), 21.0, 'good'), (1, FROM_UNIXTIME(1700000022), 22.0, 'good')"
        ))
        .await
        .unwrap();
        conn.disconnect().await.unwrap();
    });
}

#[derive(Clone)]
struct FixtureClock(Rc<Cell<i64>>);

impl Clock for FixtureClock {
    fn now_ms(&self) -> i64 {
        self.0.get()
    }

    fn sleep_ms(&self, _ms: u64) {}
}

#[derive(Clone, Default)]
struct MemoryStore(Rc<RefCell<Option<Checkpoint>>>);

impl CheckpointStore for MemoryStore {
    fn acquire(&self, _connector: &str, _instance: &str) -> Result<File, Error> {
        Ok(File::create(scratch("lease"))?)
    }

    fn load_schemas(&self, _: &str, _: &str) -> Result<hashi::schema::SchemaState, Error> {
        Ok(Default::default())
    }

    fn save_schemas(&self, _: &str, _: &str, _: &hashi::schema::SchemaState) -> Result<(), Error> {
        Ok(())
    }

    fn load(&self, _: &str, _: &str) -> Result<Option<Checkpoint>, Error> {
        Ok(self.0.borrow().clone())
    }

    fn commit(&self, checkpoint: &Checkpoint) -> Result<(), Error> {
        *self.0.borrow_mut() = Some(checkpoint.clone());
        Ok(())
    }

    fn quarantine(&self, _: &QuarantineRow) -> Result<(), Error> {
        Err(Error::new("no quarantine expected"))
    }

    fn quarantined(&self, _: &str, _: &str) -> Result<Vec<QuarantineRow>, Error> {
        Ok(Vec::new())
    }
}

type Source = HistorianConnector<SqlHistorian, FixtureClock>;

fn connector(historian: SqlHistorian, now: &Rc<Cell<i64>>, state: &std::path::Path) -> Source {
    HistorianConnector::new(
        "historian",
        historian,
        FixtureClock(now.clone()),
        HistorianConfig {
            start_ms: START_MS,
            chunk_ms: 4_000,
            page_limit: 2,
            settle_ms: 0,
            max_reads_per_second: 0,
            state_dir: state.into(),
        },
    )
    .unwrap()
}

fn runtime(
    source: Source,
    now: &Rc<Cell<i64>>,
    store: &MemoryStore,
) -> Runtime<Source, MemorySink, MemoryStore, FixtureClock> {
    Runtime::new(
        source,
        MemorySink::new(),
        store.clone(),
        FixtureClock(now.clone()),
        RuntimeConfig::new("plant"),
    )
}

fn delivered(sinks: &[MemorySink]) -> BTreeMap<(String, i64), usize> {
    let mut seen = BTreeMap::new();
    for sink in sinks {
        for (record, _) in &sink.delivered {
            let (tag, ts) = record.key.split_once('@').unwrap();
            *seen
                .entry((tag.to_string(), ts.parse::<i64>().unwrap() - START_MS))
                .or_insert(0) += 1;
        }
    }
    seen
}

#[test]
#[ignore = "requires the isolated database created by scripts/test-mysql-container.sh"]
fn sql_historian_backfills_and_tails_through_a_mapping() {
    let database = database("ok");
    seed(&database, "");
    let path = scratch("state");
    std::fs::create_dir_all(&path).unwrap();
    let now = Rc::new(Cell::new(START_MS + 20_000));
    let store = MemoryStore::default();
    let historian = SqlHistorian::connect(config(&database)).unwrap();
    assert!(historian.check().unwrap().ok);
    let source = connector(historian, &now, &path);
    assert!(source.check().unwrap().ok);
    let mut first = runtime(source, &now, &store);
    let report = first.run().unwrap();
    assert_eq!(report.delivered, 5);
    let (_, first_sink, _) = first.into_parts();
    let backfilled = delivered(&[first_sink]);
    assert!(backfilled.values().all(|count| *count == 1), "overlap");
    assert_eq!(
        backfilled.keys().cloned().collect::<Vec<_>>(),
        vec![
            ("line.pressure".into(), 1_000),
            ("line.pressure".into(), 2_000),
            ("line.temp".into(), 1_000),
            ("line.temp".into(), 2_000),
            ("line.temp".into(), 3_000),
        ]
    );

    insert_tail(&database);
    now.set(START_MS + 30_000);
    let historian = SqlHistorian::connect(config(&database)).unwrap();
    let mut tail = runtime(connector(historian, &now, &path), &now, &store);
    assert_eq!(tail.run().unwrap().delivered, 2);
    let (_, tail_sink, _) = tail.into_parts();
    assert_eq!(
        delivered(&[tail_sink]),
        BTreeMap::from([
            (("line.temp".into(), 21_000), 1),
            (("line.temp".into(), 22_000), 1),
        ])
    );

    let mut historian = SqlHistorian::connect(config(&database)).unwrap();
    assert!(
        historian
            .read_interpolated("line.temp", START_MS, START_MS + 3_000, 1_000)
            .is_err()
    );
}

fn refused(config: Config) -> Error {
    match SqlHistorian::connect(config) {
        Ok(_) => panic!("expected mapping to be refused"),
        Err(error) => error,
    }
}

#[test]
#[ignore = "requires the isolated database created by scripts/test-mysql-container.sh"]
fn a_wrong_mapping_is_refused_at_check_time() {
    let database = database("bad");
    seed(&database, "");

    let mut missing_table = config(&database);
    missing_table.mapping.tag_table = "missing".into();
    let error = refused(missing_table);
    assert!(
        error.message.contains("table missing is missing"),
        "{}",
        error.message
    );

    let mut missing_column = config(&database);
    missing_column.mapping.time_column = "when_ms".into();
    let error = refused(missing_column);
    assert!(
        error.message.contains("column samples.when_ms is missing"),
        "{}",
        error.message
    );

    runtime_block(async {
        let mut conn = Conn::new(opts()).await.unwrap();
        conn.query_drop(format!(
            "ALTER TABLE `{database}`.samples ADD COLUMN stamp VARCHAR(32) NOT NULL DEFAULT 'x'"
        ))
        .await
        .unwrap();
        conn.disconnect().await.unwrap();
    });
    let mut mismatched = config(&database);
    mismatched.mapping.value_tag_column = "tag_ref".into();
    let error = refused(mismatched);
    assert!(
        error
            .message
            .contains("tag identity columns are not comparable"),
        "{}",
        error.message
    );

    let mut wrong_type = config(&database);
    wrong_type.mapping.time_column = "stamp".into();
    let error = refused(wrong_type);
    assert!(
        error
            .message
            .contains("column samples.stamp has the wrong type"),
        "{}",
        error.message
    );
}
