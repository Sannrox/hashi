//! SQL Server mapping historian (#68): catalog check, backfill, and live tail.
#![cfg(feature = "mssql")]

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use hashi::checkpoint::{Checkpoint, CheckpointStore, QuarantineRow};
use hashi::historian::{Historian, HistorianConfig, HistorianConnector};
use hashi::mssql::historian::{Config, Mapping, PartitionCatalog, SqlHistorian};
use hashi::runtime::Clock;
use hashi::{Connector, Error, InstanceLease, MemorySink, Position, Runtime, RuntimeConfig};
use tiberius::{AuthMethod, Client, Config as SqlConfig};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

const START_MS: i64 = 1_700_000_000_000;

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "hashi-mssql-historian-{name}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ))
}

fn fixture_password() -> String {
    std::env::var("HASHI_MSSQL_SA_PASSWORD").expect("HASHI_MSSQL_SA_PASSWORD")
}

fn fixture_port() -> u16 {
    assert_eq!(std::env::var("HASHI_MSSQL_DISPOSABLE").as_deref(), Ok("1"));
    std::env::var("HASHI_MSSQL_TEST_PORT")
        .unwrap()
        .parse::<u16>()
        .unwrap()
}

fn mapping() -> Mapping {
    Mapping {
        database: "dbo".into(),
        tag_table: "tags".into(),
        tag_id_column: "id".into(),
        tag_name_column: "name".into(),
        value_table: "samples".into(),
        partition_catalog: None,
        value_tag_column: "tag_id".into(),
        time_column: "ts".into(),
        value_column: "value".into(),
        quality_column: "quality".into(),
    }
}

fn config(database: &str) -> Config {
    Config {
        host: "127.0.0.1".into(),
        port: fixture_port(),
        user: "sa".into(),
        password: fixture_password(),
        database: database.into(),
        mapping: mapping(),
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

async fn connect(
    port: u16,
    database: Option<&str>,
) -> Client<tokio_util::compat::Compat<TcpStream>> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mut cfg = SqlConfig::new();
    cfg.host("127.0.0.1");
    cfg.port(port);
    if let Some(database) = database {
        cfg.database(database);
    }
    cfg.authentication(AuthMethod::sql_server("sa", fixture_password()));
    cfg.trust_cert();
    let tcp = TcpStream::connect(cfg.get_addr()).await.unwrap();
    tcp.set_nodelay(true).unwrap();
    Client::connect(cfg, tcp.compat_write()).await.unwrap()
}

async fn exec(client: &mut Client<tokio_util::compat::Compat<TcpStream>>, sql: &str) {
    client
        .simple_query(sql)
        .await
        .unwrap()
        .into_first_result()
        .await
        .unwrap();
}

fn seed(database: &str, extra: &str) {
    let port = fixture_port();
    runtime_block(async {
        let mut client = connect(port, None).await;
        exec(&mut client, &format!("CREATE DATABASE [{database}]")).await;
        drop(client);
        let mut client = connect(port, Some(database)).await;
        exec(
            &mut client,
            "CREATE TABLE dbo.tags (id INT NOT NULL PRIMARY KEY, name NVARCHAR(64) NOT NULL UNIQUE)",
        )
        .await;
        exec(
            &mut client,
            "CREATE TABLE dbo.samples (tag_id INT NOT NULL, tag_ref NVARCHAR(16) NOT NULL DEFAULT N'1', ts DATETIME2(3) NOT NULL, value FLOAT NOT NULL, quality NVARCHAR(32) NULL, PRIMARY KEY (tag_id, ts))",
        )
        .await;
        exec(
            &mut client,
            "INSERT INTO dbo.tags (id, name) VALUES (1, N'line.temp'), (2, N'line.pressure')",
        )
        .await;
        exec(
            &mut client,
            "INSERT INTO dbo.samples (tag_id, ts, value, quality) VALUES (1, '2023-11-14 22:13:21.000', 1.0, N'good'), (1, '2023-11-14 22:13:22.000', 2.0, N'good'), (1, '2023-11-14 22:13:23.000', 3.0, N'good'), (2, '2023-11-14 22:13:21.000', 10.0, N'good'), (2, '2023-11-14 22:13:22.000', 20.0, N'good')",
        )
        .await;
        if !extra.is_empty() {
            exec(&mut client, &extra.replace("{db}", database)).await;
        }
    });
}

fn insert_tail(database: &str) {
    let port = fixture_port();
    runtime_block(async {
        let mut client = connect(port, Some(database)).await;
        exec(
            &mut client,
            "INSERT INTO dbo.samples (tag_id, ts, value, quality) VALUES (1, '2023-11-14 22:13:41.000', 21.0, N'good'), (1, '2023-11-14 22:13:42.000', 22.0, N'good')",
        )
        .await;
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

/// Each delivered sample, by (tag, offset from START_MS), as (value, quality)
/// per delivery, so a duplicate shows up as a second entry.
type Delivered = BTreeMap<(String, i64), Vec<(f64, Option<String>)>>;

fn delivered(sinks: &[MemorySink]) -> Delivered {
    let mut seen = Delivered::new();
    for sink in sinks {
        for (record, _) in &sink.delivered {
            let (tag, ts) = record.key.split_once('@').unwrap();
            let payload: serde_json::Value = serde_json::from_str(&record.payload).unwrap();
            seen.entry((tag.to_string(), ts.parse::<i64>().unwrap() - START_MS))
                .or_default()
                .push((payload["value"].as_f64().unwrap(), record.quality.clone()));
        }
    }
    seen
}

/// The expected map for samples delivered once, all with quality `good`.
fn good(samples: &[(&str, i64, f64)]) -> Delivered {
    samples
        .iter()
        .map(|&(tag, ts, value)| ((tag.into(), ts), vec![(value, Some("good".into()))]))
        .collect()
}

#[test]
#[ignore = "requires the isolated database created by scripts/test-mssql-container.sh"]
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
    assert_eq!(
        delivered(&[first_sink]),
        good(&[
            ("line.pressure", 1_000, 10.0),
            ("line.pressure", 2_000, 20.0),
            ("line.temp", 1_000, 1.0),
            ("line.temp", 2_000, 2.0),
            ("line.temp", 3_000, 3.0),
        ])
    );

    insert_tail(&database);
    now.set(START_MS + 30_000);
    let historian = SqlHistorian::connect(config(&database)).unwrap();
    let mut tail = runtime(connector(historian, &now, &path), &now, &store);
    assert_eq!(tail.run().unwrap().delivered, 2);
    let (_, tail_sink, _) = tail.into_parts();
    assert_eq!(
        delivered(&[tail_sink]),
        good(&[("line.temp", 21_000, 21.0), ("line.temp", 22_000, 22.0)])
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
#[ignore = "requires the isolated database created by scripts/test-mssql-container.sh"]
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
        let mut client = connect(fixture_port(), Some(&database)).await;
        exec(
            &mut client,
            "ALTER TABLE dbo.samples ADD stamp NVARCHAR(32) NOT NULL DEFAULT N'x'",
        )
        .await;
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

fn partitioned_mapping() -> Mapping {
    let mut mapping = mapping();
    mapping.value_table.clear();
    mapping.partition_catalog = Some(PartitionCatalog {
        table: "partitions".into(),
        value_table_column: "value_table".into(),
        start_column: "start_ts".into(),
        end_column: "end_ts".into(),
    });
    mapping
}

fn partitioned_config(database: &str) -> Config {
    let mut cfg = config(database);
    cfg.mapping = partitioned_mapping();
    cfg
}

fn seed_partitioned(database: &str) {
    seed(database, "");
    for sql in [
        "CREATE TABLE dbo.samples_p0 (tag_id INT NOT NULL, ts DATETIME2(3) NOT NULL, value FLOAT NOT NULL, quality NVARCHAR(32) NULL, PRIMARY KEY (tag_id, ts))",
        "CREATE TABLE dbo.samples_p1 (tag_id INT NOT NULL, ts DATETIME2(3) NOT NULL, value FLOAT NOT NULL, quality NVARCHAR(32) NULL, PRIMARY KEY (tag_id, ts))",
        "CREATE TABLE dbo.partitions (value_table NVARCHAR(64) NOT NULL, start_ts DATETIME2(3) NOT NULL, end_ts DATETIME2(3) NOT NULL)",
        "INSERT INTO dbo.partitions (value_table, start_ts, end_ts) VALUES (N'samples_p0', '2023-11-14 22:13:20.000', '2023-11-14 22:13:22.000'), (N'samples_p1', '2023-11-14 22:13:22.000', '2023-11-14 22:13:24.000')",
        "INSERT INTO dbo.samples_p0 (tag_id, ts, value, quality) VALUES (1, '2023-11-14 22:13:21.000', 1.0, N'good'), (2, '2023-11-14 22:13:21.000', 10.0, N'good')",
        "INSERT INTO dbo.samples_p1 (tag_id, ts, value, quality) VALUES (1, '2023-11-14 22:13:22.000', 2.0, N'good'), (1, '2023-11-14 22:13:23.000', 3.0, N'good'), (2, '2023-11-14 22:13:22.000', 20.0, N'good')",
    ] {
        exec_sql(database, sql);
    }
}

fn exec_sql(database: &str, sql: &str) {
    let port = fixture_port();
    runtime_block(async {
        let mut client = connect(port, Some(database)).await;
        exec(&mut client, sql).await;
    });
}

fn drain(
    source: &mut Source,
    mut from: Option<Position>,
    mut cursor: Option<String>,
    max: usize,
) -> (Vec<String>, Option<Position>, Option<String>) {
    let mut keys = Vec::new();
    for _ in 0..max {
        match source.read(from, cursor.as_deref()).unwrap() {
            None => break,
            Some(record) => {
                from = Some(record.position);
                cursor = record.source.and_then(|meta| meta.cursor);
                keys.push(record.key);
            }
        }
    }
    (keys, from, cursor)
}

#[test]
#[ignore = "requires the isolated database created by scripts/test-mssql-container.sh"]
fn sql_historian_backfills_across_two_partitions() {
    let database = database("parts");
    seed_partitioned(&database);
    let mut historian = SqlHistorian::connect(partitioned_config(&database)).unwrap();
    assert!(historian.check().unwrap().ok);
    let page = historian
        .read_raw("line.temp", START_MS, START_MS + 3_000, 2)
        .unwrap();
    assert_eq!(
        page.iter().map(|sample| sample.ts_ms).collect::<Vec<_>>(),
        vec![START_MS + 1_000, START_MS + 2_000]
    );
    let rest = historian
        .read_raw("line.temp", page[1].ts_ms, START_MS + 3_000, 8)
        .unwrap();
    assert_eq!(
        rest.iter().map(|sample| sample.ts_ms).collect::<Vec<_>>(),
        vec![START_MS + 3_000]
    );

    let path = scratch("parts-state");
    std::fs::create_dir_all(&path).unwrap();
    let now = Rc::new(Cell::new(START_MS + 3_500));
    let mut first = HistorianConnector::new(
        "historian",
        SqlHistorian::connect(partitioned_config(&database)).unwrap(),
        FixtureClock(now.clone()),
        HistorianConfig {
            start_ms: START_MS,
            chunk_ms: 4_000,
            page_limit: 1,
            settle_ms: 0,
            max_reads_per_second: 0,
            state_dir: path.clone(),
        },
    )
    .unwrap();
    let (head, from, cursor) = drain(&mut first, None, None, 2);
    assert_eq!(
        head,
        vec![
            format!("line.pressure@{}", START_MS + 1_000),
            format!("line.pressure@{}", START_MS + 2_000)
        ]
    );
    drop(first);
    let mut resumed = HistorianConnector::new(
        "historian",
        SqlHistorian::connect(partitioned_config(&database)).unwrap(),
        FixtureClock(now),
        HistorianConfig {
            start_ms: START_MS,
            chunk_ms: 4_000,
            page_limit: 1,
            settle_ms: 0,
            max_reads_per_second: 0,
            state_dir: path,
        },
    )
    .unwrap();
    let (tail, _, _) = drain(&mut resumed, from, cursor, 8);
    assert_eq!(
        tail,
        vec![
            format!("line.temp@{}", START_MS + 1_000),
            format!("line.temp@{}", START_MS + 2_000),
            format!("line.temp@{}", START_MS + 3_000)
        ]
    );
}

#[test]
#[ignore = "requires the isolated database created by scripts/test-mssql-container.sh"]
fn sql_historian_refuses_uncovered_and_vanished_partitions() {
    let holes = database("holes");
    seed_partitioned(&holes);
    let mut historian = SqlHistorian::connect(partitioned_config(&holes)).unwrap();
    let error = historian
        .read_raw("line.temp", START_MS + 5_000, START_MS + 6_000, 8)
        .unwrap_err();
    assert!(
        error
            .message
            .contains("does not cover the requested window"),
        "{}",
        error.message
    );

    let first = historian
        .read_raw("line.temp", START_MS, START_MS + 3_000, 8)
        .unwrap();
    assert_eq!(first.len(), 3);
    exec_sql(
        &holes,
        "DELETE FROM dbo.partitions WHERE value_table=N'samples_p0'",
    );
    let error = historian
        .read_raw("line.temp", START_MS, START_MS + 3_000, 8)
        .unwrap_err();
    assert!(
        error
            .message
            .contains("does not cover the requested window")
            || error.message.contains("changed during the read"),
        "{}",
        error.message
    );

    let empty = database("empty");
    seed_partitioned(&empty);
    exec_sql(&empty, "DELETE FROM dbo.partitions");
    let error = refused(partitioned_config(&empty));
    assert!(
        error.message.contains("partition catalog is empty"),
        "{}",
        error.message
    );

    let missing = database("missing");
    seed_partitioned(&missing);
    exec_sql(
        &missing,
        "UPDATE dbo.partitions SET value_table=N'gone' WHERE value_table=N'samples_p0'",
    );
    let error = refused(partitioned_config(&missing));
    assert!(
        error.message.contains("table gone is missing"),
        "{}",
        error.message
    );
}
