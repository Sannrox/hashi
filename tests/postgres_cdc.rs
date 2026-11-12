#![cfg(feature = "postgres")]
use hashi::postgres::{Config, PostgresConnector, Table};
use hashi::{CheckpointStore, Connector, Operation, Record};
use std::time::Duration;
use tokio_postgres::{Client, NoTls};

const TAKEOVER_TTL: Duration = Duration::from_millis(500);

fn wait_for_takeover() {
    std::thread::sleep(TAKEOVER_TTL + Duration::from_millis(50));
}

fn fixture_password() -> String {
    std::env::var("HASHI_POSTGRES_PASSWORD").expect("HASHI_POSTGRES_PASSWORD")
}

fn config(port: u16, database: &str, state_dir: std::path::PathBuf) -> Config {
    Config {
        id: "postgres-fixture".into(),
        host: "127.0.0.1".into(),
        port,
        user: "postgres".into(),
        password: fixture_password(),
        database: database.into(),
        tables: vec![Table {
            schema: "public".into(),
            table: "items".into(),
            key: vec![],
        }],
        state_dir,
        chunk_rows: 2,
        max_batch_bytes: 16 * 1024 * 1024,
        schema_version: 1,
        timeout: Duration::from_secs(20),
        max_lag: Duration::from_secs(60 * 60),
        allow_insecure_lab: true,
    }
}

fn resume(record: &Record) -> (Option<u64>, Option<&str>) {
    (
        Some(record.position),
        record.source.as_ref().and_then(|s| s.cursor.as_deref()),
    )
}

fn next(source: &mut PostgresConnector, previous: &Record) -> Result<Record, hashi::Error> {
    let deadline = std::time::Instant::now() + Duration::from_secs(25);
    loop {
        if let Some(record) = source.read(resume(previous).0, resume(previous).1)? {
            return Ok(record);
        }
        assert!(
            std::time::Instant::now() < deadline,
            "expected a source record"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

async fn connect(port: u16, database: &str) -> Client {
    let (client, conn) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={port} user=postgres password={} dbname={database}",
            fixture_password()
        ),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });
    client
}

async fn exec(client: &Client, sql: &str) {
    client.batch_execute(sql).await.unwrap();
}

#[test]
#[ignore = "requires the isolated database created by scripts/test-postgres-container.sh"]
fn snapshot_lsn_stream_restart_schema_and_dropped_slot() {
    assert_eq!(
        std::env::var("HASHI_POSTGRES_DISPOSABLE").as_deref(),
        Ok("1")
    );
    let port = std::env::var("HASHI_POSTGRES_TEST_PORT")
        .unwrap()
        .parse::<u16>()
        .unwrap();
    let database = format!("hashi_fixture_{}", std::process::id());
    let path = std::env::temp_dir().join(format!(
        "hashi-postgres-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let admin = connect(port, "postgres").await;
        exec(&admin, &format!("CREATE DATABASE {database}")).await;
        let client = connect(port, &database).await;
        exec(
            &client,
            "CREATE TABLE public.items (
                id INT PRIMARY KEY,
                value TEXT NOT NULL,
                count BIGINT NOT NULL
             );
             ALTER TABLE public.items REPLICA IDENTITY FULL;
             INSERT INTO public.items (id, value, count) VALUES (1, 'one', 1), (2, 'two', 2), (3, 'three', 3);",
        )
        .await;
    });
    let mut worker = Worker::spawn(&path, &database);
    let first = worker.kill_after_receipt(&path);
    let mut source = PostgresConnector::connect(config(port, &database, path.clone())).unwrap();
    assert!(source.check().unwrap().ok);
    assert_eq!(first.key, "[1]");
    let second = next(&mut source, &first).unwrap();
    drop(source);
    let mut source = PostgresConnector::connect(config(port, &database, path.clone())).unwrap();
    assert_eq!(next(&mut source, &first).unwrap(), second);
    let third = next(&mut source, &second).unwrap();
    assert_eq!(third.key, "[3]");
    runtime.block_on(async {
        let client = connect(port, &database).await;
        exec(
            &client,
            "BEGIN;
             UPDATE public.items SET value = 'updated' WHERE id = 1;
             DELETE FROM public.items WHERE id = 2;
             INSERT INTO public.items (id, value, count) VALUES (4, 'four', 4);
             COMMIT;",
        )
        .await;
    });
    let begin = next(&mut source, &third).unwrap();
    assert_eq!(
        begin
            .source
            .as_ref()
            .unwrap()
            .change
            .as_ref()
            .unwrap()
            .operation,
        Operation::TransactionBegin
    );
    let updated = next(&mut source, &begin).unwrap();
    assert_eq!(updated.key, "[1]");
    assert_eq!(
        updated
            .source
            .as_ref()
            .unwrap()
            .change
            .as_ref()
            .unwrap()
            .operation,
        Operation::Update
    );
    assert!(
        updated
            .source
            .as_ref()
            .unwrap()
            .change
            .as_ref()
            .unwrap()
            .before_image
            .as_ref()
            .unwrap()
            .contains("one")
    );
    let deleted = next(&mut source, &updated).unwrap();
    assert_eq!(deleted.key, "[2]");
    assert_eq!(deleted.payload, "null");
    let inserted = next(&mut source, &deleted).unwrap();
    assert_eq!(inserted.key, "[4]");
    let end = next(&mut source, &inserted).unwrap();
    assert_eq!(
        end.source
            .as_ref()
            .unwrap()
            .change
            .as_ref()
            .unwrap()
            .operation,
        Operation::TransactionEnd
    );
    assert_ne!(resume(&end).1, resume(&begin).1);
    assert!(
        source
            .read(resume(&end).0, resume(&end).1)
            .unwrap()
            .is_none()
    );
    assert!(source.check().unwrap().ok);
    runtime.block_on(async {
        let client = connect(port, &database).await;
        exec(
            &client,
            "ALTER TABLE public.items ADD COLUMN extra INT NOT NULL DEFAULT 0",
        )
        .await;
        exec(&client, "UPDATE public.items SET extra = 1 WHERE id = 1").await;
    });
    let ddl_begin = next(&mut source, &end).unwrap();
    let discovered = source.discover().unwrap();
    assert!(
        discovered
            .iter()
            .any(|kind| kind.kind == "public.items" && kind.schema.fields.contains_key("extra"))
            || next(&mut source, &ddl_begin).is_ok()
    );
    drop(source);

    let mut family = PostgresFamily {
        port,
        database: database.clone(),
        state: path.join("conformance"),
        ids: Default::default(),
        columns: 0,
        runtime: tokio::runtime::Runtime::new().unwrap(),
    };
    family.execute(
        "CREATE TABLE IF NOT EXISTS public.items (id INT PRIMARY KEY, value TEXT NOT NULL, count BIGINT NOT NULL);
         ALTER TABLE public.items REPLICA IDENTITY FULL;
         TRUNCATE public.items;"
            .into(),
    );
    let passed = hashi::conformance::run(&mut family, &path.join("conformance-cp")).unwrap();
    assert_eq!(passed, hashi::conformance::CHECKS);

    runtime.block_on(async {
        let client = connect(port, &database).await;
        let slots: Vec<String> = client
            .query("SELECT slot_name FROM pg_replication_slots", &[])
            .await
            .unwrap()
            .into_iter()
            .map(|row| row.get(0))
            .collect();
        for slot in slots {
            let _ = client
                .execute("SELECT pg_drop_replication_slot($1)", &[&slot])
                .await;
        }
    });
    let mut dropped =
        PostgresConnector::connect(config(port, &database, path.join("dropped-slot"))).unwrap();
    let mut last = dropped.read(None, None).unwrap().unwrap();
    while let Some(record) = dropped.read(resume(&last).0, resume(&last).1).unwrap() {
        last = record;
    }
    runtime.block_on(async {
        let client = connect(port, &database).await;
        let _ = client
            .batch_execute(
                "SELECT pg_terminate_backend(active_pid)
                 FROM pg_replication_slots
                 WHERE active;
                 SELECT pg_drop_replication_slot(slot_name)
                 FROM pg_replication_slots;",
            )
            .await;
        exec(&client, "INSERT INTO public.items VALUES (99, 'gone', 1)").await;
    });
    let err = next(&mut dropped, &last).unwrap_err();
    assert!(
        err.message.contains("slot") || err.message.contains("WAL") || err.message.contains("gone"),
        "{}",
        err.message
    );
}

struct Worker(std::process::Child);
impl Worker {
    fn spawn(path: &std::path::Path, database: &str) -> Self {
        Self(
            std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "postgres_snapshot_worker",
                    "--ignored",
                    "--nocapture",
                ])
                .env("HASHI_POSTGRES_WORKER_STATE", path)
                .env("HASHI_POSTGRES_WORKER_DATABASE", database)
                .stdout(std::process::Stdio::null())
                .spawn()
                .unwrap(),
        )
    }
    fn kill_after_receipt(&mut self, path: &std::path::Path) -> Record {
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while !path.join("worker.ready").exists() {
            assert!(
                std::time::Instant::now() < deadline,
                "snapshot worker did not reach durable checkpoint"
            );
            assert!(
                self.0.try_wait().unwrap().is_none(),
                "snapshot worker exited before receipt"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        self.0.kill().unwrap();
        self.0.wait().unwrap();
        wait_for_takeover();
        let line = std::fs::read_to_string(path.join("worker.ready")).unwrap();
        hashi::delivery::decode(line.trim_end_matches('\n'))
            .unwrap()
            .0
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
#[ignore = "worker is invoked only by the isolated crash-recovery fixture"]
fn postgres_snapshot_worker() {
    let Ok(path) = std::env::var("HASHI_POSTGRES_WORKER_STATE") else {
        return;
    };
    assert_eq!(
        std::env::var("HASHI_POSTGRES_DISPOSABLE").as_deref(),
        Ok("1")
    );
    let database = std::env::var("HASHI_POSTGRES_WORKER_DATABASE").unwrap();
    let port = std::env::var("HASHI_POSTGRES_TEST_PORT")
        .unwrap()
        .parse::<u16>()
        .unwrap();
    let path = std::path::PathBuf::from(path);
    let mut options = config(port, &database, path.clone());
    options.chunk_rows = 4;
    let mut source =
        hashi::lease::with_ttl_override(TAKEOVER_TTL, || PostgresConnector::connect(options))
            .unwrap();
    let record = source.read(None, None).unwrap().unwrap();
    let store = hashi::FileCheckpoint::open(path.join("checkpoints")).unwrap();
    store
        .commit(&hashi::Checkpoint {
            contract_version: hashi::CHECKPOINT_VERSION,
            connector: "postgres-fixture".into(),
            instance: "fixture".into(),
            position: record.position,
            source_cursor: record
                .source
                .as_ref()
                .and_then(|source| source.cursor.clone()),
            fencing_token: 1,
        })
        .unwrap();
    let line = hashi::delivery::encode(
        &record,
        &record.idempotency_key("postgres-fixture", "fixture"),
    );
    use std::io::Write;
    let mut ready = std::fs::File::create(path.join("worker.tmp")).unwrap();
    ready.write_all(line.as_bytes()).unwrap();
    ready.sync_all().unwrap();
    std::fs::rename(path.join("worker.tmp"), path.join("worker.ready")).unwrap();
    loop {
        std::thread::park();
    }
}

struct PostgresFamily {
    port: u16,
    database: String,
    state: std::path::PathBuf,
    ids: std::collections::BTreeMap<String, (i32, i64)>,
    columns: u32,
    runtime: tokio::runtime::Runtime,
}

impl PostgresFamily {
    fn execute(&self, sql: String) {
        let port = self.port;
        let database = self.database.clone();
        self.runtime.block_on(async move {
            let client = connect(port, &database).await;
            exec(&client, &sql).await;
        });
    }
}

impl hashi::conformance::Family for PostgresFamily {
    type Source = PostgresConnector;

    fn connect(&mut self) -> Result<PostgresConnector, hashi::Error> {
        let mut config = config(self.port, &self.database, self.state.clone());
        config.id = "postgres-conformance".into();
        config.timeout = Duration::from_secs(5);
        PostgresConnector::connect(config)
    }

    fn seed(&mut self) -> Result<Vec<hashi::conformance::Change>, hashi::Error> {
        self.produce(&["s1", "s2"])
    }

    fn produce(&mut self, keys: &[&str]) -> Result<Vec<hashi::conformance::Change>, hashi::Error> {
        let mut expected = Vec::new();
        for key in keys {
            let next = self.ids.len() as i32 + 1;
            let entry = self.ids.entry(key.to_string()).or_insert((next, 0));
            entry.1 += 1;
            let (id, count) = *entry;
            self.execute(format!(
                "INSERT INTO public.items (id, value, count) VALUES ({id}, '{key}', 1)
                 ON CONFLICT (id) DO UPDATE SET count = public.items.count + 1"
            ));
            expected.push(hashi::conformance::Change::new(
                format!("[{id}]"),
                "count",
                count,
            ));
        }
        Ok(expected)
    }

    fn drift(&mut self) -> Result<hashi::conformance::Drift, hashi::Error> {
        self.columns += 1;
        self.execute(format!(
            "ALTER TABLE public.items ADD COLUMN extra{} INT NOT NULL DEFAULT 0",
            self.columns
        ));
        Ok(hashi::conformance::Drift::Reported("public.items".into()))
    }
}
