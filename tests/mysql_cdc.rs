#![cfg(feature = "mysql")]
use hashi::mysql::{Config, MySqlConnector, Table};
use hashi::{CheckpointStore, Connector, Operation, Record};
use mysql_async::{Conn, Opts, OptsBuilder, prelude::Queryable};
use std::time::Duration;
fn config(options: Opts, database: &str, state_dir: std::path::PathBuf) -> Config {
    Config {
        id: "mysql-fixture".into(),
        options,
        tables: vec![Table {
            database: database.into(),
            table: "items".into(),
            key: vec![],
        }],
        state_dir,
        chunk_rows: 2,
        max_batch_bytes: 16 * 1024 * 1024,
        server_id: 1024,
        schema_version: 1,
        timeout: Duration::from_secs(20),
        allow_insecure_lab: true,
    }
}
fn resume(record: &Record) -> (Option<u64>, Option<&str>) {
    (
        Some(record.position),
        record.source.as_ref().and_then(|s| s.cursor.as_deref()),
    )
}
fn next(source: &mut MySqlConnector, previous: &Record) -> Result<Record, hashi::Error> {
    let deadline = std::time::Instant::now() + Duration::from_secs(25);
    loop {
        if let Some(record) = source.read(resume(previous).0, resume(previous).1)? {
            return Ok(record);
        }
        assert!(
            std::time::Instant::now() < deadline,
            "expected a source record"
        );
    }
}

#[test]
#[ignore = "requires the isolated database created by scripts/test-mysql-container.sh"]
fn snapshot_gtid_stream_restart_and_schema_changes_keep_source_order() {
    assert_eq!(std::env::var("HASHI_MYSQL_DISPOSABLE").as_deref(), Ok("1"));
    let port = std::env::var("HASHI_MYSQL_TEST_PORT")
        .unwrap()
        .parse::<u16>()
        .unwrap();
    let opts: Opts = OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .tcp_port(port)
        .user(Some("root"))
        .pass(Some("hashi-fixture-only"))
        .into();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let database = format!("hashi_fixture_{}", std::process::id());
    let path = std::env::temp_dir().join(format!(
        "hashi-mysql-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    runtime.block_on(async {
        let mut conn=Conn::new(opts.clone()).await.unwrap();
        conn.query_drop(format!("CREATE DATABASE `{database}`")).await.unwrap();
        conn.query_drop(format!("CREATE TABLE `{database}`.items (id INT PRIMARY KEY, value VARCHAR(100) NOT NULL, count BIGINT UNSIGNED NOT NULL, stamp TIMESTAMP(6) NOT NULL DEFAULT '2020-01-02 03:04:05.123456', status ENUM('one','two') NOT NULL DEFAULT 'one', flags SET('red','green') NOT NULL DEFAULT 'red,green', document JSON NOT NULL DEFAULT (JSON_OBJECT('a',1)), octets VARBINARY(4) NOT NULL DEFAULT X'00ff', hidden INT NOT NULL DEFAULT 7 INVISIBLE, zero_year YEAR NOT NULL DEFAULT 0, normal_year YEAR NOT NULL DEFAULT 2020) ENGINE=InnoDB")).await.unwrap();
        conn.query_drop(format!("INSERT INTO `{database}`.items (id,value,count) VALUES (1,'one',18446744073709551615),(2,'two',2),(3,'three',3)")).await.unwrap();
        conn.disconnect().await.unwrap();
    });
    eprintln!("starting snapshot crash proof");
    let mut worker = Worker::spawn(&path, &database);
    let first = worker.kill_after_receipt(&path);
    let mut source =
        MySqlConnector::connect(config(opts.clone(), &database, path.clone())).unwrap();
    assert!(source.check().unwrap().ok);
    assert_eq!(first.key, "[1]");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&first.payload).unwrap()["count"],
        "18446744073709551615"
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&first.payload).unwrap()["zero_year"],
        "0"
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&first.payload).unwrap()["normal_year"],
        "2020"
    );
    let second = next(&mut source, &first).unwrap();
    drop(source);
    let mut source =
        MySqlConnector::connect(config(opts.clone(), &database, path.clone())).unwrap();
    assert_eq!(next(&mut source, &first).unwrap(), second);
    let third = next(&mut source, &second).unwrap();
    assert_eq!(third.key, "[3]");
    runtime.block_on(async {
        let mut conn = Conn::new(opts.clone()).await.unwrap();
        conn.query_drop("BEGIN").await.unwrap();
        conn.query_drop(format!(
            "UPDATE `{database}`.items SET value='updated' WHERE id=1"
        ))
        .await
        .unwrap();
        conn.query_drop(format!("DELETE FROM `{database}`.items WHERE id=2"))
            .await
            .unwrap();
        conn.query_drop(format!(
            "INSERT INTO `{database}`.items (id,value,count) VALUES (4,'four',4)"
        ))
        .await
        .unwrap();
        conn.query_drop("COMMIT").await.unwrap();
        conn.disconnect().await.unwrap();
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
    for field in [
        "status",
        "flags",
        "document",
        "octets",
        "hidden",
        "zero_year",
        "normal_year",
    ] {
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&updated.payload).unwrap()[field],
            serde_json::from_str::<serde_json::Value>(&first.payload).unwrap()[field],
            "{field}"
        );
    }

    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&updated.payload).unwrap()["stamp"],
        serde_json::from_str::<serde_json::Value>(&first.payload).unwrap()["stamp"]
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&updated.payload).unwrap()["count"],
        "18446744073709551615"
    );
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
        let mut conn = Conn::new(opts.clone()).await.unwrap();
        conn.query_drop(format!(
            "ALTER TABLE `{database}`.items ADD COLUMN extra INT NOT NULL DEFAULT 0"
        ))
        .await
        .unwrap();
        conn.disconnect().await.unwrap();
    });
    let ddl_begin = next(&mut source, &end).unwrap();
    let discovered = source.discover().unwrap();
    assert_eq!(
        discovered
            .iter()
            .find(|kind| kind.kind == format!("{database}.items"))
            .unwrap()
            .schema
            .version,
        2
    );
    assert!(
        discovered
            .iter()
            .any(|kind| kind.schema.fields.contains_key("extra"))
    );
    let ddl_end = next(&mut source, &ddl_begin).unwrap();
    drop(source);
    let mut source =
        MySqlConnector::connect(config(opts.clone(), &database, path.clone())).unwrap();
    assert_eq!(next(&mut source, &ddl_begin).unwrap(), ddl_end);
    // Two DDLs can overtake the reader. Never decode the intervening row
    // using the newer catalog's same-width renamed column.
    runtime.block_on(async {
        let mut conn = Conn::new(opts.clone()).await.unwrap();
        conn.query_drop(format!(
            "ALTER TABLE `{database}`.items ADD COLUMN historical INT NOT NULL DEFAULT 0"
        ))
        .await
        .unwrap();
        conn.query_drop(format!(
            "UPDATE `{database}`.items SET historical=7 WHERE id=1"
        ))
        .await
        .unwrap();
        conn.query_drop(format!(
            "ALTER TABLE `{database}`.items RENAME COLUMN historical TO renamed"
        ))
        .await
        .unwrap();
        conn.disconnect().await.unwrap();
    });
    let boundary1 = next(&mut source, &ddl_end).unwrap();
    let boundary2 = next(&mut source, &boundary1).unwrap();
    assert!(
        next(&mut source, &boundary2)
            .unwrap_err()
            .message
            .contains("historical schema")
    );
    drop(source);
    runtime.block_on(async {
        let mut conn = Conn::new(opts.clone()).await.unwrap();
        conn.query_drop(format!("CREATE TABLE `{database}`.key_history (id INT PRIMARY KEY, other_id INT NOT NULL) ENGINE=InnoDB")).await.unwrap();
        conn.query_drop(format!("INSERT INTO `{database}`.key_history VALUES (1,11),(2,22)")).await.unwrap();
        conn.disconnect().await.unwrap();
    });
    let mut key_config = config(opts.clone(), &database, path.join("key-history"));
    key_config.tables[0].table = "key_history".into();
    let mut key_source = MySqlConnector::connect(key_config).unwrap();
    let key_first = key_source.read(None, None).unwrap().unwrap();
    let key_last = next(&mut key_source, &key_first).unwrap();
    runtime.block_on(async {
        let mut conn = Conn::new(opts.clone()).await.unwrap();
        conn.query_drop(format!(
            "ALTER TABLE `{database}`.key_history ADD INDEX(other_id)"
        ))
        .await
        .unwrap();
        conn.query_drop(format!(
            "UPDATE `{database}`.key_history SET other_id=33 WHERE id=1"
        ))
        .await
        .unwrap();
        conn.query_drop(format!(
            "ALTER TABLE `{database}`.key_history DROP PRIMARY KEY, ADD PRIMARY KEY(other_id)"
        ))
        .await
        .unwrap();
        conn.disconnect().await.unwrap();
    });
    let key_begin = next(&mut key_source, &key_last).unwrap();
    let key_end = next(&mut key_source, &key_begin).unwrap();
    assert!(
        next(&mut key_source, &key_end)
            .unwrap_err()
            .message
            .contains("historical primary key")
    );
    drop(key_source);
    runtime.block_on(async {
        let mut conn = Conn::new(opts.clone()).await.unwrap();
        conn.query_drop(format!("CREATE TABLE `{database}`.encoding_history (id INT PRIMARY KEY, label VARCHAR(20) CHARACTER SET utf8mb4 NOT NULL) ENGINE=InnoDB")).await.unwrap();
        conn.query_drop(format!("INSERT INTO `{database}`.encoding_history VALUES (1,'initial')")).await.unwrap();
        conn.disconnect().await.unwrap();
    });
    let mut encoding_config = config(opts.clone(), &database, path.join("encoding-history"));
    encoding_config.tables[0].table = "encoding_history".into();
    let mut encoding_source = MySqlConnector::connect(encoding_config).unwrap();
    let encoding_last = encoding_source.read(None, None).unwrap().unwrap();
    runtime.block_on(async {
        let mut conn = Conn::new(opts.clone()).await.unwrap();
        conn.query_drop(format!("ALTER TABLE `{database}`.encoding_history MODIFY label VARCHAR(20) CHARACTER SET latin1 NOT NULL")).await.unwrap();
        conn.query_drop(format!("UPDATE `{database}`.encoding_history SET label=CONVERT(X'C3A9' USING latin1) WHERE id=1")).await.unwrap();
        conn.query_drop(format!("ALTER TABLE `{database}`.encoding_history MODIFY label VARCHAR(20) CHARACTER SET utf8mb4 NOT NULL")).await.unwrap();
        conn.disconnect().await.unwrap();
    });
    assert!(
        next(&mut encoding_source, &encoding_last)
            .unwrap_err()
            .message
            .contains("data-transforming ALTER")
    );
    drop(encoding_source);
    runtime.block_on(async {
        let mut conn = Conn::new(opts.clone()).await.unwrap();
        conn.query_drop(format!(
            "CREATE TABLE `{database}`.category_keys (id ENUM('z','a') PRIMARY KEY) ENGINE=InnoDB"
        ))
        .await
        .unwrap();
        conn.query_drop(format!(
            "INSERT INTO `{database}`.category_keys VALUES ('z'),('a')"
        ))
        .await
        .unwrap();
        conn.disconnect().await.unwrap();
    });
    let mut category_config = config(opts.clone(), &database, path.join("category-keys"));
    category_config.tables[0].table = "category_keys".into();
    category_config.chunk_rows = 1;
    assert!(
        MySqlConnector::connect(category_config)
            .err()
            .unwrap()
            .message
            .contains("incompatible snapshot ordering")
    );
    runtime.block_on(async {
        let mut conn = Conn::new(opts.clone()).await.unwrap();
        conn.query_drop(format!("CREATE TABLE `{database}`.parents (id INT PRIMARY KEY) ENGINE=InnoDB")).await.unwrap();
        conn.query_drop(format!("CREATE TABLE `{database}`.children (id INT PRIMARY KEY, parent INT, FOREIGN KEY(parent) REFERENCES `{database}`.parents(id) ON DELETE CASCADE) ENGINE=InnoDB")).await.unwrap();
        conn.disconnect().await.unwrap();
    });
    let mut cascade_config = config(opts.clone(), &database, path.join("cascade"));
    cascade_config.tables[0].table = "children".into();
    assert!(
        MySqlConnector::connect(cascade_config)
            .err()
            .unwrap()
            .message
            .contains("cascading foreign keys")
    );
    runtime.block_on(async {
        let mut conn = Conn::new(opts.clone()).await.unwrap();
        conn.query_drop(format!("CREATE TABLE `{database}`.fractional_time (id INT PRIMARY KEY, duration TIME(2) NOT NULL) ENGINE=InnoDB")).await.unwrap();
        conn.query_drop(format!("INSERT INTO `{database}`.fractional_time VALUES (1,'-00:00:00.01')")).await.unwrap();
        conn.disconnect().await.unwrap();
    });
    let mut time_config = config(opts.clone(), &database, path.join("fractional-time"));
    time_config.tables[0].table = "fractional_time".into();
    assert!(
        MySqlConnector::connect(time_config)
            .err()
            .unwrap()
            .message
            .contains("TIME(1)/TIME(2)")
    );
    let mut create_source =
        MySqlConnector::connect(config(opts.clone(), &database, path.join("create-select")))
            .unwrap();
    let mut create_last = create_source.read(None, None).unwrap().unwrap();
    for _ in 0..2 {
        create_last = next(&mut create_source, &create_last).unwrap();
    }
    runtime.block_on(async {
        let mut conn = Conn::new(opts.clone()).await.unwrap();
        conn.query_drop(format!(
            "CREATE TABLE `{database}`.copied AS SELECT * FROM `{database}`.items"
        ))
        .await
        .unwrap();
        conn.disconnect().await.unwrap();
    });
    assert!(
        next(&mut create_source, &create_last)
            .unwrap_err()
            .message
            .contains("CREATE transaction boundary")
    );
    drop(create_source);
    runtime.block_on(async {
        let mut conn=Conn::new(opts.clone()).await.unwrap();
        conn.query_drop(format!("CREATE TABLE `{database}`.prefixes (a INT NOT NULL, b VARCHAR(20) NOT NULL, UNIQUE(a,b(10))) ENGINE=InnoDB")).await.unwrap();
        conn.disconnect().await.unwrap();
    });
    let mut prefix_config = config(opts.clone(), &database, path.join("prefix"));
    prefix_config.tables[0].table = "prefixes".into();
    prefix_config.tables[0].key = vec!["a".into()];
    assert!(
        MySqlConnector::connect(prefix_config)
            .err()
            .unwrap()
            .message
            .contains("without prefixes")
    );
    runtime.block_on(async {
        let mut conn = Conn::new(opts.clone()).await.unwrap();
        conn.query_drop("SET GLOBAL binlog_row_metadata='MINIMAL'")
            .await
            .unwrap();
        conn.disconnect().await.unwrap();
    });
    assert!(
        MySqlConnector::connect(config(opts.clone(), &database, path.join("minimal")))
            .err()
            .unwrap()
            .message
            .contains("FULL row metadata")
    );
    runtime.block_on(async {
        let mut conn = Conn::new(opts.clone()).await.unwrap();
        conn.query_drop("SET GLOBAL binlog_row_metadata='FULL'")
            .await
            .unwrap();
        conn.disconnect().await.unwrap();
    });
    runtime.block_on(async {
        let mut conn = Conn::new(opts.clone()).await.unwrap();
        conn.query_drop("CREATE USER 'cdc_reader'@'%' IDENTIFIED BY 'hashi-fixture-only'")
            .await
            .unwrap();
        conn.query_drop("GRANT SELECT,RELOAD,REPLICATION CLIENT ON *.* TO 'cdc_reader'@'%'")
            .await
            .unwrap();
        conn.disconnect().await.unwrap();
    });
    let restricted_opts: Opts = OptsBuilder::from_opts(opts.clone())
        .user(Some("cdc_reader"))
        .into();
    let restricted =
        MySqlConnector::connect(config(restricted_opts, &database, path.join("restricted")))
            .unwrap();
    let missing = restricted.check().unwrap();
    assert!(!missing.ok);
    assert!(missing.message.contains("REPLICATION SLAVE"));
    runtime.block_on(async {
        let mut conn = Conn::new(opts.clone()).await.unwrap();
        conn.query_drop("CREATE ROLE 'fixture_capture'")
            .await
            .unwrap();
        conn.query_drop("GRANT REPLICATION SLAVE ON *.* TO 'fixture_capture'")
            .await
            .unwrap();
        conn.query_drop("GRANT 'fixture_capture' TO 'cdc_reader'@'%'")
            .await
            .unwrap();
        conn.query_drop("SET DEFAULT ROLE 'fixture_capture' TO 'cdc_reader'@'%'")
            .await
            .unwrap();
        conn.disconnect().await.unwrap();
    });
    assert!(
        restricted.check().unwrap().ok,
        "effective role grants must satisfy the privilege probe"
    );
    drop(restricted);
    // Hold an actual repeatable-read snapshot across thousands of chunks,
    // then commit a write before its final row has been staged.
    runtime.block_on(async {
        let mut conn = Conn::new(opts.clone()).await.unwrap();
        for start in (100..10_100).step_by(1000) {
            let rows: Vec<_> = (start..start + 1000)
                .map(|id| format!("({id},'before',{id})"))
                .collect();
            conn.query_drop(format!(
                "INSERT INTO `{database}`.items (id,value,count) VALUES {}",
                rows.join(",")
            ))
            .await
            .unwrap();
        }
        conn.disconnect().await.unwrap();
    });
    let concurrent_path = path.join("concurrent");
    eprintln!("starting concurrent snapshot proof");
    let mut concurrent_worker = Worker::spawn(&concurrent_path, &database);
    runtime.block_on(async {
        let mut conn=Conn::new(opts.clone()).await.unwrap();
        let deadline=std::time::Instant::now()+Duration::from_secs(20);
        loop {
            let active:Option<u64>=conn.exec_first("SELECT COUNT(*) FROM performance_schema.metadata_locks WHERE OBJECT_SCHEMA=? AND OBJECT_NAME='items' AND LOCK_DURATION='TRANSACTION' AND LOCK_STATUS='GRANTED'", (&database,)).await.unwrap();
            if active.is_some_and(|count|count>0) { break; }
            assert!(!concurrent_path.join("worker.ready").exists(),"snapshot finished before concurrent-write evidence");
            assert!(std::time::Instant::now()<deadline,"snapshot transaction did not appear");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        conn.query_drop(format!("UPDATE `{database}`.items SET value='concurrent' WHERE id=10099")).await.unwrap();
        assert!(!concurrent_path.join("worker.ready").exists(),"write must commit during snapshot construction");
        conn.disconnect().await.unwrap();
    });
    let mut previous = concurrent_worker.kill_after_receipt(&concurrent_path);
    let mut concurrent =
        MySqlConnector::connect(config(opts.clone(), &database, concurrent_path)).unwrap();
    for _ in 1..10_003 {
        previous = next(&mut concurrent, &previous).unwrap();
    }
    assert_eq!(previous.key, "[10099]");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&previous.payload).unwrap()["value"],
        "before"
    );
    let begin = next(&mut concurrent, &previous).unwrap();
    let changed = next(&mut concurrent, &begin).unwrap();
    assert_eq!(changed.key, "[10099]");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&changed.payload).unwrap()["value"],
        "concurrent"
    );
    drop(concurrent);
    runtime.block_on(async {
        let mut conn = Conn::new(opts.clone()).await.unwrap();
        conn.query_drop(format!("DELETE FROM `{database}`.items WHERE id>=100"))
            .await
            .unwrap();
        conn.disconnect().await.unwrap();
    });
    let expired_state = path.join("expired");
    let mut expired =
        MySqlConnector::connect(config(opts.clone(), &database, expired_state)).unwrap();
    let snap1 = expired.read(None, None).unwrap().unwrap();
    let snap2 = next(&mut expired, &snap1).unwrap();
    let snap3 = next(&mut expired, &snap2).unwrap();
    runtime.block_on(async {
        let mut conn = Conn::new(opts.clone()).await.unwrap();
        conn.query_drop(format!(
            "UPDATE `{database}`.items SET value='after-snapshot' WHERE id=1"
        ))
        .await
        .unwrap();
        conn.query_drop("FLUSH BINARY LOGS").await.unwrap();
        let logs: Vec<mysql_async::Row> = conn.query("SHOW BINARY LOGS").await.unwrap();
        let latest: String = logs.last().unwrap().get(0).unwrap();
        assert!(
            latest
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
        );
        conn.query_drop(format!("PURGE BINARY LOGS TO '{latest}'"))
            .await
            .unwrap();
        conn.disconnect().await.unwrap();
    });
    assert!(
        next(&mut expired, &snap3)
            .unwrap_err()
            .message
            .contains("retention expired")
    );
    assert!(!expired.check().unwrap().ok);
    drop(expired);
    let mut truncated =
        MySqlConnector::connect(config(opts.clone(), &database, path.join("truncate"))).unwrap();
    let t1 = truncated.read(None, None).unwrap().unwrap();
    let t2 = next(&mut truncated, &t1).unwrap();
    let t3 = next(&mut truncated, &t2).unwrap();
    runtime.block_on(async {
        let mut conn = Conn::new(opts.clone()).await.unwrap();
        conn.query_drop(format!("TRUNCATE TABLE `{database}`.items"))
            .await
            .unwrap();
        conn.disconnect().await.unwrap();
    });
    assert!(
        next(&mut truncated, &t3)
            .unwrap_err()
            .message
            .contains("consumer recovery")
    );
    drop(truncated);
    runtime.block_on(async {
        let mut conn = Conn::new(opts.clone()).await.unwrap();
        conn.query_drop(format!(
            "INSERT INTO `{database}`.items (id,value,count) VALUES (9,'retained',9)"
        ))
        .await
        .unwrap();
        conn.query_drop(format!(
            "CREATE TABLE `{database}`.replacement LIKE `{database}`.items"
        ))
        .await
        .unwrap();
        conn.disconnect().await.unwrap();
    });
    let mut replaced =
        MySqlConnector::connect(config(opts.clone(), &database, path.join("replacement"))).unwrap();
    let retained = replaced.read(None, None).unwrap().unwrap();
    runtime.block_on(async {
        let mut conn = Conn::new(opts.clone()).await.unwrap();
        conn.query_drop(format!("DROP TABLE `{database}`.items"))
            .await
            .unwrap();
        conn.query_drop(format!(
            "RENAME TABLE `{database}`.replacement TO `{database}`.items"
        ))
        .await
        .unwrap();
        conn.disconnect().await.unwrap();
    });
    assert!(
        next(&mut replaced, &retained)
            .unwrap_err()
            .message
            .contains("destructive DDL")
    );
    drop(replaced);
    runtime.block_on(async {
        let mut conn = Conn::new(opts).await.unwrap();
        conn.query_drop(format!("DROP DATABASE `{database}`"))
            .await
            .unwrap();
        conn.disconnect().await.unwrap();
    });
    std::fs::remove_dir_all(path).unwrap();
}

struct Worker(std::process::Child);
impl Worker {
    fn spawn(path: &std::path::Path, database: &str) -> Self {
        Self(
            std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "mysql_snapshot_worker",
                    "--ignored",
                    "--nocapture",
                ])
                .env("HASHI_MYSQL_WORKER_STATE", path)
                .env("HASHI_MYSQL_WORKER_DATABASE", database)
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
fn mysql_snapshot_worker() {
    let Ok(path) = std::env::var("HASHI_MYSQL_WORKER_STATE") else {
        return;
    };
    assert_eq!(std::env::var("HASHI_MYSQL_DISPOSABLE").as_deref(), Ok("1"));
    let database = std::env::var("HASHI_MYSQL_WORKER_DATABASE").unwrap();
    let port = std::env::var("HASHI_MYSQL_TEST_PORT")
        .unwrap()
        .parse::<u16>()
        .unwrap();
    let opts: Opts = OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .tcp_port(port)
        .user(Some("root"))
        .pass(Some("hashi-fixture-only"))
        .into();
    let path = std::path::PathBuf::from(path);
    let mut options = config(opts, &database, path.clone());
    options.chunk_rows = 4;
    let mut source = MySqlConnector::connect(options).unwrap();
    let record = source.read(None, None).unwrap().unwrap();
    let store = hashi::FileCheckpoint::open(path.join("checkpoints")).unwrap();
    store
        .commit(&hashi::Checkpoint {
            contract_version: hashi::CONTRACT_VERSION,
            connector: "mysql-fixture".into(),
            instance: "fixture".into(),
            position: record.position,
            source_cursor: record
                .source
                .as_ref()
                .and_then(|source| source.cursor.clone()),
        })
        .unwrap();
    let line =
        hashi::delivery::encode(&record, &record.idempotency_key("mysql-fixture", "fixture"));
    use std::io::Write;
    let mut ready = std::fs::File::create(path.join("worker.tmp")).unwrap();
    ready.write_all(line.as_bytes()).unwrap();
    ready.sync_all().unwrap();
    std::fs::rename(path.join("worker.tmp"), path.join("worker.ready")).unwrap();
    loop {
        std::thread::park();
    }
}

/// Conformance kit (#28) driver: rows keyed by id; repeating a key updates its row;
/// drift adds a column.
struct MySqlFamily {
    opts: Opts,
    database: String,
    state: std::path::PathBuf,
    ids: std::collections::BTreeMap<String, (u32, u64)>,
    columns: u32,
    runtime: tokio::runtime::Runtime,
}

impl MySqlFamily {
    fn execute(&self, sql: String) {
        let opts = self.opts.clone();
        self.runtime.block_on(async move {
            let mut conn = Conn::new(opts).await.unwrap();
            conn.query_drop(sql).await.unwrap();
            conn.disconnect().await.unwrap();
        });
    }
}

impl hashi::conformance::Family for MySqlFamily {
    type Source = MySqlConnector;

    fn connect(&mut self) -> Result<MySqlConnector, hashi::Error> {
        let mut config = config(self.opts.clone(), &self.database, self.state.clone());
        config.id = "mysql-conformance".into();
        config.server_id = 2048;
        config.timeout = Duration::from_secs(5);
        MySqlConnector::connect(config)
    }

    /// Rows that exist before the first connect, so the snapshot path delivers them.
    fn seed(&mut self) -> Result<Vec<hashi::conformance::Change>, hashi::Error> {
        self.produce(&["s1", "s2"])
    }

    fn produce(&mut self, keys: &[&str]) -> Result<Vec<hashi::conformance::Change>, hashi::Error> {
        let mut expected = Vec::new();
        for key in keys {
            let next = self.ids.len() as u32 + 1;
            let entry = self.ids.entry(key.to_string()).or_insert((next, 0));
            entry.1 += 1;
            let (id, count) = *entry;
            self.execute(format!(
                "INSERT INTO `{}`.items (id, value, count) VALUES ({id}, '{key}', 1) \
                 ON DUPLICATE KEY UPDATE count = count + 1",
                self.database
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
            "ALTER TABLE `{}`.items ADD COLUMN extra{} INT NOT NULL DEFAULT 0",
            self.database, self.columns
        ));
        Ok(hashi::conformance::Drift::Reported(format!(
            "{}.items",
            self.database
        )))
    }
}

#[test]
#[ignore = "requires the isolated database created by scripts/test-mysql-container.sh"]
fn mysql_passes_the_conformance_kit() {
    assert_eq!(std::env::var("HASHI_MYSQL_DISPOSABLE").as_deref(), Ok("1"));
    let port = std::env::var("HASHI_MYSQL_TEST_PORT")
        .unwrap()
        .parse::<u16>()
        .unwrap();
    let opts: Opts = OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .tcp_port(port)
        .user(Some("root"))
        .pass(Some("hashi-fixture-only"))
        .into();
    let database = format!("hashi_conformance_{}", std::process::id());
    let path = std::env::temp_dir().join(format!(
        "hashi-mysql-conformance-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let family = MySqlFamily {
        opts,
        database: database.clone(),
        state: path.join("source"),
        ids: Default::default(),
        columns: 0,
        runtime: tokio::runtime::Runtime::new().unwrap(),
    };
    family.execute(format!("CREATE DATABASE `{database}`"));
    family.execute(format!(
        "CREATE TABLE `{database}`.items (id INT PRIMARY KEY, value VARCHAR(100) NOT NULL, \
         count BIGINT UNSIGNED NOT NULL) ENGINE=InnoDB"
    ));
    let mut family = family;
    let passed = hashi::conformance::run(&mut family, &path.join("checkpoints")).unwrap();
    assert_eq!(passed, hashi::conformance::CHECKS);
}

/// Runs `docker` against the fixture container and returns its trimmed stdout.
fn docker(args: &[&str]) -> Result<String, hashi::Error> {
    let output = std::process::Command::new("docker")
        .args(args)
        .output()
        .map_err(|error| hashi::Error::new(format!("docker: {error}")))?;
    if !output.status.success() {
        return Err(hashi::Error::new(format!(
            "docker {}: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

const SOAK_USER: &str = "hashi_soak";

/// Soak (#48) driver over the conformance rows. The connector reads as its own
/// user, so a credential rotation changes that user's password. The fixture's
/// data lives on tmpfs, which a container restart would wipe, so a source
/// restart kills every session of that user instead: the connector sees what a
/// restarting server does to its clients.
struct MySqlSoak {
    rows: MySqlFamily,
    container: String,
    password: u64,
}

impl MySqlSoak {
    fn password(&self) -> String {
        format!("hashi-soak-{}", self.password)
    }

    fn admin<T>(
        &self,
        work: impl AsyncFnOnce(&mut Conn) -> Result<T, mysql_async::Error>,
    ) -> Result<T, hashi::Error> {
        let opts = self.rows.opts.clone();
        self.rows
            .runtime
            .block_on(async move {
                let mut conn = Conn::new(opts).await?;
                let out = work(&mut conn).await?;
                conn.disconnect().await?;
                Ok(out)
            })
            .map_err(|error: mysql_async::Error| hashi::Error::new(format!("mysql admin: {error}")))
    }
}

impl hashi::conformance::Family for MySqlSoak {
    type Source = MySqlConnector;

    fn connect(&mut self) -> Result<MySqlConnector, hashi::Error> {
        let reader: Opts = OptsBuilder::from_opts(self.rows.opts.clone())
            .user(Some(SOAK_USER))
            .pass(Some(self.password()))
            .into();
        let mut config = config(reader, &self.rows.database, self.rows.state.clone());
        config.id = "mysql-soak".into();
        config.server_id = 2049;
        config.timeout = Duration::from_secs(5);
        MySqlConnector::connect(config)
    }

    fn seed(&mut self) -> Result<Vec<hashi::conformance::Change>, hashi::Error> {
        self.rows.seed()
    }

    fn produce(&mut self, keys: &[&str]) -> Result<Vec<hashi::conformance::Change>, hashi::Error> {
        self.rows.produce(keys)
    }

    fn drift(&mut self) -> Result<hashi::conformance::Drift, hashi::Error> {
        Err(hashi::Error::new("the soak harness never drifts"))
    }
}

impl hashi::soak::Faults for MySqlSoak {
    fn faults(&self) -> Vec<hashi::soak::Fault> {
        vec![
            hashi::soak::Fault::NetworkDrop,
            hashi::soak::Fault::SourceRestart,
            hashi::soak::Fault::CredentialRotation,
        ]
    }

    fn inject(&mut self, fault: hashi::soak::Fault) -> Result<(), hashi::Error> {
        match fault {
            hashi::soak::Fault::NetworkDrop => docker(&["pause", &self.container]).map(drop),
            hashi::soak::Fault::SourceRestart => self.admin(async |conn| {
                let sessions: Vec<u64> = conn
                    .exec(
                        "SELECT id FROM information_schema.processlist WHERE user = ?",
                        (SOAK_USER,),
                    )
                    .await?;
                for id in sessions {
                    // A session may end on its own first.
                    let _ = conn.query_drop(format!("KILL {id}")).await;
                }
                Ok(())
            }),
            hashi::soak::Fault::CredentialRotation => {
                self.password += 1;
                // ALTER USER takes no placeholders; the password is generated here.
                let statement = format!(
                    "ALTER USER '{SOAK_USER}'@'%' IDENTIFIED BY '{}'",
                    self.password()
                );
                // Kept out of the binlog: the connector fails closed on account
                // statements there (#49), and this fault is about the new credential.
                self.admin(async move |conn| {
                    conn.query_drop("SET SESSION sql_log_bin = 0").await?;
                    conn.query_drop(statement).await
                })
            }
        }
    }

    fn heal(&mut self, fault: hashi::soak::Fault) -> Result<(), hashi::Error> {
        match fault {
            hashi::soak::Fault::NetworkDrop => docker(&["unpause", &self.container]).map(drop),
            _ => Ok(()),
        }
    }
}

#[test]
#[ignore = "requires the isolated database created by scripts/test-mysql-container.sh"]
fn mysql_survives_a_soak_with_every_fault() {
    assert_eq!(std::env::var("HASHI_MYSQL_DISPOSABLE").as_deref(), Ok("1"));
    let container = std::env::var("HASHI_MYSQL_CONTAINER").unwrap();
    let port = std::env::var("HASHI_MYSQL_TEST_PORT")
        .unwrap()
        .parse::<u16>()
        .unwrap();
    let opts: Opts = OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .tcp_port(port)
        .user(Some("root"))
        .pass(Some("hashi-fixture-only"))
        .into();
    let database = format!("hashi_soak_{}", std::process::id());
    let path = std::env::temp_dir().join(format!(
        "hashi-mysql-soak-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let rows = MySqlFamily {
        opts,
        database: database.clone(),
        state: path.join("source"),
        ids: Default::default(),
        columns: 0,
        runtime: tokio::runtime::Runtime::new().unwrap(),
    };
    rows.execute(format!("CREATE DATABASE `{database}`"));
    rows.execute(format!(
        "CREATE TABLE `{database}`.items (id INT PRIMARY KEY, value VARCHAR(100) NOT NULL, \
         count BIGINT UNSIGNED NOT NULL) ENGINE=InnoDB"
    ));
    let mut family = MySqlSoak {
        rows,
        container,
        password: 0,
    };
    let password = family.password();
    family.rows.execute(format!(
        "CREATE USER '{SOAK_USER}'@'%' IDENTIFIED BY '{password}'"
    ));
    family.rows.execute(format!(
        "GRANT SELECT, RELOAD, REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO '{SOAK_USER}'@'%'"
    ));
    let report = hashi::soak::soak_from_env(
        &mut family,
        "mysql",
        &path.join("checkpoints"),
        Duration::from_secs(45),
    );
    assert!(report.passed, "{report:#?}");
    for fault in hashi::soak::Faults::faults(&family) {
        assert!(report.faults.contains_key(&fault), "{fault:?}: {report:#?}");
    }
}

/// One outbox run (#63) from `committed` into a fresh outbox: the pages it
/// wrote, in order.
fn outbox_run(
    port: u16,
    database: &str,
    committed: &str,
    work: &std::path::Path,
    run: usize,
) -> Vec<serde_json::Value> {
    let dir = work.join(format!("run-{run}"));
    let outbox = dir.join("outbox");
    std::fs::create_dir_all(&outbox).unwrap();
    std::fs::write(dir.join("checkpoint"), committed).unwrap();
    std::fs::write(dir.join("credential"), "hashi-fixture-only\n").unwrap();
    std::fs::write(
        dir.join("config.json"),
        serde_json::json!({
            "envelope": {
                "namespace": "orders",
                "producer_identity": "connector.hashi",
                "source": "mysql",
                "source_instance": "fixture",
                "type_digest": format!("sha256:{}", "a".repeat(64)),
            },
            "url": format!("mysql://root@127.0.0.1:{port}"),
            "tables": [{"database": database, "table": "orders"}],
            "server_id": 2048,
            "allow_insecure_lab": true,
        })
        .to_string(),
    )
    .unwrap();
    let pages = hashi::outbox::run_mysql_once(&hashi::outbox::HostFiles {
        checkpoint: dir.join("checkpoint"),
        credential: dir.join("credential"),
        config: dir.join("config.json"),
        outbox: outbox.clone(),
    })
    .unwrap();
    (1..=pages)
        .map(|n| {
            assert!(outbox.join(format!("{n:06}.ok")).exists());
            serde_json::from_slice(&std::fs::read(outbox.join(format!("{n:06}.json"))).unwrap())
                .unwrap()
        })
        .collect()
}

#[test]
#[ignore = "requires the isolated database created by scripts/test-mysql-container.sh"]
fn outbox_runs_page_an_insert_update_and_delete_cursor_to_cursor() {
    assert_eq!(std::env::var("HASHI_MYSQL_DISPOSABLE").as_deref(), Ok("1"));
    let port = std::env::var("HASHI_MYSQL_TEST_PORT")
        .unwrap()
        .parse::<u16>()
        .unwrap();
    let opts: Opts = OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .tcp_port(port)
        .user(Some("root"))
        .pass(Some("hashi-fixture-only"))
        .into();
    let database = format!("hashi_outbox_{}", std::process::id());
    let work = std::env::temp_dir().join(format!(
        "hashi-outbox-it-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let sql = |statements: Vec<String>| {
        runtime.block_on(async {
            let mut conn = Conn::new(opts.clone()).await.unwrap();
            for statement in statements {
                conn.query_drop(statement).await.unwrap();
            }
            conn.disconnect().await.unwrap();
        })
    };
    sql(vec![
        format!("CREATE DATABASE `{database}`"),
        format!(
            "CREATE TABLE `{database}`.orders (id INT PRIMARY KEY, status VARCHAR(20) NOT NULL) ENGINE=InnoDB"
        ),
        format!("INSERT INTO `{database}`.orders VALUES (1, 'open')"),
    ]);

    // The first run snapshots the table.
    let snapshot = outbox_run(port, &database, "", &work, 1);
    assert_eq!(snapshot.len(), 1);
    assert_eq!(snapshot[0]["current_cursor"], "");
    assert_eq!(snapshot[0]["records"][0]["properties"]["status"], "open");
    let committed = snapshot[0]["proposed_next_cursor"]
        .as_str()
        .unwrap()
        .to_owned();

    sql(vec![
        format!("INSERT INTO `{database}`.orders VALUES (2, 'open')"),
        format!("UPDATE `{database}`.orders SET status = 'paid' WHERE id = 1"),
        format!("DELETE FROM `{database}`.orders WHERE id = 2"),
    ]);
    let changes = outbox_run(port, &database, &committed, &work, 2);
    assert!(!changes.is_empty());
    let mut expected = committed.clone();
    let mut seen = Vec::new();
    for page in &changes {
        assert_eq!(page["current_cursor"], expected.as_str());
        assert!(page.get("delivery").is_none());
        expected = page["proposed_next_cursor"].as_str().unwrap().to_owned();
        for record in page["records"].as_array().unwrap() {
            seen.push((
                record["external_id"].as_str().unwrap().to_owned(),
                record["properties"]["status"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned(),
                record["deleted"].as_bool().unwrap(),
            ));
        }
    }
    assert_eq!(
        seen,
        [
            ("[2]".to_owned(), "open".to_owned(), false),
            ("[1]".to_owned(), "paid".to_owned(), false),
            // A delete carries the key, not the row.
            ("[2]".to_owned(), String::new(), true),
        ]
    );

    // The host refused them: a rerun from the same cursor names its pages
    // the same and carries the same records.
    let again = outbox_run(port, &database, &committed, &work, 3);
    let keys = |pages: &[serde_json::Value]| -> Vec<(String, String)> {
        pages
            .iter()
            .map(|page| {
                (
                    page["idempotency_key"].as_str().unwrap().to_owned(),
                    page["batch_digest"].as_str().unwrap().to_owned(),
                )
            })
            .collect()
    };
    assert_eq!(keys(&again), keys(&changes));
    // Nothing new past the last cursor.
    assert!(outbox_run(port, &database, &expected, &work, 4).is_empty());
    sql(vec![format!("DROP DATABASE `{database}`")]);
    let _ = std::fs::remove_dir_all(&work);
}
