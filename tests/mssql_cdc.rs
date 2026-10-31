#![cfg(feature = "mssql")]
use hashi::mssql::{Config, MssqlConnector, Table};
use hashi::{Connector, Operation, Record};
use std::time::Duration;
use tiberius::{AuthMethod, Client, Config as SqlConfig};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

fn fixture_password() -> String {
    std::env::var("HASHI_MSSQL_SA_PASSWORD").expect("HASHI_MSSQL_SA_PASSWORD")
}

fn config(host: &str, port: u16, database: &str, state_dir: std::path::PathBuf) -> Config {
    Config {
        id: "mssql-fixture".into(),
        host: host.into(),
        port,
        user: "sa".into(),
        password: fixture_password(),
        database: database.into(),
        tables: vec![Table {
            schema: "dbo".into(),
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

fn next(source: &mut MssqlConnector, previous: &Record) -> Result<Record, hashi::Error> {
    let deadline = std::time::Instant::now() + Duration::from_secs(25);
    loop {
        if let Some(record) = source.read(resume(previous).0, resume(previous).1)? {
            return Ok(record);
        }
        assert!(
            std::time::Instant::now() < deadline,
            "expected a source record"
        );
        std::thread::sleep(Duration::from_millis(200));
    }
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

async fn wait_agent(client: &mut Client<tokio_util::compat::Compat<TcpStream>>) {
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let rows = match client
            .simple_query(
                "SELECT TOP 1 status_desc FROM sys.dm_server_services WHERE servicename LIKE N'SQL Server Agent%'",
            )
            .await
        {
            Ok(stream) => stream.into_first_result().await.unwrap_or_default(),
            Err(_) => Vec::new(),
        };
        let running = rows.first().is_some_and(|row| {
            row.try_get::<&str, _>(0)
                .ok()
                .flatten()
                .is_some_and(|status| status.eq_ignore_ascii_case("Running"))
        });
        if running {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "SQL Server Agent was not running"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

fn count_i32(row: &tiberius::Row) -> i32 {
    row.try_get::<i32, _>(0)
        .ok()
        .flatten()
        .or_else(|| {
            row.try_get::<i64, _>(0)
                .ok()
                .flatten()
                .and_then(|v| i32::try_from(v).ok())
        })
        .unwrap_or(0)
}

async fn try_exec(client: &mut Client<tokio_util::compat::Compat<TcpStream>>, sql: &str) {
    if let Ok(stream) = client.simple_query(sql).await {
        let _ = stream.into_first_result().await;
    }
}

async fn wait_capture_instance(
    client: &mut Client<tokio_util::compat::Compat<TcpStream>>,
    min: i32,
) {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let rows = client
            .simple_query("SELECT COUNT(*) FROM cdc.change_tables")
            .await
            .unwrap()
            .into_first_result()
            .await
            .unwrap();
        let count = rows.first().map(count_i32).unwrap_or(0);
        if count >= min {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "capture instance was not created"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn wait_max_lsn(client: &mut Client<tokio_util::compat::Compat<TcpStream>>) {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        try_exec(client, "EXEC sys.sp_cdc_scan").await;
        let rows = match client.simple_query("SELECT sys.fn_cdc_get_max_lsn()").await {
            Ok(stream) => stream.into_first_result().await.unwrap_or_default(),
            Err(_) => Vec::new(),
        };
        let ready = rows.first().is_some_and(|row| {
            row.try_get::<&[u8], _>(0)
                .ok()
                .flatten()
                .is_some_and(|bytes| bytes.len() == 10 && bytes.iter().any(|b| *b != 0))
        });
        if ready {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "CDC max LSN was not available"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[test]
#[ignore = "requires the isolated database created by scripts/test-mssql-container.sh"]
fn snapshot_lsn_stream_resume_capture_switch_and_retention() {
    assert_eq!(std::env::var("HASHI_MSSQL_DISPOSABLE").as_deref(), Ok("1"));
    let port = std::env::var("HASHI_MSSQL_TEST_PORT")
        .unwrap()
        .parse::<u16>()
        .unwrap();
    let database = format!("hashi_fixture_{}", std::process::id());
    let path = std::env::temp_dir().join(format!(
        "hashi-mssql-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let mut client = connect(port, None).await;
        exec(&mut client, &format!("CREATE DATABASE [{database}]")).await;
        exec(
            &mut client,
            &format!("ALTER DATABASE [{database}] SET ALLOW_SNAPSHOT_ISOLATION ON"),
        )
        .await;
        exec(
            &mut client,
            &format!("ALTER DATABASE [{database}] SET READ_COMMITTED_SNAPSHOT ON"),
        )
        .await;
        drop(client);
        let mut client = connect(port, Some(&database)).await;
        exec(
            &mut client,
            "CREATE TABLE dbo.items (id INT NOT NULL PRIMARY KEY, value NVARCHAR(100) NOT NULL)",
        )
        .await;
        exec(
            &mut client,
            "INSERT INTO dbo.items (id, value) VALUES (1, N'one'), (2, N'two'), (3, N'three')",
        )
        .await;
        wait_agent(&mut client).await;
        exec(&mut client, "EXEC sys.sp_cdc_enable_db").await;
        exec(
            &mut client,
            "EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', @source_name=N'items', @role_name=NULL, @supports_net_changes=0",
        )
        .await;
        wait_capture_instance(&mut client, 1).await;
        wait_max_lsn(&mut client).await;
    });
    let mut source =
        MssqlConnector::connect(config("127.0.0.1", port, &database, path.clone())).unwrap();
    let report = source.check().unwrap();
    assert!(report.ok, "{}", report.message);
    let first = source.read(None, None).unwrap().unwrap();
    assert_eq!(first.key, "[1]");
    let second = next(&mut source, &first).unwrap();
    drop(source);
    let mut source =
        MssqlConnector::connect(config("127.0.0.1", port, &database, path.clone())).unwrap();
    assert_eq!(next(&mut source, &first).unwrap(), second);
    let third = next(&mut source, &second).unwrap();
    assert_eq!(third.key, "[3]");
    runtime.block_on(async {
        let mut client = connect(port, Some(&database)).await;
        exec(&mut client, "BEGIN TRANSACTION").await;
        exec(
            &mut client,
            "UPDATE dbo.items SET value=N'updated' WHERE id=1",
        )
        .await;
        exec(&mut client, "DELETE FROM dbo.items WHERE id=2").await;
        exec(
            &mut client,
            "INSERT INTO dbo.items (id, value) VALUES (4, N'four')",
        )
        .await;
        exec(&mut client, "COMMIT").await;
        try_exec(&mut client, "EXEC sys.sp_cdc_scan").await;
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
    let mut seen = vec![
        begin
            .source
            .as_ref()
            .unwrap()
            .change
            .as_ref()
            .unwrap()
            .operation
            .clone(),
    ];
    let mut last = begin;
    let mut updated = None;
    for _ in 0..8 {
        last = next(&mut source, &last).unwrap();
        let change = last.source.as_ref().unwrap().change.as_ref().unwrap();
        seen.push(change.operation.clone());
        if change.operation == Operation::Update {
            updated = Some(change.before_image.clone());
        }
        if seen.contains(&Operation::TransactionEnd)
            && seen.contains(&Operation::Update)
            && seen.contains(&Operation::Delete)
            && seen.contains(&Operation::Insert)
        {
            break;
        }
    }
    assert!(seen.contains(&Operation::Update));
    assert!(seen.contains(&Operation::Delete));
    assert!(seen.contains(&Operation::Insert));
    assert!(seen.contains(&Operation::TransactionEnd));
    assert!(
        updated
            .as_ref()
            .is_some_and(|image| image.as_ref().is_some_and(|text| text.contains("one"))),
        "CDC update must carry the before image"
    );
    let end = last;
    runtime.block_on(async {
        let mut client = connect(port, Some(&database)).await;
        exec(&mut client, "ALTER TABLE dbo.items ADD extra INT NOT NULL CONSTRAINT df_items_extra DEFAULT 0").await;
        exec(
            &mut client,
            "EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', @source_name=N'items', @role_name=NULL, @capture_instance=N'dbo_items_v2', @supports_net_changes=0",
        )
        .await;
        wait_capture_instance(&mut client, 2).await;
        wait_max_lsn(&mut client).await;
        exec(&mut client, "INSERT INTO dbo.items (id, value, extra) VALUES (5, N'five', 9)").await;
        try_exec(&mut client, "EXEC sys.sp_cdc_scan").await;
    });
    let mut last = end;
    let mut found_five = false;
    for _ in 0..16 {
        last = next(&mut source, &last).unwrap();
        if last.key == "[5]" {
            found_five = true;
            break;
        }
    }
    assert!(found_five);
    let discovered = source.discover().unwrap();
    assert!(
        discovered
            .iter()
            .any(|kind| kind.kind == "dbo.items" && kind.schema.fields.contains_key("extra"))
    );
    drop(source);
    let expired_dir = std::env::temp_dir().join(format!(
        "hashi-mssql-expired-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let error = MssqlConnector::resume(
        config("127.0.0.1", port, &database, expired_dir),
        1,
        "00000000000000000000",
    )
    .and_then(|mut source| source.read(Some(1), Some("00000000000000000000")))
    .unwrap_err();
    assert!(
        error.message.contains("retention expired"),
        "{}",
        error.message
    );
    let mut short = config("127.0.0.1", port, &database, path);
    short.max_lag = Duration::from_secs(60 * 60 * 24 * 365);
    let source = MssqlConnector::connect(short).unwrap();
    let report = source.check().unwrap();
    assert!(!report.ok, "{}", report.message);
    assert!(report.message.contains("retention"));
}
