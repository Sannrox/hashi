//! Consistent staged snapshots and GTID-ordered row change capture.
pub mod historian;
mod schema;
mod state;
mod statement;
use crate::schema::{FieldType, Schema};
use crate::{
    Change, CheckReport, Connector, DiscoveredKind, Error, Operation, Position, Record,
    SourceMetadata,
};
use futures_util::StreamExt;
use mysql_async::binlog::events::{EventData, GtidEvent};
use mysql_async::{
    BinlogStream, BinlogStreamRequest, Conn, Opts, Params, Value, prelude::Queryable,
};
use schema::{ident, qualified};
use state::{Batch, Column, Layout, State, add_gtid, parse_gtids};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::{Path, PathBuf};
use std::time::Duration;
const TRANSACTIONS: &str = "__mysql_transaction";

#[derive(Clone, serde::Serialize)]
pub struct Table {
    pub database: String,
    pub table: String,
    /// Empty selects the primary key. An explicit key must be a complete non-null unique index.
    pub key: Vec<String>,
}
pub struct Config {
    pub id: String,
    pub options: Opts,
    pub tables: Vec<Table>,
    pub state_dir: PathBuf,
    pub chunk_rows: usize,
    /// Bounds each completely staged snapshot or transaction, before any record is exposed.
    pub max_batch_bytes: usize,
    pub server_id: u32,
    pub schema_version: u64,
    pub timeout: Duration,
    pub allow_insecure_lab: bool,
}
pub struct MySqlConnector {
    config: Config,
    runtime: Option<tokio::runtime::Runtime>,
    state: State,
    pending: VecDeque<Record>,
    stream: Option<BinlogStream>,
    failed: bool,
    last_resume: Option<(Position, String)>,
    _lease: std::fs::File,
}
impl MySqlConnector {
    /// Synchronous API; embed on a dedicated OS thread, outside a Tokio runtime.
    pub fn connect(config: Config) -> Result<Self, Error> {
        Self::open(config, None)
    }

    /// Opens the stream at a committed stream boundary instead of a spool:
    /// `position` and `gtids` as a record at a transaction end (or the end
    /// of the snapshot) carried them. Nothing is snapshotted; table layouts
    /// are read as they are now. The one-shot outbox mode uses this, where
    /// the host's committed cursor is the only state across runs. `state_dir`
    /// must not hold another spool.
    pub fn resume(config: Config, position: Position, gtids: &str) -> Result<Self, Error> {
        parse_gtids(gtids)?;
        Self::open(config, Some((position, gtids.to_owned())))
    }

    /// The last position staged when the stream opened: after a fresh
    /// snapshot, its final record.
    pub fn opened_at(&self) -> Position {
        self.state.high_water
    }

    /// The server's executed GTID set now: what a one-shot run reads up to.
    pub fn executed_gtids(&self) -> Result<String, Error> {
        self.query_text("SELECT @@GLOBAL.gtid_executed", Vec::new())
    }

    /// Whether `cursor` (a record's GTID set) contains `target`.
    pub fn reached(&self, cursor: &str, target: &str) -> Result<bool, Error> {
        Ok(self.query_text(
            "SELECT CAST(GTID_SUBSET(?, ?) AS CHAR)",
            vec![target.into(), cursor.into()],
        )? == "1")
    }

    fn query_text(&self, sql: &str, params: Vec<Value>) -> Result<String, Error> {
        let runtime = self
            .runtime
            .as_ref()
            .ok_or_else(|| Error::new("MySQL runtime closed"))?;
        runtime.block_on(async {
            tokio::time::timeout(self.config.timeout, async {
                let mut conn = Self::connection(&self.config).await?;
                let value: Option<String> = conn
                    .exec_first(sql, Params::Positional(params))
                    .await
                    .map_err(sql_error)?;
                conn.disconnect().await.map_err(sql_error)?;
                value.ok_or_else(|| Error::new("MySQL query returned nothing"))
            })
            .await
            .map_err(|_| Error::new("MySQL query timed out"))?
        })
    }

    fn open(config: Config, boundary: Option<(Position, String)>) -> Result<Self, Error> {
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err(Error::new(
                "call MySQL synchronous API outside an async runtime",
            ));
        }
        if config.id.trim().is_empty()
            || config.tables.is_empty()
            || config.tables.len() > 256
            || config.chunk_rows == 0
            || config.chunk_rows > 10000
            || config.max_batch_bytes == 0
            || config.server_id == 0
            || config.schema_version == 0
            || config.timeout.is_zero()
            || config.tables.iter().any(|t| {
                [&t.database, &t.table]
                    .iter()
                    .any(|name| name.is_empty() || name.len() > 64 || name.contains('\0'))
            })
            || config
                .tables
                .iter()
                .map(|t| format!("{}.{}", t.database, t.table))
                .collect::<BTreeSet<_>>()
                .len()
                != config.tables.len()
        {
            return Err(Error::new("invalid MySQL connector configuration"));
        }
        if !config.allow_insecure_lab
            && config.options.ssl_opts().is_none_or(|ssl| {
                ssl.accept_invalid_certs()
                    || ssl.skip_domain_validation()
                    || ssl.tls_hostname_override().is_some()
            })
        {
            return Err(Error::new(
                "MySQL requires verified TLS; plaintext is restricted to explicit lab configuration",
            ));
        }
        std::fs::create_dir_all(&config.state_dir)?;
        let lease = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(config.state_dir.join("mysql.lease"))?;
        lease
            .try_lock()
            .map_err(|_| Error::new("MySQL source spool is already owned"))?;
        let fingerprint = serde_json::json!([
            config.id,
            config.options.ip_or_hostname(),
            config.options.tcp_port(),
            config.tables,
            config.schema_version
        ])
        .to_string();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()?;
        let loaded = state::load(&config.state_dir, &fingerprint)?;
        let state = if let Some((position, gtids)) = boundary {
            if loaded.is_some() {
                return Err(Error::new(
                    "MySQL resume needs an empty state directory; it already holds a spool",
                ));
            }
            let layouts = runtime
                .block_on(async {
                    tokio::time::timeout(config.timeout, async {
                        let mut conn = Self::connection(&config).await?;
                        let layouts =
                            schema::layouts(&mut conn, &config.tables, config.schema_version)
                                .await?;
                        conn.disconnect().await.map_err(sql_error)?;
                        Ok::<_, Error>(layouts)
                    })
                    .await
                })
                .map_err(|_| Error::new("MySQL resume timed out reading table layouts"))??;
            let state = State {
                version: 1,
                fingerprint,
                high_water: position,
                gtids,
                layouts,
                batch: None,
            };
            state::save(&config.state_dir, &state)?;
            state
        } else if let Some(state) = loaded {
            state
        } else {
            runtime
                .block_on(async {
                    tokio::time::timeout(config.timeout, Self::snapshot(&config, fingerprint)).await
                })
                .map_err(|_| Error::new("MySQL snapshot timed out before publication"))??
        };
        let pending = state::recover_batch(&config.state_dir, &state, config.max_batch_bytes)?;
        Ok(Self {
            config,
            runtime: Some(runtime),
            state,
            pending,
            stream: None,
            failed: false,
            last_resume: None,
            _lease: lease,
        })
    }

    async fn connection(config: &Config) -> Result<Conn, Error> {
        let mut conn = Conn::new(config.options.clone()).await.map_err(sql_error)?;
        conn.query_drop("SET SESSION time_zone='+00:00'")
            .await
            .map_err(sql_error)?;
        let variables: Option<(String,String,String,String,String)> = conn.query_first("SELECT @@GLOBAL.gtid_mode,@@GLOBAL.enforce_gtid_consistency,@@GLOBAL.binlog_format,@@GLOBAL.binlog_row_image,@@GLOBAL.binlog_row_metadata").await.map_err(sql_error)?;
        if !matches!(variables,Some((ref g,ref c,ref f,ref i,ref m)) if g=="ON" && c=="ON" && f=="ROW" && i=="FULL" && m=="FULL")
        {
            return Err(Error::new(
                "MySQL requires GTID mode, GTID consistency, ROW binlog format, FULL row images and FULL row metadata",
            ));
        }
        Ok(conn)
    }
    async fn snapshot(config: &Config, fingerprint: String) -> Result<State, Error> {
        let mut lock = Self::connection(config).await?;
        lock.query_drop("FLUSH TABLES WITH READ LOCK")
            .await
            .map_err(|_| Error::new("MySQL snapshot requires RELOAD or FLUSH_TABLES privilege"))?;
        let mut conn = Self::connection(config).await?;
        conn.query_drop("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .await
            .map_err(sql_error)?;
        conn.query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT")
            .await
            .map_err(sql_error)?;
        let gtids: String = lock
            .query_first("SELECT @@GLOBAL.gtid_executed")
            .await
            .map_err(sql_error)?
            .ok_or_else(|| Error::new("MySQL snapshot GTID set missing"))?;
        parse_gtids(&gtids)?;
        let layouts = schema::layouts(&mut conn, &config.tables, config.schema_version).await?;
        // Each selected table now holds a metadata lock in the consistent transaction.
        lock.query_drop("UNLOCK TABLES").await.map_err(sql_error)?;
        lock.disconnect().await.map_err(sql_error)?;
        let mut records = VecDeque::new();
        let mut bytes = 0;
        let timestamp = now_ms()?;
        let mut position = 0;
        for layout in &layouts {
            let keys: Vec<_> = layout.keys.iter().map(|key| ident(key)).collect();
            let mut last: Option<Vec<Value>> = None;
            loop {
                let predicate = if last.is_some() {
                    format!(
                        " WHERE ({}) > ({})",
                        keys.join(","),
                        vec!["?"; keys.len()].join(",")
                    )
                } else {
                    String::new()
                };
                let sql = format!(
                    "SELECT {} FROM {}{} ORDER BY {} LIMIT {}",
                    layout
                        .columns
                        .iter()
                        .map(|column| ident(&column.name))
                        .collect::<Vec<_>>()
                        .join(","),
                    qualified(&layout.database, &layout.table),
                    predicate,
                    keys.join(","),
                    config.chunk_rows
                );
                let rows: Vec<mysql_async::Row> = conn
                    .exec(sql, Params::Positional(last.clone().unwrap_or_default()))
                    .await
                    .map_err(sql_error)?;
                if rows.is_empty() {
                    break;
                }
                for row in &rows {
                    let values: Vec<_> = (0..row.len())
                        .map(|i| {
                            row.as_ref(i)
                                .cloned()
                                .ok_or_else(|| Error::new("MySQL snapshot row is incomplete"))
                        })
                        .collect::<Result<_, _>>()?;
                    position = next_position(position)?;
                    let record = Record {
                        kind: layout.kind(),
                        key: schema::key(layout, &values)?,
                        source_ts_ms: timestamp,
                        ingest_ts_ms: 0,
                        quality: None,
                        position,
                        payload: schema::object(layout, &values)?.to_string(),
                        source: Some(SourceMetadata {
                            cursor: Some(gtids.clone()),
                            change: Some(Change {
                                operation: Operation::Insert,
                                before_image: None,
                                transaction_id: None,
                                commit_ts_ms: None,
                            }),
                        }),
                    };
                    append(&mut records, &mut bytes, config.max_batch_bytes, record)?;
                    last = Some(
                        layout
                            .keys
                            .iter()
                            .map(|key| {
                                let index = layout
                                    .columns
                                    .iter()
                                    .position(|c| &c.name == key)
                                    .ok_or_else(|| Error::new("MySQL key column missing"))?;
                                Ok(values[index].clone())
                            })
                            .collect::<Result<_, Error>>()?,
                    );
                }
                if rows.len() < config.chunk_rows {
                    break;
                }
            }
        }
        conn.query_drop("COMMIT").await.map_err(sql_error)?;
        conn.disconnect().await.map_err(sql_error)?;
        // An empty snapshot still emits a durable boundary, so its GTID becomes checkpointed.
        if records.is_empty() {
            position = 1;
            append(
                &mut records,
                &mut bytes,
                config.max_batch_bytes,
                marker(
                    position,
                    &gtids,
                    "snapshot",
                    Operation::TransactionEnd,
                    timestamp,
                ),
            )?;
        }
        let state = State {
            version: 1,
            fingerprint,
            high_water: position,
            gtids,
            layouts,
            batch: Some(Batch {
                first: 1,
                last: position,
                cursor_before: String::new(),
            }),
        };
        write_batch(config, &state, &records)?;
        Ok(state)
    }

    fn read_inner(
        &mut self,
        from: Option<Position>,
        cursor: Option<&str>,
    ) -> Result<Option<Record>, Error> {
        if self.failed {
            return Err(Error::new(
                "MySQL reader failed; restart with its durable state to recover",
            ));
        }
        let committed = from.unwrap_or(0);
        if committed > self.state.high_water {
            return Err(Error::new("MySQL checkpoint is ahead of the source spool"));
        }
        if let Some(batch) = &self.state.batch {
            if committed < batch.first.saturating_sub(1) {
                return Err(Error::new(
                    "MySQL checkpoint predates retained source records",
                ));
            }
            let expected = if committed < batch.first {
                Some(batch.cursor_before.as_str())
            } else {
                self.pending
                    .iter()
                    .find(|r| r.position == committed)
                    .and_then(|r| r.source.as_ref())
                    .and_then(|s| s.cursor.as_deref())
                    .or_else(|| {
                        self.last_resume
                            .as_ref()
                            .filter(|(position, _)| *position == committed)
                            .map(|(_, cursor)| cursor.as_str())
                    })
            };
            if expected.is_none_or(|expected| cursor.unwrap_or("") != expected) {
                return Err(Error::new("MySQL position and source cursor do not match"));
            }
        } else if committed != self.state.high_water || cursor != Some(self.state.gtids.as_str()) {
            return Err(Error::new(
                "MySQL checkpoint does not match retained stream boundary",
            ));
        }
        self.last_resume = Some((committed, cursor.unwrap_or("").to_string()));
        while self
            .pending
            .front()
            .is_some_and(|record| record.position <= committed)
        {
            self.pending.pop_front();
        }
        if let Some(record) = self.pending.front() {
            return Ok(Some(record.clone()));
        }
        if let Some(batch) = self.state.batch.take() {
            state::save(&self.config.state_dir, &self.state)?;
            std::fs::remove_file(state::batch_path(&self.config.state_dir, &batch))?;
        }
        let runtime = self
            .runtime
            .take()
            .ok_or_else(|| Error::new("MySQL runtime closed"))?;
        let timeout = self.config.timeout;
        let result = runtime.block_on(async {
            tokio::time::timeout(timeout, self.transaction())
                .await
                .map_err(|_| {
                    Error::new("MySQL stream operation timed out; restart from the durable cursor")
                })?
        });
        self.runtime = Some(runtime);
        if result.is_err() {
            self.failed = true;
        }
        result?;
        Ok(self.pending.front().cloned())
    }

    async fn transaction(&mut self) -> Result<(), Error> {
        if self.stream.is_none() {
            let mut conn = Self::connection(&self.config).await?;
            let purged: String = conn
                .query_first("SELECT @@GLOBAL.gtid_purged")
                .await
                .map_err(sql_error)?
                .ok_or_else(|| Error::new("MySQL purged GTID set missing"))?;
            let retained: Option<u8> = conn
                .exec_first(
                    "SELECT GTID_SUBSET(?, ?) AND GTID_SUBSET(?, @@GLOBAL.gtid_executed)",
                    (&purged, &self.state.gtids, &self.state.gtids),
                )
                .await
                .map_err(sql_error)?;
            if retained != Some(1) {
                return Err(Error::new(
                    "MySQL binlog retention expired; an explicit new snapshot and consumer recovery are required",
                ));
            }
            let heartbeat_ns = (self.config.timeout.as_nanos() / 2).clamp(1, 1_000_000_000) as u64;
            conn.exec_drop("SET @master_heartbeat_period=?", (heartbeat_ns,))
                .await
                .map_err(sql_error)?;
            self.stream = Some(
                conn.get_binlog_stream(
                    BinlogStreamRequest::new(self.config.server_id)
                        .with_gtid()
                        .with_gtid_set(parse_gtids(&self.state.gtids)?),
                )
                .await
                .map_err(sql_error)?,
            );
        }
        let mut records = VecDeque::new();
        let mut bytes = 0;
        let mut transaction: Option<GtidEvent> = None;
        let mut position = self.state.high_water;
        loop {
            let stream = self
                .stream
                .as_mut()
                .ok_or_else(|| Error::new("MySQL binlog stream missing"))?;
            let event = tokio::time::timeout(self.config.timeout, stream.next())
                .await
                .map_err(|_| {
                    Error::new("MySQL binlog read timed out; restart from the durable cursor")
                })?
                .ok_or_else(|| Error::new("MySQL binlog stream ended"))?
                .map_err(sql_error)?;
            let data = event
                .read_data()
                .map_err(|_| Error::new("MySQL binlog event decoding failed"))?
                .ok_or_else(|| Error::new("MySQL binlog event has no data"))?;
            let mut complete = false;
            let mut ddl = false;
            match data {
                EventData::GtidEvent(gtid) => {
                    if transaction.is_some() {
                        return Err(Error::new("MySQL nested GTID transaction"));
                    }
                    let id = gtid_id(&gtid);
                    let ts = (gtid.immediate_commit_timestamp() / 1000) as i64;
                    position = next_position(position)?;
                    append(
                        &mut records,
                        &mut bytes,
                        self.config.max_batch_bytes,
                        marker(
                            position,
                            &self.state.gtids,
                            &id,
                            Operation::TransactionBegin,
                            ts,
                        ),
                    )?;
                    transaction = Some(gtid);
                }
                EventData::RowsEvent(rows) => {
                    let gtid = transaction
                        .as_ref()
                        .ok_or_else(|| Error::new("MySQL row event outside a GTID transaction"))?;
                    let map = stream
                        .get_tme(rows.table_id())
                        .ok_or_else(|| Error::new("MySQL row table map missing"))?;
                    let layout = self.state.layouts.iter().find(|layout| {
                        layout.database.as_bytes() == map.database_name_raw()
                            && layout.table.as_bytes() == map.table_name_raw()
                    });
                    if let Some(layout) = layout {
                        schema::validate_map(layout, map)?;
                        let labels = schema::labels(map)?;
                        for pair in rows.rows(map) {
                            let (before, after) =
                                pair.map_err(|_| Error::new("MySQL row decoding failed"))?;
                            let before = binlog_row(before, &labels)?;
                            let after = binlog_row(after, &labels)?;
                            let operation = match (&before, &after) {
                                (None, Some(_)) => Operation::Insert,
                                (Some(_), None) => Operation::Delete,
                                (Some(_), Some(_)) => Operation::Update,
                                _ => return Err(Error::new("MySQL empty row change")),
                            };
                            let values = after
                                .as_ref()
                                .or(before.as_ref())
                                .ok_or_else(|| Error::new("MySQL row image missing"))?;
                            // A changed primary key retains its old identity in the before image;
                            // consumers own remapping, as for every other source identity change.
                            position = next_position(position)?;
                            let record = Record {
                                kind: layout.kind(),
                                key: schema::key(layout, values)?,
                                position,
                                source_ts_ms: (gtid.immediate_commit_timestamp() / 1000) as i64,
                                ingest_ts_ms: 0,
                                quality: None,
                                payload: if after.is_some() {
                                    schema::object(layout, values)?.to_string()
                                } else {
                                    "null".into()
                                },
                                source: Some(SourceMetadata {
                                    cursor: Some(self.state.gtids.clone()),
                                    change: Some(Change {
                                        operation,
                                        before_image: before
                                            .as_ref()
                                            .map(|v| {
                                                schema::object(layout, v).map(|o| o.to_string())
                                            })
                                            .transpose()?,
                                        transaction_id: Some(gtid_id(gtid)),
                                        commit_ts_ms: Some(
                                            (gtid.immediate_commit_timestamp() / 1000) as i64,
                                        ),
                                    }),
                                }),
                            };
                            append(
                                &mut records,
                                &mut bytes,
                                self.config.max_batch_bytes,
                                record,
                            )?;
                        }
                    }
                }
                EventData::XidEvent(_) => complete = true,
                EventData::QueryEvent(query) => match statement::classify(&query.query())? {
                    statement::Statement::Begin => {}
                    statement::Statement::Commit => complete = true,
                    // Its own transaction with no row events (#49). It rewrites
                    // the grant tables without row events too, so capturing
                    // those keeps the recovery gate.
                    statement::Statement::Account if captures_grant_tables(&self.config.tables) => {
                        return Err(Error::new(
                            "MySQL account statement changes captured grant tables without row events; explicit recovery required",
                        ));
                    }
                    statement::Statement::Account => complete = true,
                    statement::Statement::Schema { altering } => {
                        // Table maps prove primary keys but omit secondary unique indexes.
                        if altering && self.config.tables.iter().any(|table| !table.key.is_empty())
                        {
                            return Err(Error::new(
                                "MySQL ALTER with configured keys requires explicit schema recovery",
                            ));
                        }
                        complete = true;
                        ddl = true;
                    }
                },
                EventData::HeartbeatEvent if transaction.is_none() => return Ok(()),
                EventData::HeartbeatEvent => {}
                EventData::FormatDescriptionEvent(_)
                | EventData::RotateEvent(_)
                | EventData::PreviousGtidsEvent(_)
                | EventData::TableMapEvent(_)
                | EventData::RowsQueryEvent(_) => {}
                _ => {
                    return Err(Error::new(
                        "MySQL unsupported binlog event; refusing to skip source changes",
                    ));
                }
            }
            if complete {
                let gtid = transaction
                    .take()
                    .ok_or_else(|| Error::new("MySQL commit without GTID"))?;
                let gtids = add_gtid(&self.state.gtids, &gtid)?;
                position = next_position(position)?;
                append(
                    &mut records,
                    &mut bytes,
                    self.config.max_batch_bytes,
                    marker(
                        position,
                        &gtids,
                        &gtid_id(&gtid),
                        Operation::TransactionEnd,
                        (gtid.immediate_commit_timestamp() / 1000) as i64,
                    ),
                )?;
                let mut next = self.state.clone();
                if ddl {
                    let mut conn = Self::connection(&self.config).await?;
                    let mut current =
                        schema::layouts(&mut conn, &self.config.tables, self.config.schema_version)
                            .await?;
                    conn.disconnect().await.map_err(sql_error)?;
                    for (current, previous) in current.iter_mut().zip(&self.state.layouts) {
                        current.version = previous.version;
                        if current != previous {
                            current.version = previous
                                .version
                                .checked_add(1)
                                .ok_or_else(|| Error::new("MySQL schema version exhausted"))?;
                        }
                    }
                    next.layouts = current;
                }
                next.batch = Some(Batch {
                    first: self.state.high_water + 1,
                    last: position,
                    cursor_before: self.state.gtids.clone(),
                });
                next.high_water = position;
                next.gtids = gtids;
                write_batch(&self.config, &next, &records)?;
                self.state = next;
                self.pending = records;
                return Ok(());
            }
        }
    }
}
impl Connector for MySqlConnector {
    fn id(&self) -> &str {
        &self.config.id
    }
    fn check(&self) -> Result<CheckReport, Error> {
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err(Error::new(
                "call MySQL synchronous API outside an async runtime",
            ));
        }
        if self.failed {
            return Ok(CheckReport {
                ok: false,
                message: "reader failed; durable restart required".into(),
            });
        }
        let runtime = self
            .runtime
            .as_ref()
            .ok_or_else(|| Error::new("MySQL runtime closed"))?;
        runtime.block_on(async {
            tokio::time::timeout(self.config.timeout, async {
                let mut conn = Self::connection(&self.config).await?;
                let roles: Vec<(String, String)> = conn
                    .query("SELECT ROLE_NAME,ROLE_HOST FROM information_schema.ENABLED_ROLES")
                    .await
                    .map_err(sql_error)?;
                let using = if roles.is_empty() {
                    String::new()
                } else {
                    format!(
                        " USING {}",
                        roles
                            .iter()
                            .map(|(name, host)| format!("{}@{}", ident(name), ident(host)))
                            .collect::<Vec<_>>()
                            .join(",")
                    )
                };
                let grants: Vec<String> = conn
                    .query(format!("SHOW GRANTS FOR CURRENT_USER{using}"))
                    .await
                    .map_err(sql_error)?;
                let global: BTreeSet<&str> = grants
                    .iter()
                    .filter_map(|grant| {
                        let (privileges, scope) = grant.split_once(" ON ")?;
                        let (scope, _) = scope.split_once(" TO ")?;
                        if scope == "*.*" {
                            privileges.strip_prefix("GRANT ")
                        } else {
                            None
                        }
                    })
                    .flat_map(|privileges| privileges.split(',').map(str::trim))
                    .collect();
                let all = global.contains("ALL PRIVILEGES");
                let mut missing = Vec::new();
                if !all && !global.contains("REPLICATION SLAVE") {
                    missing.push("REPLICATION SLAVE");
                }
                if !all && !global.contains("REPLICATION CLIENT") {
                    missing.push("REPLICATION CLIENT");
                }
                if !all && !global.contains("RELOAD") && !global.contains("FLUSH_TABLES") {
                    missing.push("RELOAD or FLUSH_TABLES");
                }
                for table in &self.config.tables {
                    conn.query_drop(format!(
                        "SELECT * FROM {} LIMIT 0",
                        qualified(&table.database, &table.table)
                    ))
                    .await
                    .map_err(sql_error)?;
                }
                conn.disconnect().await.map_err(sql_error)?;
                Ok(CheckReport {
                    ok: missing.is_empty(),
                    message: if missing.is_empty() {
                        "row-based GTID source and required privileges verified".into()
                    } else {
                        format!("missing MySQL privileges: {}", missing.join(", "))
                    },
                })
            })
            .await
            .map_err(|_| Error::new("MySQL check timed out"))?
        })
    }
    fn discover(&self) -> Result<Vec<DiscoveredKind>, Error> {
        let mut kinds: Vec<_> = self.state.layouts.iter().map(Layout::discovered).collect();
        kinds.push(DiscoveredKind {
            kind: TRANSACTIONS.into(),
            schema: Schema {
                version: 1,
                key_fields: vec![],
                fields: BTreeMap::from([("transaction_id".into(), FieldType::Text)]),
            },
            metadata: BTreeMap::new(),
        });
        Ok(kinds)
    }
    fn read(
        &mut self,
        from: Option<Position>,
        cursor: Option<&str>,
    ) -> Result<Option<Record>, Error> {
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err(Error::new(
                "call MySQL synchronous API outside an async runtime",
            ));
        }
        let result = self.read_inner(from, cursor);
        if result.is_err() {
            self.failed = true;
        }
        result
    }
}
impl Drop for MySqlConnector {
    fn drop(&mut self) {
        self.stream.take();
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}
fn append(
    records: &mut VecDeque<Record>,
    bytes: &mut usize,
    max: usize,
    record: Record,
) -> Result<(), Error> {
    *bytes = bytes.saturating_add(
        crate::delivery::encode(&record, &record.idempotency_key("mysql", "spool")).len(),
    );
    if *bytes > max {
        return Err(Error::new(
            "MySQL staged batch exceeds configured byte bound",
        ));
    }
    records.push_back(record);
    Ok(())
}
fn write_batch(config: &Config, state: &State, records: &VecDeque<Record>) -> Result<(), Error> {
    let batch = state
        .batch
        .as_ref()
        .ok_or_else(|| Error::new("MySQL batch manifest missing"))?;
    let text: String = records
        .iter()
        .map(|record| crate::delivery::encode(record, &record.idempotency_key("mysql", "spool")))
        .collect();
    crate::checkpoint::atomic_write(&state::batch_path(&config.state_dir, batch), &text)?;
    state::save(&config.state_dir, state)
}
fn marker(
    position: Position,
    cursor: &str,
    id: &str,
    operation: Operation,
    timestamp: i64,
) -> Record {
    Record {
        kind: TRANSACTIONS.into(),
        key: id.into(),
        position,
        source_ts_ms: timestamp,
        ingest_ts_ms: 0,
        quality: None,
        payload: serde_json::json!({"transaction_id":id}).to_string(),
        source: Some(SourceMetadata {
            cursor: Some(cursor.into()),
            change: Some(Change {
                operation,
                before_image: None,
                transaction_id: Some(id.into()),
                commit_ts_ms: Some(timestamp),
            }),
        }),
    }
}
fn next_position(position: Position) -> Result<Position, Error> {
    position
        .checked_add(1)
        .ok_or_else(|| Error::new("MySQL position exhausted"))
}
fn now_ms() -> Result<i64, Error> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|d| i64::try_from(d.as_millis()).ok())
        .ok_or_else(|| Error::new("MySQL clock is out of range"))
}
fn sql_error(error: mysql_async::Error) -> Error {
    match error {
        mysql_async::Error::Server(server) if matches!(server.code, 1044 | 1045 | 1142 | 1227) => {
            Error::new(
                "MySQL authorization failed; verify SELECT, RELOAD/FLUSH_TABLES, REPLICATION CLIENT and REPLICATION SLAVE privileges",
            )
        }
        mysql_async::Error::Server(server) if server.code == 1236 => Error::new(
            "MySQL binlog position unavailable; explicit re-snapshot and consumer recovery required",
        ),
        _ => Error::new("MySQL source operation failed"),
    }
}
fn gtid_id(gtid: &GtidEvent) -> String {
    let uuid = gtid.sid();
    let hex: String = uuid
        .iter()
        .enumerate()
        .map(|(i, b)| {
            format!(
                "{}{b:02x}",
                if matches!(i, 4 | 6 | 8 | 10) { "-" } else { "" }
            )
        })
        .collect();
    match gtid.tag() {
        Some(tag) => format!("{hex}:{tag}:{}", gtid.gno()),
        None => format!("{hex}:{}", gtid.gno()),
    }
}
fn binlog_row(
    row: Option<mysql_async::binlog::row::BinlogRow>,
    labels: &BTreeMap<usize, schema::Labels>,
) -> Result<Option<Vec<Value>>, Error> {
    row.map(|row| {
        (0..row.len())
            .map(|index| {
                row.as_ref(index)
                    .cloned()
                    .ok_or_else(|| Error::new("MySQL partial row image is unsupported"))
                    .and_then(|value| {
                        Value::try_from(value)
                            .map_err(|_| Error::new("MySQL row value conversion failed"))
                    })
                    .and_then(|value| schema::category(value, labels.get(&index)))
            })
            .collect()
    })
    .transpose()
}

/// Whether capture includes the `mysql` system schema, whose grant tables
/// account statements change without row events (#49).
fn captures_grant_tables(tables: &[Table]) -> bool {
    tables
        .iter()
        .any(|table| table.database.eq_ignore_ascii_case("mysql"))
}

#[cfg(test)]
mod tests {
    use super::{Table, captures_grant_tables};

    fn table(database: &str) -> Table {
        Table {
            database: database.into(),
            table: "t".into(),
            key: Vec::new(),
        }
    }

    /// Account statements are skipped only when no captured table is a grant
    /// table they could change without row events (#49).
    #[test]
    fn grant_tables_in_capture_are_detected() {
        assert!(!captures_grant_tables(&[table("shop")]));
        assert!(captures_grant_tables(&[table("shop"), table("mysql")]));
        assert!(captures_grant_tables(&[table("MySQL")]));
    }
}
