//! Snapshot plus LSN-ordered change capture, with change tracking as fallback.
mod schema;
mod state;
use crate::schema::{FieldType, Schema};
use crate::{
    Change, CheckReport, Connector, DiscoveredKind, Error, Operation, Position, Record,
    SourceMetadata,
};
use schema::{
    Cell, capture_function, captured_columns, cells, ident, key, layouts, object, qualified, rows,
};
use state::{
    Batch, CaptureInstance, Layout, Mode, State, format_lsn, parse_lsn, parse_version,
    select_instance,
};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tiberius::{AuthMethod, EncryptionLevel};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

const TRANSACTIONS: &str = "__mssql_transaction";
type SqlClient = tiberius::Client<Compat<TcpStream>>;

struct CdcChange {
    lsn: [u8; 10],
    seqval: [u8; 10],
    operation: i32,
    layout: Layout,
    values: Vec<Cell>,
}

struct TrackingChange {
    version: i64,
    op: String,
    layout: Layout,
    values: Vec<Cell>,
}

#[derive(Clone, serde::Serialize)]
pub struct Table {
    pub schema: String,
    pub table: String,
    /// Empty selects the primary key. An explicit key must match that primary key.
    pub key: Vec<String>,
}

pub struct Config {
    pub id: String,
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    pub tables: Vec<Table>,
    pub state_dir: PathBuf,
    pub chunk_rows: usize,
    pub max_batch_bytes: usize,
    pub schema_version: u64,
    pub timeout: Duration,
    /// Cleanup retention must cover this lag; shorter retention fails `check`.
    pub max_lag: Duration,
    pub allow_insecure_lab: bool,
}

pub struct MssqlConnector {
    config: Config,
    runtime: Option<tokio::runtime::Runtime>,
    state: State,
    pending: VecDeque<Record>,
    failed: bool,
    last_resume: Option<(Position, String)>,
    lease: crate::lease::DropRelease,
}

impl MssqlConnector {
    /// Synchronous API; embed on a dedicated OS thread, outside a Tokio runtime.
    pub fn connect(config: Config) -> Result<Self, Error> {
        Self::open(config, None)
    }

    /// Opens at a committed stream boundary instead of a spool: `position` and
    /// `cursor` as a record at a transaction end (or the snapshot end) carried
    /// them. `state_dir` must not hold another spool.
    pub fn resume(config: Config, position: Position, cursor: &str) -> Result<Self, Error> {
        Self::open(config, Some((position, cursor.to_owned())))
    }

    pub fn opened_at(&self) -> Position {
        self.state.high_water
    }

    fn open(config: Config, boundary: Option<(Position, String)>) -> Result<Self, Error> {
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err(Error::new(
                "call SQL Server synchronous API outside an async runtime",
            ));
        }
        if config.id.trim().is_empty()
            || config.host.trim().is_empty()
            || config.port == 0
            || config.user.trim().is_empty()
            || config.password.is_empty()
            || config.database.trim().is_empty()
            || config.tables.is_empty()
            || config.tables.len() > 256
            || config.chunk_rows == 0
            || config.chunk_rows > 10000
            || config.max_batch_bytes == 0
            || config.schema_version == 0
            || config.timeout.is_zero()
            || config.max_lag.is_zero()
            || config.tables.iter().any(|t| {
                [&t.schema, &t.table]
                    .iter()
                    .any(|name| name.is_empty() || name.len() > 128 || name.contains('\0'))
            })
            || config
                .tables
                .iter()
                .map(|t| format!("{}.{}", t.schema, t.table))
                .collect::<BTreeSet<_>>()
                .len()
                != config.tables.len()
        {
            return Err(Error::new("invalid SQL Server connector configuration"));
        }
        std::fs::create_dir_all(&config.state_dir)?;
        let lease = crate::lease::hold(
            &config.state_dir,
            "mssql",
            crate::lease::hold_ttl(),
            "SQL Server source spool is already owned",
        )?;
        let fingerprint = serde_json::json!([
            config.id,
            config.host,
            config.port,
            config.database,
            config.tables,
            config.schema_version
        ])
        .to_string();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()?;
        let loaded = state::load(&config.state_dir, &fingerprint)?;
        let state = if let Some((position, cursor)) = boundary {
            if loaded.is_some() {
                return Err(Error::new(
                    "SQL Server resume needs an empty state directory; it already holds a spool",
                ));
            }
            let (layouts, mode) = runtime
                .block_on(async {
                    tokio::time::timeout(config.timeout, async {
                        let mut client = Self::connection(&config).await?;
                        let mode = detect_mode(&mut client, &config).await?;
                        let mut layouts =
                            layouts(&mut client, &config.tables, config.schema_version).await?;
                        attach_instances(&mut client, &mut layouts).await?;
                        Ok::<_, Error>((layouts, mode))
                    })
                    .await
                })
                .map_err(|_| Error::new("SQL Server resume timed out reading table layouts"))??;
            match mode {
                Mode::Cdc => {
                    parse_lsn(&cursor)?;
                }
                Mode::ChangeTracking => {
                    parse_version(&cursor)?;
                }
            }
            let state = State {
                version: 1,
                fingerprint,
                high_water: position,
                cursor,
                mode,
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
                .map_err(|_| Error::new("SQL Server snapshot timed out before publication"))??
        };
        let pending = state::recover_batch(&config.state_dir, &state, config.max_batch_bytes)?;
        Ok(Self {
            config,
            runtime: Some(runtime),
            state,
            pending,
            failed: false,
            last_resume: None,
            lease,
        })
    }

    async fn connection(config: &Config) -> Result<SqlClient, Error> {
        install_crypto();
        let mut cfg = tiberius::Config::new();
        cfg.host(&config.host);
        cfg.port(config.port);
        cfg.database(&config.database);
        cfg.authentication(AuthMethod::sql_server(&config.user, &config.password));
        if config.allow_insecure_lab {
            cfg.trust_cert();
        } else {
            cfg.encryption(EncryptionLevel::Required);
        }
        let tcp = tokio::time::timeout(config.timeout, TcpStream::connect(cfg.get_addr()))
            .await
            .map_err(|_| Error::new("SQL Server connection timed out"))?
            .map_err(|_| Error::new("SQL Server connection failed"))?;
        tcp.set_nodelay(true)
            .map_err(|_| Error::new("SQL Server connection failed"))?;
        tokio::time::timeout(
            config.timeout,
            tiberius::Client::connect(cfg, tcp.compat_write()),
        )
        .await
        .map_err(|_| Error::new("SQL Server login timed out"))?
        .map_err(sql_error)
    }

    async fn snapshot(config: &Config, fingerprint: String) -> Result<State, Error> {
        let mut client = Self::connection(config).await?;
        let mode = detect_mode(&mut client, config).await?;
        let mut layouts = layouts(&mut client, &config.tables, config.schema_version).await?;
        attach_instances(&mut client, &mut layouts).await?;
        if mode == Mode::Cdc {
            wait_covering_lsn(&mut client, &layouts).await?;
        }
        execute(&mut client, "SET TRANSACTION ISOLATION LEVEL SNAPSHOT")
            .await
            .map_err(|_| {
                Error::new("SQL Server snapshot requires ALLOW_SNAPSHOT_ISOLATION on the database")
            })?;
        execute(&mut client, "BEGIN TRANSACTION").await?;
        let cursor = match mode {
            Mode::Cdc => snapshot_lsn(&mut client, &layouts).await?,
            Mode::ChangeTracking => current_version(&mut client).await?,
        };
        let mut records = VecDeque::new();
        let mut bytes = 0;
        let timestamp = now_ms()?;
        let mut position = 0;
        for layout in &layouts {
            let keys: Vec<_> = layout.keys.iter().map(|k| ident(k)).collect();
            let mut last: Option<Vec<Cell>> = None;
            loop {
                let sql = snapshot_page(layout, &keys, last.as_ref(), config.chunk_rows);
                let page = if let Some(last) = &last {
                    let mut query = tiberius::Query::new(sql);
                    bind_key(&mut query, last)?;
                    rows(&mut client, query).await?
                } else {
                    client
                        .simple_query(sql)
                        .await
                        .map_err(sql_error)?
                        .into_first_result()
                        .await
                        .map_err(sql_error)?
                };
                if page.is_empty() {
                    break;
                }
                for row in &page {
                    let values = cells(row, 0, layout)?;
                    position = next_position(position)?;
                    append(
                        &mut records,
                        &mut bytes,
                        config.max_batch_bytes,
                        Record {
                            kind: layout.kind(),
                            key: key(layout, &values)?,
                            source_ts_ms: timestamp,
                            ingest_ts_ms: 0,
                            quality: None,
                            position,
                            payload: object(layout, &values)?.to_string(),
                            source: Some(SourceMetadata {
                                cursor: Some(cursor.clone()),
                                change: Some(Change {
                                    operation: Operation::Insert,
                                    before_image: None,
                                    transaction_id: None,
                                    commit_ts_ms: None,
                                }),
                            }),
                        },
                    )?;
                    last = Some(
                        layout
                            .keys
                            .iter()
                            .map(|name| {
                                let index = layout
                                    .columns
                                    .iter()
                                    .position(|c| &c.name == name)
                                    .ok_or_else(|| Error::new("SQL Server key column missing"))?;
                                Ok(values[index].clone())
                            })
                            .collect::<Result<_, Error>>()?,
                    );
                }
                if page.len() < config.chunk_rows {
                    break;
                }
            }
        }
        execute(&mut client, "COMMIT").await?;
        if records.is_empty() {
            position = 1;
            append(
                &mut records,
                &mut bytes,
                config.max_batch_bytes,
                marker(
                    position,
                    &cursor,
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
            cursor,
            mode,
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
                "SQL Server reader failed; restart with its durable state to recover",
            ));
        }
        let committed = from.unwrap_or(0);
        if committed > self.state.high_water {
            return Err(Error::new(
                "SQL Server checkpoint is ahead of the source spool",
            ));
        }
        if let Some(batch) = &self.state.batch {
            if committed < batch.first.saturating_sub(1) {
                return Err(Error::new(
                    "SQL Server checkpoint predates retained source records",
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
                return Err(Error::new(
                    "SQL Server position and source cursor do not match",
                ));
            }
        } else if committed != self.state.high_water || cursor != Some(self.state.cursor.as_str()) {
            return Err(Error::new(
                "SQL Server checkpoint does not match retained stream boundary",
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
            .ok_or_else(|| Error::new("SQL Server runtime closed"))?;
        let timeout = self.config.timeout;
        let result = runtime.block_on(async {
            tokio::time::timeout(timeout, self.poll_changes())
                .await
                .map_err(|_| {
                    Error::new(
                        "SQL Server stream operation timed out; restart from the durable cursor",
                    )
                })?
        });
        self.runtime = Some(runtime);
        result?;
        Ok(self.pending.front().cloned())
    }

    async fn poll_changes(&mut self) -> Result<(), Error> {
        let mut client = Self::connection(&self.config).await?;
        match self.state.mode {
            Mode::Cdc => self.poll_cdc(&mut client).await,
            Mode::ChangeTracking => self.poll_tracking(&mut client).await,
        }
    }

    async fn poll_cdc(&mut self, client: &mut SqlClient) -> Result<(), Error> {
        let from = parse_lsn(&self.state.cursor)?;
        let Some(to) = read_max_lsn(client).await? else {
            return Ok(());
        };
        if to <= from {
            return Ok(());
        }
        let mut changes = Vec::new();
        for index in 0..self.state.layouts.len() {
            let instances = capture_instances(client, &self.state.layouts[index]).await?;
            let covering = select_instance(&instances, &from)?;
            let min = instance_min_lsn(client, &covering.name).await?;
            if from < min {
                return Err(Error::new(
                    "SQL Server CDC retention expired; an explicit new snapshot and consumer recovery are required",
                ));
            }
            let mut ordered = instances;
            ordered.sort_by(|a, b| {
                (a.start_lsn, a.create_date_ms, a.name.as_str()).cmp(&(
                    b.start_lsn,
                    b.create_date_ms,
                    b.name.as_str(),
                ))
            });
            for (instance_index, instance) in ordered.iter().enumerate() {
                let next_start = ordered.get(instance_index + 1).map(|next| next.start_lsn);
                if next_start.is_some_and(|start| start <= from) || instance.start_lsn > to {
                    continue;
                }
                follow_instance(client, &mut self.state.layouts[index], instance).await?;
                let layout = self.state.layouts[index].clone();
                let function = capture_function(&instance.name)?;
                let mut query = tiberius::Query::new(format!(
                    "SELECT __$start_lsn, __$seqval, __$operation, {} FROM {}(@P1, @P2, N'all update old') ORDER BY __$start_lsn, __$seqval, __$operation",
                    layout
                        .columns
                        .iter()
                        .map(|c| ident(&c.name))
                        .collect::<Vec<_>>()
                        .join(", "),
                    function
                ));
                query.bind(from.as_slice());
                query.bind(to.as_slice());
                for row in rows(client, query).await? {
                    let lsn = schema::binary10(&row, 0)?;
                    if from >= lsn || next_start.is_some_and(|start| lsn >= start) {
                        continue;
                    }
                    changes.push(CdcChange {
                        lsn,
                        seqval: schema::binary10(&row, 1)?,
                        operation: row
                            .try_get(2)
                            .map_err(sql_error)?
                            .ok_or_else(|| Error::new("SQL Server CDC operation is missing"))?,
                        layout: layout.clone(),
                        values: cells(&row, 3, &layout)?,
                    });
                }
            }
        }
        changes.sort_by(|a, b| {
            (
                a.lsn,
                a.seqval,
                a.operation,
                a.layout.kind(),
                a.layout.version,
            )
                .cmp(&(
                    b.lsn,
                    b.seqval,
                    b.operation,
                    b.layout.kind(),
                    b.layout.version,
                ))
        });
        let mut records = VecDeque::new();
        let mut bytes = 0;
        let mut position = self.state.high_water;
        let timestamp = now_ms()?;
        let mut cursor = self.state.cursor.clone();
        let mut open: Option<[u8; 10]> = None;
        let mut before: Option<([u8; 10], String)> = None;
        for change in changes {
            if open != Some(change.lsn) {
                if let Some(previous) = open {
                    position = next_position(position)?;
                    cursor = format_lsn(&previous)?;
                    append(
                        &mut records,
                        &mut bytes,
                        self.config.max_batch_bytes,
                        marker(
                            position,
                            &cursor,
                            &cursor,
                            Operation::TransactionEnd,
                            timestamp,
                        ),
                    )?;
                }
                position = next_position(position)?;
                cursor = format_lsn(&change.lsn)?;
                append(
                    &mut records,
                    &mut bytes,
                    self.config.max_batch_bytes,
                    marker(
                        position,
                        &cursor,
                        &cursor,
                        Operation::TransactionBegin,
                        timestamp,
                    ),
                )?;
                open = Some(change.lsn);
            }
            match change.operation {
                3 => {
                    before = Some((
                        change.seqval,
                        object(&change.layout, &change.values)?.to_string(),
                    ));
                }
                1 | 2 | 4 => {
                    position = next_position(position)?;
                    cursor = format_lsn(&change.lsn)?;
                    let (op, before_image, payload, record_key) = match change.operation {
                        1 => (
                            Operation::Delete,
                            Some(object(&change.layout, &change.values)?.to_string()),
                            "null".into(),
                            key(&change.layout, &change.values)?,
                        ),
                        2 => (
                            Operation::Insert,
                            None,
                            object(&change.layout, &change.values)?.to_string(),
                            key(&change.layout, &change.values)?,
                        ),
                        _ => {
                            let payload = object(&change.layout, &change.values)?.to_string();
                            let record_key = key(&change.layout, &change.values)?;
                            let before_image = before
                                .take()
                                .filter(|(seqval, _)| *seqval == change.seqval)
                                .map(|(_, image)| image);
                            (Operation::Update, before_image, payload, record_key)
                        }
                    };
                    append(
                        &mut records,
                        &mut bytes,
                        self.config.max_batch_bytes,
                        Record {
                            kind: change.layout.kind(),
                            key: record_key,
                            source_ts_ms: timestamp,
                            ingest_ts_ms: 0,
                            quality: None,
                            position,
                            payload,
                            source: Some(SourceMetadata {
                                cursor: Some(cursor.clone()),
                                change: Some(Change {
                                    operation: op,
                                    before_image,
                                    transaction_id: Some(cursor.clone()),
                                    commit_ts_ms: Some(timestamp),
                                }),
                            }),
                        },
                    )?;
                }
                _ => {
                    return Err(Error::new("SQL Server CDC operation is unsupported"));
                }
            }
        }
        if let Some(previous) = open {
            position = next_position(position)?;
            cursor = format_lsn(&previous)?;
            append(
                &mut records,
                &mut bytes,
                self.config.max_batch_bytes,
                marker(
                    position,
                    &cursor,
                    &cursor,
                    Operation::TransactionEnd,
                    timestamp,
                ),
            )?;
        }
        if records.is_empty() {
            self.state.cursor = format_lsn(&to)?;
            state::save(&self.config.state_dir, &self.state)?;
            return Ok(());
        }
        self.state.high_water = position;
        self.state.cursor = cursor;
        self.state.batch = Some(Batch {
            first: records.front().map(|r| r.position).unwrap_or(1),
            last: position,
            cursor_before: self
                .last_resume
                .as_ref()
                .map(|(_, cursor)| cursor.clone())
                .unwrap_or_default(),
        });
        write_batch(&self.config, &self.state, &records)?;
        self.pending = records;
        Ok(())
    }

    async fn poll_tracking(&mut self, client: &mut SqlClient) -> Result<(), Error> {
        let from = parse_version(&self.state.cursor)?;
        let to = parse_version(&current_version(client).await?)?;
        if to <= from {
            return Ok(());
        }
        let mut changes = Vec::new();
        for layout in self.state.layouts.clone() {
            let min = tracking_min_version(client, &layout).await?;
            if from < min {
                return Err(Error::new(
                    "SQL Server change tracking retention expired; an explicit new snapshot and consumer recovery are required",
                ));
            }
            let key_list = layout
                .keys
                .iter()
                .map(|k| ident(k))
                .collect::<Vec<_>>()
                .join(", ");
            let col_list = layout
                .columns
                .iter()
                .map(|c| {
                    let name = ident(&c.name);
                    if layout.keys.iter().any(|key| key == &c.name) {
                        format!("COALESCE(src.{name}, ct.{name})")
                    } else {
                        format!("src.{name}")
                    }
                })
                .collect::<Vec<_>>()
                .join(", ");
            let join = layout
                .keys
                .iter()
                .map(|k| format!("src.{k}=ct.{k}", k = ident(k)))
                .collect::<Vec<_>>()
                .join(" AND ");
            let sql = format!(
                "SELECT ct.SYS_CHANGE_VERSION, ct.SYS_CHANGE_OPERATION, {col_list} FROM CHANGETABLE(CHANGES {}, @P1) AS ct LEFT JOIN {} src ON {join} ORDER BY ct.SYS_CHANGE_VERSION, {key_list}",
                qualified(&layout.schema, &layout.table),
                qualified(&layout.schema, &layout.table)
            );
            let mut query = tiberius::Query::new(sql);
            query.bind(from);
            for row in rows(client, query).await? {
                let version: i64 = row
                    .try_get(0)
                    .map_err(sql_error)?
                    .ok_or_else(|| Error::new("SQL Server change tracking version is missing"))?;
                if version <= from {
                    continue;
                }
                let op: String = row
                    .try_get::<&str, _>(1)
                    .map_err(sql_error)?
                    .ok_or_else(|| Error::new("SQL Server change tracking operation is missing"))?
                    .to_owned();
                changes.push(TrackingChange {
                    version,
                    op,
                    layout: layout.clone(),
                    values: cells(&row, 2, &layout)?,
                });
            }
        }
        changes.sort_by(|a, b| {
            (a.version, a.layout.kind(), a.op.as_str()).cmp(&(
                b.version,
                b.layout.kind(),
                b.op.as_str(),
            ))
        });
        let mut records = VecDeque::new();
        let mut bytes = 0;
        let mut position = self.state.high_water;
        let timestamp = now_ms()?;
        let mut cursor = self.state.cursor.clone();
        let mut open: Option<i64> = None;
        for change in changes {
            if open != Some(change.version) {
                if let Some(previous) = open {
                    position = next_position(position)?;
                    cursor = previous.to_string();
                    append(
                        &mut records,
                        &mut bytes,
                        self.config.max_batch_bytes,
                        marker(
                            position,
                            &cursor,
                            &cursor,
                            Operation::TransactionEnd,
                            timestamp,
                        ),
                    )?;
                }
                position = next_position(position)?;
                cursor = change.version.to_string();
                append(
                    &mut records,
                    &mut bytes,
                    self.config.max_batch_bytes,
                    marker(
                        position,
                        &cursor,
                        &cursor,
                        Operation::TransactionBegin,
                        timestamp,
                    ),
                )?;
                open = Some(change.version);
            }
            position = next_position(position)?;
            cursor = change.version.to_string();
            let (operation, payload) = match change.op.as_str() {
                "D" => (Operation::Delete, "null".into()),
                "I" => (
                    Operation::Insert,
                    object(&change.layout, &change.values)?.to_string(),
                ),
                "U" => (
                    Operation::Update,
                    object(&change.layout, &change.values)?.to_string(),
                ),
                _ => {
                    return Err(Error::new(
                        "SQL Server change tracking operation is unsupported",
                    ));
                }
            };
            append(
                &mut records,
                &mut bytes,
                self.config.max_batch_bytes,
                Record {
                    kind: change.layout.kind(),
                    key: key(&change.layout, &change.values)?,
                    source_ts_ms: timestamp,
                    ingest_ts_ms: 0,
                    quality: None,
                    position,
                    payload,
                    source: Some(SourceMetadata {
                        cursor: Some(cursor.clone()),
                        change: Some(Change {
                            operation,
                            before_image: None,
                            transaction_id: Some(cursor.clone()),
                            commit_ts_ms: Some(timestamp),
                        }),
                    }),
                },
            )?;
        }
        if let Some(previous) = open {
            position = next_position(position)?;
            cursor = previous.to_string();
            append(
                &mut records,
                &mut bytes,
                self.config.max_batch_bytes,
                marker(
                    position,
                    &cursor,
                    &cursor,
                    Operation::TransactionEnd,
                    timestamp,
                ),
            )?;
        }
        if records.is_empty() {
            self.state.cursor = to.to_string();
            state::save(&self.config.state_dir, &self.state)?;
            return Ok(());
        }
        self.state.high_water = position;
        self.state.cursor = cursor;
        self.state.batch = Some(Batch {
            first: records.front().map(|r| r.position).unwrap_or(1),
            last: position,
            cursor_before: self
                .last_resume
                .as_ref()
                .map(|(_, cursor)| cursor.clone())
                .unwrap_or_default(),
        });
        write_batch(&self.config, &self.state, &records)?;
        self.pending = records;
        Ok(())
    }
}

impl Connector for MssqlConnector {
    fn id(&self) -> &str {
        &self.config.id
    }
    fn check(&self) -> Result<CheckReport, Error> {
        self.lease.renew()?;
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err(Error::new(
                "call SQL Server synchronous API outside an async runtime",
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
            .ok_or_else(|| Error::new("SQL Server runtime closed"))?;
        runtime.block_on(async {
            tokio::time::timeout(self.config.timeout, async {
                let mut client = Self::connection(&self.config).await?;
                let report = match self.state.mode {
                    Mode::Cdc => check_cdc(&mut client, &self.config, &self.state).await?,
                    Mode::ChangeTracking => {
                        check_tracking(&mut client, &self.config, &self.state).await?
                    }
                };
                Ok(report)
            })
            .await
            .map_err(|_| Error::new("SQL Server check timed out"))?
        })
    }
    fn discover(&self) -> Result<Vec<DiscoveredKind>, Error> {
        self.lease.renew()?;
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
        self.lease.renew()?;
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err(Error::new(
                "call SQL Server synchronous API outside an async runtime",
            ));
        }
        let result = self.read_inner(from, cursor);
        if result.is_err() {
            self.failed = true;
        }
        result
    }
}

impl Drop for MssqlConnector {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}

fn install_crypto() {
    static ONCE: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    ONCE.get_or_init(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

fn append(
    records: &mut VecDeque<Record>,
    bytes: &mut usize,
    max: usize,
    record: Record,
) -> Result<(), Error> {
    *bytes = bytes.saturating_add(
        crate::delivery::encode(&record, &record.idempotency_key("mssql", "spool")).len(),
    );
    if *bytes > max {
        return Err(Error::new(
            "SQL Server staged batch exceeds configured byte bound",
        ));
    }
    records.push_back(record);
    Ok(())
}
fn write_batch(config: &Config, state: &State, records: &VecDeque<Record>) -> Result<(), Error> {
    let batch = state
        .batch
        .as_ref()
        .ok_or_else(|| Error::new("SQL Server batch is missing"))?;
    let mut text = String::new();
    for record in records {
        text.push_str(&crate::delivery::encode(
            record,
            &record.idempotency_key("mssql", "spool"),
        ));
    }
    crate::checkpoint::atomic_write(&state::batch_path(&config.state_dir, batch), &text)?;
    state::save(&config.state_dir, state)
}
fn marker(
    position: Position,
    cursor: &str,
    transaction_id: &str,
    operation: Operation,
    timestamp: i64,
) -> Record {
    Record {
        kind: TRANSACTIONS.into(),
        key: transaction_id.into(),
        source_ts_ms: timestamp,
        ingest_ts_ms: 0,
        quality: None,
        position,
        payload: serde_json::json!({"transaction_id": transaction_id}).to_string(),
        source: Some(SourceMetadata {
            cursor: Some(cursor.into()),
            change: Some(Change {
                operation,
                before_image: None,
                transaction_id: Some(transaction_id.into()),
                commit_ts_ms: Some(timestamp),
            }),
        }),
    }
}
fn next_position(position: Position) -> Result<Position, Error> {
    position
        .checked_add(1)
        .ok_or_else(|| Error::new("SQL Server position overflow"))
}
fn now_ms() -> Result<i64, Error> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|d| i64::try_from(d.as_millis()).ok())
        .ok_or_else(|| Error::new("SQL Server clock is out of range"))
}
fn sql_error(_error: tiberius::error::Error) -> Error {
    Error::new("SQL Server source operation failed")
}

async fn detect_mode(client: &mut SqlClient, config: &Config) -> Result<Mode, Error> {
    if scalar_flag(
        client,
        "SELECT is_cdc_enabled FROM sys.databases WHERE name=DB_NAME()",
    )
    .await?
    {
        return Ok(Mode::Cdc);
    }
    let tracking: i32 = scalar_i32(
        client,
        "SELECT COUNT(*) FROM sys.change_tracking_databases WHERE database_id=DB_ID()",
    )
    .await?;
    if tracking == 1 {
        for table in &config.tables {
            let enabled: i32 = {
                let mut query = tiberius::Query::new(
                    "SELECT COUNT(*) FROM sys.change_tracking_tables ct JOIN sys.tables t ON t.object_id=ct.object_id JOIN sys.schemas s ON s.schema_id=t.schema_id WHERE s.name=@P1 AND t.name=@P2",
                );
                query.bind(table.schema.as_str());
                query.bind(table.table.as_str());
                let row = rows(client, query)
                    .await?
                    .into_iter()
                    .next()
                    .ok_or_else(|| Error::new("SQL Server change tracking lookup failed"))?;
                row.try_get::<i32, _>(0)
                    .map_err(sql_error)?
                    .ok_or_else(|| Error::new("SQL Server change tracking lookup failed"))?
            };
            if enabled == 0 {
                return Err(Error::new(
                    "SQL Server change tracking is not enabled on a selected table",
                ));
            }
        }
        return Ok(Mode::ChangeTracking);
    }
    Err(Error::new(
        "SQL Server requires change data capture or change tracking on the database",
    ))
}

async fn attach_instances(client: &mut SqlClient, layouts: &mut [Layout]) -> Result<(), Error> {
    for layout in layouts {
        let instances = capture_instances(client, layout).await?;
        if layout.capture_instance.is_none()
            && let Some(newest) = instances
                .iter()
                .max_by_key(|instance| (instance.create_date_ms, instance.name.as_str()))
        {
            layout.capture_instance = Some(newest.name.clone());
        }
    }
    Ok(())
}

async fn follow_instance(
    client: &mut SqlClient,
    layout: &mut Layout,
    instance: &CaptureInstance,
) -> Result<(), Error> {
    if layout.capture_instance.as_deref() == Some(instance.name.as_str()) {
        return Ok(());
    }
    let columns = captured_columns(client, &instance.name).await?;
    for key in &layout.keys {
        if !columns.iter().any(|column| &column.name == key) {
            return Err(Error::new(
                "SQL Server capture instance is missing a key column",
            ));
        }
    }
    if layout
        .columns
        .iter()
        .map(|column| (&column.name, &column.field))
        .ne(columns.iter().map(|column| (&column.name, &column.field)))
    {
        layout.version = layout.version.saturating_add(1);
        layout.columns = columns;
    }
    layout.capture_instance = Some(instance.name.clone());
    Ok(())
}

async fn capture_instances(
    client: &mut SqlClient,
    layout: &Layout,
) -> Result<Vec<CaptureInstance>, Error> {
    let mut query = tiberius::Query::new(
        "SELECT ct.capture_instance, ct.start_lsn, DATEDIFF_BIG(MILLISECOND, '1970-01-01', ct.create_date) FROM cdc.change_tables ct JOIN sys.tables t ON t.object_id=ct.source_object_id JOIN sys.schemas s ON s.schema_id=t.schema_id WHERE s.name=@P1 AND t.name=@P2",
    );
    query.bind(layout.schema.as_str());
    query.bind(layout.table.as_str());
    rows(client, query)
        .await?
        .into_iter()
        .map(|row| {
            Ok(CaptureInstance {
                name: schema::required_text(&row, 0)?,
                start_lsn: schema::binary10(&row, 1)?,
                create_date_ms: row
                    .try_get::<i64, _>(2)
                    .map_err(sql_error)?
                    .ok_or_else(|| {
                        Error::new("SQL Server capture instance timestamp is missing")
                    })?,
            })
        })
        .collect()
}

async fn read_max_lsn(client: &mut SqlClient) -> Result<Option<[u8; 10]>, Error> {
    let row = client
        .simple_query("SELECT sys.fn_cdc_get_max_lsn()")
        .await
        .map_err(sql_error)?
        .into_row()
        .await
        .map_err(sql_error)?
        .ok_or_else(|| Error::new("SQL Server CDC max LSN is missing"))?;
    match row.try_get::<&[u8], _>(0).map_err(sql_error)? {
        Some(bytes) if bytes.len() != 10 => Err(Error::new("SQL Server LSN must be 10 bytes")),
        Some(bytes) if bytes.iter().any(|b| *b != 0) => {
            let mut out = [0u8; 10];
            out.copy_from_slice(bytes);
            Ok(Some(out))
        }
        Some(_) | None => Ok(None),
    }
}

async fn covering_lsn(
    client: &mut SqlClient,
    layouts: &[Layout],
) -> Result<Option<[u8; 10]>, Error> {
    let Some(max) = read_max_lsn(client).await? else {
        return Ok(None);
    };
    for layout in layouts {
        let instances = capture_instances(client, layout).await?;
        if select_instance(&instances, &max).is_err() {
            return Ok(None);
        }
    }
    Ok(Some(max))
}

/// The capture job must scan past each instance start LSN before a snapshot cursor is valid.
async fn wait_covering_lsn(client: &mut SqlClient, layouts: &[Layout]) -> Result<(), Error> {
    for _ in 0..40 {
        if covering_lsn(client, layouts).await?.is_some() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Err(Error::new("SQL Server CDC max LSN is missing"))
}

async fn snapshot_lsn(client: &mut SqlClient, layouts: &[Layout]) -> Result<String, Error> {
    covering_lsn(client, layouts)
        .await?
        .ok_or_else(|| Error::new("SQL Server CDC max LSN is missing"))
        .and_then(|lsn| format_lsn(&lsn))
}
async fn instance_min_lsn(client: &mut SqlClient, instance: &str) -> Result<[u8; 10], Error> {
    capture_function(instance)?;
    let mut query = tiberius::Query::new("SELECT sys.fn_cdc_get_min_lsn(@P1)");
    query.bind(instance);
    let row = rows(client, query)
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| Error::new("SQL Server CDC min LSN is missing"))?;
    match row.try_get::<&[u8], _>(0).map_err(sql_error)? {
        Some(bytes) => {
            let mut out = [0u8; 10];
            if bytes.len() != 10 {
                return Err(Error::new("SQL Server LSN must be 10 bytes"));
            }
            out.copy_from_slice(bytes);
            Ok(out)
        }
        None => Err(Error::new("SQL Server CDC min LSN is missing")),
    }
}

async fn tracking_min_version(client: &mut SqlClient, layout: &Layout) -> Result<i64, Error> {
    let mut query = tiberius::Query::new(
        "SELECT CHANGE_TRACKING_MIN_VALID_VERSION(t.object_id) FROM sys.tables t JOIN sys.schemas s ON s.schema_id=t.schema_id WHERE s.name=@P1 AND t.name=@P2",
    );
    query.bind(layout.schema.as_str());
    query.bind(layout.table.as_str());
    let row = rows(client, query)
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| Error::new("SQL Server change tracking min version is missing"))?;
    if let Some(v) = row.try_get::<i64, _>(0).map_err(sql_error)? {
        return Ok(v);
    }
    Ok(i64::from(
        row.try_get::<i32, _>(0)
            .map_err(sql_error)?
            .ok_or_else(|| Error::new("SQL Server change tracking min version is missing"))?,
    ))
}
async fn current_version(client: &mut SqlClient) -> Result<String, Error> {
    Ok(
        scalar_i64(client, "SELECT CHANGE_TRACKING_CURRENT_VERSION()")
            .await?
            .to_string(),
    )
}
async fn execute(client: &mut SqlClient, sql: &str) -> Result<(), Error> {
    client
        .simple_query(sql)
        .await
        .map_err(sql_error)?
        .into_first_result()
        .await
        .map_err(sql_error)?;
    Ok(())
}

async fn scalar_flag(client: &mut SqlClient, sql: &str) -> Result<bool, Error> {
    let row = client
        .simple_query(sql)
        .await
        .map_err(sql_error)?
        .into_row()
        .await
        .map_err(sql_error)?
        .ok_or_else(|| Error::new("SQL Server scalar query returned nothing"))?;
    if let Ok(Some(v)) = row.try_get::<bool, _>(0) {
        return Ok(v);
    }
    if let Ok(Some(v)) = row.try_get::<i32, _>(0) {
        return Ok(v != 0);
    }
    if let Ok(Some(v)) = row.try_get::<u8, _>(0) {
        return Ok(v != 0);
    }
    Err(Error::new("SQL Server flag query returned nothing"))
}

fn count_i32(row: &tiberius::Row) -> Result<i32, Error> {
    if let Ok(Some(v)) = row.try_get::<i32, _>(0) {
        return Ok(v);
    }
    if let Ok(Some(v)) = row.try_get::<i64, _>(0) {
        return i32::try_from(v).map_err(|_| Error::new("SQL Server count is out of range"));
    }
    if let Ok(Some(v)) = row.try_get::<i16, _>(0) {
        return Ok(i32::from(v));
    }
    Err(Error::new("SQL Server count is missing"))
}

async fn scalar_i32(client: &mut SqlClient, sql: &str) -> Result<i32, Error> {
    let row = client
        .simple_query(sql)
        .await
        .map_err(sql_error)?
        .into_row()
        .await
        .map_err(sql_error)?
        .ok_or_else(|| Error::new("SQL Server scalar query returned nothing"))?;
    row.try_get::<i32, _>(0)
        .map_err(sql_error)?
        .ok_or_else(|| Error::new("SQL Server scalar query returned nothing"))
}
async fn scalar_i64(client: &mut SqlClient, sql: &str) -> Result<i64, Error> {
    let row = client
        .simple_query(sql)
        .await
        .map_err(sql_error)?
        .into_row()
        .await
        .map_err(sql_error)?
        .ok_or_else(|| Error::new("SQL Server scalar query returned nothing"))?;
    if let Some(v) = row.try_get::<i64, _>(0).map_err(sql_error)? {
        return Ok(v);
    }
    Ok(i64::from(
        row.try_get::<i32, _>(0)
            .map_err(sql_error)?
            .ok_or_else(|| Error::new("SQL Server scalar query returned nothing"))?,
    ))
}

async fn check_cdc(
    client: &mut SqlClient,
    config: &Config,
    state: &State,
) -> Result<CheckReport, Error> {
    let mut missing = Vec::new();
    if !scalar_flag(
        client,
        "SELECT is_cdc_enabled FROM sys.databases WHERE name=DB_NAME()",
    )
    .await?
    {
        missing.push("change data capture on the database");
    }
    if !scalar_flag(
        client,
        "SELECT CONVERT(bit, CASE WHEN EXISTS (
            SELECT 1 FROM sys.dm_server_services
            WHERE servicename LIKE N'SQL Server Agent%'
              AND (status = 4 OR LTRIM(RTRIM(status_desc)) = N'Running')
        ) THEN 1 ELSE 0 END)",
    )
    .await?
    {
        missing.push("SQL Server Agent running");
    }
    let retention_minutes = scalar_i64(
        client,
        "SELECT COALESCE((SELECT TOP 1 retention FROM msdb.dbo.cdc_jobs WHERE job_type=N'cleanup'), 4320)",
    )
    .await?;
    if Duration::from_secs(retention_minutes.max(0) as u64 * 60) < config.max_lag {
        missing.push("CDC retention covering configured lag");
    }
    for table in &config.tables {
        let mut query = tiberius::Query::new(
            "SELECT COUNT(*) FROM cdc.change_tables ct JOIN sys.tables t ON t.object_id=ct.source_object_id JOIN sys.schemas s ON s.schema_id=t.schema_id WHERE s.name=@P1 AND t.name=@P2",
        );
        query.bind(table.schema.as_str());
        query.bind(table.table.as_str());
        let count = rows(client, query)
            .await?
            .into_iter()
            .next()
            .map(|row| count_i32(&row))
            .transpose()?
            .unwrap_or(0);
        if count == 0 {
            missing.push("capture instance on each selected table");
            break;
        }
    }
    if let Ok(from) = parse_lsn(&state.cursor) {
        for layout in &state.layouts {
            let instances = capture_instances(client, layout).await?;
            match select_instance(&instances, &from) {
                Ok(instance) => {
                    let min = instance_min_lsn(client, &instance.name).await?;
                    if from < min {
                        missing.push("retained LSNs covering the checkpoint");
                        break;
                    }
                }
                Err(_) => {
                    missing.push("retained LSNs covering the checkpoint");
                    break;
                }
            }
        }
    }
    Ok(CheckReport {
        ok: missing.is_empty(),
        message: if missing.is_empty() {
            "change data capture, agent job, and retention verified".into()
        } else {
            format!("SQL Server check failed: {}", missing.join(", "))
        },
    })
}

async fn check_tracking(
    client: &mut SqlClient,
    config: &Config,
    state: &State,
) -> Result<CheckReport, Error> {
    let mut missing = Vec::new();
    if scalar_i32(
        client,
        "SELECT COUNT(*) FROM sys.change_tracking_databases WHERE database_id=DB_ID()",
    )
    .await?
        != 1
    {
        missing.push("change tracking on the database");
    }
    let retention_days = scalar_i64(
        client,
        "SELECT COALESCE((SELECT retention_period FROM sys.change_tracking_databases WHERE database_id=DB_ID() AND retention_period_units_desc=N'DAYS'), 2)",
    )
    .await?;
    if Duration::from_secs(retention_days.max(0) as u64 * 86400) < config.max_lag {
        missing.push("change tracking retention covering configured lag");
    }
    if let Ok(from) = parse_version(&state.cursor) {
        for layout in &state.layouts {
            let min = tracking_min_version(client, layout).await?;
            if from < min {
                missing.push("retained change tracking versions covering the checkpoint");
                break;
            }
        }
    }
    Ok(CheckReport {
        ok: missing.is_empty(),
        message: if missing.is_empty() {
            "change tracking fallback and retention verified".into()
        } else {
            format!("SQL Server check failed: {}", missing.join(", "))
        },
    })
}

fn snapshot_page(
    layout: &Layout,
    keys: &[String],
    last: Option<&Vec<schema::Cell>>,
    limit: usize,
) -> String {
    let columns = layout
        .columns
        .iter()
        .map(|c| ident(&c.name))
        .collect::<Vec<_>>()
        .join(", ");
    let order = keys.join(", ");
    match last {
        None => format!(
            "SELECT TOP ({limit}) {columns} FROM {} ORDER BY {order}",
            qualified(&layout.schema, &layout.table)
        ),
        Some(_) => format!(
            "SELECT TOP ({limit}) {columns} FROM {} WHERE {} ORDER BY {order}",
            qualified(&layout.schema, &layout.table),
            seek_predicate(keys)
        ),
    }
}

fn seek_predicate(keys: &[String]) -> String {
    (0..keys.len())
        .map(|end| {
            let mut parts: Vec<String> = (0..end)
                .map(|index| format!("{} = @P{}", keys[index], index + 1))
                .collect();
            parts.push(format!("{} > @P{}", keys[end], end + 1));
            format!("({})", parts.join(" AND "))
        })
        .collect::<Vec<_>>()
        .join(" OR ")
}

fn bind_key(query: &mut tiberius::Query<'_>, last: &[schema::Cell]) -> Result<(), Error> {
    for cell in last {
        match cell {
            schema::Cell::I32(v) => {
                query.bind(*v);
            }
            schema::Cell::I64(v) => {
                query.bind(*v);
            }
            schema::Cell::Text(v) => {
                query.bind(v.clone());
            }
            schema::Cell::Bool(v) => {
                query.bind(*v);
            }
            _ => {
                return Err(Error::new(
                    "SQL Server snapshot key type is unsupported for pagination",
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_configuration_fails_closed() {
        let error = match MssqlConnector::connect(Config {
            id: String::new(),
            host: "127.0.0.1".into(),
            port: 1433,
            user: "sa".into(),
            password: "x".into(),
            database: "db".into(),
            tables: vec![Table {
                schema: "dbo".into(),
                table: "items".into(),
                key: vec![],
            }],
            state_dir: std::env::temp_dir().join("hashi-mssql-invalid"),
            chunk_rows: 100,
            max_batch_bytes: 1024,
            schema_version: 1,
            timeout: Duration::from_secs(1),
            max_lag: Duration::from_secs(3600),
            allow_insecure_lab: true,
        }) {
            Ok(_) => panic!("invalid configuration was accepted"),
            Err(error) => error,
        };
        assert!(
            error
                .message
                .contains("invalid SQL Server connector configuration")
        );
    }

    #[test]
    fn capture_function_rejects_injection() {
        assert!(capture_function("dbo_items").is_ok());
        assert!(capture_function("dbo-items").is_err());
        assert!(capture_function("dbo_items; DROP").is_err());
    }

    #[test]
    fn snapshot_page_uses_key_order_seek() {
        let layout = Layout {
            schema: "dbo".into(),
            table: "items".into(),
            keys: vec!["tenant".into(), "id".into()],
            columns: vec![
                super::state::Column {
                    name: "note".into(),
                    sql_type: "nvarchar".into(),
                    field: FieldType::Text,
                },
                super::state::Column {
                    name: "tenant".into(),
                    sql_type: "int".into(),
                    field: FieldType::I32,
                },
                super::state::Column {
                    name: "id".into(),
                    sql_type: "int".into(),
                    field: FieldType::I32,
                },
            ],
            version: 1,
            capture_instance: None,
        };
        let keys: Vec<_> = layout.keys.iter().map(|key| ident(key)).collect();
        let first = snapshot_page(&layout, &keys, None, 10);
        assert!(first.contains("ORDER BY [tenant], [id]"));
        assert!(!first.contains("WHERE"));
        let next = snapshot_page(&layout, &keys, Some(&vec![Cell::I32(1), Cell::I32(2)]), 10);
        assert!(next.contains("([tenant] > @P1)"));
        assert!(next.contains("([tenant] = @P1 AND [id] > @P2)"));
        assert!(!next.contains("([tenant], [id]) >"));
    }
}
