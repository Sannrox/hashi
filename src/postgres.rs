//! Consistent staged snapshots and LSN-ordered logical replication capture.
mod replication;
mod schema;
mod state;
mod wire;
use crate::schema::{FieldType, Schema};
use crate::{
    Change, CheckReport, Connector, DiscoveredKind, Error, Operation, Position, Record,
    SourceMetadata,
};
use replication::{
    PgOutput, Wal, complete_tuple, decode_pgoutput, format_lsn, parse_copy, parse_lsn,
    resolve_toast, source_ts_ms, standby_status,
};
use schema::{ident, object, qualified, text_cells};
use state::{Batch, Layout, State};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::PathBuf;
use std::time::Duration;
use tokio_postgres::config::SslMode;
use tokio_postgres::{Client, Config as PgConfig, NoTls};
use wire::Replication;

const TRANSACTIONS: &str = "__postgres_transaction";

#[derive(Clone, serde::Serialize)]
pub struct Table {
    pub schema: String,
    pub table: String,
    /// Empty selects the primary key. An explicit key must be a complete non-null unique key.
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
    /// A slot whose confirmed LSN lags the current WAL by more than this fails `check`.
    pub max_lag: Duration,
    pub allow_insecure_lab: bool,
}

struct Stream {
    repl: Replication,
    confirmed: u64,
    types: BTreeMap<u32, Vec<u32>>,
    relations: BTreeMap<u32, (String, String)>,
}

pub struct PostgresConnector {
    config: Config,
    runtime: Option<tokio::runtime::Runtime>,
    state: State,
    pending: VecDeque<Record>,
    stream: Option<Stream>,
    failed: bool,
    last_resume: Option<(Position, String)>,
    lease: crate::lease::DropRelease,
}

impl PostgresConnector {
    /// Synchronous API; embed on a dedicated OS thread, outside a Tokio runtime.
    pub fn connect(config: Config) -> Result<Self, Error> {
        Self::open(config)
    }

    pub fn opened_at(&self) -> Position {
        self.state.high_water
    }

    fn open(config: Config) -> Result<Self, Error> {
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err(Error::new(
                "call PostgreSQL synchronous API outside an async runtime",
            ));
        }
        validate_config(&config)?;
        if !config.allow_insecure_lab {
            install_crypto()?;
        }
        std::fs::create_dir_all(&config.state_dir)?;
        let lease = crate::lease::hold(
            &config.state_dir,
            "postgres",
            crate::lease::hold_ttl(),
            "PostgreSQL source spool is already owned",
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
        let state = if let Some(state) = loaded {
            state
        } else {
            runtime
                .block_on(async {
                    tokio::time::timeout(
                        config.timeout,
                        Self::snapshot(&config, fingerprint, &lease),
                    )
                    .await
                })
                .map_err(|_| Error::new("PostgreSQL snapshot timed out before publication"))??
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
            lease,
        })
    }

    async fn sql(config: &Config) -> Result<Client, Error> {
        connect_client(config).await
    }

    async fn snapshot(
        config: &Config,
        fingerprint: String,
        lease: &crate::lease::DropRelease,
    ) -> Result<State, Error> {
        let slot = object_name(&config.id, &config.database)?;
        let publication = slot.clone();
        let sql = Self::sql(config).await?;
        ensure_wal_level(&sql).await?;
        drop_leftovers(&sql, &slot, &publication, &config.database).await?;
        create_publication(&sql, &publication, &config.tables).await?;
        let mut repl = Replication::connect(config).await?;
        let (lsn, snapshot) = create_slot(&mut repl, &slot).await?;
        sql.batch_execute(&format!(
            "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;\nSET TRANSACTION SNAPSHOT {};",
            literal(&snapshot)
        ))
        .await
        .map_err(|_| {
            Error::new("PostgreSQL snapshot export expired before table reads; retry the snapshot")
        })?;
        let layouts = schema::layouts(&sql, &config.tables, config.schema_version).await?;
        let mut records = VecDeque::new();
        let mut bytes = 0;
        let timestamp = now_ms()?;
        let mut position = 0;
        for layout in &layouts {
            let cursor = format!("hashi_snap_{}", position);
            sql.batch_execute(&format!(
                "DECLARE {cursor} NO SCROLL CURSOR FOR SELECT {} FROM ONLY {} ORDER BY {}",
                layout
                    .columns
                    .iter()
                    .map(|c| format!("{}::text", ident(&c.name)))
                    .collect::<Vec<_>>()
                    .join(","),
                qualified(&layout.schema, &layout.table),
                layout
                    .keys
                    .iter()
                    .map(|k| ident(k))
                    .collect::<Vec<_>>()
                    .join(",")
            ))
            .await
            .map_err(sql_error)?;
            loop {
                let rows = sql
                    .query(
                        &format!("FETCH {n} FROM {cursor}", n = config.chunk_rows),
                        &[],
                    )
                    .await
                    .map_err(sql_error)?;
                if rows.is_empty() {
                    break;
                }
                for row in rows {
                    let values = text_cells(&row, layout)?;
                    position = next_position(position)?;
                    let record = Record {
                        kind: layout.kind(),
                        key: schema::key(layout, &values)?,
                        source_ts_ms: timestamp,
                        ingest_ts_ms: 0,
                        quality: None,
                        position,
                        payload: object(layout, &values)?.to_string(),
                        source: Some(SourceMetadata {
                            cursor: Some(lsn.clone()),
                            change: Some(Change {
                                operation: Operation::Insert,
                                before_image: None,
                                transaction_id: None,
                                commit_ts_ms: None,
                            }),
                        }),
                    };
                    append(&mut records, &mut bytes, config.max_batch_bytes, record)?;
                }
                lease.renew()?;
            }
            sql.batch_execute(&format!("CLOSE {cursor}"))
                .await
                .map_err(sql_error)?;
        }
        sql.batch_execute("COMMIT").await.map_err(sql_error)?;
        drop(repl);
        if records.is_empty() {
            position = 1;
            append(
                &mut records,
                &mut bytes,
                config.max_batch_bytes,
                marker(
                    position,
                    &lsn,
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
            lsn,
            slot,
            publication,
            layouts,
            batch: Some(Batch {
                first: 1,
                last: position,
                cursor_before: String::new(),
            }),
        };
        write_batch(config, &state, &records, lease)?;
        Ok(state)
    }

    fn read_inner(
        &mut self,
        from: Option<Position>,
        cursor: Option<&str>,
    ) -> Result<Option<Record>, Error> {
        if self.failed {
            return Err(Error::new(
                "PostgreSQL reader failed; restart with its durable state to recover",
            ));
        }
        let committed = from.unwrap_or(0);
        if committed > self.state.high_water {
            return Err(Error::new(
                "PostgreSQL checkpoint is ahead of the source spool",
            ));
        }
        if let Some(batch) = &self.state.batch {
            if committed < batch.first.saturating_sub(1) {
                return Err(Error::new(
                    "PostgreSQL checkpoint predates retained source records",
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
                    "PostgreSQL position and source cursor do not match",
                ));
            }
        } else if committed != self.state.high_water || cursor != Some(self.state.lsn.as_str()) {
            return Err(Error::new(
                "PostgreSQL checkpoint does not match retained stream boundary",
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
            self.lease.renew()?;
            state::save(&self.config.state_dir, &self.state)?;
            std::fs::remove_file(state::batch_path(&self.config.state_dir, &batch))?;
        }
        let runtime = self
            .runtime
            .take()
            .ok_or_else(|| Error::new("PostgreSQL runtime closed"))?;
        let timeout = self.config.timeout;
        let result = runtime.block_on(async {
            tokio::time::timeout(timeout, self.transaction())
                .await
                .map_err(|_| {
                    Error::new(
                        "PostgreSQL stream operation timed out; restart from the durable cursor",
                    )
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
            self.open_stream().await?;
        }
        let mut records = VecDeque::new();
        let mut bytes = 0;
        let mut position = self.state.high_water;
        let mut xid: Option<u32> = None;
        let mut commit_ts = 0i64;
        loop {
            let wait = if xid.is_none() && records.is_empty() {
                Duration::from_millis(250)
            } else {
                self.config.timeout
            };
            let next = {
                let stream = self
                    .stream
                    .as_mut()
                    .ok_or_else(|| Error::new("PostgreSQL replication stream missing"))?;
                tokio::time::timeout(wait, stream.repl.next_copy()).await
            };
            let bytes_in = match next {
                Err(_elapsed) if xid.is_none() && records.is_empty() => {
                    let confirmed = parse_lsn(&self.state.lsn)?;
                    self.send_status(confirmed, 1).await?;
                    return Ok(());
                }
                Err(_elapsed) => {
                    return Err(Error::new(
                        "PostgreSQL replication read timed out; restart from the durable cursor",
                    ));
                }
                Ok(Err(error)) => return Err(error),
                Ok(Ok(None)) => {
                    return Err(Error::new("PostgreSQL replication stream ended"));
                }
                Ok(Ok(Some(bytes))) => bytes,
            };
            match parse_copy(bytes_in)? {
                Wal::Keepalive { reply } => {
                    if reply {
                        self.send_status(parse_lsn(&self.state.lsn)?, 1).await?;
                    }
                }
                Wal::Data { payload } => match decode_pgoutput(
                    &payload,
                    &self
                        .stream
                        .as_ref()
                        .ok_or_else(|| Error::new("PostgreSQL replication stream missing"))?
                        .types,
                )? {
                    None => {}
                    Some(PgOutput::Begin { timestamp, xid: id }) => {
                        if xid.is_some() {
                            return Err(Error::new("PostgreSQL nested logical transaction"));
                        }
                        commit_ts = source_ts_ms(timestamp);
                        position = next_position(position)?;
                        append(
                            &mut records,
                            &mut bytes,
                            self.config.max_batch_bytes,
                            marker(
                                position,
                                &self.state.lsn,
                                &id.to_string(),
                                Operation::TransactionBegin,
                                commit_ts,
                            ),
                        )?;
                        xid = Some(id);
                    }
                    Some(PgOutput::Relation(relation)) => {
                        {
                            let stream = self.stream.as_mut().ok_or_else(|| {
                                Error::new("PostgreSQL replication stream missing")
                            })?;
                            stream.types.insert(
                                relation.id,
                                relation.columns.iter().map(|c| c.type_oid).collect(),
                            );
                            stream.relations.insert(
                                relation.id,
                                (relation.schema.clone(), relation.table.clone()),
                            );
                        }
                        let Some(table) = self.config.tables.iter().find(|table| {
                            table.schema == relation.schema && table.table == relation.table
                        }) else {
                            return Err(Error::new(
                                "PostgreSQL relation is not a selected table; a rename or publication change requires explicit recovery",
                            ));
                        };
                        let table = table.clone();
                        let sql = Self::sql(&self.config).await?;
                        let version = self
                            .state
                            .layouts
                            .iter()
                            .find(|layout| {
                                layout.schema == relation.schema && layout.table == relation.table
                            })
                            .map(|layout| layout.version)
                            .unwrap_or(self.config.schema_version);
                        let fresh = schema::layouts(&sql, std::slice::from_ref(&table), version)
                            .await?
                            .into_iter()
                            .next()
                            .ok_or_else(|| {
                                Error::new("PostgreSQL relation catalog refresh returned no layout")
                            })?;
                        if let Some(layout) = self.state.layouts.iter_mut().find(|layout| {
                            layout.schema == relation.schema && layout.table == relation.table
                        }) {
                            let columns_changed = relation.columns.len() != layout.columns.len()
                                || relation.columns.iter().zip(&layout.columns).any(
                                    |(incoming, current)| {
                                        incoming.name != current.name
                                            || incoming.type_oid != current.type_oid
                                    },
                                );
                            if fresh.keys != layout.keys {
                                return Err(Error::new(
                                    "PostgreSQL unique key changed; an explicit new snapshot and consumer recovery are required",
                                ));
                            }
                            if columns_changed {
                                if xid.is_some()
                                    && records.iter().any(|r| {
                                        r.source
                                            .as_ref()
                                            .and_then(|s| s.change.as_ref())
                                            .is_some_and(|c| {
                                                matches!(
                                                    c.operation,
                                                    Operation::Insert
                                                        | Operation::Update
                                                        | Operation::Delete
                                                )
                                            })
                                    })
                                {
                                    return Err(Error::new(
                                        "PostgreSQL schema changed inside a data transaction; explicit recovery required",
                                    ));
                                }
                                layout.columns = relation
                                    .columns
                                    .iter()
                                    .map(|column| {
                                        Ok(state::Column {
                                            name: column.name.clone(),
                                            type_oid: column.type_oid,
                                            field: schema::field(column.type_oid)?,
                                        })
                                    })
                                    .collect::<Result<Vec<_>, Error>>()?;
                                if layout.keys.iter().any(|key| {
                                    !layout.columns.iter().any(|column| &column.name == key)
                                }) {
                                    return Err(Error::new(
                                        "PostgreSQL key columns missing after schema change",
                                    ));
                                }
                                layout.version =
                                    layout.version.checked_add(1).ok_or_else(|| {
                                        Error::new("PostgreSQL schema version exhausted")
                                    })?;
                            }
                        }
                    }
                    Some(PgOutput::Insert { rel_id, new }) => {
                        let new = complete_tuple(&new)?;
                        self.emit_row(
                            &mut records,
                            &mut bytes,
                            &mut position,
                            rel_id,
                            Operation::Insert,
                            None,
                            &new,
                            xid,
                            commit_ts,
                        )?;
                    }
                    Some(PgOutput::Update {
                        rel_id,
                        old,
                        old_complete,
                        new,
                    }) => {
                        let (old, new) = resolve_toast(old.as_deref(), old_complete, &new)?;
                        self.emit_row(
                            &mut records,
                            &mut bytes,
                            &mut position,
                            rel_id,
                            Operation::Update,
                            old.as_deref(),
                            &new,
                            xid,
                            commit_ts,
                        )?;
                    }
                    Some(PgOutput::Delete { rel_id, old }) => {
                        let old = complete_tuple(&old)?;
                        self.emit_row(
                            &mut records,
                            &mut bytes,
                            &mut position,
                            rel_id,
                            Operation::Delete,
                            Some(&old),
                            &old,
                            xid,
                            commit_ts,
                        )?;
                    }
                    Some(PgOutput::Commit { end_lsn, timestamp }) => {
                        let id = xid
                            .take()
                            .ok_or_else(|| Error::new("PostgreSQL commit without begin"))?;
                        commit_ts = source_ts_ms(timestamp);
                        let lsn = format_lsn(end_lsn);
                        position = next_position(position)?;
                        append(
                            &mut records,
                            &mut bytes,
                            self.config.max_batch_bytes,
                            marker(
                                position,
                                &lsn,
                                &id.to_string(),
                                Operation::TransactionEnd,
                                commit_ts,
                            ),
                        )?;
                        let mut next = self.state.clone();
                        next.batch = Some(Batch {
                            first: self.state.high_water + 1,
                            last: position,
                            cursor_before: self.state.lsn.clone(),
                        });
                        next.high_water = position;
                        next.lsn = lsn;
                        write_batch(&self.config, &next, &records, &self.lease)?;
                        self.send_status(end_lsn, 0).await?;
                        self.state = next;
                        self.pending = records;
                        return Ok(());
                    }
                    Some(PgOutput::Type | PgOutput::Origin | PgOutput::LogicalMessage) => {}
                },
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn emit_row(
        &self,
        records: &mut VecDeque<Record>,
        bytes: &mut usize,
        position: &mut Position,
        rel_id: u32,
        operation: Operation,
        before: Option<&[Option<schema::Cell>]>,
        values: &[Option<schema::Cell>],
        xid: Option<u32>,
        commit_ts: i64,
    ) -> Result<(), Error> {
        let xid = xid.ok_or_else(|| Error::new("PostgreSQL row event outside a transaction"))?;
        let (schema_name, table) = self
            .stream
            .as_ref()
            .ok_or_else(|| Error::new("PostgreSQL replication stream missing"))?
            .relations
            .get(&rel_id)
            .ok_or_else(|| Error::new("PostgreSQL row relation is unknown"))?;
        let Some(layout) = self
            .state
            .layouts
            .iter()
            .find(|layout| layout.schema == *schema_name && layout.table == *table)
        else {
            return Err(Error::new(
                "PostgreSQL row relation is not a selected table; a rename or publication change requires explicit recovery",
            ));
        };
        let identity = if operation == Operation::Delete {
            before.unwrap_or(values)
        } else {
            values
        };
        *position = next_position(*position)?;
        let record = Record {
            kind: layout.kind(),
            key: schema::key(layout, identity)?,
            source_ts_ms: commit_ts,
            ingest_ts_ms: 0,
            quality: None,
            position: *position,
            payload: if operation == Operation::Delete {
                "null".into()
            } else {
                object(layout, values)?.to_string()
            },
            source: Some(SourceMetadata {
                cursor: Some(self.state.lsn.clone()),
                change: Some(Change {
                    operation,
                    before_image: before
                        .map(|v| object(layout, v).map(|o| o.to_string()))
                        .transpose()?,
                    transaction_id: Some(xid.to_string()),
                    commit_ts_ms: Some(commit_ts),
                }),
            }),
        };
        append(records, bytes, self.config.max_batch_bytes, record)
    }

    async fn open_stream(&mut self) -> Result<(), Error> {
        let sql = Self::sql(&self.config).await?;
        let row = sql
            .query_opt(
                "SELECT slot_name, restart_lsn::text, confirmed_flush_lsn::text, wal_status
                 FROM pg_replication_slots WHERE slot_name = $1",
                &[&self.state.slot],
            )
            .await
            .map_err(sql_error)?;
        let Some(row) = row else {
            return Err(Error::new(
                "PostgreSQL replication slot is gone; an explicit new snapshot and consumer recovery are required",
            ));
        };
        let restart: Option<String> = row.get(1);
        let confirmed: Option<String> = row.get(2);
        let Some(restart) = restart else {
            return Err(Error::new(
                "PostgreSQL replication slot lost its restart LSN; WAL may have been recycled",
            ));
        };
        let start = parse_lsn(&self.state.lsn)?;
        if parse_lsn(&restart)? > start {
            return Err(Error::new(
                "PostgreSQL replication slot lost WAL before the durable cursor; an explicit new snapshot and consumer recovery are required",
            ));
        }
        if confirmed
            .as_deref()
            .map(parse_lsn)
            .transpose()?
            .is_some_and(|lsn| lsn > start)
        {
            return Err(Error::new(
                "PostgreSQL replication slot advanced past the durable cursor; an explicit new snapshot and consumer recovery are required",
            ));
        }
        sql.execute(
            "SELECT pg_terminate_backend(active_pid)
             FROM pg_replication_slots
             WHERE slot_name = $1 AND active",
            &[&self.state.slot],
        )
        .await
        .map_err(sql_error)?;
        let query = format!(
            "START_REPLICATION SLOT {} LOGICAL {} (proto_version '1', publication_names {})",
            self.state.slot,
            format_lsn(start),
            literal(&self.state.publication)
        );
        let mut last = Error::new("PostgreSQL logical stream failed to start");
        for attempt in 0..8 {
            let mut repl = match Replication::connect(&self.config).await {
                Ok(repl) => repl,
                Err(error) => {
                    last = error;
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
            };
            match repl.start_copy(&query).await {
                Ok(()) => {
                    repl.send_copy(standby_status(start, 1)).await?;
                    self.stream = Some(Stream {
                        repl,
                        confirmed: start,
                        types: BTreeMap::new(),
                        relations: BTreeMap::new(),
                    });
                    return Ok(());
                }
                Err(error) => {
                    last = error;
                    if attempt == 7 {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
        Err(last)
    }

    async fn send_status(&mut self, lsn: u64, reply: u8) -> Result<(), Error> {
        let stream = self
            .stream
            .as_mut()
            .ok_or_else(|| Error::new("PostgreSQL replication stream missing"))?;
        stream
            .repl
            .send_copy(standby_status(lsn.max(stream.confirmed), reply))
            .await?;
        stream.confirmed = stream.confirmed.max(lsn);
        Ok(())
    }
}

impl Connector for PostgresConnector {
    fn id(&self) -> &str {
        &self.config.id
    }
    fn check(&self) -> Result<CheckReport, Error> {
        self.lease.renew()?;
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err(Error::new(
                "call PostgreSQL synchronous API outside an async runtime",
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
            .ok_or_else(|| Error::new("PostgreSQL runtime closed"))?;
        runtime.block_on(async {
            tokio::time::timeout(self.config.timeout, async {
                let sql = Self::sql(&self.config).await?;
                ensure_wal_level(&sql).await?;
                let mut missing = Vec::new();
                let replication: bool = sql
                    .query_one("SELECT pg_catalog.pg_is_in_recovery() IS FALSE AND (current_setting('wal_level') = 'logical')", &[])
                    .await
                    .map_err(sql_error)?
                    .get(0);
                if !replication {
                    missing.push("logical WAL on a primary");
                }
                let slot = sql
                    .query_opt(
                        "SELECT s.restart_lsn::text, s.confirmed_flush_lsn::text,
                                (SELECT EXTRACT(EPOCH FROM r.flush_lag)::bigint
                                 FROM pg_stat_replication r WHERE r.pid = s.active_pid)
                         FROM pg_replication_slots s WHERE s.slot_name = $1",
                        &[&self.state.slot],
                    )
                    .await
                    .map_err(sql_error)?;
                match slot {
                    None => missing.push("replication slot"),
                    Some(row) => {
                        let restart: Option<String> = row.get(0);
                        let confirmed: Option<String> = row.get(1);
                        let start = parse_lsn(&self.state.lsn)?;
                        if restart.is_none() {
                            missing.push("retained WAL for the replication slot");
                        }
                        if restart
                            .as_deref()
                            .map(parse_lsn)
                            .transpose()?
                            .is_some_and(|lsn| lsn > start)
                        {
                            missing.push("retained WAL for the durable cursor");
                        }
                        if confirmed
                            .as_deref()
                            .map(parse_lsn)
                            .transpose()?
                            .is_some_and(|lsn| lsn > start)
                        {
                            missing.push("replication slot still at the durable cursor");
                        }
                        // flush_lag is only present while a walsender is connected.
                        // check() also runs after snapshot, before open_stream, so a
                        // NULL lag is "not yet measured", not "caught up forever".
                        let flush_lag_s: Option<i64> = row.get(2);
                        if flush_lag_s.is_some_and(|secs| {
                            secs > 0
                                && Duration::from_secs(u64::try_from(secs).unwrap_or(u64::MAX))
                                    > self.config.max_lag
                        }) {
                            missing.push("capture lag within configured max_lag");
                        }
                    }
                }
                let published: Vec<(String, String)> = sql
                    .query(
                        "SELECT n.nspname, c.relname
                         FROM pg_publication_rel pr
                         JOIN pg_publication p ON p.oid = pr.prpubid
                         JOIN pg_class c ON c.oid = pr.prrelid
                         JOIN pg_namespace n ON n.oid = c.relnamespace
                         WHERE p.pubname = $1",
                        &[&self.state.publication],
                    )
                    .await
                    .map_err(sql_error)?
                    .into_iter()
                    .map(|row| (row.get(0), row.get(1)))
                    .collect();
                let ops = sql
                    .query_opt(
                        "SELECT pubinsert, pubupdate, pubdelete FROM pg_publication WHERE pubname = $1",
                        &[&self.state.publication],
                    )
                    .await
                    .map_err(sql_error)?;
                match ops {
                    Some(row)
                        if row.get::<_, bool>(0) && row.get::<_, bool>(1) && row.get::<_, bool>(2) => {}
                    Some(_) => missing.push("publication insert, update, and delete"),
                    None => missing.push("publication"),
                }
                for table in &self.config.tables {
                    sql.query(
                        &format!(
                            "SELECT * FROM ONLY {} LIMIT 0",
                            qualified(&table.schema, &table.table)
                        ),
                        &[],
                    )
                    .await
                    .map_err(sql_error)?;
                    if !published
                        .iter()
                        .any(|(schema, name)| schema == &table.schema && name == &table.table)
                    {
                        missing.push("publication membership for every selected table");
                        break;
                    }
                }
                Ok(CheckReport {
                    ok: missing.is_empty(),
                    message: if missing.is_empty() {
                        "logical replication slot and required privileges verified".into()
                    } else {
                        format!("PostgreSQL capture is lagging or incomplete: {}", missing.join(", "))
                    },
                })
            })
            .await
            .map_err(|_| Error::new("PostgreSQL check timed out"))?
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
                "call PostgreSQL synchronous API outside an async runtime",
            ));
        }
        let result = self.read_inner(from, cursor);
        if result.is_err() {
            self.failed = true;
        }
        result
    }
}

impl Drop for PostgresConnector {
    fn drop(&mut self) {
        self.stream.take();
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}

fn validate_config(config: &Config) -> Result<(), Error> {
    if config.id.trim().is_empty()
        || config.host.trim().is_empty()
        || config.port == 0
        || config.user.trim().is_empty()
        || config.database.trim().is_empty()
        || config.tables.is_empty()
        || config.tables.len() > 256
        || config.chunk_rows == 0
        || config.chunk_rows > 10_000
        || config.max_batch_bytes == 0
        || config.schema_version == 0
        || config.timeout.is_zero()
        || config.max_lag.is_zero()
        || config.tables.iter().any(|t| {
            [&t.schema, &t.table]
                .iter()
                .any(|name| name.is_empty() || name.len() > 63 || name.contains('\0'))
        })
        || config
            .tables
            .iter()
            .map(|t| format!("{}.{}", t.schema, t.table))
            .collect::<BTreeSet<_>>()
            .len()
            != config.tables.len()
    {
        return Err(Error::new("invalid PostgreSQL connector configuration"));
    }
    Ok(())
}

fn install_crypto() -> Result<(), Error> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();
    Ok(())
}

async fn connect_client(config: &Config) -> Result<Client, Error> {
    let mut pg = PgConfig::new();
    pg.host(&config.host);
    pg.port(config.port);
    pg.user(&config.user);
    pg.password(&config.password);
    pg.dbname(&config.database);
    pg.application_name("hashi");
    if config.allow_insecure_lab {
        pg.ssl_mode(SslMode::Disable);
        let (client, conn) = tokio::time::timeout(config.timeout, pg.connect(NoTls))
            .await
            .map_err(|_| Error::new("PostgreSQL connection timed out"))?
            .map_err(sql_error)?;
        tokio::spawn(async move {
            let _ = conn.await;
        });
        return Ok(client);
    }
    pg.ssl_mode(SslMode::Require);
    let tls = rustls_connect()?;
    let (client, conn) = tokio::time::timeout(config.timeout, pg.connect(tls))
        .await
        .map_err(|_| Error::new("PostgreSQL connection timed out"))?
        .map_err(sql_error)?;
    tokio::spawn(async move {
        let _ = conn.await;
    });
    Ok(client)
}

fn rustls_connect() -> Result<tokio_postgres_rustls::MakeRustlsConnect, Error> {
    install_crypto()?;
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let tls = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(tokio_postgres_rustls::MakeRustlsConnect::new(tls))
}

async fn ensure_wal_level(client: &Client) -> Result<(), Error> {
    let level: String = client
        .query_one("SHOW wal_level", &[])
        .await
        .map_err(sql_error)?
        .get(0);
    if level != "logical" {
        return Err(Error::new(
            "PostgreSQL requires wal_level=logical for change capture",
        ));
    }
    Ok(())
}

async fn drop_leftovers(
    client: &Client,
    slot: &str,
    publication: &str,
    database: &str,
) -> Result<(), Error> {
    let owner: Option<String> = client
        .query_opt(
            "SELECT database FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .map_err(sql_error)?
        .and_then(|row| row.get(0));
    if owner.as_deref().is_some_and(|owner| owner != database) {
        return Err(Error::new(
            "PostgreSQL replication slot name is owned by another database",
        ));
    }
    client
        .execute(
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .map_err(sql_error)?;
    client
        .batch_execute(&format!(
            "DROP PUBLICATION IF EXISTS {}",
            ident(publication)
        ))
        .await
        .map_err(sql_error)?;
    Ok(())
}

async fn create_publication(
    client: &Client,
    publication: &str,
    tables: &[Table],
) -> Result<(), Error> {
    let list = tables
        .iter()
        .map(|t| format!("ONLY {}", qualified(&t.schema, &t.table)))
        .collect::<Vec<_>>()
        .join(", ");
    client
        .batch_execute(&format!(
            "CREATE PUBLICATION {} FOR TABLE {}",
            ident(publication),
            list
        ))
        .await
        .map_err(sql_error)?;
    Ok(())
}

async fn create_slot(repl: &mut Replication, slot: &str) -> Result<(String, String), Error> {
    let rows = repl
        .simple_query(&format!(
            "CREATE_REPLICATION_SLOT {slot} LOGICAL pgoutput EXPORT_SNAPSHOT"
        ))
        .await?;
    let row = rows
        .into_iter()
        .next()
        .ok_or_else(|| Error::new("PostgreSQL CREATE_REPLICATION_SLOT returned no row"))?;
    let lsn = row
        .get("consistent_point")
        .and_then(|v| v.as_deref())
        .ok_or_else(|| Error::new("PostgreSQL slot consistent_point missing"))?;
    let snapshot = row
        .get("snapshot_name")
        .and_then(|v| v.as_deref())
        .ok_or_else(|| Error::new("PostgreSQL slot snapshot_name missing"))?;
    parse_lsn(lsn)?;
    Ok((lsn.to_owned(), snapshot.to_owned()))
}

fn object_name(id: &str, database: &str) -> Result<String, Error> {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(id.as_bytes());
    hasher.update([0]);
    hasher.update(database.as_bytes());
    let digest = hasher.finalize();
    let digest: String = digest[..8].iter().map(|b| format!("{b:02x}")).collect();
    let mut body: String = id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect();
    body = body.trim_matches('_').to_owned();
    if body.is_empty() {
        body = "src".into();
    }
    let mut name = format!("hashi_{body}_{digest}");
    if name.len() > 63 {
        name = format!("hashi_{digest}");
    }
    Ok(name)
}

fn literal(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
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
        payload: serde_json::json!({"transaction_id": id}).to_string(),
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
        .ok_or_else(|| Error::new("PostgreSQL position exhausted"))
}

fn now_ms() -> Result<i64, Error> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|d| i64::try_from(d.as_millis()).ok())
        .ok_or_else(|| Error::new("PostgreSQL clock is out of range"))
}

fn sql_error(_error: tokio_postgres::Error) -> Error {
    Error::new("PostgreSQL source operation failed")
}

fn append(
    records: &mut VecDeque<Record>,
    bytes: &mut usize,
    max: usize,
    record: Record,
) -> Result<(), Error> {
    *bytes = bytes.saturating_add(
        crate::delivery::encode(&record, &record.idempotency_key("postgres", "spool")).len(),
    );
    if *bytes > max {
        return Err(Error::new(
            "PostgreSQL staged batch exceeds configured byte bound",
        ));
    }
    records.push_back(record);
    Ok(())
}

fn write_batch(
    config: &Config,
    state: &State,
    records: &VecDeque<Record>,
    lease: &crate::lease::DropRelease,
) -> Result<(), Error> {
    lease.renew()?;
    let batch = state
        .batch
        .as_ref()
        .ok_or_else(|| Error::new("PostgreSQL batch manifest missing"))?;
    let text: String = records
        .iter()
        .map(|record| crate::delivery::encode(record, &record.idempotency_key("postgres", "spool")))
        .collect();
    crate::checkpoint::atomic_write(&state::batch_path(&config.state_dir, batch), &text)?;
    state::save(&config.state_dir, state)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_configuration_fails_closed() {
        match PostgresConnector::connect(Config {
            id: String::new(),
            host: "127.0.0.1".into(),
            port: 5432,
            user: "postgres".into(),
            password: "x".into(),
            database: "db".into(),
            tables: vec![Table {
                schema: "public".into(),
                table: "items".into(),
                key: vec![],
            }],
            state_dir: std::env::temp_dir().join("hashi-postgres-invalid"),
            chunk_rows: 1,
            max_batch_bytes: 1024,
            schema_version: 1,
            timeout: Duration::from_secs(1),
            max_lag: Duration::from_secs(1),
            allow_insecure_lab: true,
        }) {
            Ok(_) => panic!("expected invalid configuration"),
            Err(error) => assert!(error.message.contains("invalid PostgreSQL connector")),
        }
    }

    #[test]
    fn object_names_are_postgres_safe() {
        let a = object_name("line-1", "plant").unwrap();
        let b = object_name("line_1", "plant").unwrap();
        let c = object_name("line-1", "other").unwrap();
        assert_ne!(a, b);
        assert_ne!(a, c);
        assert!(a.len() <= 63);
        assert!(a.starts_with("hashi_"));
    }
}
