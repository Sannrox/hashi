//! Generic SQL historian behind the historian trait, SQL Server mapping only (#68).
//!
//! [`SqlHistorian`] discovers tags from a tag table and reads samples from a
//! value table by the mapped identity column, not the name, so a
//! case-insensitive collation cannot alias two tags. The mapping is checked
//! against the catalog before any read: tables, columns, types, and unique
//! identity and name columns. An optional partition catalog names time-sliced
//! value tables; omitting it keeps one `value_table`. Interpolated reads are
//! not offered: SQL tables do not define a processed history.
//! `mapping.database` is the schema that holds the tables; the connection
//! database is the catalog.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use chrono::{DateTime, FixedOffset, NaiveDateTime, Utc};
use tiberius::{AuthMethod, EncryptionLevel, Row};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

use super::schema::{ident, qualified};
use crate::historian::{
    Historian, PartitionRange, Sample, Tag, ordered_partition_ranges, partition_set_rotated,
    partitioned_raw_windows,
};
use crate::{CheckReport, Error};

type SqlClient = tiberius::Client<Compat<TcpStream>>;

/// Catalog table that names value tables and their half-open time ranges.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionCatalog {
    pub table: String,
    pub value_table_column: String,
    pub start_column: String,
    pub end_column: String,
}

/// One mapping from catalog tables onto tags and samples.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mapping {
    pub database: String,
    pub tag_table: String,
    pub tag_id_column: String,
    pub tag_name_column: String,
    /// Empty when [`partition_catalog`](Self::partition_catalog) is set.
    pub value_table: String,
    pub partition_catalog: Option<PartitionCatalog>,
    pub value_tag_column: String,
    pub time_column: String,
    pub value_column: String,
    pub quality_column: String,
}

pub struct Config {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    pub mapping: Mapping,
    pub timeout: Duration,
    pub allow_insecure_lab: bool,
}

#[derive(Clone)]
enum TagId {
    I32(i32),
    I64(i64),
    Text(String),
}

struct MappedTag {
    name: String,
    id: TagId,
}

pub struct SqlHistorian {
    config: Config,
    runtime: tokio::runtime::Runtime,
    tags: Vec<MappedTag>,
}

impl SqlHistorian {
    /// Connect, refuse a mapping that does not match the catalog, and load tags.
    pub fn connect(config: Config) -> Result<Self, Error> {
        outside_runtime()?;
        validate_config(&config)?;
        validate_mapping(&config.mapping)?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()?;
        let tags = runtime.block_on(async {
            tokio::time::timeout(config.timeout, async {
                let mut client = connection(&config).await?;
                check_mapping(&mut client, &config.mapping).await?;
                let tags = load_tags(&mut client, &config.mapping).await?;
                Ok::<_, Error>(tags)
            })
            .await
            .map_err(|_| Error::new("SQL historian connect timed out"))?
        })?;
        Ok(Self {
            config,
            runtime,
            tags,
        })
    }

    /// Re-check the mapping and report the discovered tags.
    pub fn check(&self) -> Result<CheckReport, Error> {
        outside_runtime()?;
        self.runtime.block_on(async {
            tokio::time::timeout(self.config.timeout, async {
                let mut client = connection(&self.config).await?;
                check_mapping(&mut client, &self.config.mapping).await?;
                Ok(CheckReport {
                    ok: !self.tags.is_empty(),
                    message: if self.tags.is_empty() {
                        "SQL historian reports no tags".into()
                    } else {
                        format!("{} tags", self.tags.len())
                    },
                })
            })
            .await
            .map_err(|_| Error::new("SQL historian check timed out"))?
        })
    }
}

fn validate_config(config: &Config) -> Result<(), Error> {
    if config.host.trim().is_empty()
        || config.port == 0
        || config.user.trim().is_empty()
        || config.password.is_empty()
        || config.database.trim().is_empty()
        || config.timeout.is_zero()
    {
        return Err(Error::new("invalid SQL historian configuration"));
    }
    Ok(())
}

fn mapping_field(field: &str, value: &str) -> Result<(), Error> {
    if value.is_empty() || value.len() > 64 || value.contains('\0') {
        return Err(Error::new(format!(
            "invalid SQL historian mapping: {field}"
        )));
    }
    Ok(())
}

fn validate_mapping(mapping: &Mapping) -> Result<(), Error> {
    mapping_field("database", &mapping.database)?;
    mapping_field("tag_table", &mapping.tag_table)?;
    mapping_field("tag_id_column", &mapping.tag_id_column)?;
    mapping_field("tag_name_column", &mapping.tag_name_column)?;
    mapping_field("value_tag_column", &mapping.value_tag_column)?;
    mapping_field("time_column", &mapping.time_column)?;
    mapping_field("value_column", &mapping.value_column)?;
    mapping_field("quality_column", &mapping.quality_column)?;
    if mapping.tag_id_column == mapping.tag_name_column {
        return Err(Error::new(
            "invalid SQL historian mapping: tag identity and name columns must differ",
        ));
    }
    let value_columns = [
        mapping.value_tag_column.as_str(),
        mapping.time_column.as_str(),
        mapping.value_column.as_str(),
        mapping.quality_column.as_str(),
    ];
    if value_columns.iter().collect::<BTreeSet<_>>().len() != value_columns.len() {
        return Err(Error::new(
            "invalid SQL historian mapping: value-table columns must be distinct",
        ));
    }
    match &mapping.partition_catalog {
        None => mapping_field("value_table", &mapping.value_table)?,
        Some(catalog) => {
            if !mapping.value_table.is_empty() {
                return Err(Error::new(
                    "invalid SQL historian mapping: value_table and partition_catalog are mutually exclusive",
                ));
            }
            mapping_field("partition_catalog.table", &catalog.table)?;
            mapping_field(
                "partition_catalog.value_table_column",
                &catalog.value_table_column,
            )?;
            mapping_field("partition_catalog.start_column", &catalog.start_column)?;
            mapping_field("partition_catalog.end_column", &catalog.end_column)?;
            let catalog_columns = [
                catalog.value_table_column.as_str(),
                catalog.start_column.as_str(),
                catalog.end_column.as_str(),
            ];
            if catalog_columns.iter().collect::<BTreeSet<_>>().len() != catalog_columns.len() {
                return Err(Error::new(
                    "invalid SQL historian mapping: partition catalog columns must be distinct",
                ));
            }
        }
    }
    Ok(())
}

async fn connection(config: &Config) -> Result<SqlClient, Error> {
    super::install_crypto();
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
        .map_err(|_| Error::new("SQL historian connection timed out"))?
        .map_err(|_| Error::new("SQL historian connection failed"))?;
    tcp.set_nodelay(true)
        .map_err(|_| Error::new("SQL historian connection failed"))?;
    tokio::time::timeout(
        config.timeout,
        tiberius::Client::connect(cfg, tcp.compat_write()),
    )
    .await
    .map_err(|_| Error::new("SQL historian login timed out"))?
    .map_err(sql_error)
}

async fn check_mapping(client: &mut SqlClient, mapping: &Mapping) -> Result<(), Error> {
    let tag_columns = catalog_columns(client, &mapping.database, &mapping.tag_table).await?;
    let tag_id = require_column(
        &tag_columns,
        &mapping.tag_table,
        &mapping.tag_id_column,
        None,
        false,
    )?;
    require_column(
        &tag_columns,
        &mapping.tag_table,
        &mapping.tag_name_column,
        Some(name_type_ok),
        false,
    )?;
    require_unique(
        client,
        &mapping.database,
        &mapping.tag_table,
        &mapping.tag_id_column,
    )
    .await?;
    require_unique(
        client,
        &mapping.database,
        &mapping.tag_table,
        &mapping.tag_name_column,
    )
    .await?;
    match &mapping.partition_catalog {
        None => check_value_table(client, mapping, &mapping.value_table, tag_id).await,
        Some(catalog) => {
            let catalog_columns =
                catalog_columns(client, &mapping.database, &catalog.table).await?;
            require_column(
                &catalog_columns,
                &catalog.table,
                &catalog.value_table_column,
                Some(name_type_ok),
                false,
            )?;
            require_column(
                &catalog_columns,
                &catalog.table,
                &catalog.start_column,
                Some(time_type_ok),
                false,
            )?;
            require_column(
                &catalog_columns,
                &catalog.table,
                &catalog.end_column,
                Some(time_type_ok),
                false,
            )?;
            let partitions = load_partitions(client, mapping).await?;
            if partitions.is_empty() {
                return Err(Error::new("SQL historian partition catalog is empty"));
            }
            ordered_partition_ranges(&partitions)?;
            let mut seen = BTreeSet::new();
            for part in &partitions {
                if seen.insert(part.value_table.as_str()) {
                    check_value_table(client, mapping, &part.value_table, tag_id).await?;
                }
            }
            Ok(())
        }
    }
}

async fn check_value_table(
    client: &mut SqlClient,
    mapping: &Mapping,
    table: &str,
    tag_id: &CatalogColumn,
) -> Result<(), Error> {
    let value_columns = catalog_columns(client, &mapping.database, table).await?;
    let value_tag = require_column(
        &value_columns,
        table,
        &mapping.value_tag_column,
        None,
        false,
    )?;
    require_column(
        &value_columns,
        table,
        &mapping.time_column,
        Some(time_type_ok),
        false,
    )?;
    require_column(
        &value_columns,
        table,
        &mapping.value_column,
        Some(value_type_ok),
        false,
    )?;
    require_column(&value_columns, table, &mapping.quality_column, None, true)?;
    if tag_id.data_type != value_tag.data_type || tag_id.collation != value_tag.collation {
        return Err(Error::new(
            "SQL historian mapping: tag identity columns are not comparable",
        ));
    }
    Ok(())
}

async fn require_unique(
    client: &mut SqlClient,
    schema: &str,
    table: &str,
    column: &str,
) -> Result<(), Error> {
    let mut query = tiberius::Query::new(
        "SELECT i.name, c.name, ic.key_ordinal, i.has_filter FROM sys.indexes i JOIN sys.index_columns ic ON i.object_id=ic.object_id AND i.index_id=ic.index_id JOIN sys.columns c ON ic.object_id=c.object_id AND ic.column_id=c.column_id JOIN sys.tables t ON i.object_id=t.object_id JOIN sys.schemas s ON t.schema_id=s.schema_id WHERE s.name=@P1 AND t.name=@P2 AND i.is_unique=1 AND ic.is_included_column=0",
    );
    query.bind(schema);
    query.bind(table);
    let rows = query_rows(client, query).await?;
    let mut indexes: BTreeMap<String, (bool, Vec<(i32, String)>)> = BTreeMap::new();
    for row in rows {
        let name = match optional_text(&row, 0)? {
            Some(name) => name,
            None => continue,
        };
        let column_name = required_text(&row, 1)?;
        let seq = required_i32(&row, 2)?;
        let filtered = required_flag(&row, 3)?;
        let entry = indexes.entry(name).or_insert((filtered, Vec::new()));
        entry.0 |= filtered;
        entry.1.push((seq, column_name));
    }
    let unique = indexes.values().any(|(filtered, columns)| {
        if *filtered {
            return false;
        }
        let mut columns = columns.clone();
        columns.sort_by_key(|(seq, _)| *seq);
        matches!(columns.as_slice(), [(_, only)] if only == column)
    });
    if unique {
        Ok(())
    } else {
        Err(Error::new(format!(
            "SQL historian mapping: column {table}.{column} is not unique"
        )))
    }
}

struct CatalogColumn {
    data_type: String,
    nullable: bool,
    collation: Option<String>,
}

async fn catalog_columns(
    client: &mut SqlClient,
    schema: &str,
    table: &str,
) -> Result<BTreeMap<String, CatalogColumn>, Error> {
    let mut present = tiberius::Query::new(
        "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=@P1 AND TABLE_NAME=@P2 AND TABLE_TYPE='BASE TABLE'",
    );
    present.bind(schema);
    present.bind(table);
    if query_rows(client, present).await?.is_empty() {
        return Err(Error::new(format!(
            "SQL historian mapping: table {table} is missing"
        )));
    }
    let mut query = tiberius::Query::new(
        "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLLATION_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=@P1 AND TABLE_NAME=@P2",
    );
    query.bind(schema);
    query.bind(table);
    query_rows(client, query)
        .await?
        .into_iter()
        .map(|row| {
            Ok((
                required_text(&row, 0)?,
                CatalogColumn {
                    data_type: required_text(&row, 1)?.to_ascii_lowercase(),
                    nullable: required_text(&row, 2)? == "YES",
                    collation: optional_text(&row, 3)?,
                },
            ))
        })
        .collect()
}

fn require_column<'a>(
    columns: &'a BTreeMap<String, CatalogColumn>,
    table: &str,
    column: &str,
    type_ok: Option<fn(&str) -> bool>,
    allow_null: bool,
) -> Result<&'a CatalogColumn, Error> {
    match columns.get(column) {
        None => Err(Error::new(format!(
            "SQL historian mapping: column {table}.{column} is missing"
        ))),
        Some(found) if type_ok.is_some_and(|check| !check(&found.data_type)) => Err(Error::new(
            format!("SQL historian mapping: column {table}.{column} has the wrong type"),
        )),
        Some(found) if found.nullable && !allow_null => Err(Error::new(format!(
            "SQL historian mapping: column {table}.{column} is nullable"
        ))),
        Some(found) => Ok(found),
    }
}

fn time_type_ok(data_type: &str) -> bool {
    matches!(
        data_type,
        "datetime" | "datetime2" | "datetimeoffset" | "smalldatetime"
    )
}

fn value_type_ok(data_type: &str) -> bool {
    matches!(
        data_type,
        "tinyint"
            | "smallint"
            | "int"
            | "bigint"
            | "float"
            | "real"
            | "decimal"
            | "numeric"
            | "money"
            | "smallmoney"
    )
}

fn name_type_ok(data_type: &str) -> bool {
    matches!(
        data_type,
        "char" | "varchar" | "nchar" | "nvarchar" | "text" | "ntext"
    )
}

async fn load_tags(client: &mut SqlClient, mapping: &Mapping) -> Result<Vec<MappedTag>, Error> {
    let sql = format!(
        "SELECT {}, {} FROM {} ORDER BY {}",
        ident(&mapping.tag_id_column),
        ident(&mapping.tag_name_column),
        qualified(&mapping.database, &mapping.tag_table),
        ident(&mapping.tag_name_column)
    );
    let rows = query_rows(client, tiberius::Query::new(sql)).await?;
    let mut tags = Vec::with_capacity(rows.len());
    let mut seen = BTreeSet::new();
    for row in rows {
        let id = tag_id(&row, 0)?;
        let name = optional_text(&row, 1)?
            .filter(|name| !name.is_empty())
            .ok_or_else(|| Error::new("SQL historian tag table has an empty tag name"))?;
        if !seen.insert(name.clone()) {
            return Err(Error::new("SQL historian tag table reports a tag twice"));
        }
        tags.push(MappedTag { name, id });
    }
    Ok(tags)
}

async fn load_partitions(
    client: &mut SqlClient,
    mapping: &Mapping,
) -> Result<Vec<PartitionRange>, Error> {
    let catalog = mapping
        .partition_catalog
        .as_ref()
        .ok_or_else(|| Error::new("SQL historian partition catalog is not mapped"))?;
    let sql = format!(
        "SELECT {}, {}, {} FROM {}",
        ident(&catalog.value_table_column),
        ident(&catalog.start_column),
        ident(&catalog.end_column),
        qualified(&mapping.database, &catalog.table)
    );
    let rows = query_rows(client, tiberius::Query::new(sql)).await?;
    let mut partitions = Vec::with_capacity(rows.len());
    for row in rows {
        let value_table = required_text(&row, 0)?;
        mapping_field("partition value table", &value_table)?;
        partitions.push(PartitionRange {
            value_table,
            start_ms: catalog_timestamp_ms(&row, 1)?,
            end_ms: catalog_timestamp_ms(&row, 2)?,
        });
    }
    Ok(partitions)
}

async fn read_raw_samples(
    client: &mut SqlClient,
    mapping: &Mapping,
    tag: &str,
    tag_id: &TagId,
    after_ms: i64,
    until_ms: i64,
    limit: usize,
) -> Result<Vec<Sample>, Error> {
    if mapping.partition_catalog.is_none() {
        return read_samples(client, mapping, tag, tag_id, after_ms, until_ms, limit).await;
    }
    if limit == 0 || after_ms >= until_ms {
        return Ok(Vec::new());
    }
    let before = load_partitions(client, mapping).await?;
    let windows = partitioned_raw_windows(after_ms, until_ms, &before)?;
    let mut samples = Vec::new();
    let mut remaining = limit;
    let mut after = after_ms;
    let mut used = Vec::new();
    for window in windows {
        if remaining == 0 {
            break;
        }
        let mut page_mapping = mapping.clone();
        page_mapping.value_table = window.value_table.clone();
        let page = read_samples(
            client,
            &page_mapping,
            tag,
            tag_id,
            after.max(window.after_ms),
            window.until_ms,
            remaining,
        )
        .await?;
        if let Some(last) = page.last() {
            after = last.ts_ms;
        }
        remaining -= page.len();
        samples.extend(page);
        used.push(window);
    }
    let after_snap = load_partitions(client, mapping).await?;
    if partition_set_rotated(&before, &after_snap, &used) {
        return Err(Error::new(
            "SQL historian partition catalog changed during the read",
        ));
    }
    Ok(samples)
}

async fn read_samples(
    client: &mut SqlClient,
    mapping: &Mapping,
    tag: &str,
    tag_id: &TagId,
    after_ms: i64,
    until_ms: i64,
    limit: usize,
) -> Result<Vec<Sample>, Error> {
    if limit == 0 || after_ms >= until_ms {
        return Ok(Vec::new());
    }
    let sql = format!(
        "SELECT TOP (@P4) {time}, CAST({value} AS float), {quality} FROM {values} WHERE {tag_id_col} = @P1 AND {time} >= @P2 AND {time} < @P3 ORDER BY {time} ASC",
        time = ident(&mapping.time_column),
        value = ident(&mapping.value_column),
        quality = ident(&mapping.quality_column),
        values = qualified(&mapping.database, &mapping.value_table),
        tag_id_col = ident(&mapping.value_tag_column),
    );
    // Millisecond watermarks: (after_ms, until_ms] is actual times in
    // [after_ms+1ms, until_ms+1ms), so a datetime2 sample is not re-selected
    // after its truncated millisecond is committed.
    let after = next_dt(after_ms)?;
    let until = next_dt(until_ms)?;
    let want = i64::try_from(limit.saturating_add(1)).unwrap_or(i64::MAX);
    let mut query = tiberius::Query::new(sql);
    bind_tag_id(&mut query, tag_id);
    query.bind(after);
    query.bind(until);
    query.bind(want);
    let rows = query_rows(client, query).await?;
    let mut samples = Vec::with_capacity(rows.len().min(limit));
    let mut previous = after_ms;
    for row in rows {
        let ts_ms = timestamp_ms(&row, 0)?;
        if ts_ms <= previous || ts_ms > until_ms {
            return Err(Error::new(format!(
                "SQL historian returned tag {tag} samples out of order or range"
            )));
        }
        previous = ts_ms;
        samples.push(Sample {
            ts_ms,
            value: number(&row, 1)?,
            quality: quality_text(&row, 2)?,
        });
    }
    if samples.len() > limit {
        samples.truncate(limit);
    }
    Ok(samples)
}

fn bind_tag_id<'a>(query: &mut tiberius::Query<'a>, tag_id: &'a TagId) {
    match tag_id {
        TagId::I32(value) => query.bind(*value),
        TagId::I64(value) => query.bind(*value),
        TagId::Text(value) => query.bind(value.as_str()),
    }
}

fn next_dt(ms: i64) -> Result<NaiveDateTime, Error> {
    DateTime::from_timestamp_millis(
        ms.checked_add(1)
            .ok_or_else(|| Error::new("SQL historian timestamp is out of range"))?,
    )
    .map(|time| time.naive_utc())
    .ok_or_else(|| Error::new("SQL historian timestamp is out of range"))
}

fn catalog_timestamp_ms(row: &Row, index: usize) -> Result<i64, Error> {
    let ts_ms = timestamp_ms(row, index)?;
    let aligned = if let Ok(Some(value)) = row.try_get::<NaiveDateTime, _>(index) {
        value.and_utc().timestamp_subsec_nanos() % 1_000_000 == 0
    } else if let Ok(Some(value)) = row.try_get::<DateTime<Utc>, _>(index) {
        value.timestamp_subsec_nanos() % 1_000_000 == 0
    } else if let Ok(Some(value)) = row.try_get::<DateTime<FixedOffset>, _>(index) {
        value.timestamp_subsec_nanos() % 1_000_000 == 0
    } else {
        false
    };
    if aligned {
        Ok(ts_ms)
    } else {
        Err(Error::new(
            "SQL historian partition catalog range is not millisecond-aligned",
        ))
    }
}

fn timestamp_ms(row: &Row, index: usize) -> Result<i64, Error> {
    if let Ok(Some(value)) = row.try_get::<NaiveDateTime, _>(index) {
        return Ok(value.and_utc().timestamp_millis());
    }
    if let Ok(Some(value)) = row.try_get::<DateTime<Utc>, _>(index) {
        return Ok(value.timestamp_millis());
    }
    if let Ok(Some(value)) = row.try_get::<DateTime<FixedOffset>, _>(index) {
        return Ok(value.timestamp_millis());
    }
    Err(Error::new(
        "SQL historian timestamp representation is unsupported",
    ))
}

fn number(row: &Row, index: usize) -> Result<f64, Error> {
    match row.try_get::<f64, _>(index).map_err(sql_error)? {
        Some(value) if value.is_finite() => Ok(value),
        Some(_) => Err(Error::new("SQL historian value is not numeric")),
        None => Err(Error::new("SQL historian value is null")),
    }
}

fn quality_text(row: &Row, index: usize) -> Result<Option<String>, Error> {
    if let Ok(value) = row.try_get::<&str, _>(index) {
        return Ok(value.map(str::to_owned));
    }
    if let Ok(value) = row.try_get::<i32, _>(index) {
        return Ok(value.map(|value| value.to_string()));
    }
    if let Ok(value) = row.try_get::<i64, _>(index) {
        return Ok(value.map(|value| value.to_string()));
    }
    Err(Error::new(
        "SQL historian quality representation is unsupported",
    ))
}

fn tag_id(row: &Row, index: usize) -> Result<TagId, Error> {
    if let Ok(Some(value)) = row.try_get::<i32, _>(index) {
        return Ok(TagId::I32(value));
    }
    if let Ok(Some(value)) = row.try_get::<i64, _>(index) {
        return Ok(TagId::I64(value));
    }
    if let Ok(Some(value)) = row.try_get::<i16, _>(index) {
        return Ok(TagId::I32(i32::from(value)));
    }
    if let Ok(Some(value)) = row.try_get::<u8, _>(index) {
        return Ok(TagId::I32(i32::from(value)));
    }
    if let Ok(Some(value)) = row.try_get::<&str, _>(index) {
        if value.is_empty() {
            return Err(Error::new("SQL historian tag table has a null identity"));
        }
        return Ok(TagId::Text(value.to_owned()));
    }
    Err(Error::new("SQL historian tag table has a null identity"))
}

async fn query_rows(client: &mut SqlClient, query: tiberius::Query<'_>) -> Result<Vec<Row>, Error> {
    query
        .query(client)
        .await
        .map_err(sql_error)?
        .into_first_result()
        .await
        .map_err(sql_error)
}

fn required_text(row: &Row, index: usize) -> Result<String, Error> {
    optional_text(row, index)?.ok_or_else(|| Error::new("SQL historian text column is null"))
}

fn optional_text(row: &Row, index: usize) -> Result<Option<String>, Error> {
    match row.try_get::<&str, _>(index).map_err(sql_error)? {
        Some(text) => Ok(Some(text.to_owned())),
        None => Ok(None),
    }
}

fn required_i32(row: &Row, index: usize) -> Result<i32, Error> {
    if let Ok(Some(value)) = row.try_get::<i32, _>(index) {
        return Ok(value);
    }
    if let Ok(Some(value)) = row.try_get::<i16, _>(index) {
        return Ok(i32::from(value));
    }
    if let Ok(Some(value)) = row.try_get::<u8, _>(index) {
        return Ok(i32::from(value));
    }
    Err(Error::new("SQL historian integer column is missing"))
}

fn required_flag(row: &Row, index: usize) -> Result<bool, Error> {
    if let Ok(Some(value)) = row.try_get::<bool, _>(index) {
        return Ok(value);
    }
    if let Ok(Some(value)) = row.try_get::<u8, _>(index) {
        return Ok(value != 0);
    }
    Err(Error::new("SQL historian flag column is missing"))
}

fn sql_error(error: tiberius::error::Error) -> Error {
    match error {
        tiberius::error::Error::Server(server)
            if matches!(server.code(), 229 | 230 | 262 | 916 | 18456 | 4060) =>
        {
            Error::new("SQL historian authorization failed; verify SELECT on the mapped tables")
        }
        _ => Error::new("SQL historian query failed"),
    }
}

/// The synchronous API blocks on its own runtime, which panics inside another
/// one. Every entry point that blocks calls this first.
fn outside_runtime() -> Result<(), Error> {
    if tokio::runtime::Handle::try_current().is_ok() {
        return Err(Error::new(
            "call SQL historian synchronous API outside an async runtime",
        ));
    }
    Ok(())
}

impl Historian for SqlHistorian {
    fn tags(&mut self) -> Result<Vec<Tag>, Error> {
        Ok(self
            .tags
            .iter()
            .map(|tag| Tag {
                name: tag.name.clone(),
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
        let mapped = self
            .tags
            .iter()
            .find(|known| known.name == tag)
            .ok_or_else(|| Error::new(format!("SQL historian tag {tag} is not in the mapping")))?;
        let tag_id = mapped.id.clone();
        let tag = mapped.name.clone();
        outside_runtime()?;
        self.runtime.block_on(async {
            tokio::time::timeout(self.config.timeout, async {
                let mut client = connection(&self.config).await?;
                read_raw_samples(
                    &mut client,
                    &self.config.mapping,
                    &tag,
                    &tag_id,
                    after_ms,
                    until_ms,
                    limit,
                )
                .await
            })
            .await
            .map_err(|_| Error::new("SQL historian read timed out"))?
        })
    }

    fn read_interpolated(
        &mut self,
        _tag: &str,
        _start_ms: i64,
        _end_ms: i64,
        _step_ms: i64,
    ) -> Result<Vec<Sample>, Error> {
        Err(Error::new(
            "SQL historian interpolated reads are not offered by this adapter",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    fn config(mapping: Mapping, allow_insecure_lab: bool) -> Config {
        Config {
            host: "127.0.0.1".into(),
            port: 1,
            user: "hashi".into(),
            password: "fixture".into(),
            database: "plant".into(),
            mapping,
            timeout: Duration::from_secs(1),
            allow_insecure_lab,
        }
    }

    fn refused(config: Config) -> Error {
        match SqlHistorian::connect(config) {
            Ok(_) => panic!("expected mapping or TLS to be refused"),
            Err(error) => error,
        }
    }

    #[test]
    fn empty_mapping_fields_are_refused() {
        let mut mapping = mapping();
        mapping.tag_table.clear();
        let error = refused(config(mapping, true));
        assert!(error.message.contains("tag_table"), "{}", error.message);
    }

    #[test]
    fn value_table_and_partition_catalog_are_mutually_exclusive() {
        let mut both = mapping();
        both.partition_catalog = Some(PartitionCatalog {
            table: "partitions".into(),
            value_table_column: "value_table".into(),
            start_column: "start_ts".into(),
            end_column: "end_ts".into(),
        });
        let error = refused(config(both, true));
        assert!(
            error.message.contains("mutually exclusive"),
            "{}",
            error.message
        );

        let mut neither = mapping();
        neither.value_table.clear();
        let error = refused(config(neither, true));
        assert!(error.message.contains("value_table"), "{}", error.message);
    }

    #[test]
    fn empty_connection_fields_are_refused() {
        let mut cfg = config(mapping(), true);
        cfg.host.clear();
        let error = refused(cfg);
        assert!(
            error
                .message
                .contains("invalid SQL historian configuration"),
            "{}",
            error.message
        );
    }

    #[test]
    fn zero_timeout_is_refused() {
        let mut cfg = config(mapping(), true);
        cfg.timeout = Duration::ZERO;
        let error = refused(cfg);
        assert!(
            error
                .message
                .contains("invalid SQL historian configuration"),
            "{}",
            error.message
        );
    }

    #[test]
    fn time_and_value_column_types_are_classified() {
        assert!(time_type_ok("datetime2"));
        assert!(time_type_ok("datetimeoffset"));
        assert!(!time_type_ok("varchar"));
        assert!(value_type_ok("float"));
        assert!(value_type_ok("decimal"));
        assert!(!value_type_ok("nvarchar"));
        assert!(name_type_ok("nvarchar"));
        assert!(!name_type_ok("int"));
    }

    #[test]
    fn timestamp_millis_use_utc() {
        assert_eq!(
            next_dt(1_700_000_001_000).unwrap(),
            NaiveDateTime::parse_from_str("2023-11-14 22:13:21.001000", "%Y-%m-%d %H:%M:%S%.6f")
                .unwrap()
        );
    }

    #[test]
    fn blocking_calls_inside_a_runtime_return_an_error() {
        let mut historian = SqlHistorian {
            config: config(mapping(), true),
            runtime: tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap(),
            tags: vec![MappedTag {
                name: "line.temp".into(),
                id: TagId::I32(1),
            }],
        };
        let outer = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let (check, read) = outer.block_on(async {
            (
                historian.check().map(|_| ()),
                historian.read_raw("line.temp", 0, 1, 1).map(|_| ()),
            )
        });
        for result in [check, read] {
            let error = result.unwrap_err();
            assert!(
                error.message.contains("outside an async runtime"),
                "{}",
                error.message
            );
        }
    }
}
