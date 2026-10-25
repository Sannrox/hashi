//! Generic SQL historian behind the historian trait, MySQL mapping only (#24).
//!
//! [`SqlHistorian`] discovers tags from a tag table and reads samples from a
//! value table by the mapped identity column, not the name, so a
//! case-insensitive collation cannot alias two tags. The mapping is checked
//! against the catalog before any read: tables, columns, types, and unique
//! identity and name columns. Interpolated reads are not offered: SQL tables
//! do not define a processed history. SQL Server mapping is a later adapter.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use mysql_async::{Conn, Opts, Params, Value, prelude::Queryable};

use super::schema::{ident, qualified};
use crate::historian::{Historian, Sample, Tag};
use crate::{CheckReport, Error};

/// One mapping from catalog tables onto tags and samples.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mapping {
    pub database: String,
    pub tag_table: String,
    pub tag_id_column: String,
    pub tag_name_column: String,
    pub value_table: String,
    pub value_tag_column: String,
    pub time_column: String,
    pub value_column: String,
    pub quality_column: String,
}

pub struct Config {
    pub options: Opts,
    pub mapping: Mapping,
    pub timeout: Duration,
    pub allow_insecure_lab: bool,
}

struct MappedTag {
    name: String,
    id: Value,
}

pub struct SqlHistorian {
    config: Config,
    runtime: tokio::runtime::Runtime,
    tags: Vec<MappedTag>,
}

impl SqlHistorian {
    /// Connect, refuse a mapping that does not match the catalog, and load tags.
    pub fn connect(config: Config) -> Result<Self, Error> {
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err(Error::new(
                "call SQL historian synchronous API outside an async runtime",
            ));
        }
        validate_mapping(&config.mapping)?;
        if !config.allow_insecure_lab
            && config.options.ssl_opts().is_none_or(|ssl| {
                ssl.accept_invalid_certs()
                    || ssl.skip_domain_validation()
                    || ssl.tls_hostname_override().is_some()
            })
        {
            return Err(Error::new(
                "SQL historian requires verified TLS; plaintext is restricted to explicit lab configuration",
            ));
        }
        if config.timeout.is_zero() {
            return Err(Error::new("invalid SQL historian configuration"));
        }
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()?;
        let tags = runtime.block_on(async {
            tokio::time::timeout(config.timeout, async {
                let mut conn = connection(&config).await?;
                check_mapping(&mut conn, &config.mapping).await?;
                let tags = load_tags(&mut conn, &config.mapping).await?;
                conn.disconnect().await.map_err(sql_error)?;
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
        self.runtime.block_on(async {
            tokio::time::timeout(self.config.timeout, async {
                let mut conn = connection(&self.config).await?;
                check_mapping(&mut conn, &self.config.mapping).await?;
                conn.disconnect().await.map_err(sql_error)?;
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

fn validate_mapping(mapping: &Mapping) -> Result<(), Error> {
    let fields = [
        ("database", mapping.database.as_str()),
        ("tag_table", mapping.tag_table.as_str()),
        ("tag_id_column", mapping.tag_id_column.as_str()),
        ("tag_name_column", mapping.tag_name_column.as_str()),
        ("value_table", mapping.value_table.as_str()),
        ("value_tag_column", mapping.value_tag_column.as_str()),
        ("time_column", mapping.time_column.as_str()),
        ("value_column", mapping.value_column.as_str()),
        ("quality_column", mapping.quality_column.as_str()),
    ];
    for (field, value) in fields {
        if value.is_empty() || value.len() > 64 || value.contains('\0') {
            return Err(Error::new(format!(
                "invalid SQL historian mapping: {field}"
            )));
        }
    }
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
    Ok(())
}

async fn connection(config: &Config) -> Result<Conn, Error> {
    let mut conn = Conn::new(config.options.clone()).await.map_err(sql_error)?;
    conn.query_drop("SET SESSION time_zone='+00:00'")
        .await
        .map_err(sql_error)?;
    Ok(conn)
}

async fn check_mapping(conn: &mut Conn, mapping: &Mapping) -> Result<(), Error> {
    let tag_columns = catalog_columns(conn, &mapping.database, &mapping.tag_table).await?;
    let value_columns = catalog_columns(conn, &mapping.database, &mapping.value_table).await?;
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
    let value_tag = require_column(
        &value_columns,
        &mapping.value_table,
        &mapping.value_tag_column,
        None,
        false,
    )?;
    require_column(
        &value_columns,
        &mapping.value_table,
        &mapping.time_column,
        Some(time_type_ok),
        false,
    )?;
    require_column(
        &value_columns,
        &mapping.value_table,
        &mapping.value_column,
        Some(value_type_ok),
        false,
    )?;
    require_column(
        &value_columns,
        &mapping.value_table,
        &mapping.quality_column,
        None,
        true,
    )?;
    if tag_id.data_type != value_tag.data_type || tag_id.collation != value_tag.collation {
        return Err(Error::new(
            "SQL historian mapping: tag identity columns are not comparable",
        ));
    }
    require_unique(
        conn,
        &mapping.database,
        &mapping.tag_table,
        &mapping.tag_id_column,
    )
    .await?;
    require_unique(
        conn,
        &mapping.database,
        &mapping.tag_table,
        &mapping.tag_name_column,
    )
    .await?;
    Ok(())
}

async fn require_unique(
    conn: &mut Conn,
    database: &str,
    table: &str,
    column: &str,
) -> Result<(), Error> {
    let rows: Vec<(String, Option<String>, u32)> = conn
        .exec(
            "SELECT INDEX_NAME, COLUMN_NAME, SEQ_IN_INDEX FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND NON_UNIQUE=0",
            (database, table),
        )
        .await
        .map_err(sql_error)?;
    let mut indexes: BTreeMap<String, Vec<(u32, String)>> = BTreeMap::new();
    let mut functional = BTreeSet::new();
    for (name, column_name, seq) in rows {
        // Functional unique indexes report a NULL column name; they do not
        // prove a mapped identity column is unique, even as a prefix.
        match column_name {
            Some(column_name) => indexes.entry(name).or_default().push((seq, column_name)),
            None => {
                functional.insert(name);
            }
        }
    }
    for name in functional {
        indexes.remove(&name);
    }
    let unique = indexes.values().any(|columns| {
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
    conn: &mut Conn,
    database: &str,
    table: &str,
) -> Result<BTreeMap<String, CatalogColumn>, Error> {
    let present: Option<String> = conn
        .exec_first(
            "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?",
            (database, table),
        )
        .await
        .map_err(sql_error)?;
    if present.is_none() {
        return Err(Error::new(format!(
            "SQL historian mapping: table {table} is missing"
        )));
    }
    let rows: Vec<(String, String, String, Option<String>)> = conn
        .exec(
            "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLLATION_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=?",
            (database, table),
        )
        .await
        .map_err(sql_error)?;
    Ok(rows
        .into_iter()
        .map(|(name, data_type, nullable, collation)| {
            (
                name,
                CatalogColumn {
                    data_type,
                    nullable: nullable == "YES",
                    collation,
                },
            )
        })
        .collect())
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
    matches!(data_type, "timestamp" | "datetime")
}

fn value_type_ok(data_type: &str) -> bool {
    matches!(
        data_type,
        "tinyint"
            | "smallint"
            | "mediumint"
            | "int"
            | "integer"
            | "bigint"
            | "float"
            | "double"
            | "real"
            | "decimal"
            | "numeric"
    )
}

fn name_type_ok(data_type: &str) -> bool {
    matches!(
        data_type,
        "char" | "varchar" | "tinytext" | "text" | "mediumtext" | "longtext"
    )
}

async fn load_tags(conn: &mut Conn, mapping: &Mapping) -> Result<Vec<MappedTag>, Error> {
    let sql = format!(
        "SELECT {}, {} FROM {} ORDER BY {}",
        ident(&mapping.tag_id_column),
        ident(&mapping.tag_name_column),
        qualified(&mapping.database, &mapping.tag_table),
        ident(&mapping.tag_name_column)
    );
    let rows: Vec<(Value, Option<String>)> = conn.query(sql).await.map_err(sql_error)?;
    let mut tags = Vec::with_capacity(rows.len());
    let mut seen = BTreeSet::new();
    for (id, name) in rows {
        if matches!(id, Value::NULL) {
            return Err(Error::new("SQL historian tag table has a null identity"));
        }
        let name = name
            .filter(|name| !name.is_empty())
            .ok_or_else(|| Error::new("SQL historian tag table has an empty tag name"))?;
        if !seen.insert(name.clone()) {
            return Err(Error::new("SQL historian tag table reports a tag twice"));
        }
        tags.push(MappedTag { name, id });
    }
    Ok(tags)
}

async fn read_samples(
    conn: &mut Conn,
    mapping: &Mapping,
    tag: &str,
    tag_id: &Value,
    after_ms: i64,
    until_ms: i64,
    limit: usize,
) -> Result<Vec<Sample>, Error> {
    if limit == 0 || after_ms >= until_ms {
        return Ok(Vec::new());
    }
    let want = limit.saturating_add(1);
    let sql = format!(
        "SELECT {time}, {value}, {quality} FROM {values} WHERE {tag_id_col} = ? AND {time} >= ? AND {time} < ? ORDER BY {time} ASC LIMIT ?",
        time = ident(&mapping.time_column),
        value = ident(&mapping.value_column),
        quality = ident(&mapping.quality_column),
        values = qualified(&mapping.database, &mapping.value_table),
        tag_id_col = ident(&mapping.value_tag_column),
    );
    // Millisecond watermarks: (after_ms, until_ms] is actual times in
    // [after_ms+1ms, until_ms+1ms), so a TIMESTAMP(6) sample is not
    // re-selected after its truncated millisecond is committed.
    let after = next_ms(after_ms)?;
    let until = next_ms(until_ms)?;
    let rows: Vec<(Value, Value, Value)> = conn
        .exec(
            sql,
            Params::Positional(vec![
                tag_id.clone(),
                Value::from(after),
                Value::from(until),
                Value::from(want as u64),
            ]),
        )
        .await
        .map_err(sql_error)?;
    let mut samples = Vec::with_capacity(rows.len().min(limit));
    let mut previous = after_ms;
    for (time, value, quality) in rows {
        let ts_ms = timestamp_ms(&time)?;
        if ts_ms <= previous || ts_ms > until_ms {
            return Err(Error::new(format!(
                "SQL historian returned tag {tag} samples out of order or range"
            )));
        }
        previous = ts_ms;
        samples.push(Sample {
            ts_ms,
            value: number(&value)?,
            quality: quality_text(&quality)?,
        });
    }
    if samples.len() > limit {
        samples.truncate(limit);
    }
    Ok(samples)
}

fn next_ms(ms: i64) -> Result<String, Error> {
    format_ms(
        ms.checked_add(1)
            .ok_or_else(|| Error::new("SQL historian timestamp is out of range"))?,
    )
}

fn format_ms(ms: i64) -> Result<String, Error> {
    let time = chrono::DateTime::from_timestamp_millis(ms)
        .ok_or_else(|| Error::new("SQL historian timestamp is out of range"))?;
    Ok(time.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
}

fn timestamp_ms(value: &Value) -> Result<i64, Error> {
    match value {
        Value::Date(year, month, day, hour, min, sec, micros) => {
            let time =
                chrono::NaiveDate::from_ymd_opt(*year as i32, u32::from(*month), u32::from(*day))
                    .and_then(|date| {
                        date.and_hms_micro_opt(
                            u32::from(*hour),
                            u32::from(*min),
                            u32::from(*sec),
                            *micros,
                        )
                    })
                    .ok_or_else(|| Error::new("SQL historian timestamp is invalid"))?;
            Ok(time.and_utc().timestamp_millis())
        }
        _ => Err(Error::new(
            "SQL historian timestamp representation is unsupported",
        )),
    }
}

fn number(value: &Value) -> Result<f64, Error> {
    match value {
        Value::Int(value) => Ok(*value as f64),
        Value::UInt(value) => Ok(*value as f64),
        Value::Float(value) => Ok(f64::from(*value)),
        Value::Double(value) => Ok(*value),
        Value::Bytes(bytes) => std::str::from_utf8(bytes)
            .ok()
            .and_then(|text| text.parse().ok())
            .ok_or_else(|| Error::new("SQL historian value is not numeric")),
        Value::NULL => Err(Error::new("SQL historian value is null")),
        _ => Err(Error::new(
            "SQL historian value representation is unsupported",
        )),
    }
}

fn quality_text(value: &Value) -> Result<Option<String>, Error> {
    match value {
        Value::NULL => Ok(None),
        Value::Bytes(bytes) => {
            Ok(Some(String::from_utf8(bytes.clone()).map_err(|_| {
                Error::new("SQL historian quality is not UTF-8")
            })?))
        }
        Value::Int(value) => Ok(Some(value.to_string())),
        Value::UInt(value) => Ok(Some(value.to_string())),
        _ => Err(Error::new(
            "SQL historian quality representation is unsupported",
        )),
    }
}

fn sql_error(error: mysql_async::Error) -> Error {
    match error {
        mysql_async::Error::Server(server) if matches!(server.code, 1044 | 1045 | 1142 | 1227) => {
            Error::new("SQL historian authorization failed; verify SELECT on the mapped tables")
        }
        _ => Error::new("SQL historian query failed"),
    }
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
        self.runtime.block_on(async {
            tokio::time::timeout(self.config.timeout, async {
                let mut conn = connection(&self.config).await?;
                let samples = read_samples(
                    &mut conn,
                    &self.config.mapping,
                    &tag,
                    &tag_id,
                    after_ms,
                    until_ms,
                    limit,
                )
                .await?;
                conn.disconnect().await.map_err(sql_error)?;
                Ok(samples)
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
    use mysql_async::OptsBuilder;

    fn opts() -> Opts {
        OptsBuilder::default()
            .ip_or_hostname("127.0.0.1")
            .tcp_port(1)
            .user(Some("hashi"))
            .pass(Some("fixture"))
            .into()
    }

    fn mapping() -> Mapping {
        Mapping {
            database: "plant".into(),
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
        let error = refused(Config {
            options: opts(),
            mapping,
            timeout: Duration::from_secs(1),
            allow_insecure_lab: true,
        });
        assert!(error.message.contains("tag_table"), "{}", error.message);
    }

    #[test]
    fn plaintext_is_restricted_to_explicit_lab_configuration() {
        let error = refused(Config {
            options: opts(),
            mapping: mapping(),
            timeout: Duration::from_secs(1),
            allow_insecure_lab: false,
        });
        assert!(error.message.contains("verified TLS"), "{}", error.message);
    }

    #[test]
    fn time_and_value_column_types_are_classified() {
        assert!(time_type_ok("timestamp"));
        assert!(time_type_ok("datetime"));
        assert!(!time_type_ok("varchar"));
        assert!(value_type_ok("double"));
        assert!(value_type_ok("decimal"));
        assert!(!value_type_ok("text"));
        assert!(name_type_ok("varchar"));
        assert!(!name_type_ok("int"));
    }

    #[test]
    fn timestamp_millis_use_utc() {
        let value = Value::Date(2023, 11, 14, 22, 13, 21, 0);
        assert_eq!(timestamp_ms(&value).unwrap(), 1_700_000_001_000);
        assert_eq!(
            format_ms(1_700_000_001_000).unwrap(),
            "2023-11-14 22:13:21.000000"
        );
        assert_eq!(
            next_ms(1_700_000_001_000).unwrap(),
            "2023-11-14 22:13:21.001000"
        );
    }

    #[test]
    fn numeric_and_quality_values_decode() {
        assert_eq!(number(&Value::Double(1.5)).unwrap(), 1.5);
        assert_eq!(number(&Value::Bytes(b"2.25".to_vec())).unwrap(), 2.25);
        assert!(number(&Value::NULL).is_err());
        assert_eq!(quality_text(&Value::NULL).unwrap(), None);
        assert_eq!(
            quality_text(&Value::Bytes(b"good".to_vec()))
                .unwrap()
                .as_deref(),
            Some("good")
        );
    }
}
