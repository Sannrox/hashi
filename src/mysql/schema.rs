use super::*;
use mysql_async::{Value, prelude::Queryable};
type CatalogColumn = (String, String, String, Option<String>, String, Option<u16>);

#[derive(Default)]
struct UniqueIndex {
    columns: Vec<String>,
    truncated: bool,
}

pub(super) fn ident(text: &str) -> String {
    format!("`{}`", text.replace('`', "``"))
}
pub(super) fn qualified(database: &str, table: &str) -> String {
    format!("{}.{}", ident(database), ident(table))
}
pub(super) async fn layouts(
    conn: &mut mysql_async::Conn,
    tables: &[Table],
    revision: u64,
) -> Result<Vec<Layout>, Error> {
    let mut layouts = Vec::new();
    for table in tables {
        let engine: Option<String> = conn.exec_first("SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?", (&table.database, &table.table)).await.map_err(sql_error)?;
        if engine.as_deref() != Some("InnoDB") {
            return Err(Error::new("MySQL snapshot requires existing InnoDB tables"));
        }
        let cascades: Option<u64> = conn.exec_first("SELECT COUNT(*) FROM information_schema.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_SCHEMA=? AND TABLE_NAME=? AND (DELETE_RULE NOT IN ('RESTRICT','NO ACTION') OR UPDATE_RULE NOT IN ('RESTRICT','NO ACTION'))", (&table.database, &table.table)).await.map_err(sql_error)?;
        if cascades != Some(0) {
            return Err(Error::new(
                "MySQL cascading foreign keys omit child row events; explicit consumer recovery is required",
            ));
        }
        let metadata: Vec<CatalogColumn> = conn.exec("SELECT c.COLUMN_NAME,c.COLUMN_TYPE,c.IS_NULLABLE,c.CHARACTER_SET_NAME,c.EXTRA,k.ID FROM information_schema.COLUMNS c LEFT JOIN information_schema.COLLATIONS k ON k.COLLATION_NAME=c.COLLATION_NAME WHERE c.TABLE_SCHEMA=? AND c.TABLE_NAME=? ORDER BY c.ORDINAL_POSITION", (&table.database, &table.table)).await.map_err(sql_error)?;
        if metadata.is_empty() {
            return Err(Error::new("MySQL table columns are unavailable"));
        }
        let primary: Vec<String> = conn.exec("SELECT COLUMN_NAME FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND INDEX_NAME='PRIMARY' ORDER BY SEQ_IN_INDEX", (&table.database, &table.table)).await.map_err(sql_error)?;
        let keys = if table.key.is_empty() {
            primary.clone()
        } else {
            table.key.clone()
        };
        if keys.is_empty()
            || keys.iter().any(|key| {
                !metadata
                    .iter()
                    .any(|(name, _, nullable, _, _, _)| name == key && nullable == "NO")
            })
        {
            return Err(Error::new(
                "MySQL table requires a non-null primary or configured key",
            ));
        }
        let unique: Vec<(String,Option<String>,Option<u64>)> = conn.exec("SELECT INDEX_NAME,COLUMN_NAME,SUB_PART FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND NON_UNIQUE=0 ORDER BY INDEX_NAME,SEQ_IN_INDEX", (&table.database, &table.table)).await.map_err(sql_error)?;
        let mut indexes: BTreeMap<String, UniqueIndex> = BTreeMap::new();
        for (index, column, prefix) in unique {
            let index = indexes.entry(index).or_default();
            index.truncated |= prefix.is_some() || column.is_none();
            if let Some(column) = column {
                index.columns.push(column);
            }
        }
        if !indexes
            .values()
            .any(|index| !index.truncated && index.columns == keys)
        {
            return Err(Error::new(
                "MySQL configured key must match a complete unique index without prefixes",
            ));
        }
        if keys.iter().any(|key| {
            metadata.iter().any(|(name, sql_type, _, _, _, _)| {
                name == key
                    && (sql_type.starts_with("enum(")
                        || sql_type.starts_with("set(")
                        || sql_type.starts_with("bit("))
            })
        }) {
            return Err(Error::new(
                "MySQL ENUM/SET/BIT keys have incompatible snapshot ordering; select another complete unique key",
            ));
        }
        let columns = metadata
            .into_iter()
            .map(|(name, sql_type, _, charset, extra, collation)| {
                let field = field(&sql_type);
                if matches!(sql_type.as_str(), "time(1)" | "time(2)") {
                    return Err(Error::new("MySQL TIME(1)/TIME(2) decoding is unsupported; select a safe temporal precision"));
                }
                if extra.contains("VIRTUAL GENERATED")
                    || (field == FieldType::Text
                        && charset.as_deref().is_some_and(|charset| {
                            !matches!(charset, "utf8mb4" | "utf8mb3" | "utf8" | "ascii")
                        }))
                {
                    return Err(Error::new(
                        "MySQL requires stored columns and UTF-8 or ASCII text character sets",
                    ));
                }
                Ok(Column {
                    field,
                    name,
                    sql_type,
                    collation,
                })
            })
            .collect::<Result<_, Error>>()?;
        // Exercise SELECT privilege, and acquire metadata locks in the snapshot transaction.
        conn.query_drop(format!(
            "SELECT * FROM {} LIMIT 0",
            qualified(&table.database, &table.table)
        ))
        .await
        .map_err(sql_error)?;
        layouts.push(Layout {
            database: table.database.clone(),
            table: table.table.clone(),
            keys,
            primary_key: primary,
            columns,
            version: revision,
        });
    }
    Ok(layouts)
}
fn field(sql_type: &str) -> FieldType {
    let base = sql_type.split(['(', ' ']).next().unwrap_or("");
    match base {
        "tinyint" | "smallint" | "mediumint" => FieldType::I32,
        "int" | "integer" if !sql_type.contains("unsigned") => FieldType::I32,
        "int" | "integer" => FieldType::I64,
        "bigint" if !sql_type.contains("unsigned") => FieldType::I64,
        "float" => FieldType::F32,
        "double" | "real" => FieldType::F64,
        "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob" | "bit" => {
            FieldType::Bytes
        }
        _ => FieldType::Text,
    }
}
pub(super) fn object(layout: &Layout, values: &[Value]) -> Result<serde_json::Value, Error> {
    if values.len() != layout.columns.len() {
        return Err(Error::new("MySQL row does not match discovered schema"));
    }
    let mut object = serde_json::Map::new();
    for (column, value) in layout.columns.iter().zip(values) {
        object.insert(column.name.clone(), scalar(value, column)?);
    }
    Ok(object.into())
}
pub(super) fn key(layout: &Layout, values: &[Value]) -> Result<String, Error> {
    let object = object(layout, values)?;
    let parts: Vec<_> = layout.keys.iter().map(|key| &object[key]).collect();
    if parts.iter().any(|v| v.is_null()) {
        return Err(Error::new("MySQL key is null"));
    }
    serde_json::to_string(&parts).map_err(|_| Error::new("MySQL key encoding failed"))
}
fn scalar(value: &Value, column: &Column) -> Result<serde_json::Value, Error> {
    use serde_json::Value as Json;
    let field = &column.field;
    if *value == Value::NULL {
        return Ok(Json::Null);
    }
    if column.sql_type == "json" {
        let Value::Bytes(bytes) = value else {
            return Err(Error::new("MySQL JSON value encoding is invalid"));
        };
        let json: serde_json::Value =
            serde_json::from_slice(bytes).map_err(|_| Error::new("MySQL JSON value is invalid"))?;
        return Ok(json.to_string().into());
    }
    if column.sql_type.starts_with("timestamp") {
        return timestamp(value).map(Into::into);
    }
    if column.sql_type.starts_with("year") {
        let year = match value {
            Value::Int(year) => *year,
            Value::UInt(year) => {
                i64::try_from(*year).map_err(|_| Error::new("MySQL YEAR is out of range"))?
            }
            // The binlog decoder adds 1900 even to the reserved zero byte.
            // 1900 is not a valid YEAR, so it unambiguously denotes zero.
            Value::Bytes(bytes) if bytes == b"1900" => 0,
            Value::Bytes(bytes) => std::str::from_utf8(bytes)
                .ok()
                .and_then(|text| text.parse::<i64>().ok())
                .ok_or_else(|| Error::new("MySQL YEAR encoding is invalid"))?,
            _ => return Err(Error::new("MySQL YEAR representation is unsupported")),
        };
        if year != 0 && !(1901..=2155).contains(&year) {
            return Err(Error::new("MySQL YEAR is out of range"));
        }
        return Ok(year.to_string().into());
    }
    if *field == FieldType::Bytes {
        return match value {
            Value::Bytes(bytes) => Ok(serde_json::json!(bytes)),
            _ => Err(Error::new("MySQL binary value has unexpected encoding")),
        };
    }
    if *field == FieldType::Text {
        return Ok(match value {
            Value::Bytes(bytes) => String::from_utf8(bytes.clone())
                .map_err(|_| Error::new("MySQL text is not UTF-8"))?
                .into(),
            Value::Int(v) => v.to_string().into(),
            Value::UInt(v) => v.to_string().into(),
            Value::Date(y, m, d, h, min, s, micros) => {
                format!("{y:04}-{m:02}-{d:02} {h:02}:{min:02}:{s:02}.{micros:06}").into()
            }
            Value::Time(negative, days, h, m, s, micros) => format!(
                "{}{hours:02}:{m:02}:{s:02}.{micros:06}",
                if *negative { "-" } else { "" },
                hours = *days as u64 * 24 + *h as u64
            )
            .into(),
            _ => return Err(Error::new("MySQL textual value has unexpected encoding")),
        });
    }
    Ok(match value {
        Value::Int(v) => (*v).into(),
        Value::UInt(v) => (*v).into(),
        Value::Float(v) if v.is_finite() => serde_json::json!(v),
        Value::Double(v) if v.is_finite() => serde_json::json!(v),
        // Text-protocol snapshot values are parsed against their declared numeric field.
        Value::Bytes(bytes) => {
            let text = std::str::from_utf8(bytes)
                .map_err(|_| Error::new("MySQL numeric value is not UTF-8"))?;
            let parsed: Json = serde_json::from_str(text)
                .map_err(|_| Error::new("MySQL numeric value is invalid"))?;
            if !parsed.is_number() {
                return Err(Error::new("MySQL numeric value is invalid"));
            }
            parsed
        }
        _ => return Err(Error::new("MySQL numeric value has unexpected encoding")),
    })
}

/// Current catalog metadata may be newer than the consumed DDL. Never decode a
/// historical row under it unless the row's own table map establishes agreement.
pub(super) fn validate_map(
    layout: &Layout,
    map: &mysql_async::binlog::events::TableMapEvent<'_>,
) -> Result<(), Error> {
    use mysql_async::binlog::events::OptionalMetaExtractor;
    use mysql_async::consts::ColumnType::*;
    let metadata = OptionalMetaExtractor::new(map.iter_optional_meta())
        .map_err(|_| Error::new("MySQL table metadata is invalid"))?;
    let names: Vec<String> = metadata
        .iter_column_name()
        .map(|name| {
            let name = name.map_err(|_| Error::new("MySQL historical column names unavailable"))?;
            String::from_utf8(name.name_raw().to_vec())
                .map_err(|_| Error::new("MySQL historical column name is not UTF-8"))
        })
        .collect::<Result<_, _>>()?;
    if !names
        .iter()
        .eq(layout.columns.iter().map(|column| &column.name))
    {
        return Err(Error::new(
            "MySQL historical schema differs from current catalog; explicit schema recovery required",
        ));
    }
    let primary: Vec<_> = metadata
        .iter_primary_key()
        .map(|index| {
            let index = index
                .map_err(|_| Error::new("MySQL historical primary key metadata is invalid"))?;
            let index = usize::try_from(index)
                .map_err(|_| Error::new("MySQL historical primary key index is invalid"))?;
            names
                .get(index)
                .cloned()
                .ok_or_else(|| Error::new("MySQL historical primary key column is missing"))
        })
        .collect::<Result<_, _>>()?;
    if primary != layout.primary_key {
        return Err(Error::new(
            "MySQL historical primary key differs from current catalog; explicit schema recovery required",
        ));
    }
    let mut signedness = metadata.iter_signedness();
    let mut charsets = metadata.iter_charset();
    let mut categories = metadata.iter_enum_and_set_charset();
    for (index, column) in layout.columns.iter().enumerate() {
        let actual = map
            .get_column_type(index)
            .ok()
            .flatten()
            .ok_or_else(|| Error::new("MySQL historical column type missing"))?;
        if actual == MYSQL_TYPE_TIME2
            && map.get_column_metadata(index).is_none_or(|metadata| {
                metadata
                    .first()
                    .is_none_or(|precision| matches!(precision, 1 | 2))
            })
        {
            return Err(Error::new(
                "MySQL historical TIME(1)/TIME(2) decoding is unsupported",
            ));
        }
        if actual.is_numeric_type() {
            let unsigned = signedness
                .next()
                .ok_or_else(|| Error::new("MySQL historical signedness metadata missing"))?;
            if unsigned
                != (column.sql_type.contains("unsigned") || column.sql_type.starts_with("year"))
            {
                return Err(Error::new(
                    "MySQL historical signedness differs from current catalog",
                ));
            }
        }
        if actual.is_character_type() || actual.is_enum_or_set_type() {
            let charset = if actual.is_enum_or_set_type() {
                categories.next()
            } else {
                charsets.next()
            }
            .transpose()
            .map_err(|_| Error::new("MySQL historical character set is invalid"))?
            .ok_or_else(|| Error::new("MySQL historical character set is missing"))?;
            let expected = column
                .collation
                .or_else(|| (column.field == FieldType::Bytes).then_some(63));
            if expected != Some(charset) {
                return Err(Error::new(
                    "MySQL historical collation differs from current catalog; explicit schema recovery required",
                ));
            }
        }
        let base = column.sql_type.split(['(', ' ']).next().unwrap_or("");
        let matches = match base {
            "tinyint" => actual == MYSQL_TYPE_TINY,
            "smallint" => actual == MYSQL_TYPE_SHORT,
            "mediumint" => actual == MYSQL_TYPE_INT24,
            "int" | "integer" => actual == MYSQL_TYPE_LONG,
            "bigint" => actual == MYSQL_TYPE_LONGLONG,
            "float" => actual == MYSQL_TYPE_FLOAT,
            "double" | "real" => actual == MYSQL_TYPE_DOUBLE,
            "decimal" | "numeric" => actual == MYSQL_TYPE_NEWDECIMAL,
            "timestamp" => matches!(actual, MYSQL_TYPE_TIMESTAMP | MYSQL_TYPE_TIMESTAMP2),
            "datetime" => matches!(actual, MYSQL_TYPE_DATETIME | MYSQL_TYPE_DATETIME2),
            "time" => matches!(actual, MYSQL_TYPE_TIME | MYSQL_TYPE_TIME2),
            "date" => matches!(actual, MYSQL_TYPE_DATE | MYSQL_TYPE_NEWDATE),
            "year" => actual == MYSQL_TYPE_YEAR,
            "enum" => actual == MYSQL_TYPE_ENUM,
            "set" => actual == MYSQL_TYPE_SET,
            "json" => actual == MYSQL_TYPE_JSON,
            "bit" => actual == MYSQL_TYPE_BIT,
            "varchar" | "varbinary" => matches!(actual, MYSQL_TYPE_VARCHAR | MYSQL_TYPE_VAR_STRING),
            "char" | "binary" => actual == MYSQL_TYPE_STRING,
            "tinytext" | "text" | "mediumtext" | "longtext" | "tinyblob" | "blob"
            | "mediumblob" | "longblob" => matches!(
                actual,
                MYSQL_TYPE_TINY_BLOB
                    | MYSQL_TYPE_BLOB
                    | MYSQL_TYPE_MEDIUM_BLOB
                    | MYSQL_TYPE_LONG_BLOB
            ),
            _ => false,
        };
        if !matches {
            return Err(Error::new(
                "MySQL historical type differs from current catalog or is unsupported",
            ));
        }
    }
    Ok(())
}

// TIMESTAMP binlog values use decimal epoch seconds, whereas prepared reads
// return calendar components. Normalize both to UTC seconds plus six digits.
fn timestamp(value: &Value) -> Result<String, Error> {
    let (seconds, micros) = match value {
        Value::Date(y, m, d, h, min, s, micros) => {
            let time = chrono::NaiveDate::from_ymd_opt(*y as i32, *m as u32, *d as u32)
                .and_then(|date| date.and_hms_micro_opt(*h as u32, *min as u32, *s as u32, *micros))
                .ok_or_else(|| Error::new("MySQL TIMESTAMP calendar value is invalid"))?;
            (time.and_utc().timestamp(), *micros)
        }
        Value::Int(seconds) => (*seconds, 0),
        Value::UInt(seconds) => (
            i64::try_from(*seconds).map_err(|_| Error::new("MySQL TIMESTAMP is out of range"))?,
            0,
        ),
        Value::Bytes(bytes) => {
            let text = std::str::from_utf8(bytes)
                .map_err(|_| Error::new("MySQL TIMESTAMP encoding is invalid"))?;
            let (seconds, fraction) = text.split_once('.').unwrap_or((text, ""));
            if fraction.len() > 6 || !fraction.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(Error::new("MySQL TIMESTAMP fraction is invalid"));
            }
            (
                seconds
                    .parse::<i64>()
                    .map_err(|_| Error::new("MySQL TIMESTAMP seconds are invalid"))?,
                format!("{fraction:0<6}")
                    .parse::<u32>()
                    .map_err(|_| Error::new("MySQL TIMESTAMP fraction is invalid"))?,
            )
        }
        _ => return Err(Error::new("MySQL TIMESTAMP representation is unsupported")),
    };
    Ok(format!("{seconds}.{micros:06}"))
}

pub(super) enum Labels {
    Enum(Vec<String>),
    Set(Vec<String>),
}
pub(super) fn labels(
    map: &mysql_async::binlog::events::TableMapEvent<'_>,
) -> Result<BTreeMap<usize, Labels>, Error> {
    use mysql_async::binlog::events::OptionalMetadataField;
    use mysql_async::consts::ColumnType::{MYSQL_TYPE_ENUM, MYSQL_TYPE_SET};
    let mut enums = Vec::new();
    let mut sets = Vec::new();
    for field in map.iter_optional_meta() {
        match field.map_err(|_| Error::new("MySQL category metadata is invalid"))? {
            OptionalMetadataField::EnumStrValue(values) => {
                for column in values.iter_values() {
                    let column =
                        column.map_err(|_| Error::new("MySQL enum metadata is invalid"))?;
                    enums.push(
                        column
                            .values()
                            .iter()
                            .map(|value| {
                                String::from_utf8(value.value_raw().to_vec())
                                    .map_err(|_| Error::new("MySQL enum label is not UTF-8"))
                            })
                            .collect::<Result<Vec<_>, _>>()?,
                    );
                }
            }
            OptionalMetadataField::SetStrValue(values) => {
                for column in values.iter_values() {
                    let column = column.map_err(|_| Error::new("MySQL set metadata is invalid"))?;
                    sets.push(
                        column
                            .values()
                            .iter()
                            .map(|value| {
                                String::from_utf8(value.value_raw().to_vec())
                                    .map_err(|_| Error::new("MySQL set label is not UTF-8"))
                            })
                            .collect::<Result<Vec<_>, _>>()?,
                    );
                }
            }
            _ => {}
        }
    }
    let mut enums = enums.into_iter();
    let mut sets = sets.into_iter();
    let mut result = BTreeMap::new();
    for index in 0..map.columns_count() as usize {
        match map.get_column_type(index).ok().flatten() {
            Some(MYSQL_TYPE_ENUM) => {
                result.insert(
                    index,
                    Labels::Enum(
                        enums
                            .next()
                            .ok_or_else(|| Error::new("MySQL historical enum labels missing"))?,
                    ),
                );
            }
            Some(MYSQL_TYPE_SET) => {
                result.insert(
                    index,
                    Labels::Set(
                        sets.next()
                            .ok_or_else(|| Error::new("MySQL historical set labels missing"))?,
                    ),
                );
            }
            _ => {}
        }
    }
    Ok(result)
}
pub(super) fn category(value: Value, labels: Option<&Labels>) -> Result<Value, Error> {
    if value == Value::NULL {
        return Ok(value);
    }
    match labels {
        None => Ok(value),
        Some(Labels::Enum(labels)) => {
            let index = match value {
                Value::Int(v) if v >= 0 => v as u64,
                Value::UInt(v) => v,
                _ => return Err(Error::new("MySQL enum index encoding is invalid")),
            };
            let label = if index == 0 {
                ""
            } else {
                labels
                    .get((index - 1) as usize)
                    .ok_or_else(|| Error::new("MySQL enum index is out of range"))?
            };
            Ok(Value::Bytes(label.as_bytes().to_vec()))
        }
        Some(Labels::Set(labels)) => {
            let Value::Bytes(bytes) = value else {
                return Err(Error::new("MySQL set bitmap encoding is invalid"));
            };
            if bytes.len() > 8 || labels.len() > 64 {
                return Err(Error::new("MySQL set bitmap exceeds bounds"));
            }
            let bits = bytes.iter().enumerate().fold(0u64, |bits, (index, byte)| {
                bits | ((*byte as u64) << (index * 8))
            });
            if labels.len() < 64 && bits >> labels.len() != 0 {
                return Err(Error::new("MySQL set bitmap references unknown labels"));
            }
            let labels: Vec<_> = labels
                .iter()
                .enumerate()
                .filter(|(index, _)| bits & (1u64 << index) != 0)
                .map(|(_, label)| label.as_str())
                .collect();
            Ok(Value::Bytes(labels.join(",").into_bytes()))
        }
    }
}
