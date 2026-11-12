use super::state::Column;
use super::*;
use tokio_postgres::Row;

const BOOL: u32 = 16;
const BYTEA: u32 = 17;
const INT8: u32 = 20;
const INT2: u32 = 21;
const INT4: u32 = 23;
const TEXT: u32 = 25;
const FLOAT4: u32 = 700;
const FLOAT8: u32 = 701;
const BPCHAR: u32 = 1042;
const VARCHAR: u32 = 1043;
const DATE: u32 = 1082;
const TIMESTAMP: u32 = 1114;
const TIMESTAMPTZ: u32 = 1184;
const NUMERIC: u32 = 1700;
const UUID: u32 = 2950;
const JSON: u32 = 114;
const JSONB: u32 = 3802;

#[derive(Clone, Debug, PartialEq)]
pub(super) enum Cell {
    Bool(bool),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    Text(String),
    Bytes(Vec<u8>),
    /// pgoutput `u`: unchanged external TOAST; resolve from the old tuple before emit.
    Unchanged,
}

impl Cell {
    pub(super) fn from_text(oid: u32, text: &str) -> Result<Self, Error> {
        match oid {
            BOOL => match text {
                "t" | "true" => Ok(Cell::Bool(true)),
                "f" | "false" => Ok(Cell::Bool(false)),
                _ => Err(Error::new("PostgreSQL boolean text is invalid")),
            },
            INT2 | INT4 => {
                Ok(Cell::I32(text.parse().map_err(|_| {
                    Error::new("PostgreSQL integer text is invalid")
                })?))
            }
            INT8 => {
                Ok(Cell::I64(text.parse().map_err(|_| {
                    Error::new("PostgreSQL bigint text is invalid")
                })?))
            }
            FLOAT4 => {
                let v: f32 = text
                    .parse()
                    .map_err(|_| Error::new("PostgreSQL real text is invalid"))?;
                if !v.is_finite() {
                    return Err(Error::new("PostgreSQL real is not finite"));
                }
                Ok(Cell::F32(v))
            }
            FLOAT8 => {
                let v: f64 = text
                    .parse()
                    .map_err(|_| Error::new("PostgreSQL double text is invalid"))?;
                if !v.is_finite() {
                    return Err(Error::new("PostgreSQL double is not finite"));
                }
                Ok(Cell::F64(v))
            }
            BYTEA => {
                let hex = text
                    .strip_prefix("\\x")
                    .ok_or_else(|| Error::new("PostgreSQL bytea text is invalid"))?;
                if hex.len() % 2 != 0 {
                    return Err(Error::new("PostgreSQL bytea text is invalid"));
                }
                let mut bytes = Vec::with_capacity(hex.len() / 2);
                let chars: Vec<char> = hex.chars().collect();
                for chunk in chars.chunks(2) {
                    let pair: String = chunk.iter().collect();
                    bytes.push(
                        u8::from_str_radix(&pair, 16)
                            .map_err(|_| Error::new("PostgreSQL bytea text is invalid"))?,
                    );
                }
                Ok(Cell::Bytes(bytes))
            }
            BPCHAR => Ok(Cell::Text(text.trim_end_matches(' ').to_owned())),
            TEXT | VARCHAR | DATE | TIMESTAMP | TIMESTAMPTZ | NUMERIC | UUID | JSON | JSONB => {
                Ok(Cell::Text(text.to_owned()))
            }
            _ => Err(Error::new(
                "PostgreSQL column type is unsupported; refusing to guess a mapping",
            )),
        }
    }
}

pub(super) fn ident(text: &str) -> String {
    format!("\"{}\"", text.replace('"', "\"\""))
}

pub(super) fn qualified(schema: &str, table: &str) -> String {
    format!("{}.{}", ident(schema), ident(table))
}

pub(super) fn field(oid: u32) -> Result<FieldType, Error> {
    Ok(match oid {
        BOOL => FieldType::Bool,
        INT2 | INT4 => FieldType::I32,
        INT8 => FieldType::I64,
        FLOAT4 => FieldType::F32,
        FLOAT8 => FieldType::F64,
        BYTEA => FieldType::Bytes,
        TEXT | VARCHAR | BPCHAR | DATE | TIMESTAMP | TIMESTAMPTZ | NUMERIC | UUID | JSON
        | JSONB => FieldType::Text,
        _ => {
            return Err(Error::new(
                "PostgreSQL column type is unsupported; refusing to guess a mapping",
            ));
        }
    })
}

pub(super) async fn layouts(
    client: &tokio_postgres::Client,
    tables: &[Table],
    revision: u64,
) -> Result<Vec<Layout>, Error> {
    let mut layouts = Vec::new();
    for table in tables {
        let metadata = client
            .query(
                "SELECT a.attname, a.atttypid, a.attnotnull, a.attnum
                 FROM pg_attribute a
                 JOIN pg_class c ON c.oid = a.attrelid
                 JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE n.nspname = $1 AND c.relname = $2
                   AND a.attnum > 0 AND NOT a.attisdropped
                   AND a.attgenerated = ''
                 ORDER BY a.attnum",
                &[&table.schema, &table.table],
            )
            .await
            .map_err(sql_error)?;
        if metadata.is_empty() {
            return Err(Error::new("PostgreSQL table columns are unavailable"));
        }
        let catalog = client
            .query_opt(
                "SELECT c.relreplident, c.relkind
                 FROM pg_class c
                 JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE n.nspname = $1 AND c.relname = $2",
                &[&table.schema, &table.table],
            )
            .await
            .map_err(sql_error)?;
        let Some(catalog) = catalog else {
            return Err(Error::new("PostgreSQL table is missing"));
        };
        let replica: i8 = catalog.get(0);
        let relkind: i8 = catalog.get(1);
        if relkind != b'r' as i8 {
            return Err(Error::new(
                "PostgreSQL capture requires an ordinary table; partitioned tables are refused",
            ));
        }
        // 'f' FULL, 'd' DEFAULT (primary key). Nothing or index-only cannot
        // prove a before image for the selected key.
        if replica != b'f' as i8 && replica != b'd' as i8 {
            return Err(Error::new(
                "PostgreSQL table requires replica identity FULL or DEFAULT with a primary key",
            ));
        }
        let primary = key_columns(client, table).await?;
        let keys = if table.key.is_empty() {
            primary.clone()
        } else {
            table.key.clone()
        };
        if keys.is_empty()
            || keys.iter().any(|key| {
                !metadata.iter().any(|row| {
                    let name: String = row.get(0);
                    let not_null: bool = row.get(2);
                    &name == key && not_null
                })
            })
        {
            return Err(Error::new(
                "PostgreSQL table requires a non-null primary or configured key",
            ));
        }
        if replica == b'd' as i8 && keys != primary {
            return Err(Error::new(
                "PostgreSQL DEFAULT replica identity only covers the primary key; use FULL or the primary key",
            ));
        }
        let unique = unique_keys(client, table).await?;
        if !unique.iter().any(|index| index == &keys) {
            return Err(Error::new(
                "PostgreSQL configured key must match a complete unique constraint",
            ));
        }
        let columns = metadata
            .iter()
            .map(|row| {
                let name: String = row.get(0);
                let oid: u32 = row.get::<_, u32>(1);
                Ok(Column {
                    name,
                    type_oid: oid,
                    field: field(oid)?,
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;
        client
            .query(
                &format!(
                    "SELECT * FROM ONLY {} LIMIT 0",
                    qualified(&table.schema, &table.table)
                ),
                &[],
            )
            .await
            .map_err(sql_error)?;
        layouts.push(Layout {
            schema: table.schema.clone(),
            table: table.table.clone(),
            keys,
            columns,
            version: revision,
        });
    }
    Ok(layouts)
}

async fn unique_keys(
    client: &tokio_postgres::Client,
    table: &Table,
) -> Result<Vec<Vec<String>>, Error> {
    let rows = client
        .query(
            "SELECT i.indexrelid,
                    array_agg(a.attname::text ORDER BY array_position(i.indkey, a.attnum))
             FROM pg_index i
             JOIN pg_class c ON c.oid = i.indrelid
             JOIN pg_namespace n ON n.oid = c.relnamespace
             JOIN pg_attribute a ON a.attrelid = i.indrelid
               AND a.attnum = ANY (i.indkey)
               AND array_position(i.indkey, a.attnum) < i.indnkeyatts
             WHERE n.nspname = $1 AND c.relname = $2
               AND i.indisunique AND i.indisvalid
               AND i.indpred IS NULL AND i.indexprs IS NULL
               AND a.attnum > 0 AND NOT a.attisdropped
             GROUP BY i.indexrelid, i.indnkeyatts
             HAVING count(*) = i.indnkeyatts",
            &[&table.schema, &table.table],
        )
        .await
        .map_err(sql_error)?;
    Ok(rows
        .iter()
        .map(|row| row.get::<_, Vec<String>>(1))
        .collect())
}

async fn key_columns(client: &tokio_postgres::Client, table: &Table) -> Result<Vec<String>, Error> {
    let rows = client
        .query(
            "SELECT a.attname
             FROM pg_index i
             JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY (i.indkey)
               AND array_position(i.indkey, a.attnum) < i.indnkeyatts
             JOIN pg_class c ON c.oid = i.indrelid
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = $1 AND c.relname = $2 AND i.indisprimary
             ORDER BY array_position(i.indkey, a.attnum)",
            &[&table.schema, &table.table],
        )
        .await
        .map_err(sql_error)?;
    Ok(rows.iter().map(|row| row.get(0)).collect())
}

pub(super) fn object(layout: &Layout, values: &[Option<Cell>]) -> Result<serde_json::Value, Error> {
    if values.len() != layout.columns.len() {
        return Err(Error::new(
            "PostgreSQL row does not match discovered schema",
        ));
    }
    let mut object = serde_json::Map::new();
    for (column, value) in layout.columns.iter().zip(values) {
        object.insert(column.name.clone(), scalar(value.as_ref(), column)?);
    }
    Ok(object.into())
}

pub(super) fn key(layout: &Layout, values: &[Option<Cell>]) -> Result<String, Error> {
    let object = object(layout, values)?;
    let parts: Vec<_> = layout.keys.iter().map(|key| &object[key]).collect();
    if parts.iter().any(|v| v.is_null()) {
        return Err(Error::new("PostgreSQL key is null"));
    }
    serde_json::to_string(&parts).map_err(|_| Error::new("PostgreSQL key encoding failed"))
}

pub(super) fn text_cells(row: &Row, layout: &Layout) -> Result<Vec<Option<Cell>>, Error> {
    if row.len() != layout.columns.len() {
        return Err(Error::new(
            "PostgreSQL snapshot row does not match discovered schema",
        ));
    }
    layout
        .columns
        .iter()
        .enumerate()
        .map(|(i, column)| {
            let text: Option<String> = row.try_get(i).map_err(sql_error)?;
            match text {
                None => Ok(None),
                Some(text) => Cell::from_text(column.type_oid, &text).map(Some),
            }
        })
        .collect()
}

fn scalar(value: Option<&Cell>, column: &Column) -> Result<serde_json::Value, Error> {
    let Some(value) = value else {
        return Ok(serde_json::Value::Null);
    };
    match (value, &column.field) {
        (Cell::Bool(v), FieldType::Bool) => Ok(serde_json::json!(v)),
        (Cell::I32(v), FieldType::I32) => Ok(serde_json::json!(v)),
        (Cell::I64(v), FieldType::I64) => Ok(serde_json::json!(v)),
        (Cell::F32(v), FieldType::F32) if v.is_finite() => Ok(serde_json::json!(v)),
        (Cell::F64(v), FieldType::F64) if v.is_finite() => Ok(serde_json::json!(v)),
        (Cell::Text(v), FieldType::Text) => Ok(serde_json::Value::String(v.clone())),
        (Cell::Bytes(v), FieldType::Bytes) => Ok(serde_json::Value::Array(
            v.iter().map(|b| serde_json::json!(b)).collect(),
        )),
        (Cell::Unchanged, _) => Err(Error::new(
            "PostgreSQL unchanged TOAST value was not resolved",
        )),
        _ => Err(Error::new(
            "PostgreSQL row does not match discovered schema",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn boolean_text_accepts_snapshot_and_pgoutput_forms() {
        assert!(matches!(Cell::from_text(BOOL, "t"), Ok(Cell::Bool(true))));
        assert!(matches!(
            Cell::from_text(BOOL, "true"),
            Ok(Cell::Bool(true))
        ));
        assert!(matches!(Cell::from_text(BOOL, "f"), Ok(Cell::Bool(false))));
        assert!(matches!(
            Cell::from_text(BOOL, "false"),
            Ok(Cell::Bool(false))
        ));
        assert!(Cell::from_text(BOOL, "yes").is_err());
        assert_eq!(
            Cell::from_text(BPCHAR, "a   ").unwrap(),
            Cell::Text("a".into())
        );
        assert_eq!(
            Cell::from_text(BPCHAR, "a").unwrap(),
            Cell::Text("a".into())
        );
    }
}
