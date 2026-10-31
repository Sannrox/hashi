use super::state::Column;
use super::*;
use tiberius::Row;

pub(super) fn ident(text: &str) -> String {
    format!("[{}]", text.replace(']', "]]"))
}
pub(super) fn qualified(schema: &str, table: &str) -> String {
    format!("{}.{}", ident(schema), ident(table))
}
pub(super) fn capture_function(instance: &str) -> Result<String, Error> {
    if instance.is_empty()
        || instance.len() > 128
        || !instance
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Err(Error::new("SQL Server capture instance name is invalid"));
    }
    Ok(format!("cdc.fn_cdc_get_all_changes_{instance}"))
}

#[derive(Clone)]
pub(super) enum Cell {
    Null,
    Bool(bool),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    Text(String),
    Bytes(Vec<u8>),
}

pub(super) fn field(sql_type: &str) -> FieldType {
    match sql_type.to_ascii_lowercase().as_str() {
        "bit" => FieldType::Bool,
        "tinyint" | "smallint" | "int" => FieldType::I32,
        "bigint" => FieldType::I64,
        "real" => FieldType::F32,
        "float" => FieldType::F64,
        "binary" | "varbinary" | "image" | "timestamp" | "rowversion" => FieldType::Bytes,
        _ => FieldType::Text,
    }
}

pub(super) async fn layouts(
    client: &mut SqlClient,
    tables: &[Table],
    revision: u64,
) -> Result<Vec<Layout>, Error> {
    let mut layouts = Vec::new();
    for table in tables {
        let metadata = column_metadata(client, table).await?;
        if metadata.is_empty() {
            return Err(Error::new("SQL Server table columns are unavailable"));
        }
        let primary = key_columns(client, table).await?;
        let keys = if table.key.is_empty() {
            primary.clone()
        } else {
            table.key.clone()
        };
        if keys.is_empty()
            || keys.iter().any(|key| {
                !metadata
                    .iter()
                    .any(|(name, _, nullable)| name == key && nullable == "NO")
            })
            || (!table.key.is_empty() && table.key != primary)
        {
            return Err(Error::new(
                "SQL Server table requires a non-null primary key, or a configured key that matches it",
            ));
        }
        let columns = metadata
            .into_iter()
            .map(|(name, sql_type, _)| Column {
                field: field(&sql_type),
                name,
                sql_type,
            })
            .collect();
        client
            .simple_query(format!(
                "SELECT TOP 0 * FROM {}",
                qualified(&table.schema, &table.table)
            ))
            .await
            .map_err(sql_error)?
            .into_first_result()
            .await
            .map_err(sql_error)?;
        layouts.push(Layout {
            schema: table.schema.clone(),
            table: table.table.clone(),
            keys,
            columns,
            version: revision,
            capture_instance: None,
        });
    }
    Ok(layouts)
}

async fn column_metadata(
    client: &mut SqlClient,
    table: &Table,
) -> Result<Vec<(String, String, String)>, Error> {
    let mut query = tiberius::Query::new(
        "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=@P1 AND TABLE_NAME=@P2 ORDER BY ORDINAL_POSITION",
    );
    query.bind(table.schema.as_str());
    query.bind(table.table.as_str());
    rows(client, query)
        .await?
        .into_iter()
        .map(|row| {
            Ok((
                required_text(&row, 0)?,
                required_text(&row, 1)?,
                required_text(&row, 2)?,
            ))
        })
        .collect()
}

async fn key_columns(client: &mut SqlClient, table: &Table) -> Result<Vec<String>, Error> {
    let mut query = tiberius::Query::new(
        "SELECT kcu.COLUMN_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu ON tc.CONSTRAINT_NAME=kcu.CONSTRAINT_NAME AND tc.TABLE_SCHEMA=kcu.TABLE_SCHEMA AND tc.TABLE_NAME=kcu.TABLE_NAME WHERE tc.TABLE_SCHEMA=@P1 AND tc.TABLE_NAME=@P2 AND tc.CONSTRAINT_TYPE='PRIMARY KEY' ORDER BY kcu.ORDINAL_POSITION",
    );
    query.bind(table.schema.as_str());
    query.bind(table.table.as_str());
    rows(client, query)
        .await?
        .into_iter()
        .map(|row| required_text(&row, 0))
        .collect()
}

pub(super) async fn captured_columns(
    client: &mut SqlClient,
    instance: &str,
) -> Result<Vec<Column>, Error> {
    capture_function(instance)?;
    let mut query = tiberius::Query::new(
        "SELECT cc.column_name, cc.column_type FROM cdc.captured_columns cc JOIN cdc.change_tables ct ON ct.object_id=cc.object_id WHERE ct.capture_instance=@P1 ORDER BY cc.column_ordinal",
    );
    query.bind(instance);
    let columns: Vec<Column> = rows(client, query)
        .await?
        .into_iter()
        .map(|row| {
            let sql_type = required_text(&row, 1)?;
            Ok(Column {
                field: field(&sql_type),
                name: required_text(&row, 0)?,
                sql_type,
            })
        })
        .collect::<Result<_, Error>>()?;
    if columns.is_empty() {
        return Err(Error::new(
            "SQL Server capture instance columns are unavailable",
        ));
    }
    Ok(columns)
}

pub(super) async fn rows(
    client: &mut SqlClient,
    query: tiberius::Query<'_>,
) -> Result<Vec<Row>, Error> {
    query
        .query(client)
        .await
        .map_err(sql_error)?
        .into_first_result()
        .await
        .map_err(sql_error)
}

pub(super) fn required_text(row: &Row, index: usize) -> Result<String, Error> {
    match row.try_get::<&str, _>(index).map_err(sql_error)? {
        Some(text) => Ok(text.to_owned()),
        None => Err(Error::new("SQL Server text column is null")),
    }
}

pub(super) fn binary10(row: &Row, index: usize) -> Result<[u8; 10], Error> {
    let bytes: &[u8] = row
        .try_get(index)
        .map_err(sql_error)?
        .ok_or_else(|| Error::new("SQL Server LSN is missing"))?;
    format_lsn(bytes)?;
    let mut out = [0u8; 10];
    out.copy_from_slice(bytes);
    Ok(out)
}

pub(super) fn object(layout: &Layout, values: &[Cell]) -> Result<serde_json::Value, Error> {
    if values.len() != layout.columns.len() {
        return Err(Error::new(
            "SQL Server row does not match discovered schema",
        ));
    }
    let mut object = serde_json::Map::new();
    for (column, value) in layout.columns.iter().zip(values) {
        object.insert(column.name.clone(), scalar(value, column)?);
    }
    Ok(object.into())
}
pub(super) fn key(layout: &Layout, values: &[Cell]) -> Result<String, Error> {
    let object = object(layout, values)?;
    let parts: Vec<_> = layout.keys.iter().map(|key| &object[key]).collect();
    if parts.iter().any(|v| v.is_null()) {
        return Err(Error::new("SQL Server key is null"));
    }
    serde_json::to_string(&parts).map_err(|_| Error::new("SQL Server key encoding failed"))
}

pub(super) fn cells(row: &Row, start: usize, layout: &Layout) -> Result<Vec<Cell>, Error> {
    layout
        .columns
        .iter()
        .enumerate()
        .map(|(i, column)| cell(row, start + i, column.field.clone()))
        .collect()
}

fn cell(row: &Row, index: usize, field: FieldType) -> Result<Cell, Error> {
    match field {
        FieldType::Bool => Ok(match row.try_get::<bool, _>(index).map_err(sql_error)? {
            None => Cell::Null,
            Some(v) => Cell::Bool(v),
        }),
        FieldType::I32 => match row.try_get::<i32, _>(index).map_err(sql_error)? {
            None => match optional_i32(row, index)? {
                None => Ok(Cell::Null),
                Some(v) => Ok(Cell::I32(v)),
            },
            Some(v) => Ok(Cell::I32(v)),
        },
        FieldType::I64 => match row.try_get::<i64, _>(index).map_err(sql_error)? {
            None => match optional_i32(row, index)? {
                None => Ok(Cell::Null),
                Some(v) => Ok(Cell::I64(i64::from(v))),
            },
            Some(v) => Ok(Cell::I64(v)),
        },
        FieldType::F32 => Ok(match row.try_get::<f32, _>(index).map_err(sql_error)? {
            None => Cell::Null,
            Some(v) => Cell::F32(v),
        }),
        FieldType::F64 => Ok(match row.try_get::<f64, _>(index).map_err(sql_error)? {
            None => Cell::Null,
            Some(v) => Cell::F64(v),
        }),
        FieldType::Bytes => Ok(match row.try_get::<&[u8], _>(index).map_err(sql_error)? {
            None => Cell::Null,
            Some(v) => Cell::Bytes(v.to_vec()),
        }),
        FieldType::Text => match row.try_get::<&str, _>(index).map_err(sql_error)? {
            Some(v) => Ok(Cell::Text(v.to_owned())),
            None => match text_cell(row, index)? {
                Some(v) => Ok(Cell::Text(v)),
                None => Ok(Cell::Null),
            },
        },
    }
}

fn optional_i32(row: &Row, index: usize) -> Result<Option<i32>, Error> {
    if let Ok(v) = row.try_get::<i16, _>(index) {
        return Ok(v.map(i32::from));
    }
    if let Ok(v) = row.try_get::<u8, _>(index) {
        return Ok(v.map(i32::from));
    }
    Ok(None)
}

fn text_cell(row: &Row, index: usize) -> Result<Option<String>, Error> {
    if let Ok(v) = row.try_get::<chrono::NaiveDateTime, _>(index) {
        return Ok(v.map(|v| v.format("%Y-%m-%d %H:%M:%S%.6f").to_string()));
    }
    if let Ok(v) = row.try_get::<chrono::NaiveDate, _>(index) {
        return Ok(v.map(|v| v.format("%Y-%m-%d").to_string()));
    }
    if let Ok(v) = row.try_get::<chrono::NaiveTime, _>(index) {
        return Ok(v.map(|v| v.format("%H:%M:%S%.6f").to_string()));
    }
    Ok(None)
}

fn scalar(value: &Cell, column: &Column) -> Result<serde_json::Value, Error> {
    use serde_json::Value as Json;
    Ok(match value {
        Cell::Null => Json::Null,
        Cell::Bool(v) => Json::Bool(*v),
        Cell::I32(v) => Json::from(*v),
        Cell::I64(v) => Json::from(*v),
        Cell::F32(v) if v.is_finite() => serde_json::json!(v),
        Cell::F64(v) if v.is_finite() => serde_json::json!(v),
        Cell::F32(_) | Cell::F64(_) => {
            return Err(Error::new("SQL Server floating-point value is not finite"));
        }
        Cell::Text(v) => Json::from(v.clone()),
        Cell::Bytes(v) if column.field == FieldType::Bytes => serde_json::json!(v),
        Cell::Bytes(_) => {
            return Err(Error::new(
                "SQL Server binary value has unexpected encoding",
            ));
        }
    })
}
