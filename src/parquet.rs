//! Partitioned Apache Parquet files. One row group per kind partition.

use std::fs::{self, File};
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use parquet::data_type::{ByteArray, ByteArrayType, Int64Type};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::writer::SerializedFileWriter;
use parquet::record::RowAccessor;
use parquet::schema::parser::parse_message_type;

use crate::Error;
use crate::checkpoint::{path_token, sync_dir};
use crate::contract::{IdempotencyKey, Record};
use crate::sink::{DeliverError, Sink};

const SCHEMA: &str = "
  message hashi_record {
    REQUIRED BYTE_ARRAY kind (UTF8);
    REQUIRED BYTE_ARRAY key (UTF8);
    REQUIRED INT64 source_ts_ms;
    REQUIRED INT64 ingest_ts_ms;
    OPTIONAL BYTE_ARRAY quality (UTF8);
    REQUIRED INT64 position (INTEGER(64,false));
    REQUIRED BYTE_ARRAY payload (UTF8);
    REQUIRED BYTE_ARRAY idempotency_key (UTF8);
  }
";

#[derive(Debug, Clone)]
pub struct ParquetSink {
    dir: PathBuf,
}

impl ParquetSink {
    pub fn open(dir: impl Into<PathBuf>) -> Result<Self, Error> {
        let dir = dir.into();
        fs::create_dir_all(&dir)?;
        Ok(Self { dir })
    }

    fn part_path(&self, kind: &str, idk: &IdempotencyKey) -> PathBuf {
        self.dir
            .join(format!("kind={}", path_token(kind)))
            .join(format!("part-{}.parquet", part_token(idk)))
    }

    pub fn replay(&self) -> Result<Vec<(Record, IdempotencyKey)>, Error> {
        let mut rows = Vec::new();
        if !self.dir.exists() {
            return Ok(rows);
        }
        let mut parts = Vec::new();
        for entry in fs::read_dir(&self.dir)? {
            let kind_dir = entry?.path();
            if !kind_dir.is_dir() {
                continue;
            }
            for file in fs::read_dir(&kind_dir)? {
                let path = file?.path();
                if path.extension().and_then(|ext| ext.to_str()) == Some("parquet") {
                    parts.push(path);
                }
            }
        }
        parts.sort();
        for part in parts {
            rows.extend(read_parquet(&part)?);
        }
        Ok(rows)
    }
}

impl Sink for ParquetSink {
    fn deliver(&mut self, record: &Record, idk: &IdempotencyKey) -> Result<(), DeliverError> {
        let path = self.part_path(&record.kind, idk);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(io_err)?;
        }
        write_parquet(&path, &[(record.clone(), idk.clone())]).map_err(io_err)?;
        sync_dir(&self.dir).map_err(io_err)?;
        Ok(())
    }
}

fn write_parquet(path: &Path, rows: &[(Record, IdempotencyKey)]) -> Result<(), Error> {
    let extended = rows.iter().any(|(record, _)| record.source.is_some());
    let schema_text = if extended {
        SCHEMA.replacen("REQUIRED BYTE_ARRAY idempotency_key (UTF8);", "REQUIRED BYTE_ARRAY idempotency_key (UTF8); OPTIONAL BYTE_ARRAY source_metadata (UTF8);", 1)
    } else {
        SCHEMA.to_string()
    };
    let schema = Arc::new(parse_message_type(&schema_text).map_err(pq_err)?);
    let tmp = path.with_extension(format!("tmp-{}", std::process::id()));
    let file = File::create(&tmp)?;
    let mut writer = SerializedFileWriter::new(file, schema, Default::default()).map_err(pq_err)?;
    let mut row_group = writer.next_row_group().map_err(pq_err)?;

    write_bytes(
        &mut row_group,
        rows.iter()
            .map(|(record, _)| ByteArray::from(record.kind.as_str()))
            .collect::<Vec<_>>(),
        None,
    )?;
    write_bytes(
        &mut row_group,
        rows.iter()
            .map(|(record, _)| ByteArray::from(record.key.as_str()))
            .collect::<Vec<_>>(),
        None,
    )?;
    write_i64(
        &mut row_group,
        rows.iter()
            .map(|(record, _)| record.source_ts_ms)
            .collect::<Vec<_>>(),
    )?;
    write_i64(
        &mut row_group,
        rows.iter()
            .map(|(record, _)| record.ingest_ts_ms)
            .collect::<Vec<_>>(),
    )?;
    let mut quality_values = Vec::new();
    let mut quality_def = Vec::new();
    for (record, _) in rows {
        match &record.quality {
            Some(value) => {
                quality_def.push(1);
                quality_values.push(ByteArray::from(value.as_str()));
            }
            None => quality_def.push(0),
        }
    }
    write_bytes(&mut row_group, quality_values, Some(&quality_def))?;
    write_i64(
        &mut row_group,
        rows.iter()
            .map(|(record, _)| record.position as i64)
            .collect::<Vec<_>>(),
    )?;
    write_bytes(
        &mut row_group,
        rows.iter()
            .map(|(record, _)| ByteArray::from(record.payload.as_str()))
            .collect::<Vec<_>>(),
        None,
    )?;
    write_bytes(
        &mut row_group,
        rows.iter()
            .map(|(_, idk)| ByteArray::from(idk.encode().as_str()))
            .collect::<Vec<_>>(),
        None,
    )?;

    if extended {
        let mut values = Vec::new();
        let mut levels = Vec::new();
        for (record, _) in rows {
            if let Some(source) = &record.source {
                levels.push(1);
                values.push(ByteArray::from(
                    serde_json::json!(source).to_string().as_str(),
                ));
            } else {
                levels.push(0);
            }
        }
        write_bytes(&mut row_group, values, Some(&levels))?;
    }
    row_group.close().map_err(pq_err)?;
    writer.close().map_err(pq_err)?;
    let completed = File::open(&tmp)?;
    completed.sync_all()?;
    drop(completed);
    fs::rename(tmp, path)?;
    if let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }
    Ok(())
}

fn write_bytes(
    row_group: &mut parquet::file::writer::SerializedRowGroupWriter<'_, File>,
    values: Vec<ByteArray>,
    def_levels: Option<&[i16]>,
) -> Result<(), Error> {
    let mut col = row_group
        .next_column()
        .map_err(pq_err)?
        .ok_or_else(|| Error::new("parquet schema missing column"))?;
    col.typed::<ByteArrayType>()
        .write_batch(&values, def_levels, None)
        .map_err(pq_err)?;
    col.close().map_err(pq_err)?;
    Ok(())
}

fn write_i64(
    row_group: &mut parquet::file::writer::SerializedRowGroupWriter<'_, File>,
    values: Vec<i64>,
) -> Result<(), Error> {
    let mut col = row_group
        .next_column()
        .map_err(pq_err)?
        .ok_or_else(|| Error::new("parquet schema missing column"))?;
    col.typed::<Int64Type>()
        .write_batch(&values, None, None)
        .map_err(pq_err)?;
    col.close().map_err(pq_err)?;
    Ok(())
}

fn read_parquet(path: &Path) -> Result<Vec<(Record, IdempotencyKey)>, Error> {
    let file = File::open(path)?;
    let reader = SerializedFileReader::new(file).map_err(pq_err)?;
    let mut rows = Vec::new();
    for row in reader.get_row_iter(None).map_err(pq_err)? {
        let row = row.map_err(pq_err)?;
        let quality = if row.is_null(4).map_err(pq_err)? {
            None
        } else {
            Some(row.get_string(4).map_err(pq_err)?.clone())
        };
        let source = if row.len() == 8 {
            None
        } else if row.len() == 9 {
            if row.is_null(8).map_err(pq_err)? {
                None
            } else {
                Some(
                    serde_json::from_str(row.get_string(8).map_err(pq_err)?)
                        .map_err(|_| Error::new("invalid Parquet source metadata"))?,
                )
            }
        } else {
            return Err(Error::new("unsupported Parquet record columns"));
        };
        let record = Record {
            source,
            kind: row.get_string(0).map_err(pq_err)?.clone(),
            key: row.get_string(1).map_err(pq_err)?.clone(),
            source_ts_ms: row.get_long(2).map_err(pq_err)?,
            ingest_ts_ms: row.get_long(3).map_err(pq_err)?,
            quality,
            position: row.get_ulong(5).map_err(pq_err)?,
            payload: row.get_string(6).map_err(pq_err)?.clone(),
        };
        let idk = IdempotencyKey::decode(row.get_string(7).map_err(pq_err)?)?;
        rows.push((record, idk));
    }
    Ok(rows)
}

fn part_token(idk: &IdempotencyKey) -> String {
    let bytes = idk.encode();
    format!(
        "{:020}-{:016x}{:016x}",
        idk.position,
        fnv64(bytes.as_bytes(), FNV_OFFSET),
        fnv64(bytes.as_bytes(), FNV_OFFSET ^ 0xff)
    )
}

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x1000_0000_01b3;

fn fnv64(data: &[u8], offset: u64) -> u64 {
    let mut hash = offset;
    for byte in data {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

fn pq_err(error: parquet::errors::ParquetError) -> Error {
    Error::new(error.to_string())
}

fn io_err(error: impl Into<Error>) -> DeliverError {
    DeliverError::Transient(error.into().message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fake::record;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(1);

    fn dir() -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "hashi-parquet-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn parquet_round_trips_two_rows_in_one_partition() {
        let mut sink = ParquetSink::open(dir()).unwrap();
        let a = record("tag", "k1", 1, "1.0");
        let b = record("tag", "k2", 2, "2.0");
        sink.deliver(&a, &a.idempotency_key("opc", "line-a"))
            .unwrap();
        sink.deliver(&b, &b.idempotency_key("opc", "line-a"))
            .unwrap();
        let rows = sink.replay().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0.key, "k1");
        assert_eq!(rows[1].0.key, "k2");
        let bytes = fs::read(sink.part_path("tag", &a.idempotency_key("opc", "line-a"))).unwrap();
        assert_eq!(&bytes[0..4], b"PAR1");
        assert_eq!(&bytes[bytes.len() - 4..], b"PAR1");
        assert!(
            sink.part_path("tag", &b.idempotency_key("opc", "line-a"))
                .exists()
        );
    }

    #[test]
    fn parquet_filename_stays_bounded_for_a_long_key() {
        let rec = record("tag", &"k".repeat(128), 1, "1.0");
        let idk = rec.idempotency_key("opc", "line-a");
        let sink = ParquetSink::open(dir()).unwrap();
        let name = sink
            .part_path("tag", &idk)
            .file_name()
            .unwrap()
            .to_string_lossy()
            .into_owned();
        assert!(name.len() < 255, "{name} len {}", name.len());
    }

    #[test]
    fn parquet_round_trips_a_position_above_i64_max() {
        let mut sink = ParquetSink::open(dir()).unwrap();
        let rec = record("tag", "k1", u64::MAX, "1.0");
        sink.deliver(&rec, &rec.idempotency_key("opc", "line-a"))
            .unwrap();
        let rows = sink.replay().unwrap();
        assert_eq!(rows[0].0.position, u64::MAX);
    }
}
