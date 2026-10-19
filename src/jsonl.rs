//! JSON Lines sink for debugging. Partitioned by record kind.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::Error;
use crate::checkpoint::{path_token, repair_log_tail, sync_dir};
use crate::contract::{IdempotencyKey, Record};
use crate::sink::{DeliverError, Sink};

#[derive(Debug, Clone)]
pub struct JsonLinesSink {
    dir: PathBuf,
}

impl JsonLinesSink {
    pub fn open(dir: impl Into<PathBuf>) -> Result<Self, Error> {
        let dir = dir.into();
        fs::create_dir_all(&dir)?;
        Ok(Self { dir })
    }

    fn part_path(&self, kind: &str) -> PathBuf {
        self.dir
            .join(format!("kind={}", path_token(kind)))
            .join("part.jsonl")
    }

    pub fn replay(&self) -> Result<Vec<(Record, IdempotencyKey)>, Error> {
        let mut rows = Vec::new();
        if !self.dir.exists() {
            return Ok(rows);
        }
        let mut parts = Vec::new();
        for entry in fs::read_dir(&self.dir)? {
            let entry = entry?;
            let part = entry.path().join("part.jsonl");
            if part.exists() {
                parts.push(part);
            }
        }
        parts.sort();
        for part in parts {
            rows.extend(read_jsonl(&part)?);
        }
        Ok(rows)
    }
}

impl Sink for JsonLinesSink {
    fn deliver(&mut self, record: &Record, idk: &IdempotencyKey) -> Result<(), DeliverError> {
        let path = self.part_path(&record.kind);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(io_err)?;
        }
        repair_log_tail(&path).map_err(io_err)?;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(io_err)?;
        file.write_all(to_jsonl(record, idk).as_bytes())
            .map_err(io_err)?;
        file.sync_all().map_err(io_err)?;
        drop(file);
        if let Some(parent) = path.parent() {
            sync_dir(parent).map_err(io_err)?;
        }
        sync_dir(&self.dir).map_err(io_err)?;
        Ok(())
    }
}

fn to_jsonl(record: &Record, idk: &IdempotencyKey) -> String {
    // Preserve the v1 envelope byte-for-byte when optional source metadata is absent.
    let mut line = format!(
        "{{\"kind\":{},\"key\":{},\"sourceTsMs\":{},\"ingestTsMs\":{},\"quality\":{},\"position\":{},\"payload\":{},\"idempotencyKey\":{}}}\n",
        json_str(&record.kind),
        json_str(&record.key),
        record.source_ts_ms,
        record.ingest_ts_ms,
        match &record.quality {
            Some(value) => json_str(value),
            None => "null".into(),
        },
        record.position,
        json_str(&record.payload),
        json_str(&idk.encode()),
    );
    if let Some(source) = &record.source {
        line.truncate(line.len() - 2);
        line.push_str(",\"source\":");
        line.push_str(&serde_json::json!(source).to_string());
        line.push_str("}\n");
    }
    line
}

fn json_str(value: &str) -> String {
    let mut out = String::from("\"");
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn read_jsonl(path: &Path) -> Result<Vec<(Record, IdempotencyKey)>, Error> {
    let mut buf = Vec::new();
    File::open(path)?.read_to_end(&mut buf)?;
    let complete = match buf.iter().rposition(|&byte| byte == b'\n') {
        Some(index) => &buf[..=index],
        None => return Ok(Vec::new()),
    };
    let text = std::str::from_utf8(complete).map_err(|error| Error::new(error.to_string()))?;
    let mut rows = Vec::new();
    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        rows.push(parse_jsonl(line)?);
    }
    Ok(rows)
}

fn parse_jsonl(line: &str) -> Result<(Record, IdempotencyKey), Error> {
    // Replay uses the same field order we write. Keep parse strict.
    let json: serde_json::Value =
        serde_json::from_str(line).map_err(|_| Error::new("invalid delivery JSON"))?;
    let source = json
        .get("source")
        .filter(|value| !value.is_null())
        .map(|value| {
            serde_json::from_value(value.clone()).map_err(|_| Error::new("invalid source metadata"))
        })
        .transpose()?;
    let kind = json_field(line, "kind")?;
    let key = json_field(line, "key")?;
    let source_ts_ms = json_i64(line, "sourceTsMs")?;
    let ingest_ts_ms = json_i64(line, "ingestTsMs")?;
    let quality = json_optional(line, "quality")?;
    let position = json_u64(line, "position")?;
    let payload = json_field(line, "payload")?;
    let idk = IdempotencyKey::decode(&json_field(line, "idempotencyKey")?)?;
    Ok((
        Record {
            source,
            kind,
            key,
            source_ts_ms,
            ingest_ts_ms,
            quality,
            position,
            payload,
        },
        idk,
    ))
}

fn json_field(line: &str, name: &str) -> Result<String, Error> {
    let needle = format!("\"{name}\":");
    let start = line
        .find(&needle)
        .ok_or_else(|| Error::new("jsonl row is unreadable"))?
        + needle.len();
    let rest = &line[start..];
    if rest.starts_with("null") {
        return Err(Error::new("jsonl row is unreadable"));
    }
    if !rest.starts_with('"') {
        return Err(Error::new("jsonl row is unreadable"));
    }
    unescape_json_string(&rest[1..])
}

fn json_optional(line: &str, name: &str) -> Result<Option<String>, Error> {
    let needle = format!("\"{name}\":");
    let start = line
        .find(&needle)
        .ok_or_else(|| Error::new("jsonl row is unreadable"))?
        + needle.len();
    let rest = &line[start..];
    if rest.starts_with("null") {
        return Ok(None);
    }
    if !rest.starts_with('"') {
        return Err(Error::new("jsonl row is unreadable"));
    }
    Ok(Some(unescape_json_string(&rest[1..])?))
}

fn json_i64(line: &str, name: &str) -> Result<i64, Error> {
    json_num(line, name)
}

fn json_u64(line: &str, name: &str) -> Result<u64, Error> {
    json_num(line, name)
}

fn json_num<T: std::str::FromStr>(line: &str, name: &str) -> Result<T, Error>
where
    T::Err: std::fmt::Display,
{
    let needle = format!("\"{name}\":");
    let start = line
        .find(&needle)
        .ok_or_else(|| Error::new("jsonl row is unreadable"))?
        + needle.len();
    let rest = &line[start..];
    let end = rest
        .find([',', '}'])
        .ok_or_else(|| Error::new("jsonl row is unreadable"))?;
    rest[..end]
        .parse::<T>()
        .map_err(|error| Error::new(error.to_string()))
}

fn unescape_json_string(rest: &str) -> Result<String, Error> {
    let mut out = String::new();
    let mut chars = rest.chars();
    while let Some(ch) = chars.next() {
        match ch {
            '"' => return Ok(out),
            '\\' => match chars.next() {
                Some('"') => out.push('"'),
                Some('\\') => out.push('\\'),
                Some('n') => out.push('\n'),
                Some('r') => out.push('\r'),
                Some('t') => out.push('\t'),
                Some('u') => {
                    let hex: String = chars.by_ref().take(4).collect();
                    let code = u32::from_str_radix(&hex, 16)
                        .map_err(|_| Error::new("jsonl row is unreadable"))?;
                    out.push(
                        char::from_u32(code)
                            .ok_or_else(|| Error::new("jsonl row is unreadable"))?,
                    );
                }
                _ => return Err(Error::new("jsonl row is unreadable")),
            },
            other => out.push(other),
        }
    }
    Err(Error::new("jsonl row is unreadable"))
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
            "hashi-jsonl-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn jsonl_round_trips_a_record() {
        let mut sink = JsonLinesSink::open(dir()).unwrap();
        let rec = record("tag", "k1", 1, "1.0");
        let idk = rec.idempotency_key("opc", "line-a");
        sink.deliver(&rec, &idk).unwrap();
        let rows = sink.replay().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, rec);
        assert_eq!(rows[0].1, idk);
    }

    #[test]
    fn jsonl_escapes_quotes_in_payload() {
        let mut sink = JsonLinesSink::open(dir()).unwrap();
        let rec = Record {
            source: None,
            kind: "tag".into(),
            key: "k1".into(),
            source_ts_ms: 1,
            ingest_ts_ms: 2,
            quality: None,
            position: 1,
            payload: r#"{"n":1}"#.into(),
        };
        let idk = rec.idempotency_key("opc", "line-a");
        sink.deliver(&rec, &idk).unwrap();
        assert_eq!(sink.replay().unwrap()[0].0.payload, r#"{"n":1}"#);
    }
}
