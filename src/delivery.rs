//! Durable line encoding shared by file sinks and the store-and-forward buffer.

use crate::Error;
use crate::checkpoint::{escape_field, unescape_field};
use crate::contract::{IdempotencyKey, Record};

pub fn encode(record: &Record, idk: &IdempotencyKey) -> String {
    let version = if record.source.is_some() { 2 } else { 1 };
    let mut line = format!(
        "v{version}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
        escape_field(&idk.encode()),
        escape_field(&record.kind),
        escape_field(&record.key),
        record.source_ts_ms,
        record.ingest_ts_ms,
        encode_quality(record.quality.as_deref()),
        record.position,
        escape_field(&record.payload),
    );
    if let Some(source) = &record.source {
        line.pop();
        line.push('\t');
        line.push_str(&escape_field(&serde_json::json!(source).to_string()));
        line.push('\n');
    }
    line
}

pub fn decode(line: &str) -> Result<(Record, IdempotencyKey), Error> {
    let parts: Vec<&str> = line.split('\t').collect();
    if !((parts.len() == 9 && parts[0] == "v1") || (parts.len() == 10 && parts[0] == "v2")) {
        return Err(Error::new("delivery row is unreadable"));
    }
    let record = Record {
        source: if parts[0] == "v2" {
            Some(
                serde_json::from_str(&unescape_field(parts[9])?)
                    .map_err(|_| Error::new("invalid source metadata"))?,
            )
        } else {
            None
        },
        kind: unescape_field(parts[2])?,
        key: unescape_field(parts[3])?,
        source_ts_ms: parts[4]
            .parse()
            .map_err(|error: std::num::ParseIntError| Error::new(error.to_string()))?,
        ingest_ts_ms: parts[5]
            .parse()
            .map_err(|error: std::num::ParseIntError| Error::new(error.to_string()))?,
        quality: decode_quality(parts[6])?,
        position: parts[7]
            .parse()
            .map_err(|error: std::num::ParseIntError| Error::new(error.to_string()))?,
        payload: unescape_field(parts[8])?,
    };
    Ok((record, IdempotencyKey::decode(&unescape_field(parts[1])?)?))
}

fn encode_quality(quality: Option<&str>) -> String {
    match quality {
        None => String::new(),
        Some(value) => format!(":{}", escape_field(value)),
    }
}

fn decode_quality(field: &str) -> Result<Option<String>, Error> {
    if field.is_empty() {
        return Ok(None);
    }
    let body = field
        .strip_prefix(':')
        .ok_or_else(|| Error::new("delivery row is unreadable"))?;
    Ok(Some(unescape_field(body)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fake::record;

    #[test]
    fn encode_decode_round_trips_tabs_and_newlines() {
        let rec = Record {
            source: None,
            kind: "tag".into(),
            key: "k\t1".into(),
            source_ts_ms: 10,
            ingest_ts_ms: 11,
            quality: Some("ok\n".into()),
            position: 3,
            payload: "p\tay".into(),
        };
        let idk = rec.idempotency_key("modbus", "line-a");
        let line = encode(&rec, &idk);
        let (got, got_idk) = decode(line.trim_end()).unwrap();
        assert_eq!(got, rec);
        assert_eq!(got_idk, idk);
    }

    #[test]
    fn encode_decode_distinguishes_absent_and_empty_quality() {
        let mut rec = record("tag", "k1", 1, "1.0");
        rec.quality = None;
        let idk = rec.idempotency_key("opc", "line-a");
        let (none, _) = decode(encode(&rec, &idk).trim_end()).unwrap();
        assert_eq!(none.quality, None);
        rec.quality = Some(String::new());
        let (empty, _) = decode(encode(&rec, &idk).trim_end()).unwrap();
        assert_eq!(empty.quality.as_deref(), Some(""));
    }

    #[test]
    fn decode_rejects_a_wrong_version() {
        let rec = record("tag", "k1", 1, "1.0");
        let idk = rec.idempotency_key("opc", "line-a");
        let mut line = encode(&rec, &idk);
        line.replace_range(0..2, "v9");
        assert!(decode(line.trim_end()).is_err());
    }
}
