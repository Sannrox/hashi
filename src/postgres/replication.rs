//! Logical replication transport and pgoutput decoding.

use super::schema::Cell;
use super::*;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::collections::BTreeMap;

const PG_EPOCH_MICROS: i64 = 946_684_800_000_000;

pub(super) fn parse_lsn(text: &str) -> Result<u64, Error> {
    let (hi, lo) = text
        .split_once('/')
        .ok_or_else(|| Error::new("PostgreSQL LSN is invalid"))?;
    let hi = u64::from_str_radix(hi, 16).map_err(|_| Error::new("PostgreSQL LSN is invalid"))?;
    let lo = u64::from_str_radix(lo, 16).map_err(|_| Error::new("PostgreSQL LSN is invalid"))?;
    if hi > u64::from(u32::MAX) || lo > u64::from(u32::MAX) {
        return Err(Error::new("PostgreSQL LSN is invalid"));
    }
    Ok((hi << 32) | lo)
}

pub(super) fn format_lsn(lsn: u64) -> String {
    format!("{:X}/{:X}", lsn >> 32, lsn as u32)
}

pub(super) fn pg_timestamp() -> i64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|d| i64::try_from(d.as_micros()).ok())
        .unwrap_or(PG_EPOCH_MICROS);
    now.saturating_sub(PG_EPOCH_MICROS)
}

pub(super) fn source_ts_ms(pg_micros: i64) -> i64 {
    (PG_EPOCH_MICROS.saturating_add(pg_micros)) / 1_000
}

pub(super) fn standby_status(lsn: u64, reply: u8) -> Bytes {
    let mut buf = BytesMut::with_capacity(34);
    buf.put_u8(b'r');
    buf.put_u64(lsn);
    buf.put_u64(lsn);
    buf.put_u64(lsn);
    buf.put_i64(pg_timestamp());
    buf.put_u8(reply);
    buf.freeze()
}

pub(super) enum Wal {
    Keepalive { reply: bool },
    Data { payload: Bytes },
}

pub(super) fn parse_copy(bytes: Bytes) -> Result<Wal, Error> {
    if bytes.is_empty() {
        return Err(Error::new("PostgreSQL replication message is empty"));
    }
    let kind = bytes[0];
    let mut buf = bytes.slice(1..);
    match kind {
        b'k' => {
            if buf.remaining() < 17 {
                return Err(Error::new("PostgreSQL keepalive is truncated"));
            }
            let _wal_end = buf.get_u64();
            buf.advance(8);
            Ok(Wal::Keepalive {
                reply: buf.get_u8() != 0,
            })
        }
        b'w' => {
            if buf.remaining() < 24 {
                return Err(Error::new("PostgreSQL XLogData is truncated"));
            }
            let _wal_start = buf.get_u64();
            buf.advance(16);
            Ok(Wal::Data {
                payload: buf.copy_to_bytes(buf.remaining()),
            })
        }
        _ => Err(Error::new(
            "PostgreSQL unsupported replication message; refusing to skip source changes",
        )),
    }
}

pub(super) struct Relation {
    pub id: u32,
    pub schema: String,
    pub table: String,
    pub columns: Vec<RelationColumn>,
}

pub(super) struct RelationColumn {
    pub name: String,
    pub type_oid: u32,
}

pub(super) enum PgOutput {
    Begin {
        timestamp: i64,
        xid: u32,
    },
    Commit {
        end_lsn: u64,
        timestamp: i64,
    },
    Relation(Relation),
    Insert {
        rel_id: u32,
        new: Vec<Option<Cell>>,
    },
    Update {
        rel_id: u32,
        old: Option<Vec<Option<Cell>>>,
        old_complete: bool,
        new: Vec<Option<Cell>>,
    },
    Delete {
        rel_id: u32,
        old: Vec<Option<Cell>>,
    },
    Type,
    Origin,
    LogicalMessage,
}

pub(super) fn decode_pgoutput(
    payload: &[u8],
    types: &BTreeMap<u32, Vec<u32>>,
) -> Result<Option<PgOutput>, Error> {
    if payload.is_empty() {
        return Err(Error::new("PostgreSQL pgoutput message is empty"));
    }
    let mut buf = payload;
    let kind = buf[0];
    buf = &buf[1..];
    match kind {
        b'B' => {
            require(buf, 20)?;
            let _final_lsn = read_u64(&mut buf);
            let timestamp = read_i64(&mut buf);
            let xid = read_u32(&mut buf);
            Ok(Some(PgOutput::Begin { timestamp, xid }))
        }
        b'C' => {
            require(buf, 25)?;
            buf = &buf[1..];
            let _commit_lsn = read_u64(&mut buf);
            let end_lsn = read_u64(&mut buf);
            let timestamp = read_i64(&mut buf);
            Ok(Some(PgOutput::Commit { end_lsn, timestamp }))
        }
        b'R' => Ok(Some(PgOutput::Relation(decode_relation(buf)?))),
        b'I' => {
            require(buf, 5)?;
            let rel_id = read_u32(&mut buf);
            if buf.is_empty() || buf[0] != b'N' {
                return Err(Error::new("PostgreSQL insert is missing the new tuple"));
            }
            buf = &buf[1..];
            let oids = types
                .get(&rel_id)
                .ok_or_else(|| Error::new("PostgreSQL insert relation is unknown"))?;
            Ok(Some(PgOutput::Insert {
                rel_id,
                new: decode_tuple(&mut buf, oids)?,
            }))
        }
        b'U' => decode_update(buf, types).map(Some),
        b'D' => {
            require(buf, 5)?;
            let rel_id = read_u32(&mut buf);
            if buf.is_empty() || (buf[0] != b'K' && buf[0] != b'O') {
                return Err(Error::new("PostgreSQL delete is missing the old tuple"));
            }
            buf = &buf[1..];
            let oids = types
                .get(&rel_id)
                .ok_or_else(|| Error::new("PostgreSQL delete relation is unknown"))?;
            Ok(Some(PgOutput::Delete {
                rel_id,
                old: decode_tuple(&mut buf, oids)?,
            }))
        }
        b'T' => Err(Error::new(
            "PostgreSQL TRUNCATE is unsupported; explicit recovery is required",
        )),
        b'Y' => Ok(Some(PgOutput::Type)),
        b'O' => Ok(Some(PgOutput::Origin)),
        b'M' => Ok(Some(PgOutput::LogicalMessage)),
        other => Err(Error::new(format!(
            "PostgreSQL unsupported pgoutput message {other}; refusing to skip source changes"
        ))),
    }
}

fn decode_update(mut buf: &[u8], types: &BTreeMap<u32, Vec<u32>>) -> Result<PgOutput, Error> {
    require(buf, 5)?;
    let rel_id = read_u32(&mut buf);
    let oids = types
        .get(&rel_id)
        .ok_or_else(|| Error::new("PostgreSQL update relation is unknown"))?;
    let mut old = None;
    let mut old_complete = false;
    if buf.is_empty() {
        return Err(Error::new("PostgreSQL update is truncated"));
    }
    match buf[0] {
        b'K' => {
            buf = &buf[1..];
            old = Some(decode_tuple(&mut buf, oids)?);
        }
        b'O' => {
            buf = &buf[1..];
            old = Some(decode_tuple(&mut buf, oids)?);
            old_complete = true;
        }
        b'N' => {}
        _ => return Err(Error::new("PostgreSQL update tuple kind is invalid")),
    }
    if buf.is_empty() || buf[0] != b'N' {
        return Err(Error::new("PostgreSQL update is missing the new tuple"));
    }
    buf = &buf[1..];
    Ok(PgOutput::Update {
        rel_id,
        old,
        old_complete,
        new: decode_tuple(&mut buf, oids)?,
    })
}

fn decode_relation(mut buf: &[u8]) -> Result<Relation, Error> {
    require(buf, 4)?;
    let id = read_u32(&mut buf);
    let schema = read_cstr(&mut buf)?;
    let table = read_cstr(&mut buf)?;
    require(buf, 3)?;
    buf = &buf[1..];
    let natts = read_u16(&mut buf) as usize;
    let mut columns = Vec::with_capacity(natts);
    for _ in 0..natts {
        require(buf, 1)?;
        buf = &buf[1..];
        let name = read_cstr(&mut buf)?;
        require(buf, 8)?;
        let type_oid = read_u32(&mut buf);
        buf = &buf[4..];
        columns.push(RelationColumn { name, type_oid });
    }
    Ok(Relation {
        id,
        schema,
        table,
        columns,
    })
}

fn decode_tuple(buf: &mut &[u8], oids: &[u32]) -> Result<Vec<Option<Cell>>, Error> {
    require(buf, 2)?;
    let natts = read_u16(buf) as usize;
    if natts != oids.len() {
        return Err(Error::new(
            "PostgreSQL tuple column count does not match the relation",
        ));
    }
    let mut values = Vec::with_capacity(natts);
    for oid in oids {
        require(buf, 1)?;
        let kind = buf[0];
        *buf = &buf[1..];
        match kind {
            b'n' => values.push(None),
            b'u' => values.push(Some(Cell::Unchanged)),
            b't' => {
                require(buf, 4)?;
                let len = read_u32(buf) as usize;
                require(buf, len)?;
                let bytes = &buf[..len];
                *buf = &buf[len..];
                let text = std::str::from_utf8(bytes)
                    .map_err(|_| Error::new("PostgreSQL tuple text is not UTF-8"))?;
                values.push(Some(Cell::from_text(*oid, text)?));
            }
            _ => {
                return Err(Error::new(
                    "PostgreSQL tuple value kind is unsupported; refusing to skip source changes",
                ));
            }
        }
    }
    Ok(values)
}

type Tuple = Vec<Option<Cell>>;

pub(super) fn complete_tuple(values: &[Option<Cell>]) -> Result<Tuple, Error> {
    if values
        .iter()
        .any(|value| matches!(value, Some(Cell::Unchanged)))
    {
        return Err(Error::new(
            "PostgreSQL tuple omitted an unchanged TOAST value; replica identity FULL is required for a complete row",
        ));
    }
    Ok(values.to_vec())
}

pub(super) fn resolve_toast(
    old: Option<&[Option<Cell>]>,
    old_complete: bool,
    new: &[Option<Cell>],
) -> Result<(Option<Tuple>, Tuple), Error> {
    let complete = match old {
        Some(old) if old_complete => Some(complete_tuple(old)?),
        _ => None,
    };
    let before = if old_complete {
        complete.clone()
    } else {
        old.map(|values| values.to_vec())
    };
    let mut resolved = Vec::with_capacity(new.len());
    for (index, value) in new.iter().enumerate() {
        if matches!(value, Some(Cell::Unchanged)) {
            let Some(complete) = complete.as_ref() else {
                return Err(Error::new(
                    "PostgreSQL update omitted an unchanged TOAST value without a complete before image",
                ));
            };
            let Some(cell) = complete.get(index) else {
                return Err(Error::new(
                    "PostgreSQL update TOAST column is missing from the before image",
                ));
            };
            resolved.push(cell.clone());
        } else {
            resolved.push(value.clone());
        }
    }
    Ok((before, resolved))
}

fn read_cstr(buf: &mut &[u8]) -> Result<String, Error> {
    let end = buf
        .iter()
        .position(|b| *b == 0)
        .ok_or_else(|| Error::new("PostgreSQL string is truncated"))?;
    let text = std::str::from_utf8(&buf[..end])
        .map_err(|_| Error::new("PostgreSQL identifier is not UTF-8"))?
        .to_owned();
    *buf = &buf[end + 1..];
    Ok(text)
}

fn require(buf: &[u8], n: usize) -> Result<(), Error> {
    if buf.len() < n {
        return Err(Error::new("PostgreSQL pgoutput message is truncated"));
    }
    Ok(())
}

fn read_u16(buf: &mut &[u8]) -> u16 {
    let v = u16::from_be_bytes([buf[0], buf[1]]);
    *buf = &buf[2..];
    v
}
fn read_u32(buf: &mut &[u8]) -> u32 {
    let v = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    *buf = &buf[4..];
    v
}
fn read_u64(buf: &mut &[u8]) -> u64 {
    let v = u64::from_be_bytes([
        buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
    ]);
    *buf = &buf[8..];
    v
}
fn read_i64(buf: &mut &[u8]) -> i64 {
    read_u64(buf) as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lsn_round_trips() {
        assert_eq!(parse_lsn("0/16B3748").unwrap(), 0x16B3748);
        assert_eq!(format_lsn(0x16B3748), "0/16B3748");
        assert!(parse_lsn("nope").is_err());
    }

    #[test]
    fn pgoutput_begin_commit_and_insert_decode() {
        let mut begin = vec![b'B'];
        begin.extend(0x11u64.to_be_bytes());
        begin.extend(0i64.to_be_bytes());
        begin.extend(7u32.to_be_bytes());
        let decoded = decode_pgoutput(&begin, &BTreeMap::new()).unwrap();
        match decoded {
            Some(PgOutput::Begin { xid, .. }) => {
                assert_eq!(xid, 7);
            }
            _ => panic!("expected begin"),
        }

        let mut rel = vec![b'R'];
        rel.extend(42u32.to_be_bytes());
        rel.extend(b"public\0items\0");
        rel.push(b'd');
        rel.extend(1u16.to_be_bytes());
        rel.push(0);
        rel.extend(b"id\0");
        rel.extend(23u32.to_be_bytes());
        rel.extend((-1i32).to_be_bytes());
        let relation = match decode_pgoutput(&rel, &BTreeMap::new()).unwrap() {
            Some(PgOutput::Relation(relation)) => relation,
            _ => panic!("expected relation"),
        };
        assert_eq!(relation.table, "items");
        let mut types = BTreeMap::new();
        types.insert(42, vec![23]);

        let mut insert = vec![b'I'];
        insert.extend(42u32.to_be_bytes());
        insert.push(b'N');
        insert.extend(1u16.to_be_bytes());
        insert.push(b't');
        insert.extend(2u32.to_be_bytes());
        insert.extend(b"12");
        match decode_pgoutput(&insert, &types).unwrap() {
            Some(PgOutput::Insert { new, .. }) => match &new[0] {
                Some(Cell::I32(12)) => {}
                other => panic!("{other:?}"),
            },
            _ => panic!("expected insert"),
        }
    }

    #[test]
    fn unchanged_toast_resolves_from_the_old_tuple() {
        let mut types = BTreeMap::new();
        types.insert(1, vec![25]);
        let mut insert = vec![b'I'];
        insert.extend(1u32.to_be_bytes());
        insert.push(b'N');
        insert.extend(1u16.to_be_bytes());
        insert.push(b'u');
        match decode_pgoutput(&insert, &types) {
            Ok(Some(PgOutput::Insert { new, .. })) => match complete_tuple(&new) {
                Err(error) => assert!(error.message.contains("TOAST")),
                Ok(_) => panic!("insert TOAST omission should fail"),
            },
            Ok(_) => panic!("expected insert"),
            Err(_) => panic!("insert TOAST should decode then fail closed"),
        }

        let old = vec![Some(Cell::Text("kept".into()))];
        let new = vec![Some(Cell::Unchanged)];
        let (before, resolved) = resolve_toast(Some(&old), true, &new).unwrap();
        assert_eq!(before, Some(old.clone()));
        assert_eq!(resolved, old);
        match resolve_toast(Some(&old), false, &new) {
            Err(error) => assert!(error.message.contains("TOAST")),
            Ok(_) => panic!("TOAST from a key tuple should fail"),
        }
        let key_old = vec![Some(Cell::Text("old-key".into()))];
        let replaced = vec![Some(Cell::Text("new-key".into()))];
        let (before, resolved) = resolve_toast(Some(&key_old), false, &replaced).unwrap();
        assert_eq!(before, Some(key_old));
        assert_eq!(resolved, replaced);
        match resolve_toast(None, false, &new) {
            Err(error) => assert!(error.message.contains("TOAST")),
            Ok(_) => panic!("TOAST without a before image should fail"),
        }
    }
}
