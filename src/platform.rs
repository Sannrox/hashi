//! Out-of-process platform source-adapter protocol. Versioned frames, peer acks.

use crate::Error;
use crate::contract::{IdempotencyKey, Record};
use crate::sink::{DeliverError, Sink};

/// Protocol name carried on every frame so a peer can refuse a mismatch.
pub const PLATFORM_SINK_PROTOCOL: &str = "hashi.sink.v2";
const LEGACY_PROTOCOL: &str = "hashi.sink.v1";
const MAGIC: &[u8; 4] = b"HSK1";
const VERSION: u32 = 2;

pub trait PlatformPeer {
    fn exchange(&mut self, frame: &[u8]) -> Result<(), DeliverError>;
}

/// In-process peer used by tests and as the default when no subprocess is wired.
#[derive(Debug, Default)]
pub struct CapturePeer {
    pub frames: Vec<Vec<u8>>,
    pub fail_times: u32,
    remaining_failures: u32,
}

impl CapturePeer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn fail_next(&mut self, times: u32) {
        self.fail_times = times;
        self.remaining_failures = times;
    }

    pub fn decoded(&self) -> Result<Vec<(Record, IdempotencyKey)>, Error> {
        self.frames
            .iter()
            .map(|frame| decode_frame(frame))
            .collect()
    }
}

impl PlatformPeer for CapturePeer {
    fn exchange(&mut self, frame: &[u8]) -> Result<(), DeliverError> {
        if self.remaining_failures > 0 {
            self.remaining_failures -= 1;
            return Err(DeliverError::Transient("injected peer failure".into()));
        }
        self.frames.push(frame.to_vec());
        Ok(())
    }
}

#[derive(Debug)]
pub struct PlatformSdkSink<P> {
    peer: P,
}

impl<P> PlatformSdkSink<P> {
    pub fn new(peer: P) -> Self {
        Self { peer }
    }

    pub fn into_peer(self) -> P {
        self.peer
    }
}

impl<P: PlatformPeer> Sink for PlatformSdkSink<P> {
    fn deliver(&mut self, record: &Record, idk: &IdempotencyKey) -> Result<(), DeliverError> {
        let frame = encode_frame(record, idk);
        self.peer.exchange(&frame)
    }
}

pub fn encode_frame(record: &Record, idk: &IdempotencyKey) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(MAGIC);
    let version = if record.source.is_some() { VERSION } else { 1 };
    buf.extend_from_slice(&version.to_le_bytes());
    put_str(
        &mut buf,
        if version == 1 {
            LEGACY_PROTOCOL
        } else {
            PLATFORM_SINK_PROTOCOL
        },
    );
    put_str(&mut buf, &idk.encode());
    put_str(&mut buf, &record.kind);
    put_str(&mut buf, &record.key);
    buf.extend_from_slice(&record.source_ts_ms.to_le_bytes());
    buf.extend_from_slice(&record.ingest_ts_ms.to_le_bytes());
    match &record.quality {
        Some(value) => {
            buf.push(1);
            put_str(&mut buf, value);
        }
        None => buf.push(0),
    }
    buf.extend_from_slice(&record.position.to_le_bytes());
    put_str(&mut buf, &record.payload);
    if let Some(source) = &record.source {
        put_str(&mut buf, &serde_json::json!(source).to_string());
    }
    buf
}

pub fn decode_frame(frame: &[u8]) -> Result<(Record, IdempotencyKey), Error> {
    let mut cur = frame;
    cur = take_magic(cur)?;
    let version = take_u32(&mut cur)?;
    if version != 1 && version != VERSION {
        return Err(Error::new(format!(
            "unsupported platform sink protocol {version}"
        )));
    }
    let protocol = take_str(&mut cur)?;
    if protocol
        != if version == 1 {
            LEGACY_PROTOCOL
        } else {
            PLATFORM_SINK_PROTOCOL
        }
    {
        return Err(Error::new("unsupported platform sink protocol"));
    }
    let idk = IdempotencyKey::decode(&take_str(&mut cur)?)?;
    let kind = take_str(&mut cur)?;
    let key = take_str(&mut cur)?;
    let source_ts_ms = take_i64(&mut cur)?;
    let ingest_ts_ms = take_i64(&mut cur)?;
    let quality = match take_u8(&mut cur)? {
        0 => None,
        1 => Some(take_str(&mut cur)?),
        _ => return Err(Error::new("platform frame is unreadable")),
    };
    let position = take_u64(&mut cur)?;
    let payload = take_str(&mut cur)?;
    let source = if version == 2 {
        Some(
            serde_json::from_str(&take_str(&mut cur)?)
                .map_err(|_| Error::new("invalid source metadata"))?,
        )
    } else {
        None
    };
    if !cur.is_empty() {
        return Err(Error::new("platform frame is unreadable"));
    }
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

fn put_str(buf: &mut Vec<u8>, value: &str) {
    let bytes = value.as_bytes();
    buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(bytes);
}

fn take_magic(frame: &[u8]) -> Result<&[u8], Error> {
    if frame.len() < 4 || &frame[..4] != MAGIC {
        return Err(Error::new("platform frame is unreadable"));
    }
    Ok(&frame[4..])
}

fn take_u8(cur: &mut &[u8]) -> Result<u8, Error> {
    if cur.is_empty() {
        return Err(Error::new("platform frame is unreadable"));
    }
    let value = cur[0];
    *cur = &cur[1..];
    Ok(value)
}

fn take_u32(cur: &mut &[u8]) -> Result<u32, Error> {
    if cur.len() < 4 {
        return Err(Error::new("platform frame is unreadable"));
    }
    let bytes: [u8; 4] = cur[..4]
        .try_into()
        .map_err(|_| Error::new("platform frame is unreadable"))?;
    let value = u32::from_le_bytes(bytes);
    *cur = &cur[4..];
    Ok(value)
}

fn take_i64(cur: &mut &[u8]) -> Result<i64, Error> {
    if cur.len() < 8 {
        return Err(Error::new("platform frame is unreadable"));
    }
    let bytes: [u8; 8] = cur[..8]
        .try_into()
        .map_err(|_| Error::new("platform frame is unreadable"))?;
    let value = i64::from_le_bytes(bytes);
    *cur = &cur[8..];
    Ok(value)
}

fn take_u64(cur: &mut &[u8]) -> Result<u64, Error> {
    if cur.len() < 8 {
        return Err(Error::new("platform frame is unreadable"));
    }
    let bytes: [u8; 8] = cur[..8]
        .try_into()
        .map_err(|_| Error::new("platform frame is unreadable"))?;
    let value = u64::from_le_bytes(bytes);
    *cur = &cur[8..];
    Ok(value)
}

fn take_str(cur: &mut &[u8]) -> Result<String, Error> {
    let len = take_u32(cur)? as usize;
    if cur.len() < len {
        return Err(Error::new("platform frame is unreadable"));
    }
    let value = std::str::from_utf8(&cur[..len]).map_err(|error| Error::new(error.to_string()))?;
    let owned = value.to_string();
    *cur = &cur[len..];
    Ok(owned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fake::record;

    #[test]
    fn frame_round_trips_through_the_capture_peer() {
        let rec = record("tag", "k1", 1, "1.0");
        let idk = rec.idempotency_key("opc", "line-a");
        let mut sink = PlatformSdkSink::new(CapturePeer::new());
        sink.deliver(&rec, &idk).unwrap();
        let peer = sink.into_peer();
        let rows = peer.decoded().unwrap();
        assert_eq!(rows[0].0, rec);
        assert_eq!(rows[0].1, idk);
        assert!(
            decode_frame(&peer.frames[0])
                .unwrap()
                .1
                .encode()
                .contains("opc")
        );
    }

    #[test]
    fn peer_transient_failure_surfaces() {
        let rec = record("tag", "k1", 1, "1.0");
        let idk = rec.idempotency_key("opc", "line-a");
        let mut peer = CapturePeer::new();
        peer.fail_next(1);
        let mut sink = PlatformSdkSink::new(peer);
        let err = sink.deliver(&rec, &idk).unwrap_err();
        assert!(matches!(err, DeliverError::Transient(_)));
    }
}
