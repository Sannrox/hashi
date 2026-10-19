//! Versioned connector contract. Connectors and consumers upgrade independently.

/// Contract revision carried on every persisted checkpoint and quarantine row.
pub const CONTRACT_VERSION: u32 = 1;

/// Monotonic read position inside one connector instance.
pub type Position = u64;

/// One typed record emitted by `read`. Payload is type-normalized text; mapping
/// to objects, markings, or Actions is the consumer's job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub kind: String,
    pub key: String,
    pub source_ts_ms: i64,
    pub ingest_ts_ms: i64,
    pub quality: Option<String>,
    pub position: Position,
    pub payload: String,
}

impl Record {
    pub fn idempotency_key(&self, connector: &str, instance: &str) -> IdempotencyKey {
        IdempotencyKey {
            connector: connector.to_string(),
            instance: instance.to_string(),
            record_kind: self.kind.clone(),
            key: self.key.clone(),
            position: self.position,
        }
    }
}

/// At-least-once identity. A consumer can apply a pair of deliveries exactly
/// once by storing this key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IdempotencyKey {
    pub connector: String,
    pub instance: String,
    pub record_kind: String,
    pub key: String,
    pub position: Position,
}

impl IdempotencyKey {
    /// Length-prefixed fields so `/` inside a component cannot collide.
    pub fn encode(&self) -> String {
        format!(
            "{}:{}/{}:{}/{}:{}/{}:{}/{}",
            self.connector.len(),
            self.connector,
            self.instance.len(),
            self.instance,
            self.record_kind.len(),
            self.record_kind,
            self.key.len(),
            self.key,
            self.position
        )
    }

    pub fn decode(encoded: &str) -> Result<Self, crate::Error> {
        let (connector, rest) = take_len_field(encoded)?;
        let (instance, rest) = take_len_field(rest)?;
        let (record_kind, rest) = take_len_field(rest)?;
        let (key, rest) = take_len_field(rest)?;
        let position = rest
            .parse()
            .map_err(|error: std::num::ParseIntError| crate::Error::new(error.to_string()))?;
        Ok(Self {
            connector,
            instance,
            record_kind,
            key,
            position,
        })
    }
}

fn take_len_field(input: &str) -> Result<(String, &str), crate::Error> {
    let colon = input
        .find(':')
        .ok_or_else(|| crate::Error::new("idempotency key is unreadable"))?;
    let len: usize = input[..colon]
        .parse()
        .map_err(|error: std::num::ParseIntError| crate::Error::new(error.to_string()))?;
    let start = colon + 1;
    let end = start
        .checked_add(len)
        .ok_or_else(|| crate::Error::new("idempotency key is unreadable"))?;
    if end > input.len() {
        return Err(crate::Error::new("idempotency key is unreadable"));
    }
    let value = input
        .get(start..end)
        .ok_or_else(|| crate::Error::new("idempotency key is unreadable"))?
        .to_string();
    let rest = &input[end..];
    match rest.strip_prefix('/') {
        Some(rest) => Ok((value, rest)),
        None => Err(crate::Error::new("idempotency key is unreadable")),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckReport {
    pub ok: bool,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveredKind {
    pub kind: String,
    pub schema: crate::schema::Schema,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idempotency_key_binds_connector_instance_kind_key_and_position() {
        let record = Record {
            kind: "tag".into(),
            key: "pump-1".into(),
            source_ts_ms: 10,
            ingest_ts_ms: 11,
            quality: None,
            position: 7,
            payload: "1.0".into(),
        };
        let key = record.idempotency_key("modbus", "line-a");
        assert_eq!(key.encode(), "6:modbus/6:line-a/3:tag/6:pump-1/7");
        let slash_instance = record.idempotency_key("a", "b/c");
        let slash_connector = record.idempotency_key("a/b", "c");
        assert_ne!(slash_instance.encode(), slash_connector.encode());
        assert_eq!(IdempotencyKey::decode(&key.encode()).unwrap(), key);
        assert_eq!(
            IdempotencyKey::decode(&slash_instance.encode()).unwrap(),
            slash_instance
        );
        assert!(IdempotencyKey::decode("1:é/0").is_err());
    }
}
