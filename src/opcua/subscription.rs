//! Manual Publish keeps server acknowledgements behind durable local receipt.
use super::*;
use ::opcua::client::UARequest;
use ::opcua::client::services::{
    CreateMonitoredItems, CreateSubscription, Publish, Republish, TransferSubscriptions,
};
use ::opcua::types::{
    DataChangeFilter, DataChangeNotification, DataChangeTrigger, ExtensionObject,
    MonitoredItemCreateRequest, MonitoringMode, MonitoringParameters, SubscriptionAcknowledgement,
};
use std::path::Path;

/// The explicit node set is resolved once per connection. Restart to change its schema revision.
pub struct SubscriptionConfig {
    pub nodes: Vec<NodeId>,
    pub publishing_interval: Duration,
    pub sampling_interval: Duration,
    /// Absolute deadband; None preserves status/value changes without a value threshold.
    pub deadband: Option<f64>,
    pub queue_size: u32,
    pub state_dir: PathBuf,
    pub max_pending_bytes: usize,
}
impl SubscriptionConfig {
    pub(super) fn validate(&self) -> Result<(), Error> {
        if self.nodes.is_empty()
            || self.nodes.len() > 10_000
            || self
                .nodes
                .iter()
                .map(ToString::to_string)
                .collect::<BTreeSet<_>>()
                .len()
                != self.nodes.len()
            || self.publishing_interval.is_zero()
            || self.sampling_interval.is_zero()
            || self.deadband.is_some_and(|v| !v.is_finite() || v < 0.0)
            || self.queue_size == 0
            || self.queue_size > 1000
            || self.max_pending_bytes == 0
        {
            return Err(Error::new("invalid OPC UA subscription configuration"));
        }
        Ok(())
    }
}

pub(super) struct Reader {
    config: SubscriptionConfig,
    pub kinds: Vec<DiscoveredKind>,
    subscription_id: u32,
    publish_timeout: Duration,
    next_sequence: u32,
    pending: VecDeque<Record>,
    high_water: Position,
    ack: Vec<SubscriptionAcknowledgement>,
    fingerprint: String,
    failed: bool,
    history: Option<history::History>,
    needs_history: bool,
    requires_full_history: bool,
    lease: crate::lease::DropRelease,
}
impl Reader {
    pub async fn open(
        source: &OpcUaConnector,
        config: SubscriptionConfig,
        history: Option<HistoryConfig>,
    ) -> Result<Self, Error> {
        let mut history = history
            .map(|history| history::History::new(history, config.nodes.len()))
            .transpose()?;
        std::fs::create_dir_all(&config.state_dir)?;
        let lease = crate::lease::hold(
            &config.state_dir,
            "opcua",
            crate::lease::hold_ttl(),
            "OPC UA subscription spool is already owned",
        )?;
        let kinds = inspect_kinds(&source.session, &config.nodes, source.schema_version).await?;
        let mut fingerprint = serde_json::json!([
            source.id,
            source
                .session
                .endpoint_info()
                .endpoint
                .endpoint_url
                .as_ref(),
            config
                .nodes
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            config.publishing_interval.as_nanos().to_string(),
            config.sampling_interval.as_nanos().to_string(),
            config.deadband,
            config.queue_size,
            source.schema_version,
            kinds.iter().map(|kind| &kind.metadata).collect::<Vec<_>>()
        ]);
        if let (Some(parts), Some(history)) = (fingerprint.as_array_mut(), &history) {
            parts.push(serde_json::json!(history.fingerprint()));
        }
        let fingerprint = fingerprint.to_string();
        let (high_water, pending) = recover(
            &config.state_dir,
            &fingerprint,
            config.max_pending_bytes,
            &mut history,
        )?;
        let mut reader = Self {
            config,
            kinds,
            subscription_id: 0,
            publish_timeout: source.timeout,
            next_sequence: 1,
            pending,
            high_water,
            ack: Vec::new(),
            fingerprint,
            failed: false,
            needs_history: history.is_some(),
            requires_full_history: history.is_some() && high_water > 0,
            history,
            lease,
        };
        reader.create(&source.session).await?;
        Ok(reader)
    }

    async fn create(&mut self, session: &Session) -> Result<(), Error> {
        if self.subscription_id != 0 {
            self.requires_full_history = self.history.is_some();
            let revision = self.kinds[0].schema.version;
            let current = inspect_kinds(session, &self.config.nodes, revision).await?;
            if current != self.kinds {
                return Err(Error::new(
                    "OPC UA monitored schema changed; restart with an explicit schema revision",
                ));
            }
        }
        let result = CreateSubscription::new(session)
            .publishing_interval(self.config.publishing_interval)
            .max_lifetime_count(3600)
            .max_keep_alive_count(1)
            .max_notifications_per_publish(1024)
            .send(session.channel())
            .await
            .map_err(|_| Error::new("OPC UA subscription creation failed"))?;
        let interval = Duration::try_from_secs_f64(result.revised_publishing_interval / 1000.0)
            .map_err(|_| Error::new("OPC UA invalid negotiated publishing interval"))?;
        if interval.is_zero() || result.revised_max_keep_alive_count == 0 {
            return Err(Error::new("OPC UA invalid negotiated keepalive"));
        }
        let keepalive = interval
            .checked_mul(result.revised_max_keep_alive_count)
            .and_then(|duration| duration.checked_mul(2))
            .ok_or_else(|| Error::new("OPC UA negotiated keepalive exceeds timeout range"))?;
        self.publish_timeout = self.publish_timeout.max(keepalive);
        self.subscription_id = result.subscription_id;
        self.next_sequence = 1;
        self.ack.clear();
        for (chunk_index, nodes) in self.config.nodes.chunks(128).enumerate() {
            let items = nodes
                .iter()
                .enumerate()
                .map(|(index, node)| MonitoredItemCreateRequest {
                    item_to_monitor: ReadValueId {
                        node_id: node.clone(),
                        attribute_id: AttributeId::Value as u32,
                        ..Default::default()
                    },
                    monitoring_mode: MonitoringMode::Reporting,
                    requested_parameters: MonitoringParameters {
                        client_handle: (chunk_index * 128 + index + 1) as u32,
                        sampling_interval: self.config.sampling_interval.as_secs_f64() * 1000.0,
                        queue_size: self.config.queue_size,
                        discard_oldest: true,
                        filter: ExtensionObject::from_message(DataChangeFilter {
                            trigger: DataChangeTrigger::StatusValue,
                            deadband_type: u32::from(self.config.deadband.is_some()),
                            deadband_value: self.config.deadband.unwrap_or(0.0),
                        }),
                    },
                })
                .collect();
            let results = CreateMonitoredItems::new(self.subscription_id, session)
                .timestamps_to_return(TimestampsToReturn::Both)
                .items_to_create(items)
                .send(session.channel())
                .await
                .map_err(|error| {
                    Error::new(format!(
                        "OPC UA monitored item creation failed: {}",
                        error.status()
                    ))
                })?;
            if results.results.len() != nodes.len()
                || results
                    .results
                    .iter()
                    .any(|v| !v.result.status_code.is_good())
            {
                return Err(Error::new("OPC UA monitored item was refused"));
            }
        }
        Ok(())
    }

    fn persist(&self) -> Result<(), Error> {
        let mut text = format!("{}\n{}\n", self.fingerprint, self.high_water);
        if let Some(history) = &self.history {
            text.push_str(
                &serde_json::to_string(history)
                    .map_err(|_| Error::new("OPC UA history state encoding failed"))?,
            );
            text.push('\n');
        }
        for record in &self.pending {
            text.push_str(&crate::delivery::encode(
                record,
                &record.idempotency_key("opcua", "spool"),
            ));
        }
        if text.len()
            > self
                .config
                .max_pending_bytes
                .saturating_mul(if self.history.is_some() { 4 } else { 1 })
                .saturating_add(self.fingerprint.len())
                .saturating_add(64)
        {
            return Err(Error::new(
                "OPC UA receipt and history state exceed configured bound",
            ));
        }
        crate::checkpoint::atomic_write(&self.config.state_dir.join("opcua.pending.v1"), &text)
    }

    pub fn history_notice(&self) -> Option<String> {
        self.history.as_ref().and_then(|history| history.notice())
    }

    pub fn healthy(&self) -> bool {
        !self.failed
    }

    pub fn renew_lease(&self) -> Result<(), Error> {
        self.lease.renew()
    }

    pub async fn read(
        &mut self,
        session: &Session,
        from: Option<Position>,
        timeout: Duration,
    ) -> Result<Option<Record>, Error> {
        self.lease.renew()?;
        if self.failed {
            return Err(Error::new(
                "OPC UA subscription failed; restart to recover durable pending records",
            ));
        }
        if from.is_some_and(|p| p > self.high_water) {
            return Err(Error::new(
                "OPC UA checkpoint is ahead of the subscription spool",
            ));
        }
        let committed = from.unwrap_or(0);
        let oldest = self
            .pending
            .front()
            .map_or(self.high_water.saturating_add(1), |r| r.position);
        if committed.saturating_add(1) < oldest {
            return Err(Error::new(
                "OPC UA checkpoint predates retained subscription samples",
            ));
        }
        while self
            .pending
            .front()
            .is_some_and(|record| record.position <= committed)
        {
            self.pending.pop_front();
        }
        if let Some(record) = self.pending.front() {
            return Ok(Some(record.clone()));
        }
        loop {
            let collected = if self.history.is_some() {
                tokio::time::timeout(
                    timeout.max(self.publish_timeout),
                    self.collect(session, timeout),
                )
                .await
                .map_err(|_| Error::new("OPC UA history recovery timed out"))
                .and_then(|result| result)
            } else {
                self.collect(session, timeout).await
            };
            if let Err(error) = collected {
                self.failed = true;
                return Err(error);
            }
            if let Some(record) = self.pending.front() {
                return Ok(Some(record.clone()));
            }
            if self.history.is_some() {
                return Ok(None);
            }
        }
    }

    async fn fill_history(&mut self, session: &Session) -> Result<(), Error> {
        let history = self
            .history
            .as_mut()
            .ok_or_else(|| Error::new("OPC UA history unavailable"))?;
        let records = history
            .collect(
                session,
                &self.config.nodes,
                &self.kinds,
                self.config.max_pending_bytes,
            )
            .await?;
        if self.requires_full_history && !history.covers_all_nodes() {
            return Err(Error::new(
                "OPC UA replaced subscription includes nodes without history; explicit gap recovery required",
            ));
        }
        self.requires_full_history = false;
        for mut record in records {
            self.high_water = self
                .high_water
                .checked_add(1)
                .ok_or_else(|| Error::new("OPC UA position exhausted"))?;
            record.position = self.high_water;
            self.pending.push_back(record);
        }
        self.needs_history = false;
        Ok(())
    }

    async fn collect(&mut self, session: &Session, timeout: Duration) -> Result<(), Error> {
        let response = match Publish::new(session)
            .timeout(self.publish_timeout)
            .acks(self.ack.clone())
            .send(session.channel())
            .await
        {
            Ok(response) => response,
            Err(_) => {
                self.needs_history = self.history.is_some();
                if !tokio::time::timeout(timeout, session.wait_for_connection())
                    .await
                    .is_ok_and(|v| v)
                {
                    return Err(Error::new("OPC UA subscription reconnect timed out"));
                }
                // Reconnection may reactivate the same session. Its existing
                // subscription and retransmission queue must be tried before transfer.
                match Publish::new(session)
                    .timeout(self.publish_timeout)
                    .acks(self.ack.clone())
                    .send(session.channel())
                    .await
                {
                    Ok(response) => response,
                    Err(error)
                        if matches!(
                            error.status(),
                            StatusCode::BadNoSubscription | StatusCode::BadSubscriptionIdInvalid
                        ) =>
                    {
                        let transferred = TransferSubscriptions::new(session)
                            .subscription(self.subscription_id)
                            .send_initial_values(false)
                            .send(session.channel())
                            .await
                            .ok()
                            .and_then(|r| r.results)
                            .is_some_and(|r| r.len() == 1 && r[0].status_code.is_good());
                        if !transferred {
                            self.create(session).await?;
                        }
                        Publish::new(session)
                            .timeout(self.publish_timeout)
                            .acks(self.ack.clone())
                            .send(session.channel())
                            .await
                            .map_err(|_| {
                                Error::new("OPC UA subscription publish failed after reconnect")
                            })?
                    }
                    Err(_) => {
                        return Err(Error::new(
                            "OPC UA subscription recovery failed without replacing existing subscription",
                        ));
                    }
                }
            }
        };
        if self.needs_history {
            self.fill_history(session).await?;
        }
        if response.subscription_id != self.subscription_id {
            return Err(Error::new("OPC UA unexpected subscription response"));
        }
        // A response can be lost between the server and this process. Recover every
        // still-available unreceipted message before accepting newer notifications.
        let available = recovery_sequences(
            response.available_sequence_numbers.unwrap_or_default(),
            response.notification_message.sequence_number,
        );
        if available.len() > 64 {
            return Err(Error::new(
                "OPC UA notification recovery exceeds batch bound",
            ));
        }
        let mut messages = Vec::new();
        for sequence in &available {
            if *sequence != response.notification_message.sequence_number
                && !self.ack.iter().any(|ack| ack.sequence_number == *sequence)
            {
                match Republish::new(self.subscription_id, *sequence, session)
                    .send(session.channel())
                    .await
                {
                    Ok(message) => messages.push(message.notification_message),
                    Err(_) if self.history.is_some() => {
                        self.fill_history(session).await?;
                        if !self
                            .history
                            .as_ref()
                            .is_some_and(|history| history.covers_all_nodes())
                        {
                            return Err(Error::new(
                                "OPC UA notification gap includes nodes without history",
                            ));
                        }
                    }
                    Err(_) => return Err(Error::new("OPC UA notification recovery unavailable")),
                }
            }
        }
        if !self
            .ack
            .iter()
            .any(|ack| ack.sequence_number == response.notification_message.sequence_number)
        {
            messages.push(response.notification_message);
        }
        self.ack
            .retain(|ack| available.contains(&ack.sequence_number));
        if self.history.is_some()
            && messages.iter().any(|message| {
                message.notification_data.iter().flatten().any(|object| {
                    object
                        .inner_as::<DataChangeNotification>()
                        .is_some_and(|data| {
                            data.monitored_items.iter().flatten().any(|item| {
                                item.value.status.is_some_and(|status| status.overflow())
                            })
                        })
                })
            })
        {
            return Err(Error::new(
                "OPC UA queue Overflow; restart to recover history before accepting this batch",
            ));
        }
        let mut bytes = 0usize;
        let mut received = Vec::new();
        for message in messages {
            let has_data = message
                .notification_data
                .as_ref()
                .is_some_and(|v| !v.is_empty());
            self.next_sequence =
                match advance_sequence(self.next_sequence, message.sequence_number, has_data) {
                    Ok(sequence) => sequence,
                    Err(error) if self.history.is_some() => {
                        self.fill_history(session).await?;
                        if !self
                            .history
                            .as_ref()
                            .is_some_and(|history| history.covers_all_nodes())
                        {
                            return Err(error);
                        }
                        advance_sequence(message.sequence_number, message.sequence_number, has_data)
                            .map_err(|_| error)?
                    }
                    Err(error) => return Err(error),
                };
            if !has_data {
                continue;
            }
            received.push(SubscriptionAcknowledgement {
                subscription_id: self.subscription_id,
                sequence_number: message.sequence_number,
            });
            for object in message.notification_data.unwrap_or_default() {
                let data = object
                    .inner_as::<DataChangeNotification>()
                    .ok_or_else(|| Error::new("OPC UA unexpected subscription notification"))?;
                for item in data.monitored_items.clone().unwrap_or_default() {
                    let index = item
                        .client_handle
                        .checked_sub(1)
                        .ok_or_else(|| Error::new("OPC UA invalid monitored item handle"))?
                        as usize;
                    let node = self
                        .config
                        .nodes
                        .get(index)
                        .ok_or_else(|| Error::new("OPC UA unknown monitored item handle"))?
                        .clone();
                    let value = item.value;
                    let mut record = sample(&node, &self.kinds[index], value.clone())?;
                    if let Some(history) = &mut self.history
                        && !history.accept_live(index, &value, &record)?
                    {
                        continue;
                    }
                    self.high_water = self
                        .high_water
                        .checked_add(1)
                        .ok_or_else(|| Error::new("OPC UA position exhausted"))?;
                    record.position = self.high_water;
                    bytes = bytes.saturating_add(
                        crate::delivery::encode(&record, &record.idempotency_key("opcua", "spool"))
                            .len(),
                    );
                    if bytes > self.config.max_pending_bytes {
                        return Err(Error::new("OPC UA pending batch exceeds configured bound"));
                    }
                    self.pending.push_back(record);
                }
            }
        }
        let pending_bytes: usize = self
            .pending
            .iter()
            .map(|record| {
                crate::delivery::encode(record, &record.idempotency_key("opcua", "spool")).len()
            })
            .sum();
        if pending_bytes > self.config.max_pending_bytes {
            return Err(Error::new("OPC UA pending batch exceeds configured bound"));
        }
        // The next Publish can acknowledge these messages only after file and directory fsync.
        if !received.is_empty() || self.history.is_some() {
            self.persist()?;
        }
        self.ack.extend(received);
        Ok(())
    }
}

fn recover(
    dir: &Path,
    fingerprint: &str,
    max_pending_bytes: usize,
    history: &mut Option<history::History>,
) -> Result<(Position, VecDeque<Record>), Error> {
    let path = dir.join("opcua.pending.v1");
    match std::fs::metadata(&path) {
        Ok(metadata)
            if metadata.len()
                > max_pending_bytes
                    .saturating_mul(if history.is_some() { 4 } else { 1 })
                    .saturating_add(fingerprint.len())
                    .saturating_add(64) as u64 =>
        {
            return Err(Error::new(
                "OPC UA recovered spool exceeds configured bound",
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok((0, VecDeque::new()));
        }
        Err(error) => return Err(error.into()),
    }
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok((0, VecDeque::new()));
        }
        Err(error) => return Err(error.into()),
    };
    let mut lines = text.lines();
    if lines.next() != Some(fingerprint) {
        return Err(Error::new(
            "OPC UA subscription configuration changed; recover existing spool first",
        ));
    }
    let high_water = lines
        .next()
        .ok_or_else(|| Error::new("OPC UA spool missing high-water position"))?
        .parse::<Position>()
        .map_err(|_| Error::new("OPC UA invalid spool position"))?;
    if !text.ends_with('\n') {
        return Err(Error::new("OPC UA incomplete spool"));
    }
    if let Some(expected) = history.as_ref() {
        let recovered: history::History = serde_json::from_str(
            lines
                .next()
                .ok_or_else(|| Error::new("OPC UA history state missing"))?,
        )
        .map_err(|_| Error::new("OPC UA invalid history state"))?;
        recovered.validate_against(expected)?;
        *history = Some(recovered);
    }
    let mut pending = VecDeque::new();
    for line in lines {
        let (record, key) = crate::delivery::decode(line)?;
        if key != record.idempotency_key("opcua", "spool")
            || record.position > high_water
            || pending
                .back()
                .is_some_and(|r: &Record| r.position.checked_add(1) != Some(record.position))
        {
            return Err(Error::new("OPC UA invalid spool record"));
        }
        pending.push_back(record);
    }
    if pending
        .back()
        .is_some_and(|record| record.position != high_water)
    {
        return Err(Error::new("OPC UA spool high-water mismatch"));
    }
    Ok((high_water, pending))
}

pub(super) fn sample(
    node: &NodeId,
    kind: &DiscoveredKind,
    value: ::opcua::types::DataValue,
) -> Result<Record, Error> {
    let timestamp = value
        .source_timestamp
        .ok_or_else(|| Error::new("OPC UA source timestamp missing"))?
        .as_chrono()
        .timestamp_millis();
    Ok(Record {
        source: None, kind: node.to_string(), key: node.to_string(), source_ts_ms: timestamp,
        ingest_ts_ms: 0, position: 0,
        quality: Some(format!("{} (0x{:08x})", value.status.unwrap_or(StatusCode::Good), value.status.unwrap_or(StatusCode::Good).bits())),
        payload: serde_json::json!({"value": value.value.map(|value| json_value(value, &kind.schema.fields["value"])).transpose()?}).to_string(),
    })
}

fn json_value(value: Variant, field: &FieldType) -> Result<serde_json::Value, Error> {
    if matches!(value, Variant::Float(v) if !v.is_finite())
        || matches!(value, Variant::Double(v) if !v.is_finite())
    {
        return Err(Error::new("OPC UA non-finite numeric value"));
    }
    Ok(match (field, value) {
        (FieldType::Text, value) => value.to_string().into(),
        (FieldType::Bool, Variant::Boolean(v)) => v.into(),
        (FieldType::I32, Variant::SByte(v)) => v.into(),
        (FieldType::I32, Variant::Byte(v)) => v.into(),
        (FieldType::I32, Variant::Int16(v)) => v.into(),
        (FieldType::I32, Variant::UInt16(v)) => v.into(),
        (FieldType::I32, Variant::Int32(v)) => v.into(),
        (FieldType::I64, Variant::UInt32(v)) => v.into(),
        (FieldType::I64, Variant::Int64(v)) => v.into(),
        (FieldType::F32, Variant::Float(v)) => serde_json::json!(v),
        (FieldType::F64, Variant::Double(v)) => serde_json::json!(v),
        (FieldType::Bytes, Variant::ByteString(v)) => serde_json::json!(v.value),
        _ => return Err(Error::new("OPC UA value contradicts monitored schema")),
    })
}

async fn inspect_kinds(
    session: &Session,
    nodes: &[NodeId],
    schema_version: u64,
) -> Result<Vec<DiscoveredKind>, Error> {
    let mut kinds = Vec::new();
    for nodes in nodes.chunks(128) {
        let attributes: Vec<_> = nodes
            .iter()
            .flat_map(|node| {
                [
                    ReadValueId {
                        node_id: node.clone(),
                        attribute_id: AttributeId::DataType as u32,
                        ..Default::default()
                    },
                    ReadValueId {
                        node_id: node.clone(),
                        attribute_id: AttributeId::ValueRank as u32,
                        ..Default::default()
                    },
                ]
            })
            .collect();
        let values = session
            .read(&attributes, TimestampsToReturn::Neither, 0.0)
            .await
            .map_err(|_| Error::new("OPC UA monitored node schema read failed"))?;
        if values.len() != attributes.len() {
            return Err(Error::new("OPC UA monitored schema response incomplete"));
        }
        for (node, pair) in nodes.iter().zip(values.as_chunks::<2>().0) {
            let (Some(Variant::NodeId(data_type)), Some(Variant::Int32(-1))) =
                (&pair[0].value, &pair[1].value)
            else {
                return Err(Error::new(
                    "OPC UA subscription requires scalar monitored values",
                ));
            };
            if pair.iter().any(|v| v.status.is_some_and(|s| !s.is_good())) {
                return Err(Error::new("OPC UA monitored schema unavailable"));
            }
            kinds.push(DiscoveredKind {
                kind: node.to_string(),
                schema: Schema {
                    version: schema_version,
                    key_fields: vec![],
                    fields: BTreeMap::from([("value".into(), field_type(data_type))]),
                },
                metadata: BTreeMap::from([
                    ("node_id".into(), node.to_string()),
                    ("data_type".into(), data_type.to_string()),
                    ("value_rank".into(), "-1".into()),
                ]),
            });
        }
    }
    Ok(kinds)
}

// The server array is unordered. Wrapping distance behind the current Publish
// message orders retained notifications across the reserved-zero rollover.
fn recovery_sequences(mut sequences: Vec<u32>, newest: u32) -> Vec<u32> {
    sequences.sort_by_key(|sequence| std::cmp::Reverse(newest.wrapping_sub(*sequence)));
    sequences.dedup();
    sequences
}

fn advance_sequence(expected: u32, received: u32, has_data: bool) -> Result<u32, Error> {
    if received != expected {
        return Err(Error::new(
            "OPC UA notification gap is no longer recoverable",
        ));
    }
    Ok(if has_data {
        received.checked_add(1).unwrap_or(1)
    } else {
        expected
    })
}

#[cfg(test)]
mod tests {
    use super::{advance_sequence, recovery_sequences};

    #[test]
    fn notification_recovery_preserves_source_order_across_rollover() {
        for (available, newest, expected) in [
            (vec![12, 10, 11], 12, vec![10, 11, 12]),
            (
                vec![1, u32::MAX, u32::MAX - 1],
                1,
                vec![u32::MAX - 1, u32::MAX, 1],
            ),
            (vec![3, 1, 3, 2], 3, vec![1, 2, 3]),
        ] {
            assert_eq!(recovery_sequences(available, newest), expected);
        }
    }
    #[test]
    fn notification_continuity_detects_missing_data_and_keeps_keepalives_at_the_next_sequence() {
        assert_eq!(advance_sequence(1, 1, true).unwrap(), 2);
        assert_eq!(advance_sequence(2, 2, false).unwrap(), 2);
        assert_eq!(advance_sequence(u32::MAX, u32::MAX, true).unwrap(), 1);
        assert!(advance_sequence(2, 3, true).is_err());
        assert!(advance_sequence(2, 3, false).is_err());
        assert!(advance_sequence(1, 0, true).is_err());
    }
}
