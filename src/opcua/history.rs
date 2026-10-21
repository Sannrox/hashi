use super::*;
use ::opcua::client::UARequest;
use ::opcua::client::{HistoryReadAction, services::HistoryRead};
use ::opcua::types::{
    DataValue, DateTime, HistoryData, HistoryReadValueId, ReadRawModifiedDetails, VariableId,
};

/// Raw historical values are staged before subscription values after each reconnect.
pub struct HistoryConfig {
    pub start: DateTime,
    pub page_rows: u32,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Watermark {
    ticks: i64,
    samples: BTreeSet<String>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct History {
    start: i64,
    page_rows: u32,
    watermarks: Vec<Option<Watermark>>,
    unsupported: BTreeSet<usize>,
    supported: BTreeSet<usize>,
    overlap: Vec<BTreeSet<String>>,
}

impl History {
    pub fn new(config: HistoryConfig, nodes: usize) -> Result<Self, Error> {
        if config.start.is_null()
            || config.start.ticks() <= 0
            || !(1..=1024).contains(&config.page_rows)
        {
            return Err(Error::new("invalid OPC UA history configuration"));
        }
        Ok(Self {
            start: config.start.ticks(),
            page_rows: config.page_rows,
            watermarks: vec![None; nodes],
            unsupported: BTreeSet::new(),
            supported: BTreeSet::new(),
            overlap: vec![BTreeSet::new(); nodes],
        })
    }

    pub fn fingerprint(&self) -> String {
        serde_json::json!([self.start, self.page_rows]).to_string()
    }

    pub fn validate_against(&self, expected: &Self) -> Result<(), Error> {
        if self.start != expected.start
            || self.page_rows != expected.page_rows
            || self.watermarks.len() != expected.watermarks.len()
            || self.overlap.len() != expected.watermarks.len()
            || self
                .supported
                .iter()
                .any(|index| *index >= self.watermarks.len() || self.unsupported.contains(index))
            || self
                .unsupported
                .iter()
                .any(|index| *index >= self.watermarks.len())
            || self
                .watermarks
                .iter()
                .flatten()
                .any(|watermark| watermark.ticks < self.start || watermark.samples.is_empty())
        {
            return Err(Error::new("OPC UA recovered history state is incompatible"));
        }
        Ok(())
    }

    pub fn covers_all_nodes(&self) -> bool {
        self.supported.len() == self.watermarks.len()
    }

    pub fn notice(&self) -> Option<String> {
        (!self.unsupported.is_empty()).then(|| format!("history unsupported for {} monitored nodes; subscription continues without gap recovery for those nodes", self.unsupported.len()))
    }

    /// Inclusive history overlap and initial subscription notifications share source identity.
    pub fn accept_live(
        &mut self,
        index: usize,
        value: &DataValue,
        record: &Record,
    ) -> Result<bool, Error> {
        let reached = self
            .watermarks
            .get(index)
            .and_then(|watermark| watermark.as_ref())
            .is_some_and(|watermark| {
                value
                    .source_timestamp
                    .is_some_and(|time| time.ticks() >= watermark.ticks)
            });
        let accepted = self.accept(index, value, record)?;
        if reached {
            self.overlap[index].clear();
        }
        Ok(accepted)
    }

    fn accept(&mut self, index: usize, value: &DataValue, record: &Record) -> Result<bool, Error> {
        if self.unsupported.contains(&index) {
            return Ok(true);
        }
        let ticks = value
            .source_timestamp
            .ok_or_else(|| Error::new("OPC UA history source timestamp missing"))?
            .ticks();
        if ticks < self.start {
            return Ok(false);
        }
        let identity = identity(index, ticks, value, record);
        let slot = self
            .watermarks
            .get_mut(index)
            .ok_or_else(|| Error::new("OPC UA history node identity mismatch"))?;
        if let Some(previous) = slot {
            if ticks < previous.ticks {
                return if self.overlap[index].contains(&identity) {
                    Ok(false)
                } else {
                    Err(Error::new(
                        "OPC UA source timestamp regressed outside recovered overlap",
                    ))
                };
            }
            if ticks == previous.ticks {
                return Ok(previous.samples.insert(identity));
            }
        }
        *slot = Some(Watermark {
            ticks,
            samples: BTreeSet::from([identity]),
        });
        Ok(true)
    }

    pub async fn collect(
        &mut self,
        session: &Session,
        nodes: &[NodeId],
        kinds: &[DiscoveredKind],
        max_bytes: usize,
    ) -> Result<Vec<Record>, Error> {
        let clock = session
            .read(
                &[ReadValueId {
                    node_id: VariableId::Server_ServerStatus_CurrentTime.into(),
                    attribute_id: AttributeId::Value as u32,
                    ..Default::default()
                }],
                TimestampsToReturn::Neither,
                0.0,
            )
            .await
            .map_err(|_| Error::new("OPC UA history server clock unavailable"))?;
        let Some(DataValue {
            value: Some(Variant::DateTime(end)),
            status,
            ..
        }) = clock.first()
        else {
            return Err(Error::new("OPC UA history server clock missing"));
        };
        if status.is_some_and(|status| !status.is_good()) || end.ticks() < self.start {
            return Err(Error::new("OPC UA history server clock is invalid"));
        }
        let mut records = Vec::new();
        let mut bytes = 0usize;
        for (index, node) in nodes.iter().enumerate() {
            if self.unsupported.contains(&index) {
                continue;
            }
            let access = session
                .read(
                    &[ReadValueId {
                        node_id: node.clone(),
                        attribute_id: AttributeId::AccessLevel as u32,
                        ..Default::default()
                    }],
                    TimestampsToReturn::Neither,
                    0.0,
                )
                .await
                .map_err(|_| Error::new("OPC UA history capability read failed"))?;
            let Some(DataValue {
                value: Some(Variant::Byte(access)),
                status,
                ..
            }) = access.first()
            else {
                return Err(Error::new("OPC UA history capability missing"));
            };
            if status.is_some_and(|status| !status.is_good()) {
                return Err(Error::new("OPC UA history capability unavailable"));
            }
            if access & 4 == 0 {
                if self.supported.contains(&index) {
                    return Err(Error::new(
                        "OPC UA previously supported history is unavailable; recovery required",
                    ));
                }
                self.unsupported.insert(index);
                continue;
            }
            let start = self.watermarks[index]
                .as_ref()
                .map_or(self.start, |watermark| watermark.ticks);
            if start > end.ticks() {
                return Err(Error::new("OPC UA source history clock regressed"));
            }
            let mut boundary = self.watermarks[index]
                .as_ref()
                .map(|watermark| watermark.samples.clone())
                .unwrap_or_default();
            let details = ReadRawModifiedDetails {
                is_read_modified: false,
                start_time: DateTime::from(start),
                end_time: **end,
                num_values_per_node: self.page_rows,
                return_bounds: false,
            };
            let mut continuation = Default::default();
            let mut previous_tick = start;
            let mut pages = 0usize;
            loop {
                pages += 1;
                if pages > 4096 {
                    return Err(Error::new("OPC UA history continuation limit exceeded"));
                }
                let response = HistoryRead::new(
                    HistoryReadAction::ReadRawModifiedDetails(details.clone()),
                    session,
                )
                .timestamps_to_return(TimestampsToReturn::Both)
                .node(HistoryReadValueId {
                    node_id: node.clone(),
                    continuation_point: continuation,
                    ..Default::default()
                })
                .send(session.channel())
                .await;
                let response = match response {
                    Ok(response) => response,
                    Err(error) if unsupported(error.status()) => {
                        if self.supported.contains(&index) {
                            return Err(Error::new(
                                "OPC UA previously supported history is unavailable; recovery required",
                            ));
                        }
                        self.unsupported.insert(index);
                        break;
                    }
                    Err(_) => {
                        return Err(Error::new(
                            "OPC UA history read failed; gap remains recoverable",
                        ));
                    }
                };
                let mut results = response
                    .results
                    .ok_or_else(|| Error::new("OPC UA history response missing"))?;
                if results.len() != 1 {
                    return Err(Error::new("OPC UA history response count mismatch"));
                }
                let result = results.remove(0);
                if unsupported(result.status_code) {
                    if self.supported.contains(&index) {
                        return Err(Error::new(
                            "OPC UA previously supported history is unavailable; recovery required",
                        ));
                    }
                    self.unsupported.insert(index);
                    break;
                }
                if !result.status_code.is_good() {
                    return Err(Error::new(
                        "OPC UA history unavailable; refusing to skip the gap",
                    ));
                }
                self.unsupported.remove(&index);
                self.supported.insert(index);
                if result.status_code.sub_code() == ::opcua::types::SubStatusCode::GoodNoData {
                    if !boundary.is_empty() {
                        return Err(Error::new(
                            "OPC UA retained historical boundary is unavailable; explicit recovery required",
                        ));
                    }
                    break;
                }
                let data = result
                    .history_data
                    .inner_as::<HistoryData>()
                    .ok_or_else(|| Error::new("OPC UA raw history data missing"))?;
                for value in data.data_values.clone().unwrap_or_default() {
                    let ticks = value
                        .source_timestamp
                        .ok_or_else(|| Error::new("OPC UA historical timestamp missing"))?
                        .ticks();
                    if ticks < previous_tick
                        || ticks < start
                        || ticks > end.ticks()
                        || value.status.is_some_and(|status| {
                            matches!(
                                status.sub_code(),
                                ::opcua::types::SubStatusCode::BadDataLost
                                    | ::opcua::types::SubStatusCode::BadDataUnavailable
                            )
                        })
                    {
                        return Err(Error::new(
                            "OPC UA historical range is incomplete or unordered",
                        ));
                    }
                    previous_tick = ticks;
                    let record = super::subscription::sample(node, &kinds[index], value.clone())?;
                    let identity = identity(index, ticks, &value, &record);
                    if ticks == start {
                        boundary.remove(&identity);
                    }
                    if ticks > start && !boundary.is_empty() {
                        return Err(Error::new(
                            "OPC UA retained historical boundary is unavailable; explicit recovery required",
                        ));
                    }
                    self.overlap[index].insert(identity);
                    if self.accept(index, &value, &record)? {
                        bytes = bytes.saturating_add(
                            crate::delivery::encode(
                                &record,
                                &record.idempotency_key("opcua", "spool"),
                            )
                            .len(),
                        );
                        if bytes > max_bytes {
                            return Err(Error::new(
                                "OPC UA history gap exceeds pending batch bound",
                            ));
                        }
                        records.push((ticks, value.source_picoseconds.unwrap_or(0), record));
                    }
                }
                continuation = result.continuation_point;
                if continuation.is_null_or_empty() {
                    break;
                }
            }
            if !boundary.is_empty() {
                return Err(Error::new(
                    "OPC UA retained historical boundary is unavailable; explicit recovery required",
                ));
            }
        }
        // Per-node history is ordered. Cross-node ties have no protocol ordering guarantee.
        records.sort_by(|a, b| (a.0, a.1, &a.2.kind).cmp(&(b.0, b.1, &b.2.kind)));
        Ok(records.into_iter().map(|(_, _, record)| record).collect())
    }
}

fn unsupported(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::BadHistoryOperationUnsupported
            | StatusCode::BadNotSupported
            | StatusCode::BadServiceUnsupported
    )
}

fn identity(index: usize, ticks: i64, value: &DataValue, record: &Record) -> String {
    serde_json::json!([
        index,
        ticks,
        value.source_picoseconds.unwrap_or(0),
        value
            .status
            .unwrap_or(StatusCode::Good)
            .set_overflow(false)
            .bits(),
        record.payload
    ])
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_history_fallback_recognizes_service_faults_but_not_access_denials() {
        for status in [
            StatusCode::BadHistoryOperationUnsupported,
            StatusCode::BadNotSupported,
            StatusCode::BadServiceUnsupported,
        ] {
            assert!(unsupported(status), "{status}");
        }
        for status in [
            StatusCode::BadUserAccessDenied,
            StatusCode::BadTimeout,
            StatusCode::BadDataUnavailable,
        ] {
            assert!(!unsupported(status), "{status}");
        }
    }

    #[test]
    fn recovered_overlap_survives_recovery_rounds_until_the_ordered_tail_reaches_it() {
        let mut history = History::new(
            HistoryConfig {
                start: DateTime::from(10),
                page_rows: 1,
            },
            1,
        )
        .unwrap();
        history.supported.insert(0);
        let sample = |ticks, number| {
            let value = DataValue::new_at(number, DateTime::from(ticks));
            let record = Record {
                source: None,
                kind: "node".into(),
                key: "node".into(),
                source_ts_ms: 0,
                ingest_ts_ms: 0,
                quality: None,
                position: 0,
                payload: format!("{{\"value\":{number}}}"),
            };
            (value, record)
        };
        for (ticks, number) in [(10, 1.0), (11, 2.0), (12, 3.0)] {
            let (value, record) = sample(ticks, number);
            history.overlap[0].insert(identity(0, ticks, &value, &record));
            assert!(history.accept(0, &value, &record).unwrap());
        }
        let mut restored: History =
            serde_json::from_str(&serde_json::to_string(&history).unwrap()).unwrap();
        let (last, record) = sample(12, 3.0);
        assert!(
            !restored.accept(0, &last, &record).unwrap(),
            "a second recovery includes its durable watermark"
        );
        let (older, old_record) = sample(11, 2.0);
        assert!(!restored.accept_live(0, &older, &old_record).unwrap());
        let (changed, changed_record) = sample(11, 9.0);
        assert!(
            restored
                .accept_live(0, &changed, &changed_record)
                .unwrap_err()
                .message
                .contains("timestamp regressed")
        );
        assert!(!restored.accept_live(0, &last, &record).unwrap());
        assert!(
            restored.overlap[0].is_empty(),
            "the ordered tail releases retained overlap"
        );
    }
}
