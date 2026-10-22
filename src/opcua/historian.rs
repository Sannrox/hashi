//! OPC UA historical access behind the historian trait (#23).
//!
//! [`OpcUaHistorian`] discovers variables under the configured roots, keeps
//! those whose access level allows history reads as tags named by namespace
//! URI and identifier, and reports the rest. Raw reads follow `HistoryRead` continuation points until a page is
//! filled and release the one they do not follow. Interpolated reads are
//! not offered: the server's processed history is not part of this adapter.

use ::opcua::client::UARequest;
use ::opcua::client::{HistoryReadAction, services::HistoryRead};
use ::opcua::types::{
    ByteString, DataValue, DateTime, HistoryData, HistoryReadValueId, ReadRawModifiedDetails,
    VariableId,
};

use super::*;
use crate::historian::{Historian, Sample, Tag};

/// OPC UA ticks (100 ns since 1601) at the Unix epoch.
const UNIX_EPOCH_TICKS: i64 = 116_444_736_000_000_000;
const TICKS_PER_MS: i64 = 10_000;
/// Continuation pages followed for one read before it is refused.
const MAX_PAGES: usize = 4096;
/// The history-read bit of the AccessLevel attribute.
const HISTORY_READ: u8 = 4;

fn ticks(ms: i64) -> i64 {
    ms.saturating_mul(TICKS_PER_MS)
        .saturating_add(UNIX_EPOCH_TICKS)
}

fn millis(ticks: i64) -> i64 {
    (ticks - UNIX_EPOCH_TICKS).div_euclid(TICKS_PER_MS)
}

pub struct OpcUaHistorian {
    config: Config,
    /// `None` after a failed read closed the session; the next read opens
    /// a new one.
    connector: Option<OpcUaConnector>,
    /// The server's namespace table at discovery. Tags carry namespace
    /// indexes, so a read under a different table is refused.
    namespaces: Vec<String>,
    nodes: BTreeMap<String, NodeId>,
    without_history: Vec<String>,
}

impl OpcUaHistorian {
    /// Connect, discover, and split variables by history support.
    pub fn connect(config: Config) -> Result<Self, Error> {
        // Long-lived: the session reconnects after a transient loss.
        let connector = OpcUaConnector::connect_inner(config.clone(), true)?;
        // Bounded by the configured timeout and refused inside a runtime.
        let kinds = connector.discover()?;
        let runtime = connector
            .runtime
            .as_ref()
            .ok_or_else(|| Error::new("OPC UA runtime closed"))?;
        let capabilities = async {
            let namespaces = namespace_table(&connector.session).await?;
            let mut nodes = BTreeMap::new();
            let mut without = Vec::new();
            for kind in kinds {
                let node: NodeId = kind
                    .kind
                    .parse()
                    .map_err(|_| Error::new("OPC UA discovered node id is unreadable"))?;
                let access = connector
                    .session
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
                // Only a successful read without the history bit is "no
                // history"; an unreadable capability fails the connection.
                let level = match access.as_slice() {
                    [
                        DataValue {
                            value: Some(Variant::Byte(level)),
                            status,
                            ..
                        },
                    ] if status.is_none_or(|status| status.is_good()) => *level,
                    _ => return Err(Error::new("OPC UA history capability unavailable")),
                };
                // Tags name the namespace by URI, so cursors stay bound to
                // the same series when a restarted server renumbers.
                let uri = namespaces
                    .get(usize::from(node.namespace))
                    .ok_or_else(|| Error::new("OPC UA node namespace is not in the table"))?;
                // Escaping `;` in the URI makes the first `;` the separator.
                let uri = uri.replace('%', "%25").replace(';', "%3B");
                let tag = format!("nsu={uri};{}", node.identifier);
                if nodes.contains_key(&tag) || without.contains(&tag) {
                    return Err(Error::new("OPC UA discovery yields one tag twice"));
                }
                if level & HISTORY_READ != 0 {
                    nodes.insert(tag, node);
                } else {
                    without.push(tag);
                }
            }
            Ok::<_, Error>((namespaces, nodes, without))
        };
        let (namespaces, nodes, without_history) = runtime.block_on(async {
            tokio::time::timeout(connector.timeout, capabilities)
                .await
                .map_err(|_| Error::new("OPC UA history capability read timed out"))?
        })?;
        Ok(Self {
            config,
            connector: Some(connector),
            namespaces,
            nodes,
            without_history,
        })
    }

    fn node(&self, tag: &str) -> Result<NodeId, Error> {
        self.nodes
            .get(tag)
            .cloned()
            .ok_or_else(|| Error::new(format!("OPC UA tag {tag} has no history")))
    }
}

/// The server's namespace table, in index order.
async fn namespace_table(session: &Session) -> Result<Vec<String>, Error> {
    let values = session
        .read(
            &[ReadValueId {
                node_id: VariableId::Server_NamespaceArray.into(),
                attribute_id: AttributeId::Value as u32,
                ..Default::default()
            }],
            TimestampsToReturn::Neither,
            0.0,
        )
        .await
        .map_err(|_| Error::new("OPC UA namespace table read failed"))?;
    match values.as_slice() {
        [
            DataValue {
                value: Some(Variant::Array(array)),
                status,
                ..
            },
        ] if status.is_none_or(|status| status.is_good()) => array
            .values
            .iter()
            .map(|value| match value {
                Variant::String(uri) => Ok(uri.as_ref().to_string()),
                _ => Err(Error::new("OPC UA namespace table is malformed")),
            })
            .collect(),
        _ => Err(Error::new("OPC UA namespace table unavailable")),
    }
}

/// One historical value as a sample in `(after_ms, until_ms]`.
fn sample(tag: &str, value: &DataValue, after_ms: i64, until_ms: i64) -> Result<Sample, Error> {
    let ts_ms = millis(
        value
            .source_timestamp
            .ok_or_else(|| Error::new("OPC UA historical timestamp missing"))?
            .ticks(),
    );
    if ts_ms <= after_ms || ts_ms > until_ms {
        return Err(Error::new("OPC UA history returned a sample out of range"));
    }
    let bad = value.status.filter(|status| !status.is_good());
    // A bad-status entry without a value, such as lost data, is kept as NaN
    // with its status as the quality, so the gap is visible and not fatal.
    let reading = match (value.value.as_ref(), bad) {
        (None | Some(Variant::Empty), Some(_)) => f64::NAN,
        (value, _) => value
            .and_then(number)
            .ok_or_else(|| Error::new(format!("OPC UA tag {tag} is not numeric")))?,
    };
    let quality = bad.map_or_else(|| "good".to_string(), |status| status.to_string());
    Ok(Sample {
        ts_ms,
        value: reading,
        quality: Some(quality),
    })
}

fn number(value: &Variant) -> Option<f64> {
    Some(match value {
        Variant::Boolean(value) => f64::from(u8::from(*value)),
        Variant::SByte(value) => f64::from(*value),
        Variant::Byte(value) => f64::from(*value),
        Variant::Int16(value) => f64::from(*value),
        Variant::UInt16(value) => f64::from(*value),
        Variant::Int32(value) => f64::from(*value),
        Variant::UInt32(value) => f64::from(*value),
        Variant::Int64(value) => *value as f64,
        Variant::UInt64(value) => *value as f64,
        Variant::Float(value) => f64::from(*value),
        Variant::Double(value) => *value,
        _ => return None,
    })
}

impl Historian for OpcUaHistorian {
    fn tags(&mut self) -> Result<Vec<Tag>, Error> {
        Ok(self
            .nodes
            .keys()
            .map(|name| Tag { name: name.clone() })
            .collect())
    }

    fn read_raw(
        &mut self,
        tag: &str,
        after_ms: i64,
        until_ms: i64,
        limit: usize,
    ) -> Result<Vec<Sample>, Error> {
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err(Error::new(
                "call OPC UA synchronous API outside an async runtime",
            ));
        }
        let node = self.node(tag)?;
        // An empty window must not reach the server: a start after the end
        // is a reverse read there.
        if limit == 0 || after_ms >= until_ms {
            return Ok(Vec::new());
        }
        // One sample past the page shows whether the page boundary splits a
        // millisecond, which a millisecond watermark cannot resume inside.
        let want = limit.saturating_add(1);
        let per_page =
            u32::try_from(want).map_err(|_| Error::new("OPC UA page limit too large"))?;
        if self.connector.is_none() {
            self.connector = Some(OpcUaConnector::connect_inner(self.config.clone(), true)?);
        }
        let Some(connector) = self.connector.as_ref() else {
            return Err(Error::new("OPC UA session unavailable"));
        };
        let session = connector.session.clone();
        let timeout = connector.timeout;
        let namespaces = &self.namespaces;
        let runtime = connector
            .runtime
            .as_ref()
            .ok_or_else(|| Error::new("OPC UA runtime closed"))?;
        let read_page = async move {
            // Both server bounds are inclusive ticks. The window is every
            // tick whose millisecond lies in `(after_ms, until_ms]`, so a
            // sub-millisecond sample is never left between two windows.
            let details = ReadRawModifiedDetails {
                is_read_modified: false,
                start_time: DateTime::from(ticks(after_ms.saturating_add(1))),
                end_time: DateTime::from(ticks(until_ms.saturating_add(1)) - 1),
                num_values_per_node: per_page,
                return_bounds: false,
            };
            let read = |continuation: ByteString, release: bool| {
                HistoryRead::new(
                    HistoryReadAction::ReadRawModifiedDetails(details.clone()),
                    &session,
                )
                .timestamps_to_return(TimestampsToReturn::Source)
                .release_continuation_points(release)
                .node(HistoryReadValueId {
                    node_id: node.clone(),
                    continuation_point: continuation,
                    ..Default::default()
                })
                .send(session.channel())
            };
            // Checked on every read: a restart or reconnect may renumber
            // namespaces, and a stale index would read another node.
            if namespace_table(&session).await? != *namespaces {
                return Err(Error::new(
                    "OPC UA namespace table changed; rediscovery required",
                ));
            }
            let mut samples = Vec::new();
            let mut continuation = ByteString::null();
            // A server may return short pages with a continuation point, so
            // pages are followed until the request is filled or exhausted.
            for _ in 0..MAX_PAGES {
                let mut results = read(continuation, false)
                    .await
                    .map_err(|_| Error::new("OPC UA history read failed"))?
                    .results
                    .ok_or_else(|| Error::new("OPC UA history response missing"))?;
                if results.len() != 1 {
                    return Err(Error::new("OPC UA history response count mismatch"));
                }
                let result = results.remove(0);
                if result.status_code.sub_code() == ::opcua::types::SubStatusCode::GoodNoData {
                    return Ok(samples);
                }
                if !result.status_code.is_good() {
                    return Err(Error::new(format!(
                        "OPC UA history read refused: {}",
                        result.status_code
                    )));
                }
                let data = result
                    .history_data
                    .inner_as::<HistoryData>()
                    .ok_or_else(|| Error::new("OPC UA raw history data missing"))?;
                continuation = result.continuation_point;
                let page: Result<Vec<_>, _> = data
                    .data_values
                    .iter()
                    .flatten()
                    .map(|value| sample(tag, value, after_ms, until_ms))
                    .collect();
                match page {
                    Ok(page) => samples.extend(page),
                    Err(error) => {
                        if !continuation.is_null_or_empty() {
                            let _ = read(continuation, true).await;
                        }
                        return Err(error);
                    }
                }
                if continuation.is_null_or_empty() {
                    return Ok(samples);
                }
                if samples.len() >= want {
                    // The rest is read from the new watermark.
                    let _ = read(continuation, true).await;
                    return Ok(samples);
                }
            }
            let _ = read(continuation, true).await;
            Err(Error::new("OPC UA history continuation limit exceeded"))
        };
        let result = runtime.block_on(async {
            tokio::time::timeout(timeout, read_page)
                .await
                .map_err(|_| Error::new("OPC UA history read timed out"))?
        });
        if result.is_err() {
            // A failed or cancelled read may hold a continuation point the
            // client never saw. Dropping the connector closes the session,
            // which releases it; the next read opens a new session.
            self.connector = None;
        }
        let mut samples = result?;
        if samples.len() > limit {
            if samples[limit].ts_ms == samples[limit - 1].ts_ms {
                return Err(Error::new(format!(
                    "OPC UA tag {tag} has more samples in one millisecond than a page holds"
                )));
            }
            samples.truncate(limit);
        }
        Ok(samples)
    }

    /// Discovered variables whose access level does not allow history reads.
    fn without_history(&self) -> &[String] {
        &self.without_history
    }

    fn read_interpolated(
        &mut self,
        _tag: &str,
        _start_ms: i64,
        _end_ms: i64,
        _step_ms: i64,
    ) -> Result<Vec<Sample>, Error> {
        Err(Error::new(
            "OPC UA interpolated history is not offered by this adapter",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ticks_and_milliseconds_round_trip() {
        assert_eq!(millis(ticks(1_700_000_000_123)), 1_700_000_000_123);
        assert_eq!(millis(UNIX_EPOCH_TICKS), 0);
        assert_eq!(number(&Variant::Boolean(true)), Some(1.0));
        assert_eq!(number(&Variant::from("text")), None);
        let lost = DataValue {
            value: None,
            status: Some(StatusCode::BadDataLost),
            source_timestamp: Some(DateTime::from(ticks(5))),
            ..Default::default()
        };
        let sample = sample("t", &lost, 0, 10).unwrap();
        assert!(sample.value.is_nan());
        assert!(sample.quality.unwrap().contains("BadDataLost"));
    }
}
