//! Sparkplug B decoding: births, deaths, metric aliases, sequence numbers and rebirth requests.
//!
//! Topics are `spBv1.0/<group>/<type>/<edge node>[/<device>]`. Births register each
//! metric's name, alias and datatype per edge node; data may then carry only the
//! alias. Birth state is written to `state_dir` on every birth and death, so a
//! restarted connector still resolves aliases. A data message from a node with no
//! birth, or after a sequence gap, triggers an NCMD rebirth request. A metric that
//! cannot be resolved becomes an `UNRESOLVED_KIND` record, which the runtime
//! quarantines: it is never guessed.
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

use prost::Message as _;
use serde::{Deserialize, Serialize};
use serde_json::{Value as Json, json};

use crate::Error;
use crate::contract::Record;

/// Kind of records whose metric cannot be resolved or whose message cannot be decoded.
pub const UNRESOLVED_KIND: &str = "__sparkplug_unresolved";

const NAMESPACE: &str = "spBv1.0";
const REBIRTH_METRIC: &str = "Node Control/Rebirth";
const BOOLEAN: u32 = 11;

/// Sparkplug B `Payload`, limited to the fields this decoder reads. Unknown fields
/// (metadata, properties, data sets, templates) are skipped by the protobuf decoder.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Payload {
    #[prost(uint64, optional, tag = "1")]
    pub timestamp: Option<u64>,
    #[prost(message, repeated, tag = "2")]
    pub metrics: Vec<Metric>,
    #[prost(uint64, optional, tag = "3")]
    pub seq: Option<u64>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct Metric {
    #[prost(string, optional, tag = "1")]
    pub name: Option<String>,
    #[prost(uint64, optional, tag = "2")]
    pub alias: Option<u64>,
    #[prost(uint64, optional, tag = "3")]
    pub timestamp: Option<u64>,
    #[prost(uint32, optional, tag = "4")]
    pub datatype: Option<u32>,
    #[prost(bool, optional, tag = "7")]
    pub is_null: Option<bool>,
    #[prost(oneof = "Value", tags = "10, 11, 12, 13, 14, 15, 16")]
    pub value: Option<Value>,
}

#[derive(Clone, PartialEq, prost::Oneof)]
pub enum Value {
    #[prost(uint32, tag = "10")]
    Int(u32),
    #[prost(uint64, tag = "11")]
    Long(u64),
    #[prost(float, tag = "12")]
    Float(f32),
    #[prost(double, tag = "13")]
    Double(f64),
    #[prost(bool, tag = "14")]
    Boolean(bool),
    #[prost(string, tag = "15")]
    String(String),
    #[prost(bytes = "vec", tag = "16")]
    Bytes(Vec<u8>),
}

/// An NCMD the connector must publish to make an edge node send its births again.
pub struct Rebirth {
    pub topic: String,
    pub payload: Vec<u8>,
}

/// Birth state of every edge node seen by this instance, fenced by a lease on `state_dir`.
pub struct Hosts {
    nodes: BTreeMap<String, Node>,
    path: PathBuf,
    /// Minimum wait before asking the same node for a rebirth again.
    rebirth_interval_ms: i64,
    _lease: File,
}

#[derive(Default)]
struct Node {
    bd_seq: Option<u64>,
    born: bool,
    /// Expected next sequence number; unknown after a restart until the next message.
    next_seq: Option<u64>,
    metrics: BTreeMap<(Option<String>, String), Known>,
    /// Keyed by owner and alias: the spec makes aliases unique per edge node,
    /// but a device that reuses another's alias must not displace it.
    aliases: BTreeMap<(Option<String>, u64), String>,
    rebirth_requested_ms: Option<i64>,
    rebirth_pending: bool,
}

#[derive(Clone, Serialize, Deserialize)]
struct Known {
    device: Option<String>,
    name: String,
    alias: Option<u64>,
    datatype: u32,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct NodeFile {
    node: String,
    bd_seq: Option<u64>,
    born: bool,
    metrics: Vec<Known>,
}

struct Topic<'a> {
    group: &'a str,
    message: &'a str,
    edge: &'a str,
    device: Option<&'a str>,
}

impl Hosts {
    pub fn open(state_dir: &Path, rebirth_interval_ms: i64) -> Result<Self, Error> {
        std::fs::create_dir_all(state_dir)?;
        let lease = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(state_dir.join("sparkplug.lease"))?;
        lease
            .try_lock()
            .map_err(|_| Error::new("Sparkplug state directory is already owned"))?;
        let path = state_dir.join("sparkplug-births.v1.json");
        let files: Vec<NodeFile> = match std::fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map_err(|_| Error::new("Sparkplug birth state is unreadable"))?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => return Err(error.into()),
        };
        let mut nodes = BTreeMap::new();
        for file in files {
            let mut node = Node {
                bd_seq: file.bd_seq,
                born: file.born,
                ..Node::default()
            };
            for known in file.metrics {
                node.register(known);
            }
            nodes.insert(file.node, node);
        }
        Ok(Self {
            nodes,
            path,
            rebirth_interval_ms,
            _lease: lease,
        })
    }

    /// Records for one message (kind and key set, position 0) and an optional rebirth request.
    pub fn decode(
        &mut self,
        kind: &str,
        topic: &str,
        bytes: &[u8],
        now_ms: i64,
    ) -> Result<(Vec<Record>, Option<Rebirth>), Error> {
        let Some(parsed) = parse_topic(topic) else {
            return Ok((
                vec![unresolved(
                    topic,
                    now_ms,
                    json!({"reason": "not a Sparkplug B topic", "raw": hex(bytes)}),
                )],
                None,
            ));
        };
        if matches!(parsed.message, "NCMD" | "DCMD" | "STATE") {
            return Ok((Vec::new(), None));
        }
        // Node messages never name a device and device messages always do; anything
        // else must not touch birth state.
        let device_message = parsed.message.starts_with('D');
        if device_message != parsed.device.is_some() {
            return Ok((
                vec![unresolved(
                    topic,
                    now_ms,
                    json!({"reason": "topic shape does not match the message type", "raw": hex(bytes)}),
                )],
                None,
            ));
        }
        let payload = Payload::decode(bytes)
            .ok()
            .filter(|payload| payload.seq.is_none_or(|seq| seq <= 255));
        let Some(payload) = payload else {
            return Ok((
                vec![unresolved(
                    topic,
                    now_ms,
                    json!({"reason": "payload is not Sparkplug B or seq is out of range", "raw": hex(bytes)}),
                )],
                None,
            ));
        };
        let node_key = format!("{}/{}", parsed.group, parsed.edge);
        let device = parsed.device.map(str::to_string);
        let mut path = node_key.clone();
        if let Some(device) = &device {
            path = format!("{path}/{device}");
        }
        let source_ts = payload.timestamp.map_or(now_ms, |ts| ts as i64);
        let lifecycle = |extra: Json| Record {
            source: None,
            kind: kind.to_string(),
            key: path.clone(),
            source_ts_ms: source_ts,
            ingest_ts_ms: 0,
            quality: None,
            position: 0,
            payload: merge(json!({"type": parsed.message, "seq": payload.seq}), extra).to_string(),
        };
        let node = self.nodes.entry(node_key.clone()).or_default();
        let mut records = Vec::new();
        let mut changed = false;
        let mut needs_rebirth = false;
        match parsed.message {
            "NBIRTH" if payload.seq.is_none() || bd_seq(&payload).is_none() => {
                // Without its session id a later NDEATH could not end it; keep the old certificates.
                return Ok((
                    vec![unresolved(
                        topic,
                        source_ts,
                        json!({"reason": "NBIRTH without seq or bdSeq"}),
                    )],
                    None,
                ));
            }
            "NBIRTH" => {
                // A new session replaces every certificate; the rebirth rate limit carries over.
                *node = Node {
                    bd_seq: bd_seq(&payload),
                    born: true,
                    next_seq: payload.seq.map(next_seq),
                    rebirth_requested_ms: node.rebirth_requested_ms,
                    ..Node::default()
                };
                register_birth(node, None, &payload);
                records.push(lifecycle(json!({"bdSeq": node.bd_seq})));
                changed = true;
            }
            "NDEATH" => {
                let bd = bd_seq(&payload);
                // A death from an older session (its bdSeq differs) says nothing about the
                // current one. A redelivered death of this session emits its record again.
                if bd.is_none() || bd != node.bd_seq {
                    return Ok((Vec::new(), None));
                }
                // The session's certificates die with it: data before the next birth is unresolved.
                node.born = false;
                node.next_seq = None;
                node.metrics.clear();
                node.aliases.clear();
                records.push(lifecycle(json!({"bdSeq": bd})));
                changed = true;
            }
            "DBIRTH" | "DDEATH" | "NDATA" | "DDATA" => {
                needs_rebirth = !node.born || !node.check_seq(payload.seq);
                if matches!(parsed.message, "DBIRTH" | "DDEATH") {
                    // A device death invalidates its certificate until the next DBIRTH.
                    node.metrics.retain(|(owner, _), _| *owner != device);
                    node.aliases.retain(|(owner, _), _| *owner != device);
                    if parsed.message == "DBIRTH" {
                        register_birth(node, device.clone(), &payload);
                    }
                    changed = true;
                }
                if parsed.message.ends_with("BIRTH") || parsed.message.ends_with("DEATH") {
                    records.push(lifecycle(json!({})));
                }
            }
            _ => {
                return Ok((
                    vec![unresolved(
                        topic,
                        now_ms,
                        json!({"reason": "unknown Sparkplug B message type"}),
                    )],
                    None,
                ));
            }
        }
        if parsed.message != "NDEATH" {
            for metric in &payload.metrics {
                if parsed.message == "NBIRTH" && metric.name.as_deref() == Some("bdSeq") {
                    continue;
                }
                let record = node.metric_record(
                    kind,
                    &path,
                    &device,
                    parsed.message,
                    payload.seq,
                    metric,
                    source_ts,
                    topic,
                );
                // The host lacks a certificate the node thinks it sent: ask for all of them again.
                needs_rebirth |= record.kind == UNRESOLVED_KIND;
                records.push(record);
            }
        }
        // Owed until a birth arrives, so a request suppressed by the interval is retried later.
        node.rebirth_pending |= needs_rebirth;
        let rebirth = if node.rebirth_pending
            && node
                .rebirth_requested_ms
                .is_none_or(|at| now_ms - at >= self.rebirth_interval_ms)
        {
            node.rebirth_requested_ms = Some(now_ms);
            Some(rebirth(parsed.group, parsed.edge, now_ms))
        } else {
            None
        };
        if changed {
            self.persist()?;
        }
        Ok((records, rebirth))
    }

    fn persist(&self) -> Result<(), Error> {
        let files: Vec<NodeFile> = self
            .nodes
            .iter()
            .map(|(key, node)| NodeFile {
                node: key.clone(),
                bd_seq: node.bd_seq,
                born: node.born,
                metrics: node.metrics.values().cloned().collect(),
            })
            .collect();
        let body = serde_json::to_string(&files).map_err(|error| Error::new(error.to_string()))?;
        crate::checkpoint::atomic_write(&self.path, &body)
    }
}

impl Node {
    fn register(&mut self, known: Known) {
        if let Some(alias) = known.alias {
            self.aliases
                .insert((known.device.clone(), alias), known.name.clone());
        }
        self.metrics
            .insert((known.device.clone(), known.name.clone()), known);
    }

    /// True when `seq` is the expected one or nothing is expected yet. Advances the expectation.
    fn check_seq(&mut self, seq: Option<u64>) -> bool {
        let Some(seq) = seq else {
            return false;
        };
        let expected = self.next_seq.replace(next_seq(seq));
        expected.is_none_or(|expected| expected == seq)
    }

    #[allow(clippy::too_many_arguments)]
    fn metric_record(
        &self,
        kind: &str,
        path: &str,
        device: &Option<String>,
        message: &str,
        seq: Option<u64>,
        metric: &Metric,
        source_ts: i64,
        topic: &str,
    ) -> Record {
        let ts = metric.timestamp.map_or(source_ts, |ts| ts as i64);
        // Only a birth certificate makes a metric known; data is never self-describing.
        let key = match (&metric.name, metric.alias) {
            // Both identifiers given: they must name the same certificate.
            (Some(name), Some(alias))
                if self.aliases.get(&(device.clone(), alias)) != Some(name) =>
            {
                None
            }
            (Some(name), _) => Some((device.clone(), name.clone())),
            (None, Some(alias)) => self
                .aliases
                .get(&(device.clone(), alias))
                .map(|name| (device.clone(), name.clone())),
            (None, None) => None,
        };
        let known = key.and_then(|key| self.metrics.get(&key).cloned());
        let Some(known) = known else {
            return unresolved(
                topic,
                ts,
                json!({"reason": "metric is not in a birth certificate", "type": message, "seq": seq, "name": metric.name, "alias": metric.alias}),
            );
        };
        let (value, quality) = match metric.datatype {
            Some(datatype) if datatype != known.datatype => (
                Json::Null,
                Some("bad: datatype differs from the birth certificate".into()),
            ),
            _ => decode_value(known.datatype, metric),
        };
        Record {
            source: None,
            kind: kind.to_string(),
            key: format!("{path}/{}", known.name),
            source_ts_ms: ts,
            ingest_ts_ms: 0,
            quality,
            position: 0,
            payload: json!({"type": message, "seq": seq, "name": known.name, "datatype": known.datatype, "value": value}).to_string(),
        }
    }
}

/// An alias or a name declared twice in one certificate is ambiguous. A duplicate
/// alias is dropped while its names stay known; a duplicate name is not registered.
/// Data that needs either is unresolved.
fn register_birth(node: &mut Node, device: Option<String>, payload: &Payload) {
    let mut aliases = BTreeMap::new();
    let mut names = BTreeMap::new();
    for metric in &payload.metrics {
        if let Some(alias) = metric.alias {
            *aliases.entry(alias).or_insert(0) += 1;
        }
        if let Some(name) = &metric.name {
            *names.entry(name.as_str()).or_insert(0) += 1;
        }
    }
    for metric in &payload.metrics {
        if let (Some(name), Some(datatype)) = (&metric.name, metric.datatype)
            && names[name.as_str()] == 1
        {
            node.register(Known {
                device: device.clone(),
                name: name.clone(),
                alias: metric.alias.filter(|alias| aliases[alias] == 1),
                datatype,
            });
        }
    }
}

fn parse_topic(topic: &str) -> Option<Topic<'_>> {
    let parts: Vec<&str> = topic.split('/').collect();
    match parts.as_slice() {
        [NAMESPACE, "STATE", _] => Some(Topic {
            group: "",
            message: "STATE",
            edge: "",
            device: None,
        }),
        [NAMESPACE, group, message, edge] => Some(Topic {
            group,
            message,
            edge,
            device: None,
        }),
        [NAMESPACE, group, message, edge, device] => Some(Topic {
            group,
            message,
            edge,
            device: Some(device),
        }),
        _ => None,
    }
}

fn next_seq(seq: u64) -> u64 {
    (seq + 1) % 256
}

fn bd_seq(payload: &Payload) -> Option<u64> {
    payload
        .metrics
        .iter()
        .find(|metric| metric.name.as_deref() == Some("bdSeq"))
        .and_then(|metric| match metric.value {
            Some(Value::Long(value)) => Some(value),
            Some(Value::Int(value)) => Some(u64::from(value)),
            _ => None,
        })
}

/// Signed integers travel as two's complement in the unsigned protobuf fields.
fn decode_value(datatype: u32, metric: &Metric) -> (Json, Option<String>) {
    if metric.is_null == Some(true) {
        return (Json::Null, None);
    }
    let value = match (datatype, &metric.value) {
        (1, Some(Value::Int(v))) => json!(*v as u8 as i8),
        (2, Some(Value::Int(v))) => json!(*v as u16 as i16),
        (3, Some(Value::Int(v))) => json!(*v as i32),
        (5..=7, Some(Value::Int(v))) => json!(v),
        (4, Some(Value::Long(v))) => json!(*v as i64),
        (8 | 13, Some(Value::Long(v))) => json!(v),
        (9, Some(Value::Float(v))) => finite(f64::from(*v)),
        (10, Some(Value::Double(v))) => finite(*v),
        (11, Some(Value::Boolean(v))) => json!(v),
        (12 | 14 | 15, Some(Value::String(v))) => json!(v),
        (17 | 18, Some(Value::Bytes(v))) => json!(hex(v)),
        _ => {
            return (
                Json::Null,
                Some("bad: unsupported Sparkplug B value".into()),
            );
        }
    };
    if value.is_null() {
        return (value, Some("bad: non-finite value".into()));
    }
    (value, None)
}

fn finite(value: f64) -> Json {
    serde_json::Number::from_f64(value).map_or(Json::Null, Json::Number)
}

fn rebirth(group: &str, edge: &str, now_ms: i64) -> Rebirth {
    let payload = Payload {
        timestamp: Some(now_ms.max(0) as u64),
        metrics: vec![Metric {
            name: Some(REBIRTH_METRIC.into()),
            datatype: Some(BOOLEAN),
            value: Some(Value::Boolean(true)),
            ..Metric::default()
        }],
        seq: None,
    };
    Rebirth {
        topic: format!("{NAMESPACE}/{group}/NCMD/{edge}"),
        payload: payload.encode_to_vec(),
    }
}

fn unresolved(topic: &str, ts: i64, detail: Json) -> Record {
    Record {
        source: None,
        kind: UNRESOLVED_KIND.into(),
        key: topic.to_string(),
        source_ts_ms: ts,
        ingest_ts_ms: 0,
        quality: None,
        position: 0,
        payload: detail.to_string(),
    }
}

fn merge(mut base: Json, extra: Json) -> Json {
    if let (Some(base), Json::Object(extra)) = (base.as_object_mut(), extra) {
        base.extend(extra);
    }
    base
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metric(
        name: Option<&str>,
        alias: Option<u64>,
        datatype: Option<u32>,
        value: Value,
    ) -> Metric {
        Metric {
            name: name.map(str::to_string),
            alias,
            datatype,
            value: Some(value),
            ..Metric::default()
        }
    }

    fn encode(seq: Option<u64>, metrics: Vec<Metric>) -> Vec<u8> {
        Payload {
            timestamp: Some(1_000),
            metrics,
            seq,
        }
        .encode_to_vec()
    }

    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    /// Unique per test: each holds the directory's lease while it runs.
    /// An NBIRTH payload: seq 0 and a bdSeq unless the metrics carry one.
    fn born(mut metrics: Vec<Metric>) -> Vec<u8> {
        if !metrics.iter().any(|m| m.name.as_deref() == Some("bdSeq")) {
            metrics.insert(0, metric(Some("bdSeq"), None, Some(8), Value::Long(1)));
        }
        encode(Some(0), metrics)
    }

    fn dir() -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "hashi-sparkplug-{}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    fn payloads(records: &[Record]) -> Vec<Json> {
        records
            .iter()
            .map(|r| serde_json::from_str(&r.payload).unwrap())
            .collect()
    }

    #[test]
    fn aliases_resolve_after_birth_and_survive_a_restart() {
        let dir = dir();
        let mut hosts = Hosts::open(&dir, 1_000).unwrap();
        let birth = born(vec![
            metric(Some("bdSeq"), None, Some(8), Value::Long(3)),
            metric(Some("temp"), Some(7), Some(3), Value::Int(-5i32 as u32)),
        ]);
        let (records, rebirth) = hosts
            .decode("plant", "spBv1.0/g/NBIRTH/e", &birth, 0)
            .unwrap();
        assert!(rebirth.is_none());
        assert_eq!(records.len(), 2);
        assert_eq!(payloads(&records)[1]["value"], -5);
        drop(hosts);

        let mut hosts = Hosts::open(&dir, 1_000).unwrap();
        let data = encode(Some(1), vec![metric(None, Some(7), None, Value::Int(9))]);
        let (records, rebirth) = hosts
            .decode("plant", "spBv1.0/g/NDATA/e", &data, 0)
            .unwrap();
        assert!(rebirth.is_none());
        assert_eq!(records[0].key, "g/e/temp");
        assert_eq!(payloads(&records)[0]["value"], 9);
    }

    #[test]
    fn unknown_aliases_and_gaps_request_a_rebirth_without_guessing() {
        let mut hosts = Hosts::open(&dir(), 1_000).unwrap();
        let data = encode(Some(4), vec![metric(None, Some(1), None, Value::Int(1))]);
        let (records, rebirth) = hosts
            .decode("plant", "spBv1.0/g/NDATA/e", &data, 0)
            .unwrap();
        assert_eq!(records[0].kind, UNRESOLVED_KIND);
        let rebirth = rebirth.unwrap();
        assert_eq!(rebirth.topic, "spBv1.0/g/NCMD/e");
        let request = Payload::decode(rebirth.payload.as_slice()).unwrap();
        assert_eq!(request.metrics[0].name.as_deref(), Some(REBIRTH_METRIC));
        // Rate-limited until the interval passes.
        assert!(
            hosts
                .decode("plant", "spBv1.0/g/NDATA/e", &data, 10)
                .unwrap()
                .1
                .is_none()
        );

        let birth = born(vec![metric(Some("a"), Some(1), Some(7), Value::Int(1))]);
        hosts
            .decode("plant", "spBv1.0/g/NBIRTH/e", &birth, 20)
            .unwrap();
        let next = encode(Some(1), vec![metric(None, Some(1), None, Value::Int(2))]);
        assert!(
            hosts
                .decode("plant", "spBv1.0/g/NDATA/e", &next, 30)
                .unwrap()
                .1
                .is_none()
        );
        let gap = encode(Some(3), vec![metric(None, Some(1), None, Value::Int(3))]);
        let (records, rebirth) = hosts
            .decode("plant", "spBv1.0/g/NDATA/e", &gap, 40)
            .unwrap();
        assert!(
            rebirth.is_none(),
            "still inside the interval of the last request"
        );
        // The gap stays owed: the next message after the interval asks again.
        let later = encode(Some(4), vec![metric(None, Some(1), None, Value::Int(4))]);
        let (_, rebirth) = hosts
            .decode("plant", "spBv1.0/g/NDATA/e", &later, 1_000)
            .unwrap();
        assert!(rebirth.is_some(), "a sequence gap requests a rebirth");
        assert_eq!(
            payloads(&records)[0]["value"],
            3,
            "known aliases still resolve"
        );
    }

    #[test]
    fn a_stale_death_is_ignored_and_a_current_one_ends_the_session() {
        let mut hosts = Hosts::open(&dir(), 1_000).unwrap();
        let bd = |n| {
            encode(
                None,
                vec![metric(Some("bdSeq"), None, Some(8), Value::Long(n))],
            )
        };
        let birth = born(vec![
            metric(Some("bdSeq"), None, Some(8), Value::Long(2)),
            metric(Some("t"), Some(5), Some(10), Value::Double(1.0)),
        ]);
        hosts
            .decode("plant", "spBv1.0/g/NBIRTH/e", &birth, 0)
            .unwrap();
        assert!(
            hosts
                .decode("plant", "spBv1.0/g/NDEATH/e", &bd(1), 0)
                .unwrap()
                .0
                .is_empty()
        );
        let (records, _) = hosts
            .decode("plant", "spBv1.0/g/NDEATH/e", &bd(2), 0)
            .unwrap();
        assert_eq!(payloads(&records)[0]["type"], "NDEATH");
        // Redelivered after a crash before commit: the record comes again.
        let (records, _) = hosts
            .decode("plant", "spBv1.0/g/NDEATH/e", &bd(2), 0)
            .unwrap();
        assert_eq!(payloads(&records)[0]["type"], "NDEATH");
        let old_alias = encode(
            Some(1),
            vec![metric(None, Some(5), None, Value::Double(2.0))],
        );
        let (records, rebirth) = hosts
            .decode("plant", "spBv1.0/g/NDATA/e", &old_alias, 0)
            .unwrap();
        assert_eq!(
            records[0].kind, UNRESOLVED_KIND,
            "a dead session's aliases are gone"
        );
        assert!(rebirth.is_some());
    }

    #[test]
    fn data_needs_a_birth_and_a_device_death_invalidates_its_certificate() {
        let mut hosts = Hosts::open(&dir(), 1_000).unwrap();
        let birth = born(vec![]);
        hosts
            .decode("plant", "spBv1.0/g/NBIRTH/e", &birth, 0)
            .unwrap();
        let named = encode(
            Some(1),
            vec![metric(Some("x"), None, Some(11), Value::Boolean(true))],
        );
        let (records, rebirth) = hosts
            .decode("plant", "spBv1.0/g/NDATA/e", &named, 0)
            .unwrap();
        assert_eq!(
            records[0].kind, UNRESOLVED_KIND,
            "a name with a datatype is not a birth"
        );
        assert!(rebirth.is_some());

        let device = encode(
            Some(2),
            vec![metric(Some("level"), Some(4), Some(9), Value::Float(1.0))],
        );
        hosts
            .decode("plant", "spBv1.0/g/DBIRTH/e/d", &device, 0)
            .unwrap();
        let data = encode(
            Some(3),
            vec![metric(None, Some(4), None, Value::Float(2.0))],
        );
        let (records, _) = hosts
            .decode("plant", "spBv1.0/g/DDATA/e/d", &data, 0)
            .unwrap();
        assert_eq!(records[0].key, "g/e/d/level");
        let retyped = encode(
            Some(4),
            vec![metric(None, Some(4), Some(7), Value::Int(u32::MAX))],
        );
        let (records, _) = hosts
            .decode("plant", "spBv1.0/g/DDATA/e/d", &retyped, 0)
            .unwrap();
        assert_eq!(payloads(&records)[0]["value"], Json::Null);
        assert_eq!(
            records[0].quality.as_deref(),
            Some("bad: datatype differs from the birth certificate")
        );
        hosts
            .decode("plant", "spBv1.0/g/DDEATH/e/d", &encode(Some(5), vec![]), 0)
            .unwrap();
        let after = encode(
            Some(6),
            vec![metric(None, Some(4), None, Value::Float(3.0))],
        );
        let (records, _) = hosts
            .decode("plant", "spBv1.0/g/DDATA/e/d", &after, 0)
            .unwrap();
        assert_eq!(records[0].kind, UNRESOLVED_KIND);
    }

    #[test]
    fn a_topic_shape_that_contradicts_the_message_type_changes_nothing() {
        let mut hosts = Hosts::open(&dir(), 1_000).unwrap();
        let birth = born(vec![metric(
            Some("t"),
            Some(1),
            Some(10),
            Value::Double(1.0),
        )]);
        hosts
            .decode("plant", "spBv1.0/g/NBIRTH/e", &birth, 0)
            .unwrap();
        let bogus = encode(
            Some(1),
            vec![metric(Some("u"), Some(1), Some(7), Value::Int(1))],
        );
        for topic in ["spBv1.0/g/DBIRTH/e", "spBv1.0/g/NBIRTH/e/d"] {
            let (records, _) = hosts.decode("plant", topic, &bogus, 0).unwrap();
            assert_eq!(records[0].kind, UNRESOLVED_KIND);
        }
        let data = encode(
            Some(1),
            vec![metric(None, Some(1), None, Value::Double(2.0))],
        );
        let (records, _) = hosts
            .decode("plant", "spBv1.0/g/NDATA/e", &data, 0)
            .unwrap();
        assert_eq!(records[0].key, "g/e/t");
    }

    #[test]
    fn devices_may_reuse_an_alias_and_out_of_range_seq_is_unresolved() {
        let mut hosts = Hosts::open(&dir(), 1_000).unwrap();
        hosts
            .decode("plant", "spBv1.0/g/NBIRTH/e", &born(vec![]), 0)
            .unwrap();
        for (seq, device) in [(1, "a"), (2, "b")] {
            let birth = encode(
                Some(seq),
                vec![metric(Some(device), Some(1), Some(7), Value::Int(0))],
            );
            hosts
                .decode("plant", &format!("spBv1.0/g/DBIRTH/e/{device}"), &birth, 0)
                .unwrap();
        }
        let data = encode(Some(3), vec![metric(None, Some(1), None, Value::Int(5))]);
        let (records, _) = hosts
            .decode("plant", "spBv1.0/g/DDATA/e/a", &data, 0)
            .unwrap();
        assert_eq!(records[0].key, "g/e/a/a");
        let wild = encode(Some(u64::MAX), vec![]);
        let (records, _) = hosts
            .decode("plant", "spBv1.0/g/NBIRTH/e", &wild, 0)
            .unwrap();
        assert_eq!(records[0].kind, UNRESOLVED_KIND);
    }

    #[test]
    fn an_alias_declared_twice_in_a_birth_resolves_nothing() {
        let mut hosts = Hosts::open(&dir(), 1_000).unwrap();
        let birth = born(vec![
            metric(Some("a"), Some(1), Some(7), Value::Int(0)),
            metric(Some("b"), Some(1), Some(7), Value::Int(0)),
        ]);
        hosts
            .decode("plant", "spBv1.0/g/NBIRTH/e", &birth, 0)
            .unwrap();
        let data = encode(Some(1), vec![metric(None, Some(1), None, Value::Int(5))]);
        let (records, _) = hosts
            .decode("plant", "spBv1.0/g/NDATA/e", &data, 0)
            .unwrap();
        assert_eq!(records[0].kind, UNRESOLVED_KIND);
        let names = born(vec![
            metric(Some("t"), Some(2), Some(1), Value::Int(0)),
            metric(Some("t"), Some(3), Some(7), Value::Int(0)),
        ]);
        hosts
            .decode("plant", "spBv1.0/g/NBIRTH/e", &names, 0)
            .unwrap();
        let data = encode(Some(1), vec![metric(None, Some(2), None, Value::Int(255))]);
        let (records, _) = hosts
            .decode("plant", "spBv1.0/g/NDATA/e", &data, 0)
            .unwrap();
        assert_eq!(
            records[0].kind, UNRESOLVED_KIND,
            "a name declared twice is ambiguous"
        );
    }

    #[test]
    fn a_birth_without_bd_seq_and_a_conflicting_name_and_alias_change_nothing() {
        let mut hosts = Hosts::open(&dir(), 1_000).unwrap();
        let birth = born(vec![
            metric(Some("t"), Some(1), Some(7), Value::Int(0)),
            metric(Some("p"), Some(2), Some(7), Value::Int(0)),
        ]);
        hosts
            .decode("plant", "spBv1.0/g/NBIRTH/e", &birth, 0)
            .unwrap();
        let bare = encode(Some(0), vec![]);
        let (records, _) = hosts
            .decode("plant", "spBv1.0/g/NBIRTH/e", &bare, 0)
            .unwrap();
        assert_eq!(records[0].kind, UNRESOLVED_KIND);
        let conflict = encode(
            Some(1),
            vec![metric(Some("t"), Some(2), None, Value::Int(5))],
        );
        let (records, _) = hosts
            .decode("plant", "spBv1.0/g/NDATA/e", &conflict, 0)
            .unwrap();
        assert_eq!(records[0].kind, UNRESOLVED_KIND);
        let data = encode(Some(2), vec![metric(None, Some(1), None, Value::Int(6))]);
        let (records, _) = hosts
            .decode("plant", "spBv1.0/g/NDATA/e", &data, 0)
            .unwrap();
        assert_eq!(records[0].key, "g/e/t", "the earlier certificates survive");
    }
}
