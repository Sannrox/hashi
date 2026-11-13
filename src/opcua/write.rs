//! OPC UA as a [`crate::write::Target`]: one Value attribute write per command.
//!
//! `prepare` resolves the node and converts the JSON value to the node's
//! DataType, which it reads from the server; nothing is written. `send` issues
//! exactly one Write request: a Good result is `applied`, a Bad result is
//! `refused` (the server did not apply it), and anything else, including a
//! failed or unanswered request, is `unknown`. The session is connected without
//! automatic reconnects and the request is never retried. After an `unknown`,
//! the session's transport is stopped before `send` returns, so a queued
//! request cannot leave after the caller releases the write fence.

use ::opcua::types::{
    AttributeId, DataTypeId, DataValue, Identifier, NodeId, ReadValueId, StatusCode,
    TimestampsToReturn, Variant, WriteValue,
};
use serde_json::Value;

use super::{OpcUaConnector, parse_root};
use crate::write::{Command, Outcome, Target};

/// A node and a value of its DataType, ready to send.
pub struct PreparedWrite {
    node: NodeId,
    value: Variant,
}

impl Target for OpcUaConnector {
    type Prepared = PreparedWrite;

    fn prepare(&mut self, command: &Command) -> Result<PreparedWrite, String> {
        self.pki_lease
            .renew()
            .map_err(|error| format!("OPC UA PKI store lease lost: {error}"))?;
        let expanded = parse_root(&command.target).map_err(|error| error.message)?;
        let runtime = self
            .runtime
            .as_ref()
            .ok_or_else(|| "OPC UA runtime closed".to_string())?;
        runtime.block_on(async {
            let node = self
                .local_node_id(&expanded)
                .await
                .map_err(|error| error.message)?;
            let read = tokio::time::timeout(
                self.timeout,
                self.session.read(
                    &[ReadValueId {
                        node_id: node.clone(),
                        attribute_id: AttributeId::DataType as u32,
                        ..Default::default()
                    }],
                    TimestampsToReturn::Neither,
                    0.0,
                ),
            )
            .await
            .map_err(|_| "OPC UA data type read timed out".to_string())?
            .map_err(|status| format!("OPC UA data type read failed: {status}"))?;
            let data_type = match read.as_slice() {
                [value] if value.status.is_none_or(|status| !status.is_bad()) => {
                    match &value.value {
                        Some(Variant::NodeId(id)) => (**id).clone(),
                        _ => return Err("OPC UA node has no data type".to_string()),
                    }
                }
                [value] => {
                    return Err(format!(
                        "OPC UA data type unreadable: {}",
                        value.status.unwrap_or_default()
                    ));
                }
                _ => return Err("OPC UA data type read response incomplete".to_string()),
            };
            let value = convert(&data_type, &command.value)?;
            Ok(PreparedWrite { node, value })
        })
    }

    fn send(&mut self, write: PreparedWrite) -> Outcome {
        let Some(runtime) = self.runtime.as_ref() else {
            return Outcome::Refused {
                reason: "OPC UA runtime closed".into(),
            };
        };
        let request = WriteValue {
            node_id: write.node,
            attribute_id: AttributeId::Value as u32,
            index_range: Default::default(),
            value: DataValue::value_only(write.value),
        };
        let response = runtime.block_on(async {
            tokio::time::timeout(self.timeout, self.session.write(&[request])).await
        });
        let outcome = classify(response);
        if matches!(outcome, Outcome::Unknown { .. }) {
            // A dropped request can still sit in the transport queue. The
            // transport runs inside the event loop, so stop it and wait for it
            // to end before the caller records the outcome and releases the
            // fence. Later commands on this session are refused.
            self.session.disable_reconnects();
            self.event_loop.abort();
            let _ = runtime.block_on(&mut self.event_loop);
        }
        outcome
    }
}

/// Classify one Write response: Good is applied, Bad is refused, anything
/// else (uncertain status, failed or unanswered request) is unknown.
fn classify(
    response: Result<Result<Vec<StatusCode>, ::opcua::types::Error>, tokio::time::error::Elapsed>,
) -> Outcome {
    match response {
        Ok(Ok(statuses)) => match statuses.as_slice() {
            [status] if status.is_good() => Outcome::Applied,
            [status] if status.is_bad() => Outcome::Refused {
                reason: format!("OPC UA server refused the write: {status}"),
            },
            [status] => Outcome::Unknown {
                reason: format!("OPC UA write returned {status}"),
            },
            _ => Outcome::Unknown {
                reason: "OPC UA write response incomplete".into(),
            },
        },
        Ok(Err(status)) => Outcome::Unknown {
            reason: format!("OPC UA write request failed: {status}"),
        },
        Err(_) => Outcome::Unknown {
            reason: "OPC UA write response timed out".into(),
        },
    }
}

/// Convert a JSON scalar to the built-in type `data_type` names. Integers must
/// fit; Float must be finite and in range. Other types are refused.
fn convert(data_type: &NodeId, value: &Value) -> Result<Variant, String> {
    let mismatch = || format!("value does not fit OPC UA data type {data_type}");
    let id = match (data_type.namespace, &data_type.identifier) {
        (0, Identifier::Numeric(id)) => DataTypeId::try_from(*id).ok(),
        _ => None,
    }
    .ok_or_else(|| format!("unsupported OPC UA data type {data_type}"))?;
    let int = || value.as_i64().ok_or_else(mismatch);
    let uint = || value.as_u64().ok_or_else(mismatch);
    let float = || {
        value
            .as_f64()
            .filter(|number| number.is_finite())
            .ok_or_else(mismatch)
    };
    Ok(match id {
        DataTypeId::Boolean => Variant::Boolean(value.as_bool().ok_or_else(mismatch)?),
        DataTypeId::SByte => Variant::SByte(i8::try_from(int()?).map_err(|_| mismatch())?),
        DataTypeId::Byte => Variant::Byte(u8::try_from(uint()?).map_err(|_| mismatch())?),
        DataTypeId::Int16 => Variant::Int16(i16::try_from(int()?).map_err(|_| mismatch())?),
        DataTypeId::UInt16 => Variant::UInt16(u16::try_from(uint()?).map_err(|_| mismatch())?),
        DataTypeId::Int32 => Variant::Int32(i32::try_from(int()?).map_err(|_| mismatch())?),
        DataTypeId::UInt32 => Variant::UInt32(u32::try_from(uint()?).map_err(|_| mismatch())?),
        DataTypeId::Int64 => Variant::Int64(int()?),
        DataTypeId::UInt64 => Variant::UInt64(uint()?),
        DataTypeId::Float => {
            let number = float()?;
            if number.abs() > f64::from(f32::MAX) {
                return Err(mismatch());
            }
            Variant::Float(number as f32)
        }
        DataTypeId::Double | DataTypeId::Duration => Variant::Double(float()?),
        DataTypeId::String => Variant::from(value.as_str().ok_or_else(mismatch)?),
        _ => return Err(format!("unsupported OPC UA data type {data_type}")),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_that_do_not_fit_the_node_data_type_are_refused() {
        let int16: NodeId = DataTypeId::Int16.into();
        assert_eq!(
            convert(&int16, &Value::from(-5)).unwrap(),
            Variant::Int16(-5)
        );
        assert!(convert(&int16, &Value::from(40_000)).is_err());
        assert!(convert(&int16, &Value::from(1.5)).is_err());
        let float: NodeId = DataTypeId::Float.into();
        assert!(convert(&float, &Value::from(1e300)).is_err());
        let boolean: NodeId = DataTypeId::Boolean.into();
        assert!(convert(&boolean, &Value::from("true")).is_err());
        assert!(convert(&NodeId::new(2, "Custom"), &Value::from(1)).is_err());
    }
}
