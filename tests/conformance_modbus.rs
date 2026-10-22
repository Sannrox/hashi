//! Conformance kit (#28) for the Modbus family over an in-process register simulator.
use hashi::conformance::{self, CHECKS, Change, Family, INSTANCE};
use hashi::modbus::{
    Device, Endpoint, ModbusConnector, Order, PollGroup, Register, Table, Transport, ValueType,
};
use hashi::{CheckpointStore, Error, FileCheckpoint, SystemClock};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};

static SEQ: AtomicU64 = AtomicU64::new(0);

/// Answers holding-register reads with one shared value, like a device whose
/// register a process changes.
#[derive(Clone)]
struct Registers(Arc<AtomicU16>);

impl Transport for Registers {
    fn exchange(&mut self, _: &Device, request: &[u8]) -> Result<Vec<u8>, Error> {
        let [high, low] = self.0.load(Ordering::SeqCst).to_be_bytes();
        Ok(vec![request[0], 2, high, low])
    }
}

struct Modbus {
    registers: Registers,
    state: PathBuf,
    spool: PathBuf,
    schema_version: u64,
}

impl Modbus {
    fn device(&self) -> Device {
        Device {
            endpoint: Endpoint::Tcp("127.0.0.1:1".parse().unwrap()),
            groups: vec![PollGroup {
                kind: "samples".into(),
                record_key: "pump".into(),
                schema_version: self.schema_version,
                table: Table::HoldingRegisters,
                interval_ms: 20,
                registers: vec![Register {
                    name: "level".into(),
                    address: 0,
                    value_type: ValueType::U16,
                    word_order: Order::Big,
                    byte_order: Order::Big,
                    scale: 1.0,
                    offset: 0.0,
                    unit: None,
                }],
            }],
            unit: 1,
            timeout_ms: 500,
            min_request_interval_ms: 5,
            max_backoff_ms: 1000,
        }
    }
}

impl Family for Modbus {
    type Source = ModbusConnector<Registers, SystemClock>;

    fn connect(&mut self) -> Result<Self::Source, Error> {
        ModbusConnector::new(
            "modbus",
            vec![self.device()],
            self.registers.clone(),
            SystemClock,
            &self.spool,
        )
    }

    /// A poller samples current state, so one change yields one record for the group.
    fn produce(&mut self, _: &[&str]) -> Result<Vec<Change>, Error> {
        let level = self.registers.0.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(vec![Change::new("pump", "level", level)])
    }

    /// A register-map revision. The spool fences map changes until its committed
    /// samples are acknowledged, as an operator does before changing a map.
    fn drift(&mut self) -> Result<conformance::Drift, Error> {
        let committed = FileCheckpoint::open(&self.state)?
            .load("modbus", INSTANCE)?
            .map(|checkpoint| checkpoint.position);
        if let Some(committed) = committed {
            self.connect()?.acknowledge(committed)?;
        }
        self.schema_version += 1;
        Ok(conformance::Drift::Reported("samples".into()))
    }
}

#[test]
fn modbus_passes_the_conformance_kit() {
    let root = std::env::temp_dir().join(format!(
        "hashi-conformance-modbus-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let mut family = Modbus {
        registers: Registers(Arc::new(AtomicU16::new(1))),
        state: root.join("state"),
        spool: root.join("spool"),
        schema_version: 1,
    };
    let passed = conformance::run(&mut family, &root.join("state")).unwrap();
    assert_eq!(passed, CHECKS);
    std::fs::remove_dir_all(root).unwrap();
}
