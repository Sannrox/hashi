#![cfg(feature = "modbus")]
//! Soak harness (#48) for the Modbus family over an in-process register simulator.
//! A network drop makes the device stop answering; a source restart also ends
//! the session. Modbus has no credential to rotate.
use hashi::conformance::{self, Change, Family};
use hashi::modbus::{
    Device, Endpoint, ModbusConnector, Order, PollGroup, Register, Table, Transport, ValueType,
};
use hashi::soak::{self, Fault, Faults};
use hashi::{Error, SystemClock};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU64, Ordering};
use std::time::Duration;

static SEQ: AtomicU64 = AtomicU64::new(0);

/// A holding register a process changes, on a device that can stop answering.
#[derive(Clone)]
struct Registers {
    value: Arc<AtomicU16>,
    down: Arc<AtomicBool>,
}

impl Transport for Registers {
    fn exchange(&mut self, _: &Device, request: &[u8]) -> Result<Vec<u8>, Error> {
        if self.down.load(Ordering::SeqCst) {
            return Err(Error::new("device not answering"));
        }
        let [high, low] = self.value.load(Ordering::SeqCst).to_be_bytes();
        Ok(vec![request[0], 2, high, low])
    }
}

struct Modbus {
    registers: Registers,
    spool: PathBuf,
}

impl Family for Modbus {
    type Source = ModbusConnector<Registers, SystemClock>;

    fn connect(&mut self) -> Result<Self::Source, Error> {
        let device = Device {
            endpoint: Endpoint::Tcp("127.0.0.1:1".parse().unwrap()),
            groups: vec![PollGroup {
                kind: "samples".into(),
                record_key: "pump".into(),
                schema_version: 1,
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
            max_backoff_ms: 200,
        };
        ModbusConnector::new(
            "modbus",
            vec![device],
            self.registers.clone(),
            SystemClock,
            &self.spool,
        )
    }

    /// A poller samples current state, so one change yields one record.
    fn produce(&mut self, _: &[&str]) -> Result<Vec<Change>, Error> {
        let level = self
            .registers
            .value
            .fetch_add(1, Ordering::SeqCst)
            .wrapping_add(1);
        Ok(vec![Change::new("pump", "level", level)])
    }

    fn drift(&mut self) -> Result<conformance::Drift, Error> {
        Err(Error::new("the soak harness never drifts"))
    }
}

impl Faults for Modbus {
    fn faults(&self) -> Vec<Fault> {
        vec![Fault::NetworkDrop, Fault::SourceRestart]
    }

    /// A restarting device stops answering until it is back.
    fn inject(&mut self, _: Fault) -> Result<(), Error> {
        self.registers.down.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn heal(&mut self, _: Fault) -> Result<(), Error> {
        self.registers.down.store(false, Ordering::SeqCst);
        Ok(())
    }
}

#[test]
fn modbus_survives_a_soak_with_every_fault() {
    let root = std::env::temp_dir().join(format!(
        "hashi-soak-modbus-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let mut family = Modbus {
        registers: Registers {
            value: Arc::new(AtomicU16::new(1)),
            down: Arc::new(AtomicBool::new(false)),
        },
        spool: root.join("spool"),
    };
    let report = soak::soak_from_env(
        &mut family,
        "modbus",
        &root.join("state"),
        Duration::from_secs(6),
    );
    assert!(report.passed, "{report:#?}");
    for fault in family.faults() {
        assert!(report.faults.contains_key(&fault), "{fault:?}: {report:#?}");
    }
    std::fs::remove_dir_all(root).unwrap();
}
