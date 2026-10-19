use std::sync::atomic::AtomicU64;
static DIR_SEQ: AtomicU64 = AtomicU64::new(0);
fn state_dir() -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "hashi-modbus-{}-{}",
        std::process::id(),
        DIR_SEQ.fetch_add(1, Ordering::Relaxed)
    ))
}
use hashi::modbus::{
    Device, Endpoint, ModbusConnector, NetworkTransport, Order, PollGroup, Register, Table,
    Transport, ValueType,
};
use hashi::runtime::Clock;
use hashi::{Connector, Error};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{
    Arc,
    atomic::{AtomicI64, Ordering},
};

fn register(name: &str, address: u16, ty: ValueType) -> Register {
    Register {
        name: name.into(),
        address,
        value_type: ty,
        word_order: Order::Big,
        byte_order: Order::Big,
        scale: 1.0,
        offset: 0.0,
        unit: Some("unit".into()),
    }
}
fn group(table: Table, registers: Vec<Register>) -> PollGroup {
    PollGroup {
        kind: "samples".into(),
        record_key: "device".into(),
        schema_version: 1,
        table,
        interval_ms: 100,
        registers,
    }
}
fn device(endpoint: Endpoint, groups: Vec<PollGroup>) -> Device {
    Device {
        endpoint,
        groups,
        unit: 1,
        timeout_ms: 500,
        min_request_interval_ms: 10,
        max_backoff_ms: 1000,
    }
}
fn tcp_simulator(
    unit: u8,
    function: u8,
    expected_count: u16,
    data: Vec<u8>,
) -> (Endpoint, std::thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = Endpoint::Tcp(listener.local_addr().unwrap());
    let thread = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(2)))
            .unwrap();
        let mut request = [0; 12];
        stream.read_exact(&mut request).unwrap();
        assert_eq!(&request[2..8], &[0, 0, 0, 6, unit, function]);
        assert_eq!(
            u16::from_be_bytes([request[10], request[11]]),
            expected_count
        );
        let mut response = request[..7].to_vec();
        response[4..6].copy_from_slice(&((data.len() + 3) as u16).to_be_bytes());
        response.extend_from_slice(&[function, data.len() as u8]);
        response.extend_from_slice(&data);
        stream.write_all(&response).unwrap();
    });
    (endpoint, thread)
}

#[test]
fn tcp_simulator_decodes_all_numeric_types_and_word_byte_orders_in_one_batch() {
    let fixtures: [(ValueType, &[u8], f64); 6] = [
        (ValueType::I16, &[0xff, 0x9c], -100.0),
        (ValueType::U16, &[0xfe, 0xdc], 65244.0),
        (ValueType::I32, &[0xff, 0xfe, 0x1d, 0xc0], -123456.0),
        (ValueType::U32, &[0x89, 0xab, 0xcd, 0xef], 2309737967.0),
        (ValueType::F32, &[0x41, 0x48, 0, 0], 12.5),
        (ValueType::F64, &[0xc0, 0x29, 0, 0, 0, 0, 0, 0], -12.5),
    ];
    let mut regs = vec![];
    let mut raw = vec![];
    let mut expected = vec![];
    for (ty, bytes, number) in fixtures {
        for word in [Order::Big, Order::Little] {
            for byte in [Order::Big, Order::Little] {
                let name = format!("v{}", regs.len());
                let mut r = register(&name, (raw.len() / 2) as u16, ty);
                r.word_order = word;
                r.byte_order = byte;
                r.scale = 2.0;
                r.offset = 3.0;
                let mut words: Vec<Vec<u8>> = bytes.chunks(2).map(|v| v.to_vec()).collect();
                if matches!(word, Order::Little) {
                    words.reverse();
                }
                if matches!(byte, Order::Little) {
                    for w in &mut words {
                        w.reverse();
                    }
                }
                raw.extend(words.into_iter().flatten());
                regs.push(r);
                expected.push((name, number * 2.0 + 3.0));
            }
        }
    }
    for table in [Table::HoldingRegisters, Table::InputRegisters] {
        let (endpoint, server) = tcp_simulator(
            255,
            if table == Table::HoldingRegisters {
                3
            } else {
                4
            },
            (raw.len() / 2) as u16,
            raw.clone(),
        );
        let mut connector = ModbusConnector::new(
            "modbus",
            vec![Device {
                unit: 255,
                ..device(endpoint, vec![group(table, regs.clone())])
            }],
            NetworkTransport::default(),
            TestClock(Arc::new(AtomicI64::new(1))),
            state_dir(),
        )
        .unwrap();
        let row = connector.read(None).unwrap().unwrap();
        assert_eq!(row.quality, None);
        let values: serde_json::Value = serde_json::from_str(&row.payload).unwrap();
        for (name, value) in &expected {
            assert_eq!(values[name].as_f64(), Some(*value));
            assert_eq!(values[format!("{name}_unit")], "unit");
        }
        assert_eq!(
            connector.read(None).unwrap(),
            Some(row.clone()),
            "unacknowledged observation must replay"
        );

        server.join().unwrap();
    }
}

#[test]
fn bit_tables_use_lsb_first_and_batch_adjacent_bits() {
    for table in [Table::Coils, Table::DiscreteInputs] {
        let (endpoint, server) = tcp_simulator(
            1,
            if table == Table::Coils { 1 } else { 2 },
            9,
            vec![0b10000101, 1],
        );
        let regs = (0..9)
            .map(|n| register(&format!("b{n}"), n, ValueType::Bool))
            .collect();
        let mut c = ModbusConnector::new(
            "modbus",
            vec![device(endpoint, vec![group(table, regs)])],
            NetworkTransport::default(),
            TestClock(Arc::new(AtomicI64::new(1))),
            state_dir(),
        )
        .unwrap();
        let r = c.read(None).unwrap().unwrap();
        let values: serde_json::Value = serde_json::from_str(&r.payload).unwrap();
        for n in 0..9 {
            assert_eq!(values[format!("b{n}")], matches!(n, 0 | 2 | 7 | 8));
        }
        server.join().unwrap();
    }
}

#[derive(Clone)]
struct TestClock(Arc<AtomicI64>);
impl hashi::runtime::Clock for TestClock {
    fn now_ms(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
    fn sleep_ms(&self, ms: u64) {
        self.0.fetch_add(ms as i64, Ordering::SeqCst);
    }
}
struct SelectiveFailure {
    requests: Arc<std::sync::Mutex<Vec<(u8, i64)>>>,
    clock: TestClock,
}
impl Transport for SelectiveFailure {
    fn exchange(&mut self, device: &Device, request: &[u8]) -> Result<Vec<u8>, Error> {
        self.requests
            .lock()
            .unwrap()
            .push((device.unit, self.clock.now_ms()));
        if device.unit == 1 {
            Err(Error::new("device unavailable"))
        } else {
            Ok(vec![request[0], 2, 0, 7])
        }
    }
}
#[test]
fn failed_device_emits_bad_quality_with_backoff_while_other_devices_continue() {
    let clock = TestClock(Arc::new(AtomicI64::new(0)));
    let requests = Arc::new(std::sync::Mutex::new(vec![]));
    let mut a = device(
        Endpoint::Tcp("127.0.0.1:1".parse().unwrap()),
        vec![group(
            Table::HoldingRegisters,
            vec![register("v", 0, ValueType::U16)],
        )],
    );
    a.groups[0].kind = "bad".into();
    let mut b = a.clone();
    b.unit = 2;
    b.groups[0].kind = "good".into();
    b.groups[0].interval_ms = 1;
    let mut c = ModbusConnector::new(
        "modbus",
        vec![a, b],
        SelectiveFailure {
            requests: requests.clone(),
            clock: clock.clone(),
        },
        clock.clone(),
        state_dir(),
    )
    .unwrap();
    let bad = c.read(None).unwrap().unwrap();
    assert!(bad.quality.unwrap().contains("unavailable"));
    let good = c.read(Some(0)).unwrap().unwrap();
    assert_eq!(good.kind, "good");
    assert_eq!(good.quality, None);
    clock.0.store(1, Ordering::SeqCst);
    let next = c.read(Some(1)).unwrap().unwrap();
    assert_eq!(next.kind, "good");
    assert_eq!(
        next.source_ts_ms, 10,
        "timestamp follows the rate-limit wait"
    );
    let requests = requests.lock().unwrap();
    assert_eq!(requests.iter().filter(|(unit, _)| *unit == 1).count(), 1);
    assert_eq!(
        requests
            .iter()
            .filter(|(unit, _)| *unit == 2)
            .map(|(_, ms)| *ms)
            .collect::<Vec<_>>(),
        [0, 10]
    );
}

#[cfg(target_os = "linux")]
#[test]
fn rtu_serial_simulator_checks_crc_and_rejects_corruption() {
    use serialport::SerialPort;
    fn crc(data: &[u8]) -> u16 {
        let mut value = 0xffff;
        for b in data {
            value ^= u16::from(*b);
            for _ in 0..8 {
                let low = value & 1;
                value >>= 1;
                if low != 0 {
                    value ^= 0xa001;
                }
            }
        }
        value
    }
    for corrupt in [false, true] {
        let (mut master, slave) = serialport::TTYPort::pair().unwrap();
        master
            .set_timeout(std::time::Duration::from_secs(2))
            .unwrap();
        let path = slave.name().unwrap();
        let server = std::thread::spawn(move || {
            let _keep_slave = slave;
            let mut request = [0; 8];
            master.read_exact(&mut request).unwrap();
            assert_eq!(&request[..6], &[1, 3, 0, 0, 0, 1]);
            assert_eq!(
                u16::from_le_bytes([request[6], request[7]]),
                crc(&request[..6])
            );
            let mut response = vec![1, 3, 2, 0, 42];
            let check = crc(&response) ^ u16::from(corrupt);
            response.extend_from_slice(&check.to_le_bytes());
            master.write_all(&response).unwrap();
            std::thread::sleep(std::time::Duration::from_millis(20));
        });
        let endpoint = Endpoint::Rtu {
            path,
            baud: 9600,
            parity: serialport::Parity::None,
        };
        let mut c = ModbusConnector::new(
            "modbus",
            vec![device(
                endpoint,
                vec![group(
                    Table::HoldingRegisters,
                    vec![register("v", 0, ValueType::U16)],
                )],
            )],
            NetworkTransport::default(),
            TestClock(Arc::new(AtomicI64::new(0))),
            state_dir(),
        )
        .unwrap();
        let r = c.read(None).unwrap().unwrap();
        if corrupt {
            assert!(r.quality.unwrap().contains("checksum"));
        } else {
            assert_eq!(r.quality, None);
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&r.payload).unwrap()["v"],
                42.0
            );
        }
        server.join().unwrap();
    }
}

#[test]
fn unacknowledged_sample_survives_restart_and_fences_map_changes() {
    let path = state_dir();
    let clock = TestClock(Arc::new(AtomicI64::new(0)));
    let requests = Arc::new(std::sync::Mutex::new(vec![]));
    let mut config = device(
        Endpoint::Tcp("127.0.0.1:1".parse().unwrap()),
        vec![group(
            Table::HoldingRegisters,
            vec![register("v", 0, ValueType::U16)],
        )],
    );
    config.unit = 2;
    let source = || SelectiveFailure {
        requests: requests.clone(),
        clock: clock.clone(),
    };
    let mut c = ModbusConnector::new(
        "modbus",
        vec![config.clone()],
        source(),
        clock.clone(),
        &path,
    )
    .unwrap();
    let record = c.read(Some(5)).unwrap().unwrap();
    assert_eq!(record.position, 6);
    drop(c);
    let mut changed = config.clone();
    changed.groups[0].registers[0].scale = 2.0;
    assert!(ModbusConnector::new("modbus", vec![changed], source(), clock.clone(), &path).is_err());
    let mut resumed =
        ModbusConnector::new("modbus", vec![config], source(), clock.clone(), &path).unwrap();
    assert_eq!(resumed.read(Some(5)).unwrap(), Some(record));
    assert_eq!(
        requests.lock().unwrap().len(),
        1,
        "recovery must not replace the sample with a fresh poll"
    );
    resumed.acknowledge(6).unwrap();
    drop(resumed);
    let mut changed = device(
        Endpoint::Tcp("127.0.0.1:1".parse().unwrap()),
        vec![group(
            Table::HoldingRegisters,
            vec![register("v", 0, ValueType::U16)],
        )],
    );
    changed.groups[0].registers[0].scale = 2.0;
    let updated =
        ModbusConnector::new("modbus", vec![changed], source(), clock.clone(), &path).unwrap();
    drop(updated);
    std::fs::remove_dir_all(path).unwrap();
}

struct FailingGroup {
    attempts: Arc<std::sync::Mutex<Vec<i64>>>,
    clock: TestClock,
}
impl Transport for FailingGroup {
    fn exchange(&mut self, _: &Device, request: &[u8]) -> Result<Vec<u8>, Error> {
        if request[2] == 1 {
            self.attempts.lock().unwrap().push(self.clock.now_ms());
            Err(Error::new("unavailable register group"))
        } else {
            Ok(vec![3, 2, 0, 7])
        }
    }
}
#[test]
fn runtime_keeps_polling_and_healthy_groups_do_not_reset_failing_group_backoff() {
    let path = state_dir();
    let clock = TestClock(Arc::new(AtomicI64::new(0)));
    let attempts = Arc::new(std::sync::Mutex::new(vec![]));
    let mut good = group(
        Table::HoldingRegisters,
        vec![register("v", 0, ValueType::U16)],
    );
    good.kind = "good".into();
    good.interval_ms = 1;
    let mut bad = group(
        Table::HoldingRegisters,
        vec![register("v", 1, ValueType::U16)],
    );
    bad.kind = "bad".into();
    bad.interval_ms = 1;
    let source = ModbusConnector::new(
        "modbus",
        vec![device(
            Endpoint::Tcp("127.0.0.1:1".parse().unwrap()),
            vec![good, bad],
        )],
        FailingGroup {
            attempts: attempts.clone(),
            clock: clock.clone(),
        },
        clock.clone(),
        &path,
    )
    .unwrap();
    let mut rt = hashi::Runtime::new(
        source,
        hashi::MemorySink::new(),
        hashi::FileCheckpoint::open(path.join("checkpoint")).unwrap(),
        clock,
        hashi::RuntimeConfig::new("poller"),
    );
    let result = rt.run_while(|r| r.delivered < 8).unwrap();
    assert_eq!(result.delivered, 8);
    let times = attempts.lock().unwrap();
    assert_eq!(times.len(), 4);
    let gaps: Vec<i64> = times.windows(2).map(|pair| pair[1] - pair[0]).collect();
    assert!(
        gaps[1] > gaps[0] && gaps[2] > gaps[1],
        "backoff must grow despite healthy sibling polls: {times:?}"
    );
    drop(times);
    drop(rt);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn failing_first_group_cannot_starve_healthy_groups() {
    let path = state_dir();
    let clock = TestClock(Arc::new(AtomicI64::new(0)));
    let attempts = Arc::new(std::sync::Mutex::new(vec![]));
    let mut bad = group(
        Table::HoldingRegisters,
        vec![register("v", 1, ValueType::U16)],
    );
    bad.kind = "bad".into();
    bad.interval_ms = 1;
    let mut good = group(
        Table::HoldingRegisters,
        vec![register("v", 0, ValueType::U16)],
    );
    good.kind = "good".into();
    good.interval_ms = 1;
    let mut source = ModbusConnector::new(
        "modbus",
        vec![device(
            Endpoint::Tcp("127.0.0.1:1".parse().unwrap()),
            vec![bad, good],
        )],
        FailingGroup {
            attempts,
            clock: clock.clone(),
        },
        clock,
        &path,
    )
    .unwrap();
    assert_eq!(source.read(None).unwrap().unwrap().kind, "bad");
    let next = source.read(Some(0)).unwrap().unwrap();
    assert_eq!(next.kind, "good");
    assert_eq!(next.quality, None);
    drop(source);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn trickled_tcp_response_cannot_extend_the_exchange_timeout() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = Endpoint::Tcp(listener.local_addr().unwrap());
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(2)))
            .unwrap();
        let mut request = [0; 12];
        stream.read_exact(&mut request).unwrap();
        let mut response = request[..7].to_vec();
        response[4..6].copy_from_slice(&5u16.to_be_bytes());
        response.extend_from_slice(&[3, 2, 0, 42]);
        for byte in response {
            std::thread::sleep(std::time::Duration::from_millis(20));
            if stream.write_all(&[byte]).is_err() {
                break;
            }
        }
    });
    let mut config = device(
        endpoint,
        vec![group(
            Table::HoldingRegisters,
            vec![register("v", 0, ValueType::U16)],
        )],
    );
    config.timeout_ms = 80;
    let mut source = ModbusConnector::new(
        "modbus",
        vec![config],
        NetworkTransport::default(),
        TestClock(Arc::new(AtomicI64::new(0))),
        state_dir(),
    )
    .unwrap();
    let row = source.read(None).unwrap().unwrap();
    assert!(
        row.quality.is_some(),
        "a valid response trickled beyond the deadline must not be admitted"
    );
    server.join().unwrap();
}

#[test]
fn restart_preserves_device_backoff_and_request_reservations() {
    let path = state_dir();
    let clock = TestClock(Arc::new(AtomicI64::new(0)));
    let requests = Arc::new(std::sync::Mutex::new(vec![]));
    let mut config = device(
        Endpoint::Tcp("127.0.0.1:1".parse().unwrap()),
        vec![group(
            Table::HoldingRegisters,
            vec![register("v", 0, ValueType::U16)],
        )],
    );
    config.groups[0].interval_ms = 1;
    for position in 0u64..3 {
        let mut source = ModbusConnector::new(
            "modbus",
            vec![config.clone()],
            SelectiveFailure {
                requests: requests.clone(),
                clock: clock.clone(),
            },
            clock.clone(),
            &path,
        )
        .unwrap();
        let row = source.read(position.checked_sub(1)).unwrap().unwrap();
        assert_eq!(row.position, position);
        source.acknowledge(position).unwrap();
    }
    let times: Vec<i64> = requests.lock().unwrap().iter().map(|(_, ms)| *ms).collect();
    assert_eq!(
        times,
        [0, 20, 60],
        "restart must retain increasing failure backoff"
    );
    std::fs::remove_dir_all(path).unwrap();
}
