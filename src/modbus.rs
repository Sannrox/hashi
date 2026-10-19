//! Read-only Modbus polling. One map and scheduler serve TCP and serial RTU.
use std::collections::{BTreeMap, VecDeque};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

use crate::contract::{CheckReport, DiscoveredKind, Position, Record};
use crate::runtime::Clock;
use crate::schema::{FieldType, Schema};
use crate::{Connector, Error};

#[derive(Debug, Clone, serde::Serialize)]
pub enum Endpoint {
    Tcp(SocketAddr),
    Rtu {
        path: String,
        baud: u32,
        parity: serialport::Parity,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum Table {
    Coils,
    DiscreteInputs,
    HoldingRegisters,
    InputRegisters,
}
impl Table {
    fn function(self) -> u8 {
        match self {
            Self::Coils => 1,
            Self::DiscreteInputs => 2,
            Self::HoldingRegisters => 3,
            Self::InputRegisters => 4,
        }
    }
    fn bits(self) -> bool {
        matches!(self, Self::Coils | Self::DiscreteInputs)
    }
}
#[derive(Debug, Clone, Copy, serde::Serialize)]
pub enum ValueType {
    Bool,
    I16,
    U16,
    I32,
    U32,
    F32,
    F64,
}
impl ValueType {
    fn width(self) -> u16 {
        match self {
            Self::Bool | Self::I16 | Self::U16 => 1,
            Self::I32 | Self::U32 | Self::F32 => 2,
            Self::F64 => 4,
        }
    }
}
#[derive(Debug, Clone, Copy, serde::Serialize)]
pub enum Order {
    Big,
    Little,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct Register {
    pub name: String,
    /// Zero-based protocol address, not a 3xxxx/4xxxx display address.
    pub address: u16,
    pub value_type: ValueType,
    pub word_order: Order,
    pub byte_order: Order,
    pub scale: f64,
    pub offset: f64,
    pub unit: Option<String>,
}
#[derive(Debug, Clone, serde::Serialize)]
pub struct PollGroup {
    pub kind: String,
    pub record_key: String,
    pub schema_version: u64,
    pub table: Table,
    pub interval_ms: u64,
    pub registers: Vec<Register>,
}
#[derive(Debug, Clone, serde::Serialize)]
pub struct Device {
    pub endpoint: Endpoint,
    pub unit: u8,
    pub timeout_ms: u64,
    /// Minimum time between requests, including requests in different groups.
    pub min_request_interval_ms: u64,
    pub max_backoff_ms: u64,
    pub groups: Vec<PollGroup>,
}

/// Transport returns a checked PDU. Protocol exceptions and IO failures become
/// per-device quality records, rather than ending other devices' polling.
pub trait Transport {
    fn exchange(&mut self, device: &Device, request: &[u8]) -> Result<Vec<u8>, Error>;
}

#[derive(Default)]
pub struct NetworkTransport {
    transaction: u16,
}
impl Transport for NetworkTransport {
    fn exchange(&mut self, device: &Device, request: &[u8]) -> Result<Vec<u8>, Error> {
        if request.len() != 5
            || !(1..=4).contains(&request[0])
            || (matches!(&device.endpoint, Endpoint::Rtu { .. })
                && !(1..=247).contains(&device.unit))
            || device.timeout_ms == 0
        {
            return Err(Error::new("invalid read-only Modbus request"));
        }
        let quantity = u16::from_be_bytes([request[3], request[4]]);
        let limit = if request[0] <= 2 { 2000 } else { 125 };
        if quantity == 0
            || quantity > limit
            || u32::from(u16::from_be_bytes([request[1], request[2]])) + u32::from(quantity) > 65536
        {
            return Err(Error::new("invalid Modbus read range"));
        }
        let timeout = Duration::from_millis(device.timeout_ms);
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| Error::new("Modbus timeout too large"))?;
        match &device.endpoint {
            Endpoint::Tcp(address) => {
                let mut stream = TcpStream::connect_timeout(address, timeout)?;
                stream.set_read_timeout(Some(timeout))?;
                stream.set_write_timeout(Some(timeout))?;
                self.transaction = self.transaction.wrapping_add(1);
                let mut frame = Vec::with_capacity(request.len() + 7);
                frame.extend_from_slice(&self.transaction.to_be_bytes());
                frame.extend_from_slice(&[0, 0]);
                frame.extend_from_slice(&((request.len() + 1) as u16).to_be_bytes());
                frame.push(device.unit);
                frame.extend_from_slice(request);
                write_before(&mut stream, &frame, deadline)?;
                let mut header = [0; 7];
                read_before(&mut stream, &mut header, deadline)?;
                let len = u16::from_be_bytes([header[4], header[5]]) as usize;
                if header[..2] != self.transaction.to_be_bytes()
                    || header[2..4] != [0, 0]
                    || header[6] != device.unit
                    || !(2..=254).contains(&len)
                {
                    return Err(Error::new("invalid Modbus TCP response header"));
                }
                let mut pdu = vec![0; len - 1];
                read_before(&mut stream, &mut pdu, deadline)?;
                Ok(pdu)
            }
            Endpoint::Rtu { path, baud, parity } => {
                if *baud == 0 {
                    return Err(Error::new("RTU baud must be nonzero"));
                }
                let mut port = serialport::new(path, *baud)
                    .parity(*parity)
                    .stop_bits(if *parity == serialport::Parity::None {
                        serialport::StopBits::Two
                    } else {
                        serialport::StopBits::One
                    })
                    .timeout(timeout)
                    .open()
                    .map_err(|e| Error::new(e.to_string()))?;
                let silence_ms = if *baud > 19200 {
                    2
                } else {
                    38500u64.div_ceil(u64::from(*baud))
                };
                std::thread::sleep(Duration::from_millis(silence_ms));
                port.clear(serialport::ClearBuffer::Input)
                    .map_err(|e| Error::new(e.to_string()))?;
                rtu_exchange(port.as_mut(), device.unit, request, deadline)
            }
        }
    }
}
// Both transports need to refresh the timeout before every partial IO operation.
trait DeadlineIo: Read + Write {
    fn timeout(&mut self, remaining: Duration, writing: bool) -> Result<(), Error>;
}
impl DeadlineIo for TcpStream {
    fn timeout(&mut self, remaining: Duration, writing: bool) -> Result<(), Error> {
        if writing {
            self.set_write_timeout(Some(remaining))?;
        } else {
            self.set_read_timeout(Some(remaining))?;
        }
        Ok(())
    }
}
impl DeadlineIo for dyn serialport::SerialPort + '_ {
    fn timeout(&mut self, remaining: Duration, _: bool) -> Result<(), Error> {
        self.set_timeout(remaining)
            .map_err(|e| Error::new(e.to_string()))
    }
}
fn remaining(deadline: Instant) -> Result<Duration, Error> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|d| !d.is_zero())
        .ok_or_else(|| Error::new("Modbus exchange deadline exceeded"))
}
fn read_before<P: DeadlineIo + ?Sized>(
    port: &mut P,
    mut bytes: &mut [u8],
    deadline: Instant,
) -> Result<(), Error> {
    while !bytes.is_empty() {
        port.timeout(remaining(deadline)?, false)?;
        match port.read(bytes) {
            Ok(0) => return Err(Error::new("Modbus response ended early")),
            Ok(count) => bytes = &mut bytes[count..],
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}
fn write_before<P: DeadlineIo + ?Sized>(
    port: &mut P,
    mut bytes: &[u8],
    deadline: Instant,
) -> Result<(), Error> {
    while !bytes.is_empty() {
        port.timeout(remaining(deadline)?, true)?;
        match port.write(bytes) {
            Ok(0) => return Err(Error::new("Modbus request write stalled")),
            Ok(count) => bytes = &bytes[count..],
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

fn rtu_exchange(
    port: &mut dyn serialport::SerialPort,
    unit: u8,
    request: &[u8],
    deadline: Instant,
) -> Result<Vec<u8>, Error> {
    let mut frame = vec![unit];
    frame.extend_from_slice(request);
    frame.extend_from_slice(&crc16(&frame).to_le_bytes());
    write_before(port, &frame, deadline)?;
    let mut prefix = [0; 3];
    read_before(port, &mut prefix, deadline)?;
    let remaining = if prefix[1] & 0x80 != 0 {
        2
    } else {
        prefix[2] as usize + 2
    };
    if remaining > 252 {
        return Err(Error::new("oversized Modbus RTU response"));
    }
    let mut frame = prefix.to_vec();
    frame.resize(3 + remaining, 0);
    read_before(port, &mut frame[3..], deadline)?;
    let end = frame.len() - 2;
    if frame[0] != unit || crc16(&frame[..end]) != u16::from_le_bytes([frame[end], frame[end + 1]])
    {
        return Err(Error::new("invalid Modbus RTU response checksum or unit"));
    }
    Ok(frame[1..end].to_vec())
}
fn crc16(bytes: &[u8]) -> u16 {
    let mut crc = 0xffff;
    for byte in bytes {
        crc ^= u16::from(*byte);
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0xa001
            } else {
                crc >> 1
            };
        }
    }
    crc
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct DeviceState {
    next_request: i64,
    failures: Vec<u32>,
    retry_at: i64,
    due: Vec<i64>,
    next_group: usize,
}
pub struct ModbusConnector<T, C> {
    id: String,
    devices: Vec<Device>,
    state: Vec<DeviceState>,
    pending: VecDeque<Record>,
    state_dir: std::path::PathBuf,
    spool_header: String,
    _lease: std::fs::File,
    transport: T,
    clock: C,
}

impl<T: Transport, C: Clock> ModbusConnector<T, C> {
    pub fn new(
        id: impl Into<String>,
        devices: Vec<Device>,
        transport: T,
        clock: C,
        state_dir: impl Into<std::path::PathBuf>,
    ) -> Result<Self, Error> {
        let id = id.into();
        if id.trim().is_empty() || devices.is_empty() {
            return Err(Error::new("Modbus requires an id and devices"));
        }
        let mut kinds = std::collections::BTreeSet::new();
        for device in &devices {
            if matches!(&device.endpoint, Endpoint::Rtu { baud: 0, .. })
                || (matches!(&device.endpoint, Endpoint::Rtu { .. })
                    && !(1..=247).contains(&device.unit))
                || device.timeout_ms == 0
                || device.min_request_interval_ms == 0
                || device.max_backoff_ms < device.min_request_interval_ms
                || device.groups.is_empty()
            {
                return Err(Error::new("invalid Modbus device limits"));
            }
            for group in &device.groups {
                if group.kind.trim().is_empty()
                    || group.kind == crate::schema::CONTROL_KIND
                    || !kinds.insert(&group.kind)
                    || group.record_key.trim().is_empty()
                    || group.schema_version == 0
                    || group.interval_ms == 0
                    || group.registers.is_empty()
                {
                    return Err(Error::new("invalid or duplicate Modbus poll group"));
                }
                let mut names = std::collections::BTreeSet::new();
                for reg in &group.registers {
                    if reg.name.trim().is_empty()
                        || !names.insert(reg.name.clone())
                        || (reg.unit.is_some() && !names.insert(format!("{}_unit", reg.name)))
                        || (matches!(reg.value_type, ValueType::Bool)
                            && (reg.scale != 1.0 || reg.offset != 0.0))
                        || !reg.scale.is_finite()
                        || !reg.offset.is_finite()
                        || u32::from(reg.address) + u32::from(reg.value_type.width()) > 65536
                        || group.table.bits() != matches!(reg.value_type, ValueType::Bool)
                    {
                        return Err(Error::new("invalid Modbus register map"));
                    }
                }
            }
        }
        let state_dir = state_dir.into();
        std::fs::create_dir_all(&state_dir)?;
        let lease = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(state_dir.join("modbus.lease"))?;
        lease
            .try_lock()
            .map_err(|e| Error::new(format!("Modbus spool already owned: {e}")))?;
        let spool_header =
            serde_json::to_string(&(1, &id, &devices)).map_err(|e| Error::new(e.to_string()))?;
        let mut recovered_state = None;
        let pending = match std::fs::read_to_string(state_dir.join("modbus.pending.v1")) {
            Ok(text) => {
                let (header, body) = text
                    .split_once('\n')
                    .ok_or_else(|| Error::new("invalid Modbus spool header"))?;
                let (schedule, body) = body
                    .split_once('\n')
                    .ok_or_else(|| Error::new("invalid Modbus schedule"))?;
                if header == spool_header {
                    recovered_state = Some(
                        serde_json::from_str::<Vec<DeviceState>>(schedule)
                            .map_err(|e| Error::new(format!("invalid Modbus schedule: {e}")))?,
                    );
                }
                if !body.is_empty() && header != spool_header {
                    return Err(Error::new(
                        "drain Modbus spool before changing its connector or register maps",
                    ));
                }
                if !body.is_empty() && !body.ends_with('\n') {
                    return Err(Error::new("incomplete Modbus spool"));
                }
                let records = body
                    .lines()
                    .map(|line| {
                        crate::delivery::decode(line).and_then(|(r, key)| {
                            if key != r.idempotency_key(&id, "poll-spool") {
                                return Err(Error::new("Modbus spool identity mismatch"));
                            }
                            Ok(r)
                        })
                    })
                    .collect::<Result<VecDeque<Record>, _>>()?;
                if records
                    .iter()
                    .zip(records.iter().skip(1))
                    .any(|(a, b)| a.position >= b.position)
                {
                    return Err(Error::new("nonmonotonic Modbus spool"));
                }
                records
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => VecDeque::new(),
            Err(error) => return Err(error.into()),
        };
        let state: Vec<DeviceState> = devices
            .iter()
            .map(|d| DeviceState {
                next_request: i64::MIN,
                failures: vec![0; d.groups.len()],
                retry_at: i64::MIN,
                due: vec![i64::MIN; d.groups.len()],
                next_group: 0,
            })
            .collect();
        let state = recovered_state.unwrap_or(state);
        if state.len() != devices.len()
            || state.iter().zip(&devices).any(|(s, d)| {
                s.due.len() != d.groups.len()
                    || s.failures.len() != d.groups.len()
                    || s.next_group >= d.groups.len()
            })
        {
            return Err(Error::new("Modbus scheduling state does not match devices"));
        }
        Ok(Self {
            id,
            devices,
            state,
            pending,
            state_dir,
            spool_header,
            _lease: lease,
            transport,
            clock,
        })
    }

    /// Persist checkpoint acknowledgement without polling replacements. Stop the
    /// runtime, use its committed position here, then drop before changing maps.
    pub fn acknowledge(&mut self, committed: Position) -> Result<(), Error> {
        self.pending.retain(|r| r.position > committed);
        self.persist_pending()
    }

    fn persist_pending(&self) -> Result<(), Error> {
        let records: String = self
            .pending
            .iter()
            .map(|r| crate::delivery::encode(r, &r.idempotency_key(&self.id, "poll-spool")))
            .collect();
        let schedule = serde_json::to_string(&self.state).map_err(|e| Error::new(e.to_string()))?;
        let body = format!("{}\n{schedule}\n{records}", self.spool_header);
        crate::checkpoint::atomic_write(&self.state_dir.join("modbus.pending.v1"), &body)
    }

    fn poll(
        &mut self,
        device_index: usize,
        group_index: usize,
    ) -> Result<BTreeMap<String, serde_json::Value>, Error> {
        let device = &self.devices[device_index];
        let group = &device.groups[group_index];
        let mut regs: Vec<&Register> = group.registers.iter().collect();
        regs.sort_by_key(|r| r.address);
        let limit = if group.table.bits() { 2000 } else { 125 };
        let mut output = BTreeMap::new();
        let mut index = 0;
        while index < regs.len() {
            let start = regs[index].address;
            let mut end = u32::from(start) + u32::from(regs[index].value_type.width());
            let mut stop = index + 1;
            while stop < regs.len() {
                let next_end =
                    u32::from(regs[stop].address) + u32::from(regs[stop].value_type.width());
                // Batch adjacent/overlapping ranges, never read undeclared gaps.
                if u32::from(regs[stop].address) > end || next_end - u32::from(start) > limit {
                    break;
                }
                end = end.max(next_end);
                stop += 1;
            }
            let count = (end - u32::from(start)) as u16;
            let now = self.clock.now_ms();
            let wait = self.state[device_index].next_request.saturating_sub(now);
            if wait > 0 {
                self.clock.sleep_ms(wait as u64);
            }
            let mut request = vec![group.table.function()];
            request.extend_from_slice(&start.to_be_bytes());
            request.extend_from_slice(&count.to_be_bytes());
            // Reserve the next request slot durably before touching the device.
            self.state[device_index].next_request = deadline(
                self.clock.now_ms(),
                device
                    .timeout_ms
                    .saturating_add(device.min_request_interval_ms),
            );
            self.persist_pending()?;
            let response = self.transport.exchange(device, &request);
            self.state[device_index].next_request =
                deadline(self.clock.now_ms(), device.min_request_interval_ms);
            let response = response?;
            if response.first() == Some(&(group.table.function() | 0x80)) {
                return Err(Error::new(format!(
                    "Modbus exception {}",
                    response.get(1).copied().unwrap_or(0)
                )));
            }
            let bytes = if group.table.bits() {
                usize::from(count).div_ceil(8)
            } else {
                usize::from(count) * 2
            };
            if response.len() != bytes + 2
                || response[0] != group.table.function()
                || usize::from(response[1]) != bytes
            {
                return Err(Error::new("invalid Modbus response length or function"));
            }
            for reg in &regs[index..stop] {
                let offset = usize::from(reg.address - start);
                let value = if group.table.bits() {
                    serde_json::Value::Bool(response[2 + offset / 8] & (1 << (offset % 8)) != 0)
                } else {
                    decode(
                        reg,
                        &response[2 + offset * 2
                            ..2 + (offset + usize::from(reg.value_type.width())) * 2],
                    )?
                };
                output.insert(reg.name.clone(), value);
                if let Some(unit) = &reg.unit {
                    output.insert(
                        format!("{}_unit", reg.name),
                        serde_json::Value::String(unit.clone()),
                    );
                }
            }
            index = stop;
        }
        Ok(output)
    }
}

fn deadline(now: i64, delay: u64) -> i64 {
    now.saturating_add(i64::try_from(delay).unwrap_or(i64::MAX))
}
fn decode(reg: &Register, raw: &[u8]) -> Result<serde_json::Value, Error> {
    let mut words = raw.as_chunks::<2>().0.to_vec();
    if matches!(reg.word_order, Order::Little) {
        words.reverse();
    }
    if matches!(reg.byte_order, Order::Little) {
        for w in &mut words {
            w.swap(0, 1);
        }
    }
    let mut bytes = [0; 8];
    for (i, word) in words.iter().enumerate() {
        bytes[i * 2..i * 2 + 2].copy_from_slice(word);
    }
    let number = match reg.value_type {
        ValueType::I16 => f64::from(i16::from_be_bytes([bytes[0], bytes[1]])),
        ValueType::U16 => f64::from(u16::from_be_bytes([bytes[0], bytes[1]])),
        ValueType::I32 => f64::from(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])),
        ValueType::U32 => f64::from(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])),
        ValueType::F32 => f64::from(f32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])),
        ValueType::F64 => f64::from_be_bytes(bytes),
        ValueType::Bool => return Err(Error::new("boolean requires a bit table")),
    };
    serde_json::Number::from_f64(number * reg.scale + reg.offset)
        .map(serde_json::Value::Number)
        .ok_or_else(|| Error::new("non-finite Modbus value"))
}

impl<T: Transport, C: Clock> Connector for ModbusConnector<T, C> {
    fn id(&self) -> &str {
        &self.id
    }
    /// Reachability is reflected per record; construction validates the map.
    fn check(&self) -> Result<CheckReport, Error> {
        Ok(CheckReport {
            ok: true,
            message: "register maps validated; device failures appear as quality".into(),
        })
    }
    fn discover(&self) -> Result<Vec<DiscoveredKind>, Error> {
        Ok(self
            .devices
            .iter()
            .flat_map(|d| &d.groups)
            .map(|g| DiscoveredKind {
                kind: g.kind.clone(),
                metadata: Default::default(),
                schema: Schema {
                    version: g.schema_version,
                    key_fields: vec![],
                    fields: g
                        .registers
                        .iter()
                        .flat_map(|r| {
                            let mut fields = vec![(
                                r.name.clone(),
                                if matches!(r.value_type, ValueType::Bool) {
                                    FieldType::Bool
                                } else {
                                    FieldType::F64
                                },
                            )];
                            if r.unit.is_some() {
                                fields.push((format!("{}_unit", r.name), FieldType::Text));
                            }
                            fields
                        })
                        .collect(),
                },
            })
            .collect())
    }
    fn read(
        &mut self,
        from: Option<Position>,
        _source_cursor: Option<&str>,
    ) -> Result<Option<Record>, Error> {
        // Keep observations until their source checkpoint acknowledges them.
        // Atomic spool replacement makes a failed delivery replay the same sample after restart.
        if let Some(position) = from {
            self.pending.retain(|r| r.position > position);
        }
        while self.pending.is_empty() {
            for di in 0..self.devices.len() {
                if self.clock.now_ms() < self.state[di].retry_at {
                    continue;
                }
                let count = self.devices[di].groups.len();
                let first = self.state[di].next_group;
                for offset in 0..count {
                    let gi = (first + offset) % count;
                    let now = self.clock.now_ms();
                    if now < self.state[di].due[gi] {
                        continue;
                    }
                    // Reserve failure/backoff before IO so a crash cannot bypass device protection.
                    self.state[di].failures[gi] = self.state[di].failures[gi].saturating_add(1);
                    let reserved_backoff = self.devices[di]
                        .min_request_interval_ms
                        .saturating_mul(1u64 << self.state[di].failures[gi].min(16))
                        .min(self.devices[di].max_backoff_ms);
                    self.state[di].retry_at = deadline(
                        now,
                        self.devices[di].timeout_ms.saturating_add(reserved_backoff),
                    );
                    self.state[di].due[gi] = deadline(
                        now,
                        self.devices[di]
                            .timeout_ms
                            .saturating_add(self.devices[di].groups[gi].interval_ms),
                    );
                    self.persist_pending()?;
                    let result = self.poll(di, gi);
                    let group = &self.devices[di].groups[gi];
                    self.state[di].due[gi] = deadline(self.clock.now_ms(), group.interval_ms);
                    let (payload, quality) = match result {
                        Ok(values) => {
                            self.state[di].failures[gi] = 0;
                            self.state[di].retry_at = self.clock.now_ms();
                            (
                                serde_json::to_string(&values)
                                    .map_err(|e| Error::new(e.to_string()))?,
                                None,
                            )
                        }
                        Err(error) => {
                            let backoff = self.devices[di]
                                .min_request_interval_ms
                                .saturating_mul(1u64 << self.state[di].failures[gi].min(16))
                                .min(self.devices[di].max_backoff_ms);
                            self.state[di].retry_at = deadline(self.clock.now_ms(), backoff);
                            ("{}".into(), Some(format!("bad: {}", error.message)))
                        }
                    };
                    let previous = self.pending.back().map(|r| r.position).or(from);
                    let position = match previous {
                        None => 0,
                        Some(p) => p
                            .checked_add(1)
                            .ok_or_else(|| Error::new("Modbus position exhausted"))?,
                    };
                    self.pending.push_back(Record {
                        source: None,
                        kind: group.kind.clone(),
                        key: group.record_key.clone(),
                        source_ts_ms: self.clock.now_ms(),
                        ingest_ts_ms: 0,
                        quality,
                        position,
                        payload,
                    });
                    if self.state[di].failures[gi] > 0 {
                        self.state[di].next_group = (gi + 1) % count;
                        break;
                    }
                }
            }
            if self.pending.is_empty() {
                let next_due = self
                    .state
                    .iter()
                    .flat_map(|state| state.due.iter().map(|due| (*due).max(state.retry_at)))
                    .min()
                    .ok_or_else(|| Error::new("Modbus has no poll groups"))?;
                let wait = next_due.saturating_sub(self.clock.now_ms()).max(1);
                self.clock.sleep_ms(wait as u64);
            }
        }
        self.persist_pending()?;
        Ok(self.pending.front().cloned())
    }
}

#[cfg(all(test, unix))]
#[test]
fn rtu_framing_checks_crc_and_rejects_corruption() {
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
        let (mut master, mut slave) = serialport::TTYPort::pair().unwrap();
        master
            .set_timeout(std::time::Duration::from_secs(2))
            .unwrap();
        slave
            .set_timeout(std::time::Duration::from_secs(2))
            .unwrap();
        let server = std::thread::spawn(move || {
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
        let result = rtu_exchange(
            &mut slave,
            1,
            &[3, 0, 0, 0, 1],
            Instant::now() + Duration::from_secs(2),
        );
        if corrupt {
            assert!(result.unwrap_err().message.contains("checksum"));
        } else {
            assert_eq!(result.unwrap(), [3, 2, 0, 42]);
        }
        server.join().unwrap();
    }
}
