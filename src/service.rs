//! `hashi run`: every configured instance driven through the runtime.
//!
//! Each pass runs every instance until its source is caught up. An
//! instance's failure is reported and does not stop the others, so a
//! credential that is missing on one pass is picked up on the next once the
//! file is rotated, and an instance whose directories cannot open yet is
//! retried. Checkpoints live under `checkpoint_dir/<instance>`, so a
//! restarted process resumes where the last one committed.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crate::buffer::{BufferStats, StoreAndForward};
use crate::config::{
    Config, HistorianAdapterConfig, InstanceConfig, KafkaSinkConfig, ModbusEndpointConfig,
    MqttSourceConfig, OpcUaSourceConfig, ParquetSinkConfig, SinkConfig, SourceConfig,
};
use crate::contract::{CheckReport, DiscoveredKind, IdempotencyKey, Position, Record};
use crate::credential::FileCredential;
use crate::fake::{FakeSource, record};
use crate::lease::{self, DropRelease, SharedLeaseStore};
use crate::runtime::Clock;
use crate::sink::{DeliverError, Sink};
use crate::{
    Connector, Error, FileCheckpoint, JsonLinesSink, KafkaCompatibleSink, RunReport, Runtime,
    RuntimeConfig, SystemClock,
};

type RunRuntime = Runtime<Box<dyn Connector>, InstanceSink, FileCheckpoint, SystemClock>;

/// An instance's sink, written directly or through its store-and-forward
/// buffer.
struct Locked<S> {
    sink: S,
    lock: DropRelease,
}

impl<S: Sink> Sink for Locked<S> {
    fn admit(&mut self, token: u64) -> Result<(), DeliverError> {
        self.lock.renew().map_err(dir_lock_err)?;
        self.sink.admit(token)
    }

    fn recover(&mut self) -> Result<(), DeliverError> {
        self.lock.renew().map_err(dir_lock_err)?;
        self.sink.recover()
    }

    fn deliver(&mut self, record: &Record, idk: &IdempotencyKey) -> Result<(), DeliverError> {
        self.lock.renew().map_err(dir_lock_err)?;
        self.sink.deliver(record, idk)
    }
}

fn dir_lock_err(error: Error) -> DeliverError {
    if lease::is_fence_lost(&error)
        || error.message.contains("already running")
        || error.message.contains("in use by another instance")
    {
        DeliverError::Blocked(error.message)
    } else {
        DeliverError::Transient(error.message)
    }
}

enum InstanceSink {
    Direct(Box<Locked<Box<dyn Sink>>>),
    Buffered(Box<Locked<StoreAndForward<LazySink>>>),
}

/// The sink behind a buffer, opened (and its directory locked) on first
/// use. A sink that cannot open yet, such as an unmounted destination, is a
/// transient error, so records keep buffering across a restart during an
/// outage.
struct LazySink {
    spec: SinkConfig,
    ttl: Duration,
    token: u64,
    open: Option<(Box<dyn Sink>, DropRelease)>,
}

impl LazySink {
    fn sink(&mut self) -> Result<&mut dyn Sink, DeliverError> {
        if self.open.is_none() {
            let dir = self.spec.dir();
            let lock = lock_sink_dir(dir, self.ttl)
                .map_err(|error| DeliverError::Transient(error.message))?;
            let mut sink = open_file_sink(&self.spec)
                .map_err(|error| DeliverError::Transient(error.message))?;
            if self.token > 0 {
                sink.admit(self.token)?;
            }
            self.open = Some((sink, lock));
        }
        let open = self.open.as_mut().expect("opened above");
        open.1.renew().map_err(dir_lock_err)?;
        Ok(&mut *open.0)
    }
}

impl Sink for LazySink {
    fn admit(&mut self, token: u64) -> Result<(), DeliverError> {
        self.token = token;
        if self.open.is_some() {
            self.sink()?.admit(token)?;
        }
        Ok(())
    }

    fn recover(&mut self) -> Result<(), DeliverError> {
        self.sink()?.recover()
    }

    fn deliver(&mut self, record: &Record, idk: &IdempotencyKey) -> Result<(), DeliverError> {
        self.sink()?.deliver(record, idk)
    }
}

impl Sink for InstanceSink {
    fn admit(&mut self, token: u64) -> Result<(), DeliverError> {
        match self {
            Self::Direct(sink) => sink.admit(token),
            Self::Buffered(sink) => sink.admit(token),
        }
    }

    fn recover(&mut self) -> Result<(), DeliverError> {
        match self {
            Self::Direct(sink) => sink.recover(),
            Self::Buffered(sink) => sink.recover(),
        }
    }

    fn deliver(&mut self, record: &Record, idk: &IdempotencyKey) -> Result<(), DeliverError> {
        match self {
            Self::Direct(sink) => sink.deliver(record, idk),
            Self::Buffered(sink) => sink.deliver(record, idk),
        }
    }
}

/// A running instance and the locks that fence its sink and buffer
/// directories. Those locks release on drop so a clean stop frees the path.
struct Running {
    runtime: RunRuntime,
    takeovers: u64,
    acquired_ms: i64,
}

/// A pass stops after this even when a live subscription still has traffic, so
/// later instances run and `--once` can exit. Snapshot sources that return
/// `None` finish earlier.
const PASS_BUDGET: Duration = Duration::from_secs(1);

impl Running {
    /// Run the instance. A pass that fails, for example because the source
    /// is unavailable, still drains the buffer: those records were already
    /// acknowledged and checkpointed, so their delivery must not wait on the
    /// source.
    fn run(&mut self) -> Result<RunReport, Error> {
        let mut started = None;
        let outcome = self.runtime.run_while(|_| match started {
            None => {
                started = Some(Instant::now());
                true
            }
            Some(started) => started.elapsed() < PASS_BUDGET,
        });
        if outcome.is_err()
            && let InstanceSink::Buffered(buffer) = self.runtime.sink_mut()
            && let Err(error @ (DeliverError::Blocked(_) | DeliverError::Poison(_))) =
                buffer.recover()
        {
            eprintln!("buffer drain stopped: {error:?}");
        }
        outcome
    }

    fn buffer(&self) -> Option<BufferStats> {
        match self.runtime.sink() {
            InstanceSink::Direct(_) => None,
            InstanceSink::Buffered(locked) => locked
                .sink
                .stats()
                .inspect_err(|error| eprintln!("buffer stats unreadable: {error}"))
                .ok(),
        }
    }
}

/// One instance's pass. `progress` counts what was committed even when `outcome` is an error.
pub struct Pass {
    pub instance: String,
    pub outcome: Result<RunReport, Error>,
    pub progress: RunReport,
    /// The buffer's fill level and counters after the pass, when it has one.
    pub buffer: Option<BufferStats>,
    /// 0 while waiting for the lease, 1 while this process holds it.
    pub role: u8,
    pub fencing_token: u64,
    pub takeovers: u64,
    pub lease_acquired_ms: Option<i64>,
}

pub struct Service {
    checkpoint_dir: PathBuf,
    leases: SharedLeaseStore,
    ttl: Duration,
    instances: Vec<(InstanceConfig, Option<Running>)>,
}

impl Service {
    /// Instances open lazily on their first pass, so one unavailable
    /// directory does not stop the others; it is retried on later passes.
    pub fn from_config(config: &Config) -> Result<Self, Error> {
        if config.instances.is_empty() {
            return Err(Error::new(
                "invalid config: instance: hashi run needs at least one instance",
            ));
        }
        Ok(Self {
            checkpoint_dir: config.checkpoint_dir.clone(),
            leases: lease::open_store(&config.lease)?,
            ttl: config.lease.ttl(),
            instances: config
                .instances
                .iter()
                .map(|instance| (instance.clone(), None))
                .collect(),
        })
    }

    /// Run every instance until caught up, in config order.
    pub fn pass(&mut self) -> Vec<(String, Result<RunReport, Error>)> {
        self.pass_with_progress()
            .into_iter()
            .map(|pass| (pass.instance, pass.outcome))
            .collect()
    }

    /// Like [`Service::pass`], plus what each instance committed before an error.
    pub fn pass_with_progress(&mut self) -> Vec<Pass> {
        let checkpoint_dir = &self.checkpoint_dir;
        let leases = &self.leases;
        let ttl = self.ttl;
        self.instances
            .iter_mut()
            .map(|(instance, runtime)| match runtime {
                Some(running) => {
                    let outcome = running.run();
                    let pass = finish_pass(&instance.name, running, outcome);
                    if pass.outcome.is_err() && pass.role != 0 {
                        *runtime = None;
                    }
                    pass
                }
                None => match open(checkpoint_dir, leases.clone(), ttl, instance) {
                    Ok(opened) => {
                        let running = runtime.insert(opened);
                        let outcome = running.run();
                        let pass = finish_pass(&instance.name, running, outcome);
                        if pass.outcome.is_err() && pass.role != 0 {
                            *runtime = None;
                        }
                        pass
                    }
                    Err(error) => {
                        let standby = is_standby(&error);
                        Pass {
                            instance: instance.name.clone(),
                            outcome: Err(error),
                            progress: RunReport::default(),
                            buffer: None,
                            role: u8::from(!standby),
                            fencing_token: 0,
                            takeovers: 0,
                            lease_acquired_ms: None,
                        }
                    }
                },
            })
            .collect()
    }
}

fn is_standby(error: &Error) -> bool {
    error.message.contains("already running")
        || error.message.contains("stale writer")
        || error.message.contains("in use by another instance")
}

fn finish_pass(name: &str, running: &mut Running, outcome: Result<RunReport, Error>) -> Pass {
    let token = running.runtime.fencing_token().unwrap_or(0);
    let standby = outcome.as_ref().is_err_and(is_standby);
    if !standby && token > 1 && running.takeovers == 0 {
        running.takeovers = 1;
    }
    Pass {
        instance: name.to_string(),
        outcome,
        progress: running.runtime.progress().clone(),
        buffer: running.buffer(),
        role: u8::from(!standby),
        fencing_token: token,
        takeovers: running.takeovers,
        lease_acquired_ms: Some(running.acquired_ms),
    }
}

/// Checkpoint leases fence one instance, not a sink directory that another
/// instance or process reaches by a different path; this lock does, and
/// releases on a clean stop.
fn lock_sink_dir(dir: &Path, ttl: Duration) -> Result<DropRelease, Error> {
    lease::lock_dir(dir, "sink", "sink", ttl)
}

/// The connector id an instance checkpoints under.
pub(crate) fn connector_id(instance: &InstanceConfig) -> String {
    format!("{}-{}", instance.source.family(), instance.name)
}

/// Open the configured source. `run` subscribes OPC UA; discover connects without a subscription.
pub(crate) fn open_connector(
    instance: &InstanceConfig,
    run: bool,
) -> Result<Box<dyn Connector>, Error> {
    let id = connector_id(instance);
    match &instance.source {
        SourceConfig::Fixture(fixture) => Ok(Box::new(open_fixture(&id, fixture))),
        SourceConfig::OpcUa(opcua) => open_opcua(&id, opcua, run),
        SourceConfig::Mqtt(mqtt) => open_mqtt(&id, mqtt),
        SourceConfig::Modbus(modbus) => open_modbus(&id, modbus),
        SourceConfig::Mysql(mysql) => open_mysql(&id, mysql),
        SourceConfig::Mssql(mssql) => open_mssql(&id, mssql),
        SourceConfig::Postgres(postgres) => open_postgres(&id, postgres),
        SourceConfig::Historian(historian) => open_historian(&id, historian),
    }
}

fn open(
    checkpoint_dir: &Path,
    leases: SharedLeaseStore,
    ttl: Duration,
    instance: &InstanceConfig,
) -> Result<Running, Error> {
    require_compiled_sink(&instance.sink)?;
    let sink = match &instance.buffer {
        None => {
            let lock = lock_sink_dir(instance.sink.dir(), ttl)?;
            InstanceSink::Direct(Box::new(Locked {
                sink: open_file_sink(&instance.sink)?,
                lock,
            }))
        }
        Some(buffer) => {
            // Two processes draining one buffer would deliver twice.
            InstanceSink::Buffered(Box::new(Locked {
                sink: StoreAndForward::with_limits(
                    &buffer.dir,
                    buffer.limits(),
                    LazySink {
                        spec: instance.sink.clone(),
                        ttl,
                        token: 0,
                        open: None,
                    },
                )?,
                lock: lease::lock_dir(&buffer.dir, "buffer", "buffer", ttl)?,
            }))
        }
    };
    let source = match open_connector(instance, true) {
        Ok(source) => source,
        Err(error) if instance.buffer.is_some() => Box::new(UnavailableSource {
            id: connector_id(instance),
            message: error.message,
        }),
        Err(error) => return Err(error),
    };
    Ok(Running {
        runtime: Runtime::new(
            source,
            sink,
            FileCheckpoint::with_store(checkpoint_dir.join(&instance.name), leases, ttl)?,
            SystemClock,
            RuntimeConfig::new(&instance.name),
        ),
        takeovers: 0,
        acquired_ms: SystemClock.now_ms(),
    })
}

fn open_file_sink(sink: &SinkConfig) -> Result<Box<dyn Sink>, Error> {
    match sink {
        SinkConfig::Jsonl(jsonl) => Ok(Box::new(JsonLinesSink::open(&jsonl.dir)?)),
        SinkConfig::Parquet(ParquetSinkConfig { dir }) => {
            #[cfg(feature = "parquet")]
            {
                Ok(Box::new(crate::ParquetSink::open(dir)?))
            }
            #[cfg(not(feature = "parquet"))]
            {
                let _ = dir;
                Err(needs_sink_feature("parquet", "parquet"))
            }
        }
        SinkConfig::Kafka(KafkaSinkConfig { dir, topic }) => {
            Ok(Box::new(KafkaCompatibleSink::open(dir, topic)?))
        }
    }
}

/// Fails check so `Runtime::run` admits the fencing token, then the pass can
/// drain an existing buffer while the real source is down.
struct UnavailableSource {
    id: String,
    message: String,
}

impl Connector for UnavailableSource {
    fn id(&self) -> &str {
        &self.id
    }

    fn check(&self) -> Result<CheckReport, Error> {
        Ok(CheckReport {
            ok: false,
            message: self.message.clone(),
        })
    }

    fn discover(&self) -> Result<Vec<DiscoveredKind>, Error> {
        Ok(Vec::new())
    }

    fn read(&mut self, _: Option<Position>, _: Option<&str>) -> Result<Option<Record>, Error> {
        Ok(None)
    }
}

fn open_fixture(id: &str, fixture: &crate::config::FixtureSourceConfig) -> FakeSource {
    let records = (1..=fixture.records)
        .map(|position| {
            record(
                "reading",
                &format!("r{position}"),
                position,
                &format!("value-{position}"),
            )
        })
        .collect();
    FakeSource::with_live_credential(
        id,
        records,
        FileCredential::new(&fixture.credential_file),
        fixture.accept_sha256.clone(),
    )
}

#[allow(dead_code)]
fn needs_feature(family: &str, feature: &str) -> Error {
    Error::new(format!(
        "hashi run needs the {feature} feature to start source kind {family}"
    ))
}

#[allow(dead_code)]
fn needs_sink_feature(family: &str, feature: &str) -> Error {
    Error::new(format!(
        "hashi run needs the {feature} feature to start sink kind {family}"
    ))
}

fn require_compiled_sink(sink: &SinkConfig) -> Result<(), Error> {
    match sink {
        SinkConfig::Parquet(_) => {
            #[cfg(feature = "parquet")]
            {
                Ok(())
            }
            #[cfg(not(feature = "parquet"))]
            {
                Err(needs_sink_feature("parquet", "parquet"))
            }
        }
        SinkConfig::Jsonl(_) | SinkConfig::Kafka(_) => Ok(()),
    }
}

fn open_opcua(id: &str, opcua: &OpcUaSourceConfig, run: bool) -> Result<Box<dyn Connector>, Error> {
    #[cfg(feature = "opcua")]
    {
        let config = opcua.connector_config(id)?;
        if !run {
            return Ok(Box::new(crate::opcua::OpcUaConnector::connect(config)?));
        }
        if opcua.nodes.is_empty() {
            return Err(Error::new(
                "invalid config: source.opcua.nodes: required to run",
            ));
        }
        let subscription = opcua.subscription_config()?;
        Ok(Box::new(crate::opcua::OpcUaConnector::subscribe(
            config,
            subscription,
        )?))
    }
    #[cfg(not(feature = "opcua"))]
    {
        let _ = (id, opcua, run);
        Err(needs_feature("opcua", "opcua"))
    }
}

fn open_mqtt(id: &str, mqtt: &MqttSourceConfig) -> Result<Box<dyn Connector>, Error> {
    #[cfg(feature = "mqtt")]
    {
        Ok(Box::new(crate::mqtt::MqttConnector::connect(
            mqtt.connector_config(id)?,
        )?))
    }
    #[cfg(not(feature = "mqtt"))]
    {
        let _ = (id, mqtt);
        Err(needs_feature("mqtt", "mqtt"))
    }
}

#[cfg(feature = "modbus")]
fn modbus_tcp_addr(host: &str, port: u16) -> Result<std::net::SocketAddr, Error> {
    use std::net::ToSocketAddrs;
    (host, port)
        .to_socket_addrs()
        .ok()
        .and_then(|mut addrs| addrs.next())
        .ok_or_else(|| Error::new("invalid Modbus TCP address"))
}

fn open_modbus(
    id: &str,
    modbus: &crate::config::ModbusSourceConfig,
) -> Result<Box<dyn Connector>, Error> {
    #[cfg(feature = "modbus")]
    {
        use crate::modbus::{
            Device, Endpoint, ModbusConnector, NetworkTransport, Order, Parity, PollGroup,
            Register, Table, ValueType,
        };
        let parse_order = |value: &str| match value {
            "big" => Ok(Order::Big),
            "little" => Ok(Order::Little),
            _ => Err(Error::new("invalid Modbus byte or word order")),
        };
        let devices = modbus
            .devices
            .iter()
            .map(|device| {
                let endpoint = match &device.endpoint {
                    ModbusEndpointConfig::Tcp { host, port } => {
                        Endpoint::Tcp(modbus_tcp_addr(host, *port)?)
                    }
                    ModbusEndpointConfig::Rtu { path, baud, parity } => Endpoint::Rtu {
                        path: path.clone(),
                        baud: *baud,
                        parity: match parity.as_str() {
                            "none" => Parity::None,
                            "even" => Parity::Even,
                            "odd" => Parity::Odd,
                            _ => return Err(Error::new("invalid Modbus RTU parity")),
                        },
                    },
                };
                let groups = device
                    .groups
                    .iter()
                    .map(|group| {
                        Ok(PollGroup {
                            kind: group.kind.clone(),
                            record_key: group.record_key.clone(),
                            schema_version: group.schema_version,
                            table: match group.table.as_str() {
                                "coils" => Table::Coils,
                                "discrete_inputs" => Table::DiscreteInputs,
                                "holding_registers" => Table::HoldingRegisters,
                                "input_registers" => Table::InputRegisters,
                                _ => return Err(Error::new("invalid Modbus table")),
                            },
                            interval_ms: group.interval_ms,
                            registers: group
                                .registers
                                .iter()
                                .map(|register| {
                                    Ok(Register {
                                        name: register.name.clone(),
                                        address: register.address,
                                        value_type: match register.value_type.as_str() {
                                            "bool" => ValueType::Bool,
                                            "i16" => ValueType::I16,
                                            "u16" => ValueType::U16,
                                            "i32" => ValueType::I32,
                                            "u32" => ValueType::U32,
                                            "f32" => ValueType::F32,
                                            "f64" => ValueType::F64,
                                            _ => {
                                                return Err(Error::new(
                                                    "invalid Modbus value_type",
                                                ));
                                            }
                                        },
                                        word_order: parse_order(&register.word_order)?,
                                        byte_order: parse_order(&register.byte_order)?,
                                        scale: register.scale,
                                        offset: register.offset,
                                        unit: register.unit.clone(),
                                    })
                                })
                                .collect::<Result<Vec<_>, _>>()?,
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Device {
                    endpoint,
                    unit: device.unit,
                    timeout_ms: device.timeout_ms,
                    min_request_interval_ms: device.min_request_interval_ms,
                    max_backoff_ms: device.max_backoff_ms,
                    groups,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Box::new(ModbusConnector::new(
            id,
            devices,
            NetworkTransport::default(),
            SystemClock,
            &modbus.state_dir,
        )?))
    }
    #[cfg(not(feature = "modbus"))]
    {
        let _ = (id, modbus);
        Err(needs_feature("modbus", "modbus"))
    }
}

fn open_mysql(
    id: &str,
    mysql: &crate::config::MysqlSourceConfig,
) -> Result<Box<dyn Connector>, Error> {
    #[cfg(feature = "mysql")]
    {
        Ok(Box::new(crate::mysql::MySqlConnector::connect(
            mysql_connector_config(id, mysql)?,
        )?))
    }
    #[cfg(not(feature = "mysql"))]
    {
        let _ = (id, mysql);
        Err(needs_feature("mysql", "mysql"))
    }
}

fn open_mssql(
    id: &str,
    mssql: &crate::config::MssqlSourceConfig,
) -> Result<Box<dyn Connector>, Error> {
    #[cfg(feature = "mssql")]
    {
        let password = FileCredential::new(&mssql.credential_file)
            .current()?
            .expose()
            .to_string();
        Ok(Box::new(crate::mssql::MssqlConnector::connect(
            crate::mssql::Config {
                id: id.to_string(),
                host: mssql.host.clone(),
                port: mssql.port,
                user: mssql.user.clone(),
                password,
                database: mssql.database.clone(),
                tables: mssql
                    .tables
                    .iter()
                    .map(|table| crate::mssql::Table {
                        schema: table.schema.clone(),
                        table: table.table.clone(),
                        key: table.key.clone(),
                    })
                    .collect(),
                state_dir: mssql.state_dir.clone(),
                chunk_rows: usize::try_from(mssql.chunk_rows).unwrap_or(usize::MAX),
                max_batch_bytes: usize::try_from(mssql.max_batch_bytes).unwrap_or(usize::MAX),
                schema_version: mssql.schema_version,
                timeout: Duration::from_millis(mssql.timeout_ms),
                max_lag: Duration::from_millis(mssql.max_lag_ms),
                allow_insecure_lab: mssql.allow_insecure_lab,
            },
        )?))
    }
    #[cfg(not(feature = "mssql"))]
    {
        let _ = (id, mssql);
        Err(needs_feature("mssql", "mssql"))
    }
}

fn open_postgres(
    id: &str,
    postgres: &crate::config::PostgresSourceConfig,
) -> Result<Box<dyn Connector>, Error> {
    #[cfg(feature = "postgres")]
    {
        let password = FileCredential::new(&postgres.credential_file)
            .current()?
            .expose()
            .to_string();
        Ok(Box::new(crate::postgres::PostgresConnector::connect(
            crate::postgres::Config {
                id: id.to_string(),
                host: postgres.host.clone(),
                port: postgres.port,
                user: postgres.user.clone(),
                password,
                database: postgres.database.clone(),
                tables: postgres
                    .tables
                    .iter()
                    .map(|table| crate::postgres::Table {
                        schema: table.schema.clone(),
                        table: table.table.clone(),
                        key: table.key.clone(),
                    })
                    .collect(),
                state_dir: postgres.state_dir.clone(),
                chunk_rows: usize::try_from(postgres.chunk_rows).unwrap_or(usize::MAX),
                max_batch_bytes: usize::try_from(postgres.max_batch_bytes).unwrap_or(usize::MAX),
                schema_version: postgres.schema_version,
                timeout: Duration::from_millis(postgres.timeout_ms),
                max_lag: Duration::from_millis(postgres.max_lag_ms),
                allow_insecure_lab: postgres.allow_insecure_lab,
            },
        )?))
    }
    #[cfg(not(feature = "postgres"))]
    {
        let _ = (id, postgres);
        Err(needs_feature("postgres", "postgres"))
    }
}

fn open_historian(
    id: &str,
    historian: &crate::config::HistorianSourceConfig,
) -> Result<Box<dyn Connector>, Error> {
    let config = crate::historian::HistorianConfig {
        start_ms: historian.start_ms,
        chunk_ms: historian.chunk_ms,
        page_limit: usize::try_from(historian.page_limit).unwrap_or(usize::MAX),
        settle_ms: historian.settle_ms,
        max_reads_per_second: historian.max_reads_per_second,
        state_dir: historian.state_dir.clone(),
    };
    let _ = (id, &config);
    match &historian.adapter {
        HistorianAdapterConfig::OpcUa(opcua) => {
            #[cfg(feature = "opcua")]
            {
                let adapter =
                    crate::opcua::historian::OpcUaHistorian::connect(opcua.connector_config(id)?)?;
                Ok(Box::new(crate::historian::HistorianConnector::new(
                    id,
                    adapter,
                    SystemClock,
                    config,
                )?))
            }
            #[cfg(not(feature = "opcua"))]
            {
                let _ = opcua;
                Err(needs_feature("historian", "opcua"))
            }
        }
        HistorianAdapterConfig::Mysql(mysql) => {
            #[cfg(feature = "mysql")]
            {
                let password = FileCredential::new(&mysql.credential_file)
                    .current()?
                    .expose()
                    .to_string();
                let adapter = crate::mysql::historian::SqlHistorian::connect(
                    crate::mysql::historian::Config {
                        options: mysql_opts(&mysql.url, &password, mysql.allow_insecure_lab)?,
                        mapping: crate::mysql::historian::Mapping {
                            database: mysql.mapping_database.clone(),
                            tag_table: mysql.tag_table.clone(),
                            tag_id_column: mysql.tag_id_column.clone(),
                            tag_name_column: mysql.tag_name_column.clone(),
                            value_table: mysql.value_table.clone(),
                            value_tag_column: mysql.value_tag_column.clone(),
                            time_column: mysql.time_column.clone(),
                            value_column: mysql.value_column.clone(),
                            quality_column: mysql.quality_column.clone(),
                        },
                        timeout: Duration::from_millis(mysql.timeout_ms),
                        allow_insecure_lab: mysql.allow_insecure_lab,
                    },
                )?;
                Ok(Box::new(crate::historian::HistorianConnector::new(
                    id,
                    adapter,
                    SystemClock,
                    config,
                )?))
            }
            #[cfg(not(feature = "mysql"))]
            {
                let _ = mysql;
                Err(needs_feature("historian", "mysql"))
            }
        }
        HistorianAdapterConfig::Mssql(mssql) => {
            #[cfg(feature = "mssql")]
            {
                let password = FileCredential::new(&mssql.credential_file)
                    .current()?
                    .expose()
                    .to_string();
                let adapter = crate::mssql::historian::SqlHistorian::connect(
                    crate::mssql::historian::Config {
                        host: mssql.host.clone(),
                        port: mssql.port,
                        user: mssql.user.clone(),
                        password,
                        database: mssql.database.clone(),
                        mapping: crate::mssql::historian::Mapping {
                            database: mssql.mapping_database.clone(),
                            tag_table: mssql.tag_table.clone(),
                            tag_id_column: mssql.tag_id_column.clone(),
                            tag_name_column: mssql.tag_name_column.clone(),
                            value_table: mssql.value_table.clone(),
                            value_tag_column: mssql.value_tag_column.clone(),
                            time_column: mssql.time_column.clone(),
                            value_column: mssql.value_column.clone(),
                            quality_column: mssql.quality_column.clone(),
                        },
                        timeout: Duration::from_millis(mssql.timeout_ms),
                        allow_insecure_lab: mssql.allow_insecure_lab,
                    },
                )?;
                Ok(Box::new(crate::historian::HistorianConnector::new(
                    id,
                    adapter,
                    SystemClock,
                    config,
                )?))
            }
            #[cfg(not(feature = "mssql"))]
            {
                let _ = mssql;
                Err(needs_feature("historian", "mssql"))
            }
        }
    }
}

#[cfg(feature = "mysql")]
fn mysql_opts(
    url: &str,
    password: &str,
    allow_insecure_lab: bool,
) -> Result<mysql_async::Opts, Error> {
    let mut builder = mysql_async::OptsBuilder::from_opts(
        mysql_async::Opts::from_url(url)
            .map_err(|_| Error::new("source.mysql.url is not a MySQL URL"))?,
    )
    .pass(Some(password));
    if !allow_insecure_lab {
        builder = builder.ssl_opts(Some(mysql_async::SslOpts::default()));
    }
    Ok(builder.into())
}

#[cfg(feature = "mysql")]
fn mysql_connector_config(
    id: &str,
    mysql: &crate::config::MysqlSourceConfig,
) -> Result<crate::mysql::Config, Error> {
    let password = FileCredential::new(&mysql.credential_file)
        .current()?
        .expose()
        .to_string();
    Ok(crate::mysql::Config {
        id: id.to_string(),
        options: mysql_opts(&mysql.url, &password, mysql.allow_insecure_lab)?,
        tables: mysql
            .tables
            .iter()
            .map(|table| crate::mysql::Table {
                database: table.database.clone(),
                table: table.table.clone(),
                key: table.key.clone(),
            })
            .collect(),
        state_dir: mysql.state_dir.clone(),
        chunk_rows: usize::try_from(mysql.chunk_rows).unwrap_or(usize::MAX),
        max_batch_bytes: usize::try_from(mysql.max_batch_bytes).unwrap_or(usize::MAX),
        server_id: mysql.server_id,
        schema_version: mysql.schema_version,
        timeout: Duration::from_millis(mysql.timeout_ms),
        allow_insecure_lab: mysql.allow_insecure_lab,
    })
}

#[cfg(feature = "mqtt")]
impl MqttSourceConfig {
    fn connector_config(&self, id: &str) -> Result<crate::mqtt::Config, Error> {
        let user_name = match &self.credential_file {
            Some(path) => {
                let (user, password) = FileCredential::new(path).user_password()?;
                Some((user.expose().to_string(), password.expose().to_string()))
            }
            None => None,
        };
        let transport = match &self.ca_file {
            Some(ca_file) => crate::mqtt::Transport::Tls {
                ca_file: ca_file.clone(),
                client_certificate: match (&self.client_certificate, &self.client_private_key) {
                    (Some(cert), Some(key)) => Some((cert.clone(), key.clone())),
                    _ => None,
                },
            },
            None => crate::mqtt::Transport::Plain,
        };
        let routes = self
            .routes
            .iter()
            .map(|route| {
                Ok(crate::mqtt::Route {
                    filter: route.filter.clone(),
                    kind: route.kind.clone(),
                    decoder: match route.decoder.as_str() {
                        "json" => crate::mqtt::Decoder::Json,
                        "raw" => crate::mqtt::Decoder::Raw,
                        "sparkplug_b" => crate::mqtt::Decoder::SparkplugB,
                        _ => return Err(Error::new("invalid MQTT decoder")),
                    },
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(crate::mqtt::Config {
            id: id.to_string(),
            host: self.host.clone(),
            port: self.port,
            protocol: match self.protocol.as_str() {
                "v311" => crate::mqtt::Protocol::V311,
                "v5" => crate::mqtt::Protocol::V5,
                _ => return Err(Error::new("invalid MQTT protocol")),
            },
            transport,
            allow_insecure_lab: self.allow_insecure_lab,
            user_name,
            client_id: self.client_id.clone(),
            shared_group: self.shared_group.clone(),
            routes,
            session_expiry: Duration::from_secs(self.session_expiry_secs.unwrap_or(3600)),
            keep_alive: Duration::from_secs(self.keep_alive_secs.unwrap_or(30)),
            timeout: Duration::from_millis(self.timeout_ms),
            max_packet_bytes: usize::try_from(self.max_packet_bytes).unwrap_or(usize::MAX),
            schema_version: self.schema_version,
            sparkplug_state_dir: self.sparkplug_state_dir.clone(),
        })
    }
}

#[cfg(all(test, feature = "modbus"))]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicU64, Ordering};

    static SEQ: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn a_modbus_config_polls_the_tcp_simulator() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(std::time::Duration::from_secs(2)))
                .unwrap();
            let mut request = [0; 12];
            stream.read_exact(&mut request).unwrap();
            let function = request[7];
            let mut response = request[..7].to_vec();
            response[4..6].copy_from_slice(&4u16.to_be_bytes());
            response.extend_from_slice(&[function, 1, 0x01]);
            let _ = stream.write_all(&response);
        });
        let dir = std::env::temp_dir().join(format!(
            "hashi-modbus-config-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let text = format!(
            "checkpoint_dir = \"state\"\n\n[[instance]]\nname = \"line-1\"\n\
             source.modbus = {{ state_dir = \"modbus\", devices = [{{ \
             endpoint = {{ type = \"tcp\", host = \"127.0.0.1\", port = {port} }}, unit = 1, timeout_ms = 500, \
             min_request_interval_ms = 10, max_backoff_ms = 1000, groups = [{{ kind = \"samples\", \
             record_key = \"device\", schema_version = 1, table = \"coils\", interval_ms = 100, \
             registers = [{{ name = \"flag\", address = 0, value_type = \"bool\", word_order = \"big\", \
             byte_order = \"big\", scale = 1.0, offset = 0.0 }}] }}] }}] }}\n\
             sink.jsonl = {{ dir = \"out\" }}\n"
        );
        let config = Config::parse(&text, &dir).unwrap();
        let mut service = Service::from_config(&config).unwrap();
        let (name, outcome) = &service.pass()[0];
        assert_eq!(name, "line-1");
        let report = outcome.as_ref().expect("modbus pass returned");
        assert!(report.delivered >= 1, "{report:?}");
        drop(service);
        let _ = server.join();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn modbus_tcp_accepts_a_hostname() {
        assert_eq!(
            modbus_tcp_addr("127.0.0.1", 502).unwrap(),
            "127.0.0.1:502".parse().unwrap()
        );
        assert!(modbus_tcp_addr("localhost", 502).is_ok());
        assert!(modbus_tcp_addr("not a host", 502).is_err());
    }
}
