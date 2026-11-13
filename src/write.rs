//! Write commands from the host, applied at most once per command id (#98).
//!
//! The host decides whether a write is allowed; Hashi only refuses targets the
//! writer's configuration does not list. `hashi write` reads a
//! [`CommandFile`], applies each command through a [`Target`] and prints one
//! [`CommandResult`] per command. The [`Ledger`], not the printed result, is
//! the durable record: rerunning the same file answers from it instead of
//! sending again.
//!
//! Outcomes:
//! - `applied`: the target confirmed the write.
//! - `refused`: the write was not applied. Before sending (not allowed,
//!   unparsable target, wrong value type, no session) nothing is recorded, so
//!   the same id may be retried. A refusal by the target is recorded.
//! - `unknown`: the write may or may not have been applied (lost response,
//!   lost session, or a crash after intent was recorded). Recorded; the same
//!   id keeps answering `unknown`. The host checks the target and sends a new
//!   id if it still wants the write.
//!
//! Each send happens under the ledger's fence lock, admitted with the writer's
//! lease token, between the intent and the outcome append. A writer that took
//! over admits its token at the fence before it reads the ledger, so it waits
//! for an in-flight send to finish, and a paused predecessor is refused before
//! it can record or send anything afterwards.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::Error;
use crate::checkpoint::{FenceLock, lock_and_admit, repair_log_tail, sync_dir};
use crate::config::{Config, WriterTarget};
use crate::lease::{self, InstanceLease};

/// Command and result document version.
pub const WRITE_VERSION: u32 = 1;

/// `{"version": 1, "commands": [...]}` as the host writes it.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandFile {
    pub version: u32,
    pub commands: Vec<Command>,
}

impl CommandFile {
    pub fn parse(text: &str) -> Result<Self, Error> {
        let file: Self = serde_json::from_str(text)
            .map_err(|error| Error::new(format!("invalid command file: {error}")))?;
        if file.version != WRITE_VERSION {
            return Err(Error::new(format!(
                "unsupported command file version {}",
                file.version
            )));
        }
        Ok(file)
    }
}

/// One write. `target` names the destination in the target's own syntax (an
/// OPC UA node id) and must equal an entry of the writer's `allow` list.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Command {
    pub id: String,
    pub target: String,
    pub value: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum Outcome {
    Applied,
    Refused { reason: String },
    Unknown { reason: String },
}

impl Outcome {
    fn refused(reason: impl Into<String>) -> Self {
        Self::Refused {
            reason: reason.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandResult {
    pub id: String,
    #[serde(flatten)]
    pub outcome: Outcome,
}

/// `{"version": 1, "results": [...]}` as `hashi write` prints it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResultFile {
    pub version: u32,
    pub results: Vec<CommandResult>,
}

impl<T: Target> Target for &mut T {
    type Prepared = T::Prepared;
    fn prepare(&mut self, command: &Command) -> Result<T::Prepared, String> {
        (**self).prepare(command)
    }
    fn send(&mut self, prepared: T::Prepared) -> Outcome {
        (**self).send(prepared)
    }
}

/// A destination that can apply one command. Implemented per protocol.
pub trait Target {
    type Prepared;
    /// Resolve and type-check `command` without sending anything. `Err` is the
    /// refusal reason.
    fn prepare(&mut self, command: &Command) -> Result<Self::Prepared, String>;
    /// Send once and classify the response. Never resends.
    fn send(&mut self, prepared: Self::Prepared) -> Outcome;
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "entry", rename_all = "snake_case")]
enum Entry {
    Intent {
        id: String,
        target: String,
        value: Value,
    },
    Outcome {
        id: String,
        #[serde(flatten)]
        outcome: Outcome,
    },
}

struct Recorded {
    target: String,
    value: Value,
    outcome: Option<Outcome>,
}

/// Append-only record of intents and outcomes for one writer, in its own
/// directory beside the instance checkpoints.
pub struct Ledger {
    dir: PathBuf,
    log: PathBuf,
    fence: PathBuf,
    entries: BTreeMap<String, Recorded>,
}

impl Ledger {
    /// Admit `token` at the fence, then load the log. Complete lines only: a
    /// torn tail from a crash is ignored here and repaired before the next
    /// append. An unreadable complete line fails closed.
    pub fn open(dir: &Path, token: u64) -> Result<Self, Error> {
        // Every directory this creates is synced into its parent below, so a
        // power loss cannot drop a new ledger after a write was applied.
        let created: Vec<PathBuf> = dir
            .ancestors()
            .take_while(|ancestor| !ancestor.as_os_str().is_empty() && !ancestor.exists())
            .map(Path::to_path_buf)
            .collect();
        fs::create_dir_all(dir)?;
        let log = dir.join("writes.v1.log");
        let fence = dir.join("fence-writes.v1");
        drop(lock_and_admit(&fence, token)?);
        sync_dir(dir)?;
        for created in &created {
            match created.parent() {
                Some(parent) if !parent.as_os_str().is_empty() => sync_dir(parent)?,
                _ => sync_dir(Path::new("."))?,
            }
        }
        let mut entries = BTreeMap::new();
        let mut bytes = Vec::new();
        match File::open(&log) {
            Ok(mut file) => {
                file.read_to_end(&mut bytes)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let complete = match bytes.iter().rposition(|&byte| byte == b'\n') {
            Some(end) => &bytes[..=end],
            None => &[][..],
        };
        for line in complete.split(|&byte| byte == b'\n') {
            if line.is_empty() {
                continue;
            }
            let entry: Entry = serde_json::from_slice(line)
                .map_err(|_| Error::new("write ledger entry is unreadable"))?;
            match entry {
                Entry::Intent { id, target, value } => {
                    entries.insert(
                        id,
                        Recorded {
                            target,
                            value,
                            outcome: None,
                        },
                    );
                }
                Entry::Outcome { id, outcome } => {
                    let recorded = entries
                        .get_mut(&id)
                        .ok_or_else(|| Error::new("write ledger outcome has no intent"))?;
                    recorded.outcome = Some(outcome);
                }
            }
        }
        Ok(Self {
            dir: dir.to_path_buf(),
            log,
            fence,
            entries,
        })
    }

    /// What an earlier attempt of `command.id` already settled, if anything.
    fn recorded(&self, command: &Command) -> Option<Outcome> {
        let recorded = self.entries.get(&command.id)?;
        if recorded.target != command.target || recorded.value != command.value {
            return Some(Outcome::refused(
                "command id was already used for a different write",
            ));
        }
        Some(
            recorded
                .outcome
                .clone()
                .unwrap_or_else(|| Outcome::Unknown {
                    reason: "an earlier attempt did not record an outcome".into(),
                }),
        )
    }

    /// Lock the fence and admit `token`. While the guard lives, a successor
    /// cannot open the ledger, so nothing it does can interleave.
    fn admit(&self, token: u64) -> Result<FenceLock, Error> {
        lock_and_admit(&self.fence, token)
    }

    #[cfg(test)]
    fn append(&mut self, entry: &Entry, token: u64) -> Result<(), Error> {
        let _fence = self.admit(token)?;
        self.append_admitted(entry)
    }

    /// Append under a fence guard the caller holds.
    fn append_admitted(&mut self, entry: &Entry) -> Result<(), Error> {
        let mut line =
            serde_json::to_string(entry).map_err(|error| Error::new(error.to_string()))?;
        line.push('\n');
        repair_log_tail(&self.log)?;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log)?;
        file.write_all(line.as_bytes())?;
        file.sync_all()?;
        drop(file);
        sync_dir(&self.dir)?;
        match entry {
            Entry::Intent { id, target, value } => {
                self.entries.insert(
                    id.clone(),
                    Recorded {
                        target: target.clone(),
                        value: value.clone(),
                        outcome: None,
                    },
                );
            }
            Entry::Outcome { id, outcome } => {
                if let Some(recorded) = self.entries.get_mut(id) {
                    recorded.outcome = Some(outcome.clone());
                }
            }
        }
        Ok(())
    }
}

/// The target, connected on first use so recorded ids are answered even when
/// it is unreachable. A failed connection refuses every later command that
/// needs it; nothing was sent.
enum Connection<T, F> {
    Pending(F),
    Open(T),
    Failed(String),
}

impl<T, F: FnOnce() -> Result<T, Error>> Connection<T, F> {
    fn get(&mut self) -> Result<&mut T, String> {
        let state = std::mem::replace(self, Self::Failed(String::new()));
        *self = match state {
            Self::Pending(connect) => match connect() {
                Ok(target) => Self::Open(target),
                Err(error) => Self::Failed(format!("target connection failed: {error}")),
            },
            other => other,
        };
        match self {
            Self::Open(target) => Ok(target),
            Self::Failed(reason) => Err(reason.clone()),
            Self::Pending(_) => Err("target was not connected".into()),
        }
    }
}

/// Apply `commands` in order, calling `connect` only when a command needs the
/// target. A ledger or lease failure stops the run with an error; rerunning
/// the same commands then answers settled ids from the ledger and reports an
/// interrupted one as `unknown`.
pub fn apply<T: Target>(
    ledger: &mut Ledger,
    lease: &mut InstanceLease,
    allow: &[String],
    connect: impl FnOnce() -> Result<T, Error>,
    commands: &[Command],
) -> Result<Vec<CommandResult>, Error> {
    let mut target = Connection::Pending(connect);
    let mut results = Vec::with_capacity(commands.len());
    for command in commands {
        let outcome = apply_one(ledger, lease, allow, &mut target, command)?;
        results.push(CommandResult {
            id: command.id.clone(),
            outcome,
        });
    }
    Ok(results)
}

fn apply_one<T: Target, F: FnOnce() -> Result<T, Error>>(
    ledger: &mut Ledger,
    lease: &mut InstanceLease,
    allow: &[String],
    target: &mut Connection<T, F>,
    command: &Command,
) -> Result<Outcome, Error> {
    if command.id.is_empty() {
        return Ok(Outcome::refused("command id must not be empty"));
    }
    if let Some(outcome) = ledger.recorded(command) {
        return Ok(outcome);
    }
    if !allow.contains(&command.target) {
        return Ok(Outcome::refused("target is not in the writer's allow list"));
    }
    let target = match target.get() {
        Ok(target) => target,
        Err(reason) => return Ok(Outcome::Refused { reason }),
    };
    let prepared = match target.prepare(command) {
        Ok(prepared) => prepared,
        Err(reason) => return Ok(Outcome::Refused { reason }),
    };
    lease.renew()?;
    // Hold the fence from intent through outcome: a writer that took over
    // waits here instead of acting while this one can still send.
    let _fence = ledger.admit(lease.token())?;
    ledger.append_admitted(&Entry::Intent {
        id: command.id.clone(),
        target: command.target.clone(),
        value: command.value.clone(),
    })?;
    let outcome = target.send(prepared);
    ledger.append_admitted(&Entry::Outcome {
        id: command.id.clone(),
        outcome: outcome.clone(),
    })?;
    Ok(outcome)
}

/// Run `commands` against the configured writer `name`: hold its lease, open
/// its ledger under `checkpoint_dir/<name>`, apply (connecting on first need),
/// release.
pub fn run(config: &Config, name: &str, commands: &CommandFile) -> Result<ResultFile, Error> {
    let writer = config
        .writers
        .iter()
        .find(|writer| writer.name == name)
        .ok_or_else(|| Error::new(format!("no writer named {name}")))?;
    let store = lease::open_store(&config.lease)?;
    let holder = lease::next_holder()?;
    let mut held = lease::acquire(&store, "write", name, &holder, config.lease.ttl())?;
    let mut ledger = Ledger::open(&config.checkpoint_dir.join(name), held.token())?;
    let results = apply_to(
        &writer.target,
        name,
        &mut ledger,
        &mut held,
        &commands.commands,
    )?;
    held.release()?;
    Ok(ResultFile {
        version: WRITE_VERSION,
        results,
    })
}

/// Opens the writer's target lazily, through [`apply`]. A target family whose
/// feature is not built is a configuration error.
#[cfg_attr(not(all(feature = "opcua", feature = "mqtt")), allow(unused_variables))]
fn apply_to(
    target: &WriterTarget,
    name: &str,
    ledger: &mut Ledger,
    lease: &mut InstanceLease,
    commands: &[Command],
) -> Result<Vec<CommandResult>, Error> {
    match target {
        #[cfg(feature = "opcua")]
        WriterTarget::OpcUa(opcua) => apply(
            ledger,
            lease,
            &opcua.allow,
            || crate::opcua::OpcUaConnector::connect(opcua.connection().connector_config(name)?),
            commands,
        ),
        #[cfg(not(feature = "opcua"))]
        WriterTarget::OpcUa(_) => Err(Error::new("OPC UA writers need the opcua feature")),
        #[cfg(feature = "mqtt")]
        WriterTarget::Mqtt(mqtt) => apply(
            ledger,
            lease,
            &mqtt.allow,
            || crate::mqtt::write::MqttWriter::connect(mqtt.connection().connector_config(name)?),
            commands,
        ),
        #[cfg(not(feature = "mqtt"))]
        WriterTarget::Mqtt(_) => Err(Error::new("MQTT writers need the mqtt feature")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::atomic::{AtomicU64, Ordering};

    static SEQ: AtomicU64 = AtomicU64::new(0);

    fn dir() -> PathBuf {
        std::env::temp_dir().join(format!(
            "hashi-write-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ))
    }

    /// Records every send into a shared log and answers Applied.
    struct Recorder(Rc<RefCell<Vec<String>>>);

    impl Target for Recorder {
        type Prepared = String;
        fn prepare(&mut self, command: &Command) -> Result<String, String> {
            Ok(command.id.clone())
        }
        fn send(&mut self, id: String) -> Outcome {
            self.0.borrow_mut().push(id);
            Outcome::Applied
        }
    }

    fn command(id: &str, value: f64) -> Command {
        Command {
            id: id.into(),
            target: "ns=2;s=Setpoint".into(),
            value: value.into(),
        }
    }

    /// Apply `commands` through a [`Recorder`] logging into `sent`.
    fn run(
        ledger: &mut Ledger,
        token: u64,
        sent: &Rc<RefCell<Vec<String>>>,
        commands: &[Command],
    ) -> Result<Vec<Outcome>, Error> {
        let recorder = Recorder(sent.clone());
        Ok(apply(
            ledger,
            &mut InstanceLease::unfenced(token),
            &["ns=2;s=Setpoint".to_string()],
            || Ok(recorder),
            commands,
        )?
        .into_iter()
        .map(|result| result.outcome)
        .collect())
    }

    #[test]
    fn a_settled_id_is_answered_from_the_ledger_after_reopening() {
        let dir = dir();
        let sent = Rc::default();
        for _ in 0..2 {
            let mut ledger = Ledger::open(&dir, 1).unwrap();
            let outcomes = run(&mut ledger, 1, &sent, &[command("a", 1.0)]).unwrap();
            assert_eq!(outcomes, [Outcome::Applied]);
        }
        assert_eq!(*sent.borrow(), ["a"]);
    }

    #[test]
    fn an_intent_without_an_outcome_reports_unknown_and_is_not_resent() {
        let dir = dir();
        let crashed = command("a", 1.0);
        // A crash between recording intent and recording the outcome.
        Ledger::open(&dir, 1)
            .unwrap()
            .append(
                &Entry::Intent {
                    id: crashed.id.clone(),
                    target: crashed.target.clone(),
                    value: crashed.value.clone(),
                },
                1,
            )
            .unwrap();
        let sent = Rc::default();
        let outcomes = run(&mut Ledger::open(&dir, 2).unwrap(), 2, &sent, &[crashed]).unwrap();
        assert!(matches!(outcomes[0], Outcome::Unknown { .. }));
        assert!(sent.borrow().is_empty());
    }

    #[test]
    fn a_reused_id_with_a_different_value_is_refused_without_sending() {
        let sent = Rc::default();
        let outcomes = run(
            &mut Ledger::open(&dir(), 1).unwrap(),
            1,
            &sent,
            &[command("a", 1.0), command("a", 2.0)],
        )
        .unwrap();
        assert_eq!(outcomes[0], Outcome::Applied);
        assert!(matches!(outcomes[1], Outcome::Refused { .. }));
        assert_eq!(*sent.borrow(), ["a"]);
    }

    #[test]
    fn a_target_outside_the_allow_list_is_refused_and_not_recorded() {
        let mut ledger = Ledger::open(&dir(), 1).unwrap();
        let sent = Rc::default();
        let mut other = command("a", 1.0);
        other.target = "ns=2;s=Other".into();
        let outcomes = run(&mut ledger, 1, &sent, &[other]).unwrap();
        assert!(matches!(outcomes[0], Outcome::Refused { .. }));
        // Nothing was sent, so the same id may still be used once allowed.
        let outcomes = run(&mut ledger, 1, &sent, &[command("a", 1.0)]).unwrap();
        assert_eq!(outcomes, [Outcome::Applied]);
        assert_eq!(*sent.borrow(), ["a"]);
    }

    #[test]
    fn recorded_ids_are_answered_while_the_target_is_unreachable() {
        let dir = dir();
        let sent = Rc::default();
        run(
            &mut Ledger::open(&dir, 1).unwrap(),
            1,
            &sent,
            &[command("a", 1.0)],
        )
        .unwrap();
        let results = apply(
            &mut Ledger::open(&dir, 1).unwrap(),
            &mut InstanceLease::unfenced(1),
            &["ns=2;s=Setpoint".to_string()],
            || Err::<Recorder, _>(Error::new("server offline")),
            &[command("a", 1.0), command("b", 2.0)],
        )
        .unwrap();
        assert_eq!(results[0].outcome, Outcome::Applied);
        assert!(matches!(
            &results[1].outcome,
            Outcome::Refused { reason } if reason.contains("server offline")
        ));
    }

    /// Sends while probing whether a successor could take the fence.
    struct Probe {
        fence: PathBuf,
        fence_free_during_send: Option<bool>,
    }

    impl Target for Probe {
        type Prepared = ();
        fn prepare(&mut self, _: &Command) -> Result<(), String> {
            Ok(())
        }
        fn send(&mut self, _: ()) -> Outcome {
            let file = File::open(&self.fence).unwrap();
            self.fence_free_during_send = Some(file.try_lock().is_ok());
            Outcome::Applied
        }
    }

    #[test]
    fn a_successor_cannot_take_the_fence_while_a_send_is_in_flight() {
        let dir = dir();
        let mut ledger = Ledger::open(&dir, 1).unwrap();
        let mut probe = Probe {
            fence: dir.join("fence-writes.v1"),
            fence_free_during_send: None,
        };
        apply(
            &mut ledger,
            &mut InstanceLease::unfenced(1),
            &["ns=2;s=Setpoint".to_string()],
            || Ok(&mut probe),
            &[command("a", 1.0)],
        )
        .unwrap();
        assert_eq!(probe.fence_free_during_send, Some(false));
    }

    #[test]
    fn a_stale_writer_cannot_record_intent_after_a_takeover_opened_the_ledger() {
        let dir = dir();
        let mut stale = Ledger::open(&dir, 1).unwrap();
        let _successor = Ledger::open(&dir, 2).unwrap();
        let sent = Rc::default();
        assert!(run(&mut stale, 1, &sent, &[command("a", 1.0)]).is_err());
        assert!(sent.borrow().is_empty());
    }

    #[test]
    fn a_torn_ledger_tail_is_ignored_and_repaired() {
        let dir = dir();
        let sent = Rc::default();
        run(
            &mut Ledger::open(&dir, 1).unwrap(),
            1,
            &sent,
            &[command("a", 1.0)],
        )
        .unwrap();
        OpenOptions::new()
            .append(true)
            .open(dir.join("writes.v1.log"))
            .unwrap()
            .write_all(b"{\"entry\":\"intent\",\"id\":\"b\"")
            .unwrap();
        let outcomes = run(
            &mut Ledger::open(&dir, 1).unwrap(),
            1,
            &sent,
            &[command("a", 1.0), command("b", 2.0)],
        )
        .unwrap();
        assert_eq!(outcomes, [Outcome::Applied, Outcome::Applied]);
        assert_eq!(*sent.borrow(), ["a", "b"]);
        assert!(Ledger::open(&dir, 1).is_ok());
    }
}
