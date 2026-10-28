//! `hashi run` from a config file with file-based credentials (#26).

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

use hashi::{Config, JsonLinesSink, Service};

static DIR_SEQ: AtomicU64 = AtomicU64::new(0);

const OLD: &str = "rotated-out-credential-value";
const NEW: &str = "rotated-in-credential-value";

fn work_dir() -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "hashi-run-config-{}-{}",
        std::process::id(),
        DIR_SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir_all(dir.join("secrets")).unwrap();
    dir
}

fn digest(value: &str) -> String {
    use sha2::{Digest, Sha256};
    format!("{:x}", Sha256::digest(value.as_bytes()))
}

/// A config with one fixture instance accepting only `accepted`.
fn write_config(dir: &Path, records: u64, accepted: &str) -> PathBuf {
    let path = dir.join("hashi.toml");
    fs::write(
        &path,
        format!(
            "checkpoint_dir = \"state\"\n\n[[instance]]\nname = \"line-1\"\n\
             source.fixture = {{ records = {records}, credential_file = \"secrets/line-1\", accept_sha256 = [\"{}\"] }}\n\
             sink.jsonl = {{ dir = \"out\" }}\n",
            digest(accepted)
        ),
    )
    .unwrap();
    path
}

fn delivered(dir: &Path) -> Vec<String> {
    JsonLinesSink::open(dir.join("out"))
        .unwrap()
        .replay()
        .unwrap()
        .into_iter()
        .map(|(record, _)| record.key)
        .collect()
}

fn hashi_run(config: &Path) -> (bool, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_hashi"))
        .args(["run", "--config", config.to_str().unwrap(), "--once"])
        .output()
        .unwrap();
    let logs = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    (output.status.success(), logs)
}

#[test]
fn it_starts_from_a_config_and_resumes_after_a_restart() {
    let dir = work_dir();
    fs::write(dir.join("secrets/line-1"), NEW).unwrap();
    let config = write_config(&dir, 3, NEW);
    let (ok, logs) = hashi_run(&config);
    assert!(ok, "{logs}");
    assert!(logs.contains("line-1: delivered 3"), "{logs}");
    assert_eq!(delivered(&dir), ["r1", "r2", "r3"]);

    // A new process with more source records resumes after the checkpoint.
    let config = write_config(&dir, 5, NEW);
    let (ok, logs) = hashi_run(&config);
    assert!(ok, "{logs}");
    assert!(
        logs.contains("line-1: delivered 2 quarantined 0 checkpoint 5"),
        "{logs}"
    );
    assert_eq!(delivered(&dir), ["r1", "r2", "r3", "r4", "r5"]);
    for value in [OLD, NEW] {
        assert!(!logs.contains(value));
    }
    fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn a_rotated_credential_file_applies_without_a_restart() {
    let dir = work_dir();
    let secret = dir.join("secrets/line-1");
    fs::write(&secret, OLD).unwrap();
    let config = Config::load(&write_config(&dir, 2, NEW)).unwrap();
    let mut service = Service::from_config(&config).unwrap();

    let first = service.pass();
    let error = first[0].1.as_ref().unwrap_err().message.clone();
    assert!(error.contains("not accepted"), "{error}");
    assert!(delivered(&dir).is_empty());

    // Rotate the file; the same service picks it up on its next pass.
    fs::write(&secret, NEW).unwrap();
    let second = service.pass();
    assert_eq!(second[0].1.as_ref().unwrap().delivered, 2);
    assert_eq!(delivered(&dir), ["r1", "r2"]);

    for message in [error, format!("{second:?}")] {
        for value in [OLD, NEW] {
            assert!(!message.contains(value), "{message}");
        }
    }
    fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn logs_and_errors_never_carry_the_credential() {
    let dir = work_dir();
    // The process accepts NEW but the file still holds OLD.
    fs::write(dir.join("secrets/line-1"), OLD).unwrap();
    let (ok, logs) = hashi_run(&write_config(&dir, 2, NEW));
    assert!(!ok);
    assert!(logs.contains("line-1: check failed"), "{logs}");
    // A missing file names its path only.
    fs::remove_file(dir.join("secrets/line-1")).unwrap();
    let (ok, missing) = hashi_run(&write_config(&dir, 2, NEW));
    assert!(!ok);
    for value in [OLD, NEW] {
        assert!(
            !logs.contains(value) && !missing.contains(value),
            "{logs}{missing}"
        );
    }
    fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn an_invalid_config_is_refused_naming_the_field() {
    let dir = work_dir();
    let path = dir.join("hashi.toml");
    fs::write(
        &path,
        "checkpoint_dir = \"state\"\n\n[[instance]]\nname = \"line-1\"\n\
         source.fixture = { records = 0, credential_file = \"secrets/line-1\", accept_sha256 = [] }\n\
         sink.jsonl = { dir = \"out\" }\n",
    )
    .unwrap();
    let (ok, logs) = hashi_run(&path);
    assert!(!ok);
    assert!(
        logs.contains("instance[0].source.fixture.records"),
        "{logs}"
    );
    fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn two_instances_cannot_write_one_sink_directory_through_an_alias() {
    let dir = work_dir();
    fs::write(dir.join("secrets/line-1"), NEW).unwrap();
    let config = Config::load(&write_config(&dir, 2, NEW)).unwrap();
    let mut first = Service::from_config(&config).unwrap();
    assert!(first.pass()[0].1.is_ok());
    // Same directory, reached through a different path.
    let mut alias = config.clone();
    alias.checkpoint_dir = dir.join("state-2");
    alias.instances[0].name = "line-2".into();
    let hashi::config::SinkConfig::Jsonl(jsonl) = &mut alias.instances[0].sink;
    jsonl.dir = dir.join("secrets").join("..").join("out");
    let mut second = Service::from_config(&alias).unwrap();
    let refused = second.pass()[0].1.clone().unwrap_err().message;
    assert!(refused.contains("in use by another instance"), "{refused}");
    // Once the first stops, the second runs.
    drop(first);
    assert!(second.pass()[0].1.is_ok());
    fs::remove_dir_all(&dir).unwrap();
}

/// With a buffer, an instance whose sink cannot open yet (its path is a
/// file, as with an unmounted destination) still runs: records are buffered,
/// the checkpoint advances, and the next pass after the sink returns drains
/// them in order (#47).
#[test]
fn a_buffered_instance_runs_while_its_sink_is_unavailable_and_drains_after() {
    let dir = work_dir();
    fs::write(dir.join("secrets/line-1"), NEW).unwrap();
    let config = write_config(&dir, 3, NEW);
    let mut text = fs::read_to_string(&config).unwrap();
    text.push_str("buffer = { dir = \"buffer\", max_bytes = 65536 }\n");
    fs::write(&config, text).unwrap();
    fs::write(dir.join("out"), b"not a directory yet").unwrap();

    let mut service = Service::from_config(&Config::load(&config).unwrap()).unwrap();
    let passes = service.pass_with_progress();
    let report = passes[0].outcome.as_ref().unwrap();
    assert_eq!((report.delivered, report.checkpoint), (3, 3));
    let buffer = passes[0].buffer.unwrap();
    assert_eq!((buffer.records, buffer.drained), (3, 0));

    fs::remove_file(dir.join("out")).unwrap();
    let passes = service.pass_with_progress();
    let buffer = passes[0].buffer.unwrap();
    assert_eq!((buffer.records, buffer.drained), (0, 3));
    drop(service);
    assert_eq!(delivered(&dir), ["r1", "r2", "r3"]);
}

/// Records already buffered drain even while the source is unavailable:
/// they were acknowledged and checkpointed, so they do not wait on it (#47).
#[test]
fn buffered_records_drain_while_the_source_credential_is_missing() {
    let dir = work_dir();
    fs::write(dir.join("secrets/line-1"), NEW).unwrap();
    let config = write_config(&dir, 2, NEW);
    let mut text = fs::read_to_string(&config).unwrap();
    text.push_str("buffer = { dir = \"buffer\", max_bytes = 65536 }\n");
    fs::write(&config, text).unwrap();
    fs::write(dir.join("out"), b"not a directory yet").unwrap();

    let mut service = Service::from_config(&Config::load(&config).unwrap()).unwrap();
    assert_eq!(service.pass_with_progress()[0].buffer.unwrap().records, 2);

    fs::remove_file(dir.join("secrets/line-1")).unwrap();
    fs::remove_file(dir.join("out")).unwrap();
    let passes = service.pass_with_progress();
    assert!(passes[0].outcome.is_err());
    assert_eq!(passes[0].buffer.unwrap().records, 0);
    drop(service);
    assert_eq!(delivered(&dir), ["r1", "r2"]);
}
