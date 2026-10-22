//! Health, metrics and the diagnostics bundle against a fixture run (#27).

use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

static DIR_SEQ: AtomicU64 = AtomicU64::new(0);

const SECRET: &str = "observe-fixture-credential-value";

fn digest(value: &str) -> String {
    use sha2::{Digest, Sha256};
    format!("{:x}", Sha256::digest(value.as_bytes()))
}

/// A fixture instance with three records whose payloads are `value-<n>`.
fn work_dir() -> (PathBuf, PathBuf) {
    let dir = std::env::temp_dir().join(format!(
        "hashi-observe-{}-{}",
        std::process::id(),
        DIR_SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir_all(dir.join("secrets")).unwrap();
    fs::write(dir.join("secrets/line-1"), SECRET).unwrap();
    let config = dir.join("hashi.toml");
    fs::write(
        &config,
        format!(
            "checkpoint_dir = \"state\"\n\n[[instance]]\nname = \"line-1\"\n\
             source.fixture = {{ records = 3, credential_file = \"secrets/line-1\", accept_sha256 = [\"{}\"] }}\n\
             sink.jsonl = {{ dir = \"out\" }}\n",
            digest(SECRET)
        ),
    )
    .unwrap();
    (dir, config)
}

/// Kills the child even when an assertion fails.
struct Running(Child);

impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn get(addr: &str, path: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).unwrap();
    write!(stream, "GET {path} HTTP/1.1\r\nHost: hashi\r\n\r\n").unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    let status = response[9..12].parse().unwrap();
    let body = response.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    (status, body)
}

fn wait_for(addr: &str, path: &str, done: impl Fn(u16, &str) -> bool) -> String {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let (status, body) = get(addr, path);
        if done(status, &body) {
            return body;
        }
        assert!(
            Instant::now() < deadline,
            "{path} never matched: {status} {body}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn metrics_and_health_reflect_a_fixture_run() {
    let (dir, config) = work_dir();
    let mut child = Running(
        Command::new(env!("CARGO_BIN_EXE_hashi"))
            .args(["run", "--config", config.to_str().unwrap()])
            .args(["--interval-ms", "50", "--listen", "127.0.0.1:0"])
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let mut stderr = BufReader::new(child.0.stderr.take().unwrap());
    let mut line = String::new();
    stderr.read_line(&mut line).unwrap();
    let addr = line
        .trim()
        .strip_prefix("hashi: listening on ")
        .unwrap_or_else(|| panic!("unexpected first line: {line}"))
        .to_string();
    std::thread::spawn(move || std::io::copy(&mut stderr, &mut std::io::sink()));

    let metrics = wait_for(&addr, "/metrics", |_, body| {
        body.contains("hashi_records_delivered_total{instance=\"line-1\"} 3\n")
    });
    for series in [
        "hashi_checkpoint_position{instance=\"line-1\"} 3\n",
        "hashi_records_quarantined_total{instance=\"line-1\"} 0\n",
        "hashi_pass_errors_total{instance=\"line-1\"} 0\n",
        "hashi_up{instance=\"line-1\"} 1\n",
        "hashi_source_lag_seconds{instance=\"line-1\"} ",
        "hashi_checkpoint_age_seconds{instance=\"line-1\"} ",
    ] {
        assert!(metrics.contains(series), "missing {series:?} in\n{metrics}");
    }
    assert_eq!(get(&addr, "/healthz").0, 200);

    // A rotated-out credential fails the next passes: health drops and errors count.
    fs::write(dir.join("secrets/line-1"), "not-the-accepted-value").unwrap();
    wait_for(&addr, "/healthz", |status, _| status == 503);
    let metrics = get(&addr, "/metrics").1;
    assert!(
        metrics.contains("hashi_up{instance=\"line-1\"} 0\n"),
        "{metrics}"
    );
    assert!(!metrics.contains("hashi_pass_errors_total{instance=\"line-1\"} 0\n"));
    assert_eq!(get(&addr, "/nope").0, 404);
}

fn hashi(args: &[&str]) -> (bool, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_hashi"))
        .args(args)
        .output()
        .unwrap();
    (
        output.status.success(),
        String::from_utf8(output.stdout).unwrap(),
    )
}

#[test]
fn the_diagnostics_bundle_holds_state_but_no_secret_or_payload() {
    let (dir, config) = work_dir();
    let config = config.to_str().unwrap();
    assert!(hashi(&["run", "--config", config, "--once"]).0);
    let (ok, bundle) = hashi(&["diagnostics", "--config", config]);
    assert!(ok);
    let json: serde_json::Value = serde_json::from_str(&bundle).unwrap();
    let instance = &json["instances"][0];
    assert_eq!(instance["name"], "line-1");
    assert_eq!(instance["checkpoint"]["position"], 3);
    assert!(instance["schema_kinds"]["reading"].is_object(), "{bundle}");

    let delivered = hashi::JsonLinesSink::open(dir.join("out"))
        .unwrap()
        .replay()
        .unwrap();
    assert_eq!(
        delivered[0].0.payload, "value-1",
        "the run delivered payloads"
    );
    for forbidden in [
        SECRET,
        &digest(SECRET),
        "value-1",
        "value-3",
        "secrets/line-1",
    ] {
        assert!(
            !bundle.contains(forbidden),
            "bundle leaks {forbidden:?}:\n{bundle}"
        );
    }
    assert!(
        !bundle.contains(dir.to_string_lossy().as_ref()),
        "bundle leaks local paths"
    );
}
