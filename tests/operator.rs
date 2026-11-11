//! Operator CLI: discover a fixture source and refuse trust on it (#96).

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

static DIR_SEQ: AtomicU64 = AtomicU64::new(0);

fn work_dir() -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "hashi-operator-cli-{}-{}",
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

fn write_config(dir: &Path, extra_instance: bool) -> PathBuf {
    let path = dir.join("hashi.toml");
    let mut text = format!(
        "checkpoint_dir = \"state\"\n\n[[instance]]\nname = \"line-1\"\n\
         source.fixture = {{ records = 2, credential_file = \"secrets/line-1\", accept_sha256 = [\"{}\"] }}\n\
         sink.jsonl = {{ dir = \"out\" }}\n",
        digest("secret")
    );
    if extra_instance {
        text.push_str(&format!(
            "\n[[instance]]\nname = \"line-2\"\n\
             source.fixture = {{ records = 1, credential_file = \"secrets/line-1\", accept_sha256 = [\"{}\"] }}\n\
             sink.jsonl = {{ dir = \"out-2\" }}\n",
            digest("secret")
        ));
    }
    fs::write(&path, text).unwrap();
    path
}

fn hashi(args: &[&str]) -> (bool, String, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_hashi"))
        .args(args)
        .output()
        .unwrap();
    (
        output.status.success(),
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

#[test]
fn discover_prints_versioned_fixture_kinds_and_omits_secrets() {
    let dir = work_dir();
    fs::write(dir.join("secrets/line-1"), "secret").unwrap();
    let config = write_config(&dir, false);
    let (ok, stdout, stderr) = hashi(&["discover", "--config", config.to_str().unwrap()]);
    assert!(ok, "{stderr}{stdout}");
    assert!(stdout.contains("\"schema_version\": 1"), "{stdout}");
    assert!(stdout.contains("\"kind\": \"reading\""), "{stdout}");
    assert!(stdout.contains("\"key\": \"reading\""), "{stdout}");
    assert!(stdout.contains("\"source\": \"fixture\""), "{stdout}");
    assert!(!stdout.contains("secret"), "{stdout}");
    assert!(!stdout.contains("BEGIN "), "{stdout}");
    let (ok, stdout, stderr) = hashi(&["trust", "list", "--config", config.to_str().unwrap()]);
    assert!(!ok, "{stdout}");
    assert!(
        stderr.contains("trust does not apply to fixture"),
        "{stderr}"
    );
    fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn discover_requires_instance_when_the_config_has_several() {
    let dir = work_dir();
    fs::write(dir.join("secrets/line-1"), "secret").unwrap();
    let config = write_config(&dir, true);
    let (ok, stdout, stderr) = hashi(&["discover", "--config", config.to_str().unwrap()]);
    assert!(!ok, "{stdout}");
    assert!(stderr.contains("--instance"), "{stderr}");
    let (ok, stdout, stderr) = hashi(&[
        "discover",
        "--config",
        config.to_str().unwrap(),
        "--instance",
        "line-2",
    ]);
    assert!(ok, "{stderr}{stdout}");
    assert!(stdout.contains("\"instance\": \"line-2\""), "{stdout}");
    fs::remove_dir_all(&dir).unwrap();
}

#[cfg(feature = "mysql")]
#[test]
fn discover_lists_mysql_tables_from_config_without_connecting() {
    let dir = work_dir();
    fs::write(dir.join("secrets/mysql"), "password").unwrap();
    let path = dir.join("hashi.toml");
    fs::write(
        &path,
        "checkpoint_dir = \"state\"\n\n[[instance]]\nname = \"line-1\"\n\
         source.mysql = { url = \"mysql://user@127.0.0.1:1\", credential_file = \"secrets/mysql\", \
         tables = [{ database = \"plant\", table = \"items\" }], state_dir = \"mysql\", \
         chunk_rows = 100, max_batch_bytes = 1048576, server_id = 1024, schema_version = 1, timeout_ms = 20 }\n\
         sink.jsonl = { dir = \"out\" }\n",
    )
    .unwrap();
    let (ok, stdout, stderr) = hashi(&["discover", "--config", path.to_str().unwrap()]);
    assert!(ok, "{stderr}{stdout}");
    assert!(stdout.contains("\"source\": \"mysql\""), "{stdout}");
    assert!(stdout.contains("\"kind\": \"plant.items\""), "{stdout}");
    assert!(
        stdout.contains("\"kind\": \"__mysql_transaction\""),
        "{stdout}"
    );
    fs::remove_dir_all(&dir).unwrap();
}
