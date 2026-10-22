//! `hashi run` configuration (TOML).
//!
//! A config declares the checkpoint directory and connector instances, each
//! with one source section (named by family) and one sink section. Each
//! family adds its own source section; credentials are file paths, never
//! values. Unknown fields are refused, and every validation error names the
//! field it is about. Relative paths resolve against the config file's
//! directory.
//!
//! ```toml
//! checkpoint_dir = "state"
//!
//! [[instance]]
//! name = "line-1"
//! source.fixture = { records = 3, credential_file = "secrets/line-1", accept_sha256 = ["<hex>"] }
//! sink.jsonl = { dir = "out/line-1" }
//! ```

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::Error;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub checkpoint_dir: PathBuf,
    #[serde(rename = "instance")]
    pub instances: Vec<InstanceConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstanceConfig {
    pub name: String,
    pub source: SourceConfig,
    pub sink: SinkConfig,
}

/// One section per source family.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum SourceConfig {
    /// The in-process fixture source: `records` programmed records, readable
    /// only while the credential file holds a value whose SHA-256 is listed.
    Fixture(FixtureSourceConfig),
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FixtureSourceConfig {
    pub records: u64,
    pub credential_file: PathBuf,
    pub accept_sha256: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum SinkConfig {
    Jsonl(JsonlSinkConfig),
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JsonlSinkConfig {
    pub dir: PathBuf,
}

impl Config {
    /// Read, parse, validate, and resolve relative paths against the file's
    /// directory.
    pub fn load(path: &Path) -> Result<Self, Error> {
        let text = std::fs::read_to_string(path)
            .map_err(|_| Error::new(format!("config {} is unreadable", path.display())))?;
        let base = path.parent().unwrap_or(Path::new("."));
        Self::parse(&text, base)
    }

    pub fn parse(text: &str, base: &Path) -> Result<Self, Error> {
        let mut config: Config = toml::from_str(text)
            .map_err(|error| Error::new(format!("invalid config: {}", error.message())))?;
        config.validate()?;
        config.checkpoint_dir = base.join(&config.checkpoint_dir);
        for instance in &mut config.instances {
            match &mut instance.source {
                SourceConfig::Fixture(fixture) => {
                    fixture.credential_file = base.join(&fixture.credential_file);
                }
            }
            match &mut instance.sink {
                SinkConfig::Jsonl(jsonl) => jsonl.dir = base.join(&jsonl.dir),
            }
        }
        Ok(config)
    }

    fn validate(&self) -> Result<(), Error> {
        let invalid = |field: String, problem: &str| {
            Err(Error::new(format!("invalid config: {field}: {problem}")))
        };
        if self.checkpoint_dir.as_os_str().is_empty() {
            return invalid("checkpoint_dir".into(), "must not be empty");
        }
        if self.instances.is_empty() {
            return invalid("instance".into(), "declare at least one instance");
        }
        let mut names = BTreeSet::new();
        // Leases fence one instance's checkpoint, not a shared sink, so two
        // instances never write one directory.
        let mut sink_dirs = BTreeSet::new();
        for (index, instance) in self.instances.iter().enumerate() {
            let field = |name: &str| format!("instance[{index}].{name}");
            // The name is a checkpoint directory component.
            if instance.name.is_empty()
                || !instance
                    .name
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
            {
                return invalid(field("name"), "use 1 or more of A-Z a-z 0-9 - _");
            }
            if !names.insert(instance.name.as_str()) {
                return invalid(field("name"), "duplicates another instance");
            }
            match &instance.source {
                SourceConfig::Fixture(fixture) => {
                    if fixture.records == 0 {
                        return invalid(field("source.fixture.records"), "must be at least 1");
                    }
                    if fixture.credential_file.as_os_str().is_empty() {
                        return invalid(
                            field("source.fixture.credential_file"),
                            "must not be empty",
                        );
                    }
                    if fixture.accept_sha256.is_empty()
                        || fixture.accept_sha256.iter().any(|digest| {
                            digest.len() != 64
                                || !digest
                                    .bytes()
                                    .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
                        })
                    {
                        return invalid(
                            field("source.fixture.accept_sha256"),
                            "list lowercase hex SHA-256 digests",
                        );
                    }
                }
            }
            match &instance.sink {
                SinkConfig::Jsonl(jsonl) => {
                    if jsonl.dir.as_os_str().is_empty() {
                        return invalid(field("sink.jsonl.dir"), "must not be empty");
                    }
                    if !sink_dirs.insert(
                        jsonl
                            .dir
                            .components()
                            .filter(|part| !matches!(part, std::path::Component::CurDir))
                            .collect::<PathBuf>(),
                    ) {
                        return invalid(field("sink.jsonl.dir"), "is used by another instance");
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DIGEST: &str = "0000000000000000000000000000000000000000000000000000000000000000";

    fn config(instance: &str) -> String {
        format!("checkpoint_dir = \"state\"\n\n[[instance]]\n{instance}\n")
    }

    fn valid_instance() -> String {
        format!(
            "name = \"line-1\"\nsource.fixture = {{ records = 2, credential_file = \"secret\", accept_sha256 = [\"{DIGEST}\"] }}\nsink.jsonl = {{ dir = \"out\" }}"
        )
    }

    #[test]
    fn a_valid_config_resolves_paths_against_its_directory() {
        let parsed = Config::parse(&config(&valid_instance()), Path::new("/etc/hashi")).unwrap();
        assert_eq!(parsed.checkpoint_dir, Path::new("/etc/hashi/state"));
        let SourceConfig::Fixture(fixture) = &parsed.instances[0].source;
        assert_eq!(fixture.credential_file, Path::new("/etc/hashi/secret"));
    }

    #[test]
    fn invalid_configs_are_refused_naming_the_field() {
        let cases = [
            (
                valid_instance().replace("line-1", "line 1"),
                "instance[0].name",
            ),
            (
                valid_instance().replace("records = 2", "records = 0"),
                "instance[0].source.fixture.records",
            ),
            (
                valid_instance().replace(DIGEST, "ABC"),
                "instance[0].source.fixture.accept_sha256",
            ),
            (
                valid_instance().replace("dir = \"out\"", "dir = \"out\", mode = \"x\""),
                "mode",
            ),
            (
                valid_instance().replace("source.fixture", "source.opcua"),
                "opcua",
            ),
            (valid_instance().replace("name = \"line-1\"\n", ""), "name"),
        ];
        for (instance, field) in cases {
            let error = Config::parse(&config(&instance), Path::new("."))
                .unwrap_err()
                .message;
            assert!(error.contains(field), "{field}: {error}");
        }
        let twice = format!(
            "{}\n\n[[instance]]\n{}",
            config(&valid_instance()),
            valid_instance()
        );
        let error = Config::parse(&twice, Path::new(".")).unwrap_err().message;
        assert!(error.contains("instance[1].name"), "{error}");
        // `./out` and `out` are one directory.
        let shared = format!(
            "{}\n\n[[instance]]\n{}",
            config(&valid_instance()),
            valid_instance()
                .replace("line-1", "line-2")
                .replace("\"out\"", "\"./out\"")
        );
        let error = Config::parse(&shared, Path::new(".")).unwrap_err().message;
        assert!(error.contains("instance[1].sink.jsonl.dir"), "{error}");
    }
}
