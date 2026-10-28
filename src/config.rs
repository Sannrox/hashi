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
//! # Optional store-and-forward buffer in front of the sink (#47).
//! buffer = { dir = "buffer/line-1", max_bytes = 1073741824, max_age_secs = 86400, when_full = "stop" }
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
    /// A local disk buffer between the source and the sink. Absent: the
    /// sink is written directly.
    #[serde(default)]
    pub buffer: Option<BufferConfig>,
}

/// `buffer = { dir, max_bytes, max_age_secs?, when_full? }`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BufferConfig {
    pub dir: PathBuf,
    pub max_bytes: u64,
    /// The oldest buffered record may be at most this old. Absent: no age cap.
    #[serde(default)]
    pub max_age_secs: Option<u64>,
    /// `stop` (default) or `drop_oldest`.
    #[serde(default)]
    pub when_full: crate::buffer::WhenFull,
}

impl BufferConfig {
    pub fn limits(&self) -> crate::buffer::BufferLimits {
        crate::buffer::BufferLimits {
            max_bytes: self.max_bytes,
            max_age_ms: self
                .max_age_secs
                .map(|secs| i64::try_from(secs.saturating_mul(1000)).unwrap_or(i64::MAX)),
            when_full: self.when_full,
        }
    }
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
            if let Some(buffer) = &mut instance.buffer {
                buffer.dir = base.join(&buffer.dir);
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
            if let Some(buffer) = &instance.buffer {
                if buffer.dir.as_os_str().is_empty() {
                    return invalid(field("buffer.dir"), "must not be empty");
                }
                // One directory, one buffer: shared with a sink or another
                // buffer, two writers would interleave its log.
                if !sink_dirs.insert(
                    buffer
                        .dir
                        .components()
                        .filter(|part| !matches!(part, std::path::Component::CurDir))
                        .collect::<PathBuf>(),
                ) {
                    return invalid(field("buffer.dir"), "is used by a sink or another buffer");
                }
                if buffer.max_bytes == 0 {
                    return invalid(field("buffer.max_bytes"), "must be at least 1");
                }
                if buffer.max_age_secs == Some(0) {
                    return invalid(field("buffer.max_age_secs"), "must be at least 1");
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

    /// A buffer section resolves its directory like the sink's, defaults to
    /// `stop`, and is refused when it shares a directory or has no room.
    #[test]
    fn a_buffer_section_is_parsed_and_validated() {
        let buffered =
            |buffer: &str| config(&format!("{}\nbuffer = {{ {buffer} }}", valid_instance()));
        let parsed = Config::parse(
            &buffered("dir = \"buf\", max_bytes = 1024, max_age_secs = 60"),
            Path::new("/etc/hashi"),
        )
        .unwrap();
        let buffer = parsed.instances[0].buffer.as_ref().unwrap();
        assert_eq!(buffer.dir, Path::new("/etc/hashi/buf"));
        assert_eq!(
            buffer.limits(),
            crate::buffer::BufferLimits {
                max_bytes: 1024,
                max_age_ms: Some(60_000),
                when_full: crate::buffer::WhenFull::Stop,
            }
        );
        let dropping = Config::parse(
            &buffered("dir = \"buf\", max_bytes = 1, when_full = \"drop_oldest\""),
            Path::new("."),
        )
        .unwrap();
        assert_eq!(
            dropping.instances[0].buffer.as_ref().unwrap().when_full,
            crate::buffer::WhenFull::DropOldest
        );
        for (buffer, field) in [
            ("dir = \"out\", max_bytes = 1", "instance[0].buffer.dir"),
            (
                "dir = \"buf\", max_bytes = 0",
                "instance[0].buffer.max_bytes",
            ),
            (
                "dir = \"buf\", max_bytes = 1, max_age_secs = 0",
                "instance[0].buffer.max_age_secs",
            ),
            (
                "dir = \"buf\", max_bytes = 1, when_full = \"block\"",
                "drop_oldest",
            ),
        ] {
            let error = Config::parse(&buffered(buffer), Path::new("."))
                .unwrap_err()
                .message;
            assert!(error.contains(field), "{buffer}: {error}");
        }
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
