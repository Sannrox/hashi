//! File-referenced credentials.
//!
//! A config names a credential by file path, never by value. The file is
//! read on every use, so a rotated file applies on the next use without a
//! restart. [`Secret`] never prints its value, and errors name only the path.

use std::fs;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use crate::Error;

/// A credential value. `Debug` and `Display` are redacted.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    pub fn expose(&self) -> &str {
        &self.0
    }

    /// Lowercase hex SHA-256 of the value, for comparing against a config
    /// that must not hold the value itself.
    pub fn sha256(&self) -> String {
        format!("{:x}", Sha256::digest(self.0.as_bytes()))
    }
}

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Secret([redacted])")
    }
}

impl std::fmt::Display for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("[redacted]")
    }
}

/// A credential read from a file on each use.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileCredential {
    path: PathBuf,
}

impl FileCredential {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The current value, trimmed of surrounding whitespace. An unreadable
    /// or empty file is an error that names the path, never the content.
    pub fn current(&self) -> Result<Secret, Error> {
        let value = fs::read_to_string(&self.path).map_err(|_| {
            Error::new(format!(
                "credential file {} is unreadable",
                self.path.display()
            ))
        })?;
        let value = value.trim();
        if value.is_empty() {
            return Err(Error::new(format!(
                "credential file {} is empty",
                self.path.display()
            )));
        }
        Ok(Secret(value.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_rotated_file_applies_on_the_next_read_and_never_prints() {
        let dir = std::env::temp_dir().join(format!(
            "hashi-credential-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("token");
        fs::write(&path, "first-secret\n").unwrap();
        let credential = FileCredential::new(&path);
        let first = credential.current().unwrap();
        assert_eq!(first.expose(), "first-secret");
        fs::write(&path, "second-secret").unwrap();
        assert_eq!(credential.current().unwrap().expose(), "second-secret");
        for printed in [format!("{first}"), format!("{first:?}")] {
            assert!(!printed.contains("secret"), "{printed}");
        }
        fs::remove_file(&path).unwrap();
        let error = credential.current().unwrap_err().message;
        assert!(error.contains("unreadable") && !error.contains("second"));
        fs::remove_dir_all(&dir).unwrap();
    }
}
