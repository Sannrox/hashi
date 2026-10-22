//! In-process source used to prove the runtime without a protocol connector.

use std::fs;
use std::path::Path;

use crate::Error;
use crate::connector::Connector;
use crate::contract::{CheckReport, DiscoveredKind, Position, Record};
use crate::credential::FileCredential;

/// File-credentialed source that yields a programmed snapshot then incrementals.
#[derive(Debug, Clone)]
pub struct FakeSource {
    id: String,
    credential_ok: bool,
    check_message: String,
    kinds: Vec<DiscoveredKind>,
    records: Vec<Record>,
    /// Checked on every call, so a rotated file applies without a restart.
    live_credential: Option<(FileCredential, Vec<String>)>,
}

impl FakeSource {
    pub fn new(id: impl Into<String>, records: Vec<Record>) -> Self {
        let kinds = unique_kinds(&records);
        Self {
            id: id.into(),
            credential_ok: true,
            check_message: "reachable".into(),
            kinds,
            records,
            live_credential: None,
        }
    }

    /// A source readable only while `credential` holds a value whose
    /// SHA-256 is in `accept_sha256`. The file is read on every call.
    pub fn with_live_credential(
        id: impl Into<String>,
        records: Vec<Record>,
        credential: FileCredential,
        accept_sha256: Vec<String>,
    ) -> Self {
        let mut source = Self::new(id, records);
        source.live_credential = Some((credential, accept_sha256));
        source
    }

    fn credential_ok(&self) -> bool {
        match &self.live_credential {
            Some((credential, accepted)) => credential
                .current()
                .is_ok_and(|secret| accepted.contains(&secret.sha256())),
            None => self.credential_ok,
        }
    }

    /// `credential` must exist next to the instance and match `expected`.
    pub fn with_credential_file(
        id: impl Into<String>,
        records: Vec<Record>,
        dir: &Path,
        expected: &str,
    ) -> Result<Self, Error> {
        let path = dir.join("credential");
        let ok = match fs::read_to_string(&path) {
            Ok(value) => value.trim() == expected.trim(),
            Err(_) => false,
        };
        let mut source = Self::new(id, records);
        source.credential_ok = ok;
        source.check_message = if ok {
            "reachable".into()
        } else {
            "credential file missing or mismatched".into()
        };
        Ok(source)
    }

    pub fn append(&mut self, record: Record) {
        if !self.kinds.iter().any(|kind| kind.kind == record.kind) {
            self.kinds.push(DiscoveredKind {
                kind: record.kind.clone(),
                schema: crate::schema::Schema::text(),
                metadata: Default::default(),
            });
        }
        self.records.push(record);
    }
}

impl Connector for FakeSource {
    fn id(&self) -> &str {
        &self.id
    }

    fn check(&self) -> Result<CheckReport, Error> {
        let ok = self.credential_ok();
        Ok(CheckReport {
            ok,
            message: if self.live_credential.is_none() {
                self.check_message.clone()
            } else if ok {
                "reachable".into()
            } else {
                "credential missing or not accepted".into()
            },
        })
    }

    fn discover(&self) -> Result<Vec<DiscoveredKind>, Error> {
        if !self.credential_ok() {
            return Err(Error::new("discover refused: check failed"));
        }
        Ok(self.kinds.clone())
    }

    fn read(
        &mut self,
        from: Option<Position>,
        _source_cursor: Option<&str>,
    ) -> Result<Option<Record>, Error> {
        if !self.credential_ok() {
            return Err(Error::new("read refused: check failed"));
        }
        Ok(self
            .records
            .iter()
            .find(|record| match from {
                Some(min) => record.position > min,
                None => true,
            })
            .cloned())
    }
}

fn unique_kinds(records: &[Record]) -> Vec<DiscoveredKind> {
    let mut kinds = Vec::new();
    for record in records {
        if !kinds
            .iter()
            .any(|kind: &DiscoveredKind| kind.kind == record.kind)
        {
            kinds.push(DiscoveredKind {
                kind: record.kind.clone(),
                schema: crate::schema::Schema::text(),
                metadata: Default::default(),
            });
        }
    }
    kinds
}

pub fn record(kind: &str, key: &str, position: Position, payload: &str) -> Record {
    Record {
        source: None,
        kind: kind.into(),
        key: key.into(),
        source_ts_ms: position as i64 * 10,
        ingest_ts_ms: 0,
        quality: None,
        position,
        payload: payload.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(1);

    fn dir() -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "hashi-fake-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn missing_credential_fails_check() {
        let source = FakeSource::with_credential_file("opc", vec![], &dir(), "secret").unwrap();
        let report = source.check().unwrap();
        assert!(!report.ok);
    }

    #[test]
    fn matching_credential_file_passes_check() {
        let dir = dir();
        fs::write(dir.join("credential"), "secret\n").unwrap();
        let source = FakeSource::with_credential_file(
            "opc",
            vec![record("tag", "a", 1, "1")],
            &dir,
            "secret",
        )
        .unwrap();
        assert!(source.check().unwrap().ok);
        assert_eq!(source.discover().unwrap()[0].kind, "tag");
    }
}
