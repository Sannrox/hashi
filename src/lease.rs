//! Pluggable instance lease. The lock store decides who may write; checkpoint
//! bytes stay in Hashi. Default is a local file slot; PostgreSQL and Kubernetes
//! Lease sit behind features so a single-box site has no extra runtime.

use std::cell::Cell;
use std::hash::{BuildHasher, Hasher};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use fencing_lease::{FencingToken, FileStore, HolderId, Lease, LeaseName, LeaseStore, unix_now_ms};

use crate::Error;
use crate::config::{LeaseBackend, LeaseConfig};

/// Shared lock store. Backends serialize each name independently.
pub type SharedLeaseStore = Arc<dyn LeaseStore + Send + Sync>;

/// Default hold so a crashed node is taken over without waiting on an operator.
pub const DEFAULT_TTL: Duration = Duration::from_millis(15_000);

thread_local! {
    static TTL_OVERRIDE: Cell<Option<Duration>> = const { Cell::new(None) };
}

/// Run `f` with a shorter hold, so the conformance kit can take over without
/// waiting on [`DEFAULT_TTL`]. Nested calls restore the previous override.
pub(crate) fn with_ttl_override<R>(ttl: Duration, f: impl FnOnce() -> R) -> R {
    TTL_OVERRIDE.with(|slot| {
        let previous = slot.replace(Some(ttl));
        let result = f();
        slot.set(previous);
        result
    })
}

pub(crate) fn hold_ttl() -> Duration {
    TTL_OVERRIDE.with(|slot| slot.get()).unwrap_or(DEFAULT_TTL)
}

/// Held exclusive lease for one connector instance.
///
/// Dropping it does not release the slot: a kill or a pause leaves the TTL to
/// expire so a standby can take over. Call [`InstanceLease::release`] on a
/// clean stop.
pub struct InstanceLease {
    token: u64,
    inner: Option<HeldLease>,
}

impl std::fmt::Debug for InstanceLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InstanceLease")
            .field("token", &self.token)
            .field("expires_at_ms", &self.expires_at_ms())
            .finish()
    }
}

struct HeldLease {
    store: SharedLeaseStore,
    lease: Lease,
    ttl: Duration,
}

impl InstanceLease {
    fn held(store: SharedLeaseStore, lease: Lease, ttl: Duration) -> Self {
        Self {
            token: lease.token.get(),
            inner: Some(HeldLease { store, lease, ttl }),
        }
    }

    /// In-memory stores and tests that do not coordinate across processes.
    pub fn unfenced(token: u64) -> Self {
        Self { token, inner: None }
    }

    pub fn token(&self) -> u64 {
        self.token
    }

    pub fn expires_at_ms(&self) -> Option<u64> {
        self.inner.as_ref().map(|held| held.lease.expires_at_ms)
    }

    pub fn renew(&mut self) -> Result<(), Error> {
        let Some(held) = &mut self.inner else {
            return Ok(());
        };
        let now = unix_now_ms();
        let remaining = held.lease.expires_at_ms.saturating_sub(now);
        if remaining > (held.ttl.as_millis() as u64) / 2 {
            return Ok(());
        }
        held.lease = held
            .store
            .renew(&held.lease, held.ttl, now)
            .map_err(lease_err)?;
        self.token = held.lease.token.get();
        Ok(())
    }

    pub fn release(mut self) -> Result<(), Error> {
        self.release_inner()
    }

    fn release_inner(&mut self) -> Result<(), Error> {
        if let Some(held) = self.inner.take() {
            match held.store.release(&held.lease) {
                Ok(()) => {}
                // Already expired or taken over: the slot is not ours.
                Err(fencing_lease::Error::FenceLost { .. }) => {}
                Err(error) => return Err(lease_err(error)),
            }
        }
        Ok(())
    }

    /// Same holder, empty or expired slot: take it again. Used by directory
    /// holds after a TTL lapse with no successor.
    fn reacquire(&mut self) -> Result<(), Error> {
        let Some(held) = &mut self.inner else {
            return Ok(());
        };
        held.lease = held
            .store
            .acquire(
                &held.lease.name,
                &held.lease.holder,
                held.ttl,
                unix_now_ms(),
            )
            .map_err(lease_err)?;
        self.token = held.lease.token.get();
        Ok(())
    }
}

/// Releases on drop. Sink and buffer directories use this so a clean stop
/// frees the path immediately, while a kill still waits for the TTL.
pub struct DropRelease(Mutex<Option<InstanceLease>>);

impl DropRelease {
    pub fn new(lease: InstanceLease) -> Self {
        Self(Mutex::new(Some(lease)))
    }

    pub fn renew(&self) -> Result<(), Error> {
        let mut inner = self.0.lock().unwrap_or_else(|poison| poison.into_inner());
        let Some(lease) = inner.as_mut() else {
            return Ok(());
        };
        match lease.renew() {
            Ok(()) => Ok(()),
            Err(error) if is_fence_lost(&error) => lease.reacquire(),
            Err(error) => Err(error),
        }
    }
}

impl Drop for DropRelease {
    fn drop(&mut self) {
        let lease = self
            .0
            .get_mut()
            .unwrap_or_else(|poison| poison.into_inner())
            .take();
        if let Some(lease) = lease {
            let _ = lease.release();
        }
    }
}

/// Build the configured backend. File is the default and needs no extras.
pub fn open_store(config: &LeaseConfig) -> Result<SharedLeaseStore, Error> {
    fencing_lease::require_auto_promote(config.members(), true, false).map_err(lease_err)?;
    match config.backend {
        LeaseBackend::File => {
            let dir = config.dir.clone().ok_or_else(|| {
                Error::new("invalid config: lease.dir: must not be empty for the file backend")
            })?;
            Ok(Arc::new(FileStore::new(dir).map_err(lease_err)?))
        }
        LeaseBackend::Postgres => open_postgres(config),
        LeaseBackend::Kubernetes => open_kubernetes(config),
    }
}

#[cfg(feature = "lease-postgres")]
fn open_postgres(config: &LeaseConfig) -> Result<SharedLeaseStore, Error> {
    let path = config.credential_file.as_ref().ok_or_else(|| {
        Error::new("invalid config: lease.credential_file: required for the postgres backend")
    })?;
    let url = std::fs::read_to_string(path)
        .map_err(|_| Error::new(format!("lease credential {} is unreadable", path.display())))?;
    let url = url.trim();
    if url.is_empty() {
        return Err(Error::new(format!(
            "lease credential {} is empty",
            path.display()
        )));
    }
    Ok(Arc::new(
        fencing_lease::PostgresStore::from_url(url).map_err(lease_err)?,
    ))
}

#[cfg(not(feature = "lease-postgres"))]
fn open_postgres(_config: &LeaseConfig) -> Result<SharedLeaseStore, Error> {
    Err(Error::new(
        "invalid config: lease.backend: postgres needs the lease-postgres feature",
    ))
}

#[cfg(feature = "lease-kubernetes")]
fn open_kubernetes(config: &LeaseConfig) -> Result<SharedLeaseStore, Error> {
    let namespace = config
        .namespace
        .as_deref()
        .filter(|value| !value.is_empty());
    let namespace = namespace.ok_or_else(|| {
        Error::new("invalid config: lease.namespace: required for the kubernetes backend")
    })?;
    let api = config.api.as_deref().filter(|value| !value.is_empty());
    let api = api.ok_or_else(|| {
        Error::new("invalid config: lease.api: required for the kubernetes backend")
    })?;
    let path = config.credential_file.as_ref().ok_or_else(|| {
        Error::new("invalid config: lease.credential_file: required for the kubernetes backend")
    })?;
    let token = std::fs::read_to_string(path)
        .map_err(|_| Error::new(format!("lease credential {} is unreadable", path.display())))?;
    let token = token.trim();
    if token.is_empty() {
        return Err(Error::new(format!(
            "lease credential {} is empty",
            path.display()
        )));
    }
    Ok(Arc::new(fencing_lease::KubernetesStore::http(
        namespace, api, token,
    )))
}

#[cfg(not(feature = "lease-kubernetes"))]
fn open_kubernetes(_config: &LeaseConfig) -> Result<SharedLeaseStore, Error> {
    Err(Error::new(
        "invalid config: lease.backend: kubernetes needs the lease-kubernetes feature",
    ))
}

pub(crate) fn acquire(
    store: &SharedLeaseStore,
    connector: &str,
    instance: &str,
    holder: &HolderId,
    ttl: Duration,
) -> Result<InstanceLease, Error> {
    let name = lease_name(connector, instance)?;
    match store.acquire(&name, holder, ttl, unix_now_ms()) {
        Ok(lease) => Ok(InstanceLease::held(store.clone(), lease, ttl)),
        Err(error) => Err(lease_err(error)),
    }
}

/// Exclusive hold on a directory for as long as the returned guard lives.
pub(crate) fn lock_dir(
    dir: &Path,
    name: &str,
    what: &str,
    ttl: Duration,
) -> Result<DropRelease, Error> {
    hold(
        dir,
        name,
        ttl,
        format!(
            "{what} directory {} is in use by another instance",
            dir.display()
        ),
    )
}

/// Family spool hold. Dropping the guard releases; a pause keeps it until TTL.
pub(crate) fn hold(
    dir: &Path,
    name: &str,
    ttl: Duration,
    contended: impl Into<String>,
) -> Result<DropRelease, Error> {
    std::fs::create_dir_all(dir)?;
    let store: SharedLeaseStore = Arc::new(FileStore::new(dir).map_err(lease_err)?);
    let lease_name = LeaseName::new(name).map_err(lease_err)?;
    let holder = next_holder()?;
    match store.acquire(&lease_name, &holder, ttl, unix_now_ms()) {
        Ok(lease) => Ok(DropRelease::new(InstanceLease::held(store, lease, ttl))),
        Err(fencing_lease::Error::Contended { .. }) => Err(Error::new(contended)),
        Err(error) => Err(lease_err(error)),
    }
}

fn process_nonce() -> u64 {
    static NONCE: OnceLock<u64> = OnceLock::new();
    *NONCE.get_or_init(|| {
        let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
        hasher.write_u32(std::process::id());
        hasher.finish()
    })
}

pub(crate) fn next_holder() -> Result<HolderId, Error> {
    static SEQ: AtomicU64 = AtomicU64::new(1);
    let id = format!(
        "p{}-r{:016x}-h{}",
        std::process::id(),
        process_nonce(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    );
    HolderId::new(id).map_err(lease_err)
}

pub(crate) fn is_fence_lost(error: &Error) -> bool {
    error.message.contains("stale writer refused")
}

pub(crate) fn lease_name(connector: &str, instance: &str) -> Result<LeaseName, Error> {
    let raw = format!("{connector}.{instance}");
    if raw.len() <= 200 && LeaseName::new(&raw).is_ok() {
        return LeaseName::new(raw).map_err(lease_err);
    }
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(connector.as_bytes());
    hasher.update([0]);
    hasher.update(instance.as_bytes());
    LeaseName::new(format!("{:x}", hasher.finalize())).map_err(lease_err)
}

pub(crate) fn lease_err(error: fencing_lease::Error) -> Error {
    match error {
        fencing_lease::Error::Contended { .. } => {
            Error::new(format!("connector instance already running: {error}"))
        }
        fencing_lease::Error::FenceLost { .. } => {
            Error::new(format!("stale writer refused: {error}"))
        }
        other => Error::new(other.to_string()),
    }
}

pub(crate) fn fencing_token(raw: u64) -> FencingToken {
    FencingToken::from_raw(raw)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fencing_lease::MemoryStore;

    #[test]
    fn a_second_holder_is_contended_until_release() {
        let store: SharedLeaseStore = Arc::new(MemoryStore::new());
        let a = HolderId::new("a").unwrap();
        let b = HolderId::new("b").unwrap();
        let first = acquire(&store, "fake", "line-a", &a, Duration::from_secs(5)).unwrap();
        let err = acquire(&store, "fake", "line-a", &b, Duration::from_secs(5)).unwrap_err();
        assert!(err.message.contains("already running"), "{}", err.message);
        first.release().unwrap();
        acquire(&store, "fake", "line-a", &b, Duration::from_secs(5)).unwrap();
    }

    #[test]
    fn a_paused_holder_is_taken_over_after_the_ttl() {
        let store: SharedLeaseStore = Arc::new(MemoryStore::new());
        let a = HolderId::new("a").unwrap();
        let b = HolderId::new("b").unwrap();
        let first = acquire(&store, "fake", "line-a", &a, Duration::from_millis(1)).unwrap();
        std::thread::sleep(Duration::from_millis(5));
        let second = acquire(&store, "fake", "line-a", &b, Duration::from_millis(50)).unwrap();
        assert!(second.token() > first.token());
        let err = acquire(&store, "fake", "line-a", &a, Duration::from_millis(50)).unwrap_err();
        assert!(err.message.contains("already running"), "{}", err.message);
    }

    #[test]
    fn holders_from_the_same_process_are_distinct() {
        let a = next_holder().unwrap();
        let b = next_holder().unwrap();
        assert_ne!(a.as_str(), b.as_str());
        assert!(a.as_str().contains("-r"), "{}", a.as_str());
    }

    #[test]
    fn directory_hold_renew_keeps_the_slot_past_the_ttl() {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let dir = std::env::temp_dir().join(format!(
            "hashi-lease-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let ttl = Duration::from_millis(500);
        let first = hold(&dir, "spool", ttl, "contended").unwrap();
        std::thread::sleep(Duration::from_millis(300));
        first.renew().unwrap();
        std::thread::sleep(Duration::from_millis(400));
        let err = match hold(&dir, "spool", ttl, "contended") {
            Err(error) => error,
            Ok(_) => panic!("second holder acquired while the first still renewed"),
        };
        assert!(err.message.contains("contended"), "{}", err.message);
        drop(first);
        hold(&dir, "spool", ttl, "contended").unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }
}
