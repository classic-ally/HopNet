//! Storage-aware admission (spec §Storage-aware admission): a fetch is
//! admitted only when the destination filesystem's free space, minus the
//! expected sizes of already-inflight writes, stays above the reserve floor.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::{IngressError, Result};

/// Free-space source. Faked in tests; statvfs in production.
pub trait FreeSpaceProbe: Send + Sync + 'static {
    fn free_bytes(&self, path: &Path) -> Result<u64>;
}

/// `statvfs`-backed probe (available bytes for unprivileged users).
pub struct StatvfsProbe;

impl FreeSpaceProbe for StatvfsProbe {
    fn free_bytes(&self, path: &Path) -> Result<u64> {
        use std::os::unix::ffi::OsStrExt;
        let c_path = std::ffi::CString::new(path.as_os_str().as_bytes())
            .map_err(|e| IngressError::Invariant(format!("statvfs path: {e}")))?;
        let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
        let rc = unsafe { libc::statvfs(c_path.as_ptr(), &mut stat) };
        if rc != 0 {
            let err = std::io::Error::last_os_error();
            // ENOENT/ENOTDIR on the blob root means the mount is gone —
            // an unmount rips the whole directory away. That is a
            // transport outage (pause class), not an invariant breach.
            return Err(match err.raw_os_error() {
                Some(libc::ENOENT) | Some(libc::ENOTDIR) => IngressError::StorageUnavailable(
                    format!("statvfs({}): {err}", path.display()),
                ),
                _ => IngressError::Invariant(format!(
                    "statvfs({}) failed: {err}",
                    path.display()
                )),
            });
        }
        Ok(stat.f_bavail as u64 * stat.f_frsize as u64)
    }
}

/// Running total of inflight expected bytes.
#[derive(Default)]
pub struct InflightBytes(AtomicU64);

impl InflightBytes {
    pub fn register(&self, bytes: u64) {
        self.0.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn unregister(&self, bytes: u64) {
        self.0.fetch_sub(bytes, Ordering::Relaxed);
    }

    pub fn total(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// Admission check: can `expected` more bytes start writing under `root`?
pub fn admit(
    probe: &dyn FreeSpaceProbe,
    root: &Path,
    inflight: &InflightBytes,
    expected: u64,
    reserve_floor: u64,
) -> Result<bool> {
    let free = probe.free_bytes(root)?;
    Ok(free
        .saturating_sub(inflight.total())
        .saturating_sub(expected)
        > reserve_floor)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fixed(u64);
    impl FreeSpaceProbe for Fixed {
        fn free_bytes(&self, _: &Path) -> Result<u64> {
            Ok(self.0)
        }
    }

    // Impact: the reserve floor is what keeps the daemon from filling the
    // storage root to the brim (and re-creating the 1005 failure mode on
    // the destination side).
    // Should: admit only when free minus inflight minus expected clears the floor.
    #[test]
    fn admission_math() {
        let inflight = InflightBytes::default();
        let probe = Fixed(100);
        let root = Path::new("/x");
        assert!(admit(&probe, root, &inflight, 10, 50).unwrap()); // 100-10 > 50
        assert!(!admit(&probe, root, &inflight, 60, 50).unwrap()); // 100-60 = 40 < 50
        inflight.register(30);
        assert!(!admit(&probe, root, &inflight, 25, 50).unwrap()); // 100-30-25 = 45 < 50
        inflight.unregister(30);
        assert!(admit(&probe, root, &inflight, 25, 50).unwrap());
    }
}
