//! Exclusive-run lock with crash detection (spec §Recovery Tier 1).
//!
//! The `drain.lock` file is pid-stamped: a starting daemon finding a lock
//! held by a dead process reclaims it and reports the start as UNCLEAN,
//! which triggers the Tier-1 refcount repair. A lock held by a live process
//! is a hard error — temp naming and the inflight set assume one process.

use std::io::Write as _;
use std::path::PathBuf;

use crate::error::{IngressError, Result};
use crate::paths::DataDir;

/// Held for the lifetime of a drain/daemon/cleanup run; file removed on drop.
pub(crate) struct DrainLock(PathBuf);

pub(crate) struct LockAcquired {
    pub lock: DrainLock,
    /// The lock was reclaimed from a dead process: the previous shutdown was
    /// unclean and refcounts may have drifted (Tier-1 repair required).
    pub unclean: bool,
}

impl DrainLock {
    pub(crate) fn acquire(data_dir: &DataDir) -> Result<LockAcquired> {
        let path = data_dir.root().join("drain.lock");
        match Self::try_create(&path) {
            Ok(lock) => Ok(LockAcquired {
                lock,
                unclean: false,
            }),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                let holder = std::fs::read_to_string(&path).unwrap_or_default();
                match holder.trim().parse::<i32>() {
                    Ok(pid) if pid_alive(pid) => Err(IngressError::Invariant(format!(
                        "another drain is running (pid {pid}): {} exists",
                        path.display()
                    ))),
                    // Dead pid, or an empty/unparseable file (pre-pid-stamp
                    // locks were empty, and a reader can race the stamp
                    // write): stale — reclaim. Two processes racing this
                    // reclaim is a known narrow window, acceptable for a
                    // single-LaunchAgent deployment.
                    _ => {
                        std::fs::remove_file(&path).map_err(|e| {
                            IngressError::Invariant(format!("stale lock removal: {e}"))
                        })?;
                        let lock = Self::try_create(&path)
                            .map_err(|e| IngressError::Invariant(format!("drain lock: {e}")))?;
                        Ok(LockAcquired {
                            lock,
                            unclean: true,
                        })
                    }
                }
            }
            Err(e) => Err(IngressError::Invariant(format!("drain lock: {e}"))),
        }
    }

    fn try_create(path: &std::path::Path) -> std::io::Result<Self> {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)?;
        // Single write: minimal torn-read window for a racing reader.
        file.write_all(std::process::id().to_string().as_bytes())?;
        Ok(Self(path.to_path_buf()))
    }
}

impl Drop for DrainLock {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// `kill(pid, 0)`: 0 or EPERM = alive; ESRCH = dead.
fn pid_alive(pid: i32) -> bool {
    if pid <= 0 {
        return false;
    }
    let rc = unsafe { libc::kill(pid, 0) };
    rc == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn data_dir() -> (tempfile::TempDir, DataDir) {
        let tmp = tempfile::tempdir().unwrap();
        let dd = DataDir::new(tmp.path());
        (tmp, dd)
    }

    // Impact: single-process exclusivity is what makes temp naming and the
    // inflight set sound — a second live daemon corrupts both.
    // Should: stamp our pid; refuse while the holder is alive.
    #[test]
    fn stamps_pid_and_refuses_live_holder() {
        let (_tmp, dd) = data_dir();
        let first = DrainLock::acquire(&dd).unwrap();
        assert!(!first.unclean);
        let stamped = std::fs::read_to_string(dd.root().join("drain.lock")).unwrap();
        assert_eq!(stamped, std::process::id().to_string());

        // Second acquire sees OUR live pid → hard error.
        let err = DrainLock::acquire(&dd);
        assert!(err.is_err());
        drop(first);
        assert!(!dd.root().join("drain.lock").exists(), "released on drop");
    }

    // Impact: a crashed LaunchAgent daemon must self-heal on restart —
    // refusing forever on its own stale lock strands the archive.
    // Should: reclaim dead-pid and empty (pre-pid-stamp) locks, reporting
    // unclean so Tier-1 repair runs.
    #[test]
    fn reclaims_dead_pid_and_empty_locks_as_unclean() {
        let (_tmp, dd) = data_dir();
        let path = dd.root().join("drain.lock");

        // A pid that has exited (short-lived reaped child).
        let mut child = std::process::Command::new("true").spawn().unwrap();
        let dead_pid = child.id();
        child.wait().unwrap();
        std::fs::write(&path, dead_pid.to_string()).unwrap();
        let a = DrainLock::acquire(&dd).unwrap();
        assert!(a.unclean, "dead-pid lock is stale");
        drop(a);

        std::fs::write(&path, "").unwrap();
        let b = DrainLock::acquire(&dd).unwrap();
        assert!(b.unclean, "empty (pre-pid-stamp) lock is stale");
    }
}
