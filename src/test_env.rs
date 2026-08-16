//! One lock for every test that touches process-global environment.
//!
//! Cargo runs `#[test]` functions on a thread pool inside ONE process, and
//! this crate is edition 2024, where `std::env::set_var` is `unsafe`
//! precisely because a concurrent `getenv` on another thread is undefined
//! behaviour — not merely flaky.
//!
//! Three variables here are read by production code paths that tests drive:
//!
//! - `XDG_DATA_HOME` resolves `get_database_path()`, and from it the
//!   rollback marker, the retained-epoch file and the seal artifact.
//! - `HOPNET_UPGRADE_VERSION_OVERRIDE` feeds `effective_running_code()`,
//!   which the boot version gate, the status view's `awaiting_upgrade`, the
//!   peer handshake and the upgrade view all read.
//! - `HOPNET_UPGRADE_STAGED_OVERRIDE` feeds `effective_staged_code()`.
//!
//! They share ONE lock rather than getting one each, because the reads
//! interleave: `get_regenesis_status` consults the data dir and the running
//! code in the same handler, so per-variable locks would not order it
//! against a test mutating the other.
//!
//! What went wrong before this existed: a lock covered only the three
//! boundary e2es that set `XDG_DATA_HOME`, and none of them restored it.
//! An unlocked route test then resolved `sealed_path()` into a live e2e's
//! directory — returning 202 where it asserted 409, and writing a real
//! rollback marker into that e2e's data dir, which the e2e's own boot path
//! dispatches on. Meanwhile a version-override test left
//! `HOPNET_UPGRADE_VERSION_OVERRIDE` set at a far-future version while
//! sibling tests asserted on `awaiting_upgrade` and `newer_than_running`,
//! both of which invert under it. Those passed only because the e2es
//! `remove_dir_all` their directory and leave the variable pointing at a
//! path that no longer exists.

/// Variables restored on drop. Extend this rather than reaching for
/// `set_var` directly in a test.
const GUARDED: &[&str] = &[
    "XDG_DATA_HOME",
    "HOPNET_UPGRADE_VERSION_OVERRIDE",
    "HOPNET_UPGRADE_STAGED_OVERRIDE",
    // The disposable-node seams (`crate::paths`). They resolve against the
    // same directory tree as `XDG_DATA_HOME`, so they share its lock.
    "HOPNET_EPHEMERAL_DB",
    "HOPNET_EPHEMERAL_ROOT",
    "HOPNET_DATA_DIR",
    "HOPNET_FRAGMENTS_DIR",
    // The RFC-021 nix-provider deployment contract (nix_provider.rs).
    "HOPNET_UPGRADE_PROVIDER",
    "HOPNET_UPGRADE_NIX_BIN",
    "HOPNET_UPGRADE_PROFILE",
    "HOPNET_UPGRADE_STAGE_DIR",
    "HOPNET_UPGRADE_FLAKE_REF",
    "HOPNET_UPGRADE_AUTO_STAGE",
    "HOPNET_UPGRADE_AUTO_ACTIVATE",
    // The RFC-023 S3 min-client raise seam (client_compat.rs).
    "HOPNET_MIN_CLIENT_OVERRIDE",
];

static PROCESS_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Holds the lock and restores every guarded variable to its prior value
/// when dropped — including on a panicking test, so one failure cannot
/// leave the rest of the run reading a deleted directory.
pub struct EnvGuard {
    _lock: std::sync::MutexGuard<'static, ()>,
    saved: Vec<(&'static str, Option<std::ffi::OsString>)>,
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        for (key, prior) in &self.saved {
            // Sound: the lock is still held for the rest of this scope, so
            // no other test thread is reading or writing these.
            unsafe {
                match prior {
                    Some(value) => std::env::set_var(key, value),
                    None => std::env::remove_var(key),
                }
            }
        }
    }
}

/// Take the process-environment lock. Poisoning is ignored: a panicking
/// test has already restored its variables via `Drop`, so the mutex is
/// only protecting ordering, not an invariant that can be left broken.
pub fn lock_env() -> EnvGuard {
    let lock = PROCESS_ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let saved = GUARDED
        .iter()
        .map(|&key| (key, std::env::var_os(key)))
        .collect();
    EnvGuard { _lock: lock, saved }
}

/// Set a guarded variable while the lock is held. Panics if `key` is not
/// in `GUARDED`, so a variable can never be set without being restored.
pub fn set(_guard: &EnvGuard, key: &str, value: impl AsRef<std::ffi::OsStr>) {
    assert_guarded(key);
    unsafe { std::env::set_var(key, value) };
}

/// Unset a guarded variable while the lock is held — genuinely absent,
/// which is not the same as set-to-empty for the override seams.
pub fn remove(_guard: &EnvGuard, key: &str) {
    assert_guarded(key);
    unsafe { std::env::remove_var(key) };
}

fn assert_guarded(key: &str) {
    assert!(
        GUARDED.contains(&key),
        "{key} must be listed in test_env::GUARDED so it is restored on drop"
    );
}
