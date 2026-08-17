//! Where a node's on-disk state lives.
//!
//! Every location a node writes to resolves through this module. Before it
//! existed, each subsystem re-derived its own path from
//! `db::shared::get_database_path()` — several by taking `.parent()` of the
//! database file — so `HOPNET_EPHEMERAL_DB` could switch the connection pool
//! to memory while photos sidecars, TLS material and the fragment store kept
//! writing to the real data directory. An "ephemeral" node leaked everything
//! EXCEPT its database, into the same directory a live node was using.
//!
//! Two rules keep that from coming back:
//!
//! 1. **Independent seams, shared default.** Nesting `tls/` and `fragments/`
//!    under one ephemeral root is a default, not a derivation. Each location
//!    has its own resolver and its own override, so the fragment store can be
//!    moved without moving the database.
//! 2. **Override, never ambient rewrite.** Ephemeral mode installs a
//!    [`NodePaths`] into a `OnceLock`; resolvers consult it and otherwise fall
//!    through to the environment unchanged. Rewriting `XDG_DATA_HOME` with
//!    `set_var` is not an option: `main` is `async`, so the runtime is already
//!    up, and a concurrent `getenv` on another thread is undefined behaviour.
//!    Caching the resolved value is not an option either — `crate::test_env`
//!    exists because tests set and restore `XDG_DATA_HOME` per test and depend
//!    on these functions re-resolving on every call.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

/// Values that turn `HOPNET_EPHEMERAL_DB` OFF. Anything else turns it on, so
/// the historical "presence enables it" contract still holds for `=1`, `=true`
/// and friends — `=0` used to enable it too, which is the bug this fixes.
const FALSEY: &[&str] = &["", "0", "false", "no", "off"];

/// Whether this process was asked for a disposable node.
pub fn ephemeral_requested() -> bool {
    match std::env::var("HOPNET_EPHEMERAL_DB") {
        Ok(v) => !FALSEY.contains(&v.trim().to_ascii_lowercase().as_str()),
        Err(_) => false,
    }
}

/// First 8 hex chars of blake3 of a canonicalized path.
pub fn hash_of_path(p: &Path) -> String {
    let canon = std::fs::canonicalize(p).unwrap_or_else(|_| p.to_path_buf());
    blake3::hash(canon.as_os_str().as_encoded_bytes()).to_hex()[..8].to_string()
}

/// Stable per-checkout hash — the isolation boundary between worktrees
/// sharing one machine. `HOPNET_ORCH_HASH` overrides (test/escape hatch).
///
/// `CARGO_MANIFEST_DIR` is the checkout root: this lib and the orchestrator
/// are the same cargo package, so both expand the macro to the same path and
/// one hash names container resources and ephemeral directories alike.
pub fn checkout_hash() -> &'static str {
    static HASH: OnceLock<String> = OnceLock::new();
    HASH.get_or_init(|| {
        std::env::var("HOPNET_ORCH_HASH")
            .unwrap_or_else(|_| hash_of_path(Path::new(env!("CARGO_MANIFEST_DIR"))))
    })
}

/// Resolved locations for one node's state. Installed once by
/// [`init_ephemeral`]; absent for an ordinary durable node.
#[derive(Debug, Clone)]
pub struct NodePaths {
    /// Database, regenesis artifacts, photos sidecars.
    pub data_dir: PathBuf,
    /// Pinned-HTTPS identity material.
    pub tls_dir: PathBuf,
    /// Content-addressed blobs. Its own seam on purpose: fragments are
    /// measured in hundreds of megabytes and have no reason to follow the
    /// database around. `hopnet-storage` cannot call into this crate, so
    /// [`init_ephemeral`] pushes this value down to it and
    /// `hopnet_storage::fragstore::get_fragments_dir` stays authoritative for
    /// every reader — including the three that bypass `AppState`.
    pub fragments_dir: PathBuf,
}

static OVERRIDE: OnceLock<NodePaths> = OnceLock::new();

fn installed() -> Option<&'static NodePaths> {
    OVERRIDE.get()
}

/// `$XDG_DATA_HOME`, or the spec's fallback. Resolved on every call so tests
/// holding `crate::test_env::lock_env()` can move it between cases.
fn xdg_data_home() -> PathBuf {
    std::env::var_os("XDG_DATA_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
            PathBuf::from(home).join(".local/share")
        })
}

/// The node's own directory: database, regenesis artifacts, photos sidecars.
///
/// `HOPNET_DATA_DIR` moves it without touching `XDG_DATA_HOME`. That matters
/// because `XDG_DATA_HOME` steers three things across two processes — this
/// directory, the fragment-store fallback, and `hopnet-mount`'s staging
/// directory — so using it to put the database on a small fast disk would drag
/// in-flight upload bytes along. The database is small, randomly written and
/// gates consensus latency; the other two are bulk.
pub fn data_dir() -> PathBuf {
    if let Some(p) = installed() {
        return p.data_dir.clone();
    }
    if let Some(dir) = std::env::var_os("HOPNET_DATA_DIR") {
        return PathBuf::from(dir);
    }
    xdg_data_home().join("hopnet")
}

/// Pinned-HTTPS identity material (RFC-022). Ephemeral nodes get a fresh
/// SPKI per boot, which is the honest behaviour: a node calling itself
/// disposable should not quietly hold a stable pin that clients trust.
pub fn tls_dir() -> PathBuf {
    match installed() {
        Some(p) => p.tls_dir.clone(),
        None => data_dir().join("tls"),
    }
}

/// `<HOPNET_EPHEMERAL_ROOT | temp dir>/hopnet-<checkout>/<pid>`.
///
/// The parent is stable per checkout so one `rm -rf` reaps a worktree's
/// leftovers; the pid leaf keeps concurrent nodes in one checkout apart.
/// `HOPNET_EPHEMERAL_ROOT` exists so a machine can put this on real disk
/// without exporting `TMPDIR` for every other program (`std::env::temp_dir`
/// is `/tmp`, which is commonly tmpfs — fragments would land in RAM).
pub fn ephemeral_root() -> PathBuf {
    std::env::var_os("HOPNET_EPHEMERAL_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir)
        .join(format!("hopnet-{}", checkout_hash()))
        .join(std::process::id().to_string())
}

/// Name of the flock'd file that marks a live owner of an ephemeral tree.
const OWNER_FILE: &str = ".owner";

/// Removes the ephemeral tree on drop, and holds the lock that tells the next
/// node this tree is still in use.
///
/// Drop alone is not enough, and cannot be: a default-disposition SIGTERM and
/// `std::process::exit` (the restart-into-new-epoch path) both end the process
/// without unwinding. The lock is what makes cleanup reliable — the kernel
/// releases it however the process dies, so the next ephemeral node reaps the
/// tree in [`reap_dead_siblings`].
pub struct EphemeralGuard {
    root: PathBuf,
    /// Held open for the process lifetime; closing releases the flock.
    _owner: std::fs::File,
}

impl EphemeralGuard {
    pub fn root(&self) -> &Path {
        &self.root
    }
}

impl Drop for EphemeralGuard {
    fn drop(&mut self) {
        if let Err(e) = std::fs::remove_dir_all(&self.root) {
            tracing::warn!(root = %self.root.display(), "ephemeral cleanup failed: {e}");
        }
    }
}

/// Open `<dir>/.owner` and take the flock, proving this process owns the tree.
fn claim(dir: &Path) -> std::io::Result<std::fs::File> {
    use fs4::fs_std::FileExt;
    let file = std::fs::File::create(dir.join(OWNER_FILE))?;
    if !file.try_lock_exclusive()? {
        return Err(std::io::Error::other(format!(
            "{} is already claimed by a live node",
            dir.display()
        )));
    }
    Ok(file)
}

/// Name of the flock'd file that marks the live node owning a durable data dir.
const INSTANCE_LOCK_FILE: &str = ".instance-lock";

/// Held for the process lifetime; the kernel releases it however the process
/// dies, so a crash never wedges the next start.
pub struct InstanceLock {
    _file: std::fs::File,
}

/// Claim exclusive ownership of `data_dir` for this node process.
///
/// Two nodes pointed at one durable data dir fail QUIETLY today: they share
/// the WAL database and the second silently loses the network bind. The
/// flock makes the collision explicit. `Ok(None)` means another live process
/// holds the dir — the caller decides how to exit (a supervised agent must
/// exit 0 so launchd's `SuccessfulExit = false` does not restart-loop it
/// against a Finder-launched copy).
pub fn try_claim_instance(data_dir: &Path) -> std::io::Result<Option<InstanceLock>> {
    use fs4::fs_std::FileExt;
    std::fs::create_dir_all(data_dir)?;
    let file = std::fs::File::create(data_dir.join(INSTANCE_LOCK_FILE))?;
    if !file.try_lock_exclusive()? {
        return Ok(None);
    }
    Ok(Some(InstanceLock { _file: file }))
}

/// Delete sibling trees in `parent` whose owning process is gone.
///
/// Liveness is the flock, not the pid: pids are recycled, and a recycled one
/// would make us delete a running node's database out from under it.
///
/// Reaping requires POSITIVE proof of death — an owner file we can lock.
/// A directory with no owner file is spared, because a node that has just
/// created its root and not yet claimed it looks exactly like an abandoned
/// one, and several nodes do start at once (the mount stack test boots six).
/// The failure direction is therefore a lingering empty directory, never a
/// live node losing its database.
fn reap_dead_siblings(parent: &Path, mine: &Path) {
    use fs4::fs_std::FileExt;
    let Ok(entries) = std::fs::read_dir(parent) else {
        return;
    };
    for entry in entries.flatten() {
        let dir = entry.path();
        if dir == mine || !dir.is_dir() {
            continue;
        }
        let Ok(file) = std::fs::File::open(dir.join(OWNER_FILE)) else {
            continue;
        };
        if !matches!(file.try_lock_exclusive(), Ok(true)) {
            continue;
        }
        let _ = FileExt::unlock(&file);
        if let Err(e) = std::fs::remove_dir_all(&dir) {
            tracing::debug!(dir = %dir.display(), "stale ephemeral tree not reaped: {e}");
        }
    }
}

/// Create the ephemeral tree and route every location into it.
///
/// One call site on purpose: installing the paths without also pushing the
/// fragment store down to `hopnet-storage` would silently reintroduce the
/// split-brain this module exists to prevent. Must run before anything
/// resolves a path — in particular before the TLS listener reads
/// [`tls_dir`].
pub fn init_ephemeral() -> std::io::Result<EphemeralGuard> {
    let root = ephemeral_root();
    let paths = NodePaths {
        data_dir: root.clone(),
        tls_dir: root.join("tls"),
        fragments_dir: root.join("fragments"),
    };
    // Claim before anything else exists: until the owner file is locked this
    // tree is indistinguishable from an abandoned one to a sibling starting
    // concurrently, so keep that window as short as possible.
    std::fs::create_dir_all(&root)?;
    let owner = claim(&root)?;
    if let Some(parent) = root.parent() {
        reap_dead_siblings(parent, &root);
    }
    for dir in [&paths.tls_dir, &paths.fragments_dir] {
        std::fs::create_dir_all(dir)?;
    }
    hopnet_storage::fragstore::set_dir_override(paths.fragments_dir.clone());
    // Only ever called once, from server startup; a second call would mean a
    // second node in one process, which the pid-keyed root cannot represent.
    OVERRIDE
        .set(paths)
        .map_err(|_| std::io::Error::other("ephemeral paths already installed"))?;
    Ok(EphemeralGuard {
        root,
        _owner: owner,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // Impact: two nodes on one durable data dir fail quietly — shared WAL
    //         database, silently split network surface — so the lock is what
    //         stands between a supervised agent and a Finder-launched copy.
    // Should: grant the instance lock to the first claimant.
    // Should: refuse a second claim on the same data dir while the first
    //         lock is alive.
    // Should: allow re-claiming once the first lock is dropped.
    #[test]
    fn instance_lock_is_exclusive_per_data_dir() {
        let dir = tempfile::tempdir().unwrap();

        let first = try_claim_instance(dir.path()).unwrap();
        assert!(first.is_some());

        let second = try_claim_instance(dir.path()).unwrap();
        assert!(second.is_none());

        drop(first);
        let third = try_claim_instance(dir.path()).unwrap();
        assert!(third.is_some());
    }

    // Impact: the hash is the isolation boundary between checkouts — equal
    //         hashes for distinct paths would silently merge two worktrees'
    //         container namespaces and ephemeral directories.
    // Should: hash distinct directories to distinct values, and the same
    //         directory (directly or via symlink) to the same value.
    #[test]
    fn path_hash_distinguishes_and_canonicalizes() {
        let a = tempfile::tempdir().unwrap();
        let b = tempfile::tempdir().unwrap();
        assert_ne!(hash_of_path(a.path()), hash_of_path(b.path()));
        assert_eq!(hash_of_path(a.path()), hash_of_path(a.path()));

        let link = b.path().join("link");
        std::os::unix::fs::symlink(a.path(), &link).unwrap();
        assert_eq!(hash_of_path(&link), hash_of_path(a.path()));
    }

    // Impact: `=0` enabling ephemeral mode is how a node meant to be durable
    //         could come up disposable.
    // Should: treat explicit negatives and an empty value as off.
    // Should: keep the historical presence-enables contract for other values.
    #[test]
    fn ephemeral_flag_rejects_negatives() {
        let guard = crate::test_env::lock_env();
        for off in ["0", "false", "no", "off", "", "  OFF  "] {
            crate::test_env::set(&guard, "HOPNET_EPHEMERAL_DB", off);
            assert!(!ephemeral_requested(), "{off:?} should not enable");
        }
        for on in ["1", "true", "yes", "anything"] {
            crate::test_env::set(&guard, "HOPNET_EPHEMERAL_DB", on);
            assert!(ephemeral_requested(), "{on:?} should enable");
        }
        crate::test_env::remove(&guard, "HOPNET_EPHEMERAL_DB");
        assert!(!ephemeral_requested());
    }

    // Should: resolve the data directory under XDG_DATA_HOME when no
    //         ephemeral override is installed.
    // Should: keep re-resolving so a test moving XDG_DATA_HOME is seen.
    #[test]
    fn data_dir_follows_xdg_when_not_overridden() {
        let guard = crate::test_env::lock_env();
        crate::test_env::remove(&guard, "HOPNET_DATA_DIR");
        crate::test_env::set(&guard, "XDG_DATA_HOME", "/tmp/hopnet-paths-a");
        assert_eq!(data_dir(), PathBuf::from("/tmp/hopnet-paths-a/hopnet"));
        crate::test_env::set(&guard, "XDG_DATA_HOME", "/tmp/hopnet-paths-b");
        assert_eq!(data_dir(), PathBuf::from("/tmp/hopnet-paths-b/hopnet"));
        assert_eq!(tls_dir(), PathBuf::from("/tmp/hopnet-paths-b/hopnet/tls"));
    }

    // Impact: a validator whose database sits on spinning storage costs the
    //         whole mesh latency on every round it proposes (~146 ms per
    //         fsync vs ~0.55 ms on NVMe, measured). Splitting the small
    //         random writes onto fast storage while bulk blobs stay on the
    //         big pool is the fix, and it only works if the two locations
    //         resolve independently on the DURABLE path — not just under
    //         ephemeral mode, where the override already separates them.
    // Should: resolve the database under HOPNET_DATA_DIR and fragments under
    //         HOPNET_FRAGMENTS_DIR with no ephemeral override installed.
    // Should not: let either variable disturb the other's location.
    #[test]
    fn durable_database_and_fragments_resolve_independently() {
        let guard = crate::test_env::lock_env();
        crate::test_env::remove(&guard, "HOPNET_EPHEMERAL_DB");
        crate::test_env::set(&guard, "XDG_DATA_HOME", "/tmp/hopnet-split-xdg");
        crate::test_env::set(&guard, "HOPNET_DATA_DIR", "/tmp/hopnet-split-fast");
        crate::test_env::set(&guard, "HOPNET_FRAGMENTS_DIR", "/tmp/hopnet-split-bulk");

        assert_eq!(data_dir(), PathBuf::from("/tmp/hopnet-split-fast"));
        assert_eq!(
            crate::db::shared::get_database_path(),
            "/tmp/hopnet-split-fast/database.db"
        );
        assert_eq!(
            hopnet_storage::fragstore::get_fragments_dir().unwrap(),
            "/tmp/hopnet-split-bulk"
        );
        // TLS follows the database, not the blobs.
        assert_eq!(tls_dir(), PathBuf::from("/tmp/hopnet-split-fast/tls"));
    }

    // Should: prefer HOPNET_DATA_DIR over XDG_DATA_HOME.
    // Should: fall back to XDG_DATA_HOME when it is unset, so existing
    //         deployments that set only XDG_DATA_HOME are unaffected.
    #[test]
    fn data_dir_override_takes_precedence_over_xdg() {
        let guard = crate::test_env::lock_env();
        crate::test_env::set(&guard, "XDG_DATA_HOME", "/tmp/hopnet-prec-xdg");
        crate::test_env::set(&guard, "HOPNET_DATA_DIR", "/tmp/hopnet-prec-explicit");
        assert_eq!(data_dir(), PathBuf::from("/tmp/hopnet-prec-explicit"));

        crate::test_env::remove(&guard, "HOPNET_DATA_DIR");
        assert_eq!(data_dir(), PathBuf::from("/tmp/hopnet-prec-xdg/hopnet"));
    }

    // Impact: SIGTERM and `std::process::exit` both end the process without
    //         unwinding, so the Drop guard alone would leave a tree behind
    //         after almost every real shutdown. The flock is what makes the
    //         cleanup survive however the previous node died.
    // Should: delete a sibling tree whose owner is gone.
    // Should not: delete a sibling whose owner still holds the lock, nor the
    //             caller's own tree, nor one with no owner file yet — that
    //             is what a node between mkdir and claim looks like, and
    //             several nodes do start at once.
    #[test]
    fn reaper_takes_dead_trees_and_spares_live_ones() {
        use fs4::fs_std::FileExt;
        let parent = tempfile::tempdir().unwrap();

        let mine = parent.path().join("mine");
        let dead = parent.path().join("dead");
        let live = parent.path().join("live");
        let starting_up = parent.path().join("starting_up");
        for dir in [&mine, &dead, &live, &starting_up] {
            std::fs::create_dir_all(dir).unwrap();
        }

        // A tree whose owner exited: the file exists, nothing holds it.
        std::fs::File::create(dead.join(OWNER_FILE)).unwrap();
        // A tree whose owner is still running.
        let held = std::fs::File::create(live.join(OWNER_FILE)).unwrap();
        assert!(held.try_lock_exclusive().unwrap());
        // Our own, claimed the way init_ephemeral does.
        let _mine_owner = claim(&mine).unwrap();

        reap_dead_siblings(parent.path(), &mine);

        assert!(mine.exists(), "must not reap its own tree");
        assert!(live.exists(), "must not reap a tree with a live owner");
        assert!(!dead.exists(), "must reap a tree whose owner is gone");
        assert!(
            starting_up.exists(),
            "must not reap a tree that has not claimed itself yet"
        );
    }

    // Impact: two nodes from one checkout must not share a disposable tree;
    //         two checkouts must not share a parent to reap.
    // Should: key the root on the checkout hash and the process id.
    // Should: honour HOPNET_EPHEMERAL_ROOT so the tree can leave tmpfs.
    #[test]
    fn ephemeral_root_is_per_checkout_and_per_process() {
        let guard = crate::test_env::lock_env();
        crate::test_env::set(&guard, "HOPNET_EPHEMERAL_ROOT", "/var/tmp/hopnet-test-root");
        let root = ephemeral_root();
        assert!(root.starts_with("/var/tmp/hopnet-test-root"));
        assert_eq!(
            root.file_name().unwrap().to_str().unwrap(),
            std::process::id().to_string()
        );
        assert_eq!(
            root.parent()
                .unwrap()
                .file_name()
                .unwrap()
                .to_str()
                .unwrap(),
            format!("hopnet-{}", checkout_hash())
        );
    }
}
