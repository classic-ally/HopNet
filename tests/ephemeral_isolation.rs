//! `HOPNET_EPHEMERAL_DB` must make a node disposable in every location it
//! writes to — not just its connection pool.
//!
//! The flag used to switch only the main r2d2 pool to an in-memory database.
//! Everything else re-derived its path from the on-disk data directory, so an
//! "ephemeral" node still wrote TLS material, photos sidecars and fragments
//! into the real `$XDG_DATA_HOME/hopnet` — colliding with any durable node or
//! mount daemon on the same machine. This test is the guard on that.
//!
//! Run explicitly (not part of `--lib --bins`):
//! `cargo test -p hopnet --test ephemeral_isolation --features skip-frontend`

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

struct NodeGuard(Child);

impl Drop for NodeGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Wait for the node to reach the point where it has opened its database.
/// Polling the file rather than an HTTP route on purpose: the plaintext
/// listener binds well before the pool is built, so a live port would not
/// prove the storage decision had been made yet.
fn wait_for_db(db: &Path) -> bool {
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if db.exists() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    false
}

/// Everything under `dir`, relative, for a legible assertion message.
fn entries(dir: &Path) -> Vec<PathBuf> {
    let Ok(read) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    read.filter_map(|e| e.ok().map(|e| e.path())).collect()
}

// Impact: this is the trust boundary between a throwaway node and a real
//         user's data. A regression here does not fail loudly — it quietly
//         mixes test state into a live node's directory, which is how the
//         original defect survived unnoticed.
// Should: place the database, TLS material and fragment store inside the
//         disposable per-process tree named by HOPNET_EPHEMERAL_ROOT.
// Should not: create anything under XDG_DATA_HOME.
#[test]
fn ephemeral_node_writes_nothing_to_the_real_data_dir() {
    let xdg = tempfile::tempdir().unwrap();
    let eph = tempfile::tempdir().unwrap();
    let port = free_port();

    let child = Command::new(env!("CARGO_BIN_EXE_hopnet"))
        .env("HOPNET_EPHEMERAL_DB", "1")
        .env("HOPNET_EPHEMERAL_ROOT", eph.path())
        .env("XDG_DATA_HOME", xdg.path())
        .env("HOPNET_HTTP_PORT", port.to_string())
        .env("HOPNET_TEST_MODE", "1")
        // Loopback-only harness: a wildcard TLS bind would fight the real
        // node's port on a developer machine.
        .env("HOPNET_DISABLE_TLS", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn hopnet");
    let pid = child.id();
    let _guard = NodeGuard(child);

    let root = eph
        .path()
        .join(format!("hopnet-{}", hopnet::paths::checkout_hash()))
        .join(pid.to_string());

    assert!(
        wait_for_db(&root.join("database.db")),
        "database belongs in the disposable tree, found: {:?}",
        entries(&root)
    );
    assert!(root.join("tls").is_dir(), "TLS dir belongs in the tree");
    assert!(
        root.join("fragments").is_dir(),
        "fragment store belongs in the tree"
    );

    let leaked = xdg.path().join("hopnet");
    assert!(
        !leaked.exists(),
        "ephemeral node wrote into the real data dir: {:?}",
        entries(&leaked)
    );
}

// Impact: `HOPNET_EPHEMERAL_DB=0` used to enable ephemeral mode, so a node
//         meant to be durable could come up disposable and lose its state on
//         the next boot.
// Should: treat an explicit negative as a request for a durable node.
#[test]
fn negative_flag_value_leaves_the_node_durable() {
    let xdg = tempfile::tempdir().unwrap();
    let eph = tempfile::tempdir().unwrap();
    let port = free_port();

    let child = Command::new(env!("CARGO_BIN_EXE_hopnet"))
        .env("HOPNET_EPHEMERAL_DB", "0")
        .env("HOPNET_EPHEMERAL_ROOT", eph.path())
        .env("XDG_DATA_HOME", xdg.path())
        .env("HOPNET_HTTP_PORT", port.to_string())
        .env("HOPNET_TEST_MODE", "1")
        .env("HOPNET_DISABLE_TLS", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn hopnet");
    let _guard = NodeGuard(child);

    assert!(
        wait_for_db(&xdg.path().join("hopnet/database.db")),
        "=0 must mean durable; data dir holds: {:?}",
        entries(&xdg.path().join("hopnet"))
    );
    assert!(
        entries(eph.path()).is_empty(),
        "=0 must not create a disposable tree"
    );
}
