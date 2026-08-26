//! CLI contract for the RFC-025 seeding clamp: `hopnet seed-guard
//! --candidate <ver>` decides whether the NixOS module may advance the
//! profile, against the on-disk markers, without booting a node.
//!
//! Run explicitly (not part of `--lib --bins`):
//! `cargo test -p hopnet --test cli_seed_guard --features skip-frontend`

use std::path::Path;
use std::process::Command;

fn guard(xdg: &Path, candidate: &str) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_hopnet"))
        .args(["seed-guard", "--candidate", candidate])
        .env("XDG_DATA_HOME", xdg)
        .env_remove("HOPNET_DATA_DIR")
        // The guard must be override-blind; set the lie to prove it.
        .env("HOPNET_TEST_MODE", "1")
        .env("HOPNET_UPGRADE_VERSION_OVERRIDE", "2031.4.2")
        .output()
        .expect("spawn hopnet seed-guard")
}

fn plant(xdg: &Path, name: &str, content: &str) {
    let dir = xdg.join("hopnet");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join(name), content).unwrap();
}

// Should: allow any candidate on a never-joined install (newest-wins);
// hold a candidate beyond the agreement once joined; let the
// awaiting-upgrade target raise the ceiling; hold on a garbage marker;
// exit 2 on a garbage candidate — all without booting a node and blind
// to the version-override env.
// Impact: this is the requirement's dividing line — install-without-
// joining keeps loading the latest version, joining pins it.
#[test]
fn seed_guard_clamps_to_the_markers() {
    // Never joined: exit 0.
    let fresh = tempfile::tempdir().unwrap();
    let out = guard(fresh.path(), "2030.1.1");
    assert!(out.status.success(), "never-joined must allow");

    // Joined at 2026.8.6: at/below allows, above holds with exit 3.
    let joined = tempfile::tempdir().unwrap();
    plant(joined.path(), "agreed-version", "2026.8.6");
    assert!(guard(joined.path(), "2026.8.6").status.success());
    assert!(guard(joined.path(), "2026.8.1").status.success());
    let held = guard(joined.path(), "2026.9.0");
    assert_eq!(held.status.code(), Some(3));
    assert!(
        String::from_utf8_lossy(&held.stderr).contains("HELD"),
        "the hold must be named in the journal"
    );

    // Parked awaiting 2026.9.0: the target seeds, beyond it holds.
    plant(joined.path(), "awaiting-upgrade", "2026.9.0");
    assert!(guard(joined.path(), "2026.9.0").status.success());
    assert_eq!(guard(joined.path(), "2026.10.0").status.code(), Some(3));

    // Garbage marker: conservative hold.
    let corrupt = tempfile::tempdir().unwrap();
    plant(corrupt.path(), "agreed-version", "not a version");
    assert_eq!(guard(corrupt.path(), "2026.8.6").status.code(), Some(3));

    // Garbage candidate: usage.
    assert_eq!(guard(fresh.path(), "banana").status.code(), Some(2));
}
