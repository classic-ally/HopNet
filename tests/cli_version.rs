//! CLI contract for the RFC-021 upgrade machinery: `hopnet --version`
//! prints the compile-time version and exits cleanly. Both the NixOS
//! module's newest-wins seeding and the nix provider's staged-bytes
//! verification exec this — it must answer without booting a node.
//!
//! Run explicitly (not part of `--lib --bins`):
//! `cargo test -p hopnet --test cli_version --features skip-frontend`

// Should: print the compile-time crate version on --version and -V and
// exit 0 without starting a node.
// Should not: reflect the test-mode override env vars — the flag
// verifies an artifact's bytes, not a running process's claims.
// Impact: the upgrade provider refuses to attest or activate staged
// bytes whose --version output disagrees with the release tag; a lying
// or node-booting --version would break staging on every deployment.
#[test]
fn version_flag_prints_compile_time_version_and_exits() {
    for flag in ["--version", "-V"] {
        let out = std::process::Command::new(env!("CARGO_BIN_EXE_hopnet"))
            .arg(flag)
            .env("HOPNET_TEST_MODE", "1")
            .env("HOPNET_UPGRADE_VERSION_OVERRIDE", "2031.4.2")
            .output()
            .expect("spawn hopnet");
        assert!(out.status.success(), "{flag} must exit 0");
        assert_eq!(
            String::from_utf8_lossy(&out.stdout).trim(),
            env!("CARGO_PKG_VERSION"),
            "{flag} must print the compile-time version, overrides ignored"
        );
    }
}
