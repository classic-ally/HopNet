//! Node version identity (RFC-019 S3).
//!
//! The pure CalVer code helpers live in `hopnet_common::version`
//! (RFC-022 — clients need them too) and are re-exported here so the
//! node's `crate::version::` call sites read naturally. This module
//! keeps what is node-only: the compile-time identity readers and the
//! test-mode override seams.
//!
//! Legacy non-CalVer tags (v0.1.0-rc.*) cannot be encoded and therefore
//! cannot be attested; they surface only in the upgrade advisory's
//! available list, as strings.

pub use hopnet_common::version::{code_is_valid, format_code, parse_code};

/// The node's version string, verbatim from Cargo.toml.
pub fn running_version_str() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// The node's version code. Panics if Cargo.toml's version is not CalVer
/// — a deliberate boot invariant now that the scheme is adopted: a
/// non-CalVer version could never satisfy an epoch boot gate or attest
/// itself, so failing at first use beats limping.
///
/// This is the raw compile-time identity. Runtime consumers of "what
/// version is this node" (attestation, readiness views, the epoch boot
/// gate, restart derivation, the peer handshake) should use
/// [`effective_running_code`], which honours the test-mode override.
pub fn running_version_code() -> u32 {
    parse_code(running_version_str()).unwrap_or_else(|| {
        panic!(
            "Cargo.toml version {:?} is not CalVer YYYY.M.N — the version \
             scheme is mandatory (RFC-019 S3)",
            running_version_str()
        )
    })
}

/// Test-mode gate for the override seams, mirroring the AppState
/// construction (src/main.rs): debug builds and HOPNET_TEST_MODE. The
/// overrides exist so orchestrator scenarios can make a release-image
/// node CLAIM a different version (awaiting-upgrade parking, upgrade-
/// target regenesis) without building a second image; production
/// release binaries ignore them entirely.
fn test_mode() -> bool {
    cfg!(debug_assertions) || std::env::var("HOPNET_TEST_MODE").is_ok()
}

/// The node's EFFECTIVE running version code: the compile-time identity,
/// unless test mode is on and `HOPNET_UPGRADE_VERSION_OVERRIDE` holds a
/// well-formed CalVer token (malformed overrides are ignored, not
/// errors). All runtime version-identity consumers go through this.
pub fn effective_running_code() -> u32 {
    if test_mode()
        && let Ok(v) = std::env::var("HOPNET_UPGRADE_VERSION_OVERRIDE")
    {
        if let Some(code) = parse_code(&v) {
            return code;
        }
        tracing::warn!(
            override_value = %v,
            "ignoring malformed HOPNET_UPGRADE_VERSION_OVERRIDE"
        );
    }
    running_version_code()
}

/// The node's effective STAGED version, if any: test mode may claim one
/// via `HOPNET_UPGRADE_STAGED_OVERRIDE` (so a mesh can satisfy the
/// regenesis start precondition for an upgrade target it never really
/// staged); otherwise staging is the upgrade provider's business and
/// this returns None — the v1 git-release provider never stages.
pub fn effective_staged_code() -> Option<u32> {
    if test_mode()
        && let Ok(v) = std::env::var("HOPNET_UPGRADE_STAGED_OVERRIDE")
    {
        return parse_code(&v);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    // Should: parse the crate's own version token as CalVer — the boot
    // invariant that makes attestation and the future boot gate possible.
    #[test]
    fn own_version_is_calver() {
        let code = running_version_code();
        assert!(code_is_valid(code));
        assert_eq!(format_code(code), running_version_str());
    }

    // Should: honour the version/staged overrides in test mode, ignore a
    // malformed running override, and report no staged version absent an
    // override (the v1 provider never stages).
    // Should not: let an override survive removal — the compile-time
    // identity is always the fallback.
    // Collapsing into one fn serialized this test's OWN reads, which was
    // never the exposure: other tests read `effective_running_code()`
    // concurrently, and a far-future override inverts them outright — the
    // status view's `awaiting_upgrade` and the upgrade view's
    // `newer_than_running` both flip. Hence the shared process-env lock,
    // which also RESTORES on drop.
    #[test]
    fn overrides_are_test_mode_seams() {
        let guard = crate::test_env::lock_env();
        // Test builds have debug_assertions, so test_mode() is on here.
        crate::test_env::set(&guard, "HOPNET_UPGRADE_VERSION_OVERRIDE", "2031.4.2");
        assert_eq!(effective_running_code(), 20310402);
        crate::test_env::set(&guard, "HOPNET_UPGRADE_VERSION_OVERRIDE", "not-calver");
        assert_eq!(effective_running_code(), running_version_code());
        crate::test_env::remove(&guard, "HOPNET_UPGRADE_VERSION_OVERRIDE");
        assert_eq!(effective_running_code(), running_version_code());

        assert_eq!(effective_staged_code(), None);
        crate::test_env::set(&guard, "HOPNET_UPGRADE_STAGED_OVERRIDE", "2031.4.3");
        assert_eq!(effective_staged_code(), Some(20310403));
        crate::test_env::remove(&guard, "HOPNET_UPGRADE_STAGED_OVERRIDE");
        assert_eq!(effective_staged_code(), None);
    }
}
