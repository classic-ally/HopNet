//! CalVer version codes (RFC-019 S3, shared per RFC-023).
//!
//! The version scheme is CalVer `YYYY.M.N` — year, month, counter-within-
//! month — with the workspace Cargo.toml as the single authoritative
//! source. In all committed state and comparisons a version is a NUMERIC
//! CODE, `year * 10000 + month * 100 + counter` (month and counter each
//! get two digits, counter capped at 99), so equality and ordering are
//! plain integer math in SQL and Rust alike: 2026.8.0 → 20260800, and
//! calendar order IS integer order. The string form exists only at the
//! edges — Cargo.toml, release tags (`v{version}`), and display.
//!
//! These are the pure code helpers; each binary reads its own identity
//! via `env!("CARGO_PKG_VERSION")` in its own crate (the node in
//! `src/version.rs`, clients per RFC-023) so the token always names the
//! bytes actually compiled.

/// The workspace CalVer code hopnet-common itself was compiled from.
/// Excluded-workspace clients (crates/ingress-*) cannot inherit the
/// workspace version; they path-dep this crate from the same checkout,
/// so its compile-time token IS the monorepo snapshot they were built
/// from. Panics on a non-CalVer token — the same boot invariant every
/// binary enforces (RFC-023 S1).
pub fn common_version_code() -> u32 {
    let version = env!("CARGO_PKG_VERSION");
    parse_code(version).unwrap_or_else(|| {
        panic!("hopnet-common version {version:?} is not CalVer YYYY.M.N (RFC-023)")
    })
}

/// Parse a CalVer string (`YYYY.M.N`, optionally `v`-prefixed as release
/// tags are) into its code. None for anything malformed — including
/// out-of-range month/counter and non-CalVer legacy tags.
pub fn parse_code(version: &str) -> Option<u32> {
    let version = version.strip_prefix('v').unwrap_or(version);
    let mut parts = version.split('.');
    let year: u32 = parts.next()?.parse().ok()?;
    let month: u32 = parts.next()?.parse().ok()?;
    let counter: u32 = parts.next()?.parse().ok()?;
    if parts.next().is_some() {
        return None;
    }
    if !(2000..=9999).contains(&year) || !(1..=12).contains(&month) || counter > 99 {
        return None;
    }
    Some(year * 10_000 + month * 100 + counter)
}

/// True iff `code` decodes to a well-formed CalVer token. The consensus
/// handler validates attested codes with this, so committed state can
/// only ever hold values `format_code` round-trips.
pub fn code_is_valid(code: u32) -> bool {
    let month = (code / 100) % 100;
    let year = code / 10_000;
    (2000..=9999).contains(&year) && (1..=12).contains(&month)
}

/// Display form of a code: `2026.8.0` (unpadded month/counter, matching
/// the Cargo.toml/tag spelling).
pub fn format_code(code: u32) -> String {
    format!("{}.{}.{}", code / 10_000, (code / 100) % 100, code % 100)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Should: roundtrip code ↔ string, including v-prefixed release
    // tags, and order codes exactly as the calendar orders releases.
    #[test]
    fn code_roundtrip_and_calendar_order() {
        assert_eq!(parse_code("2026.8.0"), Some(20260800));
        assert_eq!(parse_code("v2026.8.0"), Some(20260800));
        assert_eq!(format_code(20260800), "2026.8.0");
        assert_eq!(parse_code("2026.12.15"), Some(20261215));
        assert_eq!(format_code(20261215), "2026.12.15");

        // Month 12 > month 9 (the case string comparison gets wrong),
        // later counter > earlier, later year > everything before it.
        assert!(parse_code("2026.12.0") > parse_code("2026.9.9"));
        assert!(parse_code("2026.8.1") > parse_code("2026.8.0"));
        assert!(parse_code("2027.1.0") > parse_code("2026.12.99"));
    }

    // Should not: accept malformed or out-of-range tokens — month 0/13,
    // counter 100, legacy semver tags, trailing components.
    #[test]
    fn rejects_non_calver() {
        assert_eq!(parse_code("2026.0.0"), None);
        assert_eq!(parse_code("2026.13.0"), None);
        assert_eq!(parse_code("2026.8.100"), None);
        assert_eq!(parse_code("0.1.0-rc.2"), None);
        assert_eq!(parse_code("v0.1.0-rc.1"), None);
        assert_eq!(parse_code("2026.8"), None);
        assert_eq!(parse_code("2026.8.0.1"), None);
        assert!(!code_is_valid(20260000)); // month 0
        assert!(!code_is_valid(20261300)); // month 13
        assert!(!code_is_valid(100)); // year 0
    }
}
