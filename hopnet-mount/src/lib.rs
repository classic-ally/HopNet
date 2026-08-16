//! hopnet-mount: user-session daemon projecting the HopNet drive onto a
//! Linux mountpoint (RFC-018).
//!
//! Layering (the reusability discipline from the RFC): the platform-neutral
//! core — transport seam, id map, attr cache, `vfs::MountCore` — knows
//! nothing about fuser or HopNet HTTP; the linux-only `fuse` module is a
//! thin errno-mapping adapter over `MountCore`, and node specifics live
//! behind `transport::NodeTransport` (mock in S1, HTTP in S3).

/// This build's CalVer identity as its numeric code (RFC-022 S1): the
/// workspace version compiled into THIS crate, the value the client
/// version header sends. Panics if the token is not CalVer — the
/// same boot invariant the node enforces; a client that cannot state
/// its identity could never pass a versioned surface anyway.
pub fn version_code() -> u32 {
    let version = env!("CARGO_PKG_VERSION");
    hopnet_common::version::parse_code(version).unwrap_or_else(|| {
        panic!("Cargo.toml version {version:?} is not CalVer YYYY.M.N (RFC-022 S1)")
    })
}

/// Oldest node release this build accepts (RFC-022 S4): the newest
/// release whose surfaces provide every endpoint this daemon calls.
/// The other half of the skew window — the node's `min_client` — is
/// enforced node-side.
pub const MIN_NODE: u32 = 20260802;

/// What `--min-node` prints (RFC-023 S1): the bare CalVer token, one
/// trimmed-stdout parse for the wrapper interrogating a staged binary.
pub fn min_node_display() -> String {
    hopnet_common::version::format_code(MIN_NODE)
}

/// The daemon-side half of version negotiation, applied to the health
/// probe's answer at startup and login. `node_version: 0` is a node
/// that predates RFC-022 and never reports identity — refused with the
/// remedy named, since no client-side action can fix a stale NODE.
pub fn check_node_version(report: &transport::HealthReport) -> Result<(), String> {
    if report.node_version >= MIN_NODE {
        return Ok(());
    }
    let required = hopnet_common::version::format_code(MIN_NODE);
    Err(if report.node_version == 0 {
        format!(
            "node reports no version (pre-RFC-022) but this client requires \
             node >= {required} — upgrade the node"
        )
    } else {
        format!(
            "node {} is older than required {required} — upgrade the node",
            hopnet_common::version::format_code(report.node_version)
        )
    })
}

pub mod attrs;
pub mod cache;
pub mod http_transport;
pub mod idmap;
pub mod mock;
pub mod staging;
pub mod transport;
pub mod vfs;
pub mod watch;

#[cfg(target_os = "linux")]
pub mod fuse;

#[cfg(target_os = "linux")]
pub mod provision;

#[cfg(test)]
mod tests;
