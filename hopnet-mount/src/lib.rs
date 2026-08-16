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
/// version header (S4) sends. Panics if the token is not CalVer — the
/// same boot invariant the node enforces; a client that cannot state
/// its identity could never pass a versioned surface anyway.
pub fn version_code() -> u32 {
    let version = env!("CARGO_PKG_VERSION");
    hopnet_common::version::parse_code(version).unwrap_or_else(|| {
        panic!("Cargo.toml version {version:?} is not CalVer YYYY.M.N (RFC-022 S1)")
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
