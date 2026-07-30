//! hopnet-mount: user-session daemon projecting the HopNet drive onto a
//! Linux mountpoint (RFC-018).
//!
//! Layering (the reusability discipline from the RFC): the platform-neutral
//! core — transport seam, id map, attr cache, `vfs::MountCore` — knows
//! nothing about fuser or HopNet HTTP; the linux-only `fuse` module is a
//! thin errno-mapping adapter over `MountCore`, and node specifics live
//! behind `transport::NodeTransport` (mock in S1, HTTP in S3).

pub mod attrs;
pub mod http_transport;
pub mod idmap;
pub mod mock;
pub mod transport;
pub mod vfs;
pub mod watch;

#[cfg(target_os = "linux")]
pub mod fuse;

#[cfg(test)]
mod tests;
