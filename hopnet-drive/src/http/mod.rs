//! The drive's HTTP surface (RFC-015 Stage D4): axum routers for files,
//! shares, and the FileProvider/DocumentProvider integrations, moved from
//! the host behind the `host` seams. Route paths, status codes, and
//! response shapes are preserved EXACTLY; the host mounts these routers and
//! layers its own auth middleware (JWT / device token) around them.

pub mod documentprovider;
pub mod files;
pub mod fileprovider;
pub mod shares;

/// Per-user write gate (moved to hopnet-projection at RFC-016 Stage 2 —
/// generic over HostCapabilities, which DriveState aliases; every
/// projection's write sub-router reuses it). Semantics unchanged:
/// missing user → 401, denied → 409, check failure → 500.
pub(crate) use hopnet_projection::host::write_gate;
