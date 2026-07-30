//! The drive's HTTP surface (RFC-015 Stage D4): axum routers for files,
//! shares, and the FileProvider/DocumentProvider integrations, moved from
//! the host behind the `host` seams. Route paths, status codes, and
//! response shapes are preserved EXACTLY; the host mounts these routers and
//! layers its own auth middleware (JWT / device token) around them.

pub mod documentprovider;
pub mod files;
pub mod fileprovider;
pub mod mount;
pub mod shares;

/// Parse a `Range` header: supports `bytes=START-END` and `bytes=START-`
/// (single range only; multi-range and suffix forms are ignored).
/// Returns (start, inclusive-end-if-given).
pub(crate) fn parse_range(headers: &axum::http::HeaderMap) -> Option<(u64, Option<u64>)> {
    let val = headers.get(axum::http::header::RANGE)?;
    let s = val.to_str().ok()?;
    let s = s.strip_prefix("bytes=")?;
    if s.contains(',') {
        return None;
    }
    let mut parts = s.splitn(2, '-');
    let start: u64 = parts.next()?.parse().ok()?;
    let end: Option<u64> = parts
        .next()
        .and_then(|e| if e.is_empty() { None } else { e.parse().ok() });
    Some((start, end))
}

/// Per-user write gate (moved to hopnet-projection at RFC-016 Stage 2 —
/// generic over HostCapabilities, which DriveState aliases; every
/// projection's write sub-router reuses it). Semantics unchanged:
/// missing user → 401, denied → 409, check failure → 500.
pub(crate) use hopnet_projection::host::write_gate;
