//! Client API compatibility wire contract (RFC-022 S3).
//!
//! Shared between the node's version-enforcement layer and every client
//! that must recognize a version rejection. Version values are the
//! numeric CalVer codes from [`crate::version`].

use serde::{Deserialize, Serialize};
use typeshare::typeshare;

/// Header carrying the client's identity (its CalVer code) on every
/// request to a DeviceToken surface: `x-hopnet-client-version: 20260802`.
/// Identity only — acceptance policy lives at each end (RFC-022).
pub const CLIENT_VERSION_HEADER: &str = "x-hopnet-client-version";

/// Body of a `426 Upgrade Required` version rejection. Distinct from
/// 401/403 so a version rejection never reads as a credential problem;
/// the fields tell the operator exactly what to upgrade to.
#[derive(Debug, Deserialize, Serialize)]
#[typeshare]
pub struct UpgradeRequiredResponse {
    /// The surface that rejected the request (its mount prefix).
    pub surface: String,
    /// Oldest client version code this surface accepts.
    pub min_client: u32,
    /// The node's own version code.
    pub node_version: u32,
}
