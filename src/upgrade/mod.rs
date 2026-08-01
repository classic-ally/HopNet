//! Upgrade readiness (RFC-019 S3): the node's version attestation and,
//! behind the provider seam, discovery of what a deployment could
//! upgrade to. SAFETY lives in the epoch boot gates (S6); everything
//! here is upstream of the boundary — a wrong or missing advisory can
//! waste operator time, never diverge a mesh. The one committed output
//! (the attested version columns on `nodes`) becomes the deterministic
//! precondition input for `regenesis_start` in S5.

pub mod handlers;

use serde::{Deserialize, Serialize};

/// The `node_staged_version` tx payload: a node's objective claim about
/// its own deployment — the running version and at most one version its
/// upgrade provider has staged (downloaded/installed, pending
/// activation). Running counts as trivially staged, so `staged_code`
/// never repeats it. `attested_height` is read before submission and
/// rides in the signed bytes (metrics precedent) — deterministic with no
/// in-apply height read.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeStagedVersion {
    pub node_id: i32,
    pub running_code: u32,
    pub staged_code: Option<u32>,
    pub attested_height: u64,
}
