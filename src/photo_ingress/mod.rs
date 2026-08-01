//! Photo-ingress enablement plumbing (RFC-011 packaging): owner-only HTTP
//! routes the future settings pane calls, the SMAppService bridge for the
//! bundled LaunchAgent, and the keychain provisioning glue.
//!
//! The routes are macOS-only (SMAppService + keychain are per-user-session
//! concerns of the GUI process); the API types, assembly helpers, and the
//! `flow` orchestration (behind its `ProvisioningDeps` seam) are
//! platform-independent so Linux CI pins the sequencing invariants.

pub(crate) mod flow;
pub mod helpers;
#[cfg(target_os = "macos")]
pub mod routes;
#[cfg(target_os = "macos")]
pub mod service;

pub use hopnet_common::photo_ingress::{
    AgentRegistration, DisableRequest, DisableResponse, EnableRequest, PhotoIngressStatus,
};
