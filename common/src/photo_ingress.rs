// Photo-ingress enablement API types (macOS daemon provisioning). The
// routes are macOS+gui-only, but the types are platform-independent so the
// frontend gets them via typeshare and Linux builds can unit-test the
// assembly helpers.
use serde::{Deserialize, Serialize};
use typeshare::typeshare;

/// SMAppService registration state of the bundled LaunchAgent, plus
/// `Unavailable` for platforms/builds without ServiceManagement.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[typeshare]
pub enum AgentRegistration {
    NotRegistered,
    Enabled,
    /// Registered but blocked in System Settings > Login Items — the pane
    /// deep-links `SMAppService.openSystemSettingsLoginItems` for this.
    RequiresApproval,
    NotFound,
    Unavailable,
}

/// GET /photo-ingress/status response.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[typeshare]
pub struct PhotoIngressStatus {
    pub registration: AgentRegistration,
    /// Device token + node base URL present in the keychain.
    pub keychain_provisioned: bool,
    /// Parsed from the stored token; present iff provisioned and well-formed.
    pub device_id: Option<String>,
    /// The device row is consensus-committed. May lag `keychain_provisioned`
    /// by a few seconds right after enable (registration is a consensus
    /// submit); stays false after a revoke until re-enable.
    pub device_row_present: bool,
    pub node_base_url: Option<String>,
}

/// POST /photo-ingress/disable request.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[typeshare]
pub struct DisableRequest {
    /// Also revoke the device via consensus (default true). False keeps the
    /// device row so a later re-enable reuses nothing but loses nothing.
    #[serde(default)]
    pub revoke_device: Option<bool>,
}

/// POST /photo-ingress/disable response.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[typeshare]
pub struct DisableResponse {
    pub device_revoked: bool,
    pub status: PhotoIngressStatus,
}
