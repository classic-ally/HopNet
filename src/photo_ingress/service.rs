//! SMAppService bridge for the bundled photo-ingress LaunchAgent.
//!
//! The agent plist ships at `Contents/Library/LaunchAgents/` in HopNet.app
//! (embedded by `scripts/macos/03b-embed-photo-ingress.sh`); registration
//! makes launchd own the daemon's lifecycle (login autostart, KeepAlive)
//! and surfaces it under HopNet in System Settings > Login Items.
//!
//! All three calls are XPC-backed and may block — handlers call them via
//! `spawn_blocking`.

use objc2_foundation::{NSBundle, NSString};
use objc2_service_management::SMAppService;
use tracing::info;

use super::AgentRegistration;

/// The bundled agent plist's filename. The identifier appears in exactly
/// three places, which must stay in sync:
///   1. apple/PhotoIngress/Sources/photo-ingress/Info.plist (CFBundleIdentifier)
///   2. scripts/macos/03b-embed-photo-ingress.sh (AGENT_ID)
///   3. this constant
const AGENT_PLIST_NAME: &str = "com.hopnet.desktop.photo-ingress.plist";

fn agent() -> objc2::rc::Retained<SMAppService> {
    unsafe { SMAppService::agentServiceWithPlistName(&NSString::from_str(AGENT_PLIST_NAME)) }
}

/// Map the raw SMAppServiceStatus. `Unavailable` covers future/unknown
/// values rather than guessing.
fn map_status(raw: objc2_service_management::SMAppServiceStatus) -> AgentRegistration {
    match raw.0 {
        0 => AgentRegistration::NotRegistered,
        1 => AgentRegistration::Enabled,
        2 => AgentRegistration::RequiresApproval,
        3 => AgentRegistration::NotFound,
        _ => AgentRegistration::Unavailable,
    }
}

pub fn agent_status() -> AgentRegistration {
    map_status(unsafe { agent().status() })
}

/// The running app bundle's filesystem path, or None when not launched from
/// a bundle (dev `cargo run`). SMAppService resolves the agent plist against
/// the bundle that REGISTERED it, so this is the identity the bundle-move
/// healer compares (RFC-026: an upgraded app's old store path keeps
/// existing, and a stale registration keeps running old daemon bytes).
pub fn current_bundle_path() -> Option<String> {
    let bundle = NSBundle::mainBundle();
    let path = unsafe { bundle.bundlePath() }.to_string();
    // A bare binary's "bundle path" is its parent directory; only a real
    // .app bundle counts as an identity worth tracking.
    path.ends_with(".app").then_some(path)
}

/// Register the bundled agent. Already-Enabled is a no-op (re-register
/// errors); registering into RequiresApproval is a SUCCESS path — the user
/// blocked it in Login Items and only System Settings can unblock it.
pub fn register_agent() -> Result<AgentRegistration, String> {
    let service = agent();
    let before = map_status(unsafe { service.status() });
    if before == AgentRegistration::Enabled {
        return Ok(before);
    }
    unsafe { service.registerAndReturnError() }.map_err(|e| {
        format!(
            "SMAppService register: {} (domain={} code={})",
            e.localizedDescription(),
            e.domain(),
            e.code()
        )
    })?;
    let after = map_status(unsafe { service.status() });
    info!("photo-ingress agent registered (status {after:?})");
    Ok(after)
}

/// Unregister the bundled agent — launchd SIGTERMs the daemon (cooperative
/// cancel: rows stay pending). Not-registered is a no-op.
pub fn unregister_agent() -> Result<(), String> {
    let service = agent();
    if map_status(unsafe { service.status() }) == AgentRegistration::NotRegistered {
        return Ok(());
    }
    unsafe { service.unregisterAndReturnError() }.map_err(|e| {
        format!(
            "SMAppService unregister: {} (domain={} code={})",
            e.localizedDescription(),
            e.domain(),
            e.code()
        )
    })?;
    info!("photo-ingress agent unregistered");
    Ok(())
}
