//! Platform-independent orchestration behind the enablement routes.
//!
//! The sequencing here carries the module's security invariants — creds in
//! the keychain BEFORE the launchd handoff, device id captured BEFORE the
//! keychain wipe, owner-only gating — so it lives behind [`ProvisioningDeps`]
//! and compiles on every platform: Linux CI pins the ordering with the mock
//! tests below, while `routes.rs` (macOS) supplies the real SMAppService /
//! keychain / consensus implementations.

use std::str::FromStr;

use axum::http::StatusCode;
use tracing::warn;

use super::helpers::{build_status, device_id_from_token};
use super::{AgentRegistration, DisableRequest, DisableResponse, PhotoIngressStatus};
use crate::db::CustomUUID;

pub(crate) type Failure = (StatusCode, String);

/// Everything the enable/disable/status flows touch outside their own logic.
/// Agent methods are async because the real SMAppService calls are XPC-backed
/// and run under `spawn_blocking`.
pub(crate) trait ProvisioningDeps {
    // Node/consensus side.
    fn owner_user_id(&self) -> Option<i32>;
    async fn ensure_device_token(&self, user_id: i32) -> Result<(), StatusCode>;
    async fn revoke_device(&self, user_id: i32, device_id: CustomUUID) -> Result<(), StatusCode>;
    fn device_row_present(&self, device_id: Option<&str>) -> bool;

    // Agent lifecycle.
    async fn agent_status(&self) -> Result<AgentRegistration, String>;
    async fn register_agent(&self) -> Result<AgentRegistration, String>;
    async fn unregister_agent(&self) -> Result<(), String>;

    // Keychain. `load_config` is the stored `(api_key, base_url)` pair.
    fn load_config(&self) -> Option<(String, String)>;
    fn remove_config(&self);

    // Bundle identity (RFC-026). `current_bundle_path` is None when the
    // process is not running from an .app bundle (dev `cargo run`); the
    // stored path is whichever bundle last registered the agent.
    fn current_bundle_path(&self) -> Option<String>;
    fn stored_bundle_path(&self) -> Option<String>;
    fn store_bundle_path(&self, path: &str);
}

/// Every route is owner-only: provisioning writes THIS Mac's keychain and
/// login items — meaningless (and confusing) for any other user of the node.
fn ensure_owner(deps: &impl ProvisioningDeps, user_id: i32) -> Result<(), Failure> {
    match deps.owner_user_id() {
        Some(owner) if owner == user_id => Ok(()),
        _ => Err((
            StatusCode::FORBIDDEN,
            "photo ingress provisioning is owner-only".into(),
        )),
    }
}

fn internal(msg: impl std::fmt::Display) -> Failure {
    (StatusCode::INTERNAL_SERVER_ERROR, msg.to_string())
}

fn current_status(
    deps: &impl ProvisioningDeps,
    registration: AgentRegistration,
) -> PhotoIngressStatus {
    let keychain_pair = deps.load_config();
    let present = deps.device_row_present(
        keychain_pair
            .as_ref()
            .and_then(|(api_key, _)| device_id_from_token(api_key)),
    );
    build_status(registration, keychain_pair, present)
}

pub(crate) async fn status(
    deps: &impl ProvisioningDeps,
    user_id: i32,
) -> Result<PhotoIngressStatus, Failure> {
    ensure_owner(deps, user_id)?;
    let registration = deps.agent_status().await.map_err(internal)?;
    Ok(current_status(deps, registration))
}

pub(crate) async fn enable(
    deps: &impl ProvisioningDeps,
    user_id: i32,
) -> Result<PhotoIngressStatus, Failure> {
    ensure_owner(deps, user_id)?;

    // 1. Token (mint, or heal a revoked one; no-op while valid) — in the
    //    keychain BEFORE launchd can spawn the daemon. The daemon needs no
    //    other configuration: the spool is data-dir-derived and the
    //    personal library self-creates at startup.
    deps.ensure_device_token(user_id)
        .await
        .map_err(|status| (status, "device token provisioning failed".into()))?;
    // 2. Lifecycle handoff to launchd. RequiresApproval is a success —
    //    surfaced in the status for the caller to act on.
    let registration = deps.register_agent().await.map_err(internal)?;

    // 3. Record which bundle registered — the identity the startup
    //    bundle-move healer compares against (RFC-026).
    if let Some(path) = deps.current_bundle_path() {
        deps.store_bundle_path(&path);
    }

    Ok(current_status(deps, registration))
}

/// Startup healer for the stale-registration hazard (RFC-026): SMAppService
/// resolves the agent plist against the bundle that REGISTERED it, and after
/// an upgrade the old bundle still exists (nix store keeps it), so a stale
/// registration keeps running old daemon bytes — silently. Whenever the
/// running bundle differs from the one that registered (or the marker is
/// missing — a pre-RFC-026 install), re-register from this bundle.
///
/// Returns whether a re-registration happened.
pub(crate) async fn reregister_if_moved(deps: &impl ProvisioningDeps) -> Result<bool, String> {
    let Some(current) = deps.current_bundle_path() else {
        return Ok(false); // not running from a bundle — nothing to heal
    };
    match deps.agent_status().await? {
        AgentRegistration::Enabled => {}
        // RequiresApproval means the user blocked the agent in Login Items —
        // a re-register cannot help and may re-prompt. Anything else means
        // nothing is registered to go stale.
        _ => return Ok(false),
    }
    if deps.stored_bundle_path().as_deref() == Some(current.as_str()) {
        return Ok(false);
    }
    // Unregister first: register_agent no-ops while status is Enabled.
    deps.unregister_agent().await?;
    deps.register_agent().await?;
    deps.store_bundle_path(&current);
    Ok(true)
}

pub(crate) async fn disable(
    deps: &impl ProvisioningDeps,
    user_id: i32,
    req: DisableRequest,
) -> Result<DisableResponse, Failure> {
    ensure_owner(deps, user_id)?;

    // Capture the device id BEFORE the keychain wipe destroys the token.
    let device_id = deps
        .load_config()
        .and_then(|(api_key, _)| device_id_from_token(&api_key).map(str::to_string));

    if let Err(e) = deps.unregister_agent().await {
        warn!("photo-ingress disable: unregister failed (continuing): {e}");
    }
    deps.remove_config();

    let mut device_revoked = false;
    if req.revoke_device.unwrap_or(true)
        && let Some(id) = device_id
            .as_deref()
            .and_then(|s| CustomUUID::from_str(s).ok())
    {
        match deps.revoke_device(user_id, id).await {
            Ok(()) => device_revoked = true,
            Err(e) => warn!("photo-ingress disable: device revoke failed: {e:?}"),
        }
    }

    let registration = deps.agent_status().await.map_err(internal)?;
    Ok(DisableResponse {
        device_revoked,
        status: current_status(deps, registration),
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    const OWNER: i32 = 1;
    const TOKEN: &str = "019fb8a9-6022-7603-8044-d54b685aa5c4.s3cret";
    const DEVICE_ID: &str = "019fb8a9-6022-7603-8044-d54b685aa5c4";

    #[derive(Default)]
    struct MockDeps {
        calls: Mutex<Vec<&'static str>>,
        token: Mutex<Option<String>>,
        revoked: Mutex<Vec<CustomUUID>>,
        mint_fails: bool,
        unregister_fails: bool,
        revoke_fails: bool,
        register_requires_approval: bool,
        agent_enabled: bool,
        bundle_path: Option<String>,
        stored_path: Mutex<Option<String>>,
    }

    impl MockDeps {
        fn provisioned() -> Self {
            let deps = Self::default();
            *deps.token.lock().unwrap() = Some(TOKEN.into());
            deps
        }

        fn log(&self, call: &'static str) {
            self.calls.lock().unwrap().push(call);
        }

        fn calls(&self) -> Vec<&'static str> {
            self.calls.lock().unwrap().clone()
        }
    }

    impl ProvisioningDeps for MockDeps {
        fn owner_user_id(&self) -> Option<i32> {
            Some(OWNER)
        }

        async fn ensure_device_token(&self, _user_id: i32) -> Result<(), StatusCode> {
            self.log("mint");
            if self.mint_fails {
                return Err(StatusCode::SERVICE_UNAVAILABLE);
            }
            *self.token.lock().unwrap() = Some(TOKEN.into());
            Ok(())
        }

        async fn revoke_device(
            &self,
            _user_id: i32,
            device_id: CustomUUID,
        ) -> Result<(), StatusCode> {
            self.log("revoke");
            if self.revoke_fails {
                return Err(StatusCode::INTERNAL_SERVER_ERROR);
            }
            self.revoked.lock().unwrap().push(device_id);
            Ok(())
        }

        fn device_row_present(&self, device_id: Option<&str>) -> bool {
            device_id.is_some()
        }

        async fn agent_status(&self) -> Result<AgentRegistration, String> {
            Ok(if self.agent_enabled {
                AgentRegistration::Enabled
            } else {
                AgentRegistration::NotRegistered
            })
        }

        async fn register_agent(&self) -> Result<AgentRegistration, String> {
            self.log("register");
            Ok(if self.register_requires_approval {
                AgentRegistration::RequiresApproval
            } else {
                AgentRegistration::Enabled
            })
        }

        async fn unregister_agent(&self) -> Result<(), String> {
            self.log("unregister");
            if self.unregister_fails {
                return Err("xpc unavailable".into());
            }
            Ok(())
        }

        fn load_config(&self) -> Option<(String, String)> {
            self.token
                .lock()
                .unwrap()
                .clone()
                .map(|t| (t, "http://127.0.0.1:1".into()))
        }

        fn remove_config(&self) {
            self.log("remove");
            *self.token.lock().unwrap() = None;
        }

        fn current_bundle_path(&self) -> Option<String> {
            self.bundle_path.clone()
        }

        fn stored_bundle_path(&self) -> Option<String> {
            self.stored_path.lock().unwrap().clone()
        }

        fn store_bundle_path(&self, path: &str) {
            self.log("store_path");
            *self.stored_path.lock().unwrap() = Some(path.to_string());
        }
    }

    fn disable_req() -> DisableRequest {
        DisableRequest {
            revoke_device: None,
        }
    }

    // Impact: trust boundary — these routes write this Mac's keychain and
    // login items, so only the node owner may drive them.
    // Should: reject a non-owner caller with 403 on all three flows.
    // Should not: perform any side effect when rejecting.
    #[tokio::test]
    async fn owner_gate_rejects_without_side_effects() {
        let deps = MockDeps::provisioned();
        let intruder = OWNER + 1;

        let s = status(&deps, intruder).await;
        let e = enable(&deps, intruder).await;
        let d = disable(&deps, intruder, disable_req()).await;

        assert_eq!(s.unwrap_err().0, StatusCode::FORBIDDEN);
        assert_eq!(e.unwrap_err().0, StatusCode::FORBIDDEN);
        assert_eq!(d.unwrap_err().0, StatusCode::FORBIDDEN);
        assert!(deps.calls().is_empty());
        assert!(deps.token.lock().unwrap().is_some());
    }

    // Impact: launchd may spawn the daemon the instant registration lands;
    // the credentials must already be readable or its first publish tick
    // runs ingest-only until a restart.
    // Should: mint the token, then register — in that order.
    #[tokio::test]
    async fn enable_provisions_keychain_before_registering() {
        let deps = MockDeps::default();
        let status = enable(&deps, OWNER).await.unwrap();

        assert_eq!(deps.calls(), vec!["mint", "register"]);
        assert_eq!(status.registration, AgentRegistration::Enabled);
        assert!(status.keychain_provisioned);
        assert_eq!(status.device_id.as_deref(), Some(DEVICE_ID));
    }

    // Should not: register the agent when token minting fails; the mint
    // failure status propagates to the caller.
    #[tokio::test]
    async fn enable_stops_when_mint_fails() {
        let deps = MockDeps {
            mint_fails: true,
            ..Default::default()
        };
        let err = enable(&deps, OWNER).await.unwrap_err();

        assert_eq!(err.0, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(deps.calls(), vec!["mint"]);
    }

    // Should: treat registration landing in RequiresApproval as success and
    // surface it in the returned status (the user blocked the agent in Login
    // Items; only System Settings can unblock it).
    #[tokio::test]
    async fn enable_surfaces_requires_approval_as_success() {
        let deps = MockDeps {
            register_requires_approval: true,
            ..Default::default()
        };
        let status = enable(&deps, OWNER).await.unwrap();

        assert_eq!(status.registration, AgentRegistration::RequiresApproval);
    }

    // Impact: a reordering here silently leaves a revoked-in-intent token
    // valid on the network — the UI reports "disabled" while the device row
    // lives on.
    // Should: revoke the device id captured before the keychain wipe.
    #[tokio::test]
    async fn disable_revokes_the_device_captured_before_wipe() {
        let deps = MockDeps::provisioned();
        let resp = disable(&deps, OWNER, disable_req()).await.unwrap();

        assert!(resp.device_revoked);
        assert_eq!(
            *deps.revoked.lock().unwrap(),
            vec![CustomUUID::from_str(DEVICE_ID).unwrap()]
        );
        // Wipe really happened first from the keychain's point of view.
        let wipe_pos = deps.calls().iter().position(|c| *c == "remove").unwrap();
        let revoke_pos = deps.calls().iter().position(|c| *c == "revoke").unwrap();
        assert!(wipe_pos < revoke_pos);
        assert!(deps.token.lock().unwrap().is_none());
    }

    // Should: still wipe the keychain and revoke the device when agent
    // unregistration fails (best-effort teardown — a half-disabled state can
    // simply be disabled again).
    #[tokio::test]
    async fn disable_continues_past_unregister_failure() {
        let deps = MockDeps {
            unregister_fails: true,
            ..MockDeps::provisioned()
        };
        let resp = disable(&deps, OWNER, disable_req()).await.unwrap();

        assert!(resp.device_revoked);
        assert_eq!(deps.calls(), vec!["unregister", "remove", "revoke"]);
        assert!(deps.token.lock().unwrap().is_none());
    }

    // Should not: revoke the device when the request opts out.
    #[tokio::test]
    async fn disable_honours_revoke_opt_out() {
        let deps = MockDeps::provisioned();
        let resp = disable(
            &deps,
            OWNER,
            DisableRequest {
                revoke_device: Some(false),
            },
        )
        .await
        .unwrap();

        assert!(!resp.device_revoked);
        assert!(!deps.calls().contains(&"revoke"));
        assert!(deps.token.lock().unwrap().is_none());
    }

    // Should: report device_revoked false and still unregister + wipe when
    // nothing was provisioned (idempotent re-disable).
    #[tokio::test]
    async fn disable_of_unprovisioned_state_is_clean() {
        let deps = MockDeps::default();
        let resp = disable(&deps, OWNER, disable_req()).await.unwrap();

        assert!(!resp.device_revoked);
        assert_eq!(deps.calls(), vec!["unregister", "remove"]);
    }

    // Should: record the registering bundle's path at enable time so the
    // startup healer has an identity to compare against.
    #[tokio::test]
    async fn enable_records_the_registering_bundle_path() {
        let deps = MockDeps {
            bundle_path: Some("/nix/store/aaa/Applications/HopNet.app".into()),
            ..Default::default()
        };
        enable(&deps, OWNER).await.unwrap();

        assert_eq!(deps.calls(), vec!["mint", "register", "store_path"]);
        assert_eq!(
            deps.stored_bundle_path().as_deref(),
            Some("/nix/store/aaa/Applications/HopNet.app")
        );
    }

    // Impact: after an upgrade the old bundle still exists in the nix store,
    // so a stale SMAppService registration keeps running old daemon bytes
    // with no error anywhere — this healer is the only correction.
    // Should: re-register (unregister first) when the running bundle differs
    //         from the one that registered, and update the marker.
    #[tokio::test]
    async fn moved_bundle_reregisters_and_updates_marker() {
        let deps = MockDeps {
            agent_enabled: true,
            bundle_path: Some("/nix/store/bbb/Applications/HopNet.app".into()),
            stored_path: Mutex::new(Some("/nix/store/aaa/Applications/HopNet.app".into())),
            ..Default::default()
        };
        let healed = reregister_if_moved(&deps).await.unwrap();

        assert!(healed);
        assert_eq!(deps.calls(), vec!["unregister", "register", "store_path"]);
        assert_eq!(
            deps.stored_bundle_path().as_deref(),
            Some("/nix/store/bbb/Applications/HopNet.app")
        );
    }

    // Should: treat a missing marker (pre-RFC-026 install) as moved — one
    // healing re-registration adopts the running bundle.
    #[tokio::test]
    async fn missing_marker_heals_like_a_move() {
        let deps = MockDeps {
            agent_enabled: true,
            bundle_path: Some("/nix/store/bbb/Applications/HopNet.app".into()),
            ..Default::default()
        };
        assert!(reregister_if_moved(&deps).await.unwrap());
        assert_eq!(deps.calls(), vec!["unregister", "register", "store_path"]);
    }

    // Should not: touch the registration when the running bundle matches the
    // marker, when nothing is registered, or when not running from a bundle.
    #[tokio::test]
    async fn healer_leaves_settled_states_alone() {
        // Same path: no-op.
        let same = MockDeps {
            agent_enabled: true,
            bundle_path: Some("/Applications/HopNet.app".into()),
            stored_path: Mutex::new(Some("/Applications/HopNet.app".into())),
            ..Default::default()
        };
        assert!(!reregister_if_moved(&same).await.unwrap());
        assert!(same.calls().is_empty());

        // Not registered: never auto-enables, even when moved.
        let unregistered = MockDeps {
            bundle_path: Some("/nix/store/bbb/Applications/HopNet.app".into()),
            stored_path: Mutex::new(Some("/nix/store/aaa/Applications/HopNet.app".into())),
            ..Default::default()
        };
        assert!(!reregister_if_moved(&unregistered).await.unwrap());
        assert!(unregistered.calls().is_empty());

        // No bundle (dev run): nothing to compare.
        let bare = MockDeps {
            agent_enabled: true,
            ..Default::default()
        };
        assert!(!reregister_if_moved(&bare).await.unwrap());
        assert!(bare.calls().is_empty());
    }

    // Should: return success with device_revoked false when consensus
    // revocation errors — the route stays retryable rather than trapping the
    // caller in a half-disabled error state.
    #[tokio::test]
    async fn disable_reports_failed_revocation_without_erroring() {
        let deps = MockDeps {
            revoke_fails: true,
            ..MockDeps::provisioned()
        };
        let resp = disable(&deps, OWNER, disable_req()).await.unwrap();

        assert!(!resp.device_revoked);
        assert!(resp.status.device_id.is_none());
    }
}
