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

use super::helpers::{build_status, device_id_from_token, validate_blob_root};
use super::{AgentRegistration, DisableRequest, DisableResponse, EnableRequest, PhotoIngressStatus};
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
    fn load_blob_root(&self) -> Option<String>;
    fn store_provisioning(
        &self,
        blob_root: &str,
        sidecar_root_remote: Option<&str>,
    ) -> Result<(), String>;
    fn remove_config(&self);
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
    let blob_root = deps.load_blob_root();
    let present = deps.device_row_present(
        keychain_pair
            .as_ref()
            .and_then(|(api_key, _)| device_id_from_token(api_key)),
    );
    build_status(registration, keychain_pair, blob_root, present)
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
    req: EnableRequest,
) -> Result<PhotoIngressStatus, Failure> {
    ensure_owner(deps, user_id)?;
    validate_blob_root(&req.blob_root).map_err(|msg| (StatusCode::BAD_REQUEST, msg))?;

    // 1. Token (mint, or heal a revoked one; no-op while valid).
    deps.ensure_device_token(user_id)
        .await
        .map_err(|status| (status, "device token provisioning failed".into()))?;
    // 2. Library provisioning — in the keychain BEFORE launchd can spawn
    //    the daemon, so its startup auto-bind sees it.
    deps.store_provisioning(&req.blob_root, req.sidecar_root_remote.as_deref())
        .map_err(internal)?;
    // 3. Lifecycle handoff to launchd. RequiresApproval is a success —
    //    surfaced in the status for the caller to act on.
    let registration = deps.register_agent().await.map_err(internal)?;

    Ok(current_status(deps, registration))
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
        && let Some(id) = device_id.as_deref().and_then(|s| CustomUUID::from_str(s).ok())
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
        blob_root: Mutex<Option<String>>,
        revoked: Mutex<Vec<CustomUUID>>,
        mint_fails: bool,
        store_fails: bool,
        unregister_fails: bool,
        revoke_fails: bool,
        register_requires_approval: bool,
    }

    impl MockDeps {
        fn provisioned() -> Self {
            let deps = Self::default();
            *deps.token.lock().unwrap() = Some(TOKEN.into());
            *deps.blob_root.lock().unwrap() = Some("/blobs".into());
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
            Ok(AgentRegistration::NotRegistered)
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

        fn load_blob_root(&self) -> Option<String> {
            self.blob_root.lock().unwrap().clone()
        }

        fn store_provisioning(
            &self,
            blob_root: &str,
            _sidecar_root_remote: Option<&str>,
        ) -> Result<(), String> {
            self.log("store");
            if self.store_fails {
                return Err("keychain write refused".into());
            }
            *self.blob_root.lock().unwrap() = Some(blob_root.into());
            Ok(())
        }

        fn remove_config(&self) {
            self.log("remove");
            *self.token.lock().unwrap() = None;
            *self.blob_root.lock().unwrap() = None;
        }
    }

    fn enable_req() -> EnableRequest {
        EnableRequest {
            blob_root: "/blobs".into(),
            sidecar_root_remote: None,
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
        let e = enable(&deps, intruder, enable_req()).await;
        let d = disable(&deps, intruder, disable_req()).await;

        assert_eq!(s.unwrap_err().0, StatusCode::FORBIDDEN);
        assert_eq!(e.unwrap_err().0, StatusCode::FORBIDDEN);
        assert_eq!(d.unwrap_err().0, StatusCode::FORBIDDEN);
        assert!(deps.calls().is_empty());
        assert!(deps.token.lock().unwrap().is_some());
    }

    // Impact: launchd may spawn the daemon the instant registration lands;
    // credentials and blob root must already be readable or its startup
    // auto-bind races an empty keychain.
    // Should: mint the token, then store provisioning, then register — in
    // that order.
    #[tokio::test]
    async fn enable_provisions_keychain_before_registering() {
        let deps = MockDeps::default();
        let status = enable(&deps, OWNER, enable_req()).await.unwrap();

        assert_eq!(deps.calls(), vec!["mint", "store", "register"]);
        assert_eq!(status.registration, AgentRegistration::Enabled);
        assert!(status.keychain_provisioned);
        assert_eq!(status.device_id.as_deref(), Some(DEVICE_ID));
    }

    // Should not: write the keychain or register the agent when token
    // minting fails; the mint failure status propagates to the caller.
    #[tokio::test]
    async fn enable_stops_before_keychain_when_mint_fails() {
        let deps = MockDeps {
            mint_fails: true,
            ..Default::default()
        };
        let err = enable(&deps, OWNER, enable_req()).await.unwrap_err();

        assert_eq!(err.0, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(deps.calls(), vec!["mint"]);
        assert!(deps.blob_root.lock().unwrap().is_none());
    }

    // Should not: register the agent when the keychain write fails.
    #[tokio::test]
    async fn enable_stops_before_register_when_store_fails() {
        let deps = MockDeps {
            store_fails: true,
            ..Default::default()
        };
        let err = enable(&deps, OWNER, enable_req()).await.unwrap_err();

        assert_eq!(err.0, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(deps.calls(), vec!["mint", "store"]);
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
        let status = enable(&deps, OWNER, enable_req()).await.unwrap();

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
