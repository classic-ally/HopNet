//! Client version enforcement (RFC-023 S3).
//!
//! Every DeviceToken surface is wrapped in [`client_version_gate`],
//! which compares the client's self-declared identity header against the
//! surface's resolved minimum (RFC-023 S2 declarations) — one integer
//! comparison, no I/O. Rejection is `426 Upgrade Required` with a
//! structured body ([`hopnet_common::compat::UpgradeRequiredResponse`]),
//! deliberately distinct from 401/403: a version rejection must never
//! read as a credential problem.

use axum::{
    Json,
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
};
use hopnet_common::compat::{CLIENT_VERSION_HEADER, UpgradeRequiredResponse};

/// Per-surface gate configuration: the surface name (its full path
/// prefix, for the 426 body) and the resolved minimum client code.
#[derive(Clone)]
pub struct SurfaceCompat {
    pub surface: &'static str,
    pub min_client: u32,
}

/// Test-mode-only RAISE of a surface's minimum (RFC-024 S3's VM seam):
/// `HOPNET_MIN_CLIENT_OVERRIDE` holding a CalVer token raises the
/// compiled minimum via max() — it can never lower one, so a stray
/// variable cannot disable skew enforcement. Malformed tokens are
/// ignored with a warning (HOPNET_UPGRADE_VERSION_OVERRIDE's pattern).
/// Read per request on purpose: the compiled minimum is frozen into
/// each gate layer at router build, and the S3 scenario flips this
/// mid-run via a systemd drop-in + restart — one per-request read here
/// covers every SurfaceCompat site at once.
fn effective_min(compiled: u32) -> u32 {
    if !crate::version::test_mode() {
        return compiled;
    }
    let Ok(v) = std::env::var("HOPNET_MIN_CLIENT_OVERRIDE") else {
        return compiled;
    };
    match crate::version::parse_code(&v) {
        Some(code) => compiled.max(code),
        None => {
            tracing::warn!(
                override_value = %v,
                "ignoring malformed HOPNET_MIN_CLIENT_OVERRIDE"
            );
            compiled
        }
    }
}

/// The version gate. Missing, malformed, and too-old identities are all
/// rejected the same way: device tokens exist for separate-lifecycle
/// binaries, so an unversioned request on this auth class is exactly the
/// invisible skew RFC-023 exists to kill.
pub async fn client_version_gate(
    State(cfg): State<SurfaceCompat>,
    req: Request,
    next: Next,
) -> Response {
    // The enforced and advertised minimums come from the same read —
    // the wrapper's policy readout must see the number the gate applies.
    let min_client = effective_min(cfg.min_client);
    let claimed = req
        .headers()
        .get(CLIENT_VERSION_HEADER)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u32>().ok());
    match claimed {
        Some(code) if code >= min_client => next.run(req).await,
        _ => (
            StatusCode::UPGRADE_REQUIRED,
            Json(UpgradeRequiredResponse {
                surface: cfg.surface.to_string(),
                min_client,
                node_version: crate::version::effective_running_code(),
            }),
        )
            .into_response(),
    }
}

/// Resolved minimum for a manifest-declared mount prefix (RFC-023 S2
/// rule: mount override, else projection default). Panics on an unknown
/// or undeclared prefix — [`crate::projections::assert_client_compat_coverage`]
/// has already guaranteed every DeviceToken mount resolves, so a miss
/// here is a host wiring bug, not a runtime condition.
pub fn resolved_min(caps: &hopnet_projection::host::HostCapabilities, prefix: &str) -> u32 {
    for m in crate::projections::manifests() {
        for mount in m.mounts(caps) {
            if mount.prefix == prefix {
                return mount
                    .min_client
                    .or(m.min_client())
                    .unwrap_or_else(|| panic!("mount '{prefix}' resolves no min_client"));
            }
        }
    }
    panic!("no manifest mount declares prefix '{prefix}'")
}

/// Minimum for a host-owned DeviceToken surface (the
/// [`crate::projections::HOST_DEVICE_TOKEN_MIN_CLIENT`] table). Panics
/// on an unlisted prefix — same wiring-bug contract as [`resolved_min`].
pub fn host_min(prefix: &str) -> u32 {
    crate::projections::HOST_DEVICE_TOKEN_MIN_CLIENT
        .iter()
        .find(|(p, _)| *p == prefix)
        .map(|(_, code)| *code)
        .unwrap_or_else(|| panic!("host surface '{prefix}' not in HOST_DEVICE_TOKEN_MIN_CLIENT"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::routing::get;
    use tower::ServiceExt;

    /// Surface → health path pairing pinned for the registry walk: a
    /// future DeviceToken surface that ships without a probe fails this
    /// list by omission (RFC-023: the probe is the consumer-lifecycle
    /// anchor).
    const PROBED_SURFACES: &[(&str, &str)] = &[
        (
            "/integrations/fileprovider",
            "/integrations/fileprovider/health",
        ),
        (
            "/integrations/documentprovider",
            "/integrations/documentprovider/health",
        ),
        ("/integrations/mount", "/integrations/mount/health"),
        ("/api/photos/client", "/photos/client/health"),
    ];

    fn gated(surface: &'static str, min_client: u32) -> Router {
        Router::new().route("/probe", get(|| async { "ok" })).layer(
            axum::middleware::from_fn_with_state(
                SurfaceCompat {
                    surface,
                    min_client,
                },
                client_version_gate,
            ),
        )
    }

    async fn status_for(app: &Router, header: Option<&str>) -> StatusCode {
        let mut req = Request::builder().uri("/probe");
        if let Some(v) = header {
            req = req.header(CLIENT_VERSION_HEADER, v);
        }
        app.clone()
            .oneshot(req.body(axum::body::Body::empty()).unwrap())
            .await
            .unwrap()
            .status()
    }

    // Impact: the gate is the whole enforcement mechanism — if any of
    // these classes leaked through, a stale client would run against a
    // surface that no longer supports it, silently (the laptop-EIO gap).
    // Should: pass a client at or above the minimum.
    // Should not: pass a missing, malformed, or below-minimum identity —
    // all three reject as 426, never as an auth error.
    #[tokio::test]
    async fn gate_passes_current_and_rejects_stale_or_absent() {
        // Locked: the min-client override test mutates process env.
        let _guard = crate::test_env::lock_env();
        let app = gated("/test", 20260802);
        assert_eq!(status_for(&app, Some("20260802")).await, StatusCode::OK);
        assert_eq!(status_for(&app, Some("20270101")).await, StatusCode::OK);
        assert_eq!(
            status_for(&app, Some("20260801")).await,
            StatusCode::UPGRADE_REQUIRED
        );
        assert_eq!(status_for(&app, None).await, StatusCode::UPGRADE_REQUIRED);
        assert_eq!(
            status_for(&app, Some("not-a-code")).await,
            StatusCode::UPGRADE_REQUIRED
        );
    }

    // Should: name the rejecting surface, its minimum, and the node's
    // version in the 426 body — the operator's pointer from rejection
    // to remedy.
    #[tokio::test]
    async fn rejection_body_names_surface_minimum_and_node() {
        // Locked: the min-client override test mutates process env.
        let _guard = crate::test_env::lock_env();
        let app = gated("/integrations/mount", 20260802);
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/probe")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UPGRADE_REQUIRED);
        let bytes = axum::body::to_bytes(resp.into_body(), 1 << 16)
            .await
            .unwrap();
        let body: UpgradeRequiredResponse = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body.surface, "/integrations/mount");
        assert_eq!(body.min_client, 20260802);
        assert_eq!(body.node_version, crate::version::effective_running_code());
    }

    // Impact: the S3 VM test raises a surface's minimum mid-run through
    // this seam; if it could LOWER one, a stray variable would silently
    // disable skew enforcement wherever test mode is on.
    // Should: raise the effective minimum when the override exceeds the
    // compiled value, and advertise the RAISED value in the 426 body —
    // the wrapper's policy readout must see the number the gate applies.
    // Should not: lower the minimum below the compiled value, act on a
    // malformed token, or outlive the variable's removal.
    #[tokio::test]
    async fn min_client_override_raises_and_never_lowers() {
        let guard = crate::test_env::lock_env();
        let app = gated("/integrations/mount", 20260802);

        crate::test_env::set(&guard, "HOPNET_MIN_CLIENT_OVERRIDE", "2026.12.99");
        assert_eq!(
            status_for(&app, Some("20260802")).await,
            StatusCode::UPGRADE_REQUIRED
        );
        assert_eq!(status_for(&app, Some("20261299")).await, StatusCode::OK);
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/probe")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let bytes = axum::body::to_bytes(resp.into_body(), 1 << 16)
            .await
            .unwrap();
        let body: UpgradeRequiredResponse = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body.min_client, 20261299);

        crate::test_env::set(&guard, "HOPNET_MIN_CLIENT_OVERRIDE", "2020.1.1");
        assert_eq!(status_for(&app, Some("20260802")).await, StatusCode::OK);
        assert_eq!(
            status_for(&app, Some("20200101")).await,
            StatusCode::UPGRADE_REQUIRED
        );

        crate::test_env::set(&guard, "HOPNET_MIN_CLIENT_OVERRIDE", "not-calver");
        assert_eq!(status_for(&app, Some("20260802")).await, StatusCode::OK);

        crate::test_env::remove(&guard, "HOPNET_MIN_CLIENT_OVERRIDE");
        assert_eq!(status_for(&app, Some("20260802")).await, StatusCode::OK);
    }

    // Impact: the probe list is what makes "every DeviceToken surface
    // has a version-gated health probe" mechanical — a new surface that
    // forgets its probe fails here by omission, not in the field.
    // Should: cover every DeviceToken surface (manifest-declared and
    // host-owned) with exactly one pinned health path, and answer 200
    // with the node's version to a current client at that path.
    #[test]
    fn every_device_token_surface_has_a_versioned_probe() {
        // create_test_app_state spins its own runtime internally, so the
        // test is sync with an explicit runtime for the oneshot calls.
        let app_state = crate::consensus::tests::create_test_app_state();
        let caps = crate::capabilities::build_capabilities(&app_state);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        // Every DeviceToken mount prefix and host-owned surface appears
        // in the pinned probe list (statfs is part of the mount surface,
        // probed via /integrations/mount/health).
        for m in crate::projections::manifests() {
            for mount in m.mounts(&caps) {
                if mount.auth != hopnet_projection::AuthClass::DeviceToken {
                    continue;
                }
                assert!(
                    PROBED_SURFACES.iter().any(|(s, _)| *s == mount.prefix),
                    "DeviceToken mount '{}' has no pinned health probe",
                    mount.prefix
                );
            }
        }
        for (prefix, _) in crate::projections::HOST_DEVICE_TOKEN_MIN_CLIENT {
            let covered = *prefix == "/api/integrations/mount/statfs"
                || PROBED_SURFACES.iter().any(|(s, _)| s == prefix);
            assert!(
                covered,
                "host surface '{prefix}' has no pinned health probe"
            );
        }

        // The probe routers answer: 426 headerless, 200 + node_version
        // for a current client. Built exactly as main.rs builds them.
        rt.block_on(async {
            let health =
                crate::fileprovider::routes::health_router(&caps).with_state(app_state.clone());
            let photos_health = Router::new()
                .route(
                    "/photos/client/health",
                    get(crate::fileprovider::routes::get_health).layer(
                        axum::middleware::from_fn_with_state(
                            SurfaceCompat {
                                surface: "/api/photos/client",
                                min_client: 20260802,
                            },
                            client_version_gate,
                        ),
                    ),
                )
                .with_state(app_state.clone());
            let min = format!("{}", crate::version::effective_running_code());
            for (surface, path) in PROBED_SURFACES {
                let (app, uri) = if *surface == "/api/photos/client" {
                    (&photos_health, *path)
                } else {
                    (&health, path.strip_prefix("/integrations").unwrap())
                };
                let bare = app
                    .clone()
                    .oneshot(
                        Request::builder()
                            .uri(uri)
                            .body(axum::body::Body::empty())
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    bare.status(),
                    StatusCode::UPGRADE_REQUIRED,
                    "headerless probe of {path} must 426"
                );
                let ok = app
                    .clone()
                    .oneshot(
                        Request::builder()
                            .uri(uri)
                            .header(CLIENT_VERSION_HEADER, &min)
                            .body(axum::body::Body::empty())
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                assert_eq!(ok.status(), StatusCode::OK, "versioned probe of {path}");
                let bytes = axum::body::to_bytes(ok.into_body(), 1 << 16).await.unwrap();
                let body: hopnet_common::HealthResponse = serde_json::from_slice(&bytes).unwrap();
                assert_eq!(body.node_version, crate::version::effective_running_code());
            }
        });
    }
}
