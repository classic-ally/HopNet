//! Upgrade-readiness view assembler (RFC-019 S3) — pure composition:
//! version arithmetic lives in crate::version, committed claims in
//! db::versions, provider state in upgrade::UpgradeState.

use hopnet_common::views::{
    ActivationView, AvailableReleaseView, NodeVersionsView, ProviderStatusView,
    UpgradeReadinessView,
};

use crate::db::versions::MeshNodeVersions;
use crate::upgrade::ProviderStatus;

/// This deployment's boundary capabilities (RFC-021): resolved from the
/// module-set env contract, so the operator learns "this node will park"
/// from the advisory, not from a parked mesh.
fn activation_view() -> ActivationView {
    match crate::upgrade::nix_provider::NixEnv::from_env() {
        Some(env) => ActivationView {
            provider: "nix".into(),
            can_stage: env.auto_stage,
            auto_activate: env.auto_activate,
        },
        None => ActivationView {
            provider: "git-release".into(),
            can_stage: false,
            auto_activate: false,
        },
    }
}

pub fn upgrade_view(
    mesh: Vec<MeshNodeVersions>,
    provider: Option<&ProviderStatus>,
) -> UpgradeReadinessView {
    let running_code = crate::version::effective_running_code();

    let mesh = mesh
        .into_iter()
        .map(|node| NodeVersionsView {
            node_id: node.node_id,
            name: node.name,
            running: node.running_code.map(crate::version::format_code),
            staged: node.staged_code.map(crate::version::format_code),
        })
        .collect();

    let available = provider
        .and_then(|status| status.result.as_ref().ok())
        .map(|report| {
            report
                .available
                .iter()
                .map(|v| AvailableReleaseView {
                    version: v.version.clone(),
                    prerelease: v.prerelease,
                    newer_than_running: crate::version::parse_code(&v.version)
                        .is_some_and(|code| code > running_code),
                })
                .collect()
        })
        .unwrap_or_default();

    UpgradeReadinessView {
        running: crate::version::format_code(running_code),
        mesh,
        available,
        provider: ProviderStatusView {
            name: provider.map(|s| s.provider.to_string()),
            fetched_at: provider.map(|s| s.fetched_at.to_rfc3339()),
            error: provider.and_then(|s| s.result.as_ref().err().cloned()),
        },
        activation: activation_view(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::upgrade::{AvailableVersion, ProviderReport};

    // Should: list every registered node — an unattested node shows
    // running None and no staged value — and mark an available release
    // newer_than_running only when both tokens parse as CalVer and the
    // release's code is strictly greater.
    // Impact: the advisory presents facts; the readiness rollup is S5's
    // arithmetic when a target exists.
    #[test]
    fn lists_all_nodes_and_flags_newer_releases() {
        // Reads `effective_running_code()` via `newer_than_running`, which a
        // leaked far-future override inverts — take the process-env lock.
        let _env = crate::test_env::lock_env();
        crate::test_env::remove(&_env, "HOPNET_UPGRADE_PROVIDER");
        let mesh = vec![
            MeshNodeVersions {
                node_id: 1,
                name: "n1".into(),
                running_code: Some(20260800),
                staged_code: None,
                attested_height: Some(3),
            },
            MeshNodeVersions {
                node_id: 2,
                name: "n2".into(),
                running_code: None,
                staged_code: None,
                attested_height: None,
            },
        ];
        let status = ProviderStatus {
            provider: "git-release",
            fetched_at: chrono::Utc::now(),
            result: Ok(ProviderReport {
                available: vec![
                    AvailableVersion {
                        version: "2027.1.0".into(),
                        staged: false,
                        prerelease: false,
                    },
                    AvailableVersion {
                        version: "0.1.0-rc.2".into(),
                        staged: false,
                        prerelease: true,
                    },
                ],
            }),
        };

        let view = upgrade_view(mesh, Some(&status));
        assert_eq!(view.mesh.len(), 2);
        assert_eq!(view.mesh[0].running.as_deref(), Some("2026.8.0"));
        assert_eq!(view.mesh[1].running, None);
        assert_eq!(view.mesh[1].staged, None);
        // 2027.1.0 > the crate's own 2026.8.0; the legacy tag can't parse
        // so it is never "newer".
        assert!(view.available[0].newer_than_running);
        assert!(!view.available[1].newer_than_running);
        assert_eq!(view.provider.name.as_deref(), Some("git-release"));
        assert_eq!(view.provider.error, None);
        // Without the nix deployment contract this node advertises that it
        // parks at an upgrade boundary.
        assert_eq!(view.activation.provider, "git-release");
        assert!(!view.activation.can_stage);
        assert!(!view.activation.auto_activate);
    }
}
