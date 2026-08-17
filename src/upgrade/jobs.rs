//! Upgrade tick (RFC-019 S3): poll the upgrade provider and reconcile
//! the node's committed version attestation with reality. Runs as a
//! ~6-hourly cron, once at boot (with a readiness poll so fresh meshes
//! attest within seconds), and on demand via the maintenance route.

use apalis::prelude::{Data, Error};
use apalis_cron::CronContext;
use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::upgrade::{NodeStagedVersion, ProviderStatus, UpgradeProvider};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct UpgradeTickJob;

/// What one reconcile pass did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AttestationOutcome {
    /// Node not set up / engine inactive / submit failed — retry later.
    NotReady,
    /// Committed state already matches reality; nothing submitted.
    Converged,
    /// An attestation tx was submitted.
    Submitted,
}

#[derive(Debug, Clone, Serialize)]
pub struct UpgradeTickReport {
    pub polled_provider: bool,
    pub attestation: AttestationOutcome,
}

/// Poll the configured provider (if any) into UpgradeState.last. Skipped
/// entirely when checks are disabled, before setup, or in test mode
/// without an explicit HOPNET_UPGRADE_RELEASE_URL — orchestrator
/// containers stay offline. Returns whether a poll ran.
pub async fn poll_provider(app_state: &AppState) -> bool {
    let env_url = std::env::var("HOPNET_UPGRADE_RELEASE_URL").ok();
    if app_state.test_mode && env_url.is_none() {
        return false;
    }

    let settings = {
        let app_state = app_state.clone();
        tokio::task::spawn_blocking(move || {
            let conn = match app_state.db_pool.get() {
                Ok(c) => c,
                Err(e) => {
                    // A pool checkout timeout is TRANSIENT, and silently
                    // classifying it as "pre-setup" made a skipped poll
                    // indistinguishable from a disabled one. Say so; the
                    // next tick (or a retried manual tick) recovers.
                    tracing::warn!("upgrade poll skipped: db pool: {e}");
                    return None;
                }
            };
            crate::db::shared::read_upgrade_node_settings(&conn).ok()
        })
        .await
        .ok()
        .flatten()
    };
    let Some(settings) = settings else {
        tracing::debug!("upgrade poll skipped: no this_node settings yet (pre-setup)");
        return false;
    };
    if !settings.check_enabled {
        return false;
    }

    let url = env_url
        .or(settings.release_url)
        .unwrap_or_else(crate::upgrade::git_release::GitReleaseProvider::default_releases_url);
    // Provider selection (RFC-021/RFC-026): a deployment that declares an
    // activation contract gets staging + activation; everything else keeps
    // the report-only v1 baseline.
    let activation = crate::upgrade::ActivationEnv::from_env();
    let provider: Box<dyn UpgradeProvider> = match &activation {
        Some(crate::upgrade::ActivationEnv::Nix(env)) => Box::new(
            crate::upgrade::nix_provider::NixUpgradeProvider::new(env.clone(), url),
        ),
        Some(crate::upgrade::ActivationEnv::MacApp(env)) => Box::new(
            crate::upgrade::macos_app::MacAppProvider::new(env.clone(), url),
        ),
        None => Box::new(crate::upgrade::git_release::GitReleaseProvider::new(url)),
    };

    let mut result = provider.report().await;
    if let Err(e) = &result {
        tracing::warn!("upgrade provider poll failed: {e}");
    }

    // Auto-stage (RFC-021): the newest STABLE release strictly newer than
    // running, not already staged — one attempt per tick, so a failing
    // build retries on the cron cadence rather than looping. On success,
    // re-report so THIS tick's attestation already carries the staged
    // claim instead of waiting six hours.
    if let (Some(env), Ok(report)) = (&activation, &result)
        && env.auto_stage()
    {
        let running = crate::version::effective_running_code();
        let candidate = report
            .available
            .iter()
            .filter(|v| !v.prerelease && !v.staged)
            .filter(|v| crate::version::parse_code(&v.version).is_some_and(|code| code > running))
            .max_by_key(|v| crate::version::parse_code(&v.version));
        if let Some(candidate) = candidate {
            match provider.stage(&candidate.version).await {
                Ok(()) => {
                    tracing::info!(version = %candidate.version, "auto-staged release");
                    result = provider.report().await;
                }
                Err(e) => tracing::warn!(version = %candidate.version, "auto-stage failed: {e}"),
            }
        }
    }

    let status = ProviderStatus {
        provider: provider.name(),
        fetched_at: Utc::now(),
        result: result.map_err(|e| e.to_string()),
    };
    *app_state.upgrade.last.write().await = Some(status);
    true
}

/// Reconcile the committed attestation with reality. Desired state is
/// the running version plus whatever the provider reports as staged —
/// under the v1 git-release provider that is nothing, so attestations
/// are running-only. Submits at most one tx, and only on difference, so
/// the loop cannot spam.
pub async fn run_version_attestation(app_state: &AppState) -> AttestationOutcome {
    let Ok(node_id) = app_state.get_node_id() else {
        return AttestationOutcome::NotReady;
    };

    let running_code = crate::version::effective_running_code();
    // Staged = the test-mode override if claimed, else the newest
    // provider-staged version distinct from running. v1: the
    // git-release provider never stages, so absent an override this is
    // None.
    let staged_code = match crate::version::effective_staged_code() {
        Some(code) if code != running_code => Some(code),
        Some(_) => None,
        None => {
            let last = app_state.upgrade.last.read().await;
            last.as_ref()
                .and_then(|status| status.result.as_ref().ok())
                .and_then(|report| {
                    report
                        .available
                        .iter()
                        .filter(|v| v.staged)
                        .filter_map(|v| crate::version::parse_code(&v.version))
                        .find(|&code| code != running_code)
                })
        }
    };

    let committed = {
        let app_state = app_state.clone();
        tokio::task::spawn_blocking(move || {
            let conn = app_state.db_pool.get().ok()?;
            crate::db::versions::read_node_version(&conn, node_id).ok()?
        })
        .await
        .ok()
        .flatten()
    };
    if committed == Some((Some(running_code), staged_code)) {
        return AttestationOutcome::Converged;
    }

    // Height read pre-submission — it rides in the signed payload.
    let attested_height = {
        let app_state = app_state.clone();
        tokio::task::spawn_blocking(move || {
            let conn = app_state.db_pool.get().ok()?;
            hopnet_projection::current_height(&conn).ok()
        })
        .await
        .ok()
        .flatten()
    };
    let Some(attested_height) = attested_height else {
        return AttestationOutcome::NotReady;
    };

    let report = NodeStagedVersion {
        node_id,
        running_code,
        staged_code,
        attested_height,
    };
    let Ok(payload) = bincode::serde::encode_to_vec(&report, bincode::config::standard()) else {
        return AttestationOutcome::NotReady;
    };
    let Ok(tx) = crate::consensus::dispatch::create_signed_transaction(
        app_state,
        "node_staged_version".to_string(),
        payload,
    ) else {
        return AttestationOutcome::NotReady;
    };

    match app_state.consensus_queue.submit(tx).await {
        Ok(_) => {
            tracing::info!(
                "attested version {} (staged: {:?}) at height {}",
                crate::version::format_code(running_code),
                staged_code.map(crate::version::format_code),
                attested_height
            );
            AttestationOutcome::Submitted
        }
        Err(e) => {
            tracing::debug!("version attestation not accepted yet: {e:?}");
            AttestationOutcome::NotReady
        }
    }
}

/// One full tick: provider poll + attestation reconcile.
pub async fn run_upgrade_tick(app_state: &AppState) -> UpgradeTickReport {
    let polled_provider = poll_provider(app_state).await;
    let attestation = run_version_attestation(app_state).await;
    UpgradeTickReport {
        polled_provider,
        attestation,
    }
}

/// Cron wrapper. Network errors are expected and never fail the job
/// (metrics precedent) — the report is logged and the next tick retries.
pub async fn handle_upgrade_tick(
    _job: UpgradeTickJob,
    _ctx: CronContext<Utc>,
    data: Data<AppState>,
) -> Result<(), Error> {
    let report = run_upgrade_tick(&data).await;
    tracing::info!(
        "upgrade tick: polled_provider={} attestation={:?}",
        report.polled_provider,
        report.attestation
    );
    Ok(())
}

/// Boot task: attest as soon as the node can, then get out of the way.
/// Covers both a restarted node (node_id preset) and a fresh one
/// (setup/join sets node_id mid-loop); the 15s cadence lands the first
/// attestation within seconds of mesh formation — which is also what
/// puts version rows inside orchestrator test windows, making the
/// divergence gate the tx's standing e2e. Caps out after ~1h; the cron
/// takes over from there.
pub async fn attest_until_converged(app_state: AppState) {
    for _ in 0..240 {
        match run_version_attestation(&app_state).await {
            AttestationOutcome::Converged => return,
            AttestationOutcome::Submitted | AttestationOutcome::NotReady => {}
        }
        tokio::time::sleep(std::time::Duration::from_secs(15)).await;
    }
    tracing::warn!("boot version attestation never converged; cron will keep trying");
}
