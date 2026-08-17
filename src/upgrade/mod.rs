//! Upgrade readiness (RFC-019 S3): the node's version attestation and,
//! behind the provider seam, discovery of what a deployment could
//! upgrade to. SAFETY lives in the epoch boot gates (S6); everything
//! here is upstream of the boundary — a wrong or missing advisory can
//! waste operator time, never diverge a mesh. The one committed output
//! (the attested version columns on `nodes`) becomes the deterministic
//! precondition input for `regenesis_start` in S5.

pub mod git_release;
pub mod handlers;
pub mod jobs;
pub mod macos_app;
pub mod nix_provider;
pub mod routes;

use std::future::Future;
use std::pin::Pin;

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

/// The deployment's activation contract, resolved from env per use site
/// (the provider itself is never stored — mod.rs invariant). One enum
/// rather than a trait: activation must stay synchronously callable from
/// the seal-work thread and the pre-pool boot path, and there are exactly
/// as many arms as deployment classes with a wrapper (RFC-021 nix,
/// RFC-026 macos-app). Report-only deployments resolve to None and keep
/// the git-release baseline.
#[derive(Debug, Clone)]
pub enum ActivationEnv {
    Nix(nix_provider::NixEnv),
    MacApp(macos_app::MacEnv),
}

impl ActivationEnv {
    pub fn from_env() -> Option<Self> {
        match std::env::var("HOPNET_UPGRADE_PROVIDER").ok().as_deref() {
            Some("nix") => nix_provider::NixEnv::from_env().map(Self::Nix),
            Some("macos-app") => macos_app::MacEnv::from_env().map(Self::MacApp),
            _ => None,
        }
    }

    pub fn provider_name(&self) -> &'static str {
        match self {
            Self::Nix(_) => "nix",
            Self::MacApp(_) => "macos-app",
        }
    }

    pub fn auto_stage(&self) -> bool {
        match self {
            Self::Nix(env) => env.auto_stage,
            Self::MacApp(env) => env.auto_stage,
        }
    }

    pub fn auto_activate(&self) -> bool {
        match self {
            Self::Nix(env) => env.auto_activate,
            Self::MacApp(env) => env.auto_activate,
        }
    }

    /// One activation attempt for the quorum-decided target. Sync on
    /// purpose; the contract (verify staged bytes, crash-loop guard,
    /// atomic flip, park-on-Err by the caller) is identical per arm.
    pub fn try_activate(&self, required_code: u32) -> Result<(), String> {
        match self {
            Self::Nix(env) => nix_provider::try_activate_with(env, required_code),
            Self::MacApp(env) => macos_app::try_activate_with(env, required_code),
        }
    }
}

/// The upgrade-provider seam (RFC-019): "readiness is deployment-
/// specific" — a package manager, a nix profile, a container image, and
/// a bare git checkout all have different notions of "staged and
/// applicable". Per-platform status is the provider's problem, not the
/// mesh's; deployment orchestration (how a binary actually gets swapped)
/// is explicitly out of scope behind this boundary.
///
/// Boxed futures for dyn-compatibility (the tick drives
/// `&dyn UpgradeProvider`); errors classify retry semantics only — the
/// calling cron retries, providers never loop.
pub trait UpgradeProvider: Send + Sync {
    fn name(&self) -> &'static str;

    /// Available version(s) this deployment can reach, and whether each
    /// is staged (downloaded/installed, pending activation).
    fn report(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<ProviderReport, ProviderError>> + Send + '_>>;

    /// Pre-fetch hook for deployments that can stage. Default:
    /// unsupported — the v1 git-release provider only reports.
    fn stage<'a>(
        &'a self,
        _version: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), ProviderError>> + Send + 'a>> {
        Box::pin(async { Err(ProviderError::Unsupported) })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProviderReport {
    /// Newest first.
    pub available: Vec<AvailableVersion>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AvailableVersion {
    pub version: String,
    pub staged: bool,
    pub prerelease: bool,
}

#[derive(Debug)]
pub enum ProviderError {
    /// Worth retrying next tick (network, 5xx, decode).
    Transient(String),
    /// Retrying won't help (4xx, bad config).
    Permanent(String),
    /// This provider cannot stage.
    Unsupported,
}

impl std::fmt::Display for ProviderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transient(msg) => write!(f, "transient: {msg}"),
            Self::Permanent(msg) => write!(f, "permanent: {msg}"),
            Self::Unsupported => write!(f, "provider cannot stage"),
        }
    }
}

/// Node-local upgrade runtime state on AppState. The provider itself is
/// NOT stored: the tick constructs the concrete provider from
/// committed-at-tick config, so config changes go live with no
/// invalidation machinery — the dyn seam is still the interface.
#[derive(Default)]
pub struct UpgradeState {
    pub last: tokio::sync::RwLock<Option<ProviderStatus>>,
}

/// Outcome of the most recent provider poll, feeding the advisory route.
#[derive(Debug, Clone)]
pub struct ProviderStatus {
    pub provider: &'static str,
    pub fetched_at: chrono::DateTime<chrono::Utc>,
    /// Err = stringified ProviderError.
    pub result: Result<ProviderReport, String>,
}
