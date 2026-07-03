//! Server configuration (TOML).
//!
//! Server-owned — the per-library NAS-local `blob_root`/`sidecar_root` differ
//! from the Mac daemon's mount paths and are not recorded in `state.db`. This
//! is deliberately NOT `ingress_core::LibraryConfig` (that carries
//! `sidecar_root_remote`/`scope_binding`/`retention_days` — a different shape
//! for a different job).

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// Listen address, e.g. `0.0.0.0:8080`.
    pub bind: SocketAddr,
    /// Root for generated thumbnail/display renditions (unused until the
    /// Renderer slice; parsed now so the config shape is stable).
    pub cache_dir: PathBuf,
    /// Server-owned SQLite read index.
    pub index_db: PathBuf,
    /// Incremental refresh cadence.
    #[serde(default = "default_refresh_secs")]
    pub refresh_interval_secs: u64,
    pub libraries: Vec<LibraryEntry>,
    /// OIDC settings — parsed-but-unused placeholder; consumed in the auth slice.
    #[serde(default)]
    pub oidc: Option<OidcConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LibraryEntry {
    /// Must satisfy `LibraryId::parse` (lowercase `[a-z0-9_]`); validated on load.
    pub library_id: String,
    pub display_name: String,
    /// Local path whose `blobs/` subtree holds the content-addressed files.
    pub blob_root: PathBuf,
    /// Local path to this library's sidecar tree (`YYYY/MM/<photo_id>.json`).
    pub sidecar_root: PathBuf,
}

/// Deferred to the auth slice — parsed but not consumed here.
#[derive(Debug, Clone, Deserialize)]
pub struct OidcConfig {
    pub issuer: String,
    pub client_id: String,
    pub client_secret: String,
    pub redirect_uri: String,
}

fn default_refresh_secs() -> u64 {
    60
}

impl Config {
    /// Read + parse + validate. Rejects any `library_id` that is not a valid
    /// `LibraryId` (it becomes a filesystem path component and a SQL key).
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let raw = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("read config {}: {e}", path.display()))?;
        let cfg: Config = toml::from_str(&raw)?;
        if cfg.libraries.is_empty() {
            anyhow::bail!("config has no [[libraries]]");
        }
        for lib in &cfg.libraries {
            ingress_core::LibraryId::parse(&lib.library_id)
                .map_err(|e| anyhow::anyhow!("invalid library_id {:?}: {e}", lib.library_id))?;
        }
        Ok(cfg)
    }
}
