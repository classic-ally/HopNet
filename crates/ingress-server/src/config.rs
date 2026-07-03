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
    /// Group-based access rule (TOML: `access = { groups = ["shared_photo_library"] }`).
    #[serde(default)]
    pub access: LibraryAccess,
}

/// Per-library authorization: a session may access this library iff its OIDC
/// `groups` claim intersects this set. An unlisted library denies by default.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct LibraryAccess {
    #[serde(default)]
    pub groups: Vec<String>,
}

/// pocket-id OIDC settings. The client secret is NEVER in the TOML — it comes
/// from `$INGRESS_SERVER_OIDC_CLIENT_SECRET` so it never lands in git.
#[derive(Debug, Clone, Deserialize)]
pub struct OidcConfig {
    pub issuer: String,
    pub client_id: String,
    pub redirect_uri: String,
    #[serde(default)]
    pub post_logout_redirect_uri: Option<String>,
    /// Force the cookie `Secure` flag off (localhost dev over http). When
    /// unset, inferred from the redirect_uri scheme.
    #[serde(default)]
    pub dev_insecure_cookies: Option<bool>,
}

impl OidcConfig {
    /// Read the client secret from the environment. Errors if unset.
    pub fn client_secret(&self) -> anyhow::Result<String> {
        std::env::var("INGRESS_SERVER_OIDC_CLIENT_SECRET")
            .map_err(|_| anyhow::anyhow!("INGRESS_SERVER_OIDC_CLIENT_SECRET is unset"))
    }

    /// Cookie `Secure`: true for an https redirect (prod), false for http
    /// localhost dev. Explicit `dev_insecure_cookies` overrides.
    pub fn secure_cookies(&self) -> bool {
        match self.dev_insecure_cookies {
            Some(dev) => !dev,
            None => self.redirect_uri.starts_with("https://"),
        }
    }
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
