//! v1 upgrade provider: upstream git repo release check — our current
//! distribution (RFC-019). Reports available-but-unstaged ONLY: a bare
//! git checkout has no staging notion, so `stage` stays the trait's
//! Unsupported default and the only version a node can honestly attest
//! is its running one. All outbound HTTP in the node lives in this file.

use std::future::Future;
use std::pin::Pin;

use hopnet_common::release_feed::{self, ForgejoRelease};

use super::{AvailableVersion, ProviderError, ProviderReport, UpgradeProvider};

pub struct GitReleaseProvider {
    releases_url: String,
    client: reqwest::Client,
}

impl GitReleaseProvider {
    pub fn new(releases_url: String) -> Self {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(15))
            .connect_timeout(std::time::Duration::from_secs(15))
            .build()
            .expect("static reqwest client config");
        Self {
            releases_url,
            client,
        }
    }

    /// Forgejo releases endpoint derived from the crate's repository
    /// field: {instance}/api/v1/repos/{owner}/{repo}/releases (the same
    /// endpoints the release workflow publishes through).
    pub fn default_releases_url() -> String {
        release_feed::releases_url(env!("CARGO_PKG_REPOSITORY"))
    }

    /// The raw feed — shared by this provider's report and the macOS app
    /// provider, whose availability semantics need the asset lists.
    pub async fn fetch_releases(&self) -> Result<Vec<ForgejoRelease>, ProviderError> {
        let response = self
            .client
            .get(&self.releases_url)
            .header("Accept", "application/json")
            .send()
            .await
            .map_err(|e| ProviderError::Transient(format!("request: {e}")))?;
        let status = response.status();
        if status.is_client_error() {
            return Err(ProviderError::Permanent(format!(
                "{status} from {}",
                self.releases_url
            )));
        }
        if !status.is_success() {
            return Err(ProviderError::Transient(format!(
                "{status} from {}",
                self.releases_url
            )));
        }
        response
            .json()
            .await
            .map_err(|e| ProviderError::Transient(format!("decode: {e}")))
    }

    /// One release by tag ({releases_url}/tags/{tag}). `Ok(None)` on 404 —
    /// for the macOS app channel a missing release is a hold, not an error
    /// (asset-attached availability, RFC-026).
    pub async fn fetch_release_by_tag(
        &self,
        tag: &str,
    ) -> Result<Option<ForgejoRelease>, ProviderError> {
        let url = format!("{}/tags/{tag}", self.releases_url);
        let response = self
            .client
            .get(&url)
            .header("Accept", "application/json")
            .send()
            .await
            .map_err(|e| ProviderError::Transient(format!("request: {e}")))?;
        let status = response.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if status.is_client_error() {
            return Err(ProviderError::Permanent(format!("{status} from {url}")));
        }
        if !status.is_success() {
            return Err(ProviderError::Transient(format!("{status} from {url}")));
        }
        response
            .json::<ForgejoRelease>()
            .await
            .map(Some)
            .map_err(|e| ProviderError::Transient(format!("decode: {e}")))
    }

    /// Download a release asset to `dest`. Network and HTTP failures are
    /// Transient — the asset provably exists in the release listing that
    /// led here, so absence is a race with the forge, not a refusal.
    pub async fn download_asset(
        &self,
        url: &str,
        dest: &std::path::Path,
    ) -> Result<(), ProviderError> {
        let bytes = self.fetch_bytes(url).await?;
        tokio::fs::write(dest, &bytes)
            .await
            .map_err(|e| ProviderError::Permanent(format!("write {}: {e}", dest.display())))
    }

    /// Fetch a small text asset (the .sha256 sidecar).
    pub async fn fetch_text(&self, url: &str) -> Result<String, ProviderError> {
        let bytes = self.fetch_bytes(url).await?;
        String::from_utf8(bytes.to_vec())
            .map_err(|e| ProviderError::Permanent(format!("non-utf8 asset from {url}: {e}")))
    }

    async fn fetch_bytes(&self, url: &str) -> Result<bytes::Bytes, ProviderError> {
        let response = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|e| ProviderError::Transient(format!("request: {e}")))?;
        let status = response.status();
        if !status.is_success() {
            return Err(ProviderError::Transient(format!("{status} from {url}")));
        }
        response
            .bytes()
            .await
            .map_err(|e| ProviderError::Transient(format!("body: {e}")))
    }
}

/// Feed entries as the node reports them: staged ALWAYS false here (v1
/// reports available-but-unstaged only; the nix provider re-marks).
fn parse_releases(releases: Vec<ForgejoRelease>) -> Vec<AvailableVersion> {
    release_feed::parse_releases(releases)
        .into_iter()
        .map(|r| AvailableVersion {
            version: r.version,
            staged: false,
            prerelease: r.prerelease,
        })
        .collect()
}

impl UpgradeProvider for GitReleaseProvider {
    fn name(&self) -> &'static str {
        "git-release"
    }

    fn report(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<ProviderReport, ProviderError>> + Send + '_>> {
        Box::pin(async move {
            let releases = self.fetch_releases().await?;
            Ok(ProviderReport {
                available: parse_releases(releases),
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Should: map every feed entry to an unstaged AvailableVersion —
    // staging claims are the nix provider's to make, never the feed's.
    #[test]
    fn maps_releases_to_unstaged_available_versions() {
        let releases: Vec<ForgejoRelease> = serde_json::from_str(
            r#"[
                {"tag_name": "v2026.8.1", "prerelease": true},
                {"tag_name": "v2026.8.0"}
            ]"#,
        )
        .unwrap();
        let available = parse_releases(releases);
        assert_eq!(
            available,
            vec![
                AvailableVersion {
                    version: "2026.8.1".into(),
                    staged: false,
                    prerelease: true,
                },
                AvailableVersion {
                    version: "2026.8.0".into(),
                    staged: false,
                    prerelease: false,
                },
            ]
        );
    }

    // Should: derive the default releases URL from the crate's
    // repository field ({instance}/api/v1/repos/{owner}/{repo}/releases).
    #[test]
    fn derives_default_releases_url() {
        assert_eq!(
            GitReleaseProvider::default_releases_url(),
            "https://git.bentley.sh/api/v1/repos/HopNet/HopNet/releases"
        );
    }
}
