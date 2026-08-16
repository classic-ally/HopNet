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

            let releases: Vec<ForgejoRelease> = response
                .json()
                .await
                .map_err(|e| ProviderError::Transient(format!("decode: {e}")))?;
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
