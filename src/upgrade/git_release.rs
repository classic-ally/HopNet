//! v1 upgrade provider: upstream git repo release check — our current
//! distribution (RFC-019). Reports available-but-unstaged ONLY: a bare
//! git checkout has no staging notion, so `stage` stays the trait's
//! Unsupported default and the only version a node can honestly attest
//! is its running one. All outbound HTTP in the node lives in this file.

use std::future::Future;
use std::pin::Pin;

use serde::Deserialize;

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
        let repository = env!("CARGO_PKG_REPOSITORY");
        let (instance, path) = repository
            .rsplit_once('/')
            .and_then(|(rest, repo)| {
                rest.rsplit_once('/')
                    .map(|(instance, owner)| (instance.to_string(), format!("{owner}/{repo}")))
            })
            .expect("CARGO_PKG_REPOSITORY is {instance}/{owner}/{repo}");
        format!("{instance}/api/v1/repos/{path}/releases")
    }
}

#[derive(Debug, Deserialize)]
struct ForgejoRelease {
    tag_name: String,
    #[serde(default)]
    prerelease: bool,
    #[serde(default)]
    draft: bool,
}

/// Pure translation of a Forgejo releases payload: drafts dropped, the
/// tag's leading 'v' stripped, prerelease from the flag or a '-' in the
/// version, staged ALWAYS false (v1 reports available-but-unstaged
/// only), newest first — CalVer tokens by integer code descending, then
/// non-CalVer legacy tags lexicographically descending.
fn parse_releases(releases: Vec<ForgejoRelease>) -> Vec<AvailableVersion> {
    let mut available: Vec<AvailableVersion> = releases
        .into_iter()
        .filter(|r| !r.draft)
        .map(|r| {
            let version = r
                .tag_name
                .strip_prefix('v')
                .unwrap_or(&r.tag_name)
                .to_string();
            let prerelease = r.prerelease || version.contains('-');
            AvailableVersion {
                version,
                staged: false,
                prerelease,
            }
        })
        .collect();
    available.sort_by(|a, b| {
        match (
            crate::version::parse_code(&a.version),
            crate::version::parse_code(&b.version),
        ) {
            (Some(a), Some(b)) => b.cmp(&a),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => b.version.cmp(&a.version),
        }
    });
    available
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

    // Should: parse a canned Forgejo releases payload — drafts dropped,
    // the 'v' prefix stripped, prerelease from the flag or a '-' in the
    // version, staged always false (v1 reports available-but-unstaged
    // only), newest first with legacy non-CalVer tags after every
    // CalVer token.
    #[test]
    fn parses_canned_releases_payload() {
        let releases: Vec<ForgejoRelease> = serde_json::from_str(
            r#"[
                {"tag_name": "v0.1.0-rc.2", "prerelease": false},
                {"tag_name": "v2026.8.0"},
                {"tag_name": "v2026.9.0", "draft": true},
                {"tag_name": "v2026.8.1", "prerelease": true},
                {"tag_name": "v0.1.0-rc.1"}
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
                AvailableVersion {
                    version: "0.1.0-rc.2".into(),
                    staged: false,
                    prerelease: true, // '-' in the version
                },
                AvailableVersion {
                    version: "0.1.0-rc.1".into(),
                    staged: false,
                    prerelease: true,
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
