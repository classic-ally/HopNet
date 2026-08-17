//! Forgejo release-feed parsing and flake-ref derivation, shared by every
//! upgrade channel (the node's provider, RFC-021; the mount wrapper,
//! RFC-024). Pure: fetching stays in the leaf crates, and the
//! `env!("CARGO_PKG_REPOSITORY")` evaluations stay leaf-side so each
//! binary derives from its own repository field.

use serde::Deserialize;

/// The subset of a Forgejo release entry the upgrade channels read.
#[derive(Debug, Deserialize)]
pub struct ForgejoRelease {
    pub tag_name: String,
    #[serde(default)]
    pub prerelease: bool,
    #[serde(default)]
    pub draft: bool,
    /// Uploaded artifacts. Defaulted so channels that only care about tags
    /// (nix builds from source) parse payloads without them unchanged; the
    /// macOS app channel keys AVAILABILITY on these (RFC-026: a tag means
    /// nothing until CI attaches the signed artifact).
    #[serde(default)]
    pub assets: Vec<ForgejoAsset>,
}

/// One uploaded release artifact.
#[derive(Debug, Clone, Deserialize)]
pub struct ForgejoAsset {
    pub name: String,
    pub browser_download_url: String,
}

/// The artifact filenames the macOS release workflow publishes for a
/// version: the app zip and its sha256 sidecar
/// (scripts/macos/06-package-zip.sh owns the writing half).
pub fn app_asset_names(version: &str) -> (String, String) {
    let zip = format!("HopNet-v{version}-arm64.app.zip");
    let sha = format!("{zip}.sha256");
    (zip, sha)
}

/// Find a release's app-zip + sha256 asset URLs, if both are attached.
pub fn app_asset_urls(release: &ForgejoRelease, version: &str) -> Option<(String, String)> {
    let (zip_name, sha_name) = app_asset_names(version);
    let url_of = |name: &str| {
        release
            .assets
            .iter()
            .find(|a| a.name == name)
            .map(|a| a.browser_download_url.clone())
    };
    Some((url_of(&zip_name)?, url_of(&sha_name)?))
}

/// A published release as the channels see it.
#[derive(Debug, Clone, PartialEq)]
pub struct Release {
    pub version: String,
    pub prerelease: bool,
}

/// Pure translation of a Forgejo releases payload: drafts dropped, the
/// tag's leading 'v' stripped, prerelease from the flag or a '-' in the
/// version, newest first — CalVer tokens by integer code descending,
/// then non-CalVer legacy tags lexicographically descending.
pub fn parse_releases(releases: Vec<ForgejoRelease>) -> Vec<Release> {
    let mut available: Vec<Release> = releases
        .into_iter()
        .filter(|r| !r.draft)
        .map(|r| {
            let version = r
                .tag_name
                .strip_prefix('v')
                .unwrap_or(&r.tag_name)
                .to_string();
            let prerelease = r.prerelease || version.contains('-');
            Release {
                version,
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

/// Forgejo releases endpoint derived from a crate's repository field:
/// {instance}/api/v1/repos/{owner}/{repo}/releases (the same endpoints
/// the release workflow publishes through).
pub fn releases_url(repository: &str) -> String {
    let (instance, path) = repository
        .rsplit_once('/')
        .and_then(|(rest, repo)| {
            rest.rsplit_once('/')
                .map(|(instance, owner)| (instance.to_string(), format!("{owner}/{repo}")))
        })
        .expect("repository is {instance}/{owner}/{repo}");
    format!("{instance}/api/v1/repos/{path}/releases")
}

/// Base flake ref derived from a crate's repository field — the release
/// page and the staged source stay the same single upstream. The `.git`
/// suffix matches the form the deployments' flake inputs already use.
pub fn flake_ref(repository: &str) -> String {
    if repository.ends_with(".git") {
        format!("git+{repository}")
    } else {
        format!("git+{repository}.git")
    }
}

/// The flake ref for a release tag. `refs/tags/` is NOT decoration: nix
/// resolves a bare `?ref=X` under `refs/heads/`, so a release tag asked
/// for by name fails with "couldn't find remote ref refs/heads/vX" — which
/// is exactly how the first real staging attempt died, on all three nodes
/// at once, with the fake `nix` in every test happily ignoring the ref.
pub fn tag_ref(flake_ref: &str, version: &str) -> String {
    format!("{flake_ref}?ref=refs/tags/v{version}")
}

#[cfg(test)]
mod tests {
    use super::*;

    // Should: parse a canned Forgejo releases payload — drafts dropped,
    // the 'v' prefix stripped, prerelease from the flag or a '-' in the
    // version, newest first with legacy non-CalVer tags after every
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
                Release {
                    version: "2026.8.1".into(),
                    prerelease: true,
                },
                Release {
                    version: "2026.8.0".into(),
                    prerelease: false,
                },
                Release {
                    version: "0.1.0-rc.2".into(),
                    prerelease: true, // '-' in the version
                },
                Release {
                    version: "0.1.0-rc.1".into(),
                    prerelease: true,
                },
            ]
        );
    }

    // Should: derive a releases URL of the form
    // {instance}/api/v1/repos/{owner}/{repo}/releases.
    #[test]
    fn derives_releases_url_from_a_repository_field() {
        assert_eq!(
            releases_url("https://git.bentley.sh/HopNet/HopNet"),
            "https://git.bentley.sh/api/v1/repos/HopNet/HopNet/releases"
        );
    }

    // Should: produce git+{repository}.git.
    // Should not: double a .git suffix already present.
    #[test]
    fn derives_flake_ref_with_git_suffix() {
        assert_eq!(
            flake_ref("https://git.bentley.sh/HopNet/HopNet"),
            "git+https://git.bentley.sh/HopNet/HopNet.git"
        );
        assert_eq!(
            flake_ref("https://git.bentley.sh/HopNet/HopNet.git"),
            "git+https://git.bentley.sh/HopNet/HopNet.git"
        );
    }

    // Impact: asset-attached availability is the macOS class's whole
    // availability semantics — a tag with no artifact must read as
    // nothing-to-stage, never as a half-available release.
    // Should: return both asset URLs when the zip and its sha256 sidecar
    //         are attached under the workflow's exact filenames.
    // Should not: return anything when either sidecar or zip is missing.
    #[test]
    fn app_asset_urls_require_both_zip_and_sidecar() {
        let full: ForgejoRelease = serde_json::from_str(
            r#"{"tag_name": "v2026.8.5", "assets": [
                {"name": "HopNet-v2026.8.5-arm64.app.zip", "browser_download_url": "https://x/zip"},
                {"name": "HopNet-v2026.8.5-arm64.app.zip.sha256", "browser_download_url": "https://x/sha"}
            ]}"#,
        )
        .unwrap();
        assert_eq!(
            app_asset_urls(&full, "2026.8.5"),
            Some(("https://x/zip".into(), "https://x/sha".into()))
        );

        let zip_only: ForgejoRelease = serde_json::from_str(
            r#"{"tag_name": "v2026.8.5", "assets": [
                {"name": "HopNet-v2026.8.5-arm64.app.zip", "browser_download_url": "https://x/zip"}
            ]}"#,
        )
        .unwrap();
        assert_eq!(app_asset_urls(&zip_only, "2026.8.5"), None);

        let bare: ForgejoRelease = serde_json::from_str(r#"{"tag_name": "v2026.8.5"}"#).unwrap();
        assert_eq!(app_asset_urls(&bare, "2026.8.5"), None);
    }

    // Impact: the tag ref is the one string in the upgrade channels that
    // only a real nix and a real forge can validate — every provider test
    // and the VM test stub the nix binary, so a wrong ref namespace sails
    // through all of them and dies on the first genuine release. It did:
    // nix resolves a bare `?ref=X` under refs/heads, so `?ref=v2026.8.1`
    // asked for a BRANCH and all three nodes failed with "couldn't find
    // remote ref refs/heads/v2026.8.1".
    // Should: address a release tag under refs/tags.
    #[test]
    fn tag_ref_addresses_the_tag_namespace() {
        assert_eq!(
            tag_ref("git+https://example.invalid/HopNet.git", "2026.8.1"),
            "git+https://example.invalid/HopNet.git?ref=refs/tags/v2026.8.1"
        );
    }
}
