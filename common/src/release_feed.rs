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
