//! The macOS app upgrade provider (RFC-026 S3): fetch-certified-artifact
//! staging. Where the nix provider BUILDS its staged bytes from source,
//! this class FETCHES the CI-signed .app zip attached to the release —
//! codesign, entitlements and notarization are not locally reproducible,
//! so the artifact, never a rebuild, is the unit of truth. Availability is
//! asset-attached: a tag with no darwin artifact reads as nothing-to-stage
//! (the CI window between tag and asset is a hold, not an error).
//!
//! Activation mirrors the nix provider deliberately: same profile flip,
//! same crash-loop guard, same park-on-failure contract — `try_activate*`
//! stays synchronous so the seal-work thread and the pre-pool boot path
//! can call it. The staged verification authority is the bundle binary's
//! own `--version` (S1 stamps the bundle plists from the same workspace
//! token, so one read suffices); codesign + staple are verified once at
//! stage time, where the bytes first arrive.

use std::path::{Path, PathBuf};

use std::future::Future;
use std::pin::Pin;

use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use super::git_release::GitReleaseProvider;
use super::{AvailableVersion, ProviderError, ProviderReport, UpgradeProvider};
use hopnet_common::release_feed;

/// Deployment contract (mirrors the nix provider's env names where the
/// semantics match; tool paths are overridable so tests substitute stubs —
/// the fake-nix pattern — which also keeps this module compiling and
/// testable on Linux: everything is subprocess work, no objc).
#[derive(Debug, Clone)]
pub struct MacEnv {
    pub stage_dir: PathBuf,
    pub profile: PathBuf,
    pub auto_stage: bool,
    pub auto_activate: bool,
    pub codesign_bin: PathBuf,
    pub xcrun_bin: PathBuf,
    pub ditto_bin: PathBuf,
}

impl MacEnv {
    /// Resolve the deployment contract. None unless
    /// `HOPNET_UPGRADE_PROVIDER=macos-app` and both paths are declared —
    /// a partial contract warns and disables, like the nix provider's.
    pub fn from_env() -> Option<Self> {
        if std::env::var("HOPNET_UPGRADE_PROVIDER").ok().as_deref() != Some("macos-app") {
            return None;
        }
        let profile = std::env::var_os("HOPNET_UPGRADE_PROFILE").map(PathBuf::from);
        let stage_dir = std::env::var_os("HOPNET_UPGRADE_STAGE_DIR").map(PathBuf::from);
        let (Some(profile), Some(stage_dir)) = (profile, stage_dir) else {
            warn!(
                "HOPNET_UPGRADE_PROVIDER=macos-app but PROFILE/STAGE_DIR incomplete — provider disabled"
            );
            return None;
        };
        let knob = |key: &str| std::env::var(key).map(|v| v != "0").unwrap_or(true);
        let tool = |key: &str, default: &str| {
            std::env::var_os(key)
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from(default))
        };
        Some(Self {
            stage_dir,
            profile,
            auto_stage: knob("HOPNET_UPGRADE_AUTO_STAGE"),
            auto_activate: knob("HOPNET_UPGRADE_AUTO_ACTIVATE"),
            codesign_bin: tool("HOPNET_UPGRADE_MACOS_CODESIGN_BIN", "/usr/bin/codesign"),
            xcrun_bin: tool("HOPNET_UPGRADE_MACOS_XCRUN_BIN", "/usr/bin/xcrun"),
            ditto_bin: tool("HOPNET_UPGRADE_MACOS_DITTO_BIN", "/usr/bin/ditto"),
        })
    }

    fn staged_root(&self, version: &str) -> PathBuf {
        self.stage_dir.join(format!("v{version}"))
    }

    pub(crate) fn bundle_path(&self, version: &str) -> PathBuf {
        self.staged_root(version).join("HopNet.app")
    }

    fn provenance_path(&self, version: &str) -> PathBuf {
        self.stage_dir.join(format!("v{version}.json"))
    }

    fn zip_path(&self, version: &str) -> PathBuf {
        self.stage_dir.join(format!("v{version}.zip"))
    }
}

/// Stage-time record binding a staged bundle to the release it came from.
/// Written LAST, only after every verification passed — its presence is
/// part of the staged claim.
#[derive(Debug, Serialize, Deserialize)]
pub struct Provenance {
    pub version: String,
    pub release_url: String,
    pub asset_sha256: String,
    pub bundle_path: String,
}

pub struct MacAppProvider {
    env: MacEnv,
    releases: GitReleaseProvider,
}

impl MacAppProvider {
    pub fn new(env: MacEnv, releases_url: String) -> Self {
        Self {
            env,
            releases: GitReleaseProvider::new(releases_url),
        }
    }

    /// Steps after the bytes are on disk and their digest matched the
    /// sidecar: extract, verify Apple's chain, verify honest bytes, record
    /// provenance. Factored off the network half so the refusal logic is
    /// unit-testable with stub tools.
    async fn finish_stage(
        &self,
        version: &str,
        zip: &Path,
        release_url: &str,
        asset_sha256: &str,
    ) -> Result<(), ProviderError> {
        let root = self.env.staged_root(version);
        let bundle = self.env.bundle_path(version);

        // Re-extraction over a previous partial attempt must start clean.
        if root.exists() {
            std::fs::remove_dir_all(&root)
                .map_err(|e| ProviderError::Permanent(format!("clear {}: {e}", root.display())))?;
        }
        std::fs::create_dir_all(&root)
            .map_err(|e| ProviderError::Permanent(format!("create {}: {e}", root.display())))?;

        run_tool(
            &self.env.ditto_bin,
            &["-x", "-k", &zip.to_string_lossy(), &root.to_string_lossy()],
            "extract",
        )
        .await?;
        if !bundle.is_dir() {
            return Err(ProviderError::Permanent(format!(
                "archive did not contain HopNet.app (looked at {})",
                bundle.display()
            )));
        }

        // Apple's chain: the signature seals the entitlements too, so
        // "went through the release pipeline" is machine-checked here.
        run_tool(
            &self.env.codesign_bin,
            &["--verify", "--deep", "--strict", &bundle.to_string_lossy()],
            "codesign verify",
        )
        .await?;
        if skip_staple() {
            warn!("staple validation SKIPPED (test mode override)");
        } else {
            run_tool(
                &self.env.xcrun_bin,
                &["stapler", "validate", &bundle.to_string_lossy()],
                "staple validate",
            )
            .await?;
        }

        // Honest bytes: the bundle's own binary must answer the tag.
        let answered = staged_binary_version(&bundle).await?;
        if answered != version {
            return Err(ProviderError::Permanent(format!(
                "staged bundle answers --version {answered}, expected {version} — \
                 the release's bytes disagree with its tag"
            )));
        }

        let provenance = Provenance {
            version: version.to_string(),
            release_url: release_url.to_string(),
            asset_sha256: asset_sha256.to_string(),
            bundle_path: bundle.to_string_lossy().into_owned(),
        };
        let json = serde_json::to_string_pretty(&provenance)
            .map_err(|e| ProviderError::Permanent(format!("provenance encode: {e}")))?;
        std::fs::write(self.env.provenance_path(version), json)
            .map_err(|e| ProviderError::Permanent(format!("provenance write: {e}")))?;
        let _ = std::fs::remove_file(zip);
        info!(version, "staged and verified certified artifact");
        Ok(())
    }
}

impl UpgradeProvider for MacAppProvider {
    fn name(&self) -> &'static str {
        "macos-app"
    }

    fn report(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<ProviderReport, ProviderError>> + Send + '_>> {
        Box::pin(async move {
            let raw = self.releases.fetch_releases().await?;
            // Asset-attached availability: a release without the darwin
            // artifact pair is invisible to this class.
            let with_assets: Vec<_> = raw
                .into_iter()
                .filter(|r| {
                    let version = r.tag_name.strip_prefix('v').unwrap_or(&r.tag_name);
                    release_feed::app_asset_urls(r, version).is_some()
                })
                .collect();
            let mut available: Vec<AvailableVersion> = release_feed::parse_releases(with_assets)
                .into_iter()
                .map(|r| AvailableVersion {
                    version: r.version,
                    staged: false,
                    prerelease: r.prerelease,
                })
                .collect();
            mark_staged(&self.env, &mut available);
            Ok(ProviderReport { available })
        })
    }

    fn stage<'a>(
        &'a self,
        version: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), ProviderError>> + Send + 'a>> {
        Box::pin(async move {
            if crate::version::parse_code(version).is_none() {
                return Err(ProviderError::Permanent(format!(
                    "not a CalVer version token: {version}"
                )));
            }
            std::fs::create_dir_all(&self.env.stage_dir)
                .map_err(|e| ProviderError::Permanent(format!("create stage dir: {e}")))?;

            let Some(release) = self
                .releases
                .fetch_release_by_tag(&format!("v{version}"))
                .await?
            else {
                return Err(ProviderError::Transient(format!(
                    "release v{version} not published yet — holding"
                )));
            };
            let Some((zip_url, sha_url)) = release_feed::app_asset_urls(&release, version) else {
                return Err(ProviderError::Transient(format!(
                    "release v{version} has no darwin artifact yet — holding"
                )));
            };

            let zip = self.env.zip_path(version);
            self.releases.download_asset(&zip_url, &zip).await?;
            let sidecar = self.releases.fetch_text(&sha_url).await?;
            let expected = sidecar
                .split_whitespace()
                .next()
                .unwrap_or("")
                .to_ascii_lowercase();
            let actual = sha256_hex_of_file(&zip)?;
            if expected.is_empty() || actual != expected {
                let _ = std::fs::remove_file(&zip);
                return Err(ProviderError::Permanent(format!(
                    "asset digest mismatch for v{version}: sidecar {expected}, zip {actual}"
                )));
            }

            self.finish_stage(version, &zip, &zip_url, &actual).await
        })
    }
}

fn skip_staple() -> bool {
    // Test-mode-gated (the HOPNET_MIN_CLIENT_OVERRIDE seam pattern):
    // locally built e2e bundles are signed but not notarized. A release
    // deployment can never take this branch.
    crate::version::test_mode() && std::env::var("HOPNET_UPGRADE_MACOS_SKIP_STAPLE").is_ok()
}

fn sha256_hex_of_file(path: &Path) -> Result<String, ProviderError> {
    let bytes = std::fs::read(path)
        .map_err(|e| ProviderError::Permanent(format!("read {}: {e}", path.display())))?;
    Ok(hex::encode(ring::digest::digest(
        &ring::digest::SHA256,
        &bytes,
    )))
}

async fn run_tool(bin: &Path, args: &[&str], what: &str) -> Result<(), ProviderError> {
    let output = tokio::process::Command::new(bin)
        .args(args)
        .output()
        .await
        .map_err(|e| ProviderError::Transient(format!("{what}: spawn {}: {e}", bin.display())))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let tail: Vec<&str> = stderr.lines().rev().take(3).collect();
        return Err(ProviderError::Permanent(format!(
            "{what} failed ({}): {}",
            output.status,
            tail.into_iter().rev().collect::<Vec<_>>().join(" | ")
        )));
    }
    Ok(())
}

/// The staged bundle's own `--version` answer (async flavor, stage-time).
async fn staged_binary_version(bundle: &Path) -> Result<String, ProviderError> {
    let binary = bundle.join("Contents/MacOS/HopNet");
    let output = tokio::process::Command::new(&binary)
        .arg("--version")
        .output()
        .await
        .map_err(|e| ProviderError::Permanent(format!("run {}: {e}", binary.display())))?;
    if !output.status.success() {
        return Err(ProviderError::Permanent(format!(
            "staged binary --version exited {}",
            output.status
        )));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

/// Re-verify staged claims before reporting them (the mark_staged
/// discipline: `staged: X` means the bytes are on disk and answer X NOW,
/// not that a stage once succeeded).
pub(crate) fn mark_staged(env: &MacEnv, available: &mut [AvailableVersion]) {
    for v in available.iter_mut() {
        v.staged = verify_staged(env, &v.version).is_ok();
    }
}

/// The staged-bytes check backing both `report()` and activation: bundle
/// present, provenance agreeing, and the binary answering the version.
/// Returns the bundle path activation flips to.
pub(crate) fn verify_staged(env: &MacEnv, version: &str) -> Result<PathBuf, String> {
    let bundle = env.bundle_path(version);
    if !bundle.is_dir() {
        return Err(format!("no staged bundle at {}", bundle.display()));
    }
    let raw = std::fs::read_to_string(env.provenance_path(version))
        .map_err(|e| format!("provenance: {e}"))?;
    let prov: Provenance = serde_json::from_str(&raw).map_err(|e| format!("provenance: {e}"))?;
    if prov.version != version {
        return Err(format!(
            "provenance names {} but the link is for {version}",
            prov.version
        ));
    }
    if Path::new(&prov.bundle_path) != bundle {
        return Err(format!(
            "provenance bundle path {} disagrees with {}",
            prov.bundle_path,
            bundle.display()
        ));
    }
    let binary = bundle.join("Contents/MacOS/HopNet");
    let output = std::process::Command::new(&binary)
        .arg("--version")
        .output()
        .map_err(|e| format!("run {}: {e}", binary.display()))?;
    let answered = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if !output.status.success() || answered != version {
        return Err(format!(
            "staged binary answers '{answered}', expected {version}"
        ));
    }
    Ok(bundle)
}

/// Activate the staged bundle for `required_code`: verify honest bytes,
/// refuse a re-flip of the already-active generation (the crash-loop
/// guard, verbatim from the nix provider), then atomically flip the
/// profile symlink. The caller exits 75; launchd relaunches through the
/// profile and the S2 healer re-registers the ingress daemon.
pub(crate) fn try_activate_with(env: &MacEnv, required_code: u32) -> Result<(), String> {
    if !env.auto_activate {
        return Err("auto-activation disabled by deployment".into());
    }
    let version = crate::version::format_code(required_code);
    let target = verify_staged(env, &version)?;

    if std::fs::read_link(&env.profile).is_ok_and(|cur| cur == target) {
        return Err(format!(
            "profile already points at staged {version} yet the running version is still \
             wrong — refusing to re-flip (crash-loop guard)"
        ));
    }

    let tmp = env
        .profile
        .with_extension(format!("next.{}", std::process::id()));
    let _ = std::fs::remove_file(&tmp);
    std::os::unix::fs::symlink(&target, &tmp).map_err(|e| format!("symlink: {e}"))?;
    std::fs::rename(&tmp, &env.profile).map_err(|e| {
        let _ = std::fs::remove_file(&tmp);
        format!("profile flip: {e}")
    })?;
    info!(%version, "profile flipped to staged certified artifact");
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    fn tmpdir(tag: &str) -> tempfile::TempDir {
        tempfile::Builder::new().prefix(tag).tempdir().unwrap()
    }

    fn write_tool(dir: &Path, name: &str, body: &str) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let path = dir.join(name);
        std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        path
    }

    /// A MacEnv whose Apple tools are stubs: codesign/stapler succeed, and
    /// "ditto -x -k zip root" is a tar extraction — the tests' "zips" are
    /// tars, which keeps the fixtures pure-shell on every platform.
    pub(crate) fn test_env(dir: &Path) -> MacEnv {
        MacEnv {
            stage_dir: dir.join("staged"),
            profile: dir.join("profile"),
            auto_stage: true,
            auto_activate: true,
            codesign_bin: write_tool(dir, "fake-codesign", "exit 0"),
            xcrun_bin: write_tool(dir, "fake-xcrun", "exit 0"),
            ditto_bin: write_tool(
                dir,
                "fake-ditto",
                r#"mkdir -p "$4" && tar -xf "$3" -C "$4""#,
            ),
        }
    }

    /// A "release zip" (really a tar, see test_env): HopNet.app whose
    /// binary answers `answers` to --version.
    fn make_artifact(dir: &Path, answers: &str) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let root = dir.join("artifact-src");
        let macos = root.join("HopNet.app/Contents/MacOS");
        std::fs::create_dir_all(&macos).unwrap();
        let binary = macos.join("HopNet");
        std::fs::write(&binary, format!("#!/bin/sh\necho {answers}\n")).unwrap();
        std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o755)).unwrap();
        let tarball = dir.join("artifact.tar");
        assert!(
            std::process::Command::new("tar")
                .args([
                    "-cf",
                    &tarball.to_string_lossy(),
                    "-C",
                    &root.to_string_lossy(),
                    "HopNet.app"
                ])
                .status()
                .unwrap()
                .success()
        );
        tarball
    }

    /// Plant an already-staged bundle + provenance, the fixture the
    /// verify/activate tests share (the nix provider's plant_staged).
    pub(crate) fn plant_staged(
        dir: &Path,
        env: &MacEnv,
        version: &str,
        with_provenance: bool,
    ) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let bundle = env.bundle_path(version);
        let macos = bundle.join("Contents/MacOS");
        std::fs::create_dir_all(&macos).unwrap();
        let binary = macos.join("HopNet");
        std::fs::write(&binary, format!("#!/bin/sh\necho {version}\n")).unwrap();
        std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o755)).unwrap();
        if with_provenance {
            let prov = Provenance {
                version: version.to_string(),
                release_url: "https://example.invalid/zip".into(),
                asset_sha256: "0".repeat(64),
                bundle_path: bundle.to_string_lossy().into_owned(),
            };
            std::fs::write(
                env.provenance_path(version),
                serde_json::to_string_pretty(&prov).unwrap(),
            )
            .unwrap();
        }
        let _ = dir; // parity with the nix fixture signature
        bundle
    }

    fn block_on<F: std::future::Future>(fut: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(fut)
    }

    fn provider(env: &MacEnv) -> MacAppProvider {
        MacAppProvider::new(env.clone(), "https://example.invalid/releases".into())
    }

    // Impact: this is the honest-bytes rule for the certified-artifact
    // class — a staged claim must mean verified bytes on disk, and wrong
    // bytes under a present asset must be refused permanently.
    // Should: extract, verify, and record provenance for an artifact whose
    //         binary answers the version, deleting the downloaded archive.
    #[test]
    fn finish_stage_verifies_and_records_provenance() {
        let dir = tmpdir("mac-stage");
        let env = test_env(dir.path());
        std::fs::create_dir_all(&env.stage_dir).unwrap();
        let artifact = make_artifact(dir.path(), "2026.9.1");

        block_on(provider(&env).finish_stage("2026.9.1", &artifact, "https://x/zip", "cafe"))
            .unwrap();

        let prov: Provenance = serde_json::from_str(
            &std::fs::read_to_string(env.provenance_path("2026.9.1")).unwrap(),
        )
        .unwrap();
        assert_eq!(prov.version, "2026.9.1");
        assert_eq!(prov.asset_sha256, "cafe");
        assert!(!artifact.exists(), "archive deleted after staging");
        assert!(verify_staged(&env, "2026.9.1").is_ok());
    }

    // Should: refuse (Permanent) an artifact whose binary answers a
    //         different version than the release tag, without writing
    //         provenance.
    #[test]
    fn finish_stage_refuses_bytes_that_disagree_with_the_tag() {
        let dir = tmpdir("mac-wrong");
        let env = test_env(dir.path());
        std::fs::create_dir_all(&env.stage_dir).unwrap();
        let artifact = make_artifact(dir.path(), "2026.1.1");

        let err =
            block_on(provider(&env).finish_stage("2026.9.1", &artifact, "u", "s")).unwrap_err();
        assert!(matches!(err, ProviderError::Permanent(_)), "{err}");
        assert!(!env.provenance_path("2026.9.1").exists());
        assert!(verify_staged(&env, "2026.9.1").is_err());
    }

    // Should: refuse (Permanent) when the signature verification tool
    //         rejects the bundle — Apple's chain is part of the staged
    //         claim for this class.
    #[test]
    fn finish_stage_refuses_a_failed_signature_verification() {
        let dir = tmpdir("mac-badsig");
        let mut env = test_env(dir.path());
        env.codesign_bin = write_tool(dir.path(), "failing-codesign", "echo bad >&2; exit 1");
        std::fs::create_dir_all(&env.stage_dir).unwrap();
        let artifact = make_artifact(dir.path(), "2026.9.1");

        let err =
            block_on(provider(&env).finish_stage("2026.9.1", &artifact, "u", "s")).unwrap_err();
        assert!(matches!(err, ProviderError::Permanent(_)), "{err}");
        assert!(!env.provenance_path("2026.9.1").exists());
    }

    // Should: mark staged only versions whose bundle, provenance, and
    //         binary answer all agree — a missing provenance or a lying
    //         binary demotes the claim.
    #[test]
    fn report_marking_requires_bundle_provenance_and_binary_agreement() {
        let dir = tmpdir("mac-mark");
        let env = test_env(dir.path());
        std::fs::create_dir_all(&env.stage_dir).unwrap();
        plant_staged(dir.path(), &env, "2026.9.1", true);
        plant_staged(dir.path(), &env, "2026.9.2", false); // no provenance

        let mut available = vec![
            AvailableVersion {
                version: "2026.9.1".into(),
                staged: false,
                prerelease: false,
            },
            AvailableVersion {
                version: "2026.9.2".into(),
                staged: false,
                prerelease: false,
            },
            AvailableVersion {
                version: "2026.9.3".into(),
                staged: false,
                prerelease: false,
            },
        ];
        let direct = verify_staged(&env, "2026.9.1");
        assert!(direct.is_ok(), "direct verify: {direct:?}");
        mark_staged(&env, &mut available);
        assert!(available[0].staged);
        assert!(!available[1].staged);
        assert!(!available[2].staged);
    }

    // Should: flip the profile symlink to the staged bundle atomically.
    #[test]
    fn try_activate_flips_the_profile_to_the_staged_bundle() {
        let dir = tmpdir("mac-flip");
        let env = test_env(dir.path());
        std::fs::create_dir_all(&env.stage_dir).unwrap();
        let bundle = plant_staged(dir.path(), &env, "2026.9.1", true);

        try_activate_with(&env, crate::version::parse_code("2026.9.1").unwrap()).unwrap();
        assert_eq!(std::fs::read_link(&env.profile).unwrap(), bundle);
    }

    // Impact: without this refusal a failed flip exit-75s into the same
    // wrong binary forever — the crash-loop guard is what makes "never
    // crash-looping" mechanical (RFC-021 rule 3, inherited).
    // Should not: re-flip when the profile already points at the staged
    //             bundle yet the running version is still wrong.
    #[test]
    fn try_activate_refuses_a_reflip_of_the_active_generation() {
        let dir = tmpdir("mac-loop");
        let env = test_env(dir.path());
        std::fs::create_dir_all(&env.stage_dir).unwrap();
        let bundle = plant_staged(dir.path(), &env, "2026.9.1", true);
        std::os::unix::fs::symlink(&bundle, &env.profile).unwrap();

        let err =
            try_activate_with(&env, crate::version::parse_code("2026.9.1").unwrap()).unwrap_err();
        assert!(err.contains("crash-loop guard"), "{err}");
    }

    // Should: honor the deployment's auto-activate opt-out — the node
    //         parks instead of flipping.
    #[test]
    fn try_activate_honors_the_opt_out() {
        let dir = tmpdir("mac-optout");
        let mut env = test_env(dir.path());
        env.auto_activate = false;
        std::fs::create_dir_all(&env.stage_dir).unwrap();
        plant_staged(dir.path(), &env, "2026.9.1", true);

        let err =
            try_activate_with(&env, crate::version::parse_code("2026.9.1").unwrap()).unwrap_err();
        assert!(err.contains("disabled by deployment"), "{err}");
        assert!(std::fs::read_link(&env.profile).is_err(), "no flip");
    }
}
