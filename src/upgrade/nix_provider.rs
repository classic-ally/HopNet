//! Nix upgrade provider (RFC-021): stage a release by realizing its flake
//! ref into the store, attest it honestly, and — at a boundary — activate
//! by atomically flipping the service-owned profile symlink the unit execs
//! through. Only `stage` shells out to nix; activation and every check are
//! plain filesystem and subprocess work, so `try_activate` stays callable
//! from the synchronous hook sites (seal work thread, pre-pool boot).
//!
//! Configuration is deployment shape, not mesh policy, so it arrives as
//! env vars set by the NixOS module (`nix/hopnet-module.nix`) rather than
//! DB settings. The same seam lets tests point `NIX_BIN` at a fake.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::{AvailableVersion, ProviderError, ProviderReport, UpgradeProvider};

/// Selects this provider when set to `nix`.
pub const ENV_PROVIDER: &str = "HOPNET_UPGRADE_PROVIDER";
/// The nix binary `stage` invokes (the module sets an absolute path; tests
/// point it at a fake).
pub const ENV_NIX_BIN: &str = "HOPNET_UPGRADE_NIX_BIN";
/// The profile symlink the service unit execs through.
pub const ENV_PROFILE: &str = "HOPNET_UPGRADE_PROFILE";
/// Directory holding staged out-links and their provenance records.
pub const ENV_STAGE_DIR: &str = "HOPNET_UPGRADE_STAGE_DIR";
/// Base flake ref to stage from; `?ref=refs/tags/v<version>` is appended
/// (see `tag_ref`). Defaults
/// to the crate's repository field.
pub const ENV_FLAKE_REF: &str = "HOPNET_UPGRADE_FLAKE_REF";
/// `0` disables proactive staging on the tick (default on).
pub const ENV_AUTO_STAGE: &str = "HOPNET_UPGRADE_AUTO_STAGE";
/// `0` disables boundary activation, leaving the RFC-019 park (default on).
pub const ENV_AUTO_ACTIVATE: &str = "HOPNET_UPGRADE_AUTO_ACTIVATE";

/// The deployment contract, resolved from the module-set env vars.
#[derive(Debug, Clone)]
pub struct NixEnv {
    pub nix_bin: PathBuf,
    pub flake_ref: String,
    pub stage_dir: PathBuf,
    pub profile: PathBuf,
    pub auto_stage: bool,
    pub auto_activate: bool,
}

impl NixEnv {
    /// `Some` only when the deployment declares itself nix-provided AND
    /// supplies the two paths that have no sane in-code default. A partial
    /// contract is treated as absent (warned once per resolution) — a
    /// misconfigured provider must degrade to the report-only baseline,
    /// never to guessed paths.
    pub fn from_env() -> Option<Self> {
        if std::env::var(ENV_PROVIDER).ok()?.as_str() != "nix" {
            return None;
        }
        let (Ok(profile), Ok(stage_dir)) =
            (std::env::var(ENV_PROFILE), std::env::var(ENV_STAGE_DIR))
        else {
            tracing::warn!(
                "{ENV_PROVIDER}=nix but {ENV_PROFILE}/{ENV_STAGE_DIR} unset — provider disabled"
            );
            return None;
        };
        let on = |key: &str| std::env::var(key).map(|v| v != "0").unwrap_or(true);
        Some(Self {
            nix_bin: std::env::var(ENV_NIX_BIN)
                .unwrap_or_else(|_| "nix".into())
                .into(),
            flake_ref: std::env::var(ENV_FLAKE_REF).unwrap_or_else(|_| default_flake_ref()),
            stage_dir: stage_dir.into(),
            profile: profile.into(),
            auto_stage: on(ENV_AUTO_STAGE),
            auto_activate: on(ENV_AUTO_ACTIVATE),
        })
    }

    fn out_link(&self, version: &str) -> PathBuf {
        self.stage_dir.join(format!("v{version}"))
    }

    fn provenance_path(&self, version: &str) -> PathBuf {
        self.stage_dir.join(format!("v{version}.json"))
    }
}

/// Base flake ref derived from the crate's repository field — the release
/// page and the staged source stay the same single upstream. The `.git`
/// suffix matches the form the deployments' flake inputs already use.
pub fn default_flake_ref() -> String {
    let repository = env!("CARGO_PKG_REPOSITORY");
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

/// What `stage` records beside the out-link: enough to re-verify at
/// activation time that the bytes being activated are the ones THIS node
/// staged, from where (contract rule 2).
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Provenance {
    pub version: String,
    pub flake_ref: String,
    pub out_path: String,
}

pub struct NixUpgradeProvider {
    pub env: NixEnv,
    releases: super::git_release::GitReleaseProvider,
}

impl NixUpgradeProvider {
    pub fn new(env: NixEnv, releases_url: String) -> Self {
        Self {
            env,
            releases: super::git_release::GitReleaseProvider::new(releases_url),
        }
    }
}

impl UpgradeProvider for NixUpgradeProvider {
    fn name(&self) -> &'static str {
        "nix"
    }

    /// Availability from the same Forgejo poll as the v1 provider; each
    /// version marked staged iff its local out-link + provenance + the
    /// staged binary's own `--version` agree (contract rule 1: a staged
    /// claim is backed by verified bytes, never by a tag name).
    fn report(
        &self,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<ProviderReport, ProviderError>> + Send + '_>>
    {
        Box::pin(async move {
            let mut report = self.releases.report().await?;
            mark_staged(&self.env, &mut report.available);
            Ok(report)
        })
    }

    /// Realize the release's flake ref into the store with the out-link as
    /// the gcroot, verify the produced binary answers `--version` with
    /// exactly the requested version, then write the provenance record.
    /// One attempt per call; the tick retries transient failures next
    /// cycle. Builds can take tens of minutes on modest hosts — that is
    /// the accepted cost of staging without build infrastructure.
    fn stage<'a>(
        &'a self,
        version: &'a str,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<(), ProviderError>> + Send + 'a>> {
        Box::pin(async move {
            if crate::version::parse_code(version).is_none() {
                return Err(ProviderError::Permanent(format!(
                    "refusing to stage non-CalVer version {version:?}"
                )));
            }
            std::fs::create_dir_all(&self.env.stage_dir)
                .map_err(|e| ProviderError::Permanent(format!("stage dir: {e}")))?;

            let full_ref = tag_ref(&self.env.flake_ref, version);
            let link = self.env.out_link(version);
            tracing::info!(%full_ref, "staging release via nix build (may take a while)");
            let out = tokio::process::Command::new(&self.env.nix_bin)
                .arg("build")
                .arg(&full_ref)
                .arg("--out-link")
                .arg(&link)
                .arg("--print-out-paths")
                .output()
                .await
                .map_err(|e| ProviderError::Transient(format!("spawn nix: {e}")))?;
            if !out.status.success() {
                let stderr = String::from_utf8_lossy(&out.stderr);
                let tail: String = stderr.lines().rev().take(5).collect::<Vec<_>>().join(" | ");
                return Err(ProviderError::Transient(format!(
                    "nix build failed: {tail}"
                )));
            }
            let out_path = String::from_utf8_lossy(&out.stdout).trim().to_string();
            if out_path.is_empty() {
                return Err(ProviderError::Transient(
                    "nix build printed no out path".into(),
                ));
            }

            let built = staged_binary_version(&link).await?;
            if built != version {
                // The tag's bytes disagree with the tag's name — retrying
                // the same ref rebuilds the same wrong bytes.
                return Err(ProviderError::Permanent(format!(
                    "staged binary reports {built:?}, expected {version:?}"
                )));
            }

            let prov = Provenance {
                version: version.to_string(),
                flake_ref: full_ref,
                out_path,
            };
            let json = serde_json::to_vec_pretty(&prov)
                .map_err(|e| ProviderError::Permanent(format!("provenance encode: {e}")))?;
            std::fs::write(self.env.provenance_path(version), json)
                .map_err(|e| ProviderError::Permanent(format!("provenance write: {e}")))?;
            tracing::info!(version, "staged and verified");
            Ok(())
        })
    }
}

/// `--version` of the staged binary behind an out-link (async flavor for
/// the tick paths).
async fn staged_binary_version(link: &Path) -> Result<String, ProviderError> {
    let bin = link.join("bin/hopnet");
    let out = tokio::process::Command::new(&bin)
        .arg("--version")
        .output()
        .await
        .map_err(|e| ProviderError::Transient(format!("exec {}: {e}", bin.display())))?;
    if !out.status.success() {
        return Err(ProviderError::Permanent(
            "staged binary --version failed".into(),
        ));
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

/// Mark each available version staged iff link + provenance + binary all
/// agree. Split from `report()` so tests can drive it without HTTP.
pub(crate) fn mark_staged(env: &NixEnv, available: &mut [AvailableVersion]) {
    for v in available.iter_mut() {
        v.staged = verify_staged(env, &v.version).is_ok();
    }
}

/// The honest-bytes check shared by report-marking and activation: the
/// out-link resolves, the provenance record matches, and the staged binary
/// itself answers `--version` with the claimed version. Returns the
/// link's resolved target.
fn verify_staged(env: &NixEnv, version: &str) -> Result<PathBuf, String> {
    let link = env.out_link(version);
    let target = std::fs::read_link(&link)
        .map_err(|e| format!("no staged out-link {}: {e}", link.display()))?;
    let prov: Provenance = serde_json::from_slice(
        &std::fs::read(env.provenance_path(version))
            .map_err(|e| format!("no provenance for {version}: {e}"))?,
    )
    .map_err(|e| format!("provenance for {version} unreadable: {e}"))?;
    if prov.version != version {
        return Err(format!(
            "provenance names {:?}, expected {version:?}",
            prov.version
        ));
    }
    if Path::new(&prov.out_path) != target.as_path() {
        return Err(format!(
            "provenance out path {:?} does not match link target {}",
            prov.out_path,
            target.display()
        ));
    }
    let bin = link.join("bin/hopnet");
    let out = std::process::Command::new(&bin)
        .arg("--version")
        .output()
        .map_err(|e| format!("exec {}: {e}", bin.display()))?;
    let built = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if !out.status.success() || built != version {
        return Err(format!(
            "staged binary reports {built:?}, expected {version:?}"
        ));
    }
    Ok(target)
}

/// Boundary activation (contract rule 2): flip the profile to the staged
/// generation for `required`, atomically, only when the deployment opted
/// in and the staged bytes verify. Synchronous on purpose — callable from
/// the seal-work thread and the pre-pool boot path. On ANY failure the
/// caller parks exactly as RFC-019 ships (contract rule 3).
pub fn try_activate(required_code: u32) -> Result<(), String> {
    let env = NixEnv::from_env().ok_or("no nix upgrade provider configured")?;
    try_activate_with(&env, required_code)
}

pub(crate) fn try_activate_with(env: &NixEnv, required_code: u32) -> Result<(), String> {
    if !env.auto_activate {
        return Err("auto-activation disabled by deployment".into());
    }
    let version = crate::version::format_code(required_code);
    let target = verify_staged(env, &version)?;

    // Crash-loop guard: if the profile already points at the staged
    // generation and we are STILL here (running the wrong version), a
    // previous flip did not produce the required binary. Re-flipping
    // would exit-75 into the same state forever; park instead.
    if std::fs::read_link(&env.profile).is_ok_and(|cur| cur == target) {
        return Err(format!(
            "profile already points at staged {version} yet the running version \
             is still wrong — refusing to re-flip (crash-loop guard)"
        ));
    }

    // Atomic flip: build the new symlink beside the profile, rename over.
    let tmp = env
        .profile
        .with_extension(format!("next.{}", std::process::id()));
    let _ = std::fs::remove_file(&tmp);
    std::os::unix::fs::symlink(&target, &tmp).map_err(|e| format!("symlink: {e}"))?;
    std::fs::rename(&tmp, &env.profile).map_err(|e| {
        let _ = std::fs::remove_file(&tmp);
        format!("profile flip: {e}")
    })?;
    tracing::info!(
        version,
        profile = %env.profile.display(),
        target = %target.display(),
        "activated staged generation"
    );
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// A staged "generation": a fake store dir whose bin/hopnet prints
    /// `version`, an out-link to it, and (optionally) a provenance record
    /// — the post-`stage` disk state, built by hand. `pub(crate)` so the
    /// boot-gate tests can plant activation fixtures too.
    pub(crate) fn plant_staged(
        dir: &Path,
        env: &NixEnv,
        version: &str,
        with_provenance: bool,
    ) -> PathBuf {
        let store = dir.join(format!("store-{version}"));
        std::fs::create_dir_all(store.join("bin")).unwrap();
        let bin = store.join("bin/hopnet");
        std::fs::write(&bin, format!("#!/bin/sh\necho {version}\n")).unwrap();
        std::fs::set_permissions(&bin, {
            use std::os::unix::fs::PermissionsExt;
            std::fs::Permissions::from_mode(0o755)
        })
        .unwrap();
        std::fs::create_dir_all(&env.stage_dir).unwrap();
        std::os::unix::fs::symlink(&store, env.out_link(version)).unwrap();
        if with_provenance {
            let prov = Provenance {
                version: version.into(),
                flake_ref: tag_ref(&env.flake_ref, version),
                out_path: store.to_string_lossy().into_owned(),
            };
            std::fs::write(
                env.provenance_path(version),
                serde_json::to_vec_pretty(&prov).unwrap(),
            )
            .unwrap();
        }
        store
    }

    pub(crate) fn test_env(dir: &Path) -> NixEnv {
        NixEnv {
            nix_bin: dir.join("fake-nix"),
            flake_ref: "git+https://example.invalid/HopNet.git".into(),
            stage_dir: dir.join("staged"),
            profile: dir.join("profile"),
            auto_stage: true,
            auto_activate: true,
        }
    }

    fn tmpdir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("hopnet-nixprov-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// Write a fake `nix` that "builds" by symlinking a pre-made store dir
    /// (embedded in the script — no env needed) and printing its path,
    /// mimicking `nix build --out-link L --print-out-paths`.
    fn plant_fake_nix(dir: &Path, env: &NixEnv, version: &str) -> PathBuf {
        let store = dir.join(format!("store-{version}"));
        std::fs::create_dir_all(store.join("bin")).unwrap();
        let bin = store.join("bin/hopnet");
        std::fs::write(&bin, format!("#!/bin/sh\necho {version}\n")).unwrap();
        std::fs::set_permissions(&bin, {
            use std::os::unix::fs::PermissionsExt;
            std::fs::Permissions::from_mode(0o755)
        })
        .unwrap();
        std::fs::write(
            &env.nix_bin,
            format!(
                "#!/bin/sh\n# fake nix: args are 'build <ref> --out-link <L> --print-out-paths'\n\
                 ln -sfn {store} \"$4\"\necho {store}\n",
                store = store.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&env.nix_bin, {
            use std::os::unix::fs::PermissionsExt;
            std::fs::Permissions::from_mode(0o755)
        })
        .unwrap();
        store
    }

    fn block_on<F: Future>(f: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(f)
    }

    // Should: stage a release through the nix seam, verify the produced
    // binary's own version answer, and record provenance beside the link.
    // Impact: the staged claim that feeds the mesh's upgrade precondition
    // is backed by verified bytes on disk, not by a tag name.
    #[test]
    fn stage_builds_verifies_and_records_provenance() {
        let dir = tmpdir("stage-ok");
        let env = test_env(&dir);
        let store = plant_fake_nix(&dir, &env, "2026.9.0");
        let provider = NixUpgradeProvider::new(env.clone(), "http://unused.invalid".into());

        block_on(provider.stage("2026.9.0")).unwrap();

        assert_eq!(std::fs::read_link(env.out_link("2026.9.0")).unwrap(), store);
        let prov: Provenance =
            serde_json::from_slice(&std::fs::read(env.provenance_path("2026.9.0")).unwrap())
                .unwrap();
        assert_eq!(prov.version, "2026.9.0");
        assert_eq!(prov.out_path, store.to_string_lossy());
    }

    // Should: refuse (permanently) a staged build whose binary reports a
    // different version than the requested tag.
    // Should not: write a provenance record for unverified bytes.
    #[test]
    fn stage_refuses_bytes_that_disagree_with_the_tag() {
        let dir = tmpdir("stage-mismatch");
        let env = test_env(&dir);
        plant_fake_nix(&dir, &env, "2026.8.1"); // fake builds the WRONG version
        let provider = NixUpgradeProvider::new(env.clone(), "http://unused.invalid".into());

        let err = block_on(provider.stage("2026.9.0")).unwrap_err();
        assert!(matches!(err, ProviderError::Permanent(_)), "{err}");
        assert!(!env.provenance_path("2026.9.0").exists());
    }

    // Should: mark a version staged only when out-link, provenance and the
    // binary's version answer all agree — a missing provenance or a lying
    // binary keeps the honest default (unstaged).
    #[test]
    fn report_marking_requires_link_provenance_and_binary_agreement() {
        let dir = tmpdir("mark");
        let env = test_env(&dir);
        plant_staged(&dir, &env, "2026.9.0", true);
        plant_staged(&dir, &env, "2026.9.1", false); // link but no provenance

        let mut available = vec![
            AvailableVersion {
                version: "2026.9.0".into(),
                staged: false,
                prerelease: false,
            },
            AvailableVersion {
                version: "2026.9.1".into(),
                staged: false,
                prerelease: false,
            },
            AvailableVersion {
                version: "2026.10.0".into(),
                staged: false,
                prerelease: false,
            },
        ];
        mark_staged(&env, &mut available);
        assert!(available[0].staged);
        assert!(!available[1].staged);
        assert!(!available[2].staged);
    }

    // Should: activate by atomically flipping the profile symlink to the
    // staged generation for the quorum-decided target.
    #[test]
    fn try_activate_flips_the_profile_to_the_staged_generation() {
        let dir = tmpdir("activate");
        let env = test_env(&dir);
        let store = plant_staged(&dir, &env, "2026.9.0", true);
        // Simulate an existing profile pointing at the OLD generation.
        std::os::unix::fs::symlink(dir.join("old-gen"), &env.profile).unwrap();

        try_activate_with(&env, 20260900).unwrap();
        assert_eq!(std::fs::read_link(&env.profile).unwrap(), store);
    }

    // Impact: without this refusal a flip that fails to change the running
    // version (wrong bytes behind a right-looking link) would exit-75 into
    // the same state forever — the crash loop contract rule 3 forbids.
    // Should not: re-flip when the profile already points at the staged
    // generation; the node must park for a human instead.
    #[test]
    fn try_activate_refuses_a_reflip_of_the_active_generation() {
        let dir = tmpdir("crash-loop");
        let env = test_env(&dir);
        let store = plant_staged(&dir, &env, "2026.9.0", true);
        std::os::unix::fs::symlink(&store, &env.profile).unwrap();

        let err = try_activate_with(&env, 20260900).unwrap_err();
        assert!(err.contains("crash-loop guard"), "{err}");
    }

    // Should not: activate when the provenance record disagrees with the
    // link target — the bytes are not the ones this node staged.
    #[test]
    fn try_activate_refuses_provenance_mismatch() {
        let dir = tmpdir("prov-mismatch");
        let env = test_env(&dir);
        plant_staged(&dir, &env, "2026.9.0", true);
        // Corrupt the provenance to claim a different out path.
        let prov = Provenance {
            version: "2026.9.0".into(),
            flake_ref: tag_ref("git+https://example.invalid/HopNet.git", "2026.9.0"),
            out_path: "/nix/store/somewhere-else".into(),
        };
        std::fs::write(
            env.provenance_path("2026.9.0"),
            serde_json::to_vec_pretty(&prov).unwrap(),
        )
        .unwrap();

        let err = try_activate_with(&env, 20260900).unwrap_err();
        assert!(err.contains("does not match link target"), "{err}");
    }

    // Should not: activate when the deployment opted out — the RFC-019
    // park is the supported flow, not an error.
    #[test]
    fn try_activate_honors_the_opt_out() {
        let dir = tmpdir("opt-out");
        let mut env = test_env(&dir);
        plant_staged(&dir, &env, "2026.9.0", true);
        env.auto_activate = false;

        let err = try_activate_with(&env, 20260900).unwrap_err();
        assert!(err.contains("disabled"), "{err}");
    }

    // Impact: the tag ref is the one string in the provider that only a
    // real nix and a real forge can validate — every test here and the VM
    // test stub the nix binary, so a wrong ref namespace sails through all
    // of them and dies on the first genuine release. It did: nix resolves a
    // bare `?ref=X` under refs/heads, so `?ref=v2026.8.1` asked for a
    // BRANCH and all three nodes failed with "couldn't find remote ref
    // refs/heads/v2026.8.1".
    // Should: address a release tag under refs/tags.
    #[test]
    fn tag_ref_addresses_the_tag_namespace() {
        assert_eq!(
            tag_ref("git+https://example.invalid/HopNet.git", "2026.8.1"),
            "git+https://example.invalid/HopNet.git?ref=refs/tags/v2026.8.1"
        );
    }

    // Should: resolve the deployment contract from env only when the
    // provider is declared AND both paths are present; partial contracts
    // degrade to no provider.
    #[test]
    fn from_env_requires_the_full_contract() {
        let guard = crate::test_env::lock_env();
        crate::test_env::set(&guard, ENV_PROVIDER, "nix");
        crate::test_env::remove(&guard, ENV_PROFILE);
        crate::test_env::remove(&guard, ENV_STAGE_DIR);
        assert!(
            NixEnv::from_env().is_none(),
            "partial contract must disable"
        );

        crate::test_env::set(&guard, ENV_PROFILE, "/var/lib/hopnet/profile");
        crate::test_env::set(&guard, ENV_STAGE_DIR, "/var/lib/hopnet/staged");
        crate::test_env::set(&guard, ENV_AUTO_ACTIVATE, "0");
        let env = NixEnv::from_env().expect("full contract");
        assert!(env.auto_stage, "unset knob defaults on");
        assert!(!env.auto_activate, "explicit 0 disables");
        assert_eq!(env.profile, PathBuf::from("/var/lib/hopnet/profile"));
    }
}
