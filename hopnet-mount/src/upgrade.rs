//! The auto-upgrade wrapper (RFC-024 S1): `hopnet-mount upgrade`, one
//! shot. Reads the node's half of the skew window from a header-less
//! probe (the 426 policy readout), follows the Forgejo release feed
//! with the forward walk, stages nix builds of release tags with
//! provenance, and finishes with at most one atomic flip of the profile
//! symlink. The flip is the wrapper's ONLY output — it never touches
//! the running daemon; systemd is the sole coordinator.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use hopnet_common::compat::UpgradeRequiredResponse;
use hopnet_common::fileprovider::HealthResponse;
use hopnet_common::release_feed::{self, tag_ref, ForgejoRelease, Release};
use hopnet_common::version::{format_code, parse_code};

pub const ENV_PROFILE: &str = "HOPNET_MOUNT_UPGRADE_PROFILE";
pub const ENV_STAGE_DIR: &str = "HOPNET_MOUNT_UPGRADE_STAGE_DIR";
pub const ENV_FLAKE_REF: &str = "HOPNET_MOUNT_UPGRADE_FLAKE_REF";
pub const ENV_NIX_BIN: &str = "HOPNET_MOUNT_UPGRADE_NIX_BIN";
pub const ENV_RELEASE_URL: &str = "HOPNET_MOUNT_UPGRADE_RELEASE_URL";

/// Deployment shape from env (RFC-024's contract, mirroring the node's:
/// deployment shape, not policy). The two paths are REQUIRED — the S2
/// module provides them; paths are never guessed.
#[derive(Debug, Clone)]
pub struct UpgradeEnv {
    pub profile: PathBuf,
    pub stage_dir: PathBuf,
    pub flake_ref: String,
    pub nix_bin: PathBuf,
    /// Feed endpoint override (the node's HOPNET_UPGRADE_RELEASE_URL
    /// mirrored); None derives from the repository field at the caller.
    pub release_url: Option<String>,
}

impl UpgradeEnv {
    pub fn from_env() -> Result<Self, String> {
        Self::from_lookup(|k| std::env::var(k).ok())
    }

    /// Injectable for tests — no process-global env mutation.
    pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Result<Self, String> {
        let (Some(profile), Some(stage_dir)) = (get(ENV_PROFILE), get(ENV_STAGE_DIR)) else {
            return Err(format!(
                "{ENV_PROFILE} and {ENV_STAGE_DIR} must both be set \
                 (the nix module provides them)"
            ));
        };
        Ok(Self {
            profile: PathBuf::from(profile),
            stage_dir: PathBuf::from(stage_dir),
            flake_ref: get(ENV_FLAKE_REF).unwrap_or_else(default_flake_ref),
            nix_bin: get(ENV_NIX_BIN)
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("nix")),
            release_url: get(ENV_RELEASE_URL),
        })
    }

    fn out_link(&self, version: &str) -> PathBuf {
        self.stage_dir.join(format!("v{version}"))
    }

    fn provenance_path(&self, version: &str) -> PathBuf {
        self.stage_dir.join(format!("v{version}.json"))
    }
}

pub fn default_releases_url() -> String {
    release_feed::releases_url(env!("CARGO_PKG_REPOSITORY"))
}

pub fn default_flake_ref() -> String {
    release_feed::flake_ref(env!("CARGO_PKG_REPOSITORY"))
}

/// RFC-021's provenance record plus the RFC-024 fourth field: the staged
/// binary's interrogated `--min-node`, so no tag is questioned twice.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Provenance {
    pub version: String,
    pub flake_ref: String,
    pub out_path: String,
    pub min_node: u32,
}

/// The node's half of the skew window, read from one header-less probe.
#[derive(Debug, Clone, PartialEq)]
pub struct Readout {
    pub node_version: u32,
    pub min_client: u32,
}

#[derive(Debug, PartialEq)]
pub enum Probe {
    Policy(Readout),
    /// A node that predates RFC-023 (or reports below this build's own
    /// MIN_NODE on an ungated 200) — no policy to read, nothing to walk.
    NodeTooOld {
        node_version: u32,
    },
    /// Transient: connection refused, unexpected status, unreadable body.
    Unreachable(String),
}

/// Pure classification of the probe's answer. A gated node always 426s a
/// header-less request, and the structured body carries both halves; a
/// 200 means an UNGATED (pre-RFC-023) node, useful only if it somehow
/// reports a version this build accepts (then with no min_client policy).
pub fn parse_probe(status: u16, body: &[u8]) -> Probe {
    match status {
        426 => match serde_json::from_slice::<UpgradeRequiredResponse>(body) {
            Ok(b) => Probe::Policy(Readout {
                node_version: b.node_version,
                min_client: b.min_client,
            }),
            Err(e) => Probe::Unreachable(format!("426 with unreadable body: {e}")),
        },
        200 => match serde_json::from_slice::<HealthResponse>(body) {
            Ok(b) if b.node_version >= crate::MIN_NODE => Probe::Policy(Readout {
                node_version: b.node_version,
                min_client: 0,
            }),
            Ok(b) => Probe::NodeTooOld {
                node_version: b.node_version,
            },
            Err(e) => Probe::Unreachable(format!("200 with unreadable body: {e}")),
        },
        other => Probe::Unreachable(format!("unexpected status {other} from the health probe")),
    }
}

fn bare_client() -> Result<reqwest::Client, String> {
    // Deliberately NOT HttpTransport: its clients bake the version header
    // in as a default, and the readout requires a header-less request.
    reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(5))
        .timeout(std::time::Duration::from_secs(15))
        .build()
        .map_err(|e| format!("client: {e}"))
}

async fn probe(node_url: &str) -> Probe {
    let client = match bare_client() {
        Ok(c) => c,
        Err(e) => return Probe::Unreachable(e),
    };
    let url = format!(
        "{}/api/integrations/mount/health",
        node_url.trim_end_matches('/')
    );
    match client.get(&url).send().await {
        Ok(response) => {
            let status = response.status().as_u16();
            match response.bytes().await {
                Ok(body) => parse_probe(status, &body),
                Err(e) => Probe::Unreachable(format!("probe body: {e}")),
            }
        }
        Err(e) => Probe::Unreachable(format!("probe {url}: {e}")),
    }
}

async fn fetch_releases(url: &str) -> Result<Vec<Release>, String> {
    let client = bare_client()?;
    let response = client
        .get(url)
        .header("Accept", "application/json")
        .send()
        .await
        .map_err(|e| format!("feed request: {e}"))?;
    let status = response.status();
    if !status.is_success() {
        return Err(format!("{status} from {url}"));
    }
    let raw: Vec<ForgejoRelease> = response
        .json()
        .await
        .map_err(|e| format!("feed decode: {e}"))?;
    Ok(release_feed::parse_releases(raw))
}

#[derive(Debug, Clone, PartialEq)]
pub enum StageError {
    Transient(String),
    Permanent(String),
}

impl std::fmt::Display for StageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StageError::Transient(e) => write!(f, "{e}"),
            StageError::Permanent(e) => write!(f, "permanently refused: {e}"),
        }
    }
}

/// The staging seam the walk runs against — a fake in walk tests, nix in
/// production. `ensure_staged` answers the version's interrogated
/// min_node from verified bytes, building only on a provenance miss.
pub trait Stager {
    fn ensure_staged(&mut self, version: &str) -> Result<u32, StageError>;
}

pub struct NixStager<'a> {
    pub env: &'a UpgradeEnv,
}

impl Stager for NixStager<'_> {
    fn ensure_staged(&mut self, version: &str) -> Result<u32, StageError> {
        if let Ok((_, prov)) = verify_staged(self.env, version) {
            return Ok(prov.min_node);
        }
        stage(self.env, version)
    }
}

/// RFC-021's staging recipe pointed at the mount package: realize the
/// tag's flake ref with the out-link as gcroot, verify the bytes answer
/// `--version` with exactly the tag (wrong bytes are a PERMANENT
/// refusal, never staged — the nix store itself memoizes the build, so
/// no on-disk refusal record is needed), interrogate `--min-node`, then
/// record the four-field provenance.
fn stage(env: &UpgradeEnv, version: &str) -> Result<u32, StageError> {
    if parse_code(version).is_none() {
        return Err(StageError::Permanent(format!(
            "refusing to stage non-CalVer version {version:?}"
        )));
    }
    std::fs::create_dir_all(&env.stage_dir)
        .map_err(|e| StageError::Permanent(format!("stage dir: {e}")))?;

    let full_ref = format!("{}#hopnet-mount", tag_ref(&env.flake_ref, version));
    let link = env.out_link(version);
    tracing::info!(%full_ref, "staging release via nix build (may take a while)");
    let out = std::process::Command::new(&env.nix_bin)
        .arg("build")
        .arg(&full_ref)
        .arg("--out-link")
        .arg(&link)
        .arg("--print-out-paths")
        .output()
        .map_err(|e| StageError::Transient(format!("spawn nix: {e}")))?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        let tail: String = stderr.lines().rev().take(5).collect::<Vec<_>>().join(" | ");
        return Err(StageError::Transient(format!("nix build failed: {tail}")));
    }
    let out_path = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if out_path.is_empty() {
        return Err(StageError::Transient(
            "nix build printed no out path".into(),
        ));
    }

    let built = binary_version(&link)?;
    if built != version {
        // The tag's bytes disagree with the tag's name — retrying the
        // same ref rebuilds the same wrong bytes.
        return Err(StageError::Permanent(format!(
            "staged binary reports {built:?}, expected {version:?}"
        )));
    }
    let min_node = binary_min_node(&link);

    let prov = Provenance {
        version: version.to_string(),
        flake_ref: full_ref,
        out_path,
        min_node,
    };
    let json = serde_json::to_vec_pretty(&prov)
        .map_err(|e| StageError::Permanent(format!("provenance encode: {e}")))?;
    std::fs::write(env.provenance_path(version), json)
        .map_err(|e| StageError::Permanent(format!("provenance write: {e}")))?;
    tracing::info!(version, min_node, "staged, verified and interrogated");
    Ok(min_node)
}

/// The honest-bytes check (RFC-021's, ported): out-link resolves,
/// provenance matches, and the staged binary itself answers `--version`
/// with the claimed tag. Returns the link's resolved target and the
/// provenance (whose min_node is the cached interrogation).
fn verify_staged(env: &UpgradeEnv, version: &str) -> Result<(PathBuf, Provenance), String> {
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
    let built = binary_version(&link).map_err(|e| e.to_string())?;
    if built != version {
        return Err(format!(
            "staged binary reports {built:?}, expected {version:?}"
        ));
    }
    Ok((target, prov))
}

/// Last whitespace token of `--version` — clap prints "hopnet-mount
/// 2026.8.2", the platform stub the same; a bare token also parses.
fn binary_version(link: &Path) -> Result<String, StageError> {
    let bin = link.join("bin/hopnet-mount");
    let out = std::process::Command::new(&bin)
        .arg("--version")
        .output()
        .map_err(|e| StageError::Transient(format!("exec {}: {e}", bin.display())))?;
    if !out.status.success() {
        return Err(StageError::Permanent(
            "staged binary --version failed".into(),
        ));
    }
    let stdout = String::from_utf8_lossy(&out.stdout);
    stdout
        .split_whitespace()
        .last()
        .map(str::to_string)
        .ok_or_else(|| StageError::Permanent("staged binary --version printed nothing".into()))
}

/// `--min-node` of a staged binary. A release predating the flag exits
/// nonzero; treat that as 0 (no requirement), loudly — such a release
/// only ever appears at or below the anchor, where the lemma covers it,
/// and the node's own min_client guards the other direction.
fn binary_min_node(link: &Path) -> u32 {
    let bin = link.join("bin/hopnet-mount");
    let answer = std::process::Command::new(&bin)
        .arg("--min-node")
        .output()
        .ok()
        .filter(|out| out.status.success())
        .and_then(|out| parse_code(String::from_utf8_lossy(&out.stdout).trim()));
    answer.unwrap_or_else(|| {
        tracing::warn!(
            bin = %bin.display(),
            "staged binary does not answer --min-node (pre-RFC-024 release); assuming 0"
        );
        0
    })
}

/// What the walk decided: the newest verified-compatible release newer
/// than the current position (the flip target), and why the walk
/// stopped short of the feed's tip, if it did.
#[derive(Debug, PartialEq)]
pub struct Walk {
    pub target: Option<String>,
    pub hold: Option<String>,
}

/// The forward walk (RFC-024). `current` is the profile binary's own
/// version code (None = no profile yet). Every advance is backed by
/// staged, verified, interrogated bytes; the walk stops at the first
/// incompatibility and holds there.
pub fn forward_walk(
    releases: &[Release],
    current: Option<u32>,
    readout: &Readout,
    stager: &mut dyn Stager,
) -> Walk {
    let mut stable: Vec<(u32, &str)> = releases
        .iter()
        .filter(|r| !r.prerelease)
        .filter_map(|r| parse_code(&r.version).map(|code| (code, r.version.as_str())))
        .collect();
    stable.sort_unstable_by_key(|(code, _)| *code);

    let anchor = readout.node_version;
    let cur = current.unwrap_or(0);
    let mut best: Option<u32> = None;

    // Base candidate: when we lag the anchor, establish a deployable
    // position at the largest release the node's own tag admits — the
    // anchor itself normally, the newest release when the node runs
    // ahead of every release (a dev build).
    if cur < anchor {
        if let Some(&(code, version)) = stable.iter().rev().find(|(code, _)| *code <= anchor) {
            if code < readout.min_client {
                return Walk {
                    target: None,
                    hold: Some(format!(
                        "node's min_client {} admits no release (newest candidate {version})",
                        format_code(readout.min_client)
                    )),
                };
            }
            if code > cur {
                match stager.ensure_staged(version) {
                    // The lemma says the anchor is compatible by
                    // arithmetic; the mechanical check still wins.
                    Ok(min_node) if anchor >= min_node => best = Some(code),
                    Ok(min_node) => {
                        return Walk {
                            target: None,
                            hold: Some(format!(
                                "node {} does not satisfy min_node {} of release {version}",
                                format_code(anchor),
                                format_code(min_node)
                            )),
                        }
                    }
                    Err(e) => {
                        return Walk {
                            target: None,
                            hold: Some(format!("staging {version}: {e}")),
                        }
                    }
                }
            }
        }
    }

    // One incremental question per release newer than the position:
    // stage it (the store memoizes), read its min_node (free after the
    // build, provenance-cached), advance or stop.
    let position = cur.max(anchor);
    let mut hold = None;
    for &(code, version) in stable.iter().filter(|(code, _)| *code > position) {
        match stager.ensure_staged(version) {
            Ok(min_node) if anchor >= min_node => best = Some(code),
            Ok(min_node) => {
                hold = Some(format!(
                    "node {} does not satisfy min_node {} of release {version}",
                    format_code(anchor),
                    format_code(min_node)
                ));
                break;
            }
            Err(e) => {
                hold = Some(format!("staging {version}: {e}"));
                break;
            }
        }
    }

    Walk {
        target: best.map(format_code),
        hold,
    }
}

/// Atomic profile flip: temp link beside the profile, rename over.
/// `Ok(false)` when the profile already points at the target — the
/// wrapper is idempotent; the crash-loop guard proper is the DAEMON's
/// exit-75 gate (S2), not the wrapper's.
fn flip(profile: &Path, target: &Path) -> Result<bool, String> {
    if std::fs::read_link(profile).is_ok_and(|cur| cur == target) {
        return Ok(false);
    }
    if let Some(parent) = profile.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("profile dir: {e}"))?;
    }
    let tmp = profile.with_extension(format!("next.{}", std::process::id()));
    let _ = std::fs::remove_file(&tmp);
    std::os::unix::fs::symlink(target, &tmp).map_err(|e| format!("symlink: {e}"))?;
    std::fs::rename(&tmp, profile).map_err(|e| {
        let _ = std::fs::remove_file(&tmp);
        format!("profile flip: {e}")
    })?;
    Ok(true)
}

/// The profile binary's own `--version` answer — honest bytes, not
/// bookkeeping, so a module-seeded profile (which has no provenance
/// record) reads correctly. None when the profile is missing or broken.
/// Blocking (execs the profile binary).
pub fn current_version(profile: &Path) -> Option<u32> {
    let out = std::process::Command::new(profile.join("bin/hopnet-mount"))
        .arg("--version")
        .output()
        .ok()
        .filter(|out| out.status.success())?;
    let stdout = String::from_utf8_lossy(&out.stdout);
    parse_code(stdout.split_whitespace().last()?)
}

/// The exit-75 crash-loop guard (RFC-024 S2): true only when a profile
/// binary exists, answers `--version`, and its code differs from this
/// build's own. Missing/broken profile → false — a unit could not have
/// restarted through it anyway, and exiting 75 with an unchanged
/// profile would restart into the same 426 forever. Blocking (execs
/// the profile binary) — call off the async workers.
pub fn profile_differs(profile: &Path) -> bool {
    matches!(current_version(profile), Some(v) if v != crate::version_code())
}

/// One `upgrade` run's terminal state. Every variant here is an exit-0
/// operational outcome — S2's ExecStartPre must never block a mount
/// start; only a missing env contract exits nonzero (in main).
#[derive(Debug, PartialEq)]
pub enum Outcome {
    Flipped { from: Option<String>, to: String },
    Current { at: Option<String> },
    Held { at: Option<String>, why: String },
    Offline { why: String },
}

impl Outcome {
    /// The single greppable stdout line.
    pub fn line(&self) -> String {
        match self {
            Outcome::Flipped { from, to } => {
                format!("upgraded: {} -> {to}", from.as_deref().unwrap_or("(none)"))
            }
            Outcome::Current { at } => {
                format!("current: {}", at.as_deref().unwrap_or("(none)"))
            }
            Outcome::Held { at, why } => {
                format!("held at {}: {why}", at.as_deref().unwrap_or("(none)"))
            }
            Outcome::Offline { why } => format!("offline: {why}"),
        }
    }
}

/// One wrapper run, URLs injected (main passes the provisioning-tier
/// node URL and the derived feed URL): probe, poll, walk, at most one
/// flip. Offline-safe by construction — an unreachable feed or node
/// leaves whatever is staged and current in place.
pub fn run_with(env: &UpgradeEnv, node_url: &str, releases_url: &str) -> Outcome {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let readout = match rt.block_on(probe(node_url)) {
        Probe::Policy(readout) => readout,
        Probe::NodeTooOld { node_version } => {
            return Outcome::Held {
                at: current_version(&env.profile).map(format_code),
                why: format!(
                    "node reports {} — upgrade the node",
                    if node_version == 0 {
                        "no version (pre-RFC-023)".to_string()
                    } else {
                        format_code(node_version)
                    }
                ),
            }
        }
        Probe::Unreachable(why) => return Outcome::Offline { why },
    };
    let releases = match rt.block_on(fetch_releases(releases_url)) {
        Ok(releases) => releases,
        // Feed unreachable → proceed with what is staged: no walk, no
        // flip, the current profile stands.
        Err(why) => return Outcome::Offline { why },
    };
    drop(rt);

    let current = current_version(&env.profile);
    let at = current.map(format_code);
    let mut stager = NixStager { env };
    let walk = forward_walk(&releases, current, &readout, &mut stager);

    match walk.target {
        Some(target) => {
            let store = match std::fs::read_link(env.out_link(&target)) {
                Ok(store) => store,
                Err(e) => {
                    return Outcome::Held {
                        at,
                        why: format!("staged out-link for {target} unreadable: {e}"),
                    }
                }
            };
            match flip(&env.profile, &store) {
                Ok(_) => {
                    if let Some(why) = &walk.hold {
                        tracing::warn!(why, "walk held short of the feed tip");
                    }
                    Outcome::Flipped {
                        from: at,
                        to: target,
                    }
                }
                Err(why) => Outcome::Held { at, why },
            }
        }
        None => match walk.hold {
            Some(why) => Outcome::Held { at, why },
            None => Outcome::Current { at },
        },
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::os::unix::fs::PermissionsExt;

    use super::*;

    fn env_in(dir: &Path) -> UpgradeEnv {
        UpgradeEnv {
            profile: dir.join("profile"),
            stage_dir: dir.join("staged"),
            flake_ref: "git+https://example.invalid/HopNet.git".into(),
            nix_bin: dir.join("fake-nix"),
            release_url: None,
        }
    }

    fn write_script(path: &Path, body: &str) {
        std::fs::write(path, body).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
        // ETXTBSY guard: a test child forked concurrently by the harness
        // can briefly inherit the write fd, failing the first exec. Both
        // fake scripts treat __probe as a side-effect-free early exit.
        for _ in 0..100 {
            match std::process::Command::new(path).arg("__probe").output() {
                Err(e) if e.raw_os_error() == Some(26) => {
                    std::thread::sleep(std::time::Duration::from_millis(5))
                }
                _ => return,
            }
        }
    }

    /// A fake store output for `version`: `bin/hopnet-mount` answers
    /// `--version` in the clap format (name + token) as `reports`, and
    /// `--min-node` with the given token — or exits 1 when None, like a
    /// release predating the flag.
    fn plant_store(dir: &Path, version: &str, reports: &str, min_node: Option<&str>) -> PathBuf {
        let store = dir.join(format!("store-{version}"));
        std::fs::create_dir_all(store.join("bin")).unwrap();
        let min_node_arm = match min_node {
            Some(token) => format!("echo {token}"),
            None => "echo 'unexpected argument' >&2; exit 1".into(),
        };
        write_script(
            &store.join("bin/hopnet-mount"),
            &format!(
                "#!/bin/sh\nif [ \"$1\" = __probe ]; then exit 0; fi\n\
                 if [ \"$1\" = --min-node ]; then {min_node_arm}; \
                 else echo \"hopnet-mount {reports}\"; fi\n"
            ),
        );
        store
    }

    /// The RFC-021 fake-nix pattern, mount flavour: resolves the version
    /// from the installable ref, links the pre-planted store dir, prints
    /// it — and records each argv line in {dir}/nix-argv so tests can
    /// assert the ref shape and count invocations.
    fn plant_fake_nix(dir: &Path, env: &UpgradeEnv) {
        write_script(
            &env.nix_bin,
            &format!(
                "#!/bin/sh\n\
                 # fake nix: args are 'build <ref#attr> --out-link <L> --print-out-paths'\n\
                 if [ \"$1\" = __probe ]; then exit 0; fi\n\
                 echo \"$@\" >> {dir}/nix-argv\n\
                 ver=$(printf %s \"$2\" | sed 's|.*refs/tags/v||; s|#.*||')\n\
                 store={dir}/store-$ver\n\
                 if [ ! -d \"$store\" ]; then echo \"no such store $store\" >&2; exit 1; fi\n\
                 ln -sfn \"$store\" \"$4\"\n\
                 echo \"$store\"\n",
                dir = dir.display()
            ),
        );
    }

    fn nix_calls(dir: &Path) -> usize {
        std::fs::read_to_string(dir.join("nix-argv"))
            .map(|s| s.lines().count())
            .unwrap_or(0)
    }

    /// One canned-response HTTP/1.1 server on 127.0.0.1:0 — the workspace
    /// has no HTTP-fake dependency, and a GET responder is 30 lines.
    /// First substring route matching the request path wins.
    struct FakeHttp {
        base: String,
    }

    fn fake_http(routes: Vec<(&'static str, u16, String)>) -> FakeHttp {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { continue };
                let mut buf = Vec::new();
                let mut byte = [0u8; 1];
                while !buf.ends_with(b"\r\n\r\n") {
                    match stream.read(&mut byte) {
                        Ok(1) => buf.push(byte[0]),
                        _ => break,
                    }
                }
                let request = String::from_utf8_lossy(&buf);
                let path = request
                    .lines()
                    .next()
                    .and_then(|line| line.split_whitespace().nth(1))
                    .unwrap_or("/")
                    .to_string();
                let (status, body) = routes
                    .iter()
                    .find(|(needle, _, _)| path.contains(needle))
                    .map(|(_, status, body)| (*status, body.clone()))
                    .unwrap_or((404, String::new()));
                let _ = write!(
                    stream,
                    "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\n\
                     content-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
            }
        });
        FakeHttp { base }
    }

    fn policy_body(min_client: u32, node_version: u32) -> String {
        serde_json::to_string(&UpgradeRequiredResponse {
            surface: "/integrations/mount".into(),
            min_client,
            node_version,
        })
        .unwrap()
    }

    fn feed_body(tags: &[&str]) -> String {
        let entries: Vec<String> = tags
            .iter()
            .map(|t| format!(r#"{{"tag_name": "v{t}"}}"#))
            .collect();
        format!("[{}]", entries.join(","))
    }

    // ---- env contract ----

    // Should: refuse a partial contract with a message naming both
    // required vars, and default flake_ref/nix_bin when only they are
    // absent.
    #[test]
    fn upgrade_env_requires_profile_and_stage_dir() {
        let missing = UpgradeEnv::from_lookup(|_| None).unwrap_err();
        assert!(missing.contains(ENV_PROFILE), "{missing}");
        assert!(missing.contains(ENV_STAGE_DIR), "{missing}");

        let vars = HashMap::from([
            (ENV_PROFILE, "/run/user/1000/hopnet/mount-profile"),
            (ENV_STAGE_DIR, "/var/lib/hopnet/mount-staged"),
        ]);
        let env = UpgradeEnv::from_lookup(|k| vars.get(k).map(|v| v.to_string())).unwrap();
        assert_eq!(env.nix_bin, PathBuf::from("nix"));
        assert_eq!(env.flake_ref, default_flake_ref());
        assert_eq!(
            env.profile,
            PathBuf::from("/run/user/1000/hopnet/mount-profile")
        );
    }

    // Should: read HOPNET_MOUNT_UPGRADE_RELEASE_URL into release_url and
    // leave it None when absent (the caller derives the default lazily).
    #[test]
    fn upgrade_env_reads_optional_release_url() {
        let base = HashMap::from([(ENV_PROFILE, "/p"), (ENV_STAGE_DIR, "/s")]);
        let env = UpgradeEnv::from_lookup(|k| base.get(k).map(|v| v.to_string())).unwrap();
        assert_eq!(env.release_url, None);

        let with_url = HashMap::from([
            (ENV_PROFILE, "/p"),
            (ENV_STAGE_DIR, "/s"),
            (ENV_RELEASE_URL, "http://relay/releases"),
        ]);
        let env = UpgradeEnv::from_lookup(|k| with_url.get(k).map(|v| v.to_string())).unwrap();
        assert_eq!(env.release_url.as_deref(), Some("http://relay/releases"));
    }

    // ---- probe classification ----

    // Should: yield the node's min_client and node_version from the
    // structured 426 body — one header-less request settles the node's
    // whole half of the skew window.
    #[test]
    fn probe_reads_the_policy_from_a_426_body() {
        let probe = parse_probe(426, policy_body(20260802, 20260900).as_bytes());
        assert_eq!(
            probe,
            Probe::Policy(Readout {
                node_version: 20260900,
                min_client: 20260802,
            })
        );
    }

    // Should: hold on a 200 whose node_version is 0 (pre-RFC-023) or
    // below this build's MIN_NODE.
    // Should not: read an ungated 200 as a policy admitting an upgrade.
    #[test]
    fn probe_treats_200_low_or_absent_version_as_node_too_old() {
        use hopnet_common::fileprovider::{HealthResponse, HealthStatus};
        let body = |node_version| {
            serde_json::to_vec(&HealthResponse {
                status: HealthStatus::Ready,
                node_version,
            })
            .unwrap()
        };
        assert_eq!(
            parse_probe(200, &body(0)),
            Probe::NodeTooOld { node_version: 0 }
        );
        assert_eq!(
            parse_probe(200, &body(20250101)),
            Probe::NodeTooOld {
                node_version: 20250101
            }
        );
    }

    // Should: classify any other status as a transient offline hold,
    // never a walk input.
    #[test]
    fn probe_other_statuses_are_unreachable() {
        assert!(matches!(parse_probe(500, b""), Probe::Unreachable(_)));
        assert!(matches!(
            parse_probe(426, b"not json"),
            Probe::Unreachable(_)
        ));
    }

    // ---- the walk matrix (scripted stager) ----

    struct ScriptedStager {
        script: HashMap<&'static str, Result<u32, StageError>>,
        calls: Vec<String>,
    }

    impl ScriptedStager {
        fn new(script: Vec<(&'static str, Result<u32, StageError>)>) -> Self {
            Self {
                script: script.into_iter().collect(),
                calls: Vec::new(),
            }
        }
    }

    impl Stager for ScriptedStager {
        fn ensure_staged(&mut self, version: &str) -> Result<u32, StageError> {
            self.calls.push(version.to_string());
            self.script
                .get(version)
                .cloned()
                .unwrap_or_else(|| Err(StageError::Permanent(format!("unscripted {version}"))))
        }
    }

    fn rel(version: &str) -> Release {
        Release {
            version: version.into(),
            prerelease: false,
        }
    }

    // Should: stage candidates strictly newer than the position in
    // ascending order and land on the newest compatible release.
    // Should: interrogate each candidate exactly once.
    #[test]
    fn walk_advances_through_compatible_releases() {
        let releases = [rel("2026.9.1"), rel("2026.8.2"), rel("2026.9.0")];
        let readout = Readout {
            node_version: 20260802,
            min_client: 20260800,
        };
        let mut stager =
            ScriptedStager::new(vec![("2026.9.0", Ok(20260802)), ("2026.9.1", Ok(20260802))]);
        let walk = forward_walk(&releases, Some(20260802), &readout, &mut stager);
        assert_eq!(walk.target.as_deref(), Some("2026.9.1"));
        assert_eq!(walk.hold, None);
        assert_eq!(stager.calls, vec!["2026.9.0", "2026.9.1"]);
    }

    // Should: stop at the first release whose min_node the node fails
    // and keep the last good position as the target.
    // Should not: stage anything past the hold.
    #[test]
    fn walk_holds_at_first_incompatibility() {
        let releases = [rel("2026.9.0"), rel("2026.9.1"), rel("2026.9.2")];
        let readout = Readout {
            node_version: 20260802,
            min_client: 20260800,
        };
        let mut stager = ScriptedStager::new(vec![
            ("2026.9.0", Ok(20260802)),
            ("2026.9.1", Ok(20260900)), // requires a node newer than 2026.8.2
            ("2026.9.2", Ok(20260802)),
        ]);
        let walk = forward_walk(&releases, Some(20260802), &readout, &mut stager);
        assert_eq!(walk.target.as_deref(), Some("2026.9.0"));
        let hold = walk.hold.unwrap();
        assert!(hold.contains("min_node 2026.9.0"), "{hold}");
        assert!(hold.contains("2026.9.1"), "{hold}");
        assert_eq!(stager.calls, vec!["2026.9.0", "2026.9.1"]);
    }

    // Should: start from the node's own tag when the current position
    // lags it far behind — the anchor is compatible by arithmetic.
    // Should not: stage the skipped history.
    #[test]
    fn walk_jumps_to_the_anchor_after_a_gap() {
        let releases = [
            rel("2026.2.0"),
            rel("2026.5.0"),
            rel("2026.9.0"),
            rel("2026.9.1"),
        ];
        let readout = Readout {
            node_version: 20260900,
            min_client: 20260800,
        };
        let mut stager =
            ScriptedStager::new(vec![("2026.9.0", Ok(20260900)), ("2026.9.1", Ok(20260900))]);
        let walk = forward_walk(&releases, Some(20260100), &readout, &mut stager);
        assert_eq!(walk.target.as_deref(), Some("2026.9.1"));
        assert_eq!(stager.calls, vec!["2026.9.0", "2026.9.1"]);
    }

    // Should: treat the newest release as the candidate when the node
    // runs ahead of every release (a dev build).
    // Should: hold without staging when the node's min_client admits no
    // release at all.
    #[test]
    fn walk_node_ahead_takes_newest_release() {
        let releases = [rel("2026.9.0"), rel("2026.9.1")];
        let readout = Readout {
            node_version: 20261000,
            min_client: 20260900,
        };
        let mut stager = ScriptedStager::new(vec![("2026.9.1", Ok(20260802))]);
        let walk = forward_walk(&releases, None, &readout, &mut stager);
        assert_eq!(walk.target.as_deref(), Some("2026.9.1"));
        assert_eq!(stager.calls, vec!["2026.9.1"]);

        let strict = Readout {
            node_version: 20261000,
            min_client: 20261000,
        };
        let mut stager = ScriptedStager::new(vec![]);
        let walk = forward_walk(&releases, None, &strict, &mut stager);
        assert_eq!(walk.target, None);
        assert!(walk.hold.unwrap().contains("admits no release"));
        assert!(stager.calls.is_empty());
    }

    // Should: end with nothing to do on an empty feed.
    // Should not: invoke the stager.
    #[test]
    fn walk_empty_feed_does_nothing() {
        let readout = Readout {
            node_version: 20260802,
            min_client: 20260800,
        };
        let mut stager = ScriptedStager::new(vec![]);
        let walk = forward_walk(&[], Some(20260802), &readout, &mut stager);
        assert_eq!(
            walk,
            Walk {
                target: None,
                hold: None
            }
        );
        assert!(stager.calls.is_empty());
    }

    // Should not: consider a prerelease or a non-CalVer legacy tag a
    // candidate.
    #[test]
    fn walk_skips_prereleases() {
        let releases = [
            Release {
                version: "2026.9.0-rc.1".into(),
                prerelease: true,
            },
            Release {
                version: "0.1.0".into(),
                prerelease: false,
            },
        ];
        let readout = Readout {
            node_version: 20260802,
            min_client: 20260800,
        };
        let mut stager = ScriptedStager::new(vec![]);
        let walk = forward_walk(&releases, Some(20260802), &readout, &mut stager);
        assert_eq!(
            walk,
            Walk {
                target: None,
                hold: None
            }
        );
        assert!(stager.calls.is_empty());
    }

    // Should: keep the furthest verified position when a stage attempt
    // fails transiently, leaving the failed tag for the next run.
    #[test]
    fn walk_transient_stage_failure_holds_at_last_good() {
        let releases = [rel("2026.9.0"), rel("2026.9.1")];
        let readout = Readout {
            node_version: 20260802,
            min_client: 20260800,
        };
        let mut stager = ScriptedStager::new(vec![
            ("2026.9.0", Ok(20260802)),
            (
                "2026.9.1",
                Err(StageError::Transient("nix build failed: timeout".into())),
            ),
        ]);
        let walk = forward_walk(&releases, Some(20260802), &readout, &mut stager);
        assert_eq!(walk.target.as_deref(), Some("2026.9.0"));
        assert!(walk.hold.unwrap().contains("2026.9.1"));
    }

    // ---- NixStager against the fake nix ----

    // Should: invoke nix on the tag ref with the #hopnet-mount attr and
    // the out-link/print-out-paths shape, parse the clap-format
    // --version answer by last token, and record four-field provenance
    // including the interrogated min_node.
    #[test]
    fn stage_builds_verifies_interrogates_and_records_provenance() {
        let dir = tempfile::tempdir().unwrap();
        let env = env_in(dir.path());
        let store = plant_store(dir.path(), "2026.9.0", "2026.9.0", Some("2026.8.2"));
        plant_fake_nix(dir.path(), &env);

        let min_node = NixStager { env: &env }.ensure_staged("2026.9.0").unwrap();
        assert_eq!(min_node, 20260802);

        let argv = std::fs::read_to_string(dir.path().join("nix-argv")).unwrap();
        assert!(
            argv.contains("?ref=refs/tags/v2026.9.0#hopnet-mount"),
            "{argv}"
        );
        assert!(argv.contains("--out-link"), "{argv}");
        assert!(argv.contains("--print-out-paths"), "{argv}");

        let prov: Provenance =
            serde_json::from_slice(&std::fs::read(env.provenance_path("2026.9.0")).unwrap())
                .unwrap();
        assert_eq!(
            prov,
            Provenance {
                version: "2026.9.0".into(),
                flake_ref: format!("{}?ref=refs/tags/v2026.9.0#hopnet-mount", env.flake_ref),
                out_path: store.display().to_string(),
                min_node: 20260802,
            }
        );
    }

    // Should: report a permanent refusal when the built bytes answer a
    // different version than the tag names.
    // Should not: write provenance for unverified bytes.
    #[test]
    fn stage_refuses_bytes_that_disagree_with_the_tag() {
        let dir = tempfile::tempdir().unwrap();
        let env = env_in(dir.path());
        plant_store(dir.path(), "2026.9.0", "2026.8.1", Some("2026.8.2"));
        plant_fake_nix(dir.path(), &env);

        let err = NixStager { env: &env }
            .ensure_staged("2026.9.0")
            .unwrap_err();
        assert!(matches!(err, StageError::Permanent(_)), "{err:?}");
        assert!(!env.provenance_path("2026.9.0").exists());
    }

    // Impact: "each tag built and interrogated at most once ever" is the
    // walk's cost model — if the provenance cache misses, every wrapper
    // run rebuilds the whole history behind the node.
    // Should: answer min_node from the verified provenance record.
    // Should not: invoke nix a second time for a staged tag.
    #[test]
    fn ensure_staged_reuses_provenance_without_rebuilding() {
        let dir = tempfile::tempdir().unwrap();
        let env = env_in(dir.path());
        plant_store(dir.path(), "2026.9.0", "2026.9.0", Some("2026.8.2"));
        plant_fake_nix(dir.path(), &env);

        assert_eq!(
            NixStager { env: &env }.ensure_staged("2026.9.0").unwrap(),
            20260802
        );
        assert_eq!(
            NixStager { env: &env }.ensure_staged("2026.9.0").unwrap(),
            20260802
        );
        assert_eq!(nix_calls(dir.path()), 1);
    }

    // Should: record min_node 0 for a release predating the --min-node
    // flag instead of refusing to stage it.
    #[test]
    fn stage_tolerates_a_binary_without_min_node() {
        let dir = tempfile::tempdir().unwrap();
        let env = env_in(dir.path());
        plant_store(dir.path(), "2026.9.0", "2026.9.0", None);
        plant_fake_nix(dir.path(), &env);

        assert_eq!(
            NixStager { env: &env }.ensure_staged("2026.9.0").unwrap(),
            0
        );
        let prov: Provenance =
            serde_json::from_slice(&std::fs::read(env.provenance_path("2026.9.0")).unwrap())
                .unwrap();
        assert_eq!(prov.min_node, 0);
    }

    // ---- the flip ----

    // Should: land the profile via temp-link + rename so it always
    // resolves to either the old or the new target.
    // Should not: re-flip when the profile already points at the target.
    #[test]
    fn flip_is_atomic_and_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("gen-a");
        let b = dir.path().join("gen-b");
        std::fs::create_dir_all(&a).unwrap();
        std::fs::create_dir_all(&b).unwrap();
        let profile = dir.path().join("state/profile");

        assert!(flip(&profile, &a).unwrap());
        assert_eq!(std::fs::read_link(&profile).unwrap(), a);
        assert!(!flip(&profile, &a).unwrap(), "already-current is a no-op");
        assert!(flip(&profile, &b).unwrap());
        assert_eq!(std::fs::read_link(&profile).unwrap(), b);
    }

    // Impact: this gate is THE crash-loop guard — exit 75 with an
    // unchanged profile would restart into the same 426 forever.
    // Should: answer true only for a readable profile whose --version
    // differs from this build's own code.
    // Should not: fire for a missing profile, a broken binary, or a
    // profile already at this build's version.
    #[test]
    fn profile_differs_gate_matrix() {
        let dir = tempfile::tempdir().unwrap();
        let profile = dir.path().join("profile");
        assert!(!profile_differs(&profile), "missing profile must not fire");

        let own = hopnet_common::version::format_code(crate::version_code());
        let same = plant_store(dir.path(), "same", &own, None);
        std::os::unix::fs::symlink(&same, &profile).unwrap();
        assert!(!profile_differs(&profile), "own version must not fire");

        let newer = plant_store(dir.path(), "newer", "2099.1.0", None);
        let _ = std::fs::remove_file(&profile);
        std::os::unix::fs::symlink(&newer, &profile).unwrap();
        assert!(profile_differs(&profile), "differing version must fire");

        let broken = dir.path().join("store-broken");
        std::fs::create_dir_all(broken.join("bin")).unwrap();
        write_script(&broken.join("bin/hopnet-mount"), "#!/bin/sh\nexit 1\n");
        let _ = std::fs::remove_file(&profile);
        std::os::unix::fs::symlink(&broken, &profile).unwrap();
        assert!(!profile_differs(&profile), "broken binary must not fire");
    }

    // ---- end-to-end against fake node + fake feed + fake nix ----

    // Should: probe the 426 readout, poll the feed, stage the walk's
    // target, and flip the profile to its store path — exactly one nix
    // invocation, one flip, reported with from/to.
    #[test]
    fn upgrade_run_flips_once_against_fake_feed_and_nix() {
        let dir = tempfile::tempdir().unwrap();
        let env = env_in(dir.path());
        let store = plant_store(dir.path(), "2026.9.0", "2026.9.0", Some("2026.8.2"));
        plant_fake_nix(dir.path(), &env);
        let node = fake_http(vec![("mount/health", 426, policy_body(20260802, 20260900))]);
        let feed = fake_http(vec![("/releases", 200, feed_body(&["2026.9.0"]))]);

        let outcome = run_with(&env, &node.base, &format!("{}/releases", feed.base));
        assert_eq!(
            outcome,
            Outcome::Flipped {
                from: None,
                to: "2026.9.0".into(),
            }
        );
        assert_eq!(std::fs::read_link(&env.profile).unwrap(), store);
        assert_eq!(nix_calls(dir.path()), 1);
    }

    // Impact: ExecStartPre (S2) runs this before every mount start — an
    // offline laptop must still mount with whatever is staged.
    // Should: end Offline when the feed is unreachable.
    // Should not: touch the profile.
    #[test]
    fn upgrade_run_holds_offline_when_the_feed_is_unreachable() {
        let dir = tempfile::tempdir().unwrap();
        let env = env_in(dir.path());
        let node = fake_http(vec![("mount/health", 426, policy_body(20260802, 20260900))]);
        // A port that was bound and released: connection refused.
        let dead = {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            format!("http://{}/releases", listener.local_addr().unwrap())
        };

        let outcome = run_with(&env, &node.base, &dead);
        assert!(matches!(outcome, Outcome::Offline { .. }), "{outcome:?}");
        assert!(!env.profile.exists());
    }

    // Should: leave the profile untouched and report Current when the
    // feed offers nothing newer than the profile's own version.
    #[test]
    fn upgrade_run_is_current_when_nothing_newer_exists() {
        let dir = tempfile::tempdir().unwrap();
        let env = env_in(dir.path());
        let store = plant_store(dir.path(), "2026.8.2", "2026.8.2", Some("2026.8.2"));
        plant_fake_nix(dir.path(), &env);
        std::os::unix::fs::symlink(&store, &env.profile).unwrap();
        let node = fake_http(vec![("mount/health", 426, policy_body(20260802, 20260802))]);
        let feed = fake_http(vec![("/releases", 200, feed_body(&["2026.8.2"]))]);

        let outcome = run_with(&env, &node.base, &format!("{}/releases", feed.base));
        assert_eq!(
            outcome,
            Outcome::Current {
                at: Some("2026.8.2".into()),
            }
        );
        assert_eq!(std::fs::read_link(&env.profile).unwrap(), store);
        assert_eq!(nix_calls(dir.path()), 0);
    }
}
