# RFC-026: The macOS App as a First-Class Node — Certified-Artifact Staging and launchd Supervision

**Status**: Draft (2026-08-17)
**Depends on**: RFC-021 (the provider contract rules 1–3, profile
indirection, newest-wins seeding, exit 75); RFC-019 (the
awaiting-upgrade park, staged attestation, the boundary flow); RFC-023
(CalVer identity)
**Amends**: RFC-021 (discharges the deferred "signed macOS app bundle"
deployment class; splits stage-time availability into tag-published vs
asset-attached)
**Related**: RFC-024 (the sibling transplant, for the mount client);
`.forgejo/workflows/release-macos.yml` + `scripts/build-hopnet-macos.sh`
(the pipeline that certifies the artifact this RFC stages);
`docs/specs/apple-photos-ingress.md` (the bundled daemon this must not
strand)

## Summary

HopNet.app is a node — one binary, the `gui` feature — and today it is
the park-only deployment class: it cannot be supervised, cannot restart
across a regenesis boundary, and cannot stage an upgrade. This RFC
makes it a first-class citizen of the fleet in three moves. Delivery
becomes declarative: a nix-darwin module runs the app as a launchd user
agent through the same profile indirection as every Linux node.
Supervision becomes real: `KeepAlive` restart semantics give exit 75
its meaning, so same-version regenesis restarts work unattended from
day one. And staging gains a second strategy under the unchanged
RFC-021 contract: where a Linux node builds its staged bytes from
source, a darwin node fetches the CI-certified artifact — the signed,
entitled, notarized bundle that only the release pipeline can produce —
and verifies it with Apple's own chain. Version identity stamped
through the bundle is the precondition for all of it, and ships as the
first slice.

## Motivation

- **The app IS a node.** `src/main.rs` is both the node and the Tauri
  GUI (`tauri.conf.json` builds with `features: ["gui"]`); every mesh
  obligation — attestation, boundary crossing, restart derivation —
  applies to it in full.
- **A macOS node exiting 75 today just dies.** No supervisor relaunches
  it; a regenesis boundary strands it until a human reopens the app.
- **Fleet standardization.** Every other personal machine gets HopNet
  through the same declarative nix infrastructure; the macbook should
  not be the one imperative exception.
- **The artifact, not the source, is the unit of truth on darwin.**
  Codesign, entitlements, and notarization are not locally
  reproducible — only the release pipeline's output has been through
  them, so "staged" must mean that artifact, never a local rebuild.

## Two Staging Strategies, One Contract

- **`build-from-source`** (RFC-021, nix/Linux): a tag is stageable the
  moment it exists — every node compiles its own bytes from the forge's
  source.
- **`fetch-certified-artifact`** (this RFC, darwin): a tag means
  nothing until CI attaches the signed .app zip to the release —
  availability is **asset-attached**, not tag-published.
- Verification obligations and failure posture are identical; only how
  bytes are obtained differs. RFC-021's binary-cache note anticipated
  this: the release asset is a binary cache with exactly one writer.
- The CI window (up to the workflow's 45-minute timeout) between tag
  and asset is visible readiness lag, not a safety problem —
  `regenesis_start` already requires every seated validator to attest
  staged, so a boundary cannot start until the macs have their bytes.
- Absence is self-describing: "CI still running" and "CI failed" look
  identical to the client, and the walk holds at the newest
  asset-bearing release and retries next tick. No refusal record for a
  missing asset; wrong bytes under a *present* asset stay a permanent
  refusal, unchanged.
- Trust root shift, stated honestly: Linux nodes trust the forge's
  source; darwin nodes trust the runner's signing keychain, verified
  through Apple's chain. No local fallback exists — if CI never
  publishes, the class holds at the park, which is the supported floor.

## Version Identity Through the Bundle

- Single source: the workspace `Cargo.toml` CalVer, already parsed by
  build stage 0 for `HopNetVersion.swift`.
- Stamped at build into: `tauri.conf.json`'s version, the daemon's
  `Info.plist`, the appex's generated `Info.plist`, and the zip name.
- CI asserts tag == workspace version instead of trusting
  `git describe`.
- `CFBundleShortVersionString` is darwin's `--version`: the honest-bytes
  rule verifies staged bundles by reading it, which is impossible while
  every bundle answers `0.1.0`. This section is the precondition for
  every other section, and ships first (S1).

## Delivery: the nix-darwin Module

- `darwinModules.hopnet-desktop`, mirroring `nix/hopnet-module.nix` in
  launchd vocabulary.
- A launchd **user agent**, run as the home user — deliberate, not a
  compromise: TCC prompts, `SMAppService.agent` registration, and the
  GUI all require the user session (and the app is already unsandboxed
  for SMAppService).
- `ProgramArguments` points at a seed wrapper script — the
  `ExecStartPre` equivalent: run the newest-wins comparison (same
  semantics as the Linux seed script, comparing bundle `--version`
  answers), then `exec "${profile}/Contents/MacOS/HopNet"`.
- The agent sets `HOPNET_AUTOSTART=1`: the app starts tray-only (no
  window at login); the tray toggle creates the window on demand.
- Single-instance: the app flocks the data dir at startup; the losing
  process exits **0** on purpose, so a supervised agent losing the race
  to a Finder-launched copy never restart-loops
  (`SuccessfulExit = false` only restarts non-zero exits).
- The profile is `${dataDir}/profile`, a symlink to a store .app;
  execve follows it at spawn. The wrapper's stable store path also
  sidesteps launchd's dislike of dangling program paths at load.
- `KeepAlive = { SuccessfulExit = false }` is `Restart=on-failure`:
  exit 75 (non-zero) relaunches through the profile; a clean quit stays
  quit. `ThrottleInterval` is the only backoff — sufficient, because
  the crash-loop guard lives in the provider, not the unit.
- The app starts windowless (`"windows": []`), so an at-login launch
  presents no UI.

## Staging: fetch-certified-artifact

- The feed walk keys on asset presence: `common/src/release_feed.rs`
  grows an asset filter for this class.
- `stage(X)`:
  1. download the release zip into the stage dir
  2. verify the published `.sha256`
  3. `codesign --verify --deep --strict` — Apple's chain seals the
     entitlements too, so "went through the pipeline" is
     machine-checked, not assumed
  4. `xcrun stapler validate` — the notarization ticket, offline
  5. read the bundle's stamped version; it must answer exactly X
  6. record provenance beside the staged bundle (version, release URL,
     asset digest)
- Wrong bytes under a present asset are a permanent refusal, never
  attested (RFC-021 rule 1, unchanged).
- `report()` re-verifies the staged bundle the same way the nix
  provider re-resolves its out-link: staged claims stay honest across
  restarts.

## Activation

- The flip is RFC-021's: atomic temp-link + rename of the profile
  symlink to the staged bundle, then exit 75; the agent relaunches into
  the new bytes. The crash-loop guard transfers verbatim.
- Supervision alone is a day-one win before any staging exists:
  same-version regenesis restarts (seal-work exit 75) complete
  unattended, which no macOS node can do today.
- Activation is **unattended by default** (decided 2026-08-17,
  resolving OQ1): with S2's tray-only autostart the relaunch is a tray
  blip — no window, no dock icon — so fleet symmetry with the nix
  nodes wins over a park-and-prompt that would need UI that doesn't
  exist. Failure still parks; `upgrade.autoActivate = false` opts a
  deployment out.

## Bundle Re-Registration Hazards

Each hazard names its verification; all three are testable on a real
machine across one version-bump flip.

- **SMAppService staleness.** The ingress daemon's registration binds
  to a bundle path, and after a flip the old store bundle *still
  exists* — a stale registration silently keeps running old daemon
  bytes, the quietest possible failure. The app gains a startup check:
  registered bundle path ≠ its own path → re-register. Verified by
  flipping and confirming the running daemon's path.
  *(Verified 2026-08-17, both halves: launchd pins the RESOLVED path
  at registration — a profile flip does not move the daemon — and
  NSBundle reports the unresolved profile path, so the healer
  canonicalizes before comparing. Full move → heal → follow cycle
  proven on scratch state: marker updates, re-registration re-pins,
  the daemon respawns from the new bundle.)*
- **Appex discovery.** Launch Services must find the FileProvider appex
  in the new bundle path (pluginkit registration on launch). Verified
  by exercising the domain after a flip.
- **TCC persistence.** The Photos grant is recorded against the app's
  designated requirement (bundle ID + Developer ID), which is stable
  across versions — so grants should survive a store-path change.
  Verified across one real bump on live state before the class is
  called done.
  *(Verified 2026-08-17: the daemon ran and published from two
  different bundle paths with zero authorization prompts — the grant
  followed the designated requirement, not the path.)*

## Release Pipeline Obligations

- Every `v*` release carries the zip, its `.sha256`, and
  `dist/manifest.json` — already true; this RFC makes it a contract.
- CI gates on tag == workspace CalVer (S1).
- Pin-bump automation: after upload, the workflow updates the
  `hopnet-desktop` fetchurl pin (version + hash) — the tagged commit
  cannot contain its own artifact's hash, so the bump is necessarily
  post-release. The bump doubles as the release canary: no bump, no
  release happened.
- The runner is the trust root for this class. Its signing setup is
  currently imperative (login keychain, Actions secrets); hardening
  that is deployment-infrastructure work outside this repo, but the
  dependency is named here. Roadmapped in nix-config: declare the
  keychain unlock — the runner module unlocks the login keychain at
  agent start (password via agenix) and sets the signing key's
  partition list once, so release signing survives a reboot with no
  interactive login (today a logged-out macbook silently breaks
  releases; validated empirically during S1, where a fresh ssh
  security session hit errSecInternalComponent until an interactive
  unlock).

## Non-Goals

Each tracked, not forgotten:

- **The standalone (non-nix) macOS updater** — external users' app
  updating itself with no nix underneath. Deferred until the project is
  externally facing; everything here (version identity, launchd
  supervision, certified staging) is its groundwork.
- **Sparkle / the Tauri updater plugin** — generic updaters cannot
  express rule 2 (activate only what the quorum decided); the
  mechanics they'd provide are small once the profile exists.
- **Universal binaries** — the class is aarch64-darwin until a second
  architecture has a builder.
- **A client-only app mode** — the app connecting to an external local
  node instead of embedding one; only needed for machines wanting both
  a service node and the desktop experience.
- **iOS** — a different distribution regime entirely.

## Implementation Slices

- [x] S1 — version identity: CalVer stamped through
      `tauri.conf.json`, both Info.plists, and the zip name; CI
      tag == version gate. Independently shippable; ships first.
      *(As built, 2026-08-17: `scripts/macos/version.sh` is the single
      parse — sourced for `WORKSPACE_VERSION`/`VERSION_CODE`, run as
      `--check`/`--write` against the daemon's committed Info.plist, so
      builds never dirty the tree and drift fails stage 0 plus a Linux
      CI tripwire. `tauri.conf.json` carries no version key — Tauri 2
      falls back to the crate's workspace CalVer, and stage 2 asserts
      the built bundle answers it, so a Tauri bump can't silently
      regress the fallback. The appex heredoc interpolates the same
      pair; the zip name derives from the workspace version with the
      tag asserted equal on release builds and a `-g<sha>` suffix on
      dev builds; the release workflow gates tag == `v$version` before
      building.)*
- [x] S2 — `darwinModules.hopnet-desktop`: launchd user agent, seed
      wrapper, profile exec, `SuccessfulExit = false`; the three
      re-registration smoke tests across a real flip on a macbook.
      *(As built, 2026-08-17: the module folds seeding and exec into one
      wrapper (launchd has no ExecStartPre; coreutils supplies the
      atomic `mv -T`), runs as the login user with `HOPNET_AUTOSTART=1`
      for tray-only starts, and installs the package normally so
      mac-app-util keeps Finder presence. Three product fixes landed
      with it: tray-only autostart, the data-dir instance flock with
      the deliberate exit-0 loser, and `flow::reregister_if_moved` — a
      startup healer that re-registers the ingress agent when the
      running bundle is not the one that registered it, marker file
      beside the database, mock-tested in Linux CI. The
      `hopnet-desktop` pin bump is BLOCKED on the next release: the
      v2026.8.1–v2026.8.4 releases all have zero assets — the Swift
      test-helper breakage fixed in this branch failed every CalVer
      release build at stage 1, so `v0.1.0-rc.2` is still the newest
      artifact-bearing release. Asset-attached availability, validated
      empirically before the provider even exists.)*
- [~] S3 — the darwin provider: asset-keyed feed walk,
      `fetch-certified-artifact` staging with provenance, activation +
      crash-loop guard; an end-to-end boundary crossing (stage →
      decide → seal → flip + exit 75 → cross) on real hardware.
      *(Implemented 2026-08-17, e2e crossing pending: `ForgejoRelease`
      grew a defaulted asset list + the artifact filename contract;
      the feed provider owns the by-tag fetch (404 = hold) and asset
      download; `MacAppProvider` stages by download → sha256 sidecar →
      `codesign --deep --strict` → staple (test-mode-gated skip for
      unnotarized e2e bundles) → the bundle binary answering the tag →
      provenance written last. `ActivationEnv` (enum over Nix/MacApp)
      generalized the three hardcoded call sites — tick selection +
      auto_stage, boot/seal hooks, readiness view — so both wrapper
      classes share one seam; the flip and crash-loop guard are the nix
      provider's verbatim. Module wires the env contract with
      `upgrade.autoStage`/`autoActivate` defaulting on. Unit-tested on
      Linux via stub codesign/stapler/ditto.)*
- [ ] S4 — pin-bump automation in the release workflow, covering both
      HopNet's own flake and downstream consumers.

## Open Questions

1. ~~**Activation visibility.**~~ RESOLVED (2026-08-17): unattended by
   default — S2's tray-only autostart makes the relaunch a tray blip,
   so fleet symmetry wins; `upgrade.autoActivate = false` opts out, and
   a park-and-prompt UI can arrive later without contract changes.
2. **Pin-bump mechanism.** Direct workflow commit to master vs a PR
   the operator merges — the former is a bot writing to master, the
   latter reintroduces a human in the release path.
3. **RFC-021 OQ1, half-answered.** For this class the Developer ID
   signature IS the detached signature verified before activation —
   Apple's chain instead of a key we hold. Does the Linux class adopt
   a minisign sibling, or is TLS + source trust deemed sufficient
   there?
4. **`min_node` as release metadata** (inherited from RFC-024 OQ2): a
   selection hint to skip doomed downloads; bytes remain the authority
   via verify-after-fetch.
