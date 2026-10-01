# RFC-021: Nix Upgrade Provider — Staged Binaries and Unattended Boundary Crossing

**Status**: Draft
**Depends on**: RFC-019 (the upgrade-provider seam, `node_staged_version`
attestations, the awaiting-upgrade park, exit-75 restart derivation)
**Related**: RFC-020 (module versioning — what an upgrade means);
`.forgejo/workflows/release-macos.yml` (the release channel this
rides); RFC-024 (the mount client channel transplanting this
machinery, 2026-08-16)

## Motivation

RFC-019 made upgrade readiness a deterministic precondition —
`regenesis_start { target }` validates that every seated validator has
`target` staged in committed state — then deliberately left deployment
orchestration out of scope behind the `UpgradeProvider` trait, shipping
only the v1 git-release provider, which reports available-but-unstaged
and cannot stage.

What's missing is the other half: an atomic swap, tied to the mesh's
agreed target. The node already holds the quorum-decided
`target_version_code` in its own committed state; give it staged bytes
and one atomic pointer flip and the whole upgrade becomes automatic —
stage while running old, decide, seal, swap, cross — with no operator
in the window between close and reopen.

## Other deployment classes

Deferred. Prescribing staging paths or activation flows for non-nix
deployments now would constrain how people actually run nodes (a
docker container, a hand-managed binary, the signed macOS app bundle
each want a different notion of "staged"). The `UpgradeProvider` trait
is the seam such providers slot behind when someone needs one; until
then the awaiting-upgrade park is their fully supported flow, exactly
as RFC-019 ships it.

## The provider contract

Rules the nix provider is specified against — written so a future
provider inherits them rather than re-deriving the security argument:

1. **Staged claims are honest.** `staged: X` means the exact bytes of
   version X are locally present and activatable without network or
   human input — for nix, a realized closure rooted on disk. Attesting
   from a tag name alone is forbidden.
2. **Activation is doubly authorized.** A provider activates only when
   (a) the quorum-decided `target_version_code` in committed state
   names X, and (b) the bytes being activated are the ones this node
   staged itself. A peer cannot push a binary.
3. **Failure lands in the park.** When activation doesn't happen —
   unsupported, disabled by configuration, or attempted and failed —
   the node must end exactly where RFC-019 leaves an un-upgradable
   node today: parked awaiting-upgrade, marker naming the required
   version, engine halted, old database intact, RPC still answering
   status. No third state is permitted: never half-activated, never
   crash-looping, never running past the boundary on the wrong
   version. A parked node is a human's to resolve; the provider's only
   obligation on failure is to reach the park cleanly.

Where activation is supported, it hooks the two places RFC-019 already
branches on `running != target`: the seal-work restart derivation (a
node live at the seal) and boot Gate 1 (a node that crashed or was
down through the seal). Activate, then exit 75; the supervisor
restarts into the staged binary and Gate 1 passes on the next boot.

### Mixed outcomes across the mesh

No cross-node coordination is needed at activation time, because the
boundary itself already did the coordinating: every node decided the
same commit block at H, so every node is frozen on identical sealed
state, and crossing is a purely local, idempotent act. Any mix of
per-node outcomes therefore composes safely:

- **Everyone activates** — all cross within seconds, new epoch live at
  H+1.
- **A minority parks** — the crossed majority meets quorum in the new
  epoch and progresses. The parked node is unaffected and unaffecting:
  once its operator (or a late-succeeding activation) gets it to the
  target version, it boots, Gate 1 passes on its own sealed state, and
  it syncs forward. If it stays dark long enough the new epoch votes
  its seat out, and its eventual return is the ordinary rejoin path:
  probe-pong lag discovery, then the S7 epoch join.
- **A majority parks** — the new epoch exists but cannot reach quorum;
  the mesh stalls SAFELY (nothing decided, nothing diverged, all state
  sealed and certified). Recovery is human: finish the swaps, or
  abandon the boundary per node within the RFC-019 S8 rollback window.
  This is the "catastrophic target binary" scenario the regenesis spec
  enumerates, unchanged by this RFC.

Deliberately excluded: **automatic rollback**. A timeout-triggered
revert ("if quorum hasn't crossed in N minutes, roll back")
reintroduces the divergence the forward-only rule exists to kill —
node A reverts on its timer while node B crossed and decided, closing
the window for B while A re-enters the old epoch. Parked-until-human
is the only fallback that is safe under every partial-failure
interleaving: automation ends at the halt, and the recovery DIRECTION
is a human decision, with the rollback route's window check guarding
the unsafe side.

## The nix provider

**The indirection.** The service unit execs hopnet through a
service-owned profile symlink —
`ExecStart=/var/lib/hopnet/profile/bin/hopnet` — instead of a pinned
store path. NOT a `nix-env` profile: a plain symlink the service user
owns, moved only by atomic temp-link + rename. This is what keeps
activation a small synchronous filesystem operation callable from
every hook site; only `stage` ever shells out to nix. The system flake
seeds the profile (below); across hopnet upgrades the unit file never
changes, so `nixos-rebuild` and self-upgrade stop competing over
`ExecStart`.

**Seeding, newest-WITHIN-AGREEMENT (RFC-025).** The module's
`ExecStartPre` compares the flake-pinned package's version against
`profile/bin/hopnet --version`: profile missing → seed it
(availability wins on a wiped profile; the boot-time version-ahead
gate is the safety net); profile newer or equal → a self-upgrade
happened, leave it; flake strictly newer → ask `hopnet seed-guard
--candidate <ver>` whether the MESH permits the advance. The guard
reads two markers beside the database (bare CalVer strings, a stable
interface): `agreed-version` — the version the mesh agreed this node
runs, stamped at genesis/join and re-stamped only when a crossing
completes — and `awaiting-upgrade`, whose required target raises the
ceiling for a parked node (the sanctioned manual-upgrade path). Exit
0 = seed; 3 = held (named in the journal — `nixos-rebuild` can no
longer move a node past its mesh mid-epoch; the pin is not lost,
RFC-021 activation flips the profile when the mesh seals); 2 = usage;
any non-zero means "don't seed", so failures degrade toward holding,
and a malformed marker HOLDS (the guard is the conservative reader;
the daemon degrades a corrupt marker to absent for availability). No
markers = never joined = the old newest-wins, so a fresh install
keeps loading the latest version. The guard is deliberately
override-blind — the candidate is explicit and the markers are files.
The marker is an ADVISORY projection of committed state (epoch
lineage records); the consensus gates remain the enforcement — never
"fix" a divergence by trusting the file over the database.
(Interpolating the package into the seed script also keeps the seed
generation rooted in the system closure.) `hopnet --version` prints
the COMPILE-TIME version precisely so these comparisons verify bytes,
never a running process's test-mode overrides.

**The module ships in HopNet** (`nix/hopnet-module.nix`, exported as
`nixosModules.hopnet`): the module and the provider form one contract
and must not drift. Deployments import it; their own service modules
reduce to option values. The contract between module and provider is
env (deployment shape, not mesh policy — no DB settings, no schema):

- `HOPNET_UPGRADE_PROVIDER=nix` — selects the provider
- `HOPNET_UPGRADE_NIX_BIN` — the nix binary `stage` invokes (tests
  point it at a fake)
- `HOPNET_UPGRADE_PROFILE` — the exec symlink
- `HOPNET_UPGRADE_STAGE_DIR` — out-links + provenance records
- `HOPNET_UPGRADE_FLAKE_REF` — base ref; default derived from the
  crate's repository field
- `HOPNET_UPGRADE_AUTO_STAGE` / `HOPNET_UPGRADE_AUTO_ACTIVATE` —
  the two knobs, default on

**`stage(X)`.** Derive the flake ref from the release tag —
`<flake_ref>?ref=refs/tags/vX`, the release page as the single source of
truth (the `refs/tags/` prefix is load-bearing: nix resolves a bare
`?ref=` under `refs/heads/`, so a tag asked for by name looks like a
missing branch) —
and `nix build --out-link <stage_dir>/vX` (the out-link doubles as the
gcroot). Verify the built binary's own `--version` answers exactly X —
wrong bytes for the tag are a PERMANENT refusal, never attested — then
record provenance (version, full ref, out path) beside the link.
Nodes build the code themselves for now: a binary cache, when one
exists, turns the same step into a substitution, but no build
infrastructure is required by this RFC. Nothing running changes, and
staging happens proactively on the existing ~6-hourly tick
(`auto_stage`): newest stable release strictly newer than running, one
attempt per tick.

**`report()`.** `staged: X` iff the out-link resolves, the provenance
record matches it, and the staged binary itself answers `--version`
with X — the honest-bytes rule made mechanical. The attestation
pipeline consumes this unchanged.

**Activate.** Verify committed `target_version_code == X` and that the
staged bytes still verify; atomically flip the profile symlink; exit
75. Any failure before the flip parks (contract rule 3). One guard is
load-bearing: **if the profile already points at the staged generation
and the running version is still wrong, refuse** — a previous flip
failed to produce the required binary, and re-flipping would exit-75
into the same state forever. The crash-loop guard is what makes
"never crash-looping" in rule 3 mechanical rather than aspirational.
Activation hooks all three places RFC-019 branches on
`running != target`: the seal-work restart derivation, boot Gate 1,
and the staged-join version gate.

**Supervision.** systemd's existing `Restart=on-failure` already
covers exit 75 — it is how same-version regenesis restarts work today.
nix-darwin/launchd is DEFERRED with the other deployment classes; the
notes stand: `KeepAlive` restarts unconditionally or gates via
`SuccessfulExit`, nix's linker ad-hoc signs, substituted paths carry
no quarantine xattr.

**Privileges.** The profile and stage dir live under the service's own
state directory, owned by the service user. No root, no nix-daemon
write ceremony: staging needs the daemon socket (connect requires
write — one `ReadWritePaths` entry) and store read/build rights;
activation is a rename in a directory the service owns.

**`auto_activate`: on by default.** A nix-deployed node that staged
the target and holds the quorum decision crosses unattended — that is
the point of this RFC. The option exists to opt OUT (park for a
human). Advertised in the upgrade-readiness view (`activation` block):
nix is currently the ONLY deployment class with an activation wrapper;
every other deployment parks at an upgrade boundary and is resolved by
its operator. An activation that was attempted and failed surfaces its
reason through the boundary-error status alongside the park.

## Release publishing (prerequisite, not a slice)

The advisory pipeline is live but the feed is stale: the newest
release on the forge is a pre-CalVer app tag, so nothing
newer-than-running can ever appear. Upgrades become advertisable the
moment CalVer tags are published as releases — no code, just process.
One namespace note: node releases and macOS app releases share `v*`
tags (accepted — one product, one CalVer), so every node release also
triggers the app build on its runner.

## Slices

Resequenced after the fresh-start decision: the live fleet runs a
pre-S3 binary, so nothing polls the release feed until the branch
deploys — publishing first would advertise into a void, and the
branch's wire breaks force a re-formation anyway.

- P1 — module + provider (this RFC's implementation): the
  HopNet-shipped `nixosModules.hopnet` with the profile indirection
  and newest-wins seeding; the provider
  (`stage()`/`report()`/activation + both knobs); the three activation
  hook sites; orchestrator scenario (stage → decide → seal → flip +
  exit 75 → cross) and the NixOS VM test (a declarative relay +
  3-node mesh crossing a REAL upgrade boundary through the module's
  profile flip — the restart path no container can exercise).
- P2 — land and re-form: everything ships in one PR; existing
  deployments are nuked and set up fresh with the wrapper from the
  first boot (no migration surface — the wire breaks already forced
  re-formation).
- P3 — the first coordinated upgrade: tag and publish the next CalVer
  release. The advisory fires, nodes auto-stage and attest, the
  operator submits `regenesis_start`, and the mesh crosses unattended
  — the release publication IS the end-to-end validation.
  - Done 2026-10-01 (v2026.10.1 → epoch 6, three seated validators).
    Operator notes from the crossing: a node's boot attestation used to
    run before its first provider poll and so attested "nothing
    staged", erasing the committed staged claim after every restart —
    the boot task now polls first and a node that can stage never
    claims without an observation (`staged_claim`); the manual tick
    (`POST /maintenance/upgrade-tick`) blocks for the whole `nix build`
    when it has to stage, so run it in the background with a long
    client timeout; a node's API tokens are per process (the JWT key is
    rolled at start), so every restart — the seal's exit 75 included —
    means a fresh sign-in before the status views answer again.

## Addendum: the hotfix lane (same-code releases)

**Status**: Draft (2026-10-01, written during the v2026.10.2 incident)

**Motivation.** A crossing can be blocked by a defect in the release
every validator is already running. On 2026-10-01 the mesh crossed into
v2026.10.2, whose fatal-effect rule (a storage error in a consensus
effect aborts the process instead of being swallowed) met a proposer
whose preflight held the write lock longer than the busy budget: thor's
vote WAL append got `database is locked`, the host aborted, systemd
restarted it, the same height reproduced it — fourteen cores in twenty
minutes, the mesh stalled at 82752 with one validator left. The fix
was twenty lines of host plumbing, but every route to deploying it
needed a quorum that the defect itself was denying: a v2026.10.3
crossing needs thor's votes, and thor lived thirty seconds per boot.
The operator's only move was the manual one — build the patched
binary elsewhere, copy the closure, swap the profile symlink — which
is exactly the action this RFC's wrapper performs, minus the policy
that would let it do so unprompted.

The lane makes that policy explicit: a **hotfix release** carries new
bytes under an UNCHANGED application version, so it interoperates
with the running release by construction and may be applied node by
node, without a boundary.

**The one invariant.** Two nodes on `2026.10.2` and `2026.10.2a`
decide the same blocks, so their apply must stay byte-identical. A
hotfix may change host plumbing (effect retries, WAL handling, queue
pacing), reconciler and storage-job pacing, logging, views, the
daemon, the wrapper itself — anything whose output never reaches
committed state. A hotfix may NOT change transaction validation,
`apply`, the schema or its ordinals, snapshot sections and their
hashes, the RS layout, the wire ALPN class scheme, or any constant a
validator uses to judge a block (block shape, staleness horizon, tx
size limits). The rule is enforced, not trusted: for a hotfix tag the
release workflow runs the RFC-020 chain tripwires and the RFC-025
compat-freeze tripwires against the BASE tag, not the latest release,
and refuses the publish on any kernel-hash or frozen-step delta. A
hotfix that needs to touch the kernel is not a hotfix; it is the next
point release and crosses the ordinary way.

**Identity.** The tag is the base CalVer with a single lowercase
letter: `v2026.10.2a`, then `b`. The workspace version stays
`2026.10.2`, so the version code, the locked ALPN, the agreed-version
marker and every RFC-025 clamp are untouched without any new rule.
Two things become visible that are not today:

- a compile-time **build id** — `2026.10.2a` — baked from the tag at
  release time (`HOPNET_BUILD_ID`, `option_env!`, defaults to the bare
  version for local builds). `hopnet --version` keeps answering the
  bare version, because the seed wrapper, the providers' honest-bytes
  checks and RFC-025 all compare on it; `--build` answers the build id,
  and the readiness view's `mesh[]` rows and the boot banner show it.
  Without this nobody can tell which bytes a node runs, which is how
  the incident went undiagnosed for an hour.
- the macOS `CFBundleVersion` gains the letter as a trailing component
  (`20261002.1` for `a`) so Finder and SMAppService see a newer
  bundle; `CFBundleShortVersionString` stays `2026.10.2` and the
  honest-bytes check compares the staged binary's `--version` to the
  tag's BASE.

**Seed guard.** `hopnet seed-guard --candidate 2026.10.2a` parses the
suffix, compares on the base code, and allows when base == agreed
(the equality case that today reads as "nothing to do"). The module's
newest-wins arm is already correct: GNU `sort -V` orders `2026.10.2a`
after `2026.10.2`, so a flake pin bumped to the hotfix re-seeds a
profile on the base, and never the reverse.

**Providers.** Both staging strategies stage by tag today and pick
"newest stable strictly newer than running" — a hotfix is never
strictly newer. The selection gains a second lane: within the RUNNING
point release, prefer the highest letter whose bytes are not the
running build id. `report()` learns to say `staged: 2026.10.2`,
`build: 2026.10.2a`; the attestation pipeline carries the bare version
as before (the mesh agrees on versions, never on builds), so nothing
in committed state or the readiness quorum changes.

**Activation: locally authorized.** Contract rule 2 stands for a
version change — the quorum-decided target and the node's own staged
bytes. A same-code hotfix is authorized by local policy alone, because
there is no boundary to coordinate: the node restarts into bytes that
speak the same version, resumes at the same height, and nothing it
decides is distinguishable from its neighbours' decisions. Mechanics:

- the hook is the tick, not the seal: once `report()` shows a staged
  hotfix, `auto_activate_hotfix` (a third knob, default on) flips the
  profile and exits 75 on the next tick that finds the node idle;
- **stagger** — a per-node delay drawn from `[0, 5 min)` by node id,
  so three validators never restart in the same second and blip
  quorum; a node skips the restart while it is the pending proposer,
  while a regenesis phase is in flight, or while an epoch join is
  staged (the boundary path owns those restarts);
- the crash-loop guard applies unchanged: profile already on the
  staged generation with the running build id still wrong → refuse
  and park with the reason.

**Rollback (same-code only).** This RFC excludes automatic rollback
across an epoch boundary, and that exclusion stands — reverting a
version re-opens the divergence the forward-only rule kills. A
same-code generation is different: both generations decide
identically, so reverting one node's BYTES cannot diverge state. The
wrapper therefore keeps the previous generation link beside the
profile (`profile.prev`) and, when the supervisor records N aborts
within a minute of a hotfix flip, moves the profile back, records the
rollback in the boundary-error status, and stops auto-activating that
build id. The incident of 2026-10-01 is the case this protects
against: a hotfix that is itself wrong must degrade to the known-good
bytes, not to a crash loop.

**What the lane does not do.** It does not shorten a point release's
path — a crossing is still the only way to change what the mesh
agrees on. It does not let a node run ahead of the mesh: the
agreed-version clamp, the ALPN lock and the boot gates see the same
version before and after. And it is not a channel for schema or
state fixes, however small; the tripwire gate exists so that
temptation fails in CI rather than on the mesh.

### Slices

- [ ] H1 — identity: build id from the tag (`HOPNET_BUILD_ID`,
      `--build`, readiness rows, boot banner); `CFBundleVersion`
      trailing component via `scripts/macos/version.sh`; seed-guard
      suffix parsing with `Should:` tests for allow-on-equal-base and
      hold-on-newer-base.
- [ ] H2 — providers: hotfix-lane selection within the running point
      release for `build-from-source` and `certified-artifact`;
      `report()` build id; honest-bytes compares the base.
- [ ] H3 — activation policy: `auto_activate_hotfix`, tick-driven flip
      with stagger and the proposer / boundary / staged-join skips;
      orchestrator scenario (two nodes on `X`, one staged `Xa`,
      restarts land apart, heights keep deciding throughout).
- [ ] H4 — CI gate: `release-macos.yml` and the Linux checks run the
      RFC-020 and RFC-025 tripwires against the base tag for hotfix
      tags; a kernel delta fails the publish with the diff named.
- [ ] H5 — same-code rollback: `profile.prev`, abort counting in the
      module's wrapper, the rollback record in boundary-error status,
      VM test (hotfix generation aborts on boot → profile falls back →
      node runs the base bytes, status names the rolled-back build).
- [ ] H6 — first use: `v2026.10.2a` carries the WAL-append retry (the
      Decide retry generalized to every storage effect, a bounded
      budget long enough to outlast a preflight) and ships through
      the lane end to end on the live mesh. (Superseded for the 2026-10-01
      incident: the mesh resumed deciding before the lane existed, so that
      fix crossed as v2026.10.3; the lane's first use is the next same-code
      defect.)

## Open questions

1. **Release provenance.** Stage-time provenance pinning is specified;
   whether releases should also carry a detached signature (e.g.
   minisign) that `stage()` verifies before building is open — the
   difference between trusting the forge's TLS and trusting a key you
   hold.
