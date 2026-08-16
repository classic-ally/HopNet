# RFC-021: Nix Upgrade Provider — Staged Binaries and Unattended Boundary Crossing

**Status**: Draft
**Depends on**: RFC-019 (the upgrade-provider seam, `node_staged_version`
attestations, the awaiting-upgrade park, exit-75 restart derivation)
**Related**: RFC-020 (module versioning — what an upgrade means);
`.forgejo/workflows/release-macos.yml` (the release channel this
rides); RFC-023 (the mount client channel transplanting this
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

**Seeding, newest-wins.** The module's `ExecStartPre` compares the
flake-pinned package's version against `profile/bin/hopnet --version`:
profile missing → seed it; flake strictly newer → the operator
deliberately bumped the pin, re-seed; profile newer or equal → a
self-upgrade happened, leave it. A rebuild can therefore never regress
a mesh-coordinated upgrade, and a deliberate flake bump still works.
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

## Open questions

1. **Release provenance.** Stage-time provenance pinning is specified;
   whether releases should also carry a detached signature (e.g.
   minisign) that `stage()` verifies before building is open — the
   difference between trusting the forge's TLS and trusting a key you
   hold.
