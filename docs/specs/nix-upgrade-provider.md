# RFC-021: Nix Upgrade Provider — Staged Binaries and Unattended Boundary Crossing

**Status**: Draft
**Depends on**: RFC-019 (the upgrade-provider seam, `node_staged_version`
attestations, the awaiting-upgrade park, exit-75 restart derivation)
**Related**: RFC-020 (module versioning — what an upgrade means);
`.forgejo/workflows/release-macos.yml` (the release channel this rides)

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
service-owned nix profile — `ExecStart=/var/lib/hopnet/profile/bin/hopnet`
— instead of a pinned store path. The system flake seeds the profile's
first generation; across hopnet upgrades the unit file never changes,
so `nixos-rebuild` and self-upgrade stop competing over `ExecStart`.

**`stage(X)`.** Derive the flake ref from the release tag —
`git+<repo>?ref=vX`, the release page as the single source of truth —
and realize the closure into the store, rooting it as a pending profile
generation with the resolved rev and narHash recorded beside the root.
Nodes build the code themselves for now: a binary cache, when one
exists, turns the same step into a substitution, but no build
infrastructure is required by this RFC. Nothing running changes, and
staging can happen days before the boundary, retried on the existing
6-hourly tick.

**`report()`.** `staged: X` iff a rooted closure for X is present and
its recorded provenance parses — the honest-bytes rule made mechanical.

**Activate.** Verify committed `target_version_code == X` and that the
staged provenance matches what `stage()` recorded; atomically flip the
profile symlink to the staged generation; exit 75. Any failure before
the flip parks (contract rule 3). The flip itself is atomic — there is
no half-state to land in.

**Supervision.** systemd's existing `Restart=` policy already covers
exit 75 — it is how same-version regenesis restarts work today.
nix-darwin/launchd: `KeepAlive` restarts unconditionally or gates via
`SuccessfulExit`; either satisfies the contract. Darwin signing is a
non-issue in practice: nix's linker ad-hoc signs, and substituted
paths carry no quarantine xattr.

**Privileges.** The profile lives under the service's own state
directory, owned by the service user. No root, no nix-daemon write
ceremony: staging needs store read/build rights, activation is a
rename in a directory the service owns.

**`auto_activate`: on by default.** A nix-deployed node that staged
the target and holds the quorum decision crosses unattended — that is
the point of this RFC. The option exists to opt OUT (park for a
human). To be advertised plainly wherever upgrades surface: nix is
currently the ONLY deployment class with an activation wrapper; every
other deployment parks at an upgrade boundary and is resolved by its
operator.

## Release publishing (prerequisite, not a slice)

The advisory pipeline is live but the feed is stale: the newest
release on the forge is a pre-CalVer app tag, so nothing
newer-than-running can ever appear. Upgrades become advertisable the
moment CalVer tags are published as releases — no code, just process.
One namespace note: node releases and macOS app releases share `v*`
tags (accepted — one product, one CalVer), so every node release also
triggers the app build on its runner.

## Slices

- P1 — publish a release: tag the next CalVer, publish the Forgejo
  release, watch the advisory fire on the live mesh. Proves the S3
  pipeline end to end; zero code.
- P2 — profile indirection: the nix-config module change
  (service-owned profile, flake seeds the first generation, `ExecStart`
  via the profile). Deployable and testable before any provider code —
  behavior is identical until something flips the profile.
- P3 — the provider: `stage()`/`report()`/activation hook +
  `auto_activate`; orchestrator scenario driving stage → decide →
  activate → cross with a file-based fake release.

## Open questions

1. **Flake-vs-profile ownership.** After a self-upgrade the profile
   and the system flake disagree about the hopnet version; rebuilds
   must not regress the profile. Position: the flake pin is the seed
   generation only — but this is nix-config policy and gets decided
   there.
2. **Release provenance.** Stage-time rev/narHash pinning is
   specified; whether releases should also carry a detached signature
   (e.g. minisign) that `stage()` verifies before building is open —
   the difference between trusting the forge's TLS and trusting a key
   you hold.
