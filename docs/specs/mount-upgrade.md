# RFC-024: hopnet-mount Auto-Upgrade — the Nix Client Channel

**Status**: Implemented (S1–S3 complete, 2026-08-16)
**Depends on**: RFC-021 (the staging/flip/exit-75 machinery this
transplants — profile indirection, honest-bytes staging, newest-wins
seeding), RFC-023 (the signals this consumes — the 426 policy readout,
versioned health probes, `MIN_NODE`, the upgrade-required daemon state)
**Amends**: RFC-023 (discharges the "Deferred: rollout coupling"
sketch); RFC-018 (the mount's lifecycle gains the upgrade path)
**Related**: `nix/hopnet-mount-module.nix` + `nix/hopnet-module.nix`
(the unit definitions this reshapes and mirrors);
`.forgejo/workflows/release-macos.yml` (the release channel this rides)

## Motivation

RFC-023 made client↔node skew visible and enforceable: a stale mount
now fails loudly at its probe with the versions named, instead of
silently running buggy code. What it deliberately did not do is
provide an auto-upgrade mechanism for mismatched versions — a 426'd
mount stays 426'd until someone rebuilds it by hand.

The node's auto-upgrade does not transfer wholesale, because there is
no multi-node coordination involved with client upgrades. Clients may
activate whenever they like, provided they meet the constraints:

- **Compatible with the pointed-at node** — the candidate's `min_node`
  satisfied by the node, the candidate's version satisfying the node's
  `min_client` (the RFC-023 skew window).
- **Honest bytes** — only staged bytes whose own `--version` answers
  the release tag are ever activated (RFC-021 rule 1).
- **Restart-safe** — a mount restart is user-visible (open file
  handles die), so activation rides moments that are already restarts;
  dirty writes survive regardless through durable staging + startup
  orphan-upload recovery (RFC-018 S7).

Scope: nix deployments, building from the release tag — RFC-021's
posture for the node, unchanged. Non-nix consumers eventually get
prebuilt binaries attached to the same releases; that is a deferred
deployment class (Non-Goals), not this wrapper's concern.

## Version selection

The wrapper learns the node's half of the skew window from ONE
unauthenticated request — a header-less probe of the mount surface's
health route answers 426 with `{min_client, node_version}` (RFC-023's
structured body as a policy readout). The candidate's half — its
compiled `min_node` — comes from the staged binary itself
(`hopnet-mount --min-node`, the sibling of `--version`), recorded in
the provenance file beside the out-link so no tag is ever interrogated
twice.

**The anchor lemma.** The release tagged with the node's own version
is compatible in BOTH directions by arithmetic: a mount built from tag
X has `min_node <= X` (a release cannot require a node newer than
itself), and a node at X has `min_client <= X` (mirrored). So a
compatible candidate always exists once the node itself is
release-tagged, and no probing is needed to find it.

**Selection is a forward walk.** The wrapper is a follower: in steady
state it sits current and each new release is one incremental
question — build it (staging requires that anyway), read its
`min_node` for free, flip or hold. One rule covers steady state and
cold catch-up alike:

- `position := max(currently staged tag, the anchor)` — after a long
  gap this jumps straight to the node's tag, guaranteed compatible by
  the lemma, instead of replaying history.
- For each release newer than `position`, in order: stage, interrogate,
  advance if the node still satisfies its `min_node`.
- Stop at the first incompatibility and HOLD there — the node lags, and
  the wrapper never flips past it. The walk resumes when the node's
  advertised version moves.
- Node ahead of every release (a dev build): the anchor tag does not
  exist; treat the newest release as the candidate, and if the node's
  `min_client` admits no release, hold.

Every step ends with something deployable, each tag is built and
interrogated at most once ever (nix store + provenance cache), and the
expensive case simply does not exist: compatibility checking is a free
read of a binary that staging had to produce regardless.

## The wrapper

`hopnet-mount upgrade` — a subcommand of the daemon binary, not a
resident process. One run: resolve the node URL through the existing
provisioning tiers, take the policy readout (one header-less probe),
poll the release feed, run the forward walk (stage → verify →
interrogate → record provenance), and finish with at most one atomic
temp-link + rename of the profile symlink. The flip is the wrapper's
ONLY output; it never touches the running daemon — systemd is the sole
coordinator (RFC-023's coupling sketch, discharged).

Staging a tag is RFC-021's recipe pointed at the mount package:

1. `nix build <flake_ref>?ref=refs/tags/vX#hopnet-mount --out-link
   <stage_dir>/vX` — the out-link doubles as the gcroot.
2. Verify the built binary's `--version` answers exactly X — wrong
   bytes for the tag are a PERMANENT refusal, never staged.
3. Record provenance beside the link: version, full ref, out path,
   and the interrogated `min_node`.

Env contract, mirroring the node's (deployment shape, not policy):

- `HOPNET_MOUNT_UPGRADE_PROFILE` — the exec symlink
- `HOPNET_MOUNT_UPGRADE_STAGE_DIR` — out-links + provenance
- `HOPNET_MOUNT_UPGRADE_FLAKE_REF` — base ref, default from the
  crate's repository field
- `HOPNET_MOUNT_UPGRADE_NIX_BIN` — the nix binary (tests point it at
  a fake)

The wrapper runs from three triggers, all invoking the same
subcommand:

1. **A systemd user timer** — the steady-state follower tick,
   ~6-hourly like the node's `auto_stage`, but PHASE-OFFSET from it
   (plus `RandomizedDelaySec` jitter) so a co-located node and mount
   never check in the same window: probing a node mid-upgrade reads a
   transient version at best. The offset is hygiene, not correctness —
   a mid-upgrade node just fails the probe and the run holds.
2. **`ExecStartPre` of the mount unit** (bounded timeout,
   offline-safe: feed unreachable → proceed with what is staged) —
   every daemon lifecycle event (login, restart, crash recovery)
   checks for an upgrade before serving.
3. **The daemon itself, once, on entering the upgrade-required
   state** — a 426'd mount is dark, so it spawns one `upgrade` run
   instead of waiting out the timer. When the flip lands the profile
   differs and the daemon exits 75 (Activation); if no compatible
   release exists yet, the run holds and the daemon stays in its loud
   held state. No loop is possible: the exit stays gated on the
   profile actually differing.

## Activation

The unit (`nix/hopnet-mount-module.nix`, both home-manager and NixOS
arms) mirrors `nix/hopnet-module.nix`:

- `ExecStart` goes through the profile symlink instead of a pinned
  store path; the module seeds it newest-wins (`ExecStartPre` compares
  the flake-pinned package's `--version` against the profile's:
  missing → seed, flake strictly newer → deliberate pin bump, re-seed;
  profile newer or equal → self-upgrade happened, leave it). A rebuild
  can never regress a wrapper upgrade; a deliberate pin bump still
  wins.
- `RestartForceExitStatus=75` — exit 75 is the activation request,
  exactly the node's convention (RFC-019 S6).
- systemd resolves `ExecStart` at exec time, AFTER `ExecStartPre` —
  so a flip made by the pre-start wrapper run is picked up by the very
  start that follows it, with no extra mechanism.

The running daemon exits 75 from exactly one place: the
upgrade-required state, and only when the profile's `--version`
differs from its own compiled code — the crash-loop guard (RFC-021
rule 3's sibling). A flip under a HEALTHY running daemon deliberately
does nothing: its restart is user-visible, so the new binary waits for
the next natural lifecycle event, which trigger 2 turns into an
upgrade-and-start. Laziness is the feature; 426 is the only forced
restart.

## Non-Goals

Each tracked, not forgotten:

- **Non-nix clients** — prebuilt binaries attached to releases (a
  static build + a signature story) are the deferred deployment
  class, RFC-021's "other deployment classes" mirrored.
- **The macOS app** — its own update flow; only shares the release
  feed.
- **Quiescence-triggered activation** — flip-while-idle needs a
  quiescence signal out of MountCore; lifecycle events cover the
  fleet, revisit only if they prove too lazy in practice.
- **Binary cache** — pure infrastructure: when one exists, staging
  becomes substitution with ZERO change to this contract (RFC-021's
  note, inherited).
- **Mesh-side orchestration** — clients are not validators; nothing
  about a client upgrade ever touches consensus.

## Implementation Slices

- [x] S1 — `hopnet-mount --min-node` + the `upgrade` subcommand:
      feed poll, policy readout, forward walk, staging with
      provenance, atomic flip; tested against a fake nix and a fake
      feed (the RFC-021 provider test pattern).
      *(As built, 2026-08-16: pure feed/ref logic hoisted to
      `common/src/release_feed.rs` — the node re-imports, so the
      refs/tags lesson lives once. `--min-node` prints the bare CalVer
      token via a pre-clap intercept (Cli requires a subcommand); a
      staged binary that cannot answer it — a pre-S1 release — records
      `min_node: 0`, loudly, safe because such a release only appears
      at or below the anchor. Every operational outcome exits 0 with
      one greppable line (`upgraded:`/`current:`/`held at:`/`offline:`)
      so S2's ExecStartPre never blocks a start; only a missing env
      contract exits 1. No permanent-refusal record on disk: wrong
      bytes never gain provenance, and the nix store itself memoizes
      the build, so re-runs cost a cache hit. The current position is
      read from the profile binary's own `--version` — honest bytes,
      works for module-seeded profiles with no provenance.)*
- [x] S2 — module reshape + activation: profile `ExecStart`,
      newest-wins seeding, `RestartForceExitStatus=75`,
      `ExecStartPre` run + phase-offset timer; daemon exit-75 gate
      and the one-shot spawn on entering upgrade-required.
      *(As built, 2026-08-16: `HOPNET_MOUNT_UPGRADE_RELEASE_URL`
      joined the env contract — the follower poll is
      deployment-shapeable, and S3's fake feed needs it. The timer is
      `03,09,15,21:00` + 30 min jitter — maximally distant from the
      node's {0,6,12,18}h tick. `upgrade.enable` (default on) is
      mutually exclusive with `allowPassthrough`: security.wrappers
      copies the binary at activation, so the wrapped path can never
      follow a flip. The seed script reads the profile path from the
      unit environment because `%h` expands in unit directives only.
      The one-shot is once per ENTRY into the held state — a clear
      (node accepts us again) re-arms it; an AtomicBool keeps the
      spawned wrapper to one child at a time. The daemon's exit
      belongs to the binary: the coupling fires a Notify, main's
      signal select exits 75 only after the clean unmount. Preflight
      mirrors the gate on the start path (mount only, never login).
      Held-426 restart loops are damped by RestartSteps 5s→10min.
      Timers are session-scoped without `loginctl enable-linger`; the
      pre-start check covers every login regardless.)*
- [x] S3 — end-to-end: a VM or orchestrator scenario proving
      stage → flip → exit 75 → the daemon serves from the new binary,
      and the hold path (nothing compatible → loud held state, no
      restart loop).
      *(As built, 2026-08-16: a VM, not an orchestrator scenario — the
      subject IS the systemd machinery (user unit, ExecStartPre
      through the profile, RestartForceExitStatus=75, the timer),
      which containers cannot exercise.
      `nix/mount-upgrade-vm-test.nix`: one machine, single-seat node
      (quorum(1), local iroh relay, hermetic empty feed for the
      node's own follower), alice's session started by
      `loginctl enable-linger` only AFTER provisioning so the first
      start never crash-loops into the RestartSteps damping. Three
      mount generations: the flake pin, a real recompile at
      2026.12.99 (workspace Cargo.toml patched — the node test's
      pattern, dep artifacts cached), and a shell-stub 2099.2.0 whose
      `--min-node` 2099.1.1 no node satisfies — stage() only ever
      interrogates `--version`/`--min-node`, so the hold path needs
      no third compile. The 426 trigger is a new node-side seam:
      `HOPNET_MIN_CLIENT_OVERRIDE` (test-mode-gated, RAISE-only via
      max(), read inside `client_version_gate` so the enforced and
      advertised minimums cannot diverge) flipped mid-run by a
      systemd drop-in + node restart. The feed is nginx serving a
      file the test rewrites between phases. Also fixed here: the
      NixOS module arm now sets `programs.fuse.enable` — the setuid
      fusermount3 wrapper the unit PATH already pointed at is not a
      NixOS default.)*

## Open Questions

1. **Release provenance** — RFC-021 OQ1 verbatim: a detached
   signature `stage()` verifies before building, vs trusting the
   forge's TLS. Shared answer when it comes.
2. **`min_node` as published release metadata** — a selection HINT to
   skip doomed builds entirely (bytes remain the authority via
   verify-after-build). Worth adding when the release workflow grows
   a Linux job; changes nothing in this contract.
3. **Binary cache hosting** — where (asgard?) and when; turns every
   staging build into a substitution for the whole fleet.
