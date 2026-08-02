# RFC-019: Regenesis — Epoch Compression of Consensus History

**Status**: Draft (2026-07-30)
**Depends on**: RFC-013 (atomic decide, decided-value sync, trusted
height-0 join bootstrap), RFC-016 (projection registry + host API),
RFC-CONSENSUS-002 (membership machinery, evidence layer)
**Amends**: RFC-016 (the `Projection` trait grows a snapshot
export/import seam)
**Subsumes**: issue #10 (consensus history compression)
**Related**: RFC-020 (module versioning — the upgrade policy that rides
this mechanism); RFC-010 (takeout: export/import precedent)

## Motivation

App state is a deterministic function of block history: `on_sync_value`
(`hopnet-consensus/src/host.rs:450`) replays every historical block
through the current binary's apply path. Any change to a handler,
payload struct, or replicated schema changes the transition function —
replaying old blocks through it diverges. That replay bind is what
makes upgrades hard (RFC-020).

Regenesis severs it: compress decided state into a certified snapshot
at a chosen boundary, restart the chain from a new genesis whose
initial state IS that snapshot. Version boundary and history boundary
coincide — no block encoded under old rules is ever applied by a new
binary.

- primary role: the keystone of migration infrastructure; RFC-020 owns
  what changes across the boundary, this RFC owns the boundary
- what stops being frozen-forever: handler semantics, payload encodings
  (bincode-positional), the block envelope, inter-node wire encodings,
  genesis-fixed config — all become per-epoch concerns
- what freezes instead: the snapshot format — one contract,
  adjacent-version readable, instead of five open-ended surfaces
- subsumed payoffs (issue #10): height integer ceiling, fresh-join
  catchup cost, `decided_blocks` disk growth
- incidental: a certified snapshot is a consistent whole-mesh backup;
  orchestrator fixtures seeded from snapshots

## Design Overview

Today every node computes:

    state = fold(apply, empty_schema, blocks[0..tip])

Regenesis introduces epochs. Epoch N+1 computes:

    state = fold(apply, import(snapshot_N), blocks[boundary..tip])

`import` is a new BASE CASE of the same fold — not a second writer.

- the state-machine discipline survives: consensus handlers remain the
  ONLY writers to replicated tables while the engine runs; import
  happens strictly at initialization, before the engine spawns
- epochs are numbered; every mesh is at exactly one epoch; the epoch
  stamp lives in the genesis and travels in the peer handshake
- lineage: each genesis embeds the previous epoch's final block hash
  and decide certificate. Verifying ancestry is walking one small
  certificate per epoch (kept forever) — never replaying blocks
- trust model unchanged, not weakened: RFC-013 joins already do a
  TRUSTED height-0 genesis install before decided-value sync
  (`src/setup.rs` join bootstrap). Epoch join generalizes that exact
  step: trust the certified lineage instead of an unverified height-0
  state
- load-bearing separation, held throughout: SAFETY lives in boot-time
  gates (version, lineage, import hashes) and the peer handshake —
  small, mechanical, enforced. Everything upstream of the boundary
  (pressure advisories, upgrade readiness, operator prompts) is UX and
  may be arbitrarily rich without touching safety
- v1 scope: same-binary regenesis (target = running version) is the
  pilot — it exercises every mechanism (drain, snapshot, certify,
  restart, straggler rejoin) with zero migration semantics; an
  upgrade epoch is the SAME flow with a different target

## Triggers & Advisory Surface

Nothing in this section is safety-relevant (see Design Overview) — a
wrong or missing advisory can waste operator time, never diverge a
mesh. Regenesis is operator-initiated in v1; auto-trigger policy is a
named future knob (`hopnet_regenesis_policy`), not designed here.

Two trigger classes:

- measured pressure (→ same-version regenesis):
  - chain height vs the integer ceiling — consensus state, so the
    advisory threshold is deterministic and identical on every node
  - `decided_blocks` disk footprint, estimated fresh-join catchup cost
    — node-local observations, advisory with per-node variance
  - surface: resilience-pane band + CLI/status route, same shape as
    existing health advisories
- upgrade availability (→ upgrade regenesis):
  - a node advertises READINESS: "my deployment can actually apply
    version X" — not "upstream has a tag". Readiness is deployment-
    specific: a package-manager install, a nix profile, a container
    image, and a bare git checkout all have different notions of
    "staged and applicable"
  - abstracted behind an upgrade provider interface on the node:
    - report: available version(s) this deployment can reach, and
      whether X is staged (downloaded/installed, pending activation)
    - optional stage(X) hook for deployments that can pre-fetch
    - v1 provider: upstream git repo release check (our current
      distribution), reporting available-but-unstaged
    - long tail (apt/nix/docker/appstore…) slots in behind the same
      interface later; per-platform status is the provider's problem,
      not the mesh's
  - advertisement is consensus-tracked: a `node_staged_version` update
    tx (self-reported node attribute, like name/pubkey — one tx per
    node per release, negligible). NOT the evidence layer: that exists
    for subjective observations (reachability); staged version is a
    node's objective claim about itself
  - this makes readiness a deterministic PRECONDITION, not just UX:
    the `regenesis_start` handler validates at propose time that
    every seated validator has `target` staged in committed state —
    a mesh cannot decide an upgrade it visibly cannot complete
  - the claim can still go stale (binary deleted after advertising),
    so the boot-time version gate remains the safety net; a stale node
    strands itself at the boundary awaiting its operator (annoying,
    not unsafe)

## The Regenesis Transaction & Moratorium

The boundary is decided BY consensus, so every node observes it at the
same height. Two transactions, both ordinary consensus txs processed by
the existing pipeline:

- `regenesis_start { target }`:
  - `target` is the version the NEXT epoch requires — always present.
    A same-version restart (housekeeping: height reset, history
    compaction) simply names the current version; an upgrade names a
    newer one. No separate kinds: one precondition, one gate, and a
    sneaky binary swap during a "mere restart" is structurally refused
  - precondition, validated deterministically at propose time: every
    seated validator has `target` staged in committed state (a node's
    running version counts as trivially staged) — a mesh cannot decide
    a regenesis it visibly cannot complete
  - authorization: user-signed by a network admin (same class that
    authorizes membership operations)
  - on decide, every node enters MORATORIUM:
    - admission closes: new submissions are refused with 503 SERVICE
      UNAVAILABLE + a structured "regenesis in progress" body, so
      callers know to retry (UX precedent: RFC-010's `import_gate`)
    - already-staged transactions are NOT rejected — the pool drains
      into subsequent blocks; accepted work completes consensus
    - the staged pool is finite once admission closes, so drain
      terminates; the on-demand engine then goes quiescent on its own
- `regenesis_commit { snapshot_hash }`:
  - proposed as the SOLE transaction of the final block, once the
    proposer observes drain complete (empty pool, tip applied)
  - Rule-8-style validation before voting (RFC-013 precedent): each
    validator recomputes the snapshot over its own state at the same
    height and votes ONLY if its hash matches — snapshot computation
    inside the round is acceptable under on-demand heights (no other
    work is pending by construction; timeout tuning noted in Open
    Questions)
    - if the proposal reaches quorum DESPITE a local mismatch, the
      decided hash wins — decided is decided. The dissenting
      validator's own state is the anomaly: silent divergence or
      corruption, detected here for free. It recovers by rebuilding as
      if it were a fresh post-regenesis joiner — discard replicated
      state, import the certified snapshot — while KEEPING its local
      fragment store (fragments are content-addressed and unaffected;
      inventory reconciles after rejoin). Regenesis is thereby the
      mesh's first self-healing point for divergent replicas
  - consequence: the decide certificate of the final block IS the
    snapshot certificate — no out-of-band signature collection, no new
    crypto; the machinery that already attests blocks attests the
    snapshot
  - after commit decides: engine halts; the epoch is sealed. Nothing
    past this block will ever be decided in epoch N
  - restart behavior is DERIVED, not declared: each node compares its
    running version to `target` — equal → proceed to restart
    immediately; different → park in "awaiting upgrade" until its
    operator swaps the binary

## Snapshot & Certificate

The snapshot is a CANONICAL LOGICAL export — never a database file
copy (SQLite page layout, freelists, and vacuum state differ across
nodes; the bytes must not).

- covered set: the divergence checker's table universe — one registry
  (per-module `SNAPSHOT_SECTION` specs assembled by the host,
  `src/db/snapshot.rs`) is the single source of truth for "what is
  consensus state", pinned against `sqlite_master` by test so a new
  table must be classified at birth. Node-local tables (`this_node`,
  pins, queues, the engine WAL/cursor/certificates, drive's
  modification log) sit outside the universe entirely. One carve-out
  INSIDE the universe: `decided_blocks` (epoch history) is CHECKED for
  divergence — it is the live mesh's agreement invariant — but
  EXCLUDED from the snapshot export set: history dies with the
  retained epoch-N database (Archival & Retention), and epoch-N+1's
  chain tables are born from the genesis installer, not the import.
  The registry carries this as a per-table role (`Exported` vs
  `DivergenceOnly`)
- determinism rules:
  - sections ordered by module (manifest order = FK direction, same
    ordering `install_schema` already walks)
  - rows ordered by primary key; canonical value encoding; no
    wall-clock reads anywhere in the export path
- manifest: per-section hashes + a top hash; `snapshot_hash` in
  `regenesis_commit` is the top hash. Sections carry names + format
  version — unknown sections skip cleanly on import (takeout manifest
  v2 precedent), which is what "adjacent-version readable" rides on
- the seam (RFC-016 amendment): `Projection` grows snapshot
  export/import, the mesh-scoped sibling of `ProjectionExporter`
  (`hopnet-projection/src/lib.rs:254`, per-user takeout). The host
  orchestrates sections in manifest order; each module owns its own
  section's shape — schema evolution at import time is each module's
  translator (RFC-020's business). Landed (S2) as
  `Projection::snapshot_section`/`node_local_tables` plus
  `hopnet_common::snapshot::import_snapshot` (fresh-DB import with
  skip-reporting)
- artifact + serving: written next to the database; joiners and
  rebuilding nodes fetch it from any peer and verify against the
  certificate before import — the epoch analog of decided-value sync.
  v1 serves the LATEST snapshot only (multi-epoch stragglers: see Boot
  Paths)

## The New Genesis

Every node constructs the epoch-N+1 genesis INDEPENDENTLY and
deterministically from decided artifacts — nobody proposes it, and all
nodes must derive byte-identical geneses:

- contents:
  - `epoch`: N+1
  - `required_version`: the `target` from `regenesis_start`
  - lineage: epoch-N final block hash + its decide certificate
  - initial state: the certified snapshot (by hash reference; the
    artifact travels separately)
  - carried VERBATIM from epoch N: seated validator set as of the
    seal, quorum profile, genesis-fixed policy configs. Changing any
    of these at a boundary is upgrade-epoch business (RFC-020); the
    mechanism here only guarantees a legal mutation point exists
- heights are CONTINUOUS across epochs — the new genesis sits at the
  boundary height H; the first decided block of epoch N+1 is H+1:
  - replicated state is full of height references (placement_height,
    modification-log heights, height-anchored decay scoring); a reset
    to 0 would require rewriting them all during export — rejected
  - the height integer ceiling therefore remains (astronomically far);
    the #10 pains this RFC actually discharges are catchup cost and
    history storage
  - integer exhaustion: the engine is u64 end-to-end
    (`hopnet-consensus/src/types.rs:479`); the binding ceiling is
    app-layer i32 heights (`current_height`, validator activation
    heights). Two mitigations: widen the app layer to u64, matching
    the engine, while meshes are disposable (pre-regenesis cleanup,
    tracked in Implementation Slices); and if a ceiling ever looms
    anyway, a future upgrade-epoch may re-base heights via import
    translators — continuity is the v1 choice, not a one-way door
    - SQLite's INTEGER is signed i64; we do not care about the sign
      bit, but the u64 ⇄ i64 ser/de at every height column must be an
      explicit, lossless mapping across the full 64-bit range (bit
      cast, not bounds-checked narrowing) so no height value is
      unrepresentable or silently wrapped
- epoch join trusts this construction: a joiner receives genesis +
  snapshot from any peer, verifies lineage certificate(s) back to a
  root it trusts, verifies the snapshot hash, imports, then
  decided-value syncs the (short) tail from H+1

## Restart & Validity Gates

After the commit decides, each node seals epoch N (snapshot written,
certificate stored), then:

- `running == target`: restart immediately — process restart under the
  service manager in v1 (an in-process engine restart over the new
  genesis is a later nicety, not load-bearing)
- `running != target`: park in AWAITING-UPGRADE — a marker file +
  status surface (UI banner, CLI, distinct exit code); the node's
  upgrade provider or operator applies the swap, and the new binary
  completes the boot

Boot gates, in order — all mechanical, all mandatory:

1. VERSION: `my_version == genesis.required_version`, exact match —
   not `>=`. A newer binary joining an older epoch goes through
   RFC-020 adjacency rules, never by silently running a future
   transition function against an old epoch. Refusal parks the node at
   awaiting-upgrade; it never runs wrong semantics
2. LINEAGE: verify the certificate chain — epoch-N final block cert
   signed by the epoch-N seated set the node already trusted
3. IMPORT: import into a FRESH database (current binary's full schema
   via `initialize` — which this design redeems as the permanent
   fresh-schema installer), then recompute per-section hashes against
   the manifest. `compute_manifest` (`common/src/snapshot.rs`, the S1
   serializer) is the machinery for exactly this comparison. Crash
   mid-import is safe: fresh file + atomic swap, retry idempotent
4. NODE-LOCAL CARRY: node-local tables (`this_node`, pins, inventory)
   are carried into the new database; their schema evolution is
   ordinary linear migrations (RFC-020 owns the policy; the hook point
   is here)
5. HANDSHAKE: hello carries `(epoch, version)`; refuse mismatched
   peers. Among honest nodes this makes divergent-version meshes
   structurally impossible — a Byzantine claimant is already
   consensus's threat model and cannot produce matching votes with
   wrong semantics anyway

Liveness needs no extra machinery: the new engine decides nothing
until a quorum of the carried validator set boots past the gates.

## Boot Paths & Stragglers

After this RFC there are two boot paths, not three — epoch join
SUBSUMES today's join:

- fresh mesh creation (`post_setup`): unchanged; creates the epoch-1
  genesis with empty initial state
- joining any existing mesh: fetch the CURRENT epoch's genesis +
  snapshot from a peer, run the boot gates (version, lineage, import),
  decided-value sync the tail. On an epoch-1 mesh the snapshot is
  empty and this degenerates to exactly today's trusted height-0
  bootstrap — same flow, generalized

Straggler cases, by how far behind:

- crashed after commit, before restart: sealed artifacts are already
  local; finish the boot gates on wake — no peer needed
- offline through the regenesis (never saw `regenesis_start`): wakes
  in epoch N below H; peers answer with epoch-mismatch + the lineage
  record. Verification is subject to the weak-subjectivity limit
  below; on success it discards replicated state and epoch-joins
- multiple epochs behind: every genesis's lineage record is retained
  forever (bytes per epoch, not blocks); the node verifies the chain
  of boundary certs from its trusted epoch to current (overlap rule
  per hop), then imports the LATEST snapshot — v1 serves no older ones
- diverged validator (dissenting hash at commit, outvoted): same path
  as offline-through-regenesis — see Transaction section

Lineage verification & weak subjectivity: compression discards the
blocks that would prove validator-set transitions, so a returning
node's trusted set may predate the set that sealed the epoch:

- overlap rule (the v1 verification): accept the boundary cert if its
  signers include more than the Byzantine bound OF THE NODE'S OWN
  last-trusted seated set — we already extended trust to that set, and
  past its fault bound at least one honest member we trusted signed
  the boundary. Threshold derives from the active quorum profile via
  `hopnet_common::quorum`, never a hard-coded fraction
- churn beyond overlap → manual re-trust, and that path deserves a
  first-class UI: the operator points the node at a peer they trust
  and it re-bootstraps from the current epoch, keeping its fragment
  store. Not new trust machinery — the same TOFU ceremony as the
  original RFC-013 trusted join, re-invoked
- pre-regenesis advisory: the evidence layer already knows which known
  nodes are dark; the regenesis UI warns that node X may fall beyond
  overlap and need manual re-trust (UX only, see Triggers)
- deliberately NOT taken: retaining a membership-transition digest
  (set-change certs walked sequentially) would close arbitrary churn
  cryptographically, but it would be the one artifact required to
  stay format-consistent across every epoch forever — reintroducing
  the freeze-forever class this design exists to eliminate. Noted as
  a future possibility if manual re-trust proves burdensome

What survives on every path:

- the FRAGMENT STORE: content-addressed files on disk, untouched by
  any of this. Placement/ownership state arrives in the snapshot;
  node-local inventory reconciles through the existing
  attestation/repair flows (RFC-STORAGE-002) after rejoin
- node identity: keys unchanged; the seat is whatever the current
  epoch's genesis (plus subsequent membership blocks) says — a node
  voted out while dark rejoins as standby under existing membership
  rules (see Membership Across the Boundary)

## Membership Across the Boundary

- the seated set carries VERBATIM into the new genesis (see The New
  Genesis); standby nodes carry as standby. Regenesis is not a
  membership event — no seats are granted or revoked by crossing
- during the window: quiescence gates the membership machinery for
  free. Vote-outs and seatings are consensus decisions; no heights
  decide between seal and restart, so a coordinated upgrade can never
  be misread as mass validator failure
- the evidence layer is ephemeral BY EXISTING DESIGN — `EvidenceMap`
  is in-memory, created at process start (`src/main.rs:265`), never
  persisted. A whole-mesh restart is within its design envelope, and
  RFC-CONSENSUS-002's mechanics already compensate: probes re-seed
  within `t_probe`; vote-out attestations need an evidence span
  (`s_full`, 30 min) before acting — a built-in grace that protects
  slow restarters; proven-ness reads committed activation heights,
  not evidence, and with continuous heights every carried seat stays
  proven across the boundary. Consequence, not decision: epoch N+1
  starts with fresh evidence, a ~30 min vote-out/seating quiet
  period, and a stable ceiling
- dark seats: a validator that never returns occupies a carried seat
  with no live node behind it. The engine simply waits for quorum of
  the carried set (the liveness gate); once live, normal
  RFC-CONSENSUS-002 machinery accumulates fresh evidence and votes the
  dark seat out. If too many seats are dark to reach quorum, the mesh
  correctly refuses to proceed — surfaced in the status UI rather
  than worked around
- model obligation: the epoch transition (seal → carry → restart →
  fresh evidence) lands as an extension of
  `hopnet-consensus/spec/validator_membership.qnt`, which already
  models seating, leave, vote-out, and quorum profiles. Properties to
  check: a boundary never changes the seated set; post-restart
  vote-out of dark seats preserves quorum safety from a carried
  configuration; no epoch-N evidence influences epoch N+1

## Failure Modes

Ordered by when they can occur:

- drain wedges after `regenesis_start` (staged tx repeatedly fails,
  proposer flaps):
  - observable: moratorium holds (503s), heights still decide, no seal
  - the mesh is fully alive — old chain intact, nothing crossed
  - recovery: `regenesis_abort` (admin-signed, same authorization as
    start) reopens admission and cancels cleanly
  - the abort window is exactly (start decided, commit decided);
    after commit the epoch is sealed and recovery is forward-only
- `regenesis_commit` cannot reach quorum (validators compute
  differing snapshot hashes):
  - this is latent state divergence SURFACING, not being caused —
    the boundary is a free integrity audit
  - observable: commit proposals fail round after round; moratorium
    holds; old chain intact
  - diagnosis: the state-snapshot debug route
    (`src/consensus/routes.rs:421`) pinpoints the divergent section
    per node pair
  - recovery: the anomalous node rebuilds via epoch join (see
    Transaction section), then retry; or abort
  - nothing is ever lost by a failed commit — nobody crossed
- proposer crashes mid-commit-round:
  - the commit is an ordinary block; proposer rotation and round
    machinery already handle it
  - no special case: any validator that observed drain-complete can
    propose the commit
- node crashes between seal and restart:
  - all boundary artifacts (snapshot, certificate, genesis) are
    deterministic functions of the sealed local state
  - recovery: recompute on wake — idempotent, no peer needed
- crash mid-import:
  - import targets a fresh database file, swapped in atomically only
    on completion (see Restart & Validity Gates)
  - recovery: retry from the sealed artifacts — idempotent; the
    half-written file is discarded
- catastrophic target binary (upgrade decided, new binary fails to
  boot everywhere):
  - each node RETAINS its sealed epoch-N database until epoch N+1
    decides its first block
  - within that window: `POST /consensus/regenesis/rollback` on
    EVERY node. Each writes a rollback marker and restarts; the boot
    path then abandons the boundary before anything else runs —
    discarding the N+1 database, restoring the retained one, and
    clearing the seal state inside it. A node that sealed but never
    crossed has nothing to restore and clears in place, so the same
    request works mesh-wide (S8)
  - **restoring the retained file by hand is NOT a rollback.** It
    still carries the sealed marker and the committed Sealed phase,
    so the next boot either re-crosses the boundary (same binary —
    silently undoing the rollback) or parks with consensus refusing
    to start (older binary). The marker exists because those two
    pieces of state must be cleared with it
  - rolling back CLEARS committed `regenesis_state` outside
    consensus. Deliberate: a Sealed phase refuses every submission,
    so it is the only way the mesh runs again. Every node does it
    identically, and the row is divergence-only, so it never enters
    the exported state hash
  - roll back the WHOLE mesh. A node that rolls back beside peers
    still in N+1 is pulled straight back across by the epoch join
    (S7) — correct behaviour, not what the operator wanted
  - safe precisely because N+1 has decided nothing; no history
    exists to fork against
  - after N+1's first decide the retained database is released and
    rollback is gone — the request is refused with 409; recovery is
    another regenesis, forward
- snapshot artifact lost mesh-wide (state still live):
  - the artifact is regenerable from any live mesh: a fresh
    housekeeping regenesis mints a new snapshot at a new boundary
  - consequence: snapshots are never precious; only the lineage
    records are kept forever, and they are bytes

## Archival & Retention

Per-epoch artifacts, by lifetime:

- kept FOREVER: the lineage record — epoch number, required_version,
  final block hash, seal certificate, carried seated set. Bytes per
  epoch; the only unbounded-lifetime data in the system
- kept until epoch N+1 first decides: the sealed epoch-N database,
  whole (the rollback window — see Failure Modes). Released
  automatically after; operators MAY archive it cold instead
- kept while current: the latest snapshot artifact (serves joiners
  and rebuilders); superseded snapshots are deletable
- nothing else: the consensus WAL is empty at the seal by
  construction (quiescence), and epoch-N blocks live inside the
  retained database — when it is released, the history is gone,
  which is the point

## Non-Goals

Each tracked, not forgotten:

- what changes ACROSS a boundary — migration content, import
  translators, genesis-fixed config mutation: RFC-020
- client API versioning (the ingress daemon and Swift extensions
  skew independently of node epochs; needs `/api/version` +
  additive-route policy): separate issue, orthogonal to epochs
- fragment ciphertext format: FORMAT-FROZEN
  (`hopnet-storage/src/crypto.rs`) stays frozen; epochs never
  re-encode data at rest
- auto-triggered regenesis (`hopnet_regenesis_policy`): named,
  deferred — v1 is operator-initiated
- membership-transition digest verification: future possibility for
  churn beyond overlap (see Boot Paths); v1 answers with manual
  re-trust
- serving older-than-latest snapshots: v1 serves latest only
- in-process engine restart for housekeeping epochs: nicety, not
  load-bearing
- deployment orchestration (how a binary actually gets swapped —
  nix, apt, docker…): the upgrade provider interface is the
  boundary; everything behind it is out of scope

## Implementation Slices

Each PR-sized, landing green; detail deferred to each slice's own
planning — these fix ORDER, not content. The model precedes the
protocol it checks.

- [x] S0 — app-layer height widening to u64
  - lossless u64 ⇄ i64 mapping at every SQLite boundary, full-range
    tests; consensus-affecting, so ships while meshes are disposable
- [x] S1 — canonical snapshot serializer (pure function, no protocol)
  - covered set = divergence-checker universe (with the
    `decided_blocks` export carve-out and six previously-untracked
    replicated tables brought in); domain-tagged binary encoding,
    golden tests, orchestrator divergence gate rebuilt on the manifest
    (and made to actually fail on divergence)
- [x] S2 — Projection snapshot seam + fresh-DB import
  - the RFC-016 amendment (`snapshot_section`/`node_local_tables`,
    sections assembled through the registry); bounds-checked artifact
    import into a fresh schema with parse-to-skip for unknown sections;
    byte-identical roundtrip gate green on the full host schema
- [x] S3 — staged versions + upgrade provider
  - `node_staged_version` tx; v1 git-release provider; advisory
    surface (route only — pressure bands await OQ5)
  - landed with two recorded deviations: versions are CalVer YYYY.M.N
    integer codes (Cargo.toml authoritative; equality AND ordering are
    integer math), and the staged claim is a SINGLE slot on `nodes`
    (running/staged/attested-height columns) rather than a set — any
    real deployment has at most one pending version, and each
    attestation overwrites wholesale, so a claim upstream moved past
    self-cleans. The v1 provider reports available-but-unstaged only,
    so attestations are running-only until a staging-capable provider
    exists; the propose-time precondition remains S5's
- [x] S4 — qnt: epoch-transition extension of
      `validator_membership.qnt` + the SEAL CONTRACT
  - invariants verified BEFORE the protocol is coded: seal safety
    (nothing decides past the boundary in epoch N), abort enabled
    strictly inside (start, commit), boundary preserves the seated
    set; dark-seat vote-out from a carried configuration is
    quorum-safe; no cross-epoch evidence flow
  - the engine's seal contract lands here as a short
    `hopnet-consensus/spec/` note in engine vocabulary (terminal
    height, restart from H+1), prose deferring to the model
  - landed as `epoch_policy`: a phase machine (normal / moratorium /
    sealed / restart) over a `membership_policy` instance. The load-
    bearing move is the bridge: restart carries committed state
    (seated set, activation-height proven-ness) verbatim and wipes
    in-memory evidence, and that state satisfies the membership
    machine's existing inductive invariant — so the whole safety
    matrix (no-harm, ceiling, floor, no-exile) transfers across the
    boundary depth-free (Apalache, `epochIndInv`), including the
    dark-seat property; the quiet period falls out structurally
    (vote-out/seating windows re-accrue from zero). Seal safety is
    the `decidedPastSeal` ghost, checked inductively. Modeling notes:
    no epoch counter (properties are per-crossing); the staged pool
    is abstract and admission closes structurally at start, so drain
    termination is a bounded witness rather than a temporal proof;
    membership commits during the moratorium consume the pool (staged
    vote-outs legitimately decide in the drain). Seal contract:
    `hopnet-consensus/spec/regenesis-seal-contract.md`
- [x] S5 — boundary protocol: `regenesis_start`/`commit`/`abort`,
      admission gate, drain, vote-iff-match, seal
  - deterministic-sim + fault-fuzzing coverage
  - landed as `src/regenesis/` (handlers, gate, seal, routes) plus the
    engine hooks. Resolutions recorded:
    - OQ2 (authorization): the membership-ops class — a SEATED
      validator's node signature; no admin role exists and none was
      invented (the human gate is the authenticated
      `/consensus/regenesis/*` route). Roles stay deferred with
      issue #21.
    - OQ4 (drain-complete): staged 0 AND inflight 0 AND tip applied,
      observed by the proposer's own NeedValue path; a drain watcher
      nudges the on-demand engine with idempotent Resumes while the
      committed phase is moratorium (an empty pool never fires the
      work signal), re-armed on every engine start for crash recovery
    - `snapshot_hash` is blake3 over the ARTIFACT bytes (Exported
      tables only), NOT the manifest top hash: the top hash covers
      divergence-only state (`regenesis_state`, `decided_blocks`),
      which moves when the commit applies and with every height — no
      post-seal recompute could match it. The artifact identity is
      stable across the seal by construction and is exactly what an
      S7 joiner verifies against the certificate
    - freeze enforcement is LAYERED: HTTP 503 (+ structured
      `RegenesisRefusalView`, Retry-After) → queue-chokepoint gate
      covering every client route AND internal cron → vote-time
      checks (solo block, seal-height binding, vote-iff-match Live-
      only, own-drain, sealed-refusal) → engine halt. The
      admissibility predicate (`regenesis::gate`) is ONE seam,
      deliberately generic: future dynamic module enable/disable adds
      committed-state inputs to it, no new chokepoints
    - drain semantics kept per spec (staged work completes; the
      forwarded path stays ungated BY DESIGN). Byzantine drain-padding
      degrades to the wedged-drain failure mode, answered by abort; a
      consensus-visible drain deadline is noted as future hardening
    - the sealed mesh parks: `spawn_engine` refuses over the durable
      `regenesis_sealed_at` marker and recomputes a missing artifact
      idempotently — the S6 restart consumes both
    - observed while building the e2e: a seated node that misses the
      FINAL block (lagging at the instant the commit decides) is left
      in the moratorium with a halted mesh around it — nothing pushes
      the seal to it. This is the existing straggler case ("offline
      through the regenesis") and lands with S7's rejoin (peers answer
      epoch-mismatch + lineage); with on-demand heights the window is
      tiny (a drained moratorium is height-stable), but S7 should keep
      it explicitly in scope
- [x] S6 — genesis construction + restart: lineage, boot gates,
      awaiting-upgrade parking, `(epoch, version)` handshake
    - the canonical genesis is a deterministic engine Block at the
      boundary height H whose single synthetic transaction carries the
      `EpochGenesisRecord`; **chain_id(N+1) = that block's hash** —
      the epoch-1 rule reapplied, so the signing domain rotates at
      every boundary. The node-divergent input (the final decide
      certificate — `decided_certificates` is node-local by design)
      is EXCLUDED from the canonical bytes and travels beside the
      genesis in the lineage record (`lineage/epoch-E.bin`, kept
      forever) as per-node evidence, verified against the seated set
      rather than compared byte-for-byte. A golden test pins the
      canonical encoding
    - the transition runs at BOOT, pre-pool (`regenesis::boot`), as a
      crash-safe state machine over file presence: build the fresh
      database at `database.db.next` (ONE transaction: certified
      import with any skipped section fatal, node-local carry —
      whole tables by ATTACH-copy, fragment columns by primary-key
      join — genesis at H, fresh consensus meta incl. `epoch` and
      `epoch_genesis_height`), checkpoint both files, rename old →
      `database.db.sealed` (rollback window: deleted at the new
      epoch's first decide), rename next → live. The between-renames
      crash window is completed on the next boot without re-running
      gates; a live-db-missing state with only a retained database is
      fatal-loud, never a fresh boot over a mesh identity
    - restart behavior DERIVED per spec: the seal work compares the
      committed target with `version::effective_running_code()`;
      match → `restart_signal` and the BINARY exits **75**
      (EX_TEMPFAIL; systemd `RestartForceExitStatus=75` or plain
      Restart=always — the orchestrator restarts containers
      explicitly). Library code never exits, so in-process tests
      observe the Notify. Mismatch → awaiting-upgrade: marker file +
      status surface (`awaiting_upgrade`, `running_version`, `epoch`,
      `rollback_retained`, `boundary_error` on the status view),
      process parked ALIVE on the sealed database
    - handshake v1: status Ping/Pong carries `(epoch, version_code)`
      both ways (structured warn on mismatch — the silent signature-
      domain failure made diagnosable); `DecidedFetch` carries the
      requester's epoch, refused with a structured error on mismatch
      — the exact hook S7's epoch join extends into a lineage answer.
      DecidedFetch is now served WITHOUT a live engine (a sealed/
      parked node answering a laggard its final blocks is rejoin
      machinery). Bincode positional wire compat with older binaries
      is broken by these fields — accepted, dev meshes are disposable
    - test seams: `HOPNET_UPGRADE_VERSION_OVERRIDE` /
      `HOPNET_UPGRADE_STAGED_OVERRIDE`, honoured in test mode only —
      a release-image node can claim a different running/staged
      version, so awaiting-upgrade parking and upgrade-target starts
      are testable without a second image (the real binary swap is
      S8's)
    - sim + Quint: NOTHING added — `epoch_policy`'s `restartEpoch`
      already models the transition (the S4 bridge lemma carries
      safety across it), and process restart is host-layer file/DB
      machinery outside the sim's engine vocabulary. Evidence instead:
      boot unit tests (gates, crash states, carry, idempotency), the
      in-process single-node transition roundtrip (seal → exit signal
      → transition → fresh engine → H+1 decides → rollback close →
      epoch 2 seals itself again), and the two orchestrator scenarios
      (regenesis-restart: full cycle over real containers incl. exit
      75; regenesis-awaiting-upgrade: parked-alive + per-node swap +
      the liveness gate observed — one upgraded node decides nothing
      alone)
    - app-layer heights were already u64 end-to-end (the RFC's i32
      widening concern was discharged before S6 — verified, no work)
- [x] S7 — stragglers + rejoin: overlap verification, snapshot
      fetch, re-trust route, fragment reconcile
  - orchestrator: straggler-rejoin, diverged-node-rebuild
    (`housekeeping-regenesis` as listed IS the same-version full
    cycle S6 landed as `regenesis-restart` — recorded here rather
    than renamed, so the ledger and the scenario registry agree)
  - **OQ3 resolved — artifact transport:** a NEW `regenesis` comms
    scope, not the fragment RPC and not HTTP. It serves `EpochInfo`,
    `LineageFetch`, `SnapshotInfo`, and `SnapshotChunk{offset,len}`.
    Chunked at 4MiB under the transport's 8MiB frame cap; plain RPC
    rather than a streamed scope so the transport's retry-once and
    receiver-side dedup apply, which makes the download resumable by
    construction — across peer rotation AND across a restart. The
    scope answers WITHOUT a live engine, like DecidedFetch: a parked
    or sealed node rescuing a straggler is load-bearing. It runs on
    the queue runtime for the same reason (rejoin liveness must not
    be starved by API load).
  - **Artifact availability is honest, not fabricated.** A server
    resolves the artifact in preference order: the on-disk file when
    its blake3 matches the current lineage record; a recompute from
    the retained `.sealed` database during the rollback window; a
    re-serialize of the live database while nothing has decided past
    H (valid by the boot rebuild's roundtrip gate). Otherwise it
    answers `NotAvailable` and the requester rotates peers. Every
    node writes the artifact at seal and nothing deletes it, so the
    unavailable case needs mesh-wide file loss.
  - **The straggler stages, then restarts.** The online half only
    fetches, verifies, and stages under `<db_dir>/join-staging/`,
    with the manifest written LAST as the completion marker; the
    rebuild happens in the boot path, which already owns certified
    import, node-local carry, the atomic swap, and the rollback
    window from S6. Nothing online touches the live database, so the
    worst a lying peer achieves is a wasted download. An incomplete
    staging is KEPT (an interrupted download resumes); one that
    fails verification at boot is discarded and refetched.
  - **Gate order for a staged join: VERSION first.** The node-local
    carry copies rows blind and is only safe under exact-version
    equality, so an old binary parks (staging kept) before anything
    schema-touching runs. Then chain + overlap, then the snapshot
    hash, then the S6 `build_next` unchanged.
  - **Fresh joiners import in process, no restart.** A joining
    database holds one row and `import_snapshot_tx` refuses unless
    every exported table is empty, so the precondition is
    machine-checked; there is no file to swap. `JoinInfo` carries
    the epoch and `bootstrap_join` branches on it — epoch join
    subsumes the height-0 bootstrap, whose assertions are bypassed
    rather than loosened. The anchor is the join ceremony itself
    (TOFU), and the FULL chain from epoch 2 is verified.
  - **Overlap rule, and its floor.** `count_trusted_signers` (new,
    beside `verify_wire_certificate`, which only answers
    quorum-of-the-given-set) measures the intersection between a
    boundary certificate's signers and the verifier's own
    last-trusted set; acceptance needs MORE than that set's
    Byzantine bound, resolved from the active profile via
    `hopnet_common::quorum`. Under Majority `f_eq == 0`, so the rule
    degenerates to "at least one validator I already knew" — that is
    this spec's own floor, implemented as written and pinned by a
    test rather than silently strengthened. Votes are recreated at
    the certificate's ACTUAL round: assuming round 0 would zero the
    overlap for exactly the certificates produced under contention.
  - **Only the first hop is ever unanchored.** A TOFU joiner waives
    overlap for the record it roots at, and nothing after: a lineage
    no existing node could have followed is not one a joiner may
    adopt either. Each verified record's seated set becomes the
    trusted set for the next hop, which is what lets a multi-epoch
    straggler catch up through legitimate churn while importing only
    the latest snapshot.
  - **Every path keeps the whole chain.** The S6 transition, a
    staged join, and a fresh join all persist every verified record
    to `lineage/` — a node that arrived by joining can answer the
    next straggler.
  - Triggers, all reaching the same join: a sync refused with the
    new structured `EpochMismatch` (tip poll, driver, lag kick — the
    S5 note about a moratorium-wedged seated straggler is discharged
    here); the status pong's epoch, which is the ONLY signal for a
    node that woke beside a quiet mesh with nothing to sync and no
    gossip arriving; and a boot-gate park, whose node has no engine
    and therefore neither poll nor probe — `main.rs` starts its retry
    loop.
  - Fragment reconcile runs post-swap, pre-engine, as direct SQL:
    re-mark what the new inventory backs and the disk verifies, drop
    what it does not back. NOT the attestation path, whose exact
    previous-count guard suits a live attestation and not a
    wholesale reconciliation. `self_verified_height` stays NULL and
    the existing self-check cron re-attests at its own pace.
  - Deliberately NOT taken: a dead-pin sweep (pins already tolerate
    naming absent blobs by design), an epoch field on `PeerEvidence`
    (the pong trigger needs no stored state), and any frontend work
    (the status view carries `epoch_join` for headless checks).
- [x] S8 — upgrade epoch end-to-end: no-op version bump through the
      full flow; rollback window exercised
  - orchestrator: regenesis-rollback
  - **The version-bump flow was already discharged** by S6's
    `regenesis-awaiting-upgrade` (staged claim → upgrade-target
    start → autonomous seal → parked-alive → per-node swap → epoch 2
    → liveness gate → quorum decides past H).
  - **A second real image was considered and rejected.** Piping a
    version into the container environment IS what that scenario
    already does, and for a NO-OP bump two images built from the
    same tree have byte-identical schemas — so the one thing a real
    binary could test, that the blind node-local carry is safe
    across versions, has nothing to catch. A code-differing bump is
    RFC-020's migrations hook. Recorded so nobody re-derives it.
  - **The rollback window had never been exercised, and the
    documented procedure did not work.** `database.db.sealed` is a
    complete epoch-N database, but it still carries the sealed
    marker and the committed Sealed phase, which `boot_transition`
    reads only AFTER it would re-run the gates. A bare `mv` gave
    either a silently undone rollback (same binary: every gate
    passes again and the boundary re-crosses) or a live-but-frozen
    node (older binary: parked, with `spawn_engine` refusing to
    start consensus). Both the Failure Modes text and the state-D
    hint in `boot.rs` were wrong; both are corrected.
  - **Rollback is now a marker the boot path honours**, in the same
    convention as awaiting-upgrade and the staged-join manifest.
    It runs ahead of every other boot path — before the
    missing-database dispatch, because a crash mid-restore leaves
    exactly the arrangement state D calls fatal, and before the
    staged-join branch, because leftover staging would drag the node
    back across. One marker covers three arrangements (restore and
    clear; clear in place; refuse — nothing to abandon), and
    deleting it LAST makes the whole thing resumable: a crash
    re-enters the machine one case further along.
  - The abandoned epoch's lineage record is deleted. "Kept forever"
    is about epochs the mesh actually entered; serving a joiner a
    record for a boundary the mesh abandoned only makes it chase a
    snapshot that will honestly answer `NotAvailable`.
  - Evidence: boot units for each arrangement and each crash point,
    including a regression that pins the bare-`mv` re-cross so the
    reason for the mechanism cannot be lost; a route unit for the
    409 guard; and `regenesis-rollback`, which crosses ONE node to
    hold the window open (a live quorum closes it in ~15s, because
    `nodes` rides across the boundary and the attestation job
    decides H+1), abandons mesh-wide, and then checks the thing that
    matters — the mesh accepts a write and decides again — before
    confirming rollback is refused once the next epoch decides.

## Evidence

No new verification machinery — new scenarios for existing tools:

| property | tool |
| --- | --- |
| snapshot canonicality/determinism | golden tests + orchestrator cross-node hash compare |
| import correctness | post-import `compute_manifest` parity + byte-identical roundtrip |
| drain reaches quiescence under faults | deterministic sim + seeded fuzzing corpus |
| membership safety across the boundary | `validator_membership.qnt` extension |
| end-to-end epoch upgrade | orchestrator gates (S7/S8 scenarios) |

## Open Questions

1. Commit-round pacing: Rule-8 recompute of the snapshot inside the
   voting round is seconds-slow on large states — acceptable under
   on-demand heights, but timeout tuning needs measurement
2. Authorization class for start/abort: "network admin" needs a
   concrete definition (genesis user? explicit role?) — shared with
   issue #21's remove_node authorization
3. ~~Snapshot artifact transport~~ — RESOLVED in S7: a dedicated
   `regenesis` comms scope, chunked offset/length over plain RPC so
   retry and dedup make the fetch resumable. See the S7 ledger entry.
4. Drain-complete detection from the proposer's seat: empty pool +
   tip applied, vs forwarded-transaction races — exact rule needs
   the queue's eyes
5. Advisory thresholds: what height/disk pressure lights the
   housekeeping band once heights are u64
6. Cold-archive format for released epoch-N databases (operator
   opt-in) — standardize or leave freeform?
