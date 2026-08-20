# RFC-STORAGE-003: Block Lifecycle Convergence

**Status**: Draft (2026-08-20)
**Depends on**: RFC-STORAGE-001 (the durability policy and normative
model this extends), RFC-STORAGE-002 (the tick/engine seams and stage
discipline this builds on), RFC-014 (the distribution engine, and the
NULL-placement reconciliation follow-up named twice in engine/mod.rs
that this RFC discharges)
**Amends**: RFC-STORAGE-001 (the model gains pipeline states —
origin-held init, declare/confirm rungs, a lossy-mark adversary — and
INV-CONVERGE), RFC-014 (distribution ownership: the decide-time kick
demotes to a latency hint; the reconciliation scan becomes the
authority)
**Absorbs**: RFC-007's remaining storage surfaces (orphan-cleanup
scheduling, lost-shard recovery, rebalance triggers); the
self-attestation half of the attestation RFC (disk-truth verification;
retrieval reputation stays out)
**Related**: RFC-020 (schema evolution for any new column/tx), RFC-025
(any new RPC scope is born under the ALPN class scheme), regenesis
(`reconcile_fragment_store` is the one-shot ancestor of the disk-truth
rung)
**Unblocks**: RFC-STORAGE-004 — performance-aware placement (weight
derivation stays a single injected seam; deferred by design)

## Summary

Every committed, undeleted blob converges to policy-conformant
placement from any reachable pipeline state within a bounded calm
window, under the fault budget.

RFC-STORAGE-001 establishes this only under the assumption of
successful placement. The entire upload-to-placement pipeline sits
outside the proof, and there are gaps the model cannot see — one
found in production, two by inspection:

- blobs stranded unplaced with no retry path,
- local-state marks dropped permanently by a full channel,
- a migration cursor that re-selects the same blob forever.

This RFC extends the model to the whole pipeline and states
INV-CONVERGE as a checked invariant — safety verified exhaustively,
convergence by witness and simulation.

The implementation follows by refinement: every fairness assumption
the model makes is named and the discharge mechanism is described.

One level-triggered reconciliation loop owns all fragment movement:
distribution unifies on pull by the responsible nodes, attestation
reconciles against bytes on disk, and the staleness drain consumes
its own work-list, so progress cannot stall on a cursor. A mechanism
is in scope iff it discharges a model
assumption. Placement inputs stay frozen; the weight derivation
remains one injected seam so RFC-STORAGE-004 (performance-aware
placement) can follow without touching the reconciler. Late stages
surface per-urgency-tier time-to-conformance in the resilience pane.

## Motivation

- **The proof starts after the pipeline.** The RFC-STORAGE-001 model
  assumes every class is already on its responsible node
  (`spec/storage_policy.qnt:180-192` — "Init: conformant, fresh
  views, everyone up"). There is no unplaced state nor distribution
  modeling: the stranded-blob failure mode is unrepresentable as it
  exists outside (earlier than) the model's universe. INV-RECOVER's
  guarantee is conditioned on the chunk being inside the engine's
  loop, but whether a blob ever enters that loop is out of that
  model's scope.
- **Blobs strand at `placement_height IS NULL` — found in
  production.** If a peer restarts, the origin's transfer path can
  become stale. Distribution fails, and the blocks remain held only
  by the origin (single-copy) with no code path that would ever
  retry distribution.
- Three independent gaps compose:
  - the distribution worker logs a failure and drops the blob — no
    requeue, no backoff, no dead-letter (`engine/mod.rs:283-287`);
  - tier-1 repair structurally excludes unplaced blobs — `repair_one`
    diffs placement against a prior height that does not exist
    (`engine/mod.rs:375-379`; candidate query filters
    `placement_height IS NOT NULL`, `store.rs:777`);
  - `FAILURE_THRESHOLD_PERCENT = 10.0` is unreachable on a small
    mesh — 30 fragments across 3 nodes admits only 0/33.3/66.7/100%
    failure rates, so losing any single peer fails 100% of writes
    (`engine/policy.rs:21`, enforced `engine/mod.rs:721`).
  - Further entries into the same state, never observed but open:
    blob ids decided before the engine's OnceCell is set are silently
    dropped (`consensus/malachite/app.rs:414`); the placement batcher
    drops commits on `Rejected`, on encode failure, and after 3 flush
    attempts (`engine/mod.rs:852-874`) — fragments distributed,
    height forever NULL.
  - The operator drain (`POST /maintenance/drain-unplaced`, commit
    6692302) makes the incident recoverable by hand. It does not
    close the hole: new failures strand new blobs, and recovery
    requires a human noticing the resilience pane.
- **Local-state marks are lossy, and the loss is permanent.**
  - `mark_local`/`mark_remote_batch` are `try_send` onto a
    bounded(1024) channel; overflow drops the update with a warning
    (`substrate_host.rs:244`).
  - The trait doc claims a dropped update "self-heals via self-check
    attestation" (`hopnet-storage/src/traits.rs:209-211`) — false:
    the self-check differential reads the `stored_locally` flag,
    never the disk (`store.rs:440`).
  - A dropped mark is never attested, invisible to repair, yet not
    an orphan (its hash is in `fragment_hashes`), so the mesh
    re-encodes a byte-identical duplicate elsewhere while the bytes
    sit unused.
  - The only reconciliation against disk truth is
    `reconcile_fragment_store`, which runs solely during a regenesis
    epoch join. Relatedly, `self_verified_height` is
    blanket-restamped by every self-check without any disk read
    (`store.rs:352-357`) — and read routing ranks fragment sources
    by it (`src/db/inventory.rs:78`).
- **The migration cursor never advances.** The automatic rebalance
  rung calls `run_network_rebalancing(app_state, 1, 0)`
  (`jobs.rs:741`) against an `ORDER BY placement_height ASC LIMIT 1`
  query (`store.rs:775-780`). A `RepairOutcome::Unchanged` does not
  rewrite `placement_height`, so every 5-minute tick re-selects the
  same oldest blob: the automatic path examines one blob, mesh-wide,
  forever. Only the manual `POST /maintenance/rebalance` route
  escapes.
- **The remaining lifecycle jobs exist but never run.** Orphaned
  data-block cleanup has an apalis handler registered with no worker
  (`src/storage_host/jobs.rs:18`); orphaned fragment-file GC is
  manual, two-call, and its scan cache is process-memory — lost on
  restart (`jobs.rs:418`). RFC-007 specified schedules for both;
  none were built. This RFC absorbs those surfaces so lifecycle
  ownership lands in one place.

## The Model Extension

- **One new state pair, one new init.** The chunk gains
  `(confirmed, target)` — the model image of the two columns
  `placement_height` / `desired_placement_height`.
  - `confirmed` is the obligation epoch: the view height whose
    assignment defines who must currently hold what. NULL until the
    first confirmation — "never placed" keeps its meaning and its
    pane observable.
  - `target` is the goal: the view height whose assignment the
    reconciler always converges toward. Never NULL — stamped at blob
    insert with the insert's own decided height, moved only by
    declaration, equal to `confirmed` when quiescent.
  - Birth is a declaration: `apply_blob_insert` runs in consensus,
    so stamping `target` at insert is deterministic and free — a
    newborn blob is already in-flight toward the current view, and
    initial distribution needs no separate declaration tx.
  - The stranded state becomes unrepresentable: with
    `desired_placement_height NOT NULL` as a schema constraint,
    "no defined goal" cannot be stored, and the worker never
    case-splits on its absence — it always attempts convergence on
    `target`.
  - `initOriginHeld` models birth: every class on a single origin
    node, `confirmed = ⊥`, `target = the current view`. The
    existing conformant init remains for regression against
    RFC-STORAGE-001's properties.
- **Stuck points are regions, not histories.** The chunk's state is
  the cross-product (`copies`, `invView`, `(confirmed, target)`) —
  `copies` being the model's per-class holder sets (class → set of
  nodes), `invView` the mesh's consensus-replicated inventory belief
  of them, and the pair the control-plane record: `confirmed` the
  committed baseline (`placement_height`), `target` the declared
  goal. Every production stuck point maps to a region:
  - origin-only × unplaced — not yet processed by the distribution
    worker, or dropped after a failed distribution;
  - mixed × unplaced — partial distribution: the engine moves one
    class per tick, so the model passes through every in-between
    configuration (1 of 30 moved, 2 of 30, ...), and a fault can
    land on any of them; whatever state it lands on is the starting
    point recovery must handle;
  - conformant × unplaced — distribution finished, but the placement
    commit was dropped;
  - `invView ≠ copies`, either direction — a dropped `mark_local`
    under-claims a node's own inventory state, a dropped
    `mark_remote` over-claims it.
  - INV-CONVERGE quantifies over all reachable states, so the
    checker's obligation is exactly: no region is a fixpoint short
    of the target under calm (non-increasing pressure). The target
    is the full fixpoint defined by INV-CONVERGE below — belief
    included, because a converged-but-misbelieved state re-diverges
    through the actions that read belief.
  - The model does not track how a state was reached — entry paths
    matter for evidence, not for convergence.
  - `(confirmed, target)` gates nothing: the pair appears only in
    the invariant and in its own declare/confirm rungs. The three
    axes carry different weight — `copies` bears durability
    (INV-DURABLE reads it alone), `invView` bears action-safety
    (eviction deletes on its say-so), the pair bears neither. The
    implementation inverted this, making the least critical axis
    the gatekeeper of recovery; `placement_height` is record, never
    filter.
- **The protection predicate — the single eviction guard.** A copy
  of class f held by node n is protected iff any of:
  - `confirmed` is NULL — the blob has never been confirmed, no
    obligation has ever lapsed, and every existing copy is
    load-bearing regardless of who holds it (this is what makes the
    origin's copies safe between birth and first confirmation);
  - `confirmed` is set and n is responsible for f under
    `assignment(view@confirmed)` — the standing obligation;
  - n is responsible for f under `assignment(view@target)` — the
    in-flight destination; without this clause a pressure spike
    mid-handoff evicts the newly pulled copy and the handoff
    livelocks;
  - the copy is pinned.
  Everything else is surplus: evictable, lazily, under the normal
  pressure-driven watermark rules, with the attested-other-holder
  belief check retained as a second belt. This predicate is one pure
  function in hopnet-storage, consumed by both the reconciler and
  the evictor, and it is the model's `envEvict` guard verbatim —
  eviction timing stays adversarial in the model, so scheduling is
  free; computing the guard differently anywhere is the only way to
  reintroduce the loss, and single-sourcing forbids it.
- **INV-EVICT-SAFE — the system never self-harms.** No eviction
  step ever removes the last extant copy of a class. Encoded with a
  one-bit history variable (set when an `envEvict` empties a
  class's extant copy set; invariant: never set), and checked
  unconditionally — under any fault interleaving, with every budget
  spent — because eviction is the system's own action: the
  environment may destroy copies, the system never may. The guard
  is also checked indirectly (a too-weak predicate eventually fails
  INV-DURABLE), but the direct form isolates blame: a violation is
  definitionally the protection predicate's fault, no triage.
- **A new environment action: `envDropMark`.** The queue can lie to
  the database, the database lies to the mesh, and the mesh's belief
  is what eviction trusts — so the model gains the power to lie in
  exactly that way, and the proofs must still hold. `envDropMark`
  desynchronizes `invView` from `copies` in either direction under a
  drop budget (MAX_DROPS, the analogue of MAX_CORRUPT): the model
  image of a full local-state channel dropping a `mark_local` (a
  node holds bytes the mesh never learns about) or a `mark_remote`
  (the mesh keeps believing in a copy it should not count). This
  models an honest node whose bookkeeping went wrong — not a node
  that lies; the lying node is outside the perimeter (see below).
  Consequences:
  - **Convergence forces disk-truth attestation.** The model's
    inventory-sync rung restores belief from truth (`invView' =
    copies`). The implementation as built restores belief from the
    flag — the corrupted thing itself — so a single drop diverges
    belief forever, and the checker will exhibit exactly that
    counterexample. The only honest discharge is a mechanism that
    reads the disk: scan, compare, repair the flag and the
    attestation.
  - **Safety must be re-proven under drops.** The existing
    INV-DURABLE proof tolerates belief that is stale — lagging
    truth — but never false — contradicting it. Over-claim aims new
    adversary power at the one action that deletes bytes. The
    widened protection predicate is expected to carry the proof
    (obligations come from consensus columns; belief is demoted to
    a second belt), but expected is not proved: the check runs with
    the action enabled.
  - **The property schedules the mechanism.** Belief is dishonest
    for at most one disk-truth cycle, so the sweep's cadence enters
    the wall-clock convergence bound and is chosen from it, not
    from taste. Drops are also locally observable, and the
    implementation removes them outright: the marks become awaited
    sends (backpressure — see Mechanisms), leaving the periodic
    sweep as the backstop for divergences nobody observed: a crash
    between store and mark, manual file surgery, bugs. The
    adversary does not care why belief diverged, so the proof
    covers all of them.
  - **Recency becomes a first-class input.** `self_verified_height`
    finally means "the last height these bytes were seen on disk":
    confirmation validation and the eviction belt may demand
    attestations verified within N heights, with N sized against
    the sweep cadence the bound already fixed.
- **INV-CONVERGE.** At every moment, at least one of these holds:
  the blob was deleted, or the engine has not yet had CALM_BOUND
  uninterrupted turns, or the blob is converged. Read backwards:
  given enough uninterrupted turns, the engine finishes the job.
  (Phrased as an always-true formula because that is what the
  checker can verify — the same encoding INV-RECOVER already uses.)
  "Converged" is four plain conditions:
  - the bytes are in the right places — every class sits on the
    node the goal assignment names;
  - the paperwork is done — `confirmed = target`, no handoff in
    flight;
  - the goal is not stale — the assignment at `target` equals the
    assignment under today's view, so victory is never declared
    against an outdated map. This is also the propose hook's
    nothing-owed check: "model converged" and "consensus quiet"
    are the same test;
  - belief matches reality — `invView = copies`.
  The tick gains two rungs, which are just the two txs under model
  names: declare (move the goal when the world changed) and confirm
  (stamp the paperwork once the bytes arrived). Pulling bytes is
  the existing rung, unchanged — birth is a pull toward a goal,
  same as migration.
- **CALM_BOUND is a promise about the engine, not the weather.**
  - It does not claim the network gives us N quiet ticks; it claims
    N quiet ticks suffice to finish one chunk. If quiet never
    comes, convergence was never promised — but safety
    (INV-DURABLE, INV-EVICT-SAFE) holds the whole time regardless.
  - The number is counted, not tuned: one tick to sync the view,
    one to declare, N_FRAGS to pull each class, sync ticks for
    inventory, one to confirm. One counting subtlety: confirm
    cannot look at reality directly — the real ConfirmPlacement is
    validated against attested inventory rows, so the model's
    confirm rung reads belief (`invView`) too. Belief lags reality
    by one sync rung, so at least one sync tick must land between
    the last pull and the confirm. Whether the engine syncs after
    every pull (two ticks per class) or once after all pulls
    (N_FRAGS + 1) is decided by rung priority in the tick's ladder;
    the count, and thus CALM_BOUND, follows from that choice.
  - The checker keeps the count honest in both directions: the
    invariant must pass at CALM_BOUND (it is enough) and a witness
    must fail at CALM_BOUND − 1 (it is tight). A miscount breaks
    loudly, with a counterexample naming the true cost.
  - It is ticks for one chunk, not wall-clock for the mesh.
    Fleet-level time-to-optimality is backlog work over measured
    throughput — the ETA observable, a late stage. The bound's
    contribution there is soundness: linear-in-N_FRAGS per-chunk
    cost is what licenses summing bounded per-chunk costs into an
    ETA at all.
- **The checking regime, stated honestly.**
  - Safety — INV-DURABLE, INV-SPREAD, INV-EVICT-SAFE: exhaustive
    bounded model checking (Apalache), small depth, from all three
    inits, with `envDropMark` enabled.
  - Convergence — INV-CONVERGE: scripted witness runs from every
    region the stuck-point bullet names, plus large random
    simulation on the full-scale config. Exhaustive checking cannot
    reach depth ≥ CALM_BOUND, so this is witnesses and simulation
    by necessity — the same regime INV-RECOVER already lives under.
    The RFC claims exhaustively-checked safety and witnessed,
    simulated convergence; nothing stronger.
- **Outside the perimeter.** The proofs are strong because their
  boundaries are explicit; these are the boundaries.
  - **A node that lies about its own holdings.** The fault model is
    crash/corrupt/drop — honest nodes with wrong state, never
    dishonest nodes. The damaging lie is a responsible node faking
    its own obligation: the repair scan sees the class as live and
    never re-encodes, confirmation counts the phantom attestation,
    old holders lapse and evict — phantom durability, green
    dashboards, dead chunk. No local mechanism closes this; the
    closure is external proof-of-possession (passive retrieval
    evidence, active nonce-keyed spot-checks), deferred with the
    retrieval-reputation half of the attestation RFC. Deferrable
    because the deployment profile is a personal mesh of one's own
    and family's devices; a lying storage node is a secondary
    threat there. Two hooks land now so the successor needs no
    schema churn: verification provenance recorded beside
    `self_verified_height` (self-scan vs remote challenge), and
    "suspect" as an inventory-row state that triggers repair
    exactly as missing does.
  - **Cross-chunk scheduling fairness.** The model is one chunk;
    starvation between chunks (the LIMIT-1 bug) is invisible to it
    by construction. Fairness is structural instead: the staleness
    drain consumes its own work-list, and fulfillment samples
    randomly over a draining set — pinned by tests, not by the
    checker.
  - **Substrate liveness.** Tokio channels deliver, apalis crons
    fire, the serial worker eventually runs: trusted, not modeled.
    The model's tick abstracts all of it; a wedged runtime stalls
    convergence but cannot break safety, since deletion is guarded
    by the predicate, not by timing.
  - **Consensus itself.** Decided blocks apply identically
    everywhere (that is Malachite's BFT job), so consensus columns
    and inventory rows are agreed state. The model consumes this as
    an axiom; nothing here re-proves it.

## The Handoff Protocol

How the pair of columns and the pair of txs move a blob from any
state to converged, with obligations never dropped in between.

- **Two columns.**
  - `placement_height` (nullable) is `confirmed`: the obligation
    epoch. NULL until the first confirmation ever — "never placed"
    keeps its meaning.
  - `desired_placement_height` (NOT NULL, stamped at insert with
    the insert's decided height) is `target`: the goal the
    reconciler always converges toward.
  - Quiescent means equal; in-flight means different. Both are one
    indexed comparison.
  - The in-flight set with its ages — decided height minus
    `desired` — is the pane's new first-class observable, and it
    degrades gracefully into today's unplaced-age chart for
    newborns.
- **DeclarePlacementTarget** — `{ targets: [(blob_id, from, to)] }`,
  batched, proposer-originated (permissionless floor).
  - Apply-side validation, identical on every node (there is no
    per-tx voting; the block certificate is the vote):
    - the blob exists and is undeleted;
    - `from == desired_placement_height` — compare-and-swap, so a
      stale declaration racing a newer one rejects cleanly;
    - `to` is sane: a decided height, greater than `from`, within a
      recency window of the tip;
    - legitimate need: a storage-view transition exists in
      `(from, to]` — one height comparison against the transition
      record; no per-blob assignment computation anywhere in
      declare's apply, which is row writes only.
  - Effect: `desired_placement_height = to`. Nobody's hold
    obligation changes; the new responsibles under `view@to` gain a
    pull duty, derived from the column itself.
  - The columns record need, not activity. There is deliberately no
    cap on the in-flight set: capping declarations would make the
    control plane's liveness depend on data-plane capacity, and a
    wedged worker would silence the mechanism that records what is
    owed. Unbounded need is safe — transfer concurrency is bounded
    where it executes (pull budgets, the serial worker), and a stalled
    drain shows as the in-flight ages growing, never as refused
    txs. The model has no cap either; the implementation stays a
    faithful refinement. Declare batches are chunked for block-size
    hygiene only — pagination, not a cap.
  - Supersede is just another declare: if the view moves again
    mid-flight, a later declaration moves `desired` forward under
    the same validation. No timeouts exist anywhere — a stalled
    handoff is simply still in flight, visible by its age, and the
    next declaration or completed pull advances it.
- **ConfirmPlacement** — `{ confirmations: [(blob_id, height)] }`,
  batched, proposable by anyone.
  - The proofs are not collected by the proposer — they are already
    in consensus: attestation rows (`fragment_inventory`) placed
    there by the existing self-check machinery, which this RFC
    leaves as the only evidence path. The proposer merely points at
    blobs whose evidence is now sufficient.
  - Apply-side validation:
    - `height == desired_placement_height` — confirming exactly the
      declared goal, nothing else;
    - for every class of `assignment(view@height)`: the responsible
      node under that view has an attested inventory row for it —
      and, once disk-truth attestation lands, one verified within
      the recency bound;
    - a confirmation failing validation is a rejected no-op; the
      need stays recorded and the next pass re-proposes.
  - Effect: `placement_height = height`. Old holders' obligations
    lapse — their copies become surplus, protected now only by the
    attested-successor belt, reclaimed lazily under pressure.
  - This is the sole writer of `placement_height` after
    `apply_blob_insert`'s NULL. It retires the blind-stamping
    `update_placement_heights` (the batcher emits ConfirmPlacement
    and inherits validation) and closes repair's side door — a
    migration's re-stamp is a declare/confirm pair like any other.
  - No election protects it because none is needed: validation
    carries all the safety, so racing proposers are harmless. Any
    node's fulfillment pass proposes confirmations it discovers; an
    optional fast path lets the new class-0 responsible propose the
    moment its attestation lands.
- **The staleness pass: a proposer-driven, self-consuming drain.**
  - Selection is one indexed predicate: `desired_placement_height <
    T`, T the latest storage-view transition. Every selected blob is
    declared to the current height — no per-blob divergence check at
    declare time. Re-goaling is not movement: capped-HRW minimal
    movement means most declares are clean re-goals whose holders
    are unchanged; whether a blob actually moves is discovered
    later, by fulfillment, distributed.
  - The work-list consumes itself: a declared blob leaves the set,
    so pagination (sized for max block size) needs no cursor and no
    progress txs — the schema records processedness. The backlog
    count is the pane observable; "missed transition" detection is
    simply the set being nonempty.
  - Triggers: the primary mechanism is a propose hook — whoever
    assembles a block, at any height and round, runs the indexed
    `desired < T` check and appends owed declare pages to its own
    proposal. Origination and inclusion collapse into one act by
    the one node empowered to include; a transition block is
    naturally followed by the next proposer's hook draining the
    first pages. The role rotates wherever consensus is active,
    and recurring background traffic (metrics, uploads) reactivates
    consensus within minutes, carrying the role past dead or
    censoring proposers. Last rung, for self-containment: any node
    that has not observed the `desired < T` check occur within a
    generous wall-clock grace (~15 minutes — longer than the
    metrics heartbeat, so it never fires while the propose hook is
    healthy) submits a page directly, so convergence never rests on
    a sibling subsystem's heartbeat. Per-blob CAS dedups every
    race.
- **The fulfillment pass: universal, random-sampled, and the sole
  confirmation path.**
  - Fused with redistribution: each node's tick pulls the classes
    it owes, then samples N random in-flight blobs and recomputes
    their confirm-readiness from ground state — assignment at
    `view@desired`, attested inventory rows, recency — batching
    ConfirmPlacement for those satisfied. Recurrence over a
    draining set is what makes the sample comprehensive; the
    apply-time-derived ready-index is a latency optimization with
    no proof obligations attached.
  - After a transition the in-flight set balloons with clean
    re-goals awaiting their rubber stamp. Harmless: identical
    holders under both epochs means zero extra protected copies and
    zero eviction impact. Drain rate is M × N per tick — a 3-node
    mesh at N = 1000 per 5-minute tick retires ~36k/hour, a million
    stale blobs in about a day; N adapts upward when sampled
    ready-density is high.
  - Confirm validation recomputes assignment at apply on every node
    — M-redundant by nature, the intrinsic price of validated
    placement, bounded per block by confirmation batch size and
    spread over the drain rather than spiking at declare time.
- **Clocks, workload split, and the transition record.**
  - Every schedule is wall-clock, never height-driven: chain
    quiescence cannot pause a worker, and a worker submitting is
    precisely what wakes the chain — the dependency points one way
    only. Timers gate who proposes, never what is valid, so they
    need no agreement.
  - The split: staleness rides block assembly (the propose hook)
    plus the grace rung, costing the tick nothing; fulfillment owns
    the tick's budget on every node. If the in-flight set balloons
    pathologically, the proposer may pace its declare pages:
    scheduling backpressure, never refusal of recorded need.
  - The transition record — heights where the derived storage view
    actually changed, each with a memoized view snapshot — is
    node-local, derived incrementally at apply of view-input txs,
    prunable below `min(desired)`, rebuildable by replay. Only
    confirm validation reads historical snapshots; the sweep needs
    just the latest. Transitions are designed-rare: the decay gate
    and quantized tiers exist so metric noise does not move the
    view — days-to-weeks cadence in a steady mesh, and sustained
    churn is visible as a drain that never completes.
  - Silence is legitimate iff no blob has `desired < T` and the
    in-flight set is empty — two indexed predicates, checkable by
    any node, and exactly the model's converged fixpoint projected
    onto the schema.

## The Assumption-Discharge Table

The model's guarantees transfer to the implementation only if every
assumption the model makes is made true by a named mechanism. This
table is that binding. A mechanism is in scope for this RFC iff it
appears in the right column; anything not appearing here is an
optimization and carries no proof obligation.

```
| The model assumes              | Made true by                               |
|--------------------------------|--------------------------------------------|
| Every chunk is inside the loop | desired_placement_height NOT NULL, stamped |
| from birth -- no unreachable   | at insert; no rung filters on              |
| regions                        | placement_height (record, never filter);   |
|                                | the staleness predicate desired < T covers |
|                                | quiescent and in-flight blobs alike        |
|--------------------------------|--------------------------------------------|
| The declare rung fires when    | The propose hook: every block assembler    |
| the goal is stale              | runs the desired < T check and appends     |
|                                | owed pages; the ~15 min any-node grace     |
|                                | rung backstops a traffic-less chain        |
|--------------------------------|--------------------------------------------|
| The pull rung fires for owed   | Pull duties derived from the desired       |
| classes                        | column at declare-apply; the serial worker |
|                                | executes them; the fulfillment pass re-    |
|                                | derives them from ground state each cycle  |
|--------------------------------|--------------------------------------------|
| The confirm rung fires once    | The fulfillment pass: every node, every    |
| evidence is complete, and its  | tick, recomputes readiness for N random    |
| guard reads belief             | in-flight blobs from fragment_inventory    |
|                                | directly; ConfirmPlacement apply re-       |
|                                | validates the same predicate on every node |
|--------------------------------|--------------------------------------------|
| Belief is restored from truth  | Disk-truth attestation: a periodic disk    |
| (invView' = copies)            | sweep whose cadence enters the convergence |
|                                | bound, plus awaited-send backpressure so   |
|                                | observed drops cannot occur;               |
|                                | self_verified_height stamped only by       |
|                                | actual disk reads, with provenance         |
|--------------------------------|--------------------------------------------|
| Eviction fires only under the  | One pure function in hopnet-storage -- the |
| protection predicate           | envEvict guard verbatim -- consumed by     |
|                                | both the evictor and the reconciler; INV-  |
|                                | EVICT-SAFE pins it under full adversarial  |
|                                | interleaving                               |
|--------------------------------|--------------------------------------------|
| Obligations derive from agreed | The protection predicate reads             |
| state, never belief            | (placement_height, desired) columns;       |
|                                | obligations lapse only at ConfirmPlacement |
|                                | apply, which cannot exist without attested |
|                                | successors                                 |
|--------------------------------|--------------------------------------------|
| Assignment is a pure function  | The existing placement functions; the      |
| of (view, blob)                | literal-table parity guard (table_guard)   |
|                                | keeps model and backend in agreement       |
|--------------------------------|--------------------------------------------|
| Rungs are serviced fairly      | The self-consuming drain (processing       |
| across chunks (outside the     | removes from the set) and random sampling  |
| one-chunk model)               | over a draining set; pinned by tests, not  |
|                                | the checker                                |
|--------------------------------|--------------------------------------------|
| Consensus applies decided txs  | Malachite BFT -- trusted axiom (perimeter) |
| identically everywhere         |                                            |
|--------------------------------|--------------------------------------------|
| Faults stay within budget;     | Environment assumptions, not mechanisms -- |
| calm windows occur             | deliberately unproven; their violation is  |
|                                | visible (fault budget: repair shortfall in |
|                                | the pane; calm: a drain that never         |
|                                | completes)                                 |
|--------------------------------|--------------------------------------------|
```

## Mechanisms

- **Disk-truth attestation** — discharges `invView' = copies`;
  repairs the record, never the data.
  - The record path gets backpressure, not loss: `mark_local` /
    `mark_remote` become awaited sends on the bounded local-state
    channel, so a full queue slows the writer instead of lying to
    it. Pull unification is what makes this safe — the receiver is
    the initiator, so awaiting its own record write paces only its
    own pulls, with no cross-node stall (under push, the same
    backpressure would have blocked the sender's RPC). Accepting
    bytes faster than the record can absorb them was never
    throughput; on a slow disk the node now converges visibly
    slower rather than diverging invisibly. The remaining
    divergence sources — the crash window between store and mark,
    manual surgery, bugs — are exactly what the existence sweep
    repairs, and the model keeps `envDropMark` so the proof never
    trusts this code being right.
  - Unobserved divergence: a periodic existence sweep walks the
    fragment store, diffs it against `stored_locally`, and repairs
    the flag in both directions — bytes present but unflagged are
    re-attested; bytes flagged but gone are un-attested. The next
    self-check carries corrections into `fragment_inventory`, which
    is the only truth channel: there is no separate rebuild list.
  - Cadence is derived, not chosen: belief is dishonest for at most
    one sweep cycle, and that cycle appears inside the wall-clock
    convergence bound. An existence sweep is a readdir walk — daily
    is cheap; the weekly scrub slices keep content verification and
    share the walk when they coincide.
  - `self_verified_height` becomes honest: stamped only by
    disk-verified attestations — the blanket restamp in
    `apply_self_check` dies. A provenance column records how a row
    was verified (self-scan now; remote challenge reserved for the
    proof-of-possession successor). "Suspect" becomes an
    inventory-row state that triggers repair exactly as missing
    does.
- **Recovery is the fetch fallback** — repair-of-data belongs to
  the one worker, not to a subsystem.
  - The convergence worker's primitive is fetch-with-recovery: owed
    class f is requested from its attested holders; if none serves
    within the bound, it is RS-rebuilt from any K live classes.
    Every operation inherits recovery for free — first
    distribution, rebalance handoffs, and steady-state loss, which
    needs no view change: the responsible node's standing
    obligation is level-triggered, so its own tick discovers the
    gap the sweep exposed and falls through to rebuild.
  - Repair is thereby not a separate concern but an unfulfilled
    obligation; the standalone missing-class scan retires into the
    obligation check. The model said so first: pull and re-encode
    are adjacent rungs with the same actor and guard shape.
  - The urgency ladder survives as queue order: duties sort by
    liveness deficit first (below-watermark chunks rebuild before
    routine handoffs), then age — same serial worker, same bounded
    memory. Below W with a down responsible, the lowest-live-class
    responsible rebuilds as a deputy: a surplus, belief-protected
    copy, matching today's urgent semantics so degraded chunks do
    not wait out the decay gate.
- **What this RFC retires.** The push pipeline dies whole:
  - the distribution worker pool and `distribute_one`'s origin-push
    path, including `FAILURE_THRESHOLD_PERCENT` (a threshold with
    no reachable value on a small mesh) and the per-blob send
    permits — transfer pacing moves to the pull side;
  - the placement batcher's blind `update_placement_heights` tx —
    ConfirmPlacement with apply validation replaces it;
  - `repair_one`'s baseline diff and the `IS NOT NULL` rebalance
    query — migration is now declare/confirm like everything else;
  - the standalone missing-class repair scan — recovery folded into
    the obligation check;
  - `notify_blob_committed` survives demoted: a latency hint that
    wakes the worker early, carrying no correctness weight; the
    engine-not-yet-spawned drop becomes harmless by construction;
  - the operator drain route (`/maintenance/drain-unplaced`)
    survives as a manual re-kick during migration, then retires
    once the reconciler owns the full lifecycle — its selection
    query is the reconciler's own.
- **Observability: the pane shows the worker's own queries.**
  - Every observable is literally a work-list predicate — the same
    indexed queries the workers run, so the pane and the machinery
    cannot drift apart: unplaced-by-age keeps its chart
    (`placement_height IS NULL`, now drainable); the in-flight set
    with ages (`desired ≠ placement_height`, age = decided height
    minus `desired`) is the new first-class series, where a plateau
    means stalled handoffs; the staleness backlog (`desired < T`)
    counts declarations still owed.
  - Convergence stops being inferred from silence: "converged" is a
    displayed, checked predicate — no blob below T, nothing in
    flight — so a quiet mesh is visibly quiet-because-done rather
    than quiet-because-nobody-looked.
  - Verification freshness becomes a series once
    `self_verified_height` is honest: the distribution of "last
    seen on disk" ages, with the suspect count beside it.
  - Late stage: per-urgency-tier time-to-conformance — backlog work
    over measured transfer throughput, sound because the per-chunk
    cost is proved linear (CALM_BOUND). The transfer-timing
    histograms that power it land early, in the same shape as the
    existing commit-latency instrumentation.
- **The absorbed lifecycle jobs fold into existing machinery.**
  - Orphaned fragment files stop being a mechanism: a file with no
    `fragment_hashes` row is the third case of the existence
    sweep's diff, deleted in the same walk after the grace period.
    The two-call scan/delete API and its process-memory scan cache
    die; the manual route becomes a report of the sweep's last
    findings.
  - Orphaned data-block cleanup stays a slow cron — deletion
    policy, not convergence — but actually registered on a
    schedule, keeping the manual route and the takeout gate. The
    decorative availability-class branch is deleted, not
    implemented: RFC-007's redundant-copy cleanup is subsumed by
    watermark eviction under the protection predicate.
  - Departed-node inventory rows are pruned incrementally at
    ConfirmPlacement apply — the one moment the blob is freshly
    proven healthy without them — plus a one-time backfill at
    migration. Departed means removed from mesh membership (the
    model's GONE), never mere storage-view decay, whose rows must
    persist as flap-back insurance. Because pruning happens only at
    confirm, unrecoverable blobs keep their ghost rows — the
    evidence the resilience pane's classification depends on. Rows
    on live non-members stay: surplus copies are read sources and
    eviction guards.
- **The cutover is also the recovery event.**
  - The migration backfills `desired_placement_height =
    COALESCE(placement_height, current_height)` (riding RFC-020's
    schema evolution, together with the provenance column and the
    suspect state). Placed blobs emerge quiescent; every
    historically stranded blob emerges already in-flight toward
    the current view — the entire stranded class is enrolled into
    the new lifecycle by the schema migration itself, with no
    operator action.
  - The first propose-hook check then declares every blob placed
    against a pre-transition view — the population the starved
    migration rung never reached. One mesh-wide catch-up drain
    follows, mostly rubber-stamps by minimal movement, visible in
    the pane at the documented drain rate.
  - No pre-cutover view is ever reconstructed: quiescent backfills
    never need confirmation, and any blob entering flight gets a
    post-cutover `desired` first. The transition record bootstraps
    empty with the current view; its prune floor rises to the
    cutover height as the drain completes.

## Stages

- **S0 — Model extension.** The `(confirmed, target)` pair,
  `initOriginHeld`, `envDropMark`, the widened `envEvict` guard,
  declare/confirm rungs, INV-CONVERGE, INV-EVICT-SAFE; CALM_BOUND
  derived and witnessed tight (pass at bound, fail at bound − 1);
  safety re-verified from all three inits with drops enabled;
  `verify_table` and the spec README updated. Gate: every check in
  the regime table green. All rung-priority decisions are made
  here, before any Rust.
- **S1 — Schema and txs.** The two columns (NOT NULL, backfill),
  DeclarePlacementTarget and ConfirmPlacement handlers with full
  apply validation, the transition record memo. Rides RFC-020
  schema evolution. Gate: snapshotter parity on all touched DB
  functions; apply-side validation exercised by handler tests.
- **S2 — The protection predicate.** One pure function; evictor and
  reconciler both consume it; the unconfirmed-blob clause; a parity
  test pinning it to the model's guard, table_guard-style. Lands
  before any machinery that could delete: the safety net precedes
  the acrobatics.
- **S3 — Pull machinery.** Pull duties derived at declare-apply;
  fetch-with-recovery; awaited-send marks (backpressure); the
  serial worker's deficit-first duty ladder. The push pipeline,
  threshold, and blind placement batcher retire in the same stage —
  no period with two distribution paths.
- **S4 — The two passes.** Propose hook + grace rung (staleness);
  fulfillment sampling fused with the tick (confirm discovery).
  The missing-class scan and the LIMIT-1 rebalance rung retire.
  Gate: orchestrator suite — distribution, departure re-encode,
  rebalance — green on the new machinery only.
- **S5 — Disk-truth attestation.** Existence sweep both directions
  plus orphan-file fold-in; honest `self_verified_height` with
  provenance; suspect state; prompt attestation on pull
  completion.
- **S6 — Lifecycle closure.** Data-block cleanup registered on
  schedule; availability-class branch deleted; departed-node row
  pruning at confirm-apply plus one-time backfill.
- **S7 — Observability.** The pane series (in-flight ages,
  staleness backlog, converged predicate, verification freshness);
  transfer-timing histograms; per-tier ETA last, on top of
  measured throughput.
- **Cutover.** One release: migration backfill enrolls the stranded
  class, the first propose hook starts the catch-up drain.
  Validation on the live mesh: watch the drain complete, then the
  converged predicate hold. The 518-block incident class is the
  acceptance test.

## Out of scope

- **Performance-aware placement** — RFC-STORAGE-004. The weight
  derivation stays one injected seam, deliberately untouched here;
  reactive placement needs its own two-timescale model work
  (no-oscillation under a moving target) and arrives only after
  this RFC's reconciler provably converges.
- **External proof-of-possession and retrieval reputation** — the
  successor to the attestation RFC's other half. The phantom-
  durability scenario in the perimeter bullet is its problem
  statement; the provenance column and suspect state are its
  landing hooks. Deferred with justification recorded there.
- **Byzantine storage claims** — outside the fault model entirely
  (perimeter bullet); consensus BFT is trusted, storage honesty is
  not adjudicated here.
- **Metrics table pruning and ε(μ)/repair-budget enforcement** —
  remain on RFC-STORAGE-002's deferred list, untouched: inputs to
  the view derivation, not part of the block lifecycle.
- **Site-tag failure diversity** — RFC-STORAGE-001 deferred,
  unchanged.
- **Consensus state archival** — RFC-007's remaining non-storage
  surface, where it stays.
