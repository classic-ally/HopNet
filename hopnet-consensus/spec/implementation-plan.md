# RFC-CONSENSUS-002: Validator Membership Implementation

**Status**: Implemented (2026-07-17, S0–S6). All orchestrator gates green.
**Contract**: RFC-CONSENSUS-001 (`spec/validator-membership.md`) —
where this plan and the policy disagree, the policy wins. Normative
model: `spec/validator_membership.qnt`.

## Scope

Implements the RFC-CONSENSUS-001 policy in the shipped node:
voluntary leave, evidence-driven vote-out, gated readmission, the
candidate pool, and the AUTO quorum profile with per-height
thresholds. Mechanism and ordering only; no policy decisions are
made here. The existing activation transaction is respecced in
place; the validators table and its operations descend into
`hopnet-consensus` (handler registration stays host-side, RFC-016
principle).

## Decisions binding this implementation

- **Seating is mesh-initiated; nodes never request seats.** The
  activation transaction keeps its type but its payload becomes a
  proposer-signed batch (`Vec<node_id>`); validation checks each
  member's eligibility against the approver's own evidence plus the
  joint posture and ceiling, atomically. The auto-reseat scheduler
  is the only seating initiator: proposers pick the brightest
  eligible candidates (longest span, ties by node_id); approvers
  verify eligibility, never ranking. The join bootstrap stops
  self-requesting at S5 (the legacy self-request path survives
  S1–S4 as transitional plumbing so leave/rejoin stays testable).
  Leave stickiness dissolves with requests: returning nodes are
  pool candidates like any other; a never-seat-me node setting is
  Deferred.
- **Evidence covers all registered nodes, all RPC scopes.**
  Reachability evidence = any authenticated exchange (fragment
  serves, sync, tx forwards, metrics — not just consensus traffic);
  pool nodes doing storage work are passively bright. The probe is
  a status ping (returns current height, timeout g) so proposers
  can check catch-up eligibility; evidence records gain
  last_known_height.
- **Departure kind is a column on validators**, CHECK-constrained,
  NULL on activation rows; lastDeparture = latest deactivation row.
- **g drives the ping RPC timeout** — one source of truth in the
  policy table; the transport default is the floor.
- **Proven-ness survives approver restarts**: proven(X) = seated
  since before my evidence began (committed effective_height vs my
  boot) ∨ own-observed in-seat span ≥ P_prove. Post-restart, only
  exposed seatings briefly defer, and only if a quorum's worth of
  approvers restarted together. (S2 verifies whether decided blocks
  carry timestamps; if so, tighten to seat-age.)
- **Two implementation invariants from the model/red-team**:
  subjective checks run at ValidationOrigin::Live only (sync replay
  accepts certified history unconditionally — a fresh node must
  never wedge on a vote-out it cannot re-derive); membership
  transitions propose in dedicated solo blocks, at most one
  transition per block (joint constraints are invisible to per-tx
  validation).

No new consensus transaction types; no environment flags (constants
live in the genesis-seeded `hopnet_consensus_policy` table); nothing
is deployed, so no compatibility machinery.

## Configuration

One home: the `hopnet_consensus_policy` key/value table —
consensus-replicated, seeded at genesis via
`HOPNET_GENESIS_CONSENSUS_POLICY` ("k=v;k=v", mesh-creation input),
code defaults when rows are absent. The values parameterize
subjective votes, so per-node disagreement degrades latency, never
safety — they are replicated anyway for band alignment, and the
genesis path is the test path (orchestrator seeds tiny values).

| key | default | role |
|---|---|---|
| `probe_base` | 30 s | B — the probe ladder base (cliff B, fast 2B, lazy 4B) |
| `grace` | 5 s | g — probe response window; drives the status-ping RPC timeout |
| `s_full` | 30 min | full admission span (exposed seatings; comfortable H) |
| `p_prove` | 30 min | in-seat survival before proven (ceiling cushion) |

Derived, never stored: T_probe(band), T_unresponsive = T_probe + g,
T_out = 2·T_probe + g, S_floor = one probe cycle. V_bft = 7 stays a
compile-time constant beside `QuorumProfile` (`config.rs`); the
profile itself stays in `consensus_meta` (`quorum_profile`:
auto | bft | majority, default auto from S6, pinning via
`HOPNET_QUORUM_PROFILE` at genesis). No `this_node` settings this
RFC — the never-seat-me opt-out is Deferred.

## Stages

Ordering: leave lands first (smallest end-to-end slice through the
deactivation machinery, always green via the legacy activation
path); evidence precedes vote-out (nothing may attest without
evidence); the full seating policy precedes the AUTO profile (seam
crossings need batch admission); AUTO lands last as the only
engine-touching stage.

- [x] **S1 — voluntary leave, end to end.** Crate descent:
  validators DDL (+ `departure_kind`, + `idx_validator_node`) into
  `hopnet-consensus/src/store.rs` install_schema; host DDL deleted
  (shadowing hazard — gate asserts the column exists);
  validators read/write free fns descend
  (`deactivate_validator`, `last_departure` join them;
  `src/db/consensus.rs` becomes thin re-exports). LeaveHandler
  (self-signature; interim guard v > 1 ∧ v−1 ≥ quorum(v−1),
  tightened in S4); POST /consensus/leave, bounded await. Solo-block
  rule: ≤ 1 membership transition per block, enforced in
  build_value + validate_inner. Gossip restricted to the valset
  (stage-5 TODO) + tip-poll keeps non-validators synced. Legacy
  self-request activation retained for rejoin. Orchestrator
  `graceful-leave`: leave → v−1 → consensus continues → rejoin →
  v restored. Gate: cargo tests, graceful-leave,
  restart-persistence, file-upload-consistency, divergence.
- [x] **S2 — config + policy math, dark.** `hopnet_consensus_policy`
  table + genesis seeding + defaults; crate `membership.rs`: ΔH /
  exposure / posture / proven-quorum ceiling / zero-tolerance
  waiver / band + window math over `QuorumProfile::quorum(v)`
  (profile source pre-S6 = committed `consensus_meta`); qnt model
  tables ported as drift-guard test vectors. Verify whether decided
  blocks carry timestamps (P_prove tightening). Gate: crate tests,
  both feature sets.
- [x] **S3 — evidence layer.** `EvidenceMap` in AppState
  (parking_lot, pure-function classification): per-node
  {last_contact, last_probe_at, probes_since_contact, bright_since,
  last_known_height}; record_contact hooks on all RPC scopes +
  cert-signer sweep at on_decided (before the storage
  early-return; shell-thread-safe); status-ping probe scheduler
  (deadline scan, band cadence from policy, ±10% jitter, timeout
  g); live/H/band estimate with the fixpoint ratchet;
  GET /consensus/evidence. Covers all registered nodes, not just
  validators. Gate: unit tests; orchestrator `evidence-observe`;
  metrics-collection regression; queue-throughput bench (hook
  overhead).
- [x] **S4 — vote-out.** VoteOutHandler: subjective dark(target)
  validate, Live-origin only; objective checks (signature,
  submitter seated, target seated) origin-independent; execute =
  deactivate_validator(VotedOut). HandlerCtx gains the evidence
  handle. Leave guard tightened to evidence-based live estimate.
  Proposer scan: longest-dark past window, one in flight.
  Readmission S_min gate on activation validate (Live-origin;
  bright_since + last_departure exemption). Startup: registered ∧
  !is_active ⇒ legacy self-request with retry/backoff (replaced in
  S5). Orchestrator `vote-out-after-kill` (forced majority until
  S6, tiny genesis probe_base). Gate: + divergent-evidence block
  test + fresh-node-syncs-vote-out-chain replay test
  (malachite_integration.rs) + regressions + divergence.
- [x] **S5 — mesh-initiated seating.** Activation payload →
  proposer-signed `Vec<node_id>`; joint posture + ceiling + waiver
  validation; auto-reseat scheduler (the only initiator: eligible =
  bright ≥ req ∧ caught up per last_known_height; brightest-first
  proposal order); legacy self-request deleted (join bootstrap +
  S4 retry loop replaced — a joining node registers, syncs, and
  waits to be noticed). Orchestrator `pool-readmission` (vote-out →
  revive → noticed → re-seated without any request) + `mesh-growth`
  (add nodes → batch-seated at gain parity). Gate: + suite +
  divergence.
- [x] **S6 — AUTO quorum profile.** config.rs Auto/V_BFT/
  thresholds_for/quorum; HostCore feed-loop StartHeight
  interception + driver replacement; HandlerCtx profile;
  Effect::Verify*Certificate thresholds derived from the effect's
  valset; defaults flip to auto; sim.rs valset schedule + four sim
  tests (profile selection, seam crossing with darks,
  sync-across-seam, WAL replay post-seam); orchestrator kill tests
  drop forced majority; orchestrator `auto-seam` (grow across 7,
  thresholds flip). Gate: crate + sims + full orchestrator suite.
- [ ] **S7 — soak + closeout.** Docs sync (system-overview, spec
  status → implemented), memory, Deferred recorded.

## Deviations from plan (recorded at stage exits)

- **S6**: per-height thresholds via replacing malachite's Driver at the
  drive_once StartHeight choke point (no fork — State.params/driver are
  pub). The load-bearing sync-across-seam mechanism is deriving cert
  thresholds from the effect's OWN valset (Verify*Certificate reads
  state.params, not the driver's copy). Default flipped to Auto
  everywhere. Orchestrator auto-seam observes formation crossing the
  seam + graceful shrink (the pure BFT-threshold stall is sim-only, as
  the live vote-out scan turns a v=7 triple-kill into graceful shrink).
- **S5**: pre-S6 pinned default flipped Bft → Majority — a
  default-BFT mesh below 4 nodes cannot FORM once self-request seating
  is deleted (BFT v=1 grows only by a batch of 3; a 3-node mesh has 2
  candidates). AUTO, the S6 default, is majority at v < 7 anyway.
  consensus-bft-quorum-loss redesigned: pinned bft, 4 nodes, kill 2.
- **S5**: the orchestrator seeds s_full=6;p_prove=6 for every mesh
  (the v=1 formation batch is exposed) and create_mesh waits for
  seating to converge; even-majority meshes seat N−1 with one pooled
  spare (the last lateral refused — spec-correct). A test setting its
  own HOPNET_GENESIS_CONSENSUS_POLICY drops the global seed, so
  evidence-observe re-seeds s_full explicitly.
- **S5**: proven(X) approximated as activation_height ≤ boot_height ∨
  bright_span ≥ p_prove (the span arm credits pre-seat brightness;
  tightening candidate seated_since is an S7 note). boot_height set at
  the first scheduler pass.
- **S4**: subjective checks live host-side in
  `src/consensus/membership_guards.rs`, called from validate_inner's
  Live block and build_value's preflight — NOT via a HandlerCtx
  evidence handle as this plan originally wrote. One structural choke
  point makes the fresh-node sync-replay wedge unreachable by
  construction (the guard physically cannot run at Sync); handlers
  stay objective and replayable forever. S5's batch checks join the
  same guard module.
- **S4**: the reactivation retry loop excludes voluntary leavers (an
  operator-requested leave must not self-undo); their return path
  stays POST /consensus/activate until S5. The S_min gate is scoped
  to candidates with a committed prior departure — never-seated
  candidates keep the S1 join path until S5's mesh-initiated seating
  (else mesh creation would demand 30-minute spans).
- **S4**: S1's interim leave guard quorum clause was vacuous
  (v−1 < quorum(v−1) is false at every v) — the real survivor guard
  arrived here as membership_guards::check_leave.
- **S3**: probe deadline jitter is downward-only (0.85–0.95), not
  ±10% — upward jitter pushes worst-case evidence age past
  T_unresponsive and flickers live nodes once per cycle.
- **S3**: the status Ping carries the PROBER's decided height —
  steady-state probe circularity leaves one probe direction per pair,
  and quorum-only cert signatures would leave the third validator's
  height unknown forever.

## Risks

1. **Malachite internals coupling** (AUTO): driver replacement at
   StartHeight relies on `State.params`/`State.driver` being `pub`
   and `move_to_height` preserving only ctx/address/thresholds.
   Version pinned `=0.7.0-pre`; the seam sim test is the drift
   tripwire on any upgrade.
2. **Subjective validation vs sync replay** — the Live-origin rule
   is load-bearing; a missed gate wedges fresh nodes forever. The
   replay test (fresh node syncs a vote-out-bearing chain) is the
   regression tripwire.
3. **Joint constraints** — the solo-block rule carries INV-NO-HARM
   for concurrent transitions; enforcement must sit in both
   build_value and validate_inner or a malicious/buggy proposer
   bypasses it.
4. **Interim S1 leave guard** is weaker than the spec (live ≈
   seated) until S4 — a leave while another member is dark can
   stall a 3-node mesh; accepted for the transitional window,
   documented here.
5. **Evidence hooks on hot paths** — one mutex write per RPC;
   queue-throughput bench gates S3.
6. **Positional copies of validators** break on the new column —
   audit imports.rs + the test db-copy helper in S1; genesis
   payload change audits snapshotter fixtures in S2.
7. **Orchestrator wall times** — probe_base/s_full/p_prove
   genesis-seeded tiny; the policy-table path is the test path.

## Deferred

- Never-seat-me node setting (leave opt-out of candidacy).
- Signal-handler auto-leave on SIGTERM (graceful app exit submits
  leave before shutdown; needs signal handling that doesn't exist).
- Configurable V_bft.
- Escalating probation for repeat fault-flappers; batch removals.
- Consensus deactivation as storage absence source (GUAR-HONEST-SET
  swap; RFC-STORAGE-001 Deferred).
- Emergency sub-quorum recovery (manual).
- Runtime policy updates via consensus settings transaction (shared
  with `hopnet_storage_policy`).
- Metrics/evidence unification (status-ping RTTs could enrich
  metrics; separate concern).
