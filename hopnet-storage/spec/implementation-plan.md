# RFC-STORAGE-002: Durability Policy Implementation

**Status**: In progress.
**Contract**: RFC-STORAGE-001 (`spec/durability-policy.md`) — where this
plan and the policy disagree, the policy wins. Model:
`spec/storage_policy.qnt`.

## Scope

Implements the RFC-STORAGE-001 policy in the shipped engine: balanced
capped rendezvous placement, decay-tiered storage membership, derived
watermark, re-encode repair loop, watermark GC, pins. Mechanism and
ordering only; no policy decisions are made here.

## Decisions binding this implementation

- **Absence source: metrics-derived.** Storage membership and decay
  tiers are computed from the replicated metrics `available` history
  (~10-min collection cadence), identically on every node. No consensus
  deactivation transaction in this RFC; the consensus deactivation spec
  (deferred) swaps the source later.
- **No new consensus transaction types.** Re-encode and eviction
  settle through the existing `self_check_fragments` attestation;
  migration re-commit reuses `update_placement_heights`; mesh policy
  config is seeded through the existing genesis payload.
- **Placement changes ship as coordinated binary upgrades** — nodes
  disagreeing on the placement function compute different responsible
  sets silently. Nothing is deployed, so the cutover is a plain
  replacement with no compatibility machinery.
- **Module-namespaced DB surfaces.** Every table and `this_node` field
  this RFC adds carries the `hopnet_storage_` prefix, tying ownership
  to the module. Pre-existing storage tables keep their names for now
  (Deferred).

## Configuration

Two homes, sorted by determinism requirement — no environment flags.

**Mesh policy — `hopnet_storage_policy` table (consensus-replicated).**
Values placement/membership determinism depends on; must be
mesh-identical. Key/value schema, seeded at genesis (extends the
`insert_genesis` payload), code defaults when rows are absent. Runtime
updates become a consensus settings transaction later (deferred); the
key/value shape means that transaction needs no schema migration.

| key | default |
|---|---|
| `decay_tiers` | 6 h, 24 h, 72 h, 7 d |
| `burst_cap` (B_max) | 5 |
| `reserve_slack` (σ) | 1 |
| `climb_back` (ε) | 0 |

Orchestrator tests configure tiny tiers through genesis at mesh
creation — the test path is the production path.

**Node settings — `this_node` singleton (local-only), new
`hopnet_storage_*` fields.** Per-node values touching nobody's
determinism, surfaced in node settings UI later:

| field | default |
|---|---|
| `hopnet_storage_gc_high_pct` / `_gc_low_pct` | 90 / 80 |
| `hopnet_storage_reencode_enabled` | true |
| `hopnet_storage_repair_budget_pct` | 10 (of uplink; unenforced stub this RFC) |

Eviction safety never depends on watermark values — the guard
(surplus-only, unpinned, another live inventory holder or blob
deleted) is the invariant carrier; watermarks only decide when
pressure acts.

## Stages

Ordering: eviction lands only after re-encode exists (nothing
evictable may lack a regeneration path), and the tick wires only
proven pieces. S1–S2 are dark (no production call sites).

- [x] **S0 — this document.**
- [ ] **S1 — pure policy math, dark.** `placement.rs`:
  `quantized_weight` (small-int buckets), `assign_fragment_classes`
  (balanced capped rendezvous, integer scoring, must reproduce the
  `.qnt` `buildTable` literal tables as test vectors),
  `responsible_node`. New `membership.rs`: offline spans from
  availability samples, tier derivation (P95 rule, hysteresis, 72 h
  cold start), `storage_members`. `engine/policy.rs`: `watermark(v)`.
  All time arithmetic anchored to the newest sample, never wall
  clock. Gate: crate tests — spread bounds for v ∈ 3..=30, qnt table
  equality, W(v) spot values, tier archetypes, hysteresis anti-flap.
- [ ] **S2 — config + membership view, dark.**
  `hopnet_storage_policy` table + genesis seeding + code defaults;
  `this_node` `hopnet_storage_*` fields; availability-history query
  (10-min buckets, MAX over reporters, window anchored to the newest
  row — never `datetime('now')`); `StateReader::storage_view()` seam
  + host implementation; height-anchored weight derivation (fixes
  wall-clock nondeterminism in metrics scoring — placement
  prerequisite); `GET /storage/view` debug route. Gate:
  seeded-metrics unit tests (tier archetypes from realistic
  histories; identical derivation from identical rows) + orchestrator
  `tier-membership` (observe-only, tiny genesis tiers).
- [ ] **S3 — placement cutover.** Balanced replaces modulo:
  distribution sends each class to its single responsible node
  (backup-triple deleted; failed sends leave origin-held surplus for
  the migration rung); `repair_one` compares per-class targets
  instead of selected-node lists (the list compare is blind to the
  swap on small meshes — all validators selected both sides);
  placement consumes `storage_view` members + quantized weights.
  Gate: orchestrator fragment-distribution (updated for
  single-target), file-upload-consistency, multi-size-file-
  consistency, restart-persistence + divergence; INV-SPREAD property
  check (per-node responsible counts ≤ ⌈N/v⌉).
- [ ] **S4 — re-encode core.** Extract `decode_chunk_ciphertext`
  from the GET path; `engine/reencode.rs`: fetch any K live shards,
  decode, regenerate missing classes, verify against manifest hashes
  (mismatch = store nothing, loud), settle via `mark_local` +
  self-check; `repairer_for_chunk` election; below-watermark
  candidate query (lazy = holder down within tier / hopeless = no
  holder within tier). Gate: byte-identity round-trip tests (real
  `put` output, deleted shards, both original and recovery classes);
  fragment-health-check green.
- [ ] **S5 — pins + watermark eviction.** `hopnet_storage_pins`
  table (local-only registry entry); substrate `pins.rs` API; pure
  `eviction.rs` planner — surplus-only, unpinned, oldest first,
  guard = another live holder or blob deleted, stop at low watermark;
  host statvfs loop reading `this_node` watermarks. Gate: planner
  unit tests + new orchestrator `eviction-under-pressure`.
- [ ] **S6 — engine policy tick + scrub.** Host cron (~5 min
  randomized): view sync → self-check staleness trigger → one
  migration pull → re-encode (urgent queue unbounded, one lazy per
  tick) → eviction check. Serial repair worker generalized to
  urgent/lazy re-encode commands (single worker bounds memory ≈120 MB
  and bandwidth). Scrub: rolling deep verify, weekly full walk;
  corrupt fragment → delete + attest → repair regenerates. Gate: new
  orchestrator `re-encode-after-departure`; `tier-membership`
  asserts view resync; restart-persistence green.
- [ ] **S7 — docs + closeout.** system-overview sync; Deferred
  recorded.

## Risks

1. **Weight determinism** — metrics scoring windows use wall clock
   today; balanced rendezvous makes weights placement-relevant at
   every mesh size. Fixed in S2 (height-anchored derivation + coarse
   quantization); asserted cross-node in orchestrator.
2. **Rust ↔ Quint drift** — verification transfers only if the Rust
   scoring reproduces the model's tables bit-for-bit; literal-table
   test vectors gate S1.
3. **RS byte-identity across library versions** — manifest-hash
   verify fails safe (loud, stores nothing), but a silent library
   change would brick re-encode mesh-wide; pin the crate,
   golden-vector test.
4. **Departed nodes' inventory rows linger** — every live-holder
   query filters by the availability view; row pruning deferred
   (consensus-apply question).
5. **Self-check exact-count race** on eviction-heavy cycles — benign
   attestation bounce, retried next cycle; relax to per-hash CAS
   later if it thrashes.
6. **Availability aggregation is any-reporter** (MAX) — biases tiers
   long under partial partitions, matching the policy's
   asymmetric-cost bias; documented as tunable.
7. **Single-responsible distribution** — a down responsible node
   leaves its classes origin-held until the migration rung pulls;
   window is tick-bounded, watermark urgency covers the pathology.

## Deferred

- Consensus deactivation spec (absence-source swap).
- Consensus settings transaction (runtime `hopnet_storage_policy`
  updates; UI).
- Module schema registration API — manifest schema hook
  (consensus/local table registry, excluded columns, genesis config
  slots) so modules stop editing host-core lists; RFC-016 follow-up.
- `hopnet_storage_` prefix retrofit for pre-existing storage tables.
- macOS purgeable-aware honest capacity metric.
- Metrics table pruning (unbounded growth today).
- Inventory-row pruning for departed nodes.
- Site tags / correlated-failure diversity.
- Pre-GC surplus handoff optimization.
- ε(μ) derivation + repair-budget enforcement (stubs: ε=0, budget
  field unenforced).
