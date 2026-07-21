# RFC-STORAGE-001: Durability & Placement Policy

**Status**: Specified + implemented (RFC-STORAGE-002,
`spec/implementation-plan.md`, 2026-07-15).
**Normative model**: `spec/storage_policy.qnt` — where prose and model
disagree, the model wins. Commands: `spec/README.md`.

## Scope

The substrate stores each chunk as N fragment classes, any K of which
reconstruct it. This RFC defines the policy that keeps that property
true over time: who must hold what (placement), who may delete what
(copy classes, GC), and what restores it (repair). Mechanism and
scheduling belong to the implementation RFC.

## Invariant

**INV-DURABLE** — every committed, undeleted blob keeps ≥ K of its N
classes alive on non-departed disks, at all times, under the fault
budget.

Consensus makes the control plane (manifests, placement inputs,
inventory) agreed; the data plane only converges toward it. The policy
below is the control loop that closes that gap; the model checks no
interleaving of faults, GC, eviction, and repair breaks the invariant.

## Copy classes

The substrate derives two classes for every physical copy:

- **Responsible** — placement names this node for this class. Eviction
  only via handoff. Computed from consensus state, never stored.
- **Surplus** — everything else. Evictable locally, any time — unless
  **pinned**.

Pins are substrate state, projection meaning: a projection calls
`pin(blob_id, owner)` / `unpin` (node-local table, never replicated);
the substrate stores the pin without knowing why. Eviction never
removes pinned or responsible copies — no pressure override. The
escalation ladder under extreme contention is structural: surplus
exhausted → honest capacity metric falls → placement assigns nothing
new → new stores refuse; freeing pinned space is an unpin, owned by
the projection's own (user-facing, manual) flow.

Durability counts both classes. GC is decentralized: eviction is a
local watermark loop under disk pressure (surplus oldest-first above
the high watermark, stop at the low), guarded by the inventory
claiming another holder — or the blob being deleted. A returning
node's copies re-enter inventory via self-check and count as surplus;
catch-up never commands deletion.

Eviction safety follows from the class rule: eviction never touches
responsible copies, so a stale inventory view can forfeit only surplus
margin, never the responsible floor. Checked exhaustively (Evidence).

## Membership

Placement is computed over **storage members** = registered nodes
minus those absent beyond their **decay tier**. The validator set
reacts fast (consensus liveness needs that); storage responsibility is
sticky — a node leaves the placement view only when its absence
exceeds what its own history says is routine. Absence within tier
moves zero responsibility and costs zero repair.

Deactivation itself belongs to the consensus module (future spec).
This policy relies on:

> The view removes only nodes continuously unreachable beyond their
> tier, admits only reachable caught-up nodes, and consensus liveness
> implies ≥ 2/3 of the view online.

Storage may lag membership safely: data needs 1/3 of classes online,
consensus liveness gives 2/3 of members. **BRIDGE** (proven by
complete enumeration at real parameters): under conformant placement,
any membership trajectory keeping consensus live keeps every blob
available. Requires **INV-SPREAD** — no member responsible for more
than ⌈N/|members|⌉ classes of one chunk.

### Decay tiers

The decay gate is a prediction — is this node coming back? — and the
mesh holds the predictor: each node's replicated absence history. The
gate is therefore per-node, not global, and it *inverts* the naive
setting: an always-on server going dark is anomalous (decay fast); a
nightly-sleeping laptop is routine (decay slow).

Tier values are calendar-shaped because churn is human rhythm:

| tier | device pattern | routine absence it must clear |
|---|---|---|
| 6 h | always-on (server, NAS) | reboots, updates, same-day maintenance |
| 24 h | daily driver | overnight sleep (8–16 h) |
| 72 h | weekday-only | the weekend (Fri 18:00 → Mon 09:00 ≈ 63 h) |
| 7 d | occasional / travel | trips; also the ceiling |

There is deliberately no 48 h tier — it would fire Sunday evening on
every weekend-idle machine. The 7 d ceiling bounds the dark-margin
window, the sleep-then-vanish pattern, and the point where
assuming-return stops beating one re-encode round.

Every tier is biased long because the costs are asymmetric: a false
"gone" costs one bounded re-encode round; a false "returning" costs
margin-days — free while the chunk stays above W, since the watermark
repairs urgently regardless of tier. Safety is independent of the tier
choice; tiers tune cost only.

Assignment: smallest tier above ~P95 of the node's offline-duration
history, computed from replicated records (every node derives the same
tier — placement stays a pure function). Tier moves require the new
pattern to persist (same anti-flap discipline as placement weights).
Cold start: 72 h, blending toward measured as history accumulates.

The false-departure rate (absence outlives tier, device returns) is a
policy SLO: tiers must keep it well under monthly per device, since a
false departure pays movement in both directions (exit repair +
re-entry migration).

### Membership timescales

| set | timescale | driver |
|---|---|---|
| physically dark | instant | reality |
| validator set | minutes (vote-out) | consensus liveness |
| storage members | hours–days (decay tiers) | repair cost |

Chained, never equal: dark → voted out fast (the deactivation record
starts the decay clock) → storage membership drops only at tier
expiry. Placement keys off storage members; the watermark keys off
live copies (inventory) — so validator churn moves no placement, and
neither view's lag can fool urgency.

Gradual failure never stalls consensus: vote-out prunes the set and
quorum rebases, one loss at a time, indefinitely. The only stall is a
burst — more than the quorum profile's tolerance of the *current* set
lost within one vote-out window. That burst is what the watermark is
sized against.

## Placement

Fragment class `f` of **every** chunk of a blob goes to the same node
— file-correlated placement: file availability equals single-chunk
availability (the optimum), and repair accounting and read routing
collapse to per-file. Placement key is `(blob_id, f)`; no plaintext
input.

The function is **balanced capped rendezvous**: classes in index
order, each to the member with the best score
`hash(blob_id, f, node) / weight(node)` **among the currently
least-loaded members**. Loads at every view are the tightest integer
split — max ≤ ⌈N/|members|⌉, min ≥ ⌊N/|members|⌋, max − min ≤ 1 — so
every member carries responsibility and adversarial burst loss is
two-sided-bounded (closed form; see Watermark). Weights derive from
consensus-state metrics, quantized so placement shifts only on real
change; with the cap binding they choose *which* classes land where,
not load share.

Alternatives, quantified at N=30 / 10 nodes, average over every
possible single departure:

| variant | loads | survivors renumbered |
|---|---|---|
| select+modulo | even | 23 of 27 |
| plain weighted HRW | unbounded skew — INV-SPREAD violated | ~0 |
| greedy capped HRW | max capped, min unbounded (zero-load member witnessed) | 6.0 |
| **balanced capped rendezvous** | max − min ≤ 1 | 12.2 |

Every renumbered survivor induces repair traffic; balanced pays ~17%
more of it per departure than greedy-capped — all cheap pull-class
migration on events whose rate the tier policy pins — in exchange for
the tight spread that makes the watermark derivable and leaves no
stranded capacity.

## Repair

With one copy per class, pull-repair has no source for any fault, except
if another node happens to have a copy from pre-GC. Pull only reliable for
planned migrations. **Most fault repair is re-encode**, and it is:

- **Keyless** — fragments are encrypted before RS, so recovery shards
  are parity of ciphertext: decode/re-encode runs in the ciphertext
  domain, regenerated shards are byte-identical and verify against
  existing manifest hashes. No key custody, no plaintext on the
  repairer, no content tx — only the inventory update.
- **Flat per chunk** — any K surviving fragments regenerate all
  missing classes of that chunk in one decode. Amplification =
  K / missing-per-chunk: ~1× at 3 nodes, K× worst case at N nodes.
- **Distributed** — repairer(chunk) = responsible node of the chunk's
  lowest missing class; seeded placement shards the role uniformly
  across survivors, no coordinator, collisions harmless (identical
  bytes, hash dedup). Aggregate repair rate scales with mesh size.

Urgency is two-tier: a dead class whose copies may return (holder
down within its decay tier) waits; live classes of a chunk < W
re-encodes immediately. W bounds how much margin laziness may spend.

The engine loop per tick: sync decayed membership view → self-check
inventory → pull one migration → re-encode one hopeless-or-urgent
class. Read routing (which K live copies) is local and latency-ranked;
surplus copies improve it for free.

### Watermark

W is **derived from the current member view**, not a constant:

```
W(v) = K + reserve(v) + ε(μ)

reserve(v) = min( ⌈B(v)·N/v⌉ + σ ,  advMax(v) )
B(v)       = min( v − quorum(profile, v), B_max )  — burst design target
advMax(v)  = min(B,f)·c + max(0, B−f)·(c−1)     — adversarial ceiling
  c = ⌈N/v⌉,  f = N − v·(c−1)                   — balanced load profile
```

`quorum(profile, v)` is the SAME function the consensus engine uses
(`hopnet_common::quorum::QuorumProfile::quorum`), keyed off the mesh's
**active** profile — majority below `V_BFT`, BFT at and above, per the
RFC-CONSENSUS-002 AUTO seam. Keying `B` off BFT unconditionally
under-provisions a majority-profile mesh at small `v` (v∈{3,5,6}),
where consensus survives a burst that would drop live fragments below
K = permanent loss; the majority arm is the conservative watermark arm
(larger fault budget → larger reserve). Basis: `v` is the storage
member count — the **member-count** variant, certified by the burst /
σ-tail lemmas at the active-profile `b` (`storage_policy.qnt`
`bBudget`). The validator-set-count variant is the recorded open
alternative.

Meaning: at maximum laziness, storage survives a burst of B(v)
simultaneously lost members. B(v) tracks the control plane's own
fault tolerance at small sizes and saturates at **B_max** — the
largest single non-site event (one power strip / breaker / switch);
bursts beyond that are site-catastrophe territory, which no watermark
can insure (site diversity, deferred, is the correct tool). The
expected-load reserve (⌈B·N/v⌉) reflects that physical bursts hit
load-*average* victims — placement hash is uncorrelated with outlet
topology — with σ covering variance; the adversarial ceiling caps it
where cap-loaded victims are guaranteed anyway.

**Assumption named**: expected-load accounting requires power domains
to be load-diverse. Deployments whose high-weight always-on core
shares one UPS/rack violate it — the burst victims there *are* the
cap-loaded members; raise σ (up to the adversarial ceiling) or wait
for site tags.

ε(μ) covers failures during the climb-back window after crossing W —
proportional to affected volume over measured mesh repair throughput,
taken at a pessimistic quantile, quantized; ~0 on healthy links. The
general principle: hysteresis generosity scales with the ratio of
margin-recovery rate to margin-burn rate (the tiers, B, σ and ε are
all instances — every tunable trades laziness against burst classes,
never against K).

Burst survival is proven by complete enumeration where the tables are
enumerable (every ⌊v/3⌋-subset of every view at both model scales,
where B_max does not bind); the B_max/expected-load regime (v > 15)
is closed-form over the balanced load profile, which the spread
lemmas pin exactly.

Below everything sits the floor **K + ⌈N/v⌉** — the durability cliff,
one node from unreconstructable. At every mesh size the operating
watermark fires first, while consensus is still live; reaching the
floor means the control plane is already down and repair can
regenerate bytes but not commit inventory — data intact, liveness
lost, requires operator input.

## Constants

| constant | default | rationale |
|---|---|---|
| K / N | 10 / 30 | fixed 1:2 ratio; tolerance auto-scales 3→30 nodes (any 2/3 of nodes may vanish) |
| decay tiers | 6 h / 24 h / 72 h / 7 d | see Membership; values are config, structure is the contract |
| W (watermark) | derived — see Watermark | K + reserve(v) + ε(μ); never below K + ⌈N/v⌉ meaningfully arises (floor = durability cliff) |
| B_max (burst target cap) | 5 nodes | largest single non-site event (strip/breaker/switch); tunable |
| σ (reserve variance slack) | 1 class | covers load variance of burst victims; raise toward the adversarial ceiling for co-located high-weight cores; tunable |
| ε (climb-back cover) | ~0 on healthy links | pessimistic-quantile volume/throughput term; tunable |
| scrub period | weekly walk; urgent queue continuous | detection bound for silent corruption |
| repair budget | ~10% uplink per node | 1 TB lost across 10 survivors → re-encoded in ~1 day |

Data loss requires > 2N/3 of a chunk's classes dead within one repair
window (~days); with disk lifetimes in years, independent-failure
MTTDL sits far beyond the design horizon at mesh sizes in range. The
dominant residual risk is correlated loss (same site/power) —
placement-diversity, out of scope here.

## Evidence

All in `spec/storage_policy.qnt`; four legs:

| leg | coverage | result |
|---|---|---|
| Apalache `verify`, depth 6 | complete over interleavings of fault budget + eviction/GC/repair | INV-DURABLE, INV-SPREAD: no violation |
| scripted witnesses | 18–20-tick schedules, 10k traces each | heal-after-depart; zero-cost sleep/wake; exact repair-cost accounting; no-repair and pull-only counterexamples |
| random simulation | real parameters, 8–10k traces × 60 steps | no violation |
| complete enumeration | all views, both scales | BRIDGE (all ≥2/3-online subsets); BURST (all b(v)-subsets vs closed form, b = active-profile fault budget); SPREAD max+min; verify-table drift guard |

Limits: the model checker is depth-bounded; long-horizon evidence is
witness/simulation-grade; configs are small (small-scope argument);
the model is single-chunk with one global membership/inventory view
and a scalar decay gate (per-node tiers are a trivial generalization —
safety is gate-independent). The B_max/expected-load watermark regime
binds only above enumerable table sizes; there the argument is
closed-form over the spread-lemma-pinned load profile.

Decisions the model changed: re-encode promoted to the core repair
loop; plain HRW rejected, then greedy capped HRW rejected too
(zero-load member witnessed — min-spread unbounded) in favour of
balanced capped rendezvous; instant membership sync replaced by the
decay gate after cost accounting exposed it as the thrash source.

## Deferred

- **RFC-STORAGE-002 (implementation)**: capped-HRW cutover (placement
  seed change — nothing deployed, still free), engine tick loop
  (scrub, watermark queue, chunk-batched re-encode, repairer
  sharding), watermark GC, pin API, tiered decay view, honest capacity
  metric (macOS purgeable-aware).
- **Consensus deactivation spec**: discharges the Membership rely
  condition; fault-budget-scaled vote-out lives there.
- Correlated-failure diversity (site tags in weights).
- Model refinements: per-node decay tiers, per-node view divergence;
  metric-shift oscillation stays prose (quantized weights make
  placement reaction bucket-granular by construction).