# RFC-CONSENSUS-001: Validator Membership Policy

**Status**: Draft.
**Normative model**: `spec/validator_membership.qnt` — where prose and
model disagree, the model wins. Commands: `spec/README.md`.

## Scope

Consensus commits at height h only when a quorum of the validator set
at h signs the same value. This RFC defines the policy that keeps that
set honest about who can actually vote: how a node is admitted
(activation), how it departs voluntarily (leave), how the mesh removes
a node that stopped responding (vote-out), and how a departed node
returns (readmission). Detection mechanism, probe scheduling, and
transaction wiring belong to the implementation RFC; this document
specifies only the contracts those mechanisms must satisfy.

Out of scope: storage membership and placement (RFC-STORAGE-001 —
which consumes this policy's guarantee but runs on its own, slower
timescale), Byzantine misbehaviour beyond unresponsiveness
(equivocation, slashing), and recovery of a mesh that has already
lost quorum (no transaction can commit there; manual recovery is a
separate concern).

## Definitions

- **V** — the validator set at a height; v = |V|. Changes only by
  committed transition (activation, leave, vote-out), effective at a
  height boundary.
- **live** — the members of V currently able to vote (running,
  reachable, caught up). The environment changes this; the policy
  only observes it.
- **quorum(v)** — Q under the mesh's quorum profile: ⌊v/2⌋+1
  (majority, crash-fault) or ⌊2v/3⌋+1 (BFT, tolerates equivocation).
  Default mode is AUTO: profile(v) = majority for v < V_bft, BFT for
  v ≥ V_bft, derived per height from the committed validator set's
  size — deterministic, replicated, and locked to each height for
  certificate verification forever. Pinning to a fixed profile is a
  mesh-creation option (tests; users who know what Byzantine means).
- **fault posture (H, f_eq)** — headroom paired with equivocation
  tolerance: f_eq = ⌊(v−1)/3⌋ under BFT, 0 under majority. Admission
  optimizes posture; removal optimizes honesty and H alone.
- **headroom H** — live − quorum(v). H < 0 means consensus is
  stalled: nothing can commit, including the membership transition
  that would fix it. Every obligation below is therefore conditional
  on acting while H ≥ 0.

## Invariants

**INV-NO-HARM** (safety) — every committed membership transition
preserves live′ ≥ quorum(v′): no admission, leave, or vote-out stalls
a mesh that was live. Vote-out of a dark node strictly grows H (live
unchanged, quorum shrinks); admission of a caught-up node cannot
shrink it; voluntary leave is the one transition that can, and
carries a guard (Departure classes). Exposure is capped by headroom:
no seating may borrow more quorum than the mesh could survive losing
— e ≤ H at seating — so even a batch's complete failure never stalls
the mesh (Admission & readmission).

**INV-FLOOR** (safety) — the validator set never empties: v′ ≥ 1.

**INV-NO-EXILE** (liveness) — a bright candidate is never barred: it
is seated whenever seating it strictly gains headroom, and otherwise
stands in the pool, first in line by merit, at full standing
everywhere but consensus. Exclusion is never permanent for a healthy
node in any mesh that experiences failures — and they all do.

**RECOVERY** (liveness, under fairness) — whenever H ≥ 0 and the
failure process pauses long enough for one detect-and-commit cycle,
every dark validator is eventually voted out, restoring H to the
full budget of the surviving set. No policy survives an adversary
that kills quorum-many nodes between commits; the obligation is to
never stall itself (INV-NO-HARM) and to reclaim headroom whenever
the failure process allows.

**Partition safety** (corollary) — a minority partition cannot vote
out the majority: it lacks quorum to commit anything. Needs no
mechanism; the model checks it anyway.

**Exported guarantee — GUAR-HONEST-SET.** A validator continuously
unreachable longer than the removal window T_out is removed within
bounded heights (quorum permitting); hence every validator is either
reachable or inside its removal window. RFC-STORAGE-001 may later
consume this as its absence source (its Deferred list).

## Departure classes

Two transitions remove a validator. They share the execute path —
deactivation at a committed, deterministic effective height — and
differ only in authorization and guards.

**LEAVE (voluntary).** Authorized by the departing node's signature;
consent is the evidence, no reachability predicate. Guarded:
approvers accept only if the set survives the departure — v′ ≥ 1
and, by each approver's own liveness estimate, live′ ≥ quorum(v′).
A leave that would stall the mesh is refused. The refusal cannot
stop the node from shutting down anyway — that is just a failure,
and vote-out inherits it — but an orderly node never causes the
stall, and the refusal is the operator's signal. Graceful app exit
submits leave and waits (bounded) for the commit before stopping.

**VOTE-OUT (involuntary).** Proposed by any validator that observes
a peer dark; commits under the normal transaction approval rule,
where each yes-vote is an independent attestation — from that
validator's own evidence — that the target has been continuously
unreachable for at least T_out (Evidence & validation). Structurally
harmless: the target contributes no live vote, so removal leaves
live unchanged and shrinks quorum — H never decreases. One removal
per height; headroom is re-evaluated between removals (Headroom
schedule orders the queue when several nodes are dark). Removal near
the V_bft seam may shed the BFT profile (f_eq 2 → 0): accepted. The
set never keeps a dark seat to preserve the label — a dark seat is
not defense, it is the fiction of one; the graceful shrink to
majority is the removal policy working.

**Shared semantics.** Deactivation ends validator duties only: the
node keeps its identity, registration, data, and storage-membership
standing (RFC-STORAGE-001 tiers govern that on their own, slower
clock). A departed node returns through Readmission — never by
silently resuming.

False-positive cost: a live node wrongly voted out (asymmetric
partition, slow link) loses nothing durable — it observes its own
deactivation in replicated state and re-enters through the normal
readmission gate. One round trip. This asymmetry — cheap wrongful
removal, expensive stall — shapes the hysteresis stance below.

## Evidence & validation

Membership transactions follow the two-phase handler pattern already
established by activation: a subjective validation phase, where each
validator votes from its own local evidence, and a deterministic
execute phase, which records the outcome identically everywhere once
approved. Wall-clock time is permitted in validation — a vote is an
opinion — and forbidden in execute. Activation's catch-up check
already validates against the approver's own consensus height;
vote-out extends the same shape with a reachability predicate.

**The predicate.** dark(X): "by my own evidence, X has been
continuously unreachable for at least T_out." A yes-vote on a
vote-out proposal is exactly this attestation. Contract on the
evidence — mechanism belongs to the implementation RFC:

- **Independence** — each validator attests only from its own
  observations; verdicts are never forwarded or delegated. Quorum
  approval is meaningful (and partition safety holds) only because
  attestations share no fate.
- **Freshness on contact, direct or proven** — any authenticated
  exchange with X (RPC completion, consensus vote received from X,
  sync serve) refutes darkness and resets X's window. So does X's
  signature inside a committed certificate: cryptographic proof of a
  live vote at that height, visible even to validators X cannot
  reach. This is load-bearing for INV-NO-HARM — a node whose votes
  are landing in certificates is contributing live quorum weight,
  and removing it would not be harmless; certificate participation
  is exactly the replicated evidence that stops a partition-blinded
  quorum from doing so.
- **Bounded staleness** — evidence must not rely on consensus
  traffic alone: an idle mesh pauses heights, and a dark node would
  otherwise go unnoticed indefinitely. Each validator refreshes its
  evidence about every other validator at least every T_probe
  (active probe or equivalent).
- **Corroboration is optional, never sufficient** — replicated
  signals toward darkness exist for free (commit certificates record
  the commit round, and the round-0 proposer is deterministic, so
  cert.round > 0 is mesh-agreed evidence of a skipped proposer;
  certificate signature absence likewise). Both are ambiguous —
  network-wide slowness and late-but-honest votes look identical —
  so they may strengthen a verdict, never substitute for direct
  observation. Ambiguity runs one way: a present signature proves
  liveness; an absent one proves nothing.

**Target dissent is not a veto.** A live node wrongly proposed can
vote no — and its participation is itself counter-evidence honest
approvers will observe as fresh contact. But a veto would make an
unresponsive-but-scheming node immortal; the quorum rule alone
decides.

A node that votes in consensus but refuses every other duty
therefore keeps its seat: vote-out judges quorum contribution only,
and a node whose votes land in certificates is contributing. Other
misbehaviour is a slashing concern, out of scope.

## Headroom schedule

Vote-out urgency is a function of distance from the stall. Each
validator estimates H = live − quorum(v) from its own evidence — the
same evidence the predicate reads — and derives its removal window
T_out from the band it believes the mesh is in:

| band | condition | window | stance |
|---|---|---|---|
| lazy | H ≥ 2 | T_lazy | grace for blips; hysteresis fully applies |
| fast | H = 1 | T_fast | one failure from the cliff |
| cliff | H = 0 | T_cliff | next failure stalls the mesh; act within ~one probe cycle |

T_out is the duration parameter of dark(X): the span over which
every piece of evidence about X — direct exchange, received votes,
certificate signatures, own probes — must be stale before a
validator may attest. Its floor is one probe cycle: a window you
have not probed is a window you cannot attest.

Values in Constants. The trade is explicit: shrinking the window
raises false-positive risk — cheap, one readmission round trip — to
cut stall risk, which is unrecoverable below quorum without manual
intervention. At the cliff the asymmetry is total and the window all
but disappears.

**Payoff.** Voting out a dark validator never changes live — the
node wasn't voting — but shrinks the set, and quorum shrinks with
it: one removal lowers quorum by 1 on two of every three steps under
BFT, every other step under majority. Headroom climbs as dark nodes
leave.

Worked, v=10 BFT with 3 dark:

| step | v | quorum | live | H |
|---|---|---|---|---|
| start | 10 | 7 | 7 | 0 — at the cliff |
| remove one | 9 | 7 | 7 | 0 |
| remove two | 8 | 6 | 7 | 1 |
| remove three | 7 | 5 | 7 | 2 |

Each removal commits while H ≥ 0, one height apart. Tiny-BFT caveat:
4→3 leaves quorum at 3, buying nothing. Removal happens regardless —
GUAR-HONEST-SET is about set honesty, not only headroom.

**Ordering and duty.** Several dark: longest-dark first (most
attestors already past threshold), one per height, H re-estimated
between. Any validator past its own window may propose; duplicates
are harmless (a proposal against an already-deactivated node fails
validation trivially). RECOVERY requires only that at least one live
validator eventually proposes — a fairness obligation on the
implementation, not a coordination protocol.

**The slowest-attestor effect.** Bands are estimated subjectively,
so validators may disagree; approval needs a quorum whose own
windows have each elapsed. At the cliff this is every live validator
— live = quorum exactly — so the effective window is the slowest
live validator's. Two things keep that honest: the liveness half of
the evidence (certificate participation) is replicated, pulling H
estimates together; and any fixed window elapses under continued
darkness, so removal is delayed, never blocked. The model checks
RECOVERY under adversarial band disagreement.

## Admission & readmission

All seating passes one gate. A candidate is any registered node not
currently seated — never seated, voluntarily departed, or voted out;
the gate does not distinguish origins, only evidence. Candidates are
full citizens everywhere else: they sync decided values, hold and
serve fragments, answer probes, accrue metrics history (storage
standing is metrics-derived and never reads the validator set). The
pool is not a penalty box; it is where the mesh keeps warm,
measured, synced spares.

**bright(X)** — the admission predicate, dark(X) mirrored: "by my
own evidence, X has been continuously reachable for at least S_min."
Each approver measures its own span on its own clock; no agreement
on when the span began. Two floors never relax: X answers the
approver's live probe now, and X passes the existing catch-up gate.

**Parity.** Seating b live candidates moves headroom by
ΔH = b − (quorum(v+b) − quorum(v)) ≥ 0. Single seatings gain only
when quorum stays flat — one step in three under BFT, one in two
under majority; every other step is lateral (ΔH = 0). Batches
repair parity: any batch of three gains under BFT, any batch of two
under majority, and ΔH never decreases with batch size. Under the
AUTO profile the composite quorum has a seam: crossing V_bft pays
the BFT premium, producing a run of four consecutive lateral steps
(v = 5 through 9 at V_bft = 7), and gaining batches near the seam
need up to five members. Away from the seam the per-profile closed
forms hold.

**Seating rule.** Seat the largest eligible batch that strictly
improves fault posture — ΔH ≥ 1, or ΔH = 0 with Δf_eq ≥ 1 —
committed as one transition at one height, evaluated jointly. No
eligible subset improves ⇒ defer everyone. The posture clause is
surgical: the only lateral it admits is the V_bft crossing, the one
lateral that buys equivocation tolerance; every other lateral stays
refused — same headroom, more consensus weight, one more future
liability — and the candidate loses nothing by waiting, since
removals gain on precisely the steps admissions don't, so failures
shift parity in its favour.

**Exposure.** A seating borrows headroom equal to Δquorum = b − ΔH:
quorum points inflated on the promise the batch stays alive.
Exposure-free seatings (Δquorum = 0) are free options — if the node
dies at once, H returns exactly to its prior value, and the eventual
vote-out recoups the seat. Exposed seatings can kill: at H = 0, a
batch of three weak candidates (exposure 2) that flaps out within
one removal window leaves H = −2 — stalled, the deflating vote-outs
unable to commit — where the unbatched mesh would still be alive.
Hence:

- **Exposure ceiling: e ≤ H at seating.** A batch's complete failure
  takes the mesh to H − e; the ceiling guarantees that worst case
  stays alive. At the cliff only exposure-free seatings exist —
  "calm-weather tool" is a theorem, not advice. The V_bft crossing,
  always exposed, needs H ≥ 1.
- **Exposure-free: S_min scales with H**, mirroring T_out — full
  span when comfortable (it buys only churn hygiene; registration is
  owner-gated, seat-spam is not the threat model), floor when H ≤ 1.
  Refusing free headroom at the cliff would be malpractice.
- **Exposed: full S_min from every member, at every H.** The mesh is
  leaning on these nodes; it does not lean on strangers. Since the
  V_bft crossing is always exposed, the security upgrade never rides
  relaxed evidence.
- **Voluntary leavers are exempt from S_min on exposure-free
  seatings** — a graceful exit is not a fault, and hysteresis exists
  to damp faults. No one is exempt on exposed seatings.

One principle covers both directions: urgency compresses evidence
windows — T_out on removal, S_min on exposure-free admission — and
never buys exposure.

**Selection.** Candidates outnumbering the batch: proposers pick the
brightest they observe (longest span, ties by node_id); approvers
verify eligibility only, never ranking — ranking over subjective
spans would reintroduce coordination. Races serialize through
heights; the next height re-evaluates parity. Disagreement degrades
to deferral, never deadlock: RECOVERY rides on removals alone, and a
disputed candidate keeps accruing span. If a third of the mesh
cannot reach a candidate, its seat can wait.

**Genesis** is the one wholesale admission: V₀ is a mesh-creation
input, before this policy exists.

The equilibrium is worth naming: greedy removal (gains two steps in
three) plus stingy admission (gains one in three) drifts the seated
set small and reliable — the always-on core holds quorum while
flaky devices settle into the pool as hot spares, losing nothing
they can feel. The set self-selects toward the machines that
deserve it.

The security level heals itself the same way the set does: a fault
at v = V_bft shrinks the mesh to majority gracefully (removal is
honesty, never label preservation); the healed node accrues span in
the pool; a posture-improving crossing re-seats it; BFT returns. No
step involves a human.

## Hysteresis

The policy is deliberately light on damping. The three-timescale
design (RFC-STORAGE-001, Membership timescales) already isolates the
expensive consequences: placement keys off storage members, not the
validator set, so validator churn moves zero bytes. A full flap
cycle costs two transactions and a catch-up check, and the asymmetry
is self-punishing — removal relieves the mesh instantly, readmission
costs the flapping node a demonstrated bright span. A node with a
bad link degrades itself, not the mesh.

Three mechanisms, all mild:

- **The window is the damping.** dark(X) requires the full T_out of
  continuous silence; a single missed probe or skipped round never
  triggers. No additional consecutive-failure counting — the window
  already is that.
- **One removal per height.** Re-estimation between removals bounds
  overshoot when several nodes blip at once — a switch reboot is not
  a mass departure.
- **Reputation before re-seating.** Readmission after vote-out
  requires a demonstrated bright span — S_min(H), per Admission &
  readmission — not an elapsed timer: time proves nothing, a bright
  span proves the fault healed. Voluntary leave requires none
  (graceful exit is not a fault) outside exposed batches. Flap churn
  from fault-flappers is bounded to one cycle per S_min per node.

What is deliberately absent: storage-style adaptive tiers. Learning
per-node absence patterns is the right tool where removal is
expensive and the timescale hours to days; validator membership is
minutes-scale and churn-tolerant, and a learned tier would only
delay the honest answer.

## Constants

All values are validation-side inputs — parameters of subjective
votes, not of deterministic execution — so per-node disagreement
degrades latency, never safety (the slowest-attestor effect is the
worst case). They are replicated anyway for band alignment:
`hopnet_consensus_policy` key/value table, genesis-seeded, code
defaults when absent, following the `hopnet_storage_policy`
precedent and the module-prefix convention.

T_probe is the single clock; every window is a count of probe
cycles — a removal window of N is N consecutive missed probes, an
admission span of N is N consecutive answered ones. Rescaling
T_probe rescales the whole policy coherently (orchestrator tests
seed it small and everything compresses proportionally).

| constant | default | role |
|---|---|---|
| T_probe | 30 s | the evidence clock; every validator probes every other at least this often |
| T_cliff | 2·T_probe (60 s) | removal window at H = 0 — two missed probes |
| T_fast | 4·T_probe (2 min) | removal window at H = 1 |
| T_lazy | 10·T_probe (5 min) | removal window at H ≥ 2 |
| S_floor | 1·T_probe | admission span, exposure-free at H ≤ 1 — one observed probe |
| S_full | 60·T_probe (30 min) | admission span, comfortable H and all exposed seatings |
| V_bft | 7 | AUTO profile switch point |
| quorum_profile | auto | auto \| bft \| majority (pinned) |
| catch-up tolerance | 10 heights | existing activation gate, unchanged |
| set floor | v′ ≥ 1 | INV-FLOOR |

The orderings and multiplier structure are load-bearing; the point
values are judgment inside them:

- **T_cliff = 2 cycles** — the smallest window that both contains a
  probe attempt and preserves "one missed probe never triggers" at
  the cliff. Derived, not tuned.
- **T_probe = 30 s** sits above transient noise that is not absence
  — wifi roam, congestion, sleep-wake blips, all seconds-scale — and
  an order of magnitude below the minutes mandate (RFC-STORAGE-001,
  Membership timescales). The census arguments below assume this
  default; a production change to T_probe should re-check them.
- **T_lazy = 10 cycles** sits above the blip census (AP reboot
  ~1–2 min, ISP re-auth, lid-close-and-move-rooms) and low enough
  that GUAR-HONEST-SET stays a minutes-grade bound. Refinable later
  from fleet availability data — static mesh-wide tuning, not the
  per-node adaptation Hysteresis rejects.
- **2 < 4 < 10 cycles as steps**, not a continuous T(H): steps
  quantize band disagreement between validators; a continuous
  schedule maximizes the slowest-attestor spread.
- **T_lazy ≪ S_full ≪ smallest storage decay tier (6 h)** — a
  removal must outlast the window that triggered it (else vote-out
  is theater: the flapper re-seats before proving anything), and
  validator standing must recover faster than storage standing
  decays or the timescale ordering inverts.
- **V_bft = 7**, doubly principled: the smallest v where the
  Byzantine budget survives one crash (f_eq = ⌊(v−1)/3⌋ ≥ 2 — below
  that, the first ordinary crash spends the whole budget and the
  defense is theater), and the unique crash-neutral crossing:
  majority tolerance ⌈v/2⌉−1 and BFT tolerance ⌊(v−1)/3⌋ meet only
  at v = 7 (2 = 2). Crossing earlier or later drops crash tolerance
  at the seam.

Crash tolerance across the AUTO composite is monotone non-decreasing
in v (1, 2, 2, 2, 2, 2, 3, …): growth never makes the mesh more
fragile; it sometimes pauses making it sturdier. The v = 5..9
plateau (two crashes throughout) is the price of f_eq = 2, paid
knowingly. Under AUTO, small meshes run majority automatically — the
orchestrator's forced-majority profile for kill tests becomes
redundant (pinning remains for exercising BFT at small v).

## Evidence

To check (Quint/Apalache; results recorded here when green):

- INV-NO-HARM, including the exposure ceiling, over all transition
  interleavings — majority, BFT, and AUTO composite quorum.
- INV-FLOOR; INV-NO-EXILE (pool standing + eventual seating in
  fault-bearing runs).
- Partition safety: no minority partition commits a removal.
- RECOVERY under fairness (failure inter-arrival > one
  detect-and-commit cycle), including adversarial band disagreement
  (slowest-attestor delays, never blocks) and across the V_bft seam
  (graceful shrink, then re-upgrade).
- One-removal-per-height suffices: mass-dark converges without
  batch removal.
- Exposure NEG witnesses: (a) a relaxed exposed batch at H = 0 dies
  ⇒ stall — proves full-S_min-on-exposure load-bearing; (b) a V_bft
  crossing at H = 0 dies ⇒ stall — proves the exposure ceiling
  load-bearing.
- Parity and posture tables: ΔH and Δf_eq for single and batch
  seatings, v = 1..30, all three quorum modes — the closed forms and
  seam claims quoted in Admission & readmission.
- Crash-tolerance monotonicity of the AUTO composite.

## Deferred

- Consensus deactivation as storage absence source (GUAR-HONEST-SET
  consumption; RFC-STORAGE-001 Deferred).
- Slashing / equivocation evidence — misbehaviour beyond
  unresponsiveness.
- Batch removals (mass-dark fast path; one-per-height suffices).
- Escalating probation for repeat fault-flappers (flat S_min
  suffices at current churn cost).
- Emergency reconfiguration below quorum (manual; nothing commits
  there).
- Validator-set-scoped gossip (implementation: gossip.rs stage-5
  TODO assumes every node is a validator).
- Runtime policy updates via consensus settings transaction (shared
  concern with `hopnet_storage_policy`).
- Fleet-informed static retuning of T_lazy from availability data.
