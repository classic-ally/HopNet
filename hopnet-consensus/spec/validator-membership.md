# RFC-CONSENSUS-001: Validator Membership Policy

**Status**: Specified + model-checked (2026-07-16). Implementation
pending.
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
carries a guard (Departure classes). Exposure is capped by the proven
cushion: no seating may inflate quorum beyond what the live PROVEN
members cover, so even the joint death of every not-yet-proven seat
never stalls the mesh (Admission & readmission — the proven-quorum
ceiling, which the model substituted for this section's original
per-batch bound after finding the latter does not compose).

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
- **Freshness on contact — exactly two evidence classes refresh.**
  (1) REACHABILITY evidence: my own authenticated exchanges with X —
  probe answers, RPC completions, votes received first-hand, sync
  serves. My own evidence contradicts my attestation. (2)
  CONTRIBUTION evidence: X's signature inside a committed
  certificate, however it reached me — locally verified against the
  valset, unforgeable, identical at every node. It is not proof that
  X is reachable; it is the mesh's own ledger entry that X's vote
  counted, and removing a contributing node is precisely the harm
  INV-NO-HARM forbids — this is the replicated evidence that stops a
  partition-blinded quorum. NOTHING ELSE refreshes: relayed
  artifacts — gossip X originated arriving via Y, transactions X
  signed, however cryptographically valid — prove X is alive
  SOMEWHERE, which is the wrong property. A validator that files
  transactions through a friend but whose votes never land is
  missing from every quorum: dark for the only metric vote-out
  polices. Were relayed artifacts to refresh, one relay path would
  make such a node unremovable for the whole mesh — the asymmetric
  block generalized to every observer. Participation in consensus is
  the only defense, exactly as unresponsiveness is the only crime.
- **Bounded staleness — the probe is a deadline, not a schedule.**
  Evidence must not rely on consensus traffic alone: an idle mesh
  pauses heights, and a dark node would otherwise go unnoticed
  indefinitely. The contract: a probe fires at X exactly when X's
  evidence age reaches T_probe(band) — so a busy mesh probes almost
  never (traffic refreshes everyone), and an idle mesh is watched at
  the band's cadence. Suspicion attaches to the UNANSWERED probe,
  never to the deadline: X leaves the observer's live estimate only
  at T_unresponsive = T_probe + g (g = the probe response grace),
  so silence alone never compresses anything — a healthy silent mesh
  cycles synchronized probe rounds and stays calm. Classification
  must be a pure function of recorded evidence (ages and probe
  attempts), never of in-flight probe state; the attestation floor
  is two probe attempts since last contact.
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

**Affirmative quorum, and what it buys.** Approval counts only
affirmative attestations — quorum(v) yes-votes; absence, abstention,
and the target's dissent are never yes. The model derives a theorem
from this counting rule alone: a wrongful vote-out of a live node
requires quorum(v) OTHER live validators, so the survivors retain
quorum — even with the freshness contract fully broken, wrongful
removal can steal one point of headroom, never stall the mesh
(model: negF1HeadroomTheftTest + 10k adversarial traces). The
freshness contract's load-bearing job is therefore blocking the
theft; stall-safety comes from the counting rule.

**Fairness assumption (asymmetric darkness).** Removal liveness
assumes every live observer's evidence about a dark target eventually
goes stale. A node answering probes from a single validator while
contributing nothing to quorum blocks its own removal — at the cliff,
permanently, not slowly (model: negF2AsymmetricBlockTest — the
removal is blocked until the asymmetric half-link itself decays).
Real links that carry nothing but probe answers do decay; the model
checks RECOVERY under exactly that fairness and exhibits the
permanent block without it.

A node that votes in consensus but refuses every other duty
therefore keeps its seat: vote-out judges quorum contribution only,
and a node whose votes land in certificates is contributing. Other
misbehaviour is a slashing concern, out of scope.

## Headroom schedule

Vote-out urgency is a function of distance from the stall. Each
validator estimates H = live − quorum(v) from its own evidence — the
same evidence the predicate reads — and derives its removal window
T_out from the band it believes the mesh is in:

| band | condition | T_probe | T_out = 2·T_probe + g | stance |
|---|---|---|---|---|
| lazy | H ≥ 2 | T_probe_lazy | ~2·T_probe_lazy | grace for blips; hysteresis fully applies |
| fast | H = 1 | T_probe_fast | ~2·T_probe_fast | one failure from the cliff |
| cliff | H = 0 | T_probe_cliff | ~2·T_probe_cliff | next failure stalls the mesh |

T_out is the duration parameter of dark(X): the span over which
every piece of evidence about X — direct exchange, received votes,
certificate signatures, own probes — must be stale before a
validator may attest. Urgency scales the probe cadence itself; the
window is pinned at its floor, two probe cycles plus one response
grace — a window you have not probed twice is a window you cannot
attest, and the second probe gets its grace before the boundary
(one missed probe never triggers, in every band).

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
vote-out recoups the seat. Exposed seatings lean on their members
staying alive, and the model corrected this section's original
per-batch bound (e ≤ H at seating): it does not compose. Two
sequential batches, each individually legal, jointly die into a stall
the unbatched mesh would have survived (model:
ceilingCompositionNegTest). The load-bearing rules:

- **The proven-quorum ceiling.** A member is UNPROVEN until it has
  survived P_prove in seat — genesis seats are proven by fiat, and a
  pre-seat bright span does not count: reachability evidence is not
  survival. A seating may inflate quorum only within the proven
  cushion: quorum(v+b) − quorum(v) ≤ max(0, live_proven − quorum(v)).
  Unproven members are never load-bearing — the joint death of every
  unproven seat still leaves a live proven quorum. The per-batch
  e ≤ H bound is the special case with no unproven seats
  outstanding; the cushion self-accounts for stacked seatings (each
  one's quorum inflation shrinks it) and re-opens as members prove.
  At the cliff only exposure-free seatings pass — "calm-weather
  tool" is a theorem, not advice — and the V_bft crossing, always
  exposed, needs one point of proven cushion.
- **Zero-tolerance waiver.** A set that already stalls on any single
  death (tol(v) = 0, i.e. v ≤ 2) has no tolerance to protect — and
  without a waiver could never grow, since any growth from v = 1
  inflates quorum. Growth from there bets on the batch, and the bet
  strictly dominates: the death count needed to stall never
  decreases on a posture-legal seating. Full S_min still gates the
  evidence.
- **Exposure-free: S_min scales with H**, mirroring T_out — full
  span when comfortable, floor when H ≤ 1. Refusing free headroom at
  the cliff would be malpractice.
- **Exposed: full S_min from every member, at every H.** Since the
  V_bft crossing is always exposed, the security upgrade never rides
  relaxed evidence. Re-characterized by the model: under the
  proven-quorum ceiling a batch's death cannot stall the mesh, so
  S_min carries churn hygiene and evidence quality — the structural
  stall-safety lives in the ceiling.
- **Voluntary leavers are exempt from S_min on exposure-free
  seatings** — a graceful exit is not a fault, and hysteresis exists
  to damp faults. No one is exempt on exposed seatings.

One principle covers both directions: urgency compresses evidence
windows — T_out on removal, S_min on exposure-free admission — and
never buys exposure beyond the proven cushion.

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

The probe base B is the single clock: per-band probe deadlines are a
doubling ladder on B, the removal window is everywhere pinned at two
probe cycles plus one grace, and admission spans are wall-time.
Rescaling B rescales the whole policy coherently (orchestrator tests
seed it small and everything compresses proportionally). Urgency
lives in the CADENCE, not the multiplier: a mesh with comfortable
headroom watches lazily; only distance from the stall buys chatter.
Probes fire as deadlines (age reaching T_probe), so a busy mesh
probes almost never and an idle healthy mesh cycles quiet
synchronized rounds without ever raising suspicion.

| constant | default | role |
|---|---|---|
| B | 30 s | probe base; T_probe_cliff = B, fast = 2B, lazy = 4B |
| g | 5 s | probe response grace (= the probe RPC timeout) |
| T_probe(band) | 30 s / 60 s / 2 min | max tolerated silence before the deadline probe fires |
| T_unresponsive(band) | T_probe + g | age at which X leaves the observer's live estimate |
| T_out(band) | 2·T_probe + g (65 s / 2 m 5 s / 4 m 5 s) | removal window — two probed misses, each with its grace |
| S_floor | one probe cycle | admission span, exposure-free at H ≤ 1 |
| S_full | 30 min | admission span, comfortable H and all exposed seatings |
| P_prove | 30 min | in-seat survival before a member is proven (ceiling cushion) |
| V_bft | 7 | AUTO profile switch point |
| quorum_profile | auto | auto \| bft \| majority (pinned) |
| catch-up tolerance | 10 heights | existing activation gate, unchanged |
| set floor | v′ ≥ 1 | INV-FLOOR |

The orderings and ratio structure are load-bearing; the point values
are judgment inside them:

- **The window multiplier is pinned at its floor (2, plus g)** in
  every band — the smallest window containing two probe attempts
  with the second's grace elapsed before the boundary ("one missed
  probe never triggers", race-free). Derived, not tuned. All urgency
  dynamics live in T_probe(band).
- **Eviction latency decomposes as max(notice, window)** because
  dark age counts from last contact, not from discovery. Calm case
  (mesh stays lazy): eviction ≈ T_out_lazy ≈ 4 min, identical to a
  fixed-cadence design. Emergency case (failures drop the band):
  first lazy deadline probe discovers, the band compresses, deadlines
  shorten automatically — eviction ≈ T_probe_lazy + T_probe_cliff ≈
  2.5–3 min. The lazy cadence costs latency only in multi-failure
  emergencies, and only ~one lazy interval; independent-failure
  stacking inside that interval is negligible at home-mesh failure
  rates, and correlated failure is cadence-immune.
- **B = 30 s** sits above transient noise that is not absence —
  wifi roam, congestion, sleep-wake blips, all seconds-scale — and
  well below the minutes mandate. T_out_lazy ≈ 4 min sits above the
  blip census (AP reboot ~1–2 min, ISP re-auth,
  lid-close-and-move-rooms); refinable later from fleet availability
  data — static mesh-wide tuning, not the per-node adaptation
  Hysteresis rejects.
- **Doubling steps, not a continuous T(H)**: steps quantize band
  disagreement between validators; a continuous schedule maximizes
  the slowest-attestor spread.
- **T_out_lazy ≪ S_full ≪ smallest storage decay tier (6 h)** — a
  removal must outlast the window that triggered it (else vote-out
  is theater: the flapper re-seats before proving anything), and
  validator standing must recover faster than storage standing
  decays or the timescale ordering inverts.
- **V_bft = 7**, doubly principled — as corrected by the model
  (seamLemmaTest): the crash-neutral crossings, where
  tol_maj(v−1) = tol_bft(v), are exactly {4, 5, 7}, not 7 alone.
  What is unique about 7 is being crash-neutral AND
  Byzantine-meaningful at once: f_eq = ⌊(v−1)/3⌋ ≥ 2 first holds at
  7. At 4 and 5 the whole Byzantine budget is one fault, which the
  first ordinary crash spends — theater; crossing at 6 or 8 drops
  crash tolerance at the seam.

Crash tolerance across the AUTO composite is monotone non-decreasing
in v (1, 2, 2, 2, 2, 2, 3, …): growth never makes the mesh more
fragile; it sometimes pauses making it sturdier. The v = 5..9
plateau (two crashes throughout) is the price of f_eq = 2, paid
knowingly. Under AUTO, small meshes run majority automatically — the
orchestrator's forced-majority profile for kill tests becomes
redundant (pinning remains for exercising BFT at small v).

## Evidence

All green 2026-07-16. Model: `spec/validator_membership.qnt`;
commands and runtimes: `spec/README.md`. Layers: [lemma] = complete
enumeration v = 1..30, all three quorum modes; [ind] = Apalache
inductive, depth-free; [bounded] = Apalache bounded; [wit] =
scripted witness/NEG run.

- INV-NO-HARM incl. the proven-quorum ceiling — [ind] NoError on
  small (1..5), seam (1..9 AUTO), bft/maj pinned, and ten (1..10)
  configs, 6–57 s each. The wrongful-removal case (evidence contract
  broken): [bounded] no-stall theorem on the per-attestor evidence
  machine, depth 10, plus 10k adversarial traces — wrongful vote-out
  steals headroom, never stalls (see Evidence & validation,
  affirmative quorum).
- INV-FLOOR; INV-NO-EXILE enabledness half — [ind]. NO-EXILE
  liveness half — [wit] seam round-trip + scheduler self-heal
  (unbounded temporal property; witness-grade by design).
- Partition safety — [lemma] DISJOINT-QUORUM (2q > v everywhere) +
  [wit] minority impotence and a block-relative heal round-trip that
  crosses the seam both ways (BFT shed under split, restored by the
  crossing batch after heal and re-proving).
- RECOVERY under fairness — [wit] recovery, mass-dark with the
  worked table's intermediate H values asserted, multi-kill
  same-cycle, slowest-attestor delay. Fairness NEGs: kills outrun
  windows ⇒ stall with the machinery blameless; asymmetric refresh ⇒
  removal BLOCKED until the half-link decays (the stated fairness
  assumption).
- GUAR-HONEST-SET + longest-dark-first — [wit] + 5k traces on the
  deterministic scheduler config (calm-bound invariant clean).
- One-removal-per-height mass-dark convergence — [wit].
- Exposure NEGs — per-batch ceiling non-composition [wit]; cliff
  crossing dies ⇒ stall with the ceiling off, refused with it on
  [wit]. The originally-planned NEG (a) (relaxed S_min ⇒ stall) is
  SUBSUMED: under the proven-quorum ceiling a batch's death cannot
  stall regardless of S_min — S_min's role is re-characterized in
  Admission & readmission.
- Parity, batch (incl. the B_max = 5 licence), seam ({4,5,7}
  correction, f_eq(7) = 2), tolerance monotonicity + the literal
  0,1,1,2,2,2,2,2,3 sequence, posture surgical-clause, free-option
  identity, complementary parity — [lemma]. Literal-table drift
  guards + scaled-constant ordering guards in every config.

**Non-obligations** (assumed, not model-checked): wall-clock probe
semantics — the per-band cadence ladder, the response grace g, the
deadline-not-schedule firing rule, and the evidence-record purity
contract all live below the model's tick abstraction (its windows
are abstract ordered tick counts; the invariants are
value-agnostic); the T_probe blip census; the evidence mechanism
beyond the freshness/independence contract (probe transport,
certificate parsing); proposer-side brightest-first ranking (the model
over-approximates with any eligible batch — the safe direction);
duplicate-proposal harmlessness; catch-up sync dynamics (an unsynced
admittee is dominated by the modeled dies-at-seating case); the
cross-RFC ordering S_full ≪ smallest storage decay tier; Byzantine
equivocation itself (f_eq is arithmetic — the BFT safety theorem is
imported from Tendermint/Malachite); genesis composition.

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
