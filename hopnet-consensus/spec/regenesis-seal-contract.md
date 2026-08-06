# The seal contract (RFC-019 regenesis)

What the consensus engine promises — and requires — at an epoch
boundary, in engine vocabulary. The normative model is the
`epoch_policy` module of `validator_membership.qnt`; where this prose
and the model disagree, the model wins. The protocol that produces the
boundary (`regenesis_start` / `regenesis_commit` / `regenesis_abort`,
drain, snapshot, genesis construction) is RFC-019's business
(`docs/specs/regenesis.md`); this note is only the contract between
that protocol and the engine.

## The contract

1. **Terminal height.** The block carrying `regenesis_commit` decides
   at some height H. That block is the last block of the epoch: the
   engine halts after applying it, and nothing in this epoch is ever
   decided past H — not by this node, not by any node, not after a
   crash and recovery. H is terminal the way a decided value is final:
   unconditionally.
2. **Forward-only past the seal.** The abort window is exactly
   (`regenesis_start` decided, `regenesis_commit` decided). At H the
   window is closed; recovery from anything after H is restart-shaped,
   never rollback-shaped.
3. **Committed state carries; evidence does not.** The seated
   validator set, activation heights (and therefore proven-ness), and
   departure records cross the boundary verbatim — regenesis is not a
   membership event. Liveness evidence does NOT cross: `EvidenceMap`
   is in-memory by existing design and dies with the process. This is
   load-bearing, not incidental — the next epoch must start with empty
   evidence.
4. **Restart resumes at H+1.** Heights are continuous across epochs:
   the new genesis sits at H, and the first block decided in the new
   epoch is H+1, by the carried seated set under the carried quorum
   profile.
5. **The quiet period is structural.** With evidence empty at H+1,
   vote-out attestations cannot accumulate a qualifying staleness span,
   and seatings cannot show a qualifying bright span, until those spans
   re-accrue in the new epoch. Slow restarters and dark seats are
   therefore protected by the same windows that protect them in steady
   state — the engine adds no boundary-special grace and must not
   shorten the existing one.

## What the model proves

`epoch_policy` extends the membership machine with the boundary
(moratorium → seal → restart) and proves, inductively (no depth
bound), that the restart state satisfies the same inductive invariant
the membership safety matrix was proven from — so no-harm, the proven-
quorum ceiling, the floor, and no-exile all transfer into the new
epoch, including vote-out of seats that went dark across the boundary.
Seal safety is the `decidedPastSeal` ghost: any decide firing past the
seal would violate an inductively checked invariant.

## What the engine must implement (S5/S6)

Items 1–3 and the halt/recompute obligations below landed in S5
(`Application::sealed_after` + the terminal branch of `Effect::Decide`;
`regenesis_sealed_at` marker; idempotent artifact recompute); the boot
path at H+1 is S6's.

- Halt after applying the `regenesis_commit` block; refuse to
  propose, vote, or apply anything in this epoch above H.
- Treat sealed artifacts (snapshot, certificate, genesis) as derived,
  idempotent recomputations from local sealed state — a crash between
  seal and restart recovers by recomputing, never by peers.
- Boot the new epoch as a fresh engine at H+1 with the carried
  committed state and an empty evidence map.

> **Landed (S6).** The restart path consuming this contract shipped in
> RFC-019 S6: `regenesis::boot::boot_transition` (pre-pool, crash-safe
> swap), `regenesis::genesis` (canonical record; chain id = genesis
> block hash at H), exit code 75 restart derivation, and the
> `(epoch, version)` handshake. The contract's five clauses are
> exercised end-to-end by the in-process transition roundtrip and the
> orchestrator `regenesis-restart` / `regenesis-awaiting-upgrade`
> scenarios.
>
> **Extended (S7).** Clause 2's "recovery is restart-shaped, never
> rollback-shaped" now covers nodes that were ABSENT at the seal:
> `regenesis::join` fetches and verifies the epoch from peers, stages
> it, and requests the same restart, so the rebuild still happens in
> the boot path and no recovery reaches backwards. Clause 3 holds for
> them too — a rejoining node imports committed state only, and starts
> the new epoch with empty evidence. Clause 5 is unchanged: a rejoined
> node gets no boundary-special seating grace.
>
> **Corrected (post-review).** What a joiner's verification actually
> BINDS was narrower than clause 2's extension implied, and the gap
> mattered. A lineage record is peer-supplied; only the certificate is
> evidence. `verify_lineage` checked the block, the height and the
> certificate — but nothing tied the record's `snapshot_hash`,
> `seal_height` or `required_version_code` to what the quorum decided, so
> a peer could pair a genuine block and a genuine certificate with a
> substituted snapshot identity and have a joiner import bytes of the
> peer's choosing. Those three fields now come out of the boundary
> block's `regenesis_commit` (which carries the target version for exactly
> this reason) and are cross-checked against the record. Since the
> certificate binds the block hash, anything inside the block is
> transitively quorum-bound; anything outside it is a claim.
>
> The operator re-trust path was worse: waiving the weak-subjectivity
> overlap rule left each hop's certificate verified against the validator
> set declared INSIDE that same record — self-certifying. It now
> SUBSTITUTES an anchor rather than removing one: the operator supplies
> the target epoch's chain id out of band and the chain must terminate in
> exactly that.
>
> Also corrected: the sealed marker is `consensus_meta`, written after the
> decide commits, so a crash in between left the phase durably Sealed with
> no marker — read naively, indistinguishable from a healthy node, and the
> engine would start on the retired chain and refuse everything forever.
> H is now DERIVED from the committed row when the marker is absent, which
> is what the "derived, idempotent recomputation" obligation below always
> required of it.
>
> **Enforced (S8).** Clause 2's forward-only rule now has a mechanism
> rather than prose behind it. Within the rollback window an operator may
> abandon the boundary — `POST /consensus/regenesis/rollback` on every
> node writes a marker that the boot path honours ahead of every other
> path, restoring the retained database (or clearing the seal in place,
> for a node that parked without crossing) and clearing the seal state
> that would otherwise re-cross. Past the window the request is refused:
> the retained database is gone, and recovery is another regenesis,
> forward. Note that restoring the retained file by hand is NOT a
> rollback — it leaves the sealed marker and committed phase behind.
