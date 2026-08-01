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

- Halt after applying the `regenesis_commit` block; refuse to
  propose, vote, or apply anything in this epoch above H.
- Treat sealed artifacts (snapshot, certificate, genesis) as derived,
  idempotent recomputations from local sealed state — a crash between
  seal and restart recovers by recomputing, never by peers.
- Boot the new epoch as a fresh engine at H+1 with the carried
  committed state and an empty evidence map.
