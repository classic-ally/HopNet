// RFC-CONSENSUS-001: validator membership policy — normative model.
//
// Models the validator set lifecycle: activation (batch seating under
// the posture rule), voluntary leave, unreachability vote-out, and
// readmission, over the composite AUTO quorum profile (majority below
// V_BFT, BFT at and above). Where this model and the prose spec
// (validator-membership.md) disagree, the model wins.
//
// Layer map (mirrors hopnet-storage/spec/storage_policy.qnt):
//   quorum_math       — pure closed forms; Rust backend only. Apalache
//                       configs get literal Q/FEQ tables instead
//                       (nonlinear integer division stays out of SMT).
//   math_guard        — complete-enumeration lemmas over v = 1..30 and
//                       drift guards (literal tables == closed forms).
//   membership_policy — the state machine: adversarial environment +
//                       commit actions (each commit = one height), and
//                       a deterministic policyTick for bounded
//                       GUAR-HONEST-SET checking.
//   evidence_policy   — per-attestor staleness machine (the freshness
//                       contract): wrongful-vote-out NEG, cert-channel
//                       POS, asymmetric-refresh RECOVERY block.
//   partition_policy  — two-block dynamic partition machine: quorum
//                       commits measured against the committing block.
//   configs           — small / seam / pinned / neg_* instances plus
//                       scripted witness and NEG runs.
//
// Model-found spec corrections (carried into the prose in Q5):
//   1. Crash-neutral profile crossings are {4, 5, 7}, not 7 alone.
//      V_BFT = 7 is the unique crossing that is BOTH crash-neutral AND
//      Byzantine-meaningful (f_eq >= 2). See seamLemmaTest.
//   2. The per-batch exposure ceiling (e <= H at seating) does not
//      compose: two sequential legal exposed batches can jointly die
//      into a stall (reachable at v = 10 BFT, see
//      ceilingCompositionNegTest). The load-bearing form is the
//      proven-quorum rule: members are UNPROVEN until they survive
//      P_PROVE ticks in seat, and the live PROVEN seated members must
//      form a quorum at all times — seating is refused if it would
//      make unproven members load-bearing. Per-batch e <= H is the
//      special case with no outstanding unproven members.

// ============================================================
// Pure quorum math. Rust backend only — Apalache never sees the
// integer division here; verify configs read literal tables checked
// against these closed forms by math_guard.
// ============================================================
module quorum_math {
  // Quorum profile modes.
  pure val MAJ = 0
  pure val BFT = 1
  pure val AUTO = 2

  // AUTO switch point (spec Constants: V_bft).
  pure val V_BFT = 7

  pure def qMaj(v: int): int = v / 2 + 1
  pure def qBft(v: int): int = (2 * v) / 3 + 1

  // Composite with a parameterized switch point (for the seam lemmas).
  pure def quorumAt(vbft: int, v: int): int =
    if (v >= vbft) qBft(v) else qMaj(v)

  pure def profileAt(mode: int, v: int): int =
    if (mode == AUTO) (if (v >= V_BFT) BFT else MAJ) else mode

  pure def quorum(mode: int, v: int): int =
    if (profileAt(mode, v) == BFT) qBft(v) else qMaj(v)

  // Equivocation tolerance: Tendermint's f under BFT, none under
  // majority (its safety proof assumes no equivocation).
  pure def fEq(mode: int, v: int): int =
    if (profileAt(mode, v) == BFT) (v - 1) / 3 else 0

  // Crash tolerance: failures survivable before quorum loss.
  pure def tol(mode: int, v: int): int = v - quorum(mode, v)

  // Headroom delta of seating b live members at set size v.
  pure def dH(mode: int, v: int, b: int): int =
    b - (quorum(mode, v + b) - quorum(mode, v))

  // Borrowed quorum: the portion of a seating not covered by its own
  // headroom gain.
  pure def exposure(mode: int, v: int, b: int): int = b - dH(mode, v, b)

  // Table builders for injection into Apalache configs.
  pure def buildQTable(mode: int, lo: int, hi: int): int -> int =
    lo.to(hi).mapBy(v => quorum(mode, v))
  pure def buildFeqTable(mode: int, lo: int, hi: int): int -> int =
    lo.to(hi).mapBy(v => fEq(mode, v))
}

// ============================================================
// Complete-enumeration lemmas + drift guards. Rust backend.
// Every closed-form claim quoted in the prose spec is asserted here
// over the full 1..30 range, for all three quorum modes.
// ============================================================
module math_guard {
  import quorum_math.*

  // Literal tables used by the Apalache configs below. Domain 1..14
  // covers every reachable |seated| + batch there (REGISTERED <= 9,
  // |B| <= B_MAX = 5). v = 0 is unreachable (INV-FLOOR).
  pure val Q_AUTO_TABLE: int -> int = Map(
    1 -> 1, 2 -> 2, 3 -> 2, 4 -> 3, 5 -> 3, 6 -> 4,
    7 -> 5, 8 -> 6, 9 -> 7, 10 -> 7, 11 -> 8, 12 -> 9, 13 -> 9, 14 -> 10
  )
  pure val Q_MAJ_TABLE: int -> int = Map(
    1 -> 1, 2 -> 2, 3 -> 2, 4 -> 3, 5 -> 3, 6 -> 4,
    7 -> 4, 8 -> 5, 9 -> 5, 10 -> 6, 11 -> 6, 12 -> 7, 13 -> 7, 14 -> 8
  )
  pure val Q_BFT_TABLE: int -> int = Map(
    1 -> 1, 2 -> 2, 3 -> 3, 4 -> 3, 5 -> 4, 6 -> 5,
    7 -> 5, 8 -> 6, 9 -> 7, 10 -> 7, 11 -> 8, 12 -> 9, 13 -> 9, 14 -> 10
  )
  pure val FEQ_AUTO_TABLE: int -> int = Map(
    1 -> 0, 2 -> 0, 3 -> 0, 4 -> 0, 5 -> 0, 6 -> 0,
    7 -> 2, 8 -> 2, 9 -> 2, 10 -> 3, 11 -> 3, 12 -> 3, 13 -> 4, 14 -> 4
  )
  pure val FEQ_MAJ_TABLE: int -> int = Map(
    1 -> 0, 2 -> 0, 3 -> 0, 4 -> 0, 5 -> 0, 6 -> 0,
    7 -> 0, 8 -> 0, 9 -> 0, 10 -> 0, 11 -> 0, 12 -> 0, 13 -> 0, 14 -> 0
  )
  pure val FEQ_BFT_TABLE: int -> int = Map(
    1 -> 0, 2 -> 0, 3 -> 0, 4 -> 1, 5 -> 1, 6 -> 1,
    7 -> 2, 8 -> 2, 9 -> 2, 10 -> 3, 11 -> 3, 12 -> 3, 13 -> 4, 14 -> 4
  )

  pure val MODES = Set(MAJ, BFT, AUTO)

  var ok: bool
  action init = ok' = true

  // DRIFT GUARD: literal tables == closed forms over their domain.
  run tableDriftGuardTest =
    init.expect(1.to(14).forall(v => and {
      Q_AUTO_TABLE.get(v) == quorum(AUTO, v),
      Q_MAJ_TABLE.get(v) == quorum(MAJ, v),
      Q_BFT_TABLE.get(v) == quorum(BFT, v),
      FEQ_AUTO_TABLE.get(v) == fEq(AUTO, v),
      FEQ_MAJ_TABLE.get(v) == fEq(MAJ, v),
      FEQ_BFT_TABLE.get(v) == fEq(BFT, v),
    }))

  // PARITY: single-seat gain steps are exactly the claimed mod
  // patterns — BFT gains iff v+1 == 1 (mod 3), majority iff v+1 odd.
  run parityLemmaTest =
    init.expect(1.to(30).forall(v => and {
      dH(BFT, v, 1) == (if ((v + 1) % 3 == 1) 1 else 0),
      dH(MAJ, v, 1) == (if ((v + 1) % 2 == 1) 1 else 0),
      // dH is only ever 0 or 1 for single seats, in every mode.
      MODES.forall(m => dH(m, v, 1) == 0 or dH(m, v, 1) == 1),
    }))

  // COMPLEMENTARY PARITY: removing one (dark) member gains headroom
  // exactly where admission at v-1 is lateral.
  run complementaryParityLemmaTest =
    init.expect(2.to(30).forall(v =>
      MODES.forall(m =>
        ((quorum(m, v) - quorum(m, v - 1)) == 1) == (dH(m, v - 1, 1) == 0))))

  // BATCH: any batch of three gains exactly one under pure BFT, any
  // batch of two under majority; dH is monotone non-decreasing in
  // batch size; and on the AUTO composite a gaining batch of size
  // <= 5 exists everywhere (licenses the state machine's B_MAX = 5).
  run batchLemmaTest =
    init.expect(and {
      1.to(27).forall(v => dH(BFT, v, 3) == 1),
      1.to(28).forall(v => dH(MAJ, v, 2) == 1),
      1.to(25).forall(v => 1.to(4).forall(b =>
        MODES.forall(m => dH(m, v, b + 1) >= dH(m, v, b)))),
      1.to(25).forall(v => 1.to(5).exists(b => dH(AUTO, v, b) >= 1)),
    })

  // SEAM: crash-neutral crossings of the majority->BFT switch are
  // exactly {4, 5, 7} (a switch at vbft is crash-neutral iff
  // tol_maj(vbft - 1) == tol_bft(vbft)); V_BFT = 7 is the unique
  // crossing that is ALSO Byzantine-meaningful (f_eq >= 2) — the
  // corrected form of the spec's "doubly principled" claim. At 4 and
  // 5 the Byzantine budget is 1: the first ordinary crash spends it.
  run seamLemmaTest =
    init.expect(and {
      4.to(30).forall(vbft =>
        ((vbft - 1 - qMaj(vbft - 1)) == (vbft - qBft(vbft)))
          == Set(4, 5, 7).contains(vbft)),
      4.to(30).forall(vbft =>
        (Set(4, 5, 7).contains(vbft) and (vbft - 1) / 3 >= 2)
          == (vbft == 7)),
      fEq(AUTO, 7) == 2,
    })

  // MONOTONE: crash tolerance never decreases with growth on the AUTO
  // composite, and the literal sequence for v = 2..10 is the spec's
  // 0,1,1,2,2,2,2,2,3 (majority-region identity tol = ceil(v/2) - 1
  // asserted via 2*tol vs v parity, avoiding ceil).
  run monotoneLemmaTest =
    init.expect(and {
      2.to(30).forall(v => tol(AUTO, v) >= tol(AUTO, v - 1)),
      tol(AUTO, 2) == 0, tol(AUTO, 3) == 1, tol(AUTO, 4) == 1,
      tol(AUTO, 5) == 2, tol(AUTO, 6) == 2, tol(AUTO, 7) == 2,
      tol(AUTO, 8) == 2, tol(AUTO, 9) == 2, tol(AUTO, 10) == 3,
      // Majority-region identity: tol(MAJ, v) == ceil(v/2) - 1.
      1.to(30).forall(v => tol(MAJ, v) == (v + 1) / 2 - 1),
    })

  // POSTURE: the only lateral single seat that gains equivocation
  // tolerance on the AUTO composite is the V_BFT crossing. Inside
  // pure BFT, f_eq-gaining steps coincide with H-gaining steps, so
  // the posture clause admits no lateral there either.
  run postureLemmaTest =
    init.expect(and {
      1.to(29).forall(v =>
        (dH(AUTO, v, 1) == 0 and fEq(AUTO, v + 1) > fEq(AUTO, v))
          == (v + 1 == V_BFT)),
      1.to(29).forall(v =>
        (fEq(BFT, v + 1) > fEq(BFT, v)) implies (dH(BFT, v, 1) == 1)),
    })

  // EXPOSURE-IDENTITY: exposure is never negative (quorum functions
  // are monotone), and gain-step single seats are exposure-free —
  // quorum is flat across them, so the seat's immediate death returns
  // headroom exactly to its prior value (the free-option lemma).
  run exposureLemmaTest =
    init.expect(and {
      1.to(25).forall(v => 1.to(5).forall(b =>
        MODES.forall(m => exposure(m, v, b) >= 0))),
      1.to(29).forall(v => MODES.forall(m =>
        (dH(m, v, 1) == 1) implies (quorum(m, v + 1) == quorum(m, v)))),
    })

  // DISJOINT-QUORUM: two disjoint subsets of a v-member set can never
  // both hold quorum (2q > v in every mode at every size) — partition
  // safety rests on this arithmetic, not on prose.
  run disjointQuorumLemmaTest =
    init.expect(1.to(30).forall(v =>
      MODES.forall(m => 2 * quorum(m, v) > v)))
}
