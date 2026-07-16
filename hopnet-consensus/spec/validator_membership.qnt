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
// Literal quorum/f_eq tables — the ONLY quorum arithmetic Apalache
// ever sees. Domain 1..14 covers every reachable |seated| + batch in
// the configs below (REGISTERED <= 10, |B| <= B_MAX = 5). v = 0 is
// unreachable (INV-FLOOR). math_guard asserts table == closed form.
// ============================================================
module q_tables {
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
}

// ============================================================
// Complete-enumeration lemmas + drift guards. Rust backend.
// Every closed-form claim quoted in the prose spec is asserted here
// over the full 1..30 range, for all three quorum modes.
// ============================================================
module math_guard {
  import quorum_math.*
  import q_tables.*

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

// ============================================================
// The membership state machine. Ground-truth evidence (perfect-
// evidence axiom: vote-out targets are genuinely dark — the evidence
// CONTRACT is checked separately in evidence_policy). Each commit
// action is one consensus height; the environment is adversarial.
//
// Ceiling modes (model-found spec correction 2):
//   0 = PROVEN-QUORUM (corrected): a seating may inflate quorum only
//       within the proven cushion — quorum(v+b) - quorum(v) <=
//       max(0, |alive ∩ proven| - quorum(v)), where a member is
//       proven after P_PROVE ticks in seat (genesis proven by fiat).
//       Self-accounting: outstanding unproven seatings shrink the
//       cushion through the quorum they already inflated.
//   1 = PER-BATCH (spec as originally written): e <= H at seating.
//       Does not compose — see ceilingCompositionNegTest.
//   2 = OFF (NEG configs only).
// ============================================================
module membership_policy {
  const REGISTERED: Set[int]
  const INIT_SEATED: Set[int]
  const Q_TABLE: int -> int
  const FEQ_TABLE: int -> int
  const T_CLIFF: int              // vote-out window floor (2 probe cycles)
  const T_FAST: int
  const T_LAZY: int               // also the darkFor cap
  const S_FLOOR: int
  const S_FULL: int               // brightFor cap
  const P_PROVE: int              // in-seat proving period (seatedFor cap)
  const B_MAX: int                // licensed by batchLemmaTest
  const CEILING_MODE: int
  const ENFORCE_SMIN: bool

  // Departure kinds (lastDeparture).
  pure val D_NONE = 0
  pure val D_VOLUNTARY = 1
  pure val D_VOTEDOUT = 2

  var seated: Set[int]
  var alive: Set[int]
  var darkFor: int -> int         // ticks not-alive; 0 while alive
  var brightFor: int -> int       // ticks continuously alive; 0 while dark
  var seatedFor: int -> int       // ticks in seat; 0 while unseated
  var lastDeparture: int -> int
  var harmedByPolicy: bool        // ghost: a policy commit stalled the mesh
  var ceilingHarm: bool           // ghost: a seating worsened the proven cushion deficit
  var calm: int                   // consecutive policyTicks without env interference

  // === Derived ===
  val vSize = seated.size()
  val liveSeated = alive.intersect(seated)
  def q(v: int): int = Q_TABLE.get(v)
  def feq(v: int): int = FEQ_TABLE.get(v)
  val stalled = liveSeated.size() < q(vSize)
  val headroom = liveSeated.size() - q(vSize)
  // Removal window by band (headroom schedule). The adversarial
  // machine guards only the FLOOR (T_CLIFF); the band schedule is the
  // deterministic policyTick's trigger (upper envelope).
  val window = if (headroom >= 2) T_LAZY else if (headroom == 1) T_FAST else T_CLIFF
  val proven = seated.filter(n => seatedFor.get(n) >= P_PROVE)
  val provenCushion = alive.intersect(proven).size() - q(vSize)
  val candidates = alive.exclude(seated)

  // === Guard vocabulary (also used by enabledness invariants) ===
  def voteOutEnabled(x: int): bool = and {
    seated.contains(x),
    not(alive.contains(x)),        // perfect-evidence axiom
    darkFor.get(x) >= T_CLIFF,
    not(stalled),
  }
  def leaveEnabled(x: int): bool = and {
    liveSeated.contains(x),
    vSize > 1,
    liveSeated.exclude(Set(x)).size() >= q(vSize - 1),
    not(stalled),
  }
  def batchDH(b: int): int = b - (q(vSize + b) - q(vSize))
  def postureOk(b: int): bool = or {
    batchDH(b) >= 1,
    batchDH(b) == 0 and feq(vSize + b) > feq(vSize),
  }
  // Proven-quorum ceiling with the zero-tolerance waiver: a mesh whose
  // set already stalls on any single death (tol(v) == 0, i.e. v <= 2
  // on the composite) has no tolerance to protect — growth from there
  // bets on the batch, and its upside strictly dominates (the
  // death-count-to-stall never decreases on posture-legal seatings;
  // see exposureLemmaTest). Full S_min still gates the evidence.
  def ceilingOk(b: int): bool =
    if (CEILING_MODE == 0) or {
      vSize - q(vSize) == 0,
      (q(vSize + b) - q(vSize)) <= (if (provenCushion > 0) provenCushion else 0),
    }
    else if (CEILING_MODE == 1) (b - batchDH(b)) <= headroom
    else true
  def reqSpan(n: int, exposed: bool): int =
    if (not(ENFORCE_SMIN)) 0
    else if (exposed) S_FULL
    else if (lastDeparture.get(n) == D_VOLUNTARY) 0
    else if (headroom <= 1) S_FLOOR
    else S_FULL
  def sminOk(bs: Set[int]): bool = {
    val exposed = (bs.size() - batchDH(bs.size())) > 0
    bs.forall(n => brightFor.get(n) >= reqSpan(n, exposed))
  }
  def seatEnabled(bs: Set[int]): bool = and {
    bs.size() > 0,
    bs.size() <= B_MAX,
    bs.subseteq(candidates),
    not(stalled),
    postureOk(bs.size()),
    ceilingOk(bs.size()),
    sminOk(bs),
  }

  // === Init: genesis set seated and proven, everyone alive+bright ===
  action init = all {
    seated' = INIT_SEATED,
    alive' = REGISTERED,
    darkFor' = REGISTERED.mapBy(n => 0),
    brightFor' = REGISTERED.mapBy(n => S_FULL),
    seatedFor' = REGISTERED.mapBy(n => if (INIT_SEATED.contains(n)) P_PROVE else 0),
    lastDeparture' = REGISTERED.mapBy(n => D_NONE),
    harmedByPolicy' = false,
    ceilingHarm' = false,
    calm' = 0,
  }

  // === Environment ===
  action envKillN(n: int): bool = all {
    alive.contains(n),
    alive' = alive.exclude(Set(n)),
    brightFor' = brightFor.set(n, 0),
    seated' = seated, darkFor' = darkFor, seatedFor' = seatedFor,
    lastDeparture' = lastDeparture, harmedByPolicy' = harmedByPolicy,
    ceilingHarm' = ceilingHarm, calm' = 0,
  }
  action envKill = all {
    alive.size() > 0,
    { nondet n = alive.oneOf()
      envKillN(n) }
  }

  action envReviveN(n: int): bool = all {
    REGISTERED.contains(n),
    not(alive.contains(n)),
    alive' = alive.union(Set(n)),
    darkFor' = darkFor.set(n, 0),
    brightFor' = brightFor.set(n, 0),   // span restarts from zero
    seated' = seated, seatedFor' = seatedFor,
    lastDeparture' = lastDeparture, harmedByPolicy' = harmedByPolicy,
    ceilingHarm' = ceilingHarm, calm' = 0,
  }
  action envRevive = all {
    REGISTERED.exclude(alive).size() > 0,
    { nondet n = REGISTERED.exclude(alive).oneOf()
      envReviveN(n) }
  }

  // One probe cycle passes. Commits are sub-tick events (a height is
  // much faster than a probe cycle), so clocks advance only here.
  action envTick = all {
    darkFor' = REGISTERED.mapBy(n =>
      if (alive.contains(n)) 0
      else if (darkFor.get(n) < T_LAZY) darkFor.get(n) + 1 else T_LAZY),
    brightFor' = REGISTERED.mapBy(n =>
      if (not(alive.contains(n))) 0
      else if (brightFor.get(n) < S_FULL) brightFor.get(n) + 1 else S_FULL),
    seatedFor' = REGISTERED.mapBy(n =>
      if (not(seated.contains(n))) 0
      else if (seatedFor.get(n) < P_PROVE) seatedFor.get(n) + 1 else P_PROVE),
    seated' = seated, alive' = alive, lastDeparture' = lastDeparture,
    harmedByPolicy' = harmedByPolicy, ceilingHarm' = ceilingHarm, calm' = calm,
  }

  // === Policy commits (each = one consensus height) ===
  action commitVoteOutN(x: int): bool = all {
    voteOutEnabled(x),
    seated' = seated.exclude(Set(x)),
    seatedFor' = seatedFor.set(x, 0),
    lastDeparture' = lastDeparture.set(x, D_VOTEDOUT),
    harmedByPolicy' = (harmedByPolicy or
      alive.intersect(seated.exclude(Set(x))).size() < q(vSize - 1)),
    alive' = alive, darkFor' = darkFor, brightFor' = brightFor,
    ceilingHarm' = ceilingHarm, calm' = calm,
  }
  action commitVoteOut = all {
    seated.exists(x => voteOutEnabled(x)),
    { nondet x = seated.filter(x => voteOutEnabled(x)).oneOf()
      commitVoteOutN(x) }
  }

  action commitLeaveN(x: int): bool = all {
    leaveEnabled(x),
    seated' = seated.exclude(Set(x)),
    seatedFor' = seatedFor.set(x, 0),
    lastDeparture' = lastDeparture.set(x, D_VOLUNTARY),
    harmedByPolicy' = (harmedByPolicy or
      alive.intersect(seated.exclude(Set(x))).size() < q(vSize - 1)),
    alive' = alive, darkFor' = darkFor, brightFor' = brightFor,
    ceilingHarm' = ceilingHarm, calm' = calm,
  }
  action commitLeave = all {
    seated.exists(x => leaveEnabled(x)),
    { nondet x = seated.filter(x => leaveEnabled(x)).oneOf()
      commitLeaveN(x) }
  }

  action commitSeatBatchB(bs: Set[int]): bool = all {
    seatEnabled(bs),
    seated' = seated.union(bs),
    seatedFor' = seatedFor,          // new members start unproven (0 already)
    harmedByPolicy' = (harmedByPolicy or
      alive.intersect(seated.union(bs)).size() < q(vSize + bs.size())),
    // Harm only where the ceiling had tolerance to protect — the
    // zero-tolerance waiver (growth from v <= 2) is not harm.
    ceilingHarm' = (ceilingHarm or {
      val cushionAfter = alive.intersect(proven).size() - q(vSize + bs.size())
      val floorBefore = if (provenCushion < 0) provenCushion else 0
      vSize - q(vSize) > 0 and cushionAfter < floorBefore
    }),
    alive' = alive, darkFor' = darkFor, brightFor' = brightFor,
    lastDeparture' = lastDeparture, calm' = calm,
  }
  action commitSeatBatch = all {
    candidates.size() > 0,
    { nondet bs = candidates.powerset().filter(s =>
        s.size() > 0 and s.size() <= B_MAX).oneOf()
      commitSeatBatchB(bs) }
  }

  // === Deterministic scheduler (GUAR-HONEST-SET carrier) ===
  // One probe cycle + at most one policy action, priority: longest-dark
  // vote-out at the BAND window (upper envelope, not just the floor) >
  // best single seating > noop. calm counts undisturbed ticks.
  val removableNow = seated.filter(x =>
    not(alive.contains(x)) and darkFor.get(x) >= window and not(stalled))
  val mandatorySeats = candidates.filter(x =>
    brightFor.get(x) >= S_FULL and seatEnabled(Set(x)) and batchDH(1) >= 1)
  pure def leastOf(s: Set[int]): int = s.fold(1000000, (a, x) => if (x < a) x else a)
  val longestDark = {
    val worst = removableNow.fold(0, (a, x) => if (darkFor.get(x) > a) darkFor.get(x) else a)
    leastOf(removableNow.filter(x => darkFor.get(x) == worst))
  }

  action policyTick = all {
    // clocks advance exactly as envTick
    darkFor' = REGISTERED.mapBy(n =>
      if (alive.contains(n)) 0
      else if (darkFor.get(n) < T_LAZY) darkFor.get(n) + 1 else T_LAZY),
    brightFor' = REGISTERED.mapBy(n =>
      if (not(alive.contains(n))) 0
      else if (brightFor.get(n) < S_FULL) brightFor.get(n) + 1 else S_FULL),
    seatedFor' = REGISTERED.mapBy(n =>
      if (not(seated.contains(n))) 0
      else if (seatedFor.get(n) < P_PROVE) seatedFor.get(n) + 1 else P_PROVE),
    alive' = alive, calm' = calm + 1,
    if (removableNow.size() > 0) all {
      seated' = seated.exclude(Set(longestDark)),
      lastDeparture' = lastDeparture.set(longestDark, D_VOTEDOUT),
      harmedByPolicy' = (harmedByPolicy or
        alive.intersect(seated.exclude(Set(longestDark))).size() < q(vSize - 1)),
      ceilingHarm' = ceilingHarm,
    } else if (mandatorySeats.size() > 0) all {
      seated' = seated.union(Set(leastOf(mandatorySeats))),
      lastDeparture' = lastDeparture,
      harmedByPolicy' = (harmedByPolicy or
        alive.intersect(seated.union(Set(leastOf(mandatorySeats)))).size()
          < q(vSize + 1)),
      ceilingHarm' = ceilingHarm,
    } else all {
      seated' = seated,
      lastDeparture' = lastDeparture,
      harmedByPolicy' = harmedByPolicy,
      ceilingHarm' = ceilingHarm,
    }
  }

  // === Steps ===
  action step = any {
    envKill, envRevive, envTick, commitVoteOut, commitLeave, commitSeatBatch
  }
  action stepSched = any { envKill, envRevive, policyTick }

  // === Invariants ===
  val invNoHarm = not(harmedByPolicy)
  val invCeiling = not(ceilingHarm)
  val invFloor = vSize >= 1 and seated.subseteq(REGISTERED)
  // NO-EXILE (safety half): whenever the mesh is live, every fully
  // bright candidate whose single seating is a strict gain is seatable.
  val invNoExile = stalled or
    candidates.filter(x => brightFor.get(x) >= S_FULL).forall(x =>
      (batchDH(1) >= 1) implies seatEnabled(Set(x)))
  // GUAR-HONEST-SET (sched config): sustained calm leaves no removable
  // dark validator in the set. Bound = worst window plus one removal
  // per tick for every possible dark member.
  val invHonestSet = calm < T_LAZY + REGISTERED.size() or removableNow.size() == 0
  val safetyInv = invNoHarm and invCeiling and invFloor and invNoExile

  // Well-formedness (auxiliary strengthening for inductive checking).
  val wellFormed = and {
    seated.subseteq(REGISTERED),
    alive.subseteq(REGISTERED),
    vSize >= 1,
    REGISTERED.forall(n => and {
      darkFor.get(n) >= 0, darkFor.get(n) <= T_LAZY,
      brightFor.get(n) >= 0, brightFor.get(n) <= S_FULL,
      seatedFor.get(n) >= 0, seatedFor.get(n) <= P_PROVE,
      alive.contains(n) implies darkFor.get(n) == 0,
      not(alive.contains(n)) implies brightFor.get(n) == 0,
      not(seated.contains(n)) implies seatedFor.get(n) == 0,
    }),
  }
}

// ============================================================
// Configs + witness/NEG runs. Scaled constants (probe cycle = one
// tick): T_CLIFF=2 < T_FAST=3 < T_LAZY=4 < S_FULL=5, S_FLOOR=1,
// P_PROVE=5 — every load-bearing ordering from the spec's Constants
// section preserved (asserted by constantsGuardTest in each config).
// ============================================================

// The money config: REGISTERED 1..9 spans the whole seam (majority
// v<7, BFT v>=7) with two pool nodes.
module cfg_seam {
  import q_tables.*
  import membership_policy(
    REGISTERED = 1.to(9), INIT_SEATED = 1.to(7),
    Q_TABLE = Q_AUTO_TABLE, FEQ_TABLE = FEQ_AUTO_TABLE,
    T_CLIFF = 2, T_FAST = 3, T_LAZY = 4, S_FLOOR = 1, S_FULL = 5,
    P_PROVE = 5, B_MAX = 5, CEILING_MODE = 0, ENFORCE_SMIN = true
  ).*

  run constantsGuardTest =
    init.expect(and {
      T_CLIFF == 2, T_CLIFF < T_FAST, T_FAST < T_LAZY, T_LAZY < S_FULL,
      S_FLOOR == 1, S_FLOOR < S_FULL,
      // caps hold the windows: darkFor cap == T_LAZY, brightFor cap == S_FULL
      wellFormed,
    })

  // WITNESS (seam round-trip): fault at v=7 sheds BFT gracefully;
  // the healed node re-proves and the posture-improving crossing
  // restores BFT. No human in the loop.
  run seamRoundTripTest =
    init.then(envKillN(7)).then(2.reps(_ => envTick))
      .then(commitVoteOutN(7))
      // v=6: AUTO composite is majority here — profile shed
      .expect(vSize == 6 and q(vSize) == 4 and headroom == 2 and invFloor)
      .then(envReviveN(7)).then(5.reps(_ => envTick))
      // bright span re-proven; crossing seat = lateral + f_eq gain
      .then(commitSeatBatchB(Set(7)))
      .expect(and {
        vSize == 7, q(vSize) == 5, feq(vSize) == 2, headroom == 2,
        invNoHarm, invCeiling, invFloor,
      })

  // WITNESS (RECOVERY): two kills, both voted out once past window,
  // headroom restored to the survivor set's full budget.
  run recoveryTest =
    init.then(envKillN(6)).then(envKillN(7))
      .then(4.reps(_ => envTick))
      .then(commitVoteOut).then(commitVoteOut)
      .expect(vSize == 5 and q(vSize) == 3 and headroom == 2
        and invNoHarm and invCeiling)

  // WITNESS (multi-kill same probe cycle — switch reboot): both dark
  // with identical spans; sequential one-per-height removals recover.
  run multiKillTest =
    init.then(envKillN(6)).then(envKillN(7))
      .then(2.reps(_ => envTick))
      .then(commitVoteOutN(6)).then(commitVoteOutN(7))
      .expect(vSize == 5 and headroom == 2 and invNoHarm)

  // WITNESS (slowest-attestor delay): adversarially late commits are
  // delays, not blocks — removal still fires long past every window.
  run slowAttestorDelayTest =
    init.then(envKillN(7)).then(12.reps(_ => envTick))
      .then(commitVoteOutN(7))
      .expect(vSize == 6 and invNoHarm)

  // WITNESS (off-by-one): one missed probe never triggers — the
  // vote-out is disabled after a single tick in EVERY band (window
  // floor T_CLIFF = 2 requires two consecutive misses).
  run offByOneTest =
    init.then(envKillN(7)).then(envTick)
      .expect(not(voteOutEnabled(7)))

  // WITNESS (exposed return is NOT exempt): a leaver returning across
  // the V_bft crossing (always exposed) must re-prove a full span —
  // graceful exit exempts only exposure-free seatings.
  run leaveReturnExposedTest =
    init.then(commitLeaveN(7))
      .expect(vSize == 6 and lastDeparture.get(7) == D_VOLUNTARY)
      .then(envKillN(7)).then(envReviveN(7)).then(envTick)
      .expect(not(seatEnabled(Set(7))))         // crossing: full S_min due
      .then(4.reps(_ => envTick))               // span reaches S_FULL
      .then(commitSeatBatchB(Set(7)))
      .expect(vSize == 7 and invNoHarm and invCeiling)
}

// Small majority-region config: free-option and leave guards.
module cfg_small {
  import q_tables.*
  import membership_policy(
    REGISTERED = 1.to(5), INIT_SEATED = 1.to(4),
    Q_TABLE = Q_AUTO_TABLE, FEQ_TABLE = FEQ_AUTO_TABLE,
    T_CLIFF = 2, T_FAST = 3, T_LAZY = 4, S_FLOOR = 1, S_FULL = 5,
    P_PROVE = 5, B_MAX = 5, CEILING_MODE = 0, ENFORCE_SMIN = true
  ).*

  // WITNESS (free option): a gain-step seat that dies at once returns
  // headroom EXACTLY to its prior value — quorum was flat.
  run freeOptionTest =
    init.expect(headroom == 1 and q(4) == 3)
      .then(commitSeatBatchB(Set(5)))
      .expect(vSize == 5 and q(5) == 3 and headroom == 2)
      .then(envKillN(5))
      .expect(headroom == 1 and invNoHarm and invCeiling)

  // WITNESS (leave guard): with one member dark, the leave that would
  // stall the mesh is refused; vote-out of the dark member first
  // re-opens it.
  run leaveGuardTest =
    init.then(envKillN(4)).then(2.reps(_ => envTick))
      // live 3 = q(4) exactly: any live leave would leave 2 < q(3)=2?
      // q(3) = 2, live' = 2 >= 2 — actually legal; kill one more first
      .then(envKillN(3)).then(envTick)
      // live 2 < q(4)=3: stalled — nothing commits
      .expect(stalled and not(leaveEnabled(1)) and not(voteOutEnabled(3)))
}

// Solo and duo dead-ends: the policy's tiny-v behavior is confirmed,
// not assumed.
module cfg_solo {
  import q_tables.*
  import membership_policy(
    REGISTERED = 1.to(3), INIT_SEATED = Set(1),
    Q_TABLE = Q_AUTO_TABLE, FEQ_TABLE = FEQ_AUTO_TABLE,
    T_CLIFF = 2, T_FAST = 3, T_LAZY = 4, S_FLOOR = 1, S_FULL = 5,
    P_PROVE = 5, B_MAX = 5, CEILING_MODE = 0, ENFORCE_SMIN = true
  ).*

  // WITNESS: solo mesh — the last member cannot leave (floor); a
  // single second seat is refused (1->2 is lateral with exposure:
  // quorum 1->2 means both must live, strictly worse); the batch of
  // two gains and is accepted (1->3).
  run soloGrowthTest =
    init.expect(and {
      vSize == 1,
      not(leaveEnabled(1)),
      not(seatEnabled(Set(2))),        // lateral, no f_eq gain: refused
      seatEnabled(Set(2, 3)),          // dH = 2 - (2-1) = 1: gain
    })
      .then(commitSeatBatchB(Set(2, 3)))
      .expect(vSize == 3 and headroom == 1 and invNoHarm and invCeiling)
}

module cfg_duo {
  import q_tables.*
  import membership_policy(
    REGISTERED = 1.to(3), INIT_SEATED = Set(1, 2),
    Q_TABLE = Q_AUTO_TABLE, FEQ_TABLE = FEQ_AUTO_TABLE,
    T_CLIFF = 2, T_FAST = 3, T_LAZY = 4, S_FLOOR = 1, S_FULL = 5,
    P_PROVE = 5, B_MAX = 5, CEILING_MODE = 0, ENFORCE_SMIN = true
  ).*

  // WITNESS: v=2 with one dark is a confirmed dead-end — stalled,
  // nothing commits (including the vote-out that would fix it), and
  // the invariants do not false-fail: the stall is env-caused.
  run duoDeadEndTest =
    init.then(envKillN(2)).then(4.reps(_ => envTick))
      .expect(and {
        stalled,
        not(voteOutEnabled(2)),
        invNoHarm, invCeiling, invFloor,
      })
}

// Mass-dark at v=10: the worked payoff table from the spec, with
// every intermediate headroom value asserted.
module cfg_ten {
  import q_tables.*
  import membership_policy(
    REGISTERED = 1.to(10), INIT_SEATED = 1.to(10),
    Q_TABLE = Q_AUTO_TABLE, FEQ_TABLE = FEQ_AUTO_TABLE,
    T_CLIFF = 2, T_FAST = 3, T_LAZY = 4, S_FLOOR = 1, S_FULL = 5,
    P_PROVE = 5, B_MAX = 5, CEILING_MODE = 0, ENFORCE_SMIN = true
  ).*

  run massDarkTest =
    init.then(envKillN(8)).then(envKillN(9)).then(envKillN(10))
      .then(2.reps(_ => envTick))
      .expect(vSize == 10 and q(10) == 7 and headroom == 0)  // at the cliff
      .then(commitVoteOutN(8))
      .expect(vSize == 9 and q(9) == 7 and headroom == 0)
      .then(commitVoteOutN(9))
      .expect(vSize == 8 and q(8) == 6 and headroom == 1)
      .then(commitVoteOutN(10))
      .expect(vSize == 7 and q(7) == 5 and headroom == 2
        and invNoHarm and invCeiling)

  // WITNESS (voluntary-leave exemption): a graceful leaver returns on
  // a gain step (exposure-free) with a one-tick span at comfortable
  // headroom — where a fresh candidate would owe the full span. The
  // spec's "graceful exit is not a fault" clause, exercised.
  run leaveExemptReturnTest =
    init.then(commitLeaveN(10))
      .expect(vSize == 9 and headroom == 2)     // q(9) = 7: comfortable
      .then(envKillN(10)).then(envReviveN(10)).then(envTick)
      // 9 -> 10 is a gain step (q flat at 7); span is 1 tick; a D_NONE
      // candidate would owe S_FULL at H = 2 — the leaver is exempt.
      .expect(brightFor.get(10) == 1 and seatEnabled(Set(10)))
      .then(commitSeatBatchB(Set(10)))
      .expect(vSize == 10 and headroom == 3 and invNoHarm and invCeiling)
}

// Tiny-BFT pinned: removal at 4->3 buys nothing (quorum stays 3) but
// still commits at H = 0 — set honesty over headroom.
module cfg_bft_pinned {
  import q_tables.*
  import membership_policy(
    REGISTERED = 1.to(5), INIT_SEATED = 1.to(4),
    Q_TABLE = Q_BFT_TABLE, FEQ_TABLE = FEQ_BFT_TABLE,
    T_CLIFF = 2, T_FAST = 3, T_LAZY = 4, S_FLOOR = 1, S_FULL = 5,
    P_PROVE = 5, B_MAX = 5, CEILING_MODE = 0, ENFORCE_SMIN = true
  ).*

  run tinyBftTest =
    init.then(envKillN(4)).then(2.reps(_ => envTick))
      .expect(headroom == 0 and q(4) == 3)
      .then(commitVoteOutN(4))
      .expect(vSize == 3 and q(3) == 3 and headroom == 0 and invNoHarm)
}

// Majority-pinned 7-node universe: the ceiling POS witness and the
// proven-quorum rule's refusal/unlock pair.
module cfg_maj7 {
  import q_tables.*
  import membership_policy(
    REGISTERED = 1.to(7), INIT_SEATED = 1.to(3),
    Q_TABLE = Q_MAJ_TABLE, FEQ_TABLE = FEQ_MAJ_TABLE,
    T_CLIFF = 2, T_FAST = 3, T_LAZY = 4, S_FLOOR = 1, S_FULL = 5,
    P_PROVE = 5, B_MAX = 5, CEILING_MODE = 0, ENFORCE_SMIN = true
  ).*

  // WITNESS (ceiling POS): a maximally exposed legal batch dies whole
  // the instant it seats — the mesh survives (kill-after-seat
  // interleaving exists; catch-up laggardness is dominated by this).
  run ceilingPosTest =
    init.expect(provenCushion == 1)
      .then(commitSeatBatchB(Set(4, 5)))       // inflation 1 <= cushion 1
      .expect(vSize == 5 and q(5) == 3)
      .then(envKillN(4)).then(envKillN(5))     // whole batch dies at once
      .expect(not(stalled) and invNoHarm and invCeiling)

  // WITNESS (composition refused, then unlocked): a second exposed
  // batch stacked on an unproven first is refused by the proven-quorum
  // rule; once the first batch proves in seat, it unlocks.
  run compositionRefusedThenProvenTest =
    init.then(commitSeatBatchB(Set(4, 5)))
      .expect(not(seatEnabled(Set(6, 7))))     // cushion spent: refused
      .then(5.reps(_ => envTick))              // first batch proves
      .expect(seatEnabled(Set(6, 7)))
      .then(commitSeatBatchB(Set(6, 7)))
      .expect(vSize == 7 and invNoHarm and invCeiling)
}

// NEG (spec correction 2): under the ORIGINAL per-batch ceiling the
// same stacking is legal and the joint death stalls the mesh — the
// per-batch rule does not compose.
module cfg_maj7_perbatch {
  import q_tables.*
  import membership_policy(
    REGISTERED = 1.to(7), INIT_SEATED = 1.to(3),
    Q_TABLE = Q_MAJ_TABLE, FEQ_TABLE = FEQ_MAJ_TABLE,
    T_CLIFF = 2, T_FAST = 3, T_LAZY = 4, S_FLOOR = 1, S_FULL = 5,
    P_PROVE = 5, B_MAX = 5, CEILING_MODE = 1, ENFORCE_SMIN = true
  ).*

  run ceilingCompositionNegTest =
    init.then(commitSeatBatchB(Set(4, 5)))     // e=1 <= H=1: legal
      .then(commitSeatBatchB(Set(6, 7)))       // e=1 <= H=2: legal per-batch
      .expect(vSize == 7 and q(7) == 4)
      .then(envKillN(4)).then(envKillN(5)).then(envKillN(6)).then(envKillN(7))
      .expect(stalled)                          // live 3 < q(7) = 4
}

// NEG (ceiling off): the cliff crossing the ceiling refuses is legal
// without it, and its death stalls the mesh. The exposure ceiling is
// load-bearing; S_min alone cannot block this trace (the candidate is
// fully bright), which reformulates the spec's NEG (a) — see Evidence.
module cfg_neg_ceiling {
  import q_tables.*
  import membership_policy(
    REGISTERED = 1.to(7), INIT_SEATED = 1.to(6),
    Q_TABLE = Q_AUTO_TABLE, FEQ_TABLE = FEQ_AUTO_TABLE,
    T_CLIFF = 2, T_FAST = 3, T_LAZY = 4, S_FLOOR = 1, S_FULL = 5,
    P_PROVE = 5, B_MAX = 5, CEILING_MODE = 2, ENFORCE_SMIN = true
  ).*

  run negCeilingCliffCrossingTest =
    init.then(envKillN(5)).then(envKillN(6)).then(2.reps(_ => envTick))
      .expect(headroom == 0)                    // live 4 = q(6) = 4: cliff
      .then(commitSeatBatchB(Set(7)))           // crossing, ceiling OFF
      .expect(vSize == 7 and q(7) == 5 and headroom == 0)
      .then(envKillN(7))                        // the new seat dies
      .expect(stalled and harmedByPolicy == false and ceilingHarm)
}

// Ceiling ON at the same cliff: the crossing is refused — the theorem,
// not advice.
module cfg_cliff_guarded {
  import q_tables.*
  import membership_policy(
    REGISTERED = 1.to(7), INIT_SEATED = 1.to(6),
    Q_TABLE = Q_AUTO_TABLE, FEQ_TABLE = FEQ_AUTO_TABLE,
    T_CLIFF = 2, T_FAST = 3, T_LAZY = 4, S_FLOOR = 1, S_FULL = 5,
    P_PROVE = 5, B_MAX = 5, CEILING_MODE = 0, ENFORCE_SMIN = true
  ).*

  run cliffCrossingRefusedTest =
    init.then(envKillN(5)).then(envKillN(6)).then(2.reps(_ => envTick))
      .expect(headroom == 0 and not(seatEnabled(Set(7))))
}

// NEG (fairness bound is tight): kills arriving faster than the
// vote-out window can never be caught — the mesh stalls with the
// removal machinery healthy. No policy survives this schedule.
module cfg_neg_fairness {
  import q_tables.*
  import membership_policy(
    REGISTERED = 1.to(5), INIT_SEATED = 1.to(5),
    Q_TABLE = Q_AUTO_TABLE, FEQ_TABLE = FEQ_AUTO_TABLE,
    T_CLIFF = 2, T_FAST = 3, T_LAZY = 4, S_FLOOR = 1, S_FULL = 5,
    P_PROVE = 5, B_MAX = 5, CEILING_MODE = 0, ENFORCE_SMIN = true
  ).*

  run negFairnessTest =
    init.then(envKillN(3)).then(envTick)
      .then(envKillN(4)).then(envTick)
      .then(envKillN(5))
      // three kills one probe cycle apart outrun every window: the
      // mesh stalls with the removal machinery healthy and blameless.
      .expect(stalled and invNoHarm and invCeiling and invFloor)
}
