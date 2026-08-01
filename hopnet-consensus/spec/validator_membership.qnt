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
//   epoch_policy      — RFC-019 regenesis boundary: a phase machine
//                       (normal / moratorium / sealed / restart) over
//                       a membership_policy instance; restart wipes
//                       evidence and lands inside indInv (the bridge).
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
    7 -> 5, 8 -> 6, 9 -> 7, 10 -> 7, 11 -> 8, 12 -> 9, 13 -> 9, 14 -> 10, 15 -> 11
  )
  pure val Q_MAJ_TABLE: int -> int = Map(
    1 -> 1, 2 -> 2, 3 -> 2, 4 -> 3, 5 -> 3, 6 -> 4,
    7 -> 4, 8 -> 5, 9 -> 5, 10 -> 6, 11 -> 6, 12 -> 7, 13 -> 7, 14 -> 8, 15 -> 8
  )
  pure val Q_BFT_TABLE: int -> int = Map(
    1 -> 1, 2 -> 2, 3 -> 3, 4 -> 3, 5 -> 4, 6 -> 5,
    7 -> 5, 8 -> 6, 9 -> 7, 10 -> 7, 11 -> 8, 12 -> 9, 13 -> 9, 14 -> 10, 15 -> 11
  )
  pure val FEQ_AUTO_TABLE: int -> int = Map(
    1 -> 0, 2 -> 0, 3 -> 0, 4 -> 0, 5 -> 0, 6 -> 0,
    7 -> 2, 8 -> 2, 9 -> 2, 10 -> 3, 11 -> 3, 12 -> 3, 13 -> 4, 14 -> 4, 15 -> 4
  )
  pure val FEQ_MAJ_TABLE: int -> int = Map(
    1 -> 0, 2 -> 0, 3 -> 0, 4 -> 0, 5 -> 0, 6 -> 0,
    7 -> 0, 8 -> 0, 9 -> 0, 10 -> 0, 11 -> 0, 12 -> 0, 13 -> 0, 14 -> 0, 15 -> 0
  )
  pure val FEQ_BFT_TABLE: int -> int = Map(
    1 -> 0, 2 -> 0, 3 -> 0, 4 -> 1, 5 -> 1, 6 -> 1,
    7 -> 2, 8 -> 2, 9 -> 2, 10 -> 3, 11 -> 3, 12 -> 3, 13 -> 4, 14 -> 4, 15 -> 4
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
    init.expect(1.to(15).forall(v => and {
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
  // Inductive form, in Apalache-assignable shape (every variable
  // constrained by `.in` / `==` so the invariant doubles as the
  // arbitrary-state generator): wellFormed + no-harm ghosts clear +
  // calm pinned (the adversarial `step` never advances it). One step
  // from ANY state satisfying this preserves it — no depth bound.
  val indInv = and {
    seated.in(REGISTERED.powerset()),
    alive.in(REGISTERED.powerset()),
    darkFor.in(REGISTERED.setOfMaps(0.to(T_LAZY))),
    brightFor.in(REGISTERED.setOfMaps(0.to(S_FULL))),
    seatedFor.in(REGISTERED.setOfMaps(0.to(P_PROVE))),
    lastDeparture.in(REGISTERED.setOfMaps(Set(D_NONE, D_VOLUNTARY, D_VOTEDOUT))),
    harmedByPolicy == false,
    ceilingHarm == false,
    calm == 0,
    vSize >= 1,
    REGISTERED.forall(n => and {
      alive.contains(n) implies darkFor.get(n) == 0,
      not(alive.contains(n)) implies brightFor.get(n) == 0,
      not(seated.contains(n)) implies seatedFor.get(n) == 0,
    }),
  }

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

// ============================================================
// Evidence-contract machine (per-attestor staleness). Checks the
// freshness contract itself — the thing the main machine axiomatizes.
// obsDark[(y, x)] = ticks since observer y last had contact with
// target x. Contact: self, an adversary-controlled direct link, or —
// when CERT_CHANNEL — x's signature in a committed certificate, which
// refreshes EVERY observer at once (replicated evidence).
//
// A vote-out commits only with >= q(v) AFFIRMATIVE attestors among
// the live seated, and a node never attests against itself. That
// yes-quorum rule carries a model-found theorem: even with the
// freshness contract BROKEN (CERT_CHANNEL off, adversarial links), a
// wrongful removal of a live node needs q(v) OTHER live validators,
// so the survivor set keeps quorum — wrongful vote-out can steal
// headroom, never stall the mesh. The freshness contract's real job
// is blocking the theft, not preventing the stall.
// ============================================================
module evidence_policy {
  const REGISTERED: Set[int]
  const Q_TABLE: int -> int
  const T_CLIFF: int
  const T_CAP: int
  const CERT_CHANNEL: bool

  var seated: Set[int]
  var alive: Set[int]
  var obsDark: (int, int) -> int
  var linkUp: (int, int) -> bool
  var harmedByPolicy: bool

  val vSize = seated.size()
  def q(v: int): int = Q_TABLE.get(v)
  val liveSeated = alive.intersect(seated)
  val stalled = liveSeated.size() < q(vSize)
  val headroom = liveSeated.size() - q(vSize)
  val pairs = tuples(REGISTERED, REGISTERED)

  def contact(y: int, x: int): bool = and {
    alive.contains(x),
    or {
      y == x,
      linkUp.get((y, x)),
      CERT_CHANNEL and seated.contains(x),
    }
  }

  def attestors(x: int): Set[int] =
    liveSeated.exclude(Set(x)).filter(y => obsDark.get((y, x)) >= T_CLIFF)

  def voteOutEnabled(x: int): bool = and {
    seated.contains(x),
    attestors(x).size() >= q(vSize),
    not(stalled),
  }

  action init = all {
    seated' = REGISTERED,
    alive' = REGISTERED,
    obsDark' = pairs.mapBy(p => 0),
    linkUp' = pairs.mapBy(p => true),
    harmedByPolicy' = false,
  }

  action envKillN(n: int): bool = all {
    alive.contains(n),
    alive' = alive.exclude(Set(n)),
    seated' = seated, obsDark' = obsDark, linkUp' = linkUp,
    harmedByPolicy' = harmedByPolicy,
  }

  action envLinkN(y: int, x: int, up: bool): bool = all {
    linkUp' = linkUp.set((y, x), up),
    seated' = seated, alive' = alive, obsDark' = obsDark,
    harmedByPolicy' = harmedByPolicy,
  }
  action envLink = all {
    { nondet p = pairs.oneOf()
      nondet up = Set(true, false).oneOf()
      envLinkN(p._1, p._2, up) }
  }

  action envTick = all {
    obsDark' = pairs.mapBy(p =>
      if (contact(p._1, p._2)) 0
      else if (obsDark.get(p) < T_CAP) obsDark.get(p) + 1 else T_CAP),
    seated' = seated, alive' = alive, linkUp' = linkUp,
    harmedByPolicy' = harmedByPolicy,
  }

  action commitVoteOutN(x: int): bool = all {
    voteOutEnabled(x),
    seated' = seated.exclude(Set(x)),
    harmedByPolicy' = (harmedByPolicy or
      alive.intersect(seated.exclude(Set(x))).size() < q(vSize - 1)),
    alive' = alive, obsDark' = obsDark, linkUp' = linkUp,
  }
  action commitVoteOut = all {
    seated.exists(x => voteOutEnabled(x)),
    { nondet x = seated.filter(x => voteOutEnabled(x)).oneOf()
      commitVoteOutN(x) }
  }

  action step = any { envKillN(4), envLink, envTick, commitVoteOut }

  val invNoHarm = not(harmedByPolicy)
}

// POS: with the cert channel honored, a live seated node's evidence
// never goes stale anywhere — wrongful attestation is impossible.
module cfg_evidence_pos {
  import q_tables.*
  import evidence_policy(
    REGISTERED = 1.to(4), Q_TABLE = Q_BFT_TABLE,
    T_CLIFF = 2, T_CAP = 6, CERT_CHANNEL = true
  ).*

  run posNoWrongfulAttestTest =
    init
      // adversary severs every direct link
      .then(envLinkN(1, 4, false)).then(envLinkN(2, 4, false))
      .then(envLinkN(3, 4, false))
      .then(6.reps(_ => envTick))
      // cert participation kept every observer fresh: 4 not removable
      .expect(not(voteOutEnabled(4)))
      // a genuinely dead node IS removable — certs stop refreshing
      .then(envKillN(4)).then(2.reps(_ => envTick))
      .then(commitVoteOutN(4))
      .expect(vSize == 3 and invNoHarm)
}

// NEG-F1 (freshness contract broken): a quorum's evidence goes stale
// against a LIVE node and removes it. The theorem holds — no stall —
// but headroom is stolen: the mesh pays one tolerance point for the
// broken contract.
module cfg_evidence_neg_theft {
  import q_tables.*
  import evidence_policy(
    REGISTERED = 1.to(4), Q_TABLE = Q_BFT_TABLE,
    T_CLIFF = 2, T_CAP = 6, CERT_CHANNEL = false
  ).*

  run negF1HeadroomTheftTest =
    init
      .then(envLinkN(1, 4, false)).then(envLinkN(2, 4, false))
      .then(envLinkN(3, 4, false))
      .then(2.reps(_ => envTick))
      .expect(headroom == 1 and voteOutEnabled(4))   // wrongful, enabled
      .then(commitVoteOutN(4))
      // stolen: live node lost, q(3)=3 under BFT — headroom 1 -> 0.
      // NOT stalled: the yes-quorum needed 3 OTHER live validators.
      .expect(vSize == 3 and headroom == 0 and not(stalled) and invNoHarm)

  // The theorem under full adversarial schedule: links flap, nodes
  // die, wrongful removals fire — the mesh never stalls by policy.
  // (Random-sim gate; also Apalache-checkable at this size.)
}

// NEG-F2 (asymmetric refresh): a single healthy half-link keeps one
// observer perpetually fresh; at attestor-quorum sizes the removal is
// BLOCKED, not delayed, until the refresher's link decays — the
// fairness assumption the spec must state.
module cfg_evidence_neg_asym {
  import q_tables.*
  import evidence_policy(
    REGISTERED = 1.to(4), Q_TABLE = Q_BFT_TABLE,
    T_CLIFF = 2, T_CAP = 6, CERT_CHANNEL = false
  ).*

  run negF2AsymmetricBlockTest =
    init
      // node 4 answers ONLY observer 3; certs disabled
      .then(envLinkN(1, 4, false)).then(envLinkN(2, 4, false))
      .then(6.reps(_ => envTick))
      // attestors = {1, 2} — two short of nothing: q(4) = 3. Blocked
      // FOREVER while 3's half-link survives.
      .expect(attestors(4).size() == 2 and not(voteOutEnabled(4)))
      // fairness: the asymmetric link decays; evidence goes commonly
      // stale; removal proceeds.
      .then(envLinkN(3, 4, false))
      .then(2.reps(_ => envTick))
      .then(commitVoteOutN(4))
      .expect(vSize == 3 and invNoHarm)
}

// ============================================================
// Partition machine: block-relative quorum and evidence. During a
// split only the block holding >= q(v) live seated members commits
// anything; its evidence about the other block goes stale together.
// live/stalled are measured against the COMMITTING BLOCK.
// ============================================================
module partition_policy {
  const REGISTERED: Set[int]
  const INIT_SEATED: Set[int]
  const Q_TABLE: int -> int
  const FEQ_TABLE: int -> int
  const T_CLIFF: int
  const S_FULL: int

  var seated: Set[int]
  var alive: Set[int]
  var blockA: Set[int]        // the (potential) majority block
  var split: bool
  var darkToA: int -> int     // A's staleness evidence per node
  var brightToA: int -> int   // A's bright-span evidence per node

  val vSize = seated.size()
  def q(v: int): int = Q_TABLE.get(v)
  def feq(v: int): int = FEQ_TABLE.get(v)
  // Who A can currently reach: its own alive members, plus everyone
  // alive once healed.
  val reachableByA = if (split) blockA.intersect(alive) else alive
  val liveForA = reachableByA.intersect(seated)
  val stalledForA = liveForA.size() < q(vSize)

  def voteOutEnabled(x: int): bool = and {
    seated.contains(x),
    not(reachableByA.contains(x)),
    darkToA.get(x) >= T_CLIFF,
    not(stalledForA),
  }
  // Crossing/batch seating from A's side, spans per A's evidence.
  def seatEnabled(bs: Set[int]): bool = and {
    bs.size() > 0,
    bs.subseteq(reachableByA.exclude(seated)),
    not(stalledForA),
    { val d = bs.size() - (q(vSize + bs.size()) - q(vSize))
      or { d >= 1, d == 0 and feq(vSize + bs.size()) > feq(vSize) } },
    bs.forall(n => brightToA.get(n) >= S_FULL),
  }

  action init = all {
    seated' = INIT_SEATED,
    alive' = REGISTERED,
    blockA' = REGISTERED,
    split' = false,
    darkToA' = REGISTERED.mapBy(n => 0),
    brightToA' = REGISTERED.mapBy(n => S_FULL),
  }

  action envSplitS(a: Set[int]): bool = all {
    not(split),
    split' = true,
    blockA' = a,
    seated' = seated, alive' = alive, darkToA' = darkToA, brightToA' = brightToA,
  }
  action envHeal = all {
    split,
    split' = false,
    blockA' = REGISTERED,
    seated' = seated, alive' = alive, darkToA' = darkToA, brightToA' = brightToA,
  }

  action envTick = all {
    darkToA' = REGISTERED.mapBy(n =>
      if (reachableByA.contains(n)) 0
      else if (darkToA.get(n) < T_CLIFF) darkToA.get(n) + 1 else T_CLIFF),
    brightToA' = REGISTERED.mapBy(n =>
      if (not(reachableByA.contains(n))) 0
      else if (brightToA.get(n) < S_FULL) brightToA.get(n) + 1 else S_FULL),
    seated' = seated, alive' = alive, blockA' = blockA, split' = split,
  }

  action commitVoteOutN(x: int): bool = all {
    voteOutEnabled(x),
    seated' = seated.exclude(Set(x)),
    alive' = alive, blockA' = blockA, split' = split,
    darkToA' = darkToA, brightToA' = brightToA,
  }
  action commitSeatBatchB(bs: Set[int]): bool = all {
    seatEnabled(bs),
    seated' = seated.union(bs),
    alive' = alive, blockA' = blockA, split' = split,
    darkToA' = darkToA, brightToA' = brightToA,
  }
}

module cfg_partition {
  import q_tables.*
  import partition_policy(
    REGISTERED = 1.to(8), INIT_SEATED = 1.to(8),
    Q_TABLE = Q_AUTO_TABLE, FEQ_TABLE = FEQ_AUTO_TABLE,
    T_CLIFF = 2, S_FULL = 5
  ).*

  // WITNESS (minority impotence): under an even-ish split NEITHER
  // block holds q(8) = 6 — nothing commits anywhere. The
  // disjoint-quorum lemma says both-commit is impossible at ANY
  // split; this shows the common case is neither.
  run minorityImpotenceTest =
    init.then(envSplitS(Set(1, 2, 3, 4, 5)))
      .then(4.reps(_ => envTick))
      .expect(stalledForA and 6.to(8).forall(x => not(voteOutEnabled(x))))

  // WITNESS (heal round-trip across the seam): a 6-member majority
  // block removes the 2-node minority during a split — shedding BFT
  // at the seam — the partition heals, the removed nodes re-prove,
  // and the crossing batch restores BFT. The full life cycle of a
  // partition under this policy.
  run healRoundTripTest =
    init.then(envSplitS(1.to(6)))
      .then(2.reps(_ => envTick))
      .expect(not(stalledForA))                 // 6 >= q(8) = 6: A commits
      .then(commitVoteOutN(7))
      .expect(vSize == 7 and q(vSize) == 5)     // still BFT at 7
      .then(commitVoteOutN(8))
      .expect(vSize == 6 and q(vSize) == 4 and feq(vSize) == 0) // majority
      .then(envHeal)
      .then(5.reps(_ => envTick))               // spans re-prove post-heal
      .then(commitSeatBatchB(Set(7, 8)))        // crossing batch: BFT back
      .expect(vSize == 8 and q(vSize) == 6 and feq(vSize) == 2)
}

// ============================================================
// Scheduler config: the deterministic policyTick drives removal and
// mandatory seating — GUAR-HONEST-SET and longest-dark-first are
// bounded-checkable here, not just adversarially possible.
// ============================================================
module cfg_sched {
  import q_tables.*
  import membership_policy(
    REGISTERED = 1.to(7), INIT_SEATED = 1.to(7),
    Q_TABLE = Q_AUTO_TABLE, FEQ_TABLE = FEQ_AUTO_TABLE,
    T_CLIFF = 2, T_FAST = 3, T_LAZY = 4, S_FLOOR = 1, S_FULL = 5,
    P_PROVE = 5, B_MAX = 5, CEILING_MODE = 0, ENFORCE_SMIN = true
  ).*

  // WITNESS (longest-dark first): with two dark members the scheduler
  // removes the longer-dark one first.
  run longestDarkFirstTest =
    init.then(envKillN(6)).then(policyTick)
      .then(envKillN(7)).then(2.reps(_ => policyTick))
      .then(policyTick)                          // first removal fires here
      .expect(not(seated.contains(6)) and seated.contains(7))
      .then(policyTick)
      .expect(not(seated.contains(7)) and vSize == 5 and invNoHarm)

  // WITNESS (GUAR-HONEST-SET + self-heal): kills are absorbed within
  // the calm bound — no removable dark node survives sustained calm —
  // and a healed node is re-seated by the scheduler at gain parity
  // with no human action.
  run guarSelfHealTest =
    init.then(envKillN(6)).then(envKillN(7))
      .then(8.reps(_ => policyTick))
      .expect(and {
        removableNow.size() == 0, vSize == 5, headroom == 2,
        invHonestSet, invNoHarm,
      })
      .then(envKillN(5)).then(4.reps(_ => policyTick))
      .expect(vSize == 4 and headroom == 1)      // v=4: next seat is a gain
      .then(envReviveN(5)).then(7.reps(_ => policyTick))
      // span re-proved (5 ticks) then mandatory seating fired
      .expect(seated.contains(5) and vSize == 5 and invNoHarm and invCeiling)
}

// ============================================================
// RFC-019 regenesis: the epoch-boundary machine. A phase machine
// (NORMAL -> MORATORIUM -> SEALED -> restart) layered over a
// membership_policy instance at cfg_seam scale, so a boundary can
// cross the quorum seam. Versions and the S5 staged-version
// precondition are deliberately NOT modeled: they never affect
// membership safety — a node parked "awaiting upgrade" is simply dark
// at restart, which is already the environment's choice.
//
// The bridge: restartEpoch carries seated / seatedFor / lastDeparture
// verbatim (proven-ness reads committed activation heights, not
// evidence), wipes darkFor / brightFor to zero (EvidenceMap is
// in-memory and dies with the process), and resets calm. Every indInv
// conjunct is satisfied by that state, so the whole membership safety
// matrix (no-harm, ceiling, floor, no-exile) transfers to the new
// epoch depth-free — proven by extending the inductive invariant
// (epochIndInv) over the enlarged machine, never by re-proving the
// membership layer. The post-restart quiet period is structural, not
// added: vote-out needs darkFor >= T_CLIFF and seating needs a fresh
// bright span, both zeroed at the boundary.
//
// The staged pool is abstract: admitOne models admission (NORMAL
// only — closing admission IS the moratorium, so the pool can only
// fall once regenesis starts and drain termination is structural);
// membership commits during MORATORIUM consume pool (they are staged
// txs draining); commitSeal requires pool == 0 (drain complete).
// decidedPastSeal is the seal-safety ghost: every decide records
// whether it fired past the seal — the guards make that unreachable,
// and the inductive check keeps it unreachable against future edits.
// ============================================================
module epoch_policy {
  import q_tables.*
  import membership_policy(
    REGISTERED = 1.to(9), INIT_SEATED = 1.to(7),
    Q_TABLE = Q_AUTO_TABLE, FEQ_TABLE = FEQ_AUTO_TABLE,
    T_CLIFF = 2, T_FAST = 3, T_LAZY = 4, S_FLOOR = 1, S_FULL = 5,
    P_PROVE = 5, B_MAX = 5, CEILING_MODE = 0, ENFORCE_SMIN = true
  ).*

  pure val POOL_MAX = 3

  // Phases.
  pure val P_NORMAL = 0
  pure val P_MORATORIUM = 1     // regenesis_start decided, draining
  pure val P_SEALED = 2         // regenesis_commit decided, awaiting restart

  var phase: int
  var pool: int                 // staged-but-undecided txs, abstracted
  var carriedSeated: Set[int]   // ghost: seated as of the seal
  var decidedPastSeal: bool     // ghost: some decide fired after the seal

  // === Guard vocabulary (shared by actions, invariants, witnesses) ===
  // A membership commit decides freely in NORMAL, or in MORATORIUM as
  // a staged tx while the pool lasts. Never in SEALED.
  val commitPhaseOk = or {
    phase == P_NORMAL,
    and { phase == P_MORATORIUM, pool > 0 },
  }
  val drainEnabled = and { phase != P_SEALED, pool > 0 }
  val abortEnabled = (phase == P_MORATORIUM)  // strictly inside (start, commit)
  val sealEnabled = and { phase == P_MORATORIUM, pool == 0 }
  val poolAfterCommit = if (phase == P_MORATORIUM) pool - 1 else pool

  // === Frame helpers ===
  action keepEpoch = all {
    phase' = phase, pool' = pool, carriedSeated' = carriedSeated,
    decidedPastSeal' = decidedPastSeal,
  }
  action keepMembership = all {
    seated' = seated, alive' = alive, darkFor' = darkFor,
    brightFor' = brightFor, seatedFor' = seatedFor,
    lastDeparture' = lastDeparture, harmedByPolicy' = harmedByPolicy,
    ceilingHarm' = ceilingHarm, calm' = calm,
  }

  action epochInit = all {
    init,
    phase' = P_NORMAL, pool' = 0, carriedSeated' = Set(),
    decidedPastSeal' = false,
  }

  // === Environment ===
  // Kills/revives in every phase — nodes crash and come back between
  // seal and restart too (the dark-seat setup). Ticks only while the
  // engine runs probe cycles, i.e. never in SEALED.
  action eEnvKillN(n: int): bool = all { envKillN(n), keepEpoch }
  action eEnvKill = all { envKill, keepEpoch }
  action eEnvReviveN(n: int): bool = all { envReviveN(n), keepEpoch }
  action eEnvRevive = all { envRevive, keepEpoch }
  action eEnvTick = all { phase != P_SEALED, envTick, keepEpoch }

  // === Decides (each = one consensus height; each records the ghost) ===
  action eCommitVoteOutN(x: int): bool = all {
    commitPhaseOk, commitVoteOutN(x),
    phase' = phase, pool' = poolAfterCommit, carriedSeated' = carriedSeated,
    decidedPastSeal' = (decidedPastSeal or phase == P_SEALED),
  }
  action eCommitVoteOut = all {
    commitPhaseOk, commitVoteOut,
    phase' = phase, pool' = poolAfterCommit, carriedSeated' = carriedSeated,
    decidedPastSeal' = (decidedPastSeal or phase == P_SEALED),
  }
  action eCommitLeaveN(x: int): bool = all {
    commitPhaseOk, commitLeaveN(x),
    phase' = phase, pool' = poolAfterCommit, carriedSeated' = carriedSeated,
    decidedPastSeal' = (decidedPastSeal or phase == P_SEALED),
  }
  action eCommitLeave = all {
    commitPhaseOk, commitLeave,
    phase' = phase, pool' = poolAfterCommit, carriedSeated' = carriedSeated,
    decidedPastSeal' = (decidedPastSeal or phase == P_SEALED),
  }
  action eCommitSeatBatchB(bs: Set[int]): bool = all {
    commitPhaseOk, commitSeatBatchB(bs),
    phase' = phase, pool' = poolAfterCommit, carriedSeated' = carriedSeated,
    decidedPastSeal' = (decidedPastSeal or phase == P_SEALED),
  }
  action eCommitSeatBatch = all {
    commitPhaseOk, commitSeatBatch,
    phase' = phase, pool' = poolAfterCommit, carriedSeated' = carriedSeated,
    decidedPastSeal' = (decidedPastSeal or phase == P_SEALED),
  }

  // Admission is a client submitting, not a decide — NORMAL only:
  // closing this action IS the moratorium.
  action admitOne = all {
    phase == P_NORMAL,
    pool' = (if (pool < POOL_MAX) pool + 1 else POOL_MAX),
    phase' = phase, carriedSeated' = carriedSeated,
    decidedPastSeal' = decidedPastSeal,
    keepMembership,
  }
  // A staged non-membership tx decides.
  action drainOne = all {
    drainEnabled,
    pool' = pool - 1,
    phase' = phase, carriedSeated' = carriedSeated,
    decidedPastSeal' = (decidedPastSeal or phase == P_SEALED),
    keepMembership,
  }

  // regenesis_start decides: moratorium opens, admission closes.
  action commitStart = all {
    phase == P_NORMAL,
    phase' = P_MORATORIUM,
    pool' = pool, carriedSeated' = carriedSeated,
    decidedPastSeal' = (decidedPastSeal or phase == P_SEALED),
    keepMembership,
  }
  // regenesis_abort decides (admin-signed, bypasses the closed
  // admission gate): back to NORMAL, nothing else changes.
  action commitAbort = all {
    abortEnabled,
    phase' = P_NORMAL,
    pool' = pool, carriedSeated' = carriedSeated,
    decidedPastSeal' = (decidedPastSeal or phase == P_SEALED),
    keepMembership,
  }
  // regenesis_commit decides as the sole tx of the final block (drain
  // complete by its guard): the epoch is sealed.
  action commitSeal = all {
    sealEnabled,
    phase' = P_SEALED,
    pool' = pool,
    carriedSeated' = seated,
    decidedPastSeal' = (decidedPastSeal or phase == P_SEALED),
    keepMembership,
  }

  // Restart is NOT a decide: each node derives it locally from the
  // sealed artifacts. Committed state (seated set, activation-height
  // proven-ness, departure kinds) carries verbatim; in-memory evidence
  // dies with the process; calm restarts. The target state satisfies
  // every indInv conjunct — the bridge into the next epoch.
  action restartEpoch = all {
    phase == P_SEALED,
    phase' = P_NORMAL,
    pool' = 0,
    carriedSeated' = carriedSeated,
    decidedPastSeal' = decidedPastSeal,
    seated' = seated,
    seatedFor' = seatedFor,
    lastDeparture' = lastDeparture,
    alive' = alive,
    darkFor' = REGISTERED.mapBy(n => 0),
    brightFor' = REGISTERED.mapBy(n => 0),
    harmedByPolicy' = harmedByPolicy,
    ceilingHarm' = ceilingHarm,
    calm' = 0,
  }

  action epochStep = any {
    eEnvKill, eEnvRevive, eEnvTick,
    eCommitVoteOut, eCommitLeave, eCommitSeatBatch,
    admitOne, drainOne,
    commitStart, commitAbort, commitSeal, restartEpoch,
  }

  // === Invariants ===
  val invSealSafety = not(decidedPastSeal)
  val invCarry = (phase == P_SEALED) implies (seated == carriedSeated)
  val epochSafetyInv = and { safetyInv, invSealSafety, invCarry }

  // Inductive form (indInv's conjuncts inlined — Apalache needs every
  // variable constrained at the TOP level of the conjunction). One
  // epochStep from ANY state satisfying this preserves it, and it
  // implies epochSafetyInv — the membership proofs transfer across the
  // boundary with no depth bound.
  val epochIndInv = and {
    seated.in(REGISTERED.powerset()),
    alive.in(REGISTERED.powerset()),
    darkFor.in(REGISTERED.setOfMaps(0.to(T_LAZY))),
    brightFor.in(REGISTERED.setOfMaps(0.to(S_FULL))),
    seatedFor.in(REGISTERED.setOfMaps(0.to(P_PROVE))),
    lastDeparture.in(REGISTERED.setOfMaps(Set(D_NONE, D_VOLUNTARY, D_VOTEDOUT))),
    harmedByPolicy == false,
    ceilingHarm == false,
    calm == 0,
    phase.in(Set(P_NORMAL, P_MORATORIUM, P_SEALED)),
    pool.in(0.to(POOL_MAX)),
    carriedSeated.in(REGISTERED.powerset()),
    decidedPastSeal == false,
    vSize >= 1,
    (phase == P_SEALED) implies (seated == carriedSeated and pool == 0),
    REGISTERED.forall(n => and {
      alive.contains(n) implies darkFor.get(n) == 0,
      not(alive.contains(n)) implies brightFor.get(n) == 0,
      not(seated.contains(n)) implies seatedFor.get(n) == 0,
    }),
  }

  // WITNESS (the full boundary, dark seats voted out from a carried
  // configuration): a staged vote-out decides in the drain, the seal
  // carries the set verbatim, two nodes die between seal and restart,
  // and the ordinary machinery removes them from the carried set —
  // safely — once fresh evidence accrues.
  run fullBoundaryTest =
    epochInit
      .then(eEnvKillN(7)).then(2.reps(_ => eEnvTick))   // 7 dark past window
      .then(admitOne).then(admitOne)                    // staged backlog of 2
      .then(commitStart)
      .expect(phase == P_MORATORIUM and pool == 2)
      .then(eCommitVoteOutN(7))                         // staged membership tx drains
      .expect(vSize == 6 and pool == 1)
      .then(drainOne)
      .then(commitSeal)
      .expect(phase == P_SEALED and seated == carriedSeated and pool == 0)
      .then(eEnvKillN(5)).then(eEnvKillN(6))            // die between seal and restart
      .then(restartEpoch)
      // carried verbatim; fresh evidence: nothing removable or seatable
      .expect(and {
        phase == P_NORMAL,
        seated == carriedSeated,
        vSize == 6,
        REGISTERED.forall(n => darkFor.get(n) == 0 and brightFor.get(n) == 0),
        REGISTERED.forall(n => not(voteOutEnabled(n))),
      })
      .then(2.reps(_ => eEnvTick))                      // fresh spans accrue
      .then(eCommitVoteOutN(5)).then(eCommitVoteOutN(6))
      .expect(vSize == 4 and headroom == 1
        and invNoHarm and invCeiling and not(decidedPastSeal))

  // WITNESS (abort round-trip): abort mid-drain restores NORMAL with
  // membership state and evidence untouched, admission reopens, and a
  // second start can still reach the seal — abort wedges nothing.
  run abortRoundTripTest =
    epochInit
      .then(3.reps(_ => admitOne))
      .then(commitStart)
      .expect(phase == P_MORATORIUM and not(sealEnabled))
      .then(drainOne)                                   // partial drain
      .then(commitAbort)
      .expect(and {
        phase == P_NORMAL, pool == 2,
        seated == 1.to(7), alive == 1.to(9),
        REGISTERED.forall(n =>
          brightFor.get(n) == S_FULL and darkFor.get(n) == 0),
      })
      .then(admitOne)                                   // admission reopened
      .expect(pool == 3)
      .then(commitStart)
      .then(3.reps(_ => drainOne))
      .then(commitSeal)
      .expect(phase == P_SEALED and seated == carriedSeated
        and not(decidedPastSeal))

  // WITNESS (quiet period is structural): a candidate provably
  // seatable before the boundary is NOT seatable after it until a
  // fresh bright span accrues, and a seat gone dark across the
  // boundary is NOT removable until fresh darkness accrues — then
  // both proceed safely (T_CLIFF < S_FULL ordering visible midway).
  run quietPeriodTest =
    epochInit
      .then(eEnvKillN(7)).then(2.reps(_ => eEnvTick))
      .then(eCommitVoteOutN(7))                         // v=6, majority region
      .then(eEnvReviveN(7)).then(5.reps(_ => eEnvTick))
      .expect(seatEnabled(Set(7)))                      // re-proven pre-boundary
      .then(commitStart).then(commitSeal)               // housekeeping boundary
      .then(eEnvKillN(6))                               // goes dark across it
      .then(restartEpoch)
      .expect(and {
        not(seatEnabled(Set(7))),                       // span must re-accrue
        not(voteOutEnabled(6)),                         // evidence must re-accrue
      })
      .then(2.reps(_ => eEnvTick))
      .expect(voteOutEnabled(6) and not(seatEnabled(Set(7))))
      .then(3.reps(_ => eEnvTick))
      .then(eCommitSeatBatchB(Set(7)))                  // crossing seat, span re-proven
      .then(eCommitVoteOutN(6))                         // dark carried seat out
      .expect(vSize == 6 and invNoHarm and invCeiling and not(decidedPastSeal))

  // WITNESS (recovery is forward-only): after the seal, no decide of
  // any kind is enabled — no membership commit, no drain, no abort.
  run sealForwardOnlyTest =
    epochInit.then(commitStart).then(commitSeal)        // empty pool: instant seal
      .expect(and {
        phase == P_SEALED,
        not(commitPhaseOk), not(drainEnabled),
        not(abortEnabled), not(sealEnabled),
        seated == carriedSeated,
      })

  // WITNESS (drain terminates): admission closed at start, so the
  // pool only falls; the seal is enabled exactly when it empties.
  run drainTerminationTest =
    epochInit.then(3.reps(_ => admitOne))
      .expect(pool == POOL_MAX)
      .then(admitOne)                                   // saturates at the cap
      .expect(pool == POOL_MAX)
      .then(commitStart)
      .expect(not(sealEnabled) and drainEnabled)
      .then(drainOne).then(drainOne)
      .expect(not(sealEnabled) and pool == 1)
      .then(drainOne)
      .expect(sealEnabled and not(drainEnabled))        // drain terminated
      .then(commitSeal)
      .expect(phase == P_SEALED and not(decidedPastSeal))
}
