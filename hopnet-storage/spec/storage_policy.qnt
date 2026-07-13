// RFC-018: storage durability & placement policy — normative model (v2).
//
// Models ONE chunk of one blob (file-correlated placement makes all
// chunks of a file share fate for node-level faults; per-chunk media
// corruption is the corrupt action). Fragment classes 0..N_FRAGS-1;
// any K live classes reconstruct the chunk.
//
// Copy classes (RFC-018 decision 1): RESPONSIBLE = placement output
// (computed, never stored); SURPLUS = any other copy. INTERESTED
// (projection pinning) is deliberately absent — it only strengthens
// eviction guards, so its omission is conservative.
//
// v2 adds the hysteresis machinery (decision 11): a decay gate DELTA
// on membership-view sync, watermark-W two-tier re-encode urgency,
// and repair-cost accounting — membership flux must not trigger
// aggressive repair; cost properties are the placement-fork criterion.
//
// The engine is a DETERMINISTIC control loop (engineTick): advance
// down-clocks -> sync decayed membership view -> refresh inventory
// snapshot -> pull one missing responsible copy -> re-encode one
// hopeless-or-urgent dead class. The environment is fully adversarial
// within fault budgets. Repair work sharding across chunks (decision
// 10) is invisible in a one-chunk model — see the prose spec.

// Placement functions — PURE, stateless. Kept in their own module so
// they are referenced only where a config builds a placement TABLE
// (Rust backend, run/test). The control logic below never calls them —
// it reads an injected lookup — so `quint verify` (Apalache) never sees
// this nonlinear integer arithmetic (modulo / divide-by-node-count),
// which it rejects. The verify config passes a precomputed literal
// table instead. See spec/README.md.
module placement {
  // Variant 0: current select-then-modulo (all members sorted by id,
  // class f -> members[f mod len]).
  pure def minOf(s: Set[int]): int = s.fold(1000000, (a, x) => if (x < a) x else a)

  pure def nthSmallest(s: Set[int], n: int): int =
    range(0, n + 1).foldl((s, -1), (acc, _i) => {
      val m = minOf(acc._1)
      (acc._1.exclude(Set(m)), m)
    })._2

  pure def placeModulo(f: int, members: Set[int]): int =
    nthSmallest(members, f % members.size())

  // Variants 1/2: weighted rendezvous (HRW). Deterministic integer mix
  // stands in for blake3; properties under check depend only on
  // determinism, not hash quality. Lower score wins; higher weight
  // shrinks score (more responsibility).
  pure def mix(n: int, f: int): int = {
    val a = (n * 1103515245 + 12345) % 2147483647
    val b = (a * 31 + f * 1103515245 + 54321) % 2147483647
    (b * 65539 + a * (f + 7)) % 2147483647
  }

  pure def hrwScore(n: int, f: int, w: int -> int): int = mix(n, f) / w.get(n)

  pure def placeHrw(f: int, members: Set[int], w: int -> int): int =
    members.fold(-1, (best, n) =>
      if (best == -1) n
      else if (hrwScore(n, f, w) < hrwScore(best, f, w)) n
      else if (hrwScore(n, f, w) == hrwScore(best, f, w) and n < best) n
      else best)

  // Capped HRW (Stage-1 finding: plain HRW breaks even-spread): greedy
  // over classes in index order; a node at cap is skipped, next-best
  // takes the class. Cap = ceil(N_FRAGS / |members|) restores the
  // INV-SPREAD bridge lemma; weights bias WHICH classes land where.
  pure def placeCappedAll(nfrags: int, members: Set[int], w: int -> int): int -> int = {
    val cap = (nfrags + members.size() - 1) / members.size()
    range(0, nfrags).foldl(
      (members.mapBy(n => 0), 0.to(nfrags - 1).mapBy(f => -1)),
      (acc, f) => {
        val counts = acc._1
        val eligible = members.filter(n => counts.get(n) < cap)
        val chosen = eligible.fold(-1, (best, n) =>
          if (best == -1) n
          else if (hrwScore(n, f, w) < hrwScore(best, f, w)) n
          else if (hrwScore(n, f, w) == hrwScore(best, f, w) and n < best) n
          else best)
        (counts.set(chosen, counts.get(chosen) + 1), acc._2.set(f, chosen))
      })._2
  }

  pure def placeVariant(v: int, f: int, nfrags: int, members: Set[int], w: int -> int): int =
    if (v == 0) placeModulo(f, members)
    else if (v == 1) placeHrw(f, members, w)
    else placeCappedAll(nfrags, members, w).get(f)

  // Build the injected lookup: every non-empty member subset -> its
  // (class -> responsible node) map. Consumed by config modules; on the
  // Rust backend this is computed once. `members` is always non-empty
  // in the control logic (init = NODES, engine syncs to a non-empty
  // viewTarget), so the empty set is excluded.
  pure def buildTable(v: int, nfrags: int, allNodes: Set[int], w: int -> int): Set[int] -> (int -> int) =
    allNodes.powerset().exclude(Set(Set())).mapBy(s =>
      range(0, nfrags).foldl(Map(), (acc, f) => acc.put(f, placeVariant(v, f, nfrags, s, w))))
}

module storage_policy {
  // === Constants (fixed per config instance) ===
  const NODES: Set[int]           // node id universe
  const N_FRAGS: int              // fragment classes per chunk (30 real)
  const K: int                    // RS reconstruction threshold (10 real)
  const AUTO_PULL: bool           // policy: automatic tier-1 pull repair
  const AUTO_REENCODE: bool       // policy: scrub re-encode of dead classes
  const MAX_GONE: int             // fault budget: permanent node losses
  const MAX_CORRUPT: int          // fault budget: single-copy corruptions
  const CALM_BOUND: int           // recovery deadline in engine ticks
  const DELTA: int                // decay gate: ticks down before view drops a node
  const W: int                    // urgency watermark: live classes below W => re-encode now
  // Injected placement lookup: member subset -> (class -> resp node).
  // Decouples the control logic from placement arithmetic (see module
  // `placement`), so Apalache can verify this model.
  const PLACE: Set[int] -> (int -> int)

  type Status = int               // 0 = Up, 1 = Down (copies intact), 2 = Gone (disk lost)
  pure val UP = 0
  pure val DOWN = 1
  pure val GONE = 2

  // === State ===
  var status: int -> Status       // ground truth per node
  var downFor: int -> int         // engine ticks each node has been non-Up (capped at DELTA)
  var memberView: Set[int]        // consensus's damped membership view (lags status)
  var copies: int -> Set[int]     // frag class -> nodes holding a live copy
  var invView: int -> Set[int]    // stale inventory snapshot (drives eviction guards)
  var deleted: bool               // blob deleted at consensus (one-way)
  var calm: int                   // consecutive engine ticks without env interference
  var repairCost: int             // pulls x1, re-encodes xK (bandwidth-shaped units)
  var goneCnt: int
  var corruptCnt: int

  // === Derived ===
  val frags = 0.to(N_FRAGS - 1)
  def isUp(n: int): bool = status.get(n) == UP
  val upNodes = NODES.filter(n => isUp(n))
  val extant = NODES.filter(n => status.get(n) != GONE)

  // Responsible node for class f = injected-table lookup. The table is
  // total over non-empty subsets; memberView is always non-empty.
  def resp(f: int): int = PLACE.get(memberView).get(f)

  // Smallest element — deterministic pick for the single item repaired
  // per tick. Linear (no arithmetic Apalache dislikes). Named distinctly
  // from placement::minOf so a config importing both has no clash.
  pure def least(s: Set[int]): int = s.fold(1000000, (a, x) => if (x < a) x else a)

  // === Invariant vocabulary ===
  // Durable: enough classes survive on intact disks to reconstruct.
  val durable = frags.filter(f => copies.get(f).intersect(extant).size() > 0).size() >= K
  // Available: reconstructible from currently-online nodes.
  val available = frags.filter(f => copies.get(f).intersect(upNodes).size() > 0).size() >= K
  // Conformant: every responsible node holds its class.
  val conformant = frags.forall(f => copies.get(f).contains(resp(f)))

  // === Init: conformant, fresh views, everyone up ===
  action init = all {
    status' = NODES.mapBy(n => UP),
    downFor' = NODES.mapBy(n => 0),
    memberView' = NODES,
    copies' = 0.to(N_FRAGS - 1).mapBy(f => Set(PLACE.get(NODES).get(f))),
    invView' = 0.to(N_FRAGS - 1).mapBy(f => Set(PLACE.get(NODES).get(f))),
    deleted' = false,
    calm' = 0,
    repairCost' = 0,
    goneCnt' = 0,
    corruptCnt' = 0,
  }

  // === Environment actions (adversarial; all reset calm) ===
  action envSleepN(n: int): bool = all {
    isUp(n),
    upNodes.size() > 1,
    status' = status.set(n, DOWN),
    downFor' = downFor.set(n, 0),
    memberView' = memberView, copies' = copies, invView' = invView, deleted' = deleted,
    calm' = 0, repairCost' = repairCost, goneCnt' = goneCnt, corruptCnt' = corruptCnt,
  }

  action envSleep = all {
    upNodes.size() > 1,
    { nondet n = upNodes.oneOf()
      envSleepN(n) }
  }

  action envWake = all {
    NODES.filter(n => status.get(n) == DOWN).size() > 0,
    {
      nondet n = NODES.filter(n => status.get(n) == DOWN).oneOf()
      all {
        status' = status.set(n, UP),
        downFor' = downFor.set(n, 0),
        memberView' = memberView, copies' = copies, invView' = invView, deleted' = deleted,
        calm' = 0, repairCost' = repairCost, goneCnt' = goneCnt, corruptCnt' = corruptCnt,
      }
    }
  }

  // Permanent loss: disk destroyed, all its copies die with it. To the
  // rest of the mesh this is indistinguishable from sleep until the
  // decay gate expires — the view keeps the ghost for DELTA ticks.
  action envDepartN(n: int): bool = all {
    goneCnt < MAX_GONE,
    status.get(n) != GONE,
    extant.size() > 1,
    status' = status.set(n, GONE),
    downFor' = downFor.set(n, if (isUp(n)) 0 else downFor.get(n)),
    copies' = 0.to(N_FRAGS - 1).mapBy(f => copies.get(f).exclude(Set(n))),
    memberView' = memberView, invView' = invView, deleted' = deleted,
    calm' = 0, repairCost' = repairCost, goneCnt' = goneCnt + 1, corruptCnt' = corruptCnt,
  }

  action envDepart = all {
    goneCnt < MAX_GONE,
    extant.size() > 1,
    { nondet n = extant.oneOf()
      envDepartN(n) }
  }

  // Media corruption: one copy of one class silently dies.
  action envCorrupt = all {
    corruptCnt < MAX_CORRUPT,
    frags.filter(f => copies.get(f).size() > 0).size() > 0,
    {
      nondet f = frags.filter(f => copies.get(f).size() > 0).oneOf()
      nondet n = copies.get(f).oneOf()
      all {
        copies' = copies.set(f, copies.get(f).exclude(Set(n))),
        status' = status, downFor' = downFor, memberView' = memberView, invView' = invView,
        deleted' = deleted, calm' = 0, repairCost' = repairCost,
        goneCnt' = goneCnt, corruptCnt' = corruptCnt + 1,
      }
    }
  }

  // Disk-pressure eviction (decision 4): pressure timing is the
  // environment's; WHAT may be evicted is policy: only surplus (never
  // the responsible copy), and only if the (possibly STALE) inventory
  // view claims another holder exists — or the blob is deleted (GC).
  action envEvict = all {
    frags.exists(f => copies.get(f).exists(n =>
      isUp(n) and n != resp(f)
        and (deleted or invView.get(f).exclude(Set(n)).size() > 0))),
    {
      nondet f = frags.filter(f => copies.get(f).exists(n =>
        isUp(n) and n != resp(f)
          and (deleted or invView.get(f).exclude(Set(n)).size() > 0))).oneOf()
      nondet n = copies.get(f).filter(n =>
        isUp(n) and n != resp(f)
          and (deleted or invView.get(f).exclude(Set(n)).size() > 0)).oneOf()
      all {
        copies' = copies.set(f, copies.get(f).exclude(Set(n))),
        status' = status, downFor' = downFor, memberView' = memberView, invView' = invView,
        deleted' = deleted, calm' = 0, repairCost' = repairCost,
        goneCnt' = goneCnt, corruptCnt' = corruptCnt,
      }
    }
  }

  action envDelete = all {
    not(deleted),
    deleted' = true,
    status' = status, downFor' = downFor, memberView' = memberView, copies' = copies,
    invView' = invView, calm' = 0, repairCost' = repairCost,
    goneCnt' = goneCnt, corruptCnt' = corruptCnt,
  }

  // === The engine: one deterministic prioritized tick ===
  // Down-clocks advance every tick; the membership view drops a node
  // only after DELTA ticks non-Up (the decay gate) and re-admits Up
  // nodes immediately. Re-encode fires only for HOPELESS classes (no
  // copy inside the decay window) — or as soon as live classes sink
  // below the W watermark (urgency floor).
  val nextDownFor = NODES.mapBy(n =>
    if (isUp(n)) 0
    else if (downFor.get(n) < DELTA) downFor.get(n) + 1
    else downFor.get(n))

  val viewTarget = NODES.filter(n => isUp(n) or downFor.get(n) < DELTA)

  val pullNeedy = frags.filter(f =>
    not(copies.get(f).contains(resp(f)))
      and isUp(resp(f))
      and copies.get(f).intersect(upNodes).size() > 0)

  val liveClassCount = frags.filter(f => copies.get(f).intersect(upNodes).size() > 0).size()

  // Dead = no live copy. Hopeless = additionally no copy that might
  // come back inside the decay window. Urgency overrides hope.
  val deadClasses = frags.filter(f => copies.get(f).intersect(upNodes).size() == 0)
  val reencodeReady = deadClasses.filter(f =>
    isUp(resp(f)) and (
      liveClassCount < W
        or copies.get(f).filter(n => status.get(n) == DOWN and downFor.get(n) < DELTA).size() == 0))

  action engineTick = all {
    status' = status,
    deleted' = deleted,
    goneCnt' = goneCnt,
    corruptCnt' = corruptCnt,
    calm' = calm + 1,
    downFor' = nextDownFor,
    // Prioritized single-mutation body over (memberView, invView, copies, repairCost):
    if (memberView != viewTarget and viewTarget.size() > 0) all {
      memberView' = viewTarget,
      invView' = invView, copies' = copies, repairCost' = repairCost,
    } else if (invView != copies) all {
      invView' = copies,
      memberView' = memberView, copies' = copies, repairCost' = repairCost,
    } else if (AUTO_PULL and not(deleted) and pullNeedy.size() > 0) all {
      copies' = copies.set(least(pullNeedy),
        copies.get(least(pullNeedy)).union(Set(resp(least(pullNeedy))))),
      repairCost' = repairCost + 1,
      memberView' = memberView, invView' = invView,
    } else if (AUTO_REENCODE and not(deleted)
        and reencodeReady.size() > 0 and liveClassCount >= K) all {
      copies' = copies.set(least(reencodeReady),
        copies.get(least(reencodeReady)).union(Set(resp(least(reencodeReady))))),
      repairCost' = repairCost + K,
      memberView' = memberView, invView' = invView,
    } else all {
      memberView' = memberView, invView' = invView,
      copies' = copies, repairCost' = repairCost,
    }
  }

  action step = any {
    envSleep, envWake, envDepart, envCorrupt,
    envEvict, envDelete,
    engineTick,
  }

  // === Properties ===
  // INV-DURABLE — core safety under the fault budget.
  val safety = deleted or durable

  // INV-RECOVER — bounded self-healing: an uninterrupted engine window
  // restores full conformance. Discriminates the policy variants:
  // AUTO_PULL/AUTO_REENCODE = false (shipped policy) violates this.
  val recovery = deleted or calm < CALM_BOUND or conformant

  // INV-SPREAD — even-spread bridge lemma (feeds the 1/3-vs-2/3
  // membership-lag slack argument). Expected: holds for variants 0
  // and 2; plain HRW (1) violates it.
  val spread = memberView.size() == 0 or memberView.forall(m =>
    frags.filter(f => resp(f) == m).size()
      <= (N_FRAGS + memberView.size() - 1) / memberView.size())

  val safetyAndRecovery = safety and recovery
}

// ============================================================
// Config instances. Placement is injected as a lookup table (const
// PLACE). `run`/`test` configs BUILD the table with placement::
// buildTable (Rust backend computes the arithmetic once). The `verify`
// config passes a LITERAL table so Apalache never touches the
// arithmetic. `scaled_*` = exhaustive size (K=2, R=4 — 1:2 RS ratio);
// `full` = real parameters (K=10, 30 classes) for deep simulation.
// ============================================================

// Shared scaled parameters (Rust-backend configs).
module scaled_params {
  import placement.*
  pure val NODES4 = Set(1, 2, 3, 4)
  pure val WEIGHT4 = Map(1 -> 3, 2 -> 2, 3 -> 1, 4 -> 1)
}

// Target policy, scaled, modulo placement.
module scaled {
  import placement.*
  import scaled_params.*
  import storage_policy(
    NODES = NODES4, N_FRAGS = 6, K = 2,
    PLACE = buildTable(0, 6, NODES4, WEIGHT4),
    AUTO_PULL = true, AUTO_REENCODE = true,
    MAX_GONE = 1, MAX_CORRUPT = 1, CALM_BOUND = 13, DELTA = 3, W = 3
  ).*

  // WITNESS: permanent node loss heals fully (view decays the ghost,
  // re-encode regenerates the dead classes) within the calm bound.
  run healsAfterDepartTest =
    init.then(envDepart).then(18.reps(_ => engineTick))
      .expect(conformant and durable)

  // WITNESS (COST-SLEEP, decision 11): a sleep/wake cycle inside the
  // decay window moves NO responsibility and costs ZERO repair units —
  // the laptop scenario is free.
  run sleepWakeZeroCostTest =
    init.then(envSleep).then(2.reps(_ => engineTick)).then(envWake)
      .then(6.reps(_ => engineTick))
      .expect(conformant and durable and repairCost == 0)

  // WITNESS (COST-BOUND, decision 11): healing a depart costs exactly
  // K per class the departed node owned PLUS 1 per surviving class the
  // placement function needlessly renumbered — the modulo tax.
  run departCostExactTest = {
    val owned1 = 0.to(5).filter(f => placeVariant(0, f, 6, Set(1, 2, 3, 4), WEIGHT4) == 1)
    val movedSurvivors = 0.to(5).filter(f =>
      placeVariant(0, f, 6, Set(1, 2, 3, 4), WEIGHT4) != 1
        and placeVariant(0, f, 6, Set(2, 3, 4), WEIGHT4)
          != placeVariant(0, f, 6, Set(1, 2, 3, 4), WEIGHT4))
    init.then(envDepartN(1)).then(18.reps(_ => engineTick))
      .expect(conformant and repairCost == 2 * owned1.size() + movedSurvivors.size())
  }

  // WITNESS: modulo placement renumbers classes whose owner SURVIVED
  // when the member set shrinks (the cost capped-HRW avoids).
  run moduloRenumberingTest =
    init.expect(0.to(5).exists(f =>
      placeVariant(0, f, 6, Set(1, 2, 3, 4), WEIGHT4) != 2
        and placeVariant(0, f, 6, Set(1, 3, 4), WEIGHT4)
          != placeVariant(0, f, 6, Set(1, 2, 3, 4), WEIGHT4)))
}

// Target policy, scaled, plain HRW placement (kept for the spread
// counterexample — NOT an adoption candidate).
module scaled_hrw {
  import placement.*
  import scaled_params.*
  import storage_policy(
    NODES = NODES4, N_FRAGS = 6, K = 2,
    PLACE = buildTable(1, 6, NODES4, WEIGHT4),
    AUTO_PULL = true, AUTO_REENCODE = true,
    MAX_GONE = 1, MAX_CORRUPT = 1, CALM_BOUND = 13, DELTA = 3, W = 3
  ).*

  // WITNESS: HRW moves NOTHING whose owner survived a member removal
  // (minimal-movement property, by construction of rendezvous hashing).
  run hrwMinimalMovementTest =
    init.expect(0.to(5).forall(f =>
      placeVariant(1, f, 6, Set(1, 2, 3, 4), WEIGHT4) != 2
        implies placeVariant(1, f, 6, Set(1, 3, 4), WEIGHT4)
          == placeVariant(1, f, 6, Set(1, 2, 3, 4), WEIGHT4)))

  run healsAfterDepartHrwTest =
    init.then(envDepart).then(18.reps(_ => engineTick))
      .expect(conformant and durable)
}

// Target policy, scaled, CAPPED HRW — the adoption candidate: minimal
// movement (modulo tax gone) AND the spread lemma restored.
module scaled_chrw {
  import placement.*
  import scaled_params.*
  import storage_policy(
    NODES = NODES4, N_FRAGS = 6, K = 2,
    PLACE = buildTable(2, 6, NODES4, WEIGHT4),
    AUTO_PULL = true, AUTO_REENCODE = true,
    MAX_GONE = 1, MAX_CORRUPT = 1, CALM_BOUND = 13, DELTA = 3, W = 3
  ).*

  run healsAfterDepartChrwTest =
    init.then(envDepart).then(18.reps(_ => engineTick))
      .expect(conformant and durable)

  run sleepWakeZeroCostChrwTest =
    init.then(envSleep).then(2.reps(_ => engineTick)).then(envWake)
      .then(6.reps(_ => engineTick))
      .expect(conformant and durable and repairCost == 0)

  // COST-BOUND for capped HRW: same exact-cost accounting as modulo's
  // witness — the doc compares the two movedSurvivors terms.
  run departCostExactChrwTest = {
    val owned1 = 0.to(5).filter(f => placeVariant(2, f, 6, Set(1, 2, 3, 4), WEIGHT4) == 1)
    val movedSurvivors = 0.to(5).filter(f =>
      placeVariant(2, f, 6, Set(1, 2, 3, 4), WEIGHT4) != 1
        and placeVariant(2, f, 6, Set(2, 3, 4), WEIGHT4)
          != placeVariant(2, f, 6, Set(1, 2, 3, 4), WEIGHT4))
    init.then(envDepartN(1)).then(18.reps(_ => engineTick))
      .expect(conformant and repairCost == 2 * owned1.size() + movedSurvivors.size())
  }
}

// NEG: the currently-shipped policy — no automatic repair of any kind.
// Expected: safety holds within budget, recovery VIOLATED.
module scaled_neg {
  import placement.*
  import scaled_params.*
  import storage_policy(
    NODES = NODES4, N_FRAGS = 6, K = 2,
    PLACE = buildTable(0, 6, NODES4, WEIGHT4),
    AUTO_PULL = false, AUTO_REENCODE = false,
    MAX_GONE = 1, MAX_CORRUPT = 1, CALM_BOUND = 13, DELTA = 3, W = 3
  ).*

  // NEG WITNESS: with the shipped policy a permanent loss NEVER heals —
  // this trace is the motivating counterexample for RFC-018.
  run shippedPolicyNeverHealsTest =
    init.then(envDepart).then(20.reps(_ => engineTick))
      .expect(not(conformant))
}

// Tier-1 only: pull repair without re-encode (the RFC-014 engine if it
// were automated). Expected: recovery VIOLATED on permanent loss.
module scaled_tier1 {
  import placement.*
  import scaled_params.*
  import storage_policy(
    NODES = NODES4, N_FRAGS = 6, K = 2,
    PLACE = buildTable(0, 6, NODES4, WEIGHT4),
    AUTO_PULL = true, AUTO_REENCODE = false,
    MAX_GONE = 1, MAX_CORRUPT = 1, CALM_BOUND = 13, DELTA = 3, W = 3
  ).*

  // NEG WITNESS: pull-only repair heals membership moves but CANNOT
  // heal permanent loss — dead classes have no pull source. Re-encode
  // is necessary, not optional.
  run tier1CannotHealDepartTest =
    init.then(envDepart).then(20.reps(_ => engineTick))
      .expect(not(conformant))

  // FINDING (2026-07-13): in a single-copy-per-fragment design, pull
  // repair cannot heal a decayed sleeper's reassigned classes either —
  // the sole copy IS on the sleeper, there is no live pull source.
  // Pull's only value is planned migration (weight/membership moves
  // with the old holder still alive). ALL fault repair needs re-encode.
  run tier1CannotHealSleepReassignTest =
    init.then(envSleep).then(20.reps(_ => engineTick))
      .expect(frags.exists(f =>
        isUp(resp(f)) and copies.get(f).intersect(upNodes).size() == 0))
}

// EXHAUSTIVE VERIFICATION config (Apalache). Placement is a LITERAL
// capped-HRW table (VARIANT 2 over Set(1,2,3,4), WEIGHT 3/2/1/1),
// precomputed offline — no arithmetic for the SMT backend. Keep this
// table in sync with placement::buildTable(2, 6, Set(1,2,3,4), W): if
// the placement functions change, regenerate (see spec/README.md).
module verify_table {
  pure val CHRW_TABLE: Set[int] -> (int -> int) = Map(
    Set(1)          -> Map(0->1, 1->1, 2->1, 3->1, 4->1, 5->1),
    Set(2)          -> Map(0->2, 1->2, 2->2, 3->2, 4->2, 5->2),
    Set(3)          -> Map(0->3, 1->3, 2->3, 3->3, 4->3, 5->3),
    Set(4)          -> Map(0->4, 1->4, 2->4, 3->4, 4->4, 5->4),
    Set(1,2)        -> Map(0->2, 1->1, 2->1, 3->1, 4->2, 5->2),
    Set(1,3)        -> Map(0->1, 1->1, 2->1, 3->3, 4->3, 5->3),
    Set(1,4)        -> Map(0->1, 1->4, 2->1, 3->1, 4->4, 5->4),
    Set(2,3)        -> Map(0->2, 1->2, 2->2, 3->3, 4->3, 5->3),
    Set(2,4)        -> Map(0->2, 1->4, 2->2, 3->2, 4->4, 5->4),
    Set(3,4)        -> Map(0->3, 1->4, 2->4, 3->4, 4->3, 5->3),
    Set(1,2,3)      -> Map(0->2, 1->1, 2->1, 3->2, 4->3, 5->3),
    Set(1,2,4)      -> Map(0->2, 1->4, 2->1, 3->1, 4->2, 5->4),
    Set(1,3,4)      -> Map(0->1, 1->4, 2->1, 3->4, 4->3, 5->3),
    Set(2,3,4)      -> Map(0->2, 1->4, 2->2, 3->4, 4->3, 5->3),
    Set(1,2,3,4)    -> Map(0->2, 1->4, 2->1, 3->1, 4->3, 5->2)
  )
}

module scaled_verify {
  import verify_table.*
  import storage_policy(
    NODES = Set(1, 2, 3, 4), N_FRAGS = 6, K = 2, PLACE = CHRW_TABLE,
    AUTO_PULL = true, AUTO_REENCODE = true,
    MAX_GONE = 1, MAX_CORRUPT = 1, CALM_BOUND = 13, DELTA = 3, W = 3
  ).*
}

// Drift guard: the hand-pasted verify table MUST equal what the
// placement functions generate. Run on the Rust backend (`quint test
// --main table_guard`); if it fails, regenerate CHRW_TABLE.
module table_guard {
  import placement.*
  import verify_table.*
  var tick: int
  action init = tick' = 0
  action step = tick' = tick + 1
  run tableMatchesGeneratorTest =
    init.expect(CHRW_TABLE
      == buildTable(2, 6, Set(1, 2, 3, 4), Map(1 -> 3, 2 -> 2, 3 -> 1, 4 -> 1)))

  // BRIDGE lemma, proved by COMPLETE enumeration (all member views V,
  // all online subsets U with |U| >= 2/3 |V|) — stronger than a
  // reachable-state check: consensus-live membership + conformant
  // placement keeps >= K classes reconstructible from online nodes.
  // This is the 1/3-vs-2/3 slack that lets storage lag membership: any
  // trajectory that keeps consensus live keeps data available, however
  // stale the storage layer's reaction is, PROVIDED conformance was
  // reached for the current view (surplus copies only strengthen it).
  run bridgeLemmaScaledTest =
    init.expect(
      Set(1, 2, 3, 4).powerset().exclude(Set(Set())).forall(v =>
        v.powerset().filter(u => u.size() * 3 >= v.size() * 2).forall(u =>
          0.to(5).filter(f => u.contains(CHRW_TABLE.get(v).get(f))).size() >= 2)))

  // Same lemma at REAL parameters (K=10, 30 classes, 10 nodes,
  // capped-HRW): every view, every 2/3-online subset.
  run bridgeLemmaFullTest = {
    val w10 = Map(1 -> 3, 2 -> 3, 3 -> 2, 4 -> 2, 5 -> 1,
                  6 -> 1, 7 -> 1, 8 -> 1, 9 -> 1, 10 -> 1)
    val table = buildTable(2, 30, 1.to(10), w10)
    init.expect(
      1.to(10).powerset().exclude(Set(Set())).forall(v =>
        v.powerset().filter(u => u.size() * 3 >= v.size() * 2).forall(u =>
          0.to(29).filter(f => u.contains(table.get(v).get(f))).size() >= 10)))
  }
}

// Full-scale parameters (K=10, 30 classes, 10 nodes) for `quint run`.
// Capped-HRW table over the powerset (built once on the Rust backend).
module full {
  import placement.*
  pure val NODES10 = 1.to(10)
  pure val WEIGHT10 = Map(1 -> 3, 2 -> 3, 3 -> 2, 4 -> 2, 5 -> 1,
                          6 -> 1, 7 -> 1, 8 -> 1, 9 -> 1, 10 -> 1)
  import storage_policy(
    NODES = NODES10, N_FRAGS = 30, K = 10,
    PLACE = buildTable(2, 30, NODES10, WEIGHT10),
    AUTO_PULL = true, AUTO_REENCODE = true,
    MAX_GONE = 2, MAX_CORRUPT = 3, CALM_BOUND = 50, DELTA = 5, W = 20
  ).*
}
