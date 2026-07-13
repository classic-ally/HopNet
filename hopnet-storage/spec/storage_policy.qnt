// RFC-018: storage durability & placement policy — normative model.
//
// Models ONE chunk of one blob (file-correlated placement makes all
// chunks of a file share fate for node-level faults; per-chunk media
// corruption is the corrupt action). Fragment classes 0..N_FRAGS-1;
// any K live classes reconstruct the chunk.
//
// Copy classes (RFC-018 decision 1): RESPONSIBLE = placement output
// (computed, never stored); SURPLUS = any other copy. INTERESTED
// (projection pinning) is deliberately absent from v1 — it only
// strengthens eviction guards, so its omission is conservative.
//
// The engine is a DETERMINISTIC control loop (engineTick): sync
// membership view -> refresh inventory snapshot -> pull one missing
// responsible copy -> re-encode one dead class. The environment is
// fully adversarial within fault budgets.

module storage_policy {
  // === Constants (fixed per config instance) ===
  const NODES: Set[int]           // node id universe
  const N_FRAGS: int              // fragment classes per chunk (30 real)
  const K: int                    // RS reconstruction threshold (10 real)
  const WEIGHT: int -> int        // damped placement weights (quantized metrics)
  const USE_HRW: bool             // placement variant: HRW vs select+modulo
  const AUTO_PULL: bool           // policy: automatic tier-1 pull repair
  const AUTO_REENCODE: bool       // policy: scrub re-encode of dead classes
  const MAX_GONE: int             // fault budget: permanent node losses
  const MAX_CORRUPT: int          // fault budget: single-copy corruptions
  const CALM_BOUND: int           // recovery deadline in engine ticks

  type Status = int               // 0 = Up, 1 = Down (copies intact), 2 = Gone (disk lost)
  pure val UP = 0
  pure val DOWN = 1
  pure val GONE = 2

  // === State ===
  var status: int -> Status       // ground truth per node
  var memberView: Set[int]        // consensus's damped membership view (lags status)
  var copies: int -> Set[int]     // frag class -> nodes holding a live copy
  var invView: int -> Set[int]    // stale inventory snapshot (drives eviction guards)
  var deleted: bool               // blob deleted at consensus (one-way)
  var calm: int                   // consecutive engine ticks without env interference
  var goneCnt: int
  var corruptCnt: int

  // === Derived ===
  val frags = 0.to(N_FRAGS - 1)
  def isUp(n: int): bool = status.get(n) == UP
  val upNodes = NODES.filter(n => isUp(n))
  val extant = NODES.filter(n => status.get(n) != GONE)

  // --- Placement variant A: current select-then-modulo (small-net path:
  // all members, sorted by id, class f -> members[f mod len]) ---
  pure def minOf(s: Set[int]): int = s.fold(1000000, (a, x) => if (x < a) x else a)

  pure def nthSmallest(s: Set[int], n: int): int =
    range(0, n + 1).foldl((s, -1), (acc, _i) => {
      val m = minOf(acc._1)
      (acc._1.exclude(Set(m)), m)
    })._2

  pure def placeModulo(f: int, members: Set[int]): int =
    nthSmallest(members, f % members.size())

  // --- Placement variant B: weighted rendezvous (HRW). Deterministic
  // integer mix stands in for blake3; properties under check do not
  // depend on hash quality, only determinism. Lower score wins;
  // higher weight shrinks score (more responsibility). ---
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

  def resp(f: int): int =
    if (USE_HRW) placeHrw(f, memberView, WEIGHT) else placeModulo(f, memberView)

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
    memberView' = NODES,
    copies' = 0.to(N_FRAGS - 1).mapBy(f =>
      Set(if (USE_HRW) placeHrw(f, NODES, WEIGHT) else placeModulo(f, NODES))),
    invView' = 0.to(N_FRAGS - 1).mapBy(f =>
      Set(if (USE_HRW) placeHrw(f, NODES, WEIGHT) else placeModulo(f, NODES))),
    deleted' = false,
    calm' = 0,
    goneCnt' = 0,
    corruptCnt' = 0,
  }

  // === Environment actions (adversarial; all reset calm) ===
  action envSleep = all {
    upNodes.size() > 1,
    {
      nondet n = upNodes.oneOf()
      all {
        status' = status.set(n, DOWN),
        memberView' = memberView, copies' = copies, invView' = invView,
        deleted' = deleted, calm' = 0, goneCnt' = goneCnt, corruptCnt' = corruptCnt,
      }
    }
  }

  action envWake = all {
    NODES.filter(n => status.get(n) == DOWN).size() > 0,
    {
      nondet n = NODES.filter(n => status.get(n) == DOWN).oneOf()
      all {
        status' = status.set(n, UP),
        memberView' = memberView, copies' = copies, invView' = invView,
        deleted' = deleted, calm' = 0, goneCnt' = goneCnt, corruptCnt' = corruptCnt,
      }
    }
  }

  // Permanent loss: disk destroyed, all its copies die with it.
  action envDepart = all {
    goneCnt < MAX_GONE,
    extant.size() > 1,
    {
      nondet n = extant.oneOf()
      all {
        status' = status.set(n, GONE),
        copies' = 0.to(N_FRAGS - 1).mapBy(f => copies.get(f).exclude(Set(n))),
        memberView' = memberView, invView' = invView,
        deleted' = deleted, calm' = 0, goneCnt' = goneCnt + 1, corruptCnt' = corruptCnt,
      }
    }
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
        status' = status, memberView' = memberView, invView' = invView,
        deleted' = deleted, calm' = 0, goneCnt' = goneCnt, corruptCnt' = corruptCnt + 1,
      }
    }
  }

  // Consensus membership drift: view may only drop non-Up nodes and
  // admit Up nodes (the rely-condition on the consensus module's
  // future deactivation logic). Timing is adversarial.
  action envViewDrop = all {
    memberView.filter(n => not(isUp(n))).size() > 0,
    memberView.size() > 1,
    {
      nondet n = memberView.filter(n => not(isUp(n))).oneOf()
      all {
        memberView' = memberView.exclude(Set(n)),
        status' = status, copies' = copies, invView' = invView,
        deleted' = deleted, calm' = 0, goneCnt' = goneCnt, corruptCnt' = corruptCnt,
      }
    }
  }

  action envViewAdd = all {
    upNodes.exclude(memberView).size() > 0,
    {
      nondet n = upNodes.exclude(memberView).oneOf()
      all {
        memberView' = memberView.union(Set(n)),
        status' = status, copies' = copies, invView' = invView,
        deleted' = deleted, calm' = 0, goneCnt' = goneCnt, corruptCnt' = corruptCnt,
      }
    }
  }

  // Disk-pressure eviction (RFC-018 decision 4): pressure timing is the
  // environment's, WHAT may be evicted is policy: only surplus (not the
  // responsible copy), and only if the (possibly STALE) inventory view
  // claims another holder exists — or the blob is deleted (GC).
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
        status' = status, memberView' = memberView, invView' = invView,
        deleted' = deleted, calm' = 0, goneCnt' = goneCnt, corruptCnt' = corruptCnt,
      }
    }
  }

  action envDelete = all {
    not(deleted),
    deleted' = true,
    status' = status, memberView' = memberView, copies' = copies,
    invView' = invView, calm' = 0, goneCnt' = goneCnt, corruptCnt' = corruptCnt,
  }

  // === The engine: one deterministic prioritized tick ===
  // 1. membership view sync  2. inventory self-check
  // 3. pull one missing responsible copy (tier-1 repair)
  // 4. re-encode one dead class (scrub)  5. idle
  val viewTarget = upNodes
  val pullNeedy = frags.filter(f =>
    not(copies.get(f).contains(resp(f)))
      and isUp(resp(f))
      and copies.get(f).intersect(upNodes).size() > 0)
  val deadClasses = frags.filter(f => copies.get(f).intersect(upNodes).size() == 0)
  val liveClassCount = frags.filter(f => copies.get(f).intersect(upNodes).size() > 0).size()

  action engineTick =
    if (memberView != viewTarget and viewTarget.size() > 0) all {
      memberView' = viewTarget,
      status' = status, copies' = copies, invView' = invView,
      deleted' = deleted, calm' = calm + 1, goneCnt' = goneCnt, corruptCnt' = corruptCnt,
    } else if (invView != copies) all {
      invView' = copies,
      status' = status, memberView' = memberView, copies' = copies,
      deleted' = deleted, calm' = calm + 1, goneCnt' = goneCnt, corruptCnt' = corruptCnt,
    } else if (AUTO_PULL and not(deleted) and pullNeedy.size() > 0) all {
      copies' = copies.set(minOf(pullNeedy),
        copies.get(minOf(pullNeedy)).union(Set(resp(minOf(pullNeedy))))),
      status' = status, memberView' = memberView, invView' = invView,
      deleted' = deleted, calm' = calm + 1, goneCnt' = goneCnt, corruptCnt' = corruptCnt,
    } else if (AUTO_REENCODE and not(deleted)
        and deadClasses.size() > 0 and liveClassCount >= K
        and isUp(resp(minOf(deadClasses)))) all {
      copies' = copies.set(minOf(deadClasses),
        copies.get(minOf(deadClasses)).union(Set(resp(minOf(deadClasses))))),
      status' = status, memberView' = memberView, invView' = invView,
      deleted' = deleted, calm' = calm + 1, goneCnt' = goneCnt, corruptCnt' = corruptCnt,
    } else all {
      status' = status, memberView' = memberView, copies' = copies,
      invView' = invView, deleted' = deleted, calm' = calm + 1,
      goneCnt' = goneCnt, corruptCnt' = corruptCnt,
    }

  action step = any {
    envSleep, envWake, envDepart, envCorrupt,
    envViewDrop, envViewAdd, envEvict, envDelete,
    engineTick,
  }

  // === Properties ===
  // INV-DURABLE — core safety under the fault budget.
  val safety = deleted or durable

  // INV-RECOVER — bounded self-healing: an uninterrupted engine window
  // restores full conformance. Discriminates the policy variants:
  // AUTO_PULL/AUTO_REENCODE=false (shipped policy) violates this.
  val recovery = deleted or calm < CALM_BOUND or conformant

  // INV-SPREAD — even-spread bridge lemma (feeds the 1/3-vs-2/3
  // membership-lag slack argument).
  val spread = memberView.size() == 0 or memberView.forall(m =>
    frags.filter(f => resp(f) == m).size()
      <= (N_FRAGS + memberView.size() - 1) / memberView.size())

  val safetyAndRecovery = safety and recovery
}

// ============================================================
// Config instances. `scaled_*` = exhaustive verification size
// (K=2, R=4 — preserves the 1:2 RS ratio). `full` = real
// parameters for deep random simulation.
// ============================================================

// Target policy, scaled, modulo placement.
module scaled {
  import storage_policy(
    NODES = Set(1, 2, 3, 4),
    N_FRAGS = 6,
    K = 2,
    WEIGHT = Map(1 -> 3, 2 -> 2, 3 -> 1, 4 -> 1),
    USE_HRW = false,
    AUTO_PULL = true,
    AUTO_REENCODE = true,
    MAX_GONE = 1,
    MAX_CORRUPT = 1,
    CALM_BOUND = 16
  ).*

  // WITNESS: permanent node loss heals fully (pull for survivors'
  // classes + re-encode for the dead ones) within the calm bound.
  run healsAfterDepartTest =
    init.then(envDepart).then(16.reps(_ => engineTick))
      .expect(conformant and durable)

  // WITNESS: sleep/wake cycle needs no repair at all — copies survive,
  // the returning node's copies count again (laptop scenario).
  run sleepWakeNoRepairTest =
    init.then(envSleep).then(4.reps(_ => engineTick)).then(envWake)
      .then(8.reps(_ => engineTick))
      .expect(conformant and durable)

  // WITNESS: modulo placement moves classes whose owner SURVIVED when
  // the member set shrinks (the renumbering cost HRW avoids).
  run moduloRenumberingTest =
    init.expect(frags.exists(f =>
      placeModulo(f, Set(1, 2, 3, 4)) != 2
        and placeModulo(f, Set(1, 3, 4)) != placeModulo(f, Set(1, 2, 3, 4))))
}

// Target policy, scaled, HRW placement.
module scaled_hrw {
  import storage_policy(
    NODES = Set(1, 2, 3, 4),
    N_FRAGS = 6,
    K = 2,
    WEIGHT = Map(1 -> 3, 2 -> 2, 3 -> 1, 4 -> 1),
    USE_HRW = true,
    AUTO_PULL = true,
    AUTO_REENCODE = true,
    MAX_GONE = 1,
    MAX_CORRUPT = 1,
    CALM_BOUND = 16
  ).*

  // WITNESS: HRW moves NOTHING whose owner survived a member removal
  // (minimal-movement property, by construction of rendezvous hashing).
  run hrwMinimalMovementTest =
    init.expect(frags.forall(f =>
      placeHrw(f, Set(1, 2, 3, 4), WEIGHT) != 2
        implies placeHrw(f, Set(1, 3, 4), WEIGHT) == placeHrw(f, Set(1, 2, 3, 4), WEIGHT)))

  run healsAfterDepartHrwTest =
    init.then(envDepart).then(16.reps(_ => engineTick))
      .expect(conformant and durable)
}

// NEG: the currently-shipped policy — no automatic repair of any kind.
// Expected: safety holds within budget, recovery VIOLATED.
module scaled_neg {
  import storage_policy(
    NODES = Set(1, 2, 3, 4),
    N_FRAGS = 6,
    K = 2,
    WEIGHT = Map(1 -> 3, 2 -> 2, 3 -> 1, 4 -> 1),
    USE_HRW = false,
    AUTO_PULL = false,
    AUTO_REENCODE = false,
    MAX_GONE = 1,
    MAX_CORRUPT = 1,
    CALM_BOUND = 16
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
  import storage_policy(
    NODES = Set(1, 2, 3, 4),
    N_FRAGS = 6,
    K = 2,
    WEIGHT = Map(1 -> 3, 2 -> 2, 3 -> 1, 4 -> 1),
    USE_HRW = false,
    AUTO_PULL = true,
    AUTO_REENCODE = false,
    MAX_GONE = 1,
    MAX_CORRUPT = 1,
    CALM_BOUND = 16
  ).*

  // NEG WITNESS: pull-only repair heals membership moves but CANNOT
  // heal permanent loss — dead classes have no pull source. Re-encode
  // is necessary, not optional.
  run tier1CannotHealDepartTest =
    init.then(envDepart).then(20.reps(_ => engineTick))
      .expect(not(conformant))

  // FINDING (2026-07-13): in a single-copy-per-fragment design, pull
  // repair cannot heal a sleeping node's reassigned classes either —
  // the sole copy IS on the sleeper, there is no live pull source.
  // Pull's only value is planned migration (weight/membership moves
  // with the old holder still alive). ALL fault repair needs re-encode.
  run tier1CannotHealSleepReassignTest =
    init.then(envSleep).then(20.reps(_ => engineTick))
      .expect(frags.exists(f =>
        isUp(resp(f)) and copies.get(f).intersect(upNodes).size() == 0))
}

// Full-scale parameters (K=10, 30 classes, 10 nodes) for `quint run`.
module full {
  import storage_policy(
    NODES = 1.to(10),
    N_FRAGS = 30,
    K = 10,
    WEIGHT = Map(1 -> 3, 2 -> 3, 3 -> 2, 4 -> 2, 5 -> 1,
                 6 -> 1, 7 -> 1, 8 -> 1, 9 -> 1, 10 -> 1),
    USE_HRW = true,
    AUTO_PULL = true,
    AUTO_REENCODE = true,
    MAX_GONE = 2,
    MAX_CORRUPT = 3,
    CALM_BOUND = 45
  ).*
}
