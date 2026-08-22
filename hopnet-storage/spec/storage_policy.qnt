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

  // Balanced capped rendezvous: each class goes to the best-scoring
  // member among the CURRENTLY LEAST-LOADED (load-tiered, rendezvous
  // within the tier). Pins loads to the pigeonhole-optimal profile —
  // exactly (nfrags mod |members|... i.e. nfrags - v*(c-1)) members at
  // cap c, the rest at c-1 — which both bounds adversarial burst loss
  // tightly (no zero-load members) and makes W(v) closed-form.
  pure def placeBalancedAll(nfrags: int, members: Set[int], w: int -> int): int -> int =
    range(0, nfrags).foldl(
      (members.mapBy(n => 0), 0.to(nfrags - 1).mapBy(f => -1)),
      (acc, f) => {
        val counts = acc._1
        val minLoad = members.fold(nfrags, (a, m) =>
          if (counts.get(m) < a) counts.get(m) else a)
        val eligible = members.filter(m => counts.get(m) == minLoad)
        val chosen = eligible.fold(-1, (best, n) =>
          if (best == -1) n
          else if (hrwScore(n, f, w) < hrwScore(best, f, w)) n
          else if (hrwScore(n, f, w) == hrwScore(best, f, w) and n < best) n
          else best)
        (counts.set(chosen, counts.get(chosen) + 1), acc._2.set(f, chosen))
      })._2

  pure def placeVariant(v: int, f: int, nfrags: int, members: Set[int], w: int -> int): int =
    if (v == 0) placeModulo(f, members)
    else if (v == 1) placeHrw(f, members, w)
    else if (v == 2) placeCappedAll(nfrags, members, w).get(f)
    else placeBalancedAll(nfrags, members, w).get(f)

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
  const MAX_DROPS: int            // fault budget: lost local-state marks (RFC-STORAGE-003)
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
  // RFC-STORAGE-003 pipeline state: the (confirmed, target) pair. Views,
  // not heights — PLACE maps a member set straight to its assignment, so
  // the view snapshot is the whole meaning of a placement height here.
  var confirmedView: Set[int]     // view@placement_height; Set() encodes NULL (never confirmed)
  var targetView: Set[int]        // view@desired_placement_height; never empty
  // Per-class protected holders: the responsibility union of the
  // confirmed epoch and EVERY declared-but-unconfirmed epoch — the
  // model image of the transition record's snapshots in
  // (placement_height, desired], memoized per class exactly as the S2
  // Rust predicate will hold them. Grows at declare (union in the new
  // epoch's assignment), resets at confirm (obligations collapse onto
  // the confirmed epoch). Exists because protection must cover EVERY
  // in-flight destination, not just the newest: an S0 Apalache
  // counterexample showed a supersede-declare stripping the previous
  // destination's freshly pulled copy — possibly the only real bytes —
  // of protection, evictable through a ghost row. Obligations (and
  // protections) lapse only at confirm, never at declare. (Encoded as
  // the per-class memo rather than a set of views: a set-keyed
  // PLACE.get inside the eviction guard forces symbolic table lookups
  // that blow bounded checking from hours into days.)
  var protectedBy: int -> Set[int]
  var dropCnt: int                // lost marks spent (envDropMark)
  var evictHarm: bool             // INV-EVICT-SAFE history bit (see envEvict)

  // === Derived ===
  val frags = 0.to(N_FRAGS - 1)
  def isUp(n: int): bool = status.get(n) == UP
  val upNodes = NODES.filter(n => isUp(n))
  val extant = NODES.filter(n => status.get(n) != GONE)

  // Responsible node for class f = injected-table lookup. The table is
  // total over non-empty subsets; memberView is always non-empty.
  def resp(f: int): int = PLACE.get(memberView).get(f)

  // RFC-STORAGE-003: duties derive from the (confirmed, target) pair,
  // never from the live view — placement height is record, not filter.
  def respTarget(f: int): int = PLACE.get(targetView).get(f)

  // The protection predicate — THE eviction guard, to be transliterated
  // into one pure Rust function in S2 (reading the transition record's
  // memoized view snapshots). A copy of class f on node n is protected
  // iff any of:
  //   - the blob was never confirmed (every copy is load-bearing);
  //   - n is a protected holder: responsible under the confirmed view
  //     (standing obligation) or under ANY declared-but-unconfirmed
  //     epoch (the in-flight destinations; the current target always
  //     among them). Protection lapses only at confirm — a
  //     supersede-declare must never strip the previous destination's
  //     copy.
  // Pinning is implementation-only and deliberately absent here.
  def protected(f: int, n: int): bool =
    confirmedView == Set() or protectedBy.get(f).contains(n)

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
  // Conformant to the declared goal (RFC-STORAGE-003).
  val conformantToTarget = frags.forall(f => copies.get(f).contains(respTarget(f)))

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
    confirmedView' = NODES,
    targetView' = NODES,
    protectedBy' = 0.to(N_FRAGS - 1).mapBy(f => Set(PLACE.get(NODES).get(f))),
    dropCnt' = 0,
    evictHarm' = false,
  }

  // RFC-STORAGE-003 birth: every class held only by the uploading origin,
  // never confirmed, goal stamped at insert with the insert's own view.
  // Checked in a zero-depart-budget config (scaled_verify_birth): a
  // single-copy blob whose origin DISK dies pre-distribution is data loss
  // by nature — the upload durability window every system has until first
  // replication. Sleep, eviction pressure, corruption, and mark loss stay
  // fully adversarial here; only whole-disk death is out of budget.
  action initOriginHeldAt(origin: int): bool = all {
    status' = NODES.mapBy(n => UP),
    downFor' = NODES.mapBy(n => 0),
    memberView' = NODES,
    copies' = 0.to(N_FRAGS - 1).mapBy(f => Set(origin)),
    invView' = 0.to(N_FRAGS - 1).mapBy(f => Set(origin)),
    deleted' = false,
    calm' = 0,
    repairCost' = 0,
    goneCnt' = 0,
    corruptCnt' = 0,
    confirmedView' = Set(),
    targetView' = NODES,
    protectedBy' = 0.to(N_FRAGS - 1).mapBy(f => Set(PLACE.get(NODES).get(f))),
    dropCnt' = 0,
    evictHarm' = false,
  }

  action initOriginHeld = {
    nondet origin = NODES.oneOf()
    initOriginHeldAt(origin)
  }

  // RFC-STORAGE-003 generalized region init: exhaustive bounded checking
  // from EVERY protocol-consistent mid-pipeline neighborhood, not just
  // from birth — the poor-woman's inductive step (bounded depth explores
  // every neighborhood rather than the first steps after upload).
  // Constraints:
  //   - copies never sit on GONE disks; goneCnt matches the GONE count;
  //   - budget-robust durability over PROTECTED copies: the state must
  //     keep K + MAX_CORRUPT classes whose protection-predicate holder
  //     survives every remaining depart set. Only protected copies
  //     count — a class whose protectors are all gone or empty-handed
  //     is killable for FREE (its ghost rows launder surplus evictions,
  //     the fault that removed the protector already accounts for it in
  //     the burst math: see evictLaunderAccountingTest), so counting it
  //     as a survivor admits unreachable-break states, which is exactly
  //     the counterexample Apalache produced against the naive
  //     any-copy-counts constraint;
  //   - belief is honest-or-stale, never fabricated: an entry is either
  //     a real copy or a ghost row on a departed disk. Rows originate
  //     from holders' self-attestation (true at attestation; S5 makes
  //     them disk-verified), so fabricated positives are the lying-
  //     attester case, outside the perimeter — and admitting them here
  //     lets the checker kill classes for free (phantom-laundered
  //     confirm + eviction belt). Known under-coverage, documented: a
  //     legitimately evicted surplus leaves a stale-live entry until the
  //     next sync; such entries are excluded from this init and instead
  //     covered by the fault-anchoring argument and its witness.
  action initMidFlight = {
    nondet st = NODES.setOfMaps(Set(UP, DOWN, GONE)).oneOf()
    nondet df = NODES.setOfMaps(0.to(DELTA)).oneOf()
    nondet mv = NODES.powerset().exclude(Set(Set())).oneOf()
    nondet cps = frags.setOfMaps(NODES.powerset()).oneOf()
    nondet iv = frags.setOfMaps(NODES.powerset()).oneOf()
    nondet cv = NODES.powerset().oneOf()
    nondet tv = NODES.powerset().exclude(Set(Set())).oneOf()
    // In-flight epochs bounded to the target plus at most one extra for
    // tractability: a powerset-of-views draw explodes the symbolic init
    // (2^15 candidate sets at this scale — measured in days of solver
    // time). A chain of more than two unconfirmed epochs needs three
    // storage-view transitions inside one confirm cycle, which the
    // decay-gate hysteresis makes designed-rare; longer chains add one
    // protected assignment per epoch with no new mechanics, and are
    // exercised unboundedly by the simulations and witnesses.
    nondet pvA = NODES.powerset().exclude(Set(Set())).oneOf()
    nondet extraEpoch = Set(true, false).oneOf()
    val inflight =
      (if (cv == tv) Set() else Set(tv))
        .union(if (extraEpoch and cv != tv) Set(pvA) else Set())
    val pb = 0.to(N_FRAGS - 1).mapBy(f =>
      (if (cv != Set()) Set(PLACE.get(cv).get(f)) else Set())
        .union(inflight.map(v => PLACE.get(v).get(f))))
    all {
      NODES.filter(n => st.get(n) == GONE).size() <= MAX_GONE,
      frags.forall(f => cps.get(f).forall(n => st.get(n) != GONE)),
      frags.forall(f => iv.get(f).forall(n =>
        cps.get(f).contains(n) or st.get(n) == GONE)),
      NODES.powerset()
        .filter(d => d.size() <= MAX_GONE - NODES.filter(n => st.get(n) == GONE).size())
        .forall(d =>
          frags.filter(f => cps.get(f).exclude(d).exists(n =>
            cv == Set()
              or PLACE.get(cv).get(f) == n
              or pb.get(f).contains(n))).size() >= K + MAX_CORRUPT),
      status' = st,
      downFor' = df,
      memberView' = mv,
      copies' = cps,
      invView' = iv,
      deleted' = false,
      calm' = 0,
      repairCost' = 0,
      goneCnt' = NODES.filter(n => st.get(n) == GONE).size(),
      corruptCnt' = 0,
      confirmedView' = cv,
      targetView' = tv,
      protectedBy' = pb,
      dropCnt' = 0,
      evictHarm' = false,
    }
  }

  // === Environment actions (adversarial; all reset calm) ===
  action envSleepN(n: int): bool = all {
    isUp(n),
    upNodes.size() > 1,
    status' = status.set(n, DOWN),
    downFor' = downFor.set(n, 0),
    memberView' = memberView, copies' = copies, invView' = invView, deleted' = deleted,
    calm' = 0, repairCost' = repairCost, goneCnt' = goneCnt, corruptCnt' = corruptCnt,
    confirmedView' = confirmedView, targetView' = targetView,
    protectedBy' = protectedBy, dropCnt' = dropCnt, evictHarm' = evictHarm,
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
        confirmedView' = confirmedView, targetView' = targetView,
        protectedBy' = protectedBy, dropCnt' = dropCnt, evictHarm' = evictHarm,
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
    confirmedView' = confirmedView, targetView' = targetView,
    protectedBy' = protectedBy, dropCnt' = dropCnt, evictHarm' = evictHarm,
  }

  action envDepart = all {
    goneCnt < MAX_GONE,
    extant.size() > 1,
    { nondet n = extant.oneOf()
      envDepartN(n) }
  }

  // Media corruption: one copy of one class silently dies.
  action envCorruptN(f: int, n: int): bool = all {
    corruptCnt < MAX_CORRUPT,
    copies.get(f).contains(n),
    copies' = copies.set(f, copies.get(f).exclude(Set(n))),
    status' = status, downFor' = downFor, memberView' = memberView, invView' = invView,
    deleted' = deleted, calm' = 0, repairCost' = repairCost,
    goneCnt' = goneCnt, corruptCnt' = corruptCnt + 1,
    confirmedView' = confirmedView, targetView' = targetView, protectedBy' = protectedBy,
    dropCnt' = dropCnt, evictHarm' = evictHarm,
  }

  action envCorrupt = all {
    corruptCnt < MAX_CORRUPT,
    frags.filter(f => copies.get(f).size() > 0).size() > 0,
    {
      nondet f = frags.filter(f => copies.get(f).size() > 0).oneOf()
      nondet n = copies.get(f).oneOf()
      envCorruptN(f, n)
    }
  }

  // Disk-pressure eviction (decision 4): pressure timing is the
  // environment's; WHAT may be evicted is policy — the protection
  // predicate (RFC-STORAGE-003), with the attested-other-holder belief
  // check retained as a second belt, and deleted-blob GC unconditional.
  // Belief may be stale or (under envDropMark) false; the predicate
  // reads only agreed state.
  def evictable(f: int, n: int): bool =
    copies.get(f).contains(n) and isUp(n)
      and (deleted
        or (not(protected(f, n)) and invView.get(f).exclude(Set(n)).size() > 0))

  // INV-EVICT-SAFE history condition: an eviction that empties an
  // undeleted class's extant copy set WHILE BELIEF FOR THAT CLASS WAS
  // TRUTHFUL is self-harm — definitionally a predicate/belt regression,
  // since a truthful belt cannot claim a holder that does not exist.
  // False-belief last-copy loss is the environment's doing, budgeted,
  // and bounded by INV-DURABLE.
  def evictWouldHarm(f: int, n: int): bool =
    not(deleted)
      and copies.get(f) == Set(n)
      and invView.get(f).exclude(Set(n)).subseteq(copies.get(f).exclude(Set(n)))

  action envEvictN(f: int, n: int): bool = all {
    evictable(f, n),
    // if-then-else, not `or`: a boolean `or` in assignment position is
    // split into an action-level disjunction by the TLA translation,
    // leaving evictHarm' unassigned on one branch (Apalache rejects it)
    evictHarm' = if (evictWouldHarm(f, n)) true else evictHarm,
    copies' = copies.set(f, copies.get(f).exclude(Set(n))),
    status' = status, downFor' = downFor, memberView' = memberView, invView' = invView,
    deleted' = deleted, calm' = 0, repairCost' = repairCost,
    goneCnt' = goneCnt, corruptCnt' = corruptCnt,
    confirmedView' = confirmedView, targetView' = targetView, protectedBy' = protectedBy,
    dropCnt' = dropCnt,
  }

  action envEvict = all {
    frags.exists(f => NODES.exists(n => evictable(f, n))),
    {
      nondet f = frags.filter(f => NODES.exists(n => evictable(f, n))).oneOf()
      nondet n = NODES.filter(n => evictable(f, n)).oneOf()
      envEvictN(f, n)
    }
  }

  // RFC-STORAGE-003: the lossy-mark adversary — UNDER-claim only: a
  // lost mark_local makes a real copy vanish from belief (unattested
  // bytes, invisible to repair and to confirm evidence).
  //
  // The OVER-claim direction (a copy belief should no longer count) is
  // deliberately NOT a fabricating action. Inventory rows originate
  // solely from a holder's own self-attestation, so an over-claiming
  // row was true at attestation time and went stale through a loss —
  // which the model already represents as ordinary belief lag between
  // faults/evictions and the sync rung, under adversarial scheduling.
  // A FABRICATED positive (a row for bytes never held) is the lying/
  // buggy-attester case, outside the fault-model perimeter — and
  // provably must stay there: given one phantom entry, the checker
  // kills a class for free (the phantom launders ConfirmPlacement,
  // lapsing real obligations, and the eviction belt then trusts the
  // same phantom — one lie spent twice). That counterexample is the
  // formal justification for S5's disk-verified attestation rows.
  action envDropMarkN(f: int, n: int): bool = all {
    dropCnt < MAX_DROPS,
    copies.get(f).contains(n),
    invView.get(f).contains(n),
    invView' = invView.set(f, invView.get(f).exclude(Set(n))),
    status' = status, downFor' = downFor, memberView' = memberView, copies' = copies,
    deleted' = deleted, calm' = 0, repairCost' = repairCost,
    goneCnt' = goneCnt, corruptCnt' = corruptCnt, dropCnt' = dropCnt + 1,
    confirmedView' = confirmedView, targetView' = targetView, protectedBy' = protectedBy,
    evictHarm' = evictHarm,
  }

  action envDropMark = all {
    dropCnt < MAX_DROPS,
    {
      nondet f = frags.oneOf()
      nondet n = NODES.oneOf()
      envDropMarkN(f, n)
    }
  }

  action envDelete = all {
    not(deleted),
    deleted' = true,
    status' = status, downFor' = downFor, memberView' = memberView, copies' = copies,
    invView' = invView, calm' = 0, repairCost' = repairCost,
    goneCnt' = goneCnt, corruptCnt' = corruptCnt,
    confirmedView' = confirmedView, targetView' = targetView,
    protectedBy' = protectedBy, dropCnt' = dropCnt, evictHarm' = evictHarm,
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
    not(copies.get(f).contains(respTarget(f)))
      and isUp(respTarget(f))
      and copies.get(f).intersect(upNodes).size() > 0)

  val liveClassCount = frags.filter(f => copies.get(f).intersect(upNodes).size() > 0).size()

  // Dead = no live copy. Hopeless = additionally no copy that might
  // come back inside the decay window. Urgency overrides hope.
  val deadClasses = frags.filter(f => copies.get(f).intersect(upNodes).size() == 0)
  val reencodeReady = deadClasses.filter(f =>
    isUp(respTarget(f)) and (
      liveClassCount < W
        or copies.get(f).filter(n => status.get(n) == DOWN and downFor.get(n) < DELTA).size() == 0))

  // Prioritized single-mutation ladder (RFC-STORAGE-003 order, decided
  // in S0): view sync > declare > re-encode > pull > belief sync >
  // confirm. Declare above the movers: fix the goal before moving bytes
  // toward it. Re-encode outranks pull — liveness deficit first: a dead
  // class is a durability hole, a misplaced one is not. Belief sync
  // BELOW the movers: one sync lands after the moves and before the
  // confirm — the faithful image of the implementation, whose inventory
  // rows land via batched self-checks, not per-pull; it also makes
  // CALM_BOUND linear at ~1 tick per moved class instead of 2. (The
  // sync-above-movers alternative was measured too: same invariants,
  // bound 19 instead of 13 on the scaled config — biased-step
  // falsification found its worst trace at ~1 per 5M samples.)
  action engineTick = all {
    status' = status,
    deleted' = deleted,
    goneCnt' = goneCnt,
    corruptCnt' = corruptCnt,
    dropCnt' = dropCnt,
    evictHarm' = evictHarm,
    calm' = calm + 1,
    downFor' = nextDownFor,
    if (memberView != viewTarget and viewTarget.size() > 0) all {
      memberView' = viewTarget,
      invView' = invView, copies' = copies, repairCost' = repairCost,
      confirmedView' = confirmedView, targetView' = targetView,
      protectedBy' = protectedBy,
    } else if (not(deleted) and targetView != memberView) all {
      // declare: the goal went stale — DeclarePlacementTarget under
      // model names; duties downstream re-derive from the new target.
      // The new epoch JOINS the in-flight set; protection of earlier
      // destinations persists until confirm.
      targetView' = memberView,
      protectedBy' = 0.to(N_FRAGS - 1).mapBy(f =>
        protectedBy.get(f).union(Set(PLACE.get(memberView).get(f)))),
      memberView' = memberView, invView' = invView, copies' = copies,
      repairCost' = repairCost, confirmedView' = confirmedView,
    } else if (AUTO_REENCODE and not(deleted)
        and reencodeReady.size() > 0 and liveClassCount >= K) all {
      copies' = copies.set(least(reencodeReady),
        copies.get(least(reencodeReady)).union(Set(respTarget(least(reencodeReady))))),
      repairCost' = repairCost + K,
      memberView' = memberView, invView' = invView,
      confirmedView' = confirmedView, targetView' = targetView,
      protectedBy' = protectedBy,
    } else if (AUTO_PULL and not(deleted) and pullNeedy.size() > 0) all {
      copies' = copies.set(least(pullNeedy),
        copies.get(least(pullNeedy)).union(Set(respTarget(least(pullNeedy))))),
      repairCost' = repairCost + 1,
      memberView' = memberView, invView' = invView,
      confirmedView' = confirmedView, targetView' = targetView,
      protectedBy' = protectedBy,
    } else if (invView != copies) all {
      invView' = copies,
      memberView' = memberView, copies' = copies, repairCost' = repairCost,
      confirmedView' = confirmedView, targetView' = targetView,
      protectedBy' = protectedBy,
    } else if (not(deleted) and confirmedView != targetView
        and frags.forall(f => invView.get(f).contains(respTarget(f)))) all {
      // confirm: evidence complete — the guard reads BELIEF, exactly as
      // ConfirmPlacement validation reads attested inventory rows.
      // Obligations collapse onto the confirmed epoch: the in-flight
      // set empties, superseded destinations become surplus.
      confirmedView' = targetView,
      protectedBy' = 0.to(N_FRAGS - 1).mapBy(f => Set(respTarget(f))),
      memberView' = memberView, invView' = invView, copies' = copies,
      repairCost' = repairCost, targetView' = targetView,
    } else all {
      memberView' = memberView, invView' = invView,
      copies' = copies, repairCost' = repairCost,
      confirmedView' = confirmedView, targetView' = targetView,
      protectedBy' = protectedBy,
    }
  }

  action step = any {
    envSleep, envWake, envDepart, envCorrupt,
    envEvict, envDelete, envDropMark,
    engineTick,
  }

  // Falsification step for INV-CONVERGE (the CALM_BOUND counting
  // method): the adversary may act until the engine has strung a few
  // quiet ticks, then the trace is a pure calm window — random `step`
  // traces essentially never produce CALM_BOUND consecutive engine
  // ticks (~(1/8)^BOUND per trace), which is how v2's INV-RECOVER hole
  // stayed hidden. Use: quint run --step calmProbeStep --invariant
  // convergence --init {init,initOriginHeld,initMidFlight}. Any
  // violation is a miscounted CALM_BOUND, with the trace as the
  // counterexample.
  action calmProbeStep = any {
    all { calm <= 6, any { envSleep, envWake, envDepart, envCorrupt, envEvict, envDropMark } },
    engineTick,
  }

  // === Properties ===
  // INV-DURABLE — core safety under the fault budget.
  val safety = deleted or durable

  // INV-EVICT-SAFE — the system never self-harms: no eviction empties an
  // undeleted class's extant copy set on truthful belief. Checked
  // unconditionally, every budget enabled; a violation is definitionally
  // a protection-predicate/belt regression (see envEvict). False-belief
  // harm is the environment's, and INV-DURABLE bounds it.
  val evictSafe = not(evictHarm)

  // INV-CONVERGE — bounded convergence from ANY reachable pipeline state
  // (subsumes v2's INV-RECOVER): an uninterrupted engine window finishes
  // the job. Converged is the full fixpoint — bytes at the goal,
  // paperwork done, goal fresh (assignment-equality, so a clean re-goal
  // does not un-converge a blob), belief matching reality.
  //
  // The `available` precondition (S0 finding): convergence is promised
  // only for calm windows in which the chunk is reconstructible from
  // ONLINE nodes — you cannot converge what you cannot read. With most
  // holders asleep, pulls have no source and re-encode is (correctly)
  // gated on K live classes; the engine must idle without penalty until
  // wake or decay. v2's INV-RECOVER had this same hole unstated — it was
  // sim-unreachable (13 consecutive engine ticks after 3 sleeps is a
  // ~1e-12 event per random trace) and Apalache only checked safety.
  // `available` is monotone under engine ticks, so holding at check time
  // means it held through the calm window being judged.
  val converged =
    conformantToTarget
      and confirmedView == targetView
      and PLACE.get(targetView) == PLACE.get(memberView)
      and invView == copies
  val convergence = deleted or calm < CALM_BOUND or not(available) or converged

  // INV-SPREAD — even-spread bridge lemma (feeds the 1/3-vs-2/3
  // membership-lag slack argument). Expected: holds for variants 0
  // and 2; plain HRW (1) violates it.
  val spread = memberView.size() == 0 or memberView.forall(m =>
    frags.filter(f => resp(f) == m).size()
      <= (N_FRAGS + memberView.size() - 1) / memberView.size())

  val safetyAndConvergence = safety and evictSafe and convergence
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
    MAX_GONE = 1, MAX_CORRUPT = 1, MAX_DROPS = 1, CALM_BOUND = 13, DELTA = 3, W = 3
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
    MAX_GONE = 1, MAX_CORRUPT = 1, MAX_DROPS = 1, CALM_BOUND = 13, DELTA = 3, W = 3
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

// Target policy, scaled, BALANCED capped rendezvous — the ADOPTED
// variant: near-minimal movement, INV-SPREAD max AND min (loads are
// the tightest integer split: max - min <= 1, no zero-load members),
// closed-form burst bound / derived W.
module scaled_bal {
  import placement.*
  import scaled_params.*
  import storage_policy(
    NODES = NODES4, N_FRAGS = 6, K = 2,
    PLACE = buildTable(3, 6, NODES4, WEIGHT4),
    AUTO_PULL = true, AUTO_REENCODE = true,
    MAX_GONE = 1, MAX_CORRUPT = 1, MAX_DROPS = 1, CALM_BOUND = 13, DELTA = 3, W = 3
  ).*

  run healsAfterDepartBalTest =
    init.then(envDepart).then(18.reps(_ => engineTick))
      .expect(conformant and durable)

  run sleepWakeZeroCostBalTest =
    init.then(envSleep).then(2.reps(_ => engineTick)).then(envWake)
      .then(6.reps(_ => engineTick))
      .expect(conformant and durable and repairCost == 0)

  // COST-BOUND for the balanced variant: exact repair-cost accounting.
  run departCostExactBalTest = {
    val owned1 = 0.to(5).filter(f => placeVariant(3, f, 6, Set(1, 2, 3, 4), WEIGHT4) == 1)
    val movedSurvivors = 0.to(5).filter(f =>
      placeVariant(3, f, 6, Set(1, 2, 3, 4), WEIGHT4) != 1
        and placeVariant(3, f, 6, Set(2, 3, 4), WEIGHT4)
          != placeVariant(3, f, 6, Set(1, 2, 3, 4), WEIGHT4))
    init.then(envDepartN(1)).then(18.reps(_ => engineTick))
      .expect(conformant and repairCost == 2 * owned1.size() + movedSurvivors.size())
  }

  // ===== RFC-STORAGE-003 witnesses: the pipeline is inside the loop =====

  // WITNESS (headline): from birth — every class on one origin, never
  // confirmed — a calm window suffices to reach the full converged
  // fixpoint, whatever node uploaded.
  run originHeldConvergesTest =
    initOriginHeld.then(CALM_BOUND.reps(_ => engineTick)).expect(converged)

  // WITNESS (regions): the origin-3 birth trace passes through the
  // production stuck-point regions and exits them — mixed x unplaced
  // mid-distribution, then conformant x unplaced (distribution done,
  // paperwork not), then converged one confirm tick later. Node 3 owns
  // only class 3 under the full view, so five classes move (one pull
  // tick each), the belief sync lands on tick 6, confirm on tick 7.
  run originHeldRegionTraceTest =
    initOriginHeldAt(3)
      .then(3.reps(_ => engineTick))
      .expect(not(conformantToTarget) and confirmedView == Set())  // mixed x unplaced
      .then(3.reps(_ => engineTick))
      .expect(conformantToTarget and confirmedView == Set()
        and not(converged))                                        // conformant x unplaced
      .then(engineTick)
      .expect(converged and confirmedView == targetView)

  // WITNESS (drop-mark, under-claim): a lost mark_local diverges belief
  // from truth; the belief-sync rung — the model form of the disk-truth
  // sweep — restores it in one tick.
  run dropMarkHealsTest =
    init.then(envDropMarkN(0, 2))
      .expect(not(converged))
      .then(engineTick)
      .expect(converged)

  // WITNESS (over-claim = staleness): a corrupted sole copy leaves an
  // over-claiming belief entry on a live node; one re-encode restores
  // the bytes and the sweep rung restores belief.
  run staleOverclaimHealsTest =
    init.then(envCorruptN(0, 2))
      .expect(not(converged))
      .then(2.reps(_ => engineTick))
      .expect(converged)

  // WITNESS (the uploader-nukes-the-only-copy hole the predicate
  // closes): mid-distribution, belief truthfully shows second holders
  // for the pulled classes — the eviction belt would pass — yet every
  // origin copy stays protected while placement_height is NULL.
  run prematureEvictionSafeTest =
    initOriginHeldAt(1)
      .then(5.reps(_ => engineTick))
      .expect(confirmedView == Set()
        and frags.exists(f => invView.get(f).exclude(Set(1)).size() > 0)
        and not(frags.exists(f => evictable(f, 1))))

  // WITNESS (stale-belief eviction laundering is fault-anchored): one
  // budgeted corrupt kills its class completely — the corrupted
  // responsible's stale row launders eviction of the surviving surplus
  // — but only that class: the per-class kill accounting behind the
  // burst math holds, durability stands, and INV-EVICT-SAFE correctly
  // stays quiet (the emptying eviction acted on false belief). This is
  // the reachable remnant of the fabricated-belief break the perimeter
  // excludes; see envDropMarkN.
  run evictLaunderAccountingTest =
    initOriginHeldAt(3)
      .then(7.reps(_ => engineTick))
      .expect(converged)
      .then(envCorruptN(0, 2))
      .then(envEvictN(0, 3))
      .expect(copies.get(0) == Set() and durable and not(evictHarm))

  // Should: lapse the origin's obligations at confirm — after the
  // paperwork, its non-responsible copies are surplus and evictable
  // under the attested-successor belt.
  run originSurplusLapsesTest =
    initOriginHeldAt(3)
      .then(7.reps(_ => engineTick))
      .expect(converged and frags.forall(f =>
        respTarget(f) == 3 or evictable(f, 3)))

  // WITNESS (supersede): the in-flight destination dies mid-handoff; the
  // declare rung re-goals to the survivor view and the same machinery
  // finishes there — no timeout, no special case.
  run supersedeMidFlightTest =
    initOriginHeldAt(3)
      .then(4.reps(_ => engineTick))
      .then(envDepartN(4))
      .then(18.reps(_ => engineTick))
      .expect(converged and not(memberView.contains(4)))

  // WITNESS (CALM_BOUND tight, both directions): the counted worst
  // window at this scale. Setup: birth at origin 2; node 4 departs and
  // the first drain converges on {1,2,3} (11 ticks: 3 pre-decay pulls,
  // view sync, declare, 4 pulls, belief sync, confirm). The adversary
  // then prunes every surplus copy that would satisfy the {1,3} goal
  // (three surgical evictions — all legal: surplus under the predicate,
  // belt satisfied), and flaps the origin: sleep past the decay gate,
  // wake, sleep again — the calm window opens with the origin DOWN at
  // downFor 0 while the view already dropped it. The window then costs:
  // 1 re-admission view sync + 2 hope-gated idle ticks (the decay gate
  // re-runs) + 1 view sync + 1 declare + 2 re-encodes (origin-only
  // classes now hopeless) + 4 pulls + 1 belief sync + 1 confirm = 13
  // ticks. Twelve do not suffice; thirteen do. A ladder change that
  // alters the count breaks this witness loudly — recount, then update
  // CALM_BOUND and this comment together.
  run calmTightnessTest =
    initOriginHeldAt(2)
      .then(envDepartN(4))
      .then(11.reps(_ => engineTick))
      .then(envEvictN(2, 1))
      .then(envEvictN(3, 3))
      .then(envEvictN(4, 1))
      .then(envSleepN(2))
      .then(4.reps(_ => engineTick))
      .then(envWake)
      .then(envSleepN(2))
      .then((CALM_BOUND - 1).reps(_ => engineTick))
      .expect(not(converged))
      .then(engineTick)
      .expect(converged)

  // WITNESS (random regression): any short adversarial burst, then a
  // CALM_BOUND window — the blob is converged, excused (unreadable), or
  // deleted. This is the sampling half of the INV-CONVERGE regime run
  // on every test invocation.
  run randomBurstConvergenceTest =
    initOriginHeld
      .then(5.reps(_ => any {
        envSleep, envWake, envDepart, envCorrupt, envEvict, envDropMark, engineTick }))
      .then(CALM_BOUND.reps(_ => engineTick))
      .expect(deleted or not(available) or converged)

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
    MAX_GONE = 1, MAX_CORRUPT = 1, MAX_DROPS = 1, CALM_BOUND = 13, DELTA = 3, W = 3
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
    MAX_GONE = 1, MAX_CORRUPT = 1, MAX_DROPS = 1, CALM_BOUND = 13, DELTA = 3, W = 3
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

// EXHAUSTIVE VERIFICATION table (Apalache). Placement is a LITERAL
// balanced-capped-rendezvous table (VARIANT 3 over Set(1,2,3,4),
// WEIGHT 3/2/1/1), precomputed offline — no arithmetic for the SMT
// backend. Keep in sync with placement::buildTable(3, 6, Set(1,2,3,4),
// W): if the placement functions change, regenerate (see
// spec/README.md); table_guard asserts equality.
module verify_table {
  pure val BAL_TABLE: Set[int] -> (int -> int) = Map(
    Set(1)          -> Map(0->1, 1->1, 2->1, 3->1, 4->1, 5->1),
    Set(2)          -> Map(0->2, 1->2, 2->2, 3->2, 4->2, 5->2),
    Set(3)          -> Map(0->3, 1->3, 2->3, 3->3, 4->3, 5->3),
    Set(4)          -> Map(0->4, 1->4, 2->4, 3->4, 4->4, 5->4),
    Set(1,2)        -> Map(0->2, 1->1, 2->1, 3->2, 4->1, 5->2),
    Set(1,3)        -> Map(0->1, 1->3, 2->1, 3->3, 4->1, 5->3),
    Set(1,4)        -> Map(0->1, 1->4, 2->1, 3->4, 4->1, 5->4),
    Set(2,3)        -> Map(0->2, 1->3, 2->2, 3->3, 4->3, 5->2),
    Set(2,4)        -> Map(0->2, 1->4, 2->2, 3->4, 4->2, 5->4),
    Set(3,4)        -> Map(0->3, 1->4, 2->4, 3->3, 4->3, 5->4),
    Set(1,2,3)      -> Map(0->2, 1->1, 2->3, 3->1, 4->3, 5->2),
    Set(1,2,4)      -> Map(0->2, 1->4, 2->1, 3->1, 4->2, 5->4),
    Set(1,3,4)      -> Map(0->1, 1->4, 2->3, 3->1, 4->3, 5->4),
    Set(2,3,4)      -> Map(0->2, 1->4, 2->3, 3->2, 4->3, 5->4),
    Set(1,2,3,4)    -> Map(0->2, 1->4, 2->1, 3->3, 4->1, 5->2)
  )
}

module scaled_verify {
  import verify_table.*
  import storage_policy(
    NODES = Set(1, 2, 3, 4), N_FRAGS = 6, K = 2, PLACE = BAL_TABLE,
    AUTO_PULL = true, AUTO_REENCODE = true,
    MAX_GONE = 1, MAX_CORRUPT = 1, MAX_DROPS = 1, CALM_BOUND = 13, DELTA = 3, W = 3
  ).*
}

// Birth-window verification config (RFC-STORAGE-003): origin-held
// safety. MAX_GONE = 0 — whole-disk death of the single-copy origin
// pre-distribution is data loss by nature (the upload durability window
// every system has until first replication), not a policy defect.
// Everything else stays adversarial: sleep, eviction pressure, media
// corruption, and mark loss — the uploader-nuking-the-only-copy hole is
// exactly what this config checks the protection predicate against.
module scaled_verify_birth {
  import verify_table.*
  import storage_policy(
    NODES = Set(1, 2, 3, 4), N_FRAGS = 6, K = 2, PLACE = BAL_TABLE,
    AUTO_PULL = true, AUTO_REENCODE = true,
    MAX_GONE = 0, MAX_CORRUPT = 1, MAX_DROPS = 1, CALM_BOUND = 13, DELTA = 3, W = 3
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

  // Fault budget b(v) = |v| - quorum(profile, |v|), keyed off the ACTIVE
  // quorum profile the mesh runs at this set size (RFC-CONSENSUS-002 AUTO
  // seam: majority below V_BFT=7, BFT at and above). This replaces the
  // former floor(|v|/3) placeholder, which matched neither the code's BFT
  // budget nor the majority arm the mesh actually runs below the seam.
  // Basis: MEMBER-COUNT variant (b sized off the storage member view |v|,
  // the same view the reserve/spread use). The alternative VALIDATOR-SET
  // variant (b sized off the committed validator count, which the three-
  // timescale design lets diverge from |v|) is the open modelling item —
  // it needs a validator-count parameter introduced into these lemmas.
  pure def quorumOf(vn: int): int = if (vn >= 7) 2 * vn / 3 + 1 else vn / 2 + 1
  pure def bBudget(vn: int): int = vn - quorumOf(vn)
  run tableMatchesGeneratorTest =
    init.expect(BAL_TABLE
      == buildTable(3, 6, Set(1, 2, 3, 4), Map(1 -> 3, 2 -> 2, 3 -> 1, 4 -> 1)))

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
          0.to(5).filter(f => u.contains(BAL_TABLE.get(v).get(f))).size() >= 2)))

  // Same lemma at REAL parameters (K=10, 30 classes, 10 nodes,
  // balanced placement): every view, every 2/3-online subset.
  run bridgeLemmaFullTest = {
    val w10 = Map(1 -> 3, 2 -> 3, 3 -> 2, 4 -> 2, 5 -> 1,
                  6 -> 1, 7 -> 1, 8 -> 1, 9 -> 1, 10 -> 1)
    val table = buildTable(3, 30, 1.to(10), w10)
    init.expect(
      1.to(10).powerset().exclude(Set(Set())).forall(v =>
        v.powerset().filter(u => u.size() * 3 >= v.size() * 2).forall(u =>
          0.to(29).filter(f => u.contains(table.get(v).get(f))).size() >= 10)))
  }

  // SPREAD lemmas, both sides, by complete enumeration: balanced
  // placement pins every view's loads to the tightest integer split —
  // max <= ceil(N/|v|) AND min >= floor(N/|v|) (no zero-load members).
  run spreadTightScaledTest =
    init.expect(
      Set(1, 2, 3, 4).powerset().exclude(Set(Set())).forall(v =>
        v.forall(m => {
          val load = 0.to(5).filter(f => BAL_TABLE.get(v).get(f) == m).size()
          load <= (6 + v.size() - 1) / v.size() and load >= 6 / v.size()
        })))

  run spreadTightFullTest = {
    val w10 = Map(1 -> 3, 2 -> 3, 3 -> 2, 4 -> 2, 5 -> 1,
                  6 -> 1, 7 -> 1, 8 -> 1, 9 -> 1, 10 -> 1)
    val table = buildTable(3, 30, 1.to(10), w10)
    init.expect(
      1.to(10).powerset().exclude(Set(Set())).forall(v =>
        v.forall(m => {
          val load = 0.to(29).filter(f => table.get(v).get(f) == m).size()
          load <= (30 + v.size() - 1) / v.size() and load >= 30 / v.size()
        })))
  }

  // BURST lemma (derived watermark; RFC-STORAGE-001 "Watermark"): the
  // adversarial ceiling — no b(v)-subset's loss exceeds
  //   advMax = min(b,f)*c + max(0, b-f)*(c-1)
  // for c = ceil(N/|v|), f = N - |v|*(c-1), b = bBudget(|v|) =
  // |v| - quorum(profile, |v|) (the active-profile fault budget; see
  // bBudget above). The implementation's reserve is
  // min(ceil(b*N/|v|) + sigma, advMax) with b capped at B_max; B_max binds
  // only above enumerable table sizes (v > 15), where the argument is
  // closed-form over the spread-lemma-pinned load profile. The member-count
  // basis is certified here (all lemmas pass at the active-profile b,
  // including the sigma tail); the validator-set basis remains open.
  run burstLemmaScaledTest =
    init.expect(
      Set(1, 2, 3, 4).powerset().exclude(Set(Set())).forall(v => {
        val b = bBudget(v.size())
        val c = (6 + v.size() - 1) / v.size()
        val fcap = 6 - v.size() * (c - 1)
        val closedForm = b * (c - 1) + (if (b < fcap) b else fcap)
        val worst = v.map(m => 0.to(5).filter(f => BAL_TABLE.get(v).get(f) == m).size())
          .fold(0, (a, x) => if (x > a) x else a)
        // every b-subset's loss <= closed form, and one achieves it:
        v.powerset().filter(bs => bs.size() == b).forall(bs =>
          bs.fold(0, (a, m) => a + 0.to(5).filter(f => BAL_TABLE.get(v).get(f) == m).size())
            <= closedForm)
          and worst <= c
      }))

  run burstLemmaFullTest = {
    val w10 = Map(1 -> 3, 2 -> 3, 3 -> 2, 4 -> 2, 5 -> 1,
                  6 -> 1, 7 -> 1, 8 -> 1, 9 -> 1, 10 -> 1)
    val table = buildTable(3, 30, 1.to(10), w10)
    init.expect(
      1.to(10).powerset().exclude(Set(Set())).filter(v => v.size() >= 2).forall(v => {
        val b = bBudget(v.size())
        val c = (30 + v.size() - 1) / v.size()
        val fcap = 30 - v.size() * (c - 1)
        val closedForm = b * (c - 1) + (if (b < fcap) b else fcap)
        v.powerset().filter(bs => bs.size() == b).forall(bs =>
          bs.fold(0, (a, m) => a + 0.to(29).filter(f => table.get(v).get(f) == m).size())
            <= closedForm)
      }))
  }

  // SIGMA-TAIL lemma: quantifies the risk accepted by the expected-load
  // reserve (sigma = 1). For every view of size >= 6, the fraction of
  // b(v)-subsets whose loss exceeds reserve(v) is <= 2% (measured worst:
  // 1/84 at v = 9 — the burst that hits all three cap-loaded members).
  // This is the number behind the spec's load-diverse power-domain
  // assumption: physical bursts uncorrelated with load hit a bad subset
  // with at most this probability.
  run sigmaTailExposureFullTest = {
    val w10 = Map(1 -> 3, 2 -> 3, 3 -> 2, 4 -> 2, 5 -> 1,
                  6 -> 1, 7 -> 1, 8 -> 1, 9 -> 1, 10 -> 1)
    val table = buildTable(3, 30, 1.to(10), w10)
    init.expect(
      1.to(10).powerset().filter(v => v.size() >= 6).forall(v => {
        val b = bBudget(v.size())
        val c = (30 + v.size() - 1) / v.size()
        val fcap = 30 - v.size() * (c - 1)
        val expReserve = (b * 30 + v.size() - 1) / v.size() + 1
        val advMax = (if (b < fcap) b else fcap) * c
          + (if (b > fcap) (b - fcap) * (c - 1) else 0)
        val reserve = if (expReserve < advMax) expReserve else advMax
        val loads = v.mapBy(m => 0.to(29).filter(f => table.get(v).get(f) == m).size())
        val subs = v.powerset().filter(bs => bs.size() == b)
        val bad = subs.filter(bs => bs.fold(0, (a, m) => a + loads.get(m)) > reserve)
        bad.size() * 50 <= subs.size()
      }))
  }
}

// Full-scale parameters (K=10, 30 classes, 10 nodes) for `quint run`.
// Capped-HRW table over the powerset (built once on the Rust backend).
// CALM_BOUND: analytic count under the S0 ladder — re-admission sync
// (1) + idle re-decay (DELTA-1 = 4) + view sync (1) + declare (1) +
// worst moves (N_FRAGS - origin's minimum load = 30 - 3 = 27) + belief
// sync (1) + confirm (1) = 36, plus one staggered second flap cycle
// (+2 sync/declare) => 38; set 40 with slack for mid-window re-goal
// waste. Falsified via calmProbeStep at 20k+ samples per init at 38;
// the tight-both-directions witness pair lives at the scaled size
// (scaled_bal::calmTightnessTest), where the worst window is
// constructible by hand.
module full {
  import placement.*
  pure val NODES10 = 1.to(10)
  pure val WEIGHT10 = Map(1 -> 3, 2 -> 3, 3 -> 2, 4 -> 2, 5 -> 1,
                          6 -> 1, 7 -> 1, 8 -> 1, 9 -> 1, 10 -> 1)
  import storage_policy(
    NODES = NODES10, N_FRAGS = 30, K = 10,
    PLACE = buildTable(3, 30, NODES10, WEIGHT10),
    AUTO_PULL = true, AUTO_REENCODE = true,
    MAX_GONE = 2, MAX_CORRUPT = 3, MAX_DROPS = 2, CALM_BOUND = 40, DELTA = 5, W = 19
  ).*

}
