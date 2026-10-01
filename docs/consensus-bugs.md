# Consensus Bugs

## 1. ✅ Concurrent Block Creation Race
**Status**: FIXED (consensus mutex)
**Issue**: Multiple threads could create blocks simultaneously
**Location**: consensus_middleware
**Fix**: Added Mutex<()> guard before block creation

## 2. Missing Justify Fields
**Issue**: Blocks don't carry QC/TC for catch-up and partition recovery
**Impact**: Nodes can't recover from missed broadcasts
**Location**: BlockData struct, blocks table
**Fix**: Add `justify_qc` and `justify_tc` fields

## 3. ✅ Lock QC/TC Race Condition (Fork Vulnerability)
**Status**: FIXED (two-layer defense-in-depth)
**Issue**: Lock QC and TC can both form for same view with different arrival orders
**Impact**: Some nodes commit block, others don't → permanent state divergence
**Root cause**: Immediate commit on Lock QC arrival (1-chain commit rule)

**Attack scenario**:
```
t=60s:  Nodes A,B,C,D timeout
t=60.1s: Lock QC forms at leader
t=60.1s: TC forms from timeout votes
t=60.2s: Node A receives Lock QC → commits
t=60.2s: Nodes B,C,D receive TC → advance view without commit
→ DIVERGENCE: A committed, B,C,D didn't
```

**Fix**: Two-layer defense-in-depth mitigation

**Layer 1: Leader Abandonment** (proactive) - IMPLEMENTED
- Added `CertificateError::NetworkTimeout` variant
- `QuorumCertificate::create()` now checks timeout vote count via `TimeoutVoteCollector.get_vote_count()`
- If timeout_count >= quorum_threshold, refuses to create QC (both Propose and Lock phases)
- Uses `calculate_quorum_threshold()` for dynamic BFT/relaxed mode support
- Prevents honest-but-slow leader from completing QC after network timeout
- **Defends against**: Slow/crashed honest leader (95%+ of real failures)
- **Location**: `src/consensus/types.rs:478-502` (QuorumCertificate::create)

**Layer 2: Post-TC Bounded Wait** (reactive) - IMPLEMENTED
- **Follower side**: Added `skip_wait: bool` parameter to `apply_timeout_certificate()`
- Waits GST (500ms) before checking consensus state and applying TC
- If Lock QC applied during wait, view advances and TC becomes stale (rejected)
- Local timeout sites use `tokio::join!` to broadcast and apply in parallel for synchronization
- Catch-up uses `insert_tc_safe()` directly (no wait needed for historical replay)
- **Leader side**: Leader commits QC and broadcasts in parallel using `tokio::join!`
- Parallel broadcast minimizes window for state divergence by starting broadcast immediately
- Maximizes time for Lock QC to arrive at followers before their GST wait expires
- **Defends against**: Network reordering, asymmetric delays
- **Limitation**: Depends on GST timing assumption
- **Locations**:
  - Follower: `src/consensus/routes.rs:1428-1466` (apply_timeout_certificate)
  - Follower call sites: `src/consensus/jobs.rs:76-86`, `src/consensus/routes.rs:404-417`, `src/consensus/routes.rs:434`
  - Leader: `src/consensus/functions.rs:180-194` (QC1), `src/consensus/functions.rs:199-223` (QC2)

**Remaining attack vector**: Byzantine leader with network-level control can still cause divergence via selective delivery of Lock QC to subset of nodes. This requires:
- Malicious intent (not just crash/slow)
- Network control (selective message dropping)
- Precise timing coordination
- Detectable after-the-fact (victims have proof via Lock QC)
- **Risk assessment**: Low (requires sophisticated adversary)

**Long-term fix**: Migrate to HotStuff-2 (2-chain commit rule) eliminates all timing-based attacks but requires ~1000 LoC refactor and 2-view commit latency.

## 4. ✅ Early prepared_block_hash Setting
**Status**: FIXED (removed set_prepared parameter)
**Issue**: `prepared_block_hash` set when voting, should only set when Propose QC arrives
**Impact**: Incorrect HotStuff state machine progression
**Location**: insert_block() in db/consensus.rs, post_ballot route, Block::new_tip(), integrate_view()
**Fix**: Removed `set_prepared` parameter entirely from `insert_block()` and `insert_block_with_conn()`
- Block insertion now only stores block data, never modifies consensus state
- `prepared_block_hash` is ONLY set by `insert_qc_unsafe_tx()` when Propose QC arrives (correct HotStuff semantics)
- Updated all 3 call sites: leader block creation, validator voting, catch-up integration

## 5. ✅ No Double-Voting Protection
**Status**: FIXED (double-vote checks + vote tracking)
**Issue**: Node could vote twice in Propose phase for different blocks in same view
**Impact**: Byzantine behavior, could enable equivocation attacks
**Location**: Ballot::propose() for leaders, ballot.verify_proposal() + ballot.sign() for followers
**Fix**: Comprehensive double-vote protection for both roles:

**Schema changes:**
- Added `last_propose_vote_block_hash` field to `this_node` table
- Added `ProgressionErrorKind::DoubleVote` error variant
- Added `db::update_last_propose_vote()` function

**Leader protection (Ballot::propose()):**
- Checks `last_propose_vote_block_hash` before proposing
- Rejects different block in same view (double-vote attempt)
- Allows retry for same block (idempotent)
- Records vote after creating ballot
- Automatically creates vote signature internally

**Follower protection (ballot.verify_proposal() + ballot.sign()):**
- Checks `last_propose_vote_block_hash` before voting
- Rejects different block in same view (double-vote attempt)
- Allows retry for same block (idempotent)
- Records vote after signing

**View advancement cleanup:**
- `last_propose_vote_block_hash` is cleared when Lock QC advances to next view (db/consensus.rs:695)
- `last_propose_vote_block_hash` is cleared when TC advances to next view (db/consensus.rs:585)
- Allows leader to propose new blocks in the new view

**Note**: Lock phase doesn't need explicit tracking - `prepared_block_hash` already prevents voting on different blocks within a view

## 6. ✅ TC Doesn't Clear prepared_block_hash
**Status**: FIXED (insert_tc_safe)
**Issue**: When TC arrives, prepared_block_hash should be set to NULL (work abandoned)
**Impact**: Stale prepared state after timeout
**Location**: insert_tc_unsafe_tx() in db/consensus.rs:593-596
**Fix**: Added `prepared_block_hash = NULL` to TC processing UPDATE statement

## 7. ✅ TC Doesn't Process highest_qc
**Status**: FIXED (insert_tc_safe)
**Issue**: TC contains highest_qc field that should be extracted and inserted
**Impact**: Lose QC information during timeouts
**Location**: insert_tc_safe() in db/consensus.rs:603-672
**Fix**: Full validation pipeline:
1. Check if QC already exists → safe to proceed
2. If QC missing but block exists → verify QC cryptographically, then insert
3. If block missing → reject TC (requires catch-up first)
- Ensures consensus safety by validating justification before advancing view
- Opportunistically recovers missing QC when block available

## 8. ✅ Lock QC Without Propose QC
**Status**: FIXED (qc.verify)
**Issue**: qc.verify() doesn't check that local Propose QC exists before accepting Lock QC
**Impact**: Could accept Lock QC for block we never saw prepared
**Location**: qc.verify() in src/consensus/types.rs
**Fix**: Added Propose QC prerequisite check to qc.verify() - Lock QC validation now requires corresponding Propose QC to exist first, ensuring we never accept Lock QC for blocks we didn't see prepared

## 9. TOCTOU in Routes
**Status**: MITIGATED (consensus mutex)
**Issue**: /qc and /tc routes check state then modify separately
**Impact**: Race condition between check and modification
**Mitigation**: Consensus mutex prevents concurrent modifications
## 10. ✅ Retried Forward Duplicated in the Proposer's Pool
**Status**: FIXED 2026-09-27 (nonce-unique PendingPool + committed re-check on rejection)
**Issue**: A forwarder retries the moment a height decides without its transaction (`AckedDecided` → `RetryNow`), while the proposer still holds the original in its pending pool for a later block. The proposer's forward handler dedups against `committed_tx_nonces` only, so the retry became a second pool entry; preflight applies pool entries in order under savepoints, the first copy applied and the second was rejected `Permanent("ProcessingError")` (e.g. "pubkey already registered"). On a 3-validator mesh one rejection is final, so the API answered 500 while the transaction committed.
**Impact**: Any queue-submitted write could report failure after committing; seen as the fourth node's join in `regenesis-cutover`, likelier under the RFC-STORAGE-003 catch-up drain's transaction volume.
**Location**: `PendingPool::push` and `process_forward_results` in src/consensus/queue.rs
**Fix**: `PendingPool::push` joins a same-nonce entry's waiters (staged or in flight) instead of pushing a duplicate — `QueuedTransaction` carries `Notifiers`, a fan-out of every submitter waiting on it. On the forwarder, a `Rejected` verdict for a nonce already committed locally resolves through the settler as committed. Tests: `duplicate_nonce_joins_the_pooled_entry`, `notifiers_fan_out_and_ignore_dropped_receivers` (queue.rs), `rejection_of_a_locally_committed_nonce_resolves_as_committed` (consensus/tests/forward_dedup.rs).

## 11. ✅ Failed Decide Effect Swallowed by the Engine Macro
**Status**: FIXED 2026-10-01 (effect errors parked and returned by the host; transient decides retried)
**Issue**: Malachite's `process!` macro logs an `Err` from the effect handler ("Error when processing effect") and resumes the engine with `Resume::Continue`. A `HostError::Storage` from `Effect::Decide` — SQLITE_BUSY after the 5 s busy_timeout, a commit-time BUSY, or a handler's lock error lifted through `ApplyError` — therefore left the engine at Commit step believing the height decided, while the host never advanced `last_decided`, never queued `StartHeight(h+1)` and dropped every later sync value for the height as already decided. The shell's "storage errors are fatal" promise only covered errors returned from the input path.
**Impact**: A silent, permanent wedge of one validator. Production, 2026-09-27: the desktop node sat at height 64858 for four days (10,948 ProposedValue WAL entries for 64859) until a restart's WAL replay re-decided it; the same effect error fired 36× on the laptop and 58× on the desktop in the hour after the 2026.10.1 crossing under the disk-truth sweep's write load.
**Location**: `drive_once` / `handle_effect` in hopnet-consensus/src/host.rs; `StoreError` in hopnet-consensus/src/store.rs; `apply_block` in src/consensus/malachite/app.rs
**Fix**: The handler parks the first error in `HostCore::effect_error` and returns a marker to the macro; `drive_once` returns the parked error after `process!`, so `feed` propagates it and the shell aborts (supervision restarts, replay re-decides). Inside `Effect::Decide` a transient error (`Storage::error_is_transient`) re-runs the whole decide up to `DECIDE_RETRIES` (3) times with a 100/200/400 ms blocking backoff; `ApplyError` carries `transient`, mapped from a handler's `DatabaseError::Transient` and surfaced as `StoreError::ApplyTransient`, so contention inside a handler is retried rather than treated as a determinism failure. Tests: hopnet-consensus/tests/decide_errors.rs.
**Also exposed**: making effect errors visible surfaced a second silent loss on the same path — after a crash-replay the host restarted `wal_seq` at 0, so the first live WAL append at the replayed height violated the `(height, seq)` key and, swallowed, every post-restart vote at that height was published without its durable entry (the no-equivocation-across-crash guarantee held only for votes cast before the first crash). `start_height` now resumes `wal_seq` after the replayed entries and `wal_fetch` deletes a torn final row so the count is exact.

## 12. ✅ Fatal WAL-Append Busy Under the Proposer's Preflight Lock
**Status**: FIXED 2026-10-01 (every durability effect retries transient contention under one bounded budget; no effect runs past a failed one)
**Issue**: Entry 11 made a failed effect fatal, but only the decide got a retry. `Effect::WalAppend` persists the engine's own proposal and votes as a single autocommit INSERT on the consensus connection and relied entirely on the connection's 5 s `busy_timeout`. The proposer's `build_value` preflight dry-runs every candidate transaction inside an IMMEDIATE transaction on a second pool connection and holds the write lock for the whole dry-run; on a cold cache a 500-blob declare page took the hold past 15 s. The vote's WAL append waited 5 s, got `database is locked`, the host parked it, `drive_once` returned it, and the shell aborted the process. The same gap existed for `wal_reset`/`wal_fetch` at height start.
**Impact**: A deterministic crash loop. Production, 2026-10-01 17:37–18:03 UTC: thor was the proposer for height 82752; every boot ran the cold preflight, failed the vote append, aborted and dumped core — 24 restarts, the mesh pinned at 82752 with the desktop as the only live validator until the height moved and thor stopped being its proposer. systemd's core cap kept the disk safe; nothing else did. Also observed: after the first failed append the engine macro kept executing the input's later effects, so votes were signed and published with no WAL entry behind them before the abort — the equivocation window the WAL exists to close.
**Location**: `retry_transient`, `Effect::WalAppend`, `Effect::Decide`, `start_height`, `start_or_defer` and the `with:` arm of `drive_once` in hopnet-consensus/src/host.rs; the preflight in `build_value`, src/consensus/malachite/app.rs
**Fix**: One helper, `retry_transient`, wraps every durability effect — WAL append, WAL fetch/reset at height start, decide — and retries while `Storage::error_is_transient` holds: 7 attempts, each waiting out the 5 s busy_timeout, with 100·2^(n-1) ms between them capped at 3200 ms, ≈ 41 s of shell blocking worst case before the error is fatal (blocking votes that long is strictly better than aborting: consensus is stalled on the same lock either way). The WAL sequence number advances only after a successful insert, so a retried append reuses it. Once an effect has parked an error, `drive_once` refuses every later effect of the same input unexecuted, so nothing is signed or published past a failed append. The proposer side logs how long the preflight held the lock and stops admitting candidates past a two-second budget, restaging the rest for the next block (proposer-local block shaping; validation is untouched). Regression tests in hopnet-consensus/tests/wal_errors.rs: a lock held past one busy wait is survived; retries are bounded; no effect runs after a parked error; the height-start WAL reset retries.
