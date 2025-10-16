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