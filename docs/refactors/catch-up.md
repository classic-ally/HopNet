# Consensus Catch-Up and Node Bootstrap Refactor

## Overview

This refactor migrates from checkpoint-based node synchronization to consensus catch-up based synchronization. This eliminates dual state synchronization paths and ensures all nodes derive state from the same source: the consensus blockchain.

**Critical Bug Fix**: The current catch-up implementation has a state corruption vulnerability where failed view integrations are silently skipped, leading to missing transactions and divergent state. This must be fixed before implementing new node bootstrap.

---

## Phase 1: Catch-Up Safety & Retry Logic [Critical Foundation]

**Goal**: Fix state corruption bug in existing catch-up mechanism before building new features on it.

**Why First**: Everything else depends on catch-up working correctly. Current implementation can corrupt node state by skipping views that fail to fetch or validate.

### Requirements

#### View Completeness Validation
- [x] Create enumeration for view completeness states (committed, timed out, in-progress)
- [x] Implement validation function that checks if view data is complete
- [x] Validation logic must distinguish between historical and current views:
  - Historical views (before target) must have either a lock QC or timeout certificate
  - Current view (equals target) may be incomplete as it's in progress
  - Future views (after target) should return error
- [x] Historical view with blocks but no QC/TC should fail validation (validator is behind or corrupted)
- [x] Current view with any data (including just blocks) should pass validation (propose phase in progress)

#### Genesis Bypass Preparation
- [x] Add special handling for view 0 during integration
- [x] Genesis QCs should be inserted without signature validation (trust coordinator)
- [x] This prepares for Phase 4 but doesn't break existing functionality

#### Memory-Bounded Fetching
- [x] Implement batch fetching with configurable size limit
- [x] Each batch should process approximately 50 views at a time
- [x] Batch size chosen to limit memory footprint to 5-50MB typical, 100MB worst case
- [x] Views within batch can be fetched in parallel, batches processed sequentially

#### Retry Logic with Validator Rotation
- [x] When view fetch fails, retry with different validator from pool
- [x] When view validation fails, retry with different validator from pool
- [x] Each view should retry up to 3 times with different validators (actually retries with ALL validators)
- [x] After exhausting retry attempts for historical view, catch-up must abort (cannot skip)
- [x] Current view fetch failures should stop catch-up gracefully (network may be progressing)

#### Error Handling
- [x] Change error handling from "warn and continue" to "retry or abort"
- [x] Fatal database errors must abort catch-up immediately
- [x] Network/validation errors must trigger retry with different validator
- [x] After all validators exhausted, return error indicating which view failed

### Testing Criteria
- [ ] Stop a node, let network progress 100+ views, restart node
- [ ] Verify node catches up successfully without state divergence
- [ ] Verify sequences table matches network after catch-up
- [ ] Verify no transactions are skipped during catch-up
- [ ] Test with validator going offline mid-catch-up (rotation to next validator)
- [ ] Test with validator returning corrupted data (retry succeeds with different validator)

---

## Phase 2: Iterative Catch-Up with Convergence

**Goal**: Handle moving target problem where network progresses during long catch-ups.

**Why Second**: Builds on Phase 1's robust catch-up, critical for new node bootstrap from genesis.

### Requirements

#### Convergence Loop
- [x] Implement wrapper around catch-up that repeats until converged
- [x] After each catch-up iteration, re-check network height
- [x] If still behind by more than 2 blocks, perform another catch-up iteration
- [x] Continue until within convergence tolerance (2 blocks)
- [x] Limit to maximum 10 iterations to prevent infinite loops

#### Network Height Polling
- [x] Implement function to query multiple validators for their current view
- [x] Support both bootstrap validators (for new nodes) and local DB validators (for existing nodes)
- [x] Return maximum view reported across sampled validators
- [x] Handle validator query failures gracefully (continue with other validators)

#### Progress Reporting
- [x] Log progress every 100 views during catch-up (batch-level logging every 50 views)
- [x] Include percentage complete, views remaining, current iteration number
- [x] Log when starting new catch-up iteration with updated target

#### Integration with Existing Systems
- [x] Update timeout detection job to use convergence wrapper
- [x] Ensure reactive catch-up (from future ballots) continues to work
- [x] Maintain backward compatibility with existing catch-up callers

### Testing Criteria
- [ ] Create 1000-view gap, verify convergence even as network progresses
- [ ] Verify progress logging appears regularly during long catch-ups
- [ ] Test scenario where network advances 50+ views during catch-up
- [ ] Verify convergence loop terminates within expected iterations
- [ ] Test with all validators returning same height (single iteration)
- [ ] Test with validators returning different heights (uses maximum)

---

## Phase 3: Signal-Based Validator Activation

**Goal**: Nodes explicitly signal readiness after catch-up instead of immediate activation.

**Why Third**: Independent of bootstrap flow, can be tested with existing nodes falling behind.

**Prerequisite**: Dynamic fault tolerance thresholds must be implemented (relaxed mode for ≤6 validators, BFT mode for 7+).

### Requirements

#### Dynamic Fault Tolerance Implementation
- [x] Extract `calculate_quorum_threshold()` helper function to consensus types
- [x] Function returns simple majority for ≤6 validators, 2/3+1 for 7+ validators
- [x] Update `QuorumCertificate::verify()` to use dynamic threshold instead of hardcoded 2/3+1
- [x] Update `TimeoutCertificate::verify()` to use dynamic threshold instead of hardcoded 2/3+1
- [x] Add constant `BFT_THRESHOLD = 6` for mode transition point
- [x] Update `TimeoutCertificate::find_majority_timeout_data()` to use dynamic threshold
- [x] Update `ballot_broadcast()` and `qc_broadcast()` for consistent early termination
- [x] Add comprehensive test suite (16 tests) covering all validator counts and transitions
- [x] Add relaxed mode test (5 validators) that proves dynamic switching (would fail if BFT incorrectly applied)
- [x] Add relaxed mode test (3 validators) documenting intentional 2-of-3 quorum behavior

**Rationale**: Current QC/TC verification uses hardcoded BFT threshold even for small networks. Dynamic thresholds enable 1-node networks to grow naturally and match frontend's intended design (ConsensusHealthBar.svelte).

#### Activation Transaction Type
- [ ] Create new transaction payload structure for activation requests
- [ ] Payload must include node ID, current height (proof of catch-up), and requested effective height
- [ ] Requested effective height ensures deterministic activation (all nodes process same height)
- [ ] Transaction must be signed by requesting node

#### Activation Handler
- [ ] Implement handler that processes activation requests
- [ ] Verify authorization: only the node itself can request its own activation (signature check)
- [ ] Verify node is caught up: reported height must be within 2 views of current (tolerance for validator-elect)
- [ ] Verify requested activation height is in reasonable future (current+3 to current+10 views)
- [ ] Verify requested activation height is not already passed (must be > current view)
- [ ] Non-idempotent: refuse activation if node already active at requested height (prevents spam)
- [ ] Update validators table with exact requested effective height (deterministic)
- [ ] **NO safety margin check needed** - mathematical properties of dynamic thresholds ensure adding validators never regresses fault tolerance

**Safety Analysis**: With dynamic thresholds, adding validators:
- In relaxed mode (1-6): monotonically improves or maintains crash fault tolerance
- At boundary (6→7): lateral move in crash tolerance + gain Byzantine fault tolerance
- In BFT mode (7+): buffer ≥ 2 guaranteed by 2/3 threshold math, adding validators always safe

#### Automatic Reactivation After Catch-Up
- [ ] After successful catch-up in timeout detection job, check if node is inactive
- [ ] If inactive, automatically submit activation request with effective height = current + 3
- [ ] If activation request fails, retry on next job cycle
- [ ] Log activation status (no safety margin to calculate)

#### Initial Registration with Inactive Status
- [ ] Modify node insertion logic to mark new validators as inactive initially
- [ ] Exception: first node during network creation remains active (no catch-up needed)
- [ ] Node will activate itself after bootstrap completes (Phase 4)

### Testing Criteria
- [ ] Test dynamic threshold calculation at boundary (6 validators → simple majority, 7 validators → BFT)
- [ ] Verify QC/TC verification uses correct threshold for small networks (1-6 validators)
- [ ] Manually mark existing node as inactive in database
- [ ] Verify node reactivates itself after next catch-up cycle
- [ ] Stop node for extended period, restart, verify automatic reactivation
- [ ] Test with multiple nodes requesting activation concurrently (deterministic height processing)
- [ ] Verify 1-node network can grow to 2, 3, ... 10 nodes without artificial gates
- [ ] Verify activation window (3 views) allows validator-elect to observe system state
- [ ] Test idempotency: node attempting re-activation receives error, realizes already active

---

## Phase 4: New Node Bootstrap Flow [Complete Migration]

**Goal**: Replace checkpoint synchronization with catch-up based bootstrap.

**Why Fourth**: Requires all previous phases working. Cannot partially implement - must cut over completely.

### Requirements

#### Node Join Information Structure
- [ ] Define structure containing all information needed for new node to bootstrap
- [ ] Must include assigned node ID, user ID, and user keys
- [ ] Must include current network height at time of registration
- [ ] Must include list of all current active validators (for fetching catch-up data)
- [ ] Replaces old checkpoint sync structure entirely

#### Sequences Initialization
- [ ] Extract sequence initialization into reusable function
- [ ] Initialize all sequences to 0 (nodes, users)
- [ ] Use same function for both network creation and node joining
- [ ] Ensures new nodes start with identical sequence state before transaction replay

#### Database Initialization for New Nodes
- [ ] Initialize this_node table with node ID, private keys, view 0
- [ ] Initialize sequences table (both nodes and users to 0)
- [ ] Do NOT initialize any consensus state (blocks, QCs, validators) - comes from catch-up
- [ ] Set up connection pool and app state

#### Bootstrap Catch-Up Process
- [ ] Perform catch-up with convergence from view 0 to current height
- [ ] Pass bootstrap validators list to catch-up for data fetching
- [ ] Genesis block (view 0) inserted without validation (trust coordinator)
- [ ] All subsequent views validated normally
- [ ] As catch-up progresses, newly-inserted validators available for querying

#### Activation Request After Bootstrap
- [ ] After catch-up completes, verify node is at current height
- [ ] Submit activation request transaction
- [ ] Wait for activation to be processed through consensus
- [ ] Return success to coordinator

#### Coordinator Changes
- [ ] Keep existing consensus submission for node registration
- [ ] Keep existing polling for commit confirmation
- [ ] Replace sync dump generation with join info structure creation
- [ ] Include all active validators in join info (exhaustive list for redundancy)
- [ ] Send join info to new node via PUT request

#### Cleanup
- [ ] Remove old sync dump generation function
- [ ] Remove old sync setup object structure definition
- [ ] Remove old join setup database insertion function
- [ ] Update API route to accept new join info structure

### Testing Criteria
- [ ] Add new node to single-node network, verify catches up from genesis
- [ ] Add new node to active multi-node network, verify catches up completely
- [ ] Verify sequences table matches network after bootstrap
- [ ] Verify all nodes, users, files match network after bootstrap
- [ ] Verify fragment inventory is empty initially, populates after self-check
- [ ] Test adding node while network is actively processing transactions
- [ ] Test adding multiple nodes concurrently
- [ ] Long bootstrap test: Create 5000-view chain, add new node, verify success
- [ ] Test bootstrap failure scenarios (all validators offline, network unreachable)
- [ ] Verify node activation occurs automatically after bootstrap completes

---

## Files Modified

### Phase 1
- `src/consensus/routes.rs` - View integration and catch-up logic
- `src/consensus/types.rs` - Error types and completeness enum

### Phase 2
- `src/consensus/routes.rs` - Convergence wrapper
- `src/consensus/functions.rs` - Network polling
- `src/consensus/jobs.rs` - Integration with timeout detection

### Phase 3
- `src/consensus/types.rs` - Dynamic threshold function, activation request structure, QC/TC verification updates
- `src/handlers.rs` - Activation handler
- `src/consensus/jobs.rs` - Automatic reactivation
- `src/db/nodes.rs` - Mark new nodes inactive
- `src/db/consensus.rs` - Database helper functions (is_node_active, activate_validator)

### Phase 4
- `src/types.rs` - Join info structure
- `src/db/setup.rs` - Sequence initialization function
- `src/setup.rs` - Complete rewrite of join setup
- `src/nodes/routes.rs` - Coordinator sends join info
- `src/db/nodes.rs` - Cleanup old sync dump code

---

## Risk Assessment

**Phase 1: HIGH RISK** - Fixes critical bug, changes core consensus logic
- Extensive testing required before proceeding
- Review by multiple developers recommended
- Consider feature flag for gradual rollout

**Phase 2: MEDIUM RISK** - New logic but isolated wrapper
- Can fall back to single-iteration if issues arise
- Easy to test in isolation

**Phase 3: MEDIUM RISK** - Changes core consensus verification logic + new feature
- Dynamic threshold implementation touches QC/TC verification (critical consensus path)
- Must ensure threshold calculation correct at boundary (6→7 validators)
- Activation signaling is new feature (low risk, can be manually managed if issues)
- Worst case: QC/TC verification incorrect for small networks (breaks consensus)
- Extensive testing required for 1-7 validator networks
- Can be rolled back independently (dynamic thresholds revert to hardcoded 2/3)

**Phase 4: MEDIUM RISK** - Large change but built on solid foundation
- No fallback once deployed (checkpoint sync removed)
- Extensive integration testing required
- Consider testing in staging environment with real network conditions

---

## Success Criteria

After completion:
- [ ] No dual synchronization paths (single source of truth: blockchain)
- [ ] New nodes can bootstrap from genesis reliably
- [ ] Long catch-ups (1000+ views) complete successfully
- [ ] State divergence impossible (all nodes process same transactions)
- [ ] Nodes automatically reactivate after downtime
- [ ] Dynamic fault tolerance: 1-node networks can grow naturally through relaxed→BFT transition
- [ ] No manual intervention required for node addition
- [ ] Checkpoint sync code completely removed

## Rollback Plan

If critical issues discovered after deployment:
- Phase 1: Revert catch-up changes, restore "warn and continue" behavior (temporary - bug still exists)
- Phase 2: Remove convergence wrapper, use single-iteration catch-up
- Phase 3:
  - Revert dynamic threshold changes, restore hardcoded 2/3+1 for QC/TC verification
  - Disable automatic activation requests, manual activation via DB
  - Note: reverting thresholds breaks small networks (1-6 validators cannot reach quorum)
- Phase 4: Cannot easily rollback - would require restoring checkpoint sync code from git history
