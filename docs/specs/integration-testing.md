# Integration Testing Implementation

## Test Categories

This framework supports two types of tests:

**Consistency Tests**: One-shot operations with synchronization checkpoints to validate correctness in depth. These tests perform a single operation (e.g., upload a file), wait for consensus to process it, then validate the result across all nodes. Test names should end with `-consistency`.

**Load Tests**: High-volume continuous operations to test system behavior under sustained load. These tests fire many operations in parallel without synchronization points, let the system run for a duration, then wait for the system to settle before validating eventual consistency. Test names should end with `-load`.

## Phase 1: Test Infrastructure

### Step 1: Create test module structure
- [x] `orchestrator/tests/mod.rs`: Define `TestScenario` trait, `TestResult`, helper functions
- [x] **Validation**: Module compiles

### Step 2: Add `test` subcommand to main.rs
- [x] Parse `orchestrator test --mesh-id N --test NAME`
- [x] Route to test execution logic in `tests::handle_test_command()`
- [x] Parallel JWT token fetching for performance
- [x] **Validation**: CLI parsing works, `--list` flag works

### Step 3: Implement test helpers in `tests/files.rs`
- [x] `upload_file()`, `download_file()`
- [x] `download_file_from_all_nodes()` for multi-node verification
- [x] `verify_all_identical()` for comparing downloads
- [x] `list_files()`, `delete_file()` helpers
- [x] `list_files_from_all_nodes()` for multi-node listing
- [x] `verify_listings_identical()` for strict JSON comparison (enabled by inode ID fix)
- [x] **Validation**: Module compiles, helpers ready for use

### Step 4: Implement first test: `tests/file_upload.rs`
- [x] Implement `FileUploadConsistency` test
- [x] Register in test registry
- [x] **Validation**: `orchestrator test --mesh-id 0 --test file-upload-consistency` works on live mesh

### Step 5: Test output formatting
- [x] Pretty-print checks with ✅/❌
- [x] Duration tracking
- [x] Summary report
- [x] **Validation**: Output formatting implemented in `handle_test_command()`

### Step 5a: Implement fragment distribution test: `tests/fragment_distribution.rs`
- [x] Implement `FragmentDistribution` test
- [x] Upload file and wait for consensus propagation
- [x] Wait for fragment distribution to complete (placement_height set)
- [x] Trigger fragment inventory sync across all nodes
- [x] Verify fragment redundancy properties
- [x] Download file from all nodes to verify distributed retrieval works
- [x] Register in test registry
- [x] **Validation**: `orchestrator test --mesh-id 0 --test fragment-distribution` validates distributed fragment placement

### Step 6: Add fragment distribution verification helpers
- [x] Implement `get_fragment_distribution()` to query which nodes store which fragments
- [x] Implement `wait_for_fragment_distribution()` to poll until placement completes
- [x] Implement `verify_fragment_redundancy()` to check:
  - Total fragments = N original + 2N recovery (2:1 redundancy ratio)
  - Fragment count matches (original + recovery)
  - All fragments stored on at least one node
  - Fragment distribution achieves maximum possible failure tolerance based on network size:
    - Small networks (≤30 nodes): Can tolerate `n - ceil(10*n/30)` failures (ideal even distribution)
    - Large networks (>30 nodes): Can tolerate 20 failures (theoretical maximum with 30 fragments)
  - `placement_height` is set (distribution completed via consensus)
- [x] Implement `trigger_fragment_inventory_sync_all()` to manually trigger inventory synchronization
- [x] Implement `calculate_failure_tolerance()` helper to compute worst-case failure tolerance
- [x] Add fragment distribution checks to `fragment-distribution` test
- [x] **Validation**: Can verify Reed-Solomon redundancy and fragment placement across distributed nodes

## Phase 2: Network Simulation

### Step 6: Add network simulation API to HopNet
- [ ] `POST /debug/network-simulation` endpoint
- [ ] Execute `tc` inside container
- [ ] **Validation**: curl works

### Step 7: Add `CAP_NET_ADMIN` to containers
- [ ] Update container creation in `main.rs`
- [ ] **Validation**: tc works inside containers

### Step 8: Add `network` subcommand
- [ ] Create `src/network.rs` with network command logic
- [ ] Parse `orchestrator network --mesh-id 0 --nodes all --latency 100ms`
- [ ] Call HopNet API
- [ ] **Validation**: Network conditions apply

### Step 9: Create network profiles
- [ ] Define profiles: satellite, mobile-4g, intercontinental, dial-up
- [ ] **Validation**: `orchestrator network --mesh-id 0 --nodes all --profile satellite` works

### Step 10: Implement network condition display
- [ ] `orchestrator network --mesh-id 0 --show` displays current conditions
- [ ] **Validation**: Shows conditions for all nodes

## Phase 3: Expand Tests

### Step 11: Add more basic tests
- [ ] `file-delete-consistency`: Upload, delete, verify cleanup
- [ ] `file-list-consistency`: Multiple files, verify listing matches
- [ ] `multi-user-isolation`: Create users, verify access control
- [ ] **Validation**: All tests pass on clean 3-node mesh

### Step 12: Add catch-up/bootstrap test
- [ ] `catch-up-bootstrap`: Create mesh, add data, add new node, verify sync
- [ ] **Validation**: New node successfully catches up and can serve data

### Step 13: Add network condition tests
- [ ] `consensus-under-latency`: Apply latency, verify consensus still progresses
- [ ] `partition-recovery`: Simulate partition with packet loss, heal, verify recovery
- [ ] **Validation**: Tests demonstrate HopNet resilience under network stress

## Phase 4: Advanced Tests & CI Integration

### Step 14: Long-running stability tests
- [ ] `concurrent-operations`: Multiple uploads/downloads/deletes simultaneously
- [ ] `rebalancing-consistency`: Trigger rebalancing, verify data integrity
- [ ] **Validation**: Tests run for several minutes without failures

### Step 15: Create CI test script
- [ ] Bash script that creates mesh, runs test suite, reports results
- [ ] **Validation**: Can run in CI pipeline, fails properly on test failures

### Step 16: Documentation & profiles
- [ ] Document all test scenarios
- [ ] Document network profiles and use cases
- [ ] Add examples to README
- [ ] **Validation**: Someone else can use the testing framework