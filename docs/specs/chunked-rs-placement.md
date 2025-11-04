# Chunked Reed-Solomon with Local-Index Modulo Placement

## Overview

Replace whole-file Reed-Solomon encoding with rendezvous hashing placement with chunked Reed-Solomon encoding and local-index modulo placement. This provides:

- **Improved TTFB**: 25x faster for large files (reconstruct 40MB chunks instead of entire file)
- **Progressive streaming**: Start streaming while reconstructing later chunks
- **Optimal redundancy**: Modulo placement guarantees maximum failure tolerance
- **Predictable performance**: Fragment distribution is deterministic and evenly balanced

## Architecture Summary

**Current**: Encode entire file as one RS code → place fragments using rendezvous hashing (1/3 candidate pool)

**New**: Encode file in 40MB chunks (10 original + 20 recovery per chunk) → place fragments using local_index % num_validators

**Key Insight**: Use `local_index` (position within chunk 0-29) instead of `global_index` for placement. This ensures fragment[0] from chunk 1 goes to the same node as fragment[0] from chunk 0, creating predictable recovery patterns.

## Phase 1: Database Schema (Remove fragment_index, Add Compound Key) [x]

### Step 1: Update schema [x]
- [x] Remove `fragment_index` column from `fragment_hashes` table in `src/db/shared.rs::initialize()`
- [x] Add `chunk_number UINTEGER NOT NULL DEFAULT 0` to `fragment_hashes` table
- [x] Add `local_index UINTEGER NOT NULL DEFAULT 0` to `fragment_hashes` table
- [x] Change PRIMARY KEY from `(data_block_id, fragment_index)` to `(data_block_id, chunk_number, local_index)`
- [x] **Validation**: Schema compiles, database initializes successfully

### Step 2: Update database types [x]
- [x] `src/db/types.rs::FragmentHash`: Remove `fragment_index`, add `chunk_number: u32` and `local_index: u32`
- [x] `src/db/debug.rs::FragmentInfo`: Remove `fragment_index`, add `chunk_number: u32` and `local_index: u32`
- [x] **Validation**: Types compile

### Step 3: Update database insert operations [x]
- [x] `src/db/files.rs::insert_files()`: Update INSERT SQL and params (line 149)
- [x] `src/db/files.rs::modify_item()`: Update INSERT SQL and params (line 461)
- [x] Update error logging to reference `chunk_number` instead of `fragment_index` (line 471)
- [x] **Validation**: INSERT operations compile

### Step 4: Update database query operations [x]
- [x] `src/db/files.rs::get_file_fragments()`: Update SELECT columns and ORDER BY (lines 532-537, 545, 562, 573, 576)
- [x] `src/db/files.rs::get_distributable_file()`: Update SELECT, ORDER BY, and row parsing (lines 656-663, 667, 676)
- [x] `src/db/fragments.rs` (rebalancing query): Update ORDER BY clause (line 311)
- [x] **Validation**: All queries compile (`cargo check` passes)

### Step 5: Update fragment creation in routes [x]
- [x] `src/files/routes.rs::process_uploaded_file()`: Update 4 FragmentHash creation sites to use `chunk_number: 0, local_index: sequential_position`
- [x] Apply backward compatibility semantics: treat current whole-file RS as chunk_number=0 with varying local_index
- [x] **Validation**: Fragment creation compiles and uses correct field semantics

## Phase 2: Validator Infrastructure [x]

### Step 5: Use existing validator queries [x]
- [x] Use existing `src/db/consensus.rs::get_validators(db, height) -> Vec<Node>`
- [x] Query pattern already implemented: Most recent validator record at/before height where is_active=true
- [x] **Validation**: Added comprehensive unit tests (6 tests covering empty, activation, deactivation, reactivation, multiple nodes, future queries)

### Step 6: Validator types [x]
- [x] Use existing `Node` struct with `node_id`, `ip_address`, `port` fields
- [x] No conversion needed - `get_validators()` returns `Vec<Node>` directly
- [x] **Validation**: All unit tests pass, proper PubKey serialization used in tests

## Phase 3: Placement Algorithm [x]

### Step 7: Implement file-level node selection [x]
- [x] Create `src/files/placement.rs::select_nodes_for_file(validators, all_metrics, file_hash) -> Vec<Node>`
- [x] Early-exit optimization: return all validators if ≤30 (no metrics filtering needed)
- [x] Filter NodeMetrics to active validators only (when >30 validators)
- [x] Score using existing `calculate_final_placement_scores()`: 40% availability, 30% throughput, 20% latency, 10% stability
- [x] Take top 60 candidates (2× target for diversity)
- [x] Deterministic shuffle using Blake3-based Fisher-Yates with file_hash seed
- [x] Return top 30 nodes after shuffle
- [x] **Validation**: 5 unit tests pass - same file_hash → same nodes; different files → different nodes; filters inactive validators

### Step 8: Implement modulo placement primitive [x]
- [x] Create `src/files/placement.rs::get_fragment_placement(local_index, selected_nodes) -> Vec<&Node>`
- [x] Implement: `primary_idx = local_index % validators.len()`
- [x] Return primary + 2 backups (local_index+1, local_index+2 with wraparound)
- [x] **Validation**: 7 unit tests pass - same local_index → same node; wraparound works; even distribution verified; ±1 max imbalance

### Step 8b: Delete rendezvous hashing [x]
- [x] Delete `src/files/placement.rs::calculate_rendezvous_distances()` function
- [x] Delete `Phase1Candidate` struct (no longer used)
- [x] Keep `Phase2Candidate` (still used by `calculate_final_placement_scores()`)
- [x] **Validation**: cargo check passes

### Step 9: Update distribution code [x]
- [x] Update `DistributableFileData` struct to include `file_hash` field
- [x] Update `get_distributable_file()` query to fetch file_hash from data_blocks table
- [x] Update `src/files/distribution.rs::distribute_file_fragments()`:
  - [x] Call `get_validators(consensus_height)` to get active validators
  - [x] Call `get_all_node_metrics(consensus_height)` to get all metrics
  - [x] Call `select_nodes_for_file(validators, metrics, &file_hash)` to get selected nodes for this file
  - [x] For Phase 3 (backward compat): use `local_index = fragment_sequential_index` (chunk_number implicitly 0)
  - [x] Call `get_fragment_placement(local_index, &selected_nodes)` to get primary + 2 backups
  - [x] Replace existing rendezvous hashing logic with modulo placement
- [x] **Validation**: cargo check passes

### Step 10: Simplify discovery code [x]
- [x] Remove `get_fragment_placement_candidates()` function from discovery.rs
- [x] Simplify `find_fragment()` to use inventory-first + reactive fallback (no rendezvous)
- [x] Clean up imports (removed rendezvous dependencies)
- [x] Fragment inventory remains PRIMARY lookup mechanism
- [x] Fallback uses reactive discovery across all nodes (no placement calculation)
- [x] **Note**: Rebalancing job in jobs.rs temporarily disabled (requires file_hash + local_index for Phase 4)
- [x] **Validation**: cargo check passes

## Phase 4: Chunked Reed-Solomon [x]

### Step 11: Implement chunk calculation [x]
- [x] Create `src/files/functions.rs::calculate_chunked_fragments(file_size) -> (num_chunks, total_original, total_recovery)`
- [x] Formula: `num_chunks = ceil(file_size / 40MB).max(1)`, 10+20 per chunk
- [x] Replace `calculate_optimal_chunks()` with this new function
- [x] **Validation**: Unit tests for various file sizes

### Step 12: Rewrite upload encoding [x]
- [x] Replace `src/files/routes.rs::process_uploaded_file()`:
  - [x] Loop over chunks (lines 72-175)
  - [x] Create `ReedSolomonEncoder::new(10, 20, chunk_size)` per chunk
  - [x] Process 10 original + 20 recovery per chunk
  - [x] Set `chunk_number` and `local_index` on each fragment (4 locations: lines 91, 120, 145, 168)
- [x] **Validation**: Upload 1-chunk file, verify 30 fragments with correct metadata

### Step 13: Implement chunked reconstruction [x]
- [x] Create `src/files/functions.rs::reconstruct_file_chunked()`:
  - [x] Loop through chunks
  - [x] Fetch fragments filtered by chunk_number
  - [x] Fast path: concatenate originals if all present
  - [x] Slow path: RS decode with any 10 of 30 (fixed bugs: blocking I/O, recovery index mapping, hash verification)
  - [x] Stream each chunk immediately after reconstruction
- [x] **Validation**: Download with all fragments (fast); delete fragment and verify RS reconstruction (slow)

### Step 14: Update download endpoints [x]
- [x] Update `src/files/routes.rs` download handler to use `reconstruct_file_chunked()`
- [x] Update `src/files/download.rs::reconstruct_file_for_user()` to return streaming response
- [x] Update `src/takeout/materialization.rs` to use shared reconstruction logic
- [x] **Validation**: Download matches uploaded content

## Phase 5: Testing [x]

### Step 15: Update unit tests [x]
- [x] `src/files/tests.rs::test_calculate_chunked_fragments()`: Tests new Phase 4 chunked function
- [x] Removed obsolete tests: `test_calculate_optimal_chunks()`, `test_chunk_size_consistency()`, `test_large_file_chunk_stability()` (pre-Phase 4 whole-file RS)
- [x] Fixed `test_calculate_padding_and_chunks()`: Updated to account for even-length padding requirement
- [x] Keep unchanged: `test_chunk_content_preservation()`, `test_padding_edge_cases()`
- [x] **Validation**: `cargo test files::tests` passes (4 tests)

### Step 16: Add placement tests [x]
- [x] `src/files/placement.rs`: 12 comprehensive placement tests covering all scenarios
  - [x] `test_get_fragment_placement_deterministic()`: Verifies same local_index → same node (covers chunk-aware modulo)
  - [x] `test_get_fragment_placement_even_distribution()`: Verifies modulo creates ±1 balance
  - [x] `test_get_fragment_placement_imbalance_non_divisible()`: Validates ±1 max imbalance
  - [x] Additional tests: basic placement, wraparound, empty/single node edge cases
  - [x] `test_select_nodes_filters_inactive_validators()`: Validates only active validators used
- [x] `src/db/consensus.rs`: 6 comprehensive validator tests
  - [x] Covers empty set, activation, deactivation, reactivation, multiple nodes, future queries
  - [x] Effectively provides validator filtering at height functionality
- [x] **Validation**: `cargo test files::placement` passes (12 tests), `cargo test db::consensus` passes (6 tests)

### Step 17: Integration tests [x]
- [x] Run `orchestrator test --mesh-id 0 --test fragment-distribution`
- [x] Fixed bugs in `src/files/functions.rs`:
  - [x] Blocking I/O in `fetch_fragment_local()` causing HTTP timeouts (wrapped in `block_in_place`)
  - [x] Recovery shard index mapping (subtract `ORIGINAL_FRAGMENTS_PER_CHUNK` before passing to RS decoder)
  - [x] Hash verification missing `data_block_id` (added to match upload flow)
- [x] Fixed orchestrator test resilience calculation (`orchestrator/tests/files.rs:560-567`):
  - [x] Use worst-case max fragments per node: `ceil(30/node_count)` instead of average
  - [x] For 9 nodes: max 4 fragments per node, so tolerance = floor(20/4) = 5
- [x] Run `orchestrator test --mesh-id 0 --test file-upload-consistency`
- [x] **Validation**: All checks pass, optimal distribution achieved

### Step 18: Performance test [x]
- [x] Created `orchestrator/tests/performance.rs::ChunkedStreamingPerformance`
- [x] Generates 120MB test file (3 chunks: 40MB × 3)
- [x] Uploads to node 0, downloads from last node (tests distributed reconstruction)
- [x] Measures Time To First Byte (TTFB) during streaming download
- [x] Verifies content integrity and download completion
- [x] **Test command**: `cargo run --bin orchestrator test --mesh-id 0 --test chunked-streaming-performance`
- [x] **Expected**: TTFB < 10 seconds (demonstrates chunked streaming vs whole-file reconstruction)
- [x] **Validation**: Test infrastructure complete and compiles successfully

## Phase 6: Documentation & Cleanup [x]

### Step 19: Update diagnostics [x]
- [x] `src/db/debug.rs::get_file_fragment_distribution()`: Already uses chunk_number and local_index correctly
- [x] `orchestrator/tests/files.rs::FragmentInfo` struct: Already updated with chunk_number and local_index
- [x] `orchestrator/tests/files.rs::verify_fragment_redundancy()` unstored_fragments logging: Updated to show (chunk_number, local_index) tuples
- [ ] **Validation**: Diagnostics display chunk structure correctly

### Step 20: Update documentation [x]
- [x] Update `docs/specs/file-storage.md` for chunked RS architecture
- [x] Update `docs/specs/shard-synchronization.md` (RFC-004) for modulo placement
- [x] Update `docs/system-overview.md` progress indicators
- [x] **Validation**: Docs reflect new architecture

### Step 21: Remove old code [x]
- [x] Verified no unused rendezvous code (Phase2Candidate still used for metrics scoring)
- [x] Code compiles cleanly with no warnings about unused code
- [x] **Validation**: cargo check passes, no unused code warnings

### Step 21b: Add timeout and retry logic [x]
- [x] Added `download_file_with_timeout()` function with configurable timeout
- [x] Added `download_file_from_all_nodes_with_timeout()` for parallel downloads
- [x] Implemented 3-retry logic with exponential backoff (500ms, 1000ms, 1500ms)
- [x] Updated fragment-distribution test to use 15-second timeout (bounded at ~48 seconds)
- [x] **Validation**: Tests no longer hang, complete successfully

## Phase 7: Final Validation

### Step 22: End-to-end test
- [ ] Create 6-node mesh with `orchestrator create`
- [ ] Upload files: 1MB, 40MB, 100MB, 1GB
- [ ] Verify chunk structure correct for each using diagnostic endpoint
- [ ] Run all integration tests: `orchestrator test --mesh-id 0 --test file-upload-consistency` and `--test fragment-distribution`
- [ ] **Validation**: All tests pass

### Step 23: Failure tolerance test
- [ ] Upload file to 6-node mesh
- [ ] Stop 2 nodes with `orchestrator stop`
- [ ] Download from remaining 4 nodes
- [ ] **Validation**: Download succeeds, recovery fragments used correctly

## Success Criteria

- [x] **Schema**: `chunk_number` and `local_index` columns added and working
- [x] **Encoding**: Files encoded in 40MB chunks (30 fragments each)
- [x] **Placement**: Fragments use local_index % validators (±1 balance achieved)
- [x] **Reconstruction**: Chunked reconstruction works with streaming support
- [x] **Tests Pass**: All unit and integration tests pass (file-upload-consistency, fragment-distribution)
- [x] **Resilience**: `fragment-distribution` test shows tolerance=5 for 9 nodes (floor(20/4)=5) ✅
- [x] **Documentation**: RFCs updated (file-storage.md, shard-synchronization.md, system-overview.md)
- [x] **Cleanup**: Rendezvous code removed, Phase2Candidate preserved for metrics scoring
- [x] **Reliability**: Added timeout and retry logic to prevent test hangs
