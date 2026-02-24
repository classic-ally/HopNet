---
name: snapshotter-reference
description: Reference documentation for the HopNet snapshotter tool. Covers DB function regression testing, snapshot capture, cross-commit comparison, and fixture data.
user-invocable: false
---

# HopNet Snapshotter Reference

The snapshotter is a DB function regression tool that captures deterministic JSON snapshots of every DB read function's output against known fixture data, then compares snapshots across git commits.

## Location & Invocation

- **Source**: `snapshotter/main.rs`
- **Binary**: Defined in `Cargo.toml` as `[[bin]] name = "snapshotter"`

```bash
# Build the snapshotter
cargo build --release --bin snapshotter --features skip-frontend

# Run commands
./target/release/snapshotter <command>
```

## Command Reference

| Command | Syntax | Description |
|---------|--------|-------------|
| **capture** | `snapshotter capture [--output <path>]` | Capture snapshot against current build. Default output: `snapshot.json` |
| **capture-at** | `snapshotter capture-at --commit <hash> [--output <path>]` | Build at a git commit via worktree, capture, clean up |
| **compare** | `snapshotter compare --baseline <path> --current <path> [--epsilon <f64>]` | Diff two snapshots. Exit 0 if identical, 1 if differences. Default epsilon: `1e-10` |

## How It Works

### Capture Pipeline

1. Creates an **ephemeral in-memory SQLite database** (no disk I/O)
2. Initializes schema via `db::shared::initialize()` with custom SQL functions
3. Seeds **deterministic fixture data** (users, nodes, blocks, files, metrics, etc.)
4. Calls **72 DB read functions** with known inputs
5. Serializes all results to a single JSON snapshot file

### Fixture Data

All fixture data is fully deterministic — UUIDs use index-derived bytes, timestamps use a fixed epoch, keys use const seeds.

| Category | Count | Details |
|----------|-------|---------|
| Users | 2 | alice (user 0), bob (user 1) |
| Nodes | 3 | node-0 through node-2 |
| Validators | 3 | All active at height 0 |
| Blocks | 6 | Genesis + heights 1-5 |
| QCs | 12 | Propose + Lock for each block |
| TCs | 1 | At view 3 |
| Inodes | 6 | 2 folders + 4 files (nested 2 deep) |
| Data blocks | 3 | With 9 fragments (2 original + 1 recovery each) |
| File access | 3 | Entries for user 0 |
| Metrics | 12 | 6 node pairs x 2 timestamps |
| Fragment inventory | 27 | 9 fragments x 3 nodes |
| Device tokens | 2 | 1 per user |
| Shares | 1 | User 0 shares with user 1 |
| Committed nonces | 3 | For dedup queries |
| Modification log | 4 | 3 inserts + 1 move |

### Functions Captured (72 total)

Organized by module:

- **consensus** (22): get_consensus, get_consensus_history, get_validators (x2), get_validators_elect, get_node_pubkey (x2), get_all_node_pubkeys, get_all_user_pubkeys, get_consensus_progress, get_me, get_startup_state, get_view_consensus_data (x3), get_block (x2), get_quorum_certificate_by_hash (x2), check_committed_nonces, is_node_active (x2)
- **metrics** (3): get_metric, get_all_node_metrics, get_nodes_to_measure
- **debug** (1): compute_state_snapshot
- **resilience** (3): compute_network_resilience_stats, get_node_storage_baselines, generate_fault_tolerance_curve
- **files** (3): get_files, get_local_fragment_count, get_file_access
- **fragments** (3): get_node_availability_classification (x3, one per node)
- **inventory** (5): compute_inventory_differential (x3), batch_query_fragment_inventory
- **fileprovider** (6): get_folder_contents, get_folder_changes_since_height, get_item_metadata_by_inode_id, get_file_path_by_data_id, get_inode_id_by_path, is_folder_empty
- **documentprovider** (4): get_item, get_download_metadata, get_path_by_inode_id, get_children
- **nodes** (5): get_nodes, get_next_node_id, node_exists (x2), get_all_nodes_as_connection_info
- **users** (5): get_users, get_user_by_username (x2), get_user_by_userid (x2)
- **shares** (4): get_incoming_shares_for_user, get_incoming_share_count (x2), get_share_details
- **devices** (3): get_device_by_id, get_devices_for_user (x2)
- **takeout** (4): has_active_takeout (x2), calculate_user_data_size, get_takeouts_by_user, get_expired_takeouts_needing_status_update
- **setup** (1): get_initial_setup

## Snapshot JSON Format

```json
{
  "version": 1,
  "git_commit": "6914f79",
  "git_dirty": false,
  "captured_at": "2026-02-23T15:30:00Z",
  "fixture_version": "1.0",
  "functions": {
    "db::consensus::get_consensus": {
      "status": "ok",
      "value": { ... }
    },
    "db::files::get_files(user=0,path=/root)": {
      "status": "error",
      "error_variant": "NotFound"
    }
  }
}
```

- `functions` is a `BTreeMap` for deterministic key ordering
- Function keys include parameter suffixes when called with different inputs
- Values are `serde_json::Value` (any JSON)

## Common Workflows

### Regression Testing After Code Changes

```bash
# 1. Capture baseline before changes
./target/release/snapshotter capture --output baseline.json

# 2. Make code changes to src/db/*.rs

# 3. Rebuild snapshotter
cargo build --release --bin snapshotter --features skip-frontend

# 4. Capture after changes
./target/release/snapshotter capture --output current.json

# 5. Compare
./target/release/snapshotter compare --baseline baseline.json --current current.json
```

### Cross-Commit Comparison

```bash
# Compare current build against a specific commit
./target/release/snapshotter capture-at --commit abc1234 --output old.json
./target/release/snapshotter capture --output new.json
./target/release/snapshotter compare --baseline old.json --current new.json
```

Note: `capture-at` uses `git worktree` — the target commit must have the snapshotter binary. For commits before the snapshotter existed, you need to manually cherry-pick the snapshotter code onto a branch from that commit.

### Verifying Determinism

```bash
# Run capture twice and compare — should report IDENTICAL
./target/release/snapshotter capture --output a.json
./target/release/snapshotter capture --output b.json
./target/release/snapshotter compare --baseline a.json --current b.json
```

## Compare Output

```
Comparing: baseline.json (abc1234) vs current.json (def5678)

--- Comparison Report ---
Unchanged: 70

Added (1):
  + db::new_module::new_function

Changed (1):
  ~ db::metrics::get_all_node_metrics — value changed

Result: DIFFERENCES FOUND
```

- **Unchanged**: Functions with identical output
- **Added**: Functions in current but not baseline
- **Removed**: Functions in baseline but not current
- **Changed**: Functions in both with differing output
- Exit code 0 = identical, 1 = differences found

## Architecture

```
snapshotter/
  main.rs              # CLI (clap): capture, capture-at, compare
  schema.rs            # Snapshot, FunctionResult types
  capture.rs           # Pool creation, fixture seeding, function capture
  compare.rs           # Load two snapshots, deep-diff with float epsilon
  capture/
    fixtures/
      mod.rs           # seed_all(pool) -> FixtureContext
      keys.rs          # Deterministic Ed25519, X25519, AES-SIV keys from const seeds
      genesis.rs       # User 0, node 0, genesis block, QCs, this_node
      population.rs    # All remaining test data via actual Rust insert functions
    functions/
      mod.rs           # capture_all(pool, ctx) -> BTreeMap<String, FunctionResult>
      helpers.rs       # Generic wrap() for Serialize return types
      consensus.rs     # ~22 wrappers
      metrics.rs       # ~3 wrappers
      debug.rs         # compute_state_snapshot (proxy struct for HashMap ordering)
      resilience.rs    # ~3 wrappers
      files.rs         # ~3 wrappers (with SIV encryption)
      fragments.rs     # ~3 wrappers (proxy for AvailabilityClass)
      inventory.rs     # ~5 wrappers
      fileprovider.rs  # ~6 wrappers (proxy for FileProviderEnumerateResult)
      documentprovider.rs  # ~4 wrappers
      nodes.rs         # ~5 wrappers
      users.rs         # ~5 wrappers
      shares.rs        # ~4 wrappers (proxy for IncomingShareRow)
      devices.rs       # ~3 wrappers (proxy for DeviceTokenRecord)
      takeout.rs       # ~4 wrappers
      setup.rs         # ~1 wrapper
```

## Design Decisions

- **Fixtures use actual Rust insert functions** — tests both write AND read paths
- **Non-Serialize types use proxy structs** — no changes needed to `src/` types
- **Ephemeral in-memory DB** — no cleanup needed, fast execution
- **Float epsilon comparison** — handles floating-point rounding differences (default 1e-10)
- **BTreeMap for function registry** — deterministic key ordering in JSON output
- **Sorted HashMap/HashSet outputs** — proxy structs sort non-deterministic collections

## Troubleshooting

### Pool Deadlock
The ephemeral pool has `max_size=1`. Each transaction block in `population.rs` gets its own connection (scoped `pool.get()`). Functions like `insert_block()` that internally call `pool.get()` will deadlock if another connection is held.

### Non-Deterministic Output
If `compare` reports differences between two captures of the same code:
- Check for `HashMap`/`HashSet` iteration in function wrappers — sort the output
- Check for `Utc::now()` or `CustomUUID::new(None)` in fixtures — use fixed values
- Check for random SQL ordering — the underlying query may need `ORDER BY`
