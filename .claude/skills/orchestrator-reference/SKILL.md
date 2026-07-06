---
name: orchestrator-reference
description: Reference documentation for the HopNet orchestrator. Covers command syntax, available tests, workflows, and diagnostics.
user-invocable: false
---

# HopNet Orchestrator Reference

The orchestrator is a Docker/Podman-based tool for testing and managing Byzantine fault-tolerant consensus networks.

## Location & Invocation

- **Source**: `orchestrator/main.rs`
- **Binary**: Defined in `Cargo.toml` as `[[bin]] name = "orchestrator"`

```bash
# Build the orchestrator (always do this first to get latest)
# Use --features skip-frontend for faster builds (skips frontend compilation)
cargo build --release --bin orchestrator --features skip-frontend

# Run commands
./target/release/orchestrator <command>
```

## Command Reference

### Mesh Management

| Command | Syntax | Description |
|---------|--------|-------------|
| **create** | `orchestrator create --nodes N [--no-cleanup]` | Create new mesh with N nodes. `--no-cleanup` keeps containers on failure for debugging. |
| **add** | `orchestrator add --mesh-id M --nodes N` | Add N nodes to existing mesh M |
| **list** | `orchestrator list` | List all active meshes with node counts |
| **delete** | `orchestrator delete --mesh-id M [-y]` | Delete mesh M and all resources. `-y` skips confirmation. |
| **cleanup** | `orchestrator cleanup [-y]` | Remove orphaned networks from deleted meshes |

### Diagnostics

| Command | Syntax | Description |
|---------|--------|-------------|
| **status** | `orchestrator status --mesh-id M` | View consensus state (view, phase, role) across all nodes |
| **divergence** | `orchestrator divergence --mesh-id M` | Detect state inconsistencies between nodes |
| **history** | `orchestrator history --mesh-id M --node N [--view V]` | Query consensus history for a specific node. Optional `--view` for detailed state at that view. |

### Testing

| Command | Syntax | Description |
|---------|--------|-------------|
| **test (auto-managed)** | `orchestrator test --test NAME [--auto-nodes N] [--keep-on-pass] [--flags ...]` | Auto-creates a fresh mesh, runs the test, runs a divergence check on pass, deletes the mesh on **both** test pass and clean divergence. Either failure leaves the mesh up; the mesh id and inspection/cleanup commands are printed. Default `--auto-nodes` is 3. `--keep-on-pass` retains the mesh even on full pass. |
| **test (caller-managed)** | `orchestrator test --mesh-id M --test NAME [--flags ...]` | Runs against an existing mesh; no auto-cleanup, no auto-divergence-check. Use when running multiple tests against the same mesh, or when reusing a previously-built mesh. |
| **test list** | `orchestrator test --list` | List all available tests |

**Auto-managed mode flow:**

```
create mesh (auto-id) → run test → if test fails: leave mesh, exit non-zero
                       └─ if test passes: run divergence check
                          ├─ if clean: delete mesh, exit 0 (or keep on --keep-on-pass)
                          └─ if divergent: leave mesh, exit non-zero
```

Pass means **test passed AND no divergence**. The mesh is only deleted when the entire pipeline is clean.

## Available Integration Tests

| Test Name | Description |
|-----------|-------------|
| `file-upload-consistency` | Files uploaded to one node are retrievable from all nodes |
| `fragment-distribution` | Fragments are properly distributed across mesh after upload |
| `fragment-health-check` | Fragment integrity check across the mesh |
| `multi-size-file-consistency` | Various file sizes (small, medium, large) work correctly |
| `chunked-streaming-performance` | Streaming performance with chunked RS encoding |
| `restart-persistence` | Data survives node restarts |
| `device-token-consistency` | Device token management across nodes |
| `documentprovider-write-consistency` | Document provider write APIs (upload, rename, move, delete) |
| `multi-user-isolation` | User data isolation in multi-user scenarios |
| `multi-user-sharing` | File sharing between users with share acceptance |
| `multi-user-sharing-live-link` | Live-link propagation for file shares |
| `metrics-collection` | System metrics collection and validation |
| `iroh-ping` | Verify iroh transport connectivity between all nodes in the mesh |
| `iroh-reject-unknown` | Verify unknown peers are rejected before path registration (no IP leak) |
| `consensus-leader-down` | Idle mesh's pending proposer is stopped, work submitted elsewhere still decides (wake rules + round rotation); proposer rejoins and catches up. **Requires `HOPNET_QUORUM_PROFILE=majority`** |
| `consensus-lagging-catch-up` | Node offline while the chain advances, decided-value-syncs back to the tip on rejoin. **Requires `HOPNET_QUORUM_PROFILE=majority`** |
| `consensus-bft-quorum-loss` | Negative control on the DEFAULT profile: BFT 3-node mesh must NOT decide with one node down; progress resumes at full quorum. Run WITHOUT profile env |
| `consensus-barrier-decide-window` | `before_decide` held on one node opens an observable divergence window while quorum decides; release converges. **Requires majority profile** |
| `consensus-barrier-proposal-hold` | `before_publish_proposal` held on the proposer stalls its transaction; release recovers it through a later round. **Requires majority profile** |
| `consensus-queue-burst` | 10 concurrent mixed ops at one node, validates batching efficiency and cross-node consistency |
| `consensus-queue-cross-node` | Two-phase ACK forwarding — operations sent to different nodes concurrently, verifies cross-node coordination |
| `consensus-queue-throughput` | 30s sustained mixed-operation load, measures ops-per-height throughput (≥80% success rate) |
| `takeout-happy-path` | Upload files, initiate takeout, wait for Ready, download archive, verify manifest contract and per-file byte+hash match |
| `import-create-active-conflict` | POST `/takeout/import` creates a Pending row visible on all nodes via consensus; concurrent POSTs (same node or cross-node, same user) are rejected 429 |
| `import-upload-happy-path` | Valid manifest-only tar.gz upload returns 201 and produces a Pending import row visible on every node |
| `import-upload-version-rejected` | Manifest with future schema version → 400, no import row created |
| `import-upload-missing-manifest` | tar.gz whose first entry is not `manifest.json` → 400, no import row created |
| `import-upload-quota-exceeded` | Manifest claiming `total_bytes` above network capacity → 507, no import row created |
| `import-extraction-happy-path` | Multi-entry archive (correct hashes): bg extraction flips status to Importing, seeds 5 Pending rows in `import_paths_{id}`, no row marked failed |
| `import-extraction-hash-mismatch` | Archive whose tar bytes diverge from manifest's `file_hash` for one file: that row marked Failed (`hash_mismatch`); peer rows stay Pending |
| `import-creation-happy-path` | End-to-end import: extraction + creation walk → status Completed; all path rows Imported; every file queryable on every node |
| `import-creation-mixed-failure` | One file fails extraction (hash mismatch); creation walk imports survivors; status Completed; corrupted row remains Failed; survivors queryable, corrupted not |
| `import-write-gate` | POST `/files` returns 409 mid-import for the same user (cross-node enforcement); succeeds after status Completed |
| `import-status-counts` | `GET /takeout/import/status` returns aggregate counts after mixed-failure import (`imported=4, failed=1, total=5`); non-owner returns 404 |
| `import-resume-after-restart` | Stop owner mid-import, restart, re-login: status stays Importing pre-login; login hook fires resume; creation walk completes; all files queryable cross-node |
| `post-files-consensus-shape` | Upload N files in a single POST `/files` request; assert one consensus view advance and all N files visible on every node (tripwire for batching regressions in `post_files`) |
| `mixed-files-and-folders-one-request` | Upload N files into a deep nested path with no pre-existing parents; assert single view advance and all parents + files visible on every node |

## Common Workflows

### Development Workflow (one-shot test, recommended)

Most CI-style usage: auto-managed mode handles mesh creation, divergence check, and cleanup in one command.

```bash
# 1. Build the HopNet Docker image via nix flake (if source changed)
# Choose the target matching the Docker host architecture:
#
#   macOS (Apple Silicon): use aarch64-linux. macOS cannot build Linux
#   derivations natively, so this requires the remote builder configured
#   in /etc/nix/machines (ssh://builder@nixos.orb.local).
#
#   Linux x86_64: use x86_64-linux. Builds locally, no remote builder needed.
#
nix build .#packages.aarch64-linux.dockerImage   # macOS / Apple Silicon
# nix build .#packages.x86_64-linux.dockerImage  # Linux x86_64

# 2. Load the image into Docker
docker load < result

# 3. Build orchestrator
cargo build --release --bin orchestrator --features skip-frontend

# 4. Run a test (auto-creates mesh, runs divergence check, deletes on full pass)
./target/release/orchestrator test --test file-upload-consistency
```

On failure (test or divergence), the mesh is left up. Inspection commands are printed; clean up with `delete --mesh-id <id> -y` when done.

### Testing Workflow (multiple tests against the same mesh)

Caller-managed mode when a mesh should survive across multiple test invocations.

```bash
# Build orchestrator first
cargo build --release --bin orchestrator --features skip-frontend

# Create mesh explicitly
./target/release/orchestrator create --nodes 3

# Run multiple tests against the same mesh
./target/release/orchestrator test --mesh-id 0 --test file-upload-consistency
./target/release/orchestrator test --mesh-id 0 --test fragment-distribution
./target/release/orchestrator test --mesh-id 0 --test multi-size-file-consistency

# Manual divergence check (auto-managed mode does this automatically)
./target/release/orchestrator divergence --mesh-id 0

# Cleanup when done
./target/release/orchestrator delete --mesh-id 0 -y
```

### Diagnostics Workflow

When debugging consensus or state issues:

```bash
# 1. Check current consensus state across nodes
./target/release/orchestrator status --mesh-id 0

# 2. Look for state divergence
./target/release/orchestrator divergence --mesh-id 0

# 3. Inspect specific node's history
./target/release/orchestrator history --mesh-id 0 --node 0

# 4. Check state at specific view
./target/release/orchestrator history --mesh-id 0 --node 0 --view 5
```

### Debugging Failed Tests

When `--no-cleanup` is used on create, containers remain for inspection:

```bash
# Create mesh that won't auto-cleanup on failure
./target/release/orchestrator create --nodes 3 --no-cleanup

# If test fails, containers remain running
# Check container logs
docker logs hopnet-orchestrator-0-0

# Inspect container state
docker exec -it hopnet-orchestrator-0-0 /bin/sh

# Manual cleanup when done
./target/release/orchestrator delete --mesh-id 0 -y
```

### Cross-Node Request Tracing

HopNet uses two tracing span types for end-to-end request correlation:

- **`api_req{id, method, uri}`** — HTTP API requests from users. The `id` is a truncated UUID.
- **`rpc_req{id, to/from}`** — Iroh RPC calls between nodes. The `id` is a 16-hex-char request ID shared between sender (`to=<node_id>`) and receiver (`from=<node_id>`).

When an HTTP request triggers inter-node communication (e.g., file upload → transaction forward), `rpc_req` spans nest inside the `api_req` span on the sender. On the receiving node, the same `rpc_req` ID appears at the top level. This enables cross-node tracing:

```bash
# 1. Find the HTTP request on the originating node
docker logs hopnet-orchestrator-0-0 --timestamps 2>&1 | grep "api_req.*POST.*files"

# 2. Find rpc_req IDs spawned by that HTTP request (nested spans)
docker logs hopnet-orchestrator-0-0 --timestamps 2>&1 | grep "rpc_req.*<id-from-step-1>"

# 3. Trace the same rpc_req ID on the receiving node
docker logs hopnet-orchestrator-0-1 --timestamps 2>&1 | grep "rpc_req{id=<hex-id>}"
```

The `rpc_req` ID is also used for request-level deduplication — retried requests reuse the same ID, so the receiver can coalesce duplicates.

## Understanding Output

### Status Output (malachite engine)
- **LEADER**: The current/pending round's proposer (deterministic rotation)
- **View**: The engine HEIGHT (the `/consensus` shim reports `view := height`;
  with on-demand heights an idle mesh shows the PENDING height = decided + 1)
- **Phase**: Synthetic `"Propose"` (Tendermint rounds aren't surfaced yet)

### Divergence Output
- **Consensus**: All nodes at same height with same state hashes
- **Divergence**: Different table hashes at the same decided height (bug)
- **Catch-up**: Nodes at lower decided heights (normal during sync; an idle
  restarted node stays paused until work or a peer message arrives)
- Note: `decided_certificates` and `consensus_wal` are intentionally NOT
  compared — certificates are node-local quorum proofs and may legitimately
  contain different vote subsets

### Test Results
- **PASSED**: All checks succeeded
- **FAILED**: One or more checks failed
- Check details show exactly what passed/failed

## Container Naming Convention

- **Networks**: `hopnet-orchestrator-{mesh_id}-0`
- **Containers**: `hopnet-orchestrator-{mesh_id}-{node_id}`
- **Relay**: `hopnet-orchestrator-{mesh_id}-relay` (one per mesh — see below)
- **Volumes**: `hopnet-orchestrator-{mesh_id}-{node_id}-data`

## Self-Hosted Iroh Relay

Every mesh gets its own `iroh-relay --dev` container (same hopnet image,
entrypoint override, plain HTTP on `:3340`). Node containers receive
`HOPNET_RELAY_URL=http://hopnet-orchestrator-{mesh_id}-relay:3340`, which
switches their endpoints to that single relay with NO public discovery —
mesh tests have zero dependency on n0's public relay/DNS infrastructure
(which rate-limits under mesh churn and used to flake mesh creation).

## Mesh Environment Variables

Set on the orchestrator process at mesh CREATION time; forwarded into node
containers:

- `HOPNET_QUORUM_PROFILE=majority` — CFT majority quorum (default: `bft`).
  Required by the consensus tests that expect progress with a node down.
- `HOPNET_CONSENSUS_TIMEOUT_MS=<ms>` — round-0 propose timeout (votes and
  per-round deltas scale with it). Small values (e.g. 2000) make
  leader-down round advances fast in tests.
- `HOPNET_DB_*` — SQLite pragma tuning (see `src/db/shared.rs`).

Example:

```bash
HOPNET_QUORUM_PROFILE=majority HOPNET_CONSENSUS_TIMEOUT_MS=2000 \
  ./target/release/orchestrator test --test consensus-leader-down
```

## Port Mapping

- Internal port: `34632`
- Host port (macOS/Podman): `40000 + (mesh_id * 500) + node_id`
  - Mesh 0: ports 40000-40499
  - Mesh 1: ports 40500-40999
