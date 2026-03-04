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
| **test** | `orchestrator test --mesh-id M --test NAME [--flags ...]` | Run named integration test |
| **test list** | `orchestrator test --list` | List all available tests |

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
| `timeout-progression` | Verify timeout votes broadcast over iroh, form TC, and advance view when leader is down |
| `consensus-barrier-basic` | Hold leader between Propose and Lock phases, verify barrier mechanism and QC propagation |
| `consensus-barrier-missed-ballot` | Hold follower's ballot dispatch, let consensus proceed without it, verify message-driven catch-up |
| `consensus-barrier-tc-late` | Diagnostic: TC commits before Lock QC arrives — check for metadata divergence |
| `consensus-queue-burst` | 10 concurrent mixed ops at one node, validates batching efficiency and cross-node consistency |
| `consensus-queue-cross-node` | Two-phase ACK forwarding — operations sent to different nodes concurrently, verifies cross-node coordination |
| `consensus-queue-throughput` | 30s sustained mixed-operation load, measures ops-per-view throughput (≥80% success rate) |

## Common Workflows

### Development Workflow

When testing code changes against the orchestrator:

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

# 3. Build orchestrator (skip-frontend for faster builds)
cargo build --release --bin orchestrator --features skip-frontend

# 4. Create a test mesh
./target/release/orchestrator create --nodes 3

# 5. Run tests against the mesh
./target/release/orchestrator test --mesh-id 0 --test file-upload-consistency

# 6. Check for state divergence
./target/release/orchestrator divergence --mesh-id 0

# 7. Clean up when done
./target/release/orchestrator delete --mesh-id 0 -y
```

### Testing Workflow

Full test suite against a mesh:

```bash
# Build orchestrator first (skip-frontend for faster builds)
cargo build --release --bin orchestrator --features skip-frontend

# Create mesh
./target/release/orchestrator create --nodes 3

# Run multiple tests
./target/release/orchestrator test --mesh-id 0 --test file-upload-consistency
./target/release/orchestrator test --mesh-id 0 --test fragment-distribution
./target/release/orchestrator test --mesh-id 0 --test multi-size-file-consistency

# Check overall health
./target/release/orchestrator divergence --mesh-id 0

# Cleanup
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

### Status Output
- **LEADER**: Node leading current consensus view
- **FOLLOWER**: Node following the leader
- **View**: Current consensus view number (higher = more recent)
- **Phase**: Consensus phase (Propose, Lock, Commit, etc.)

### Divergence Output
- **Consensus**: All nodes at same view with same state hash
- **Divergence**: Different hashes at same view (consensus broken - bug)
- **Catch-up**: Nodes at lower views (normal during sync)

### Test Results
- **PASSED**: All checks succeeded
- **FAILED**: One or more checks failed
- Check details show exactly what passed/failed

## Container Naming Convention

- **Networks**: `hopnet-orchestrator-{mesh_id}-0`
- **Containers**: `hopnet-orchestrator-{mesh_id}-{node_id}`
- **Volumes**: `hopnet-orchestrator-{mesh_id}-{node_id}-data`

## Port Mapping

- Internal port: `34632`
- Host port (macOS/Podman): `40000 + (mesh_id * 500) + node_id`
  - Mesh 0: ports 40000-40499
  - Mesh 1: ports 40500-40999
