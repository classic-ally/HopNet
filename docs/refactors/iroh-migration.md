# Iroh Transport Migration

Migrate inter-node communication from HTTP/TCP to iroh for NAT traversal, built-in encryption, and connection multiplexing.

## Why Iroh

- **NAT traversal** - DERP relay fallback when hole-punching fails
- **Built-in encryption** - TLS 1.3 over QUIC, Ed25519 keys (no separate TLS infrastructure)
- **Multiplexed streams** - Multiple consensus messages on one connection
- **Streaming** - 4MB fragments without buffering entire payload
- **Node addressing** - Dial by public key, not IP:port

## Key Insight: Reuse Existing Keys

iroh's `NodeId` is an Ed25519 public key - identical to our existing `PubKey`. No new key infrastructure needed:

```rust
// Both are 32-byte Ed25519 keys, direct byte conversion
impl PubKey {
    pub fn to_iroh_node_id(&self) -> iroh::PublicKey {
        iroh::PublicKey::from_bytes(&self.0.to_bytes()).expect("valid ed25519 key")
    }
}
```

## Test-Driven Migration Strategy

Use the orchestrator's divergence detection as a safety net. After each phase, run existing integration tests to verify inter-node communication and zero divergence.

---

## Phase 0: Key Conversion Impls
**Status:** [x] Complete

Add conversion methods to existing key types:

- [x] `PrivKey::to_iroh_secret_key() -> iroh::SecretKey`
- [x] `PubKey::to_iroh_node_id() -> iroh::PublicKey`
- [ ] Unit tests for round-trip conversion

**Validation:** Unit tests pass

---

## Phase 1: IrohTransport Scaffolding + Ping/Pong
**Status:** [x] Complete

### Overview

Add iroh transport layer in `src/net/`. Nodes establish iroh connections on startup. Validate with ping/pong (connection infrastructure for warming, keepalive, health).

### Components

- `src/net/mod.rs` - `IrohTransport` with `Endpoint` and connection pool
- `src/net/protocol.rs` - Message envelope with `Ping`/`Pong` variants
- `src/net/transport.rs` - Wire format helpers and error types
- `src/net/handler.rs` - Accept loop for incoming connections
- `src/net/routes.rs` - Debug endpoint for ping testing
- AppState integration - Store transport, start accept loop on startup

### Protocol

Single ALPN (`hopnet/1.0`), length-prefixed bincode messages (4-byte length, 8MB max). Ping/Pong for connection lifecycle.

### Error Handling

Two-tier error categorization matching HTTP semantics:
- `IrohError::Transport` - Retryable (connection failed, stream failed, timeout)
- `IrohError::Protocol` - Non-retryable (value mismatch, peer error, malformed response)

### Orchestrator Test: `iroh-ping`

- Create 3-node mesh
- Each node pings all other nodes via `GET /debug/iroh-ping`
- Verify all pongs received
- Divergence = 0

### Checklist

- [x] Add `iroh` dependency (iroh 0.96.1)
- [x] Key conversion on `PrivKey`/`PubKey`
- [x] Create `src/net/` module with `IrohTransport`
- [x] Define protocol enums with Ping/Pong
- [x] Integrate into AppState, start accept loop
- [x] Add `GET /debug/iroh-ping` endpoint (pings all nodes)
- [x] Create `orchestrator/tests/iroh_ping.rs`

**Validation:** `orchestrator test --test iroh-ping` passes

---

## Phase 2: Fragment Health Checks
**Status:** [x] Complete

### Overview

Migrated fragment health checks from HTTP to iroh — first real domain message type. Validates the migration pattern for subsequent phases.

### Architecture

- `src/files/rpc.rs` — Domain RPC module: request/response types, server handler, client caller
- `src/net/protocol.rs` — Thin dispatch enum wrapping `files::rpc` types
- `src/net/handler.rs` — Dispatches to `files::rpc::handle_fragment_health_check()`
- `src/net/transport.rs` — Generic `request()` method for RPC lifecycle
- `src/files/discovery.rs` — Health checks over iroh, data fetch stays HTTP (Phase 4)

Pattern: each module owns `rpc.rs` (inter-node) alongside `routes.rs` (HTTP API). Transport provides the `request()` primitive; domain modules build typed RPCs on top.

### Changes

- Added `FragmentHealthCheck` / `FragmentHealthResult` to protocol enums
- Added `IrohTransport::request()` generic RPC method (open_bi/send/finish/recv)
- Created `src/files/rpc.rs` with server handler and client caller
- Updated `src/files/discovery.rs` to use iroh for health checks
- Added `pubkey: PubKey` to `NodeConnectionInfo` and `NodeMetrics`
- Fixed `Blake3Hash` serde to use raw bytes for binary formats (bincode)
- HTTP `/fragments/{hash}/health` endpoint preserved (other consumers may use it)

### Checklist

- [x] Fix `Blake3Hash` serde for efficient wire encoding
- [x] Add `pubkey` to `NodeConnectionInfo` and `NodeMetrics`, update SQL queries
- [x] Add protocol variants for fragment health
- [x] Create `src/files/rpc.rs` with handler and client
- [x] Add `IrohTransport::request()` generic RPC method
- [x] Update `src/files/discovery.rs` to use iroh transport
- [x] Thread `IrohTransport` through `find_fragment` callers in `functions.rs`
- [ ] Run `fragment-distribution` test to validate

**Validation:**
- `orchestrator test --test fragment-distribution` passes
- Divergence = 0

---

## Phase 3: Consensus Messages
**Status:** [x] Complete

### Overview

Migrate consensus messages one at a time, ordered by risk. Test after each migration.

### Migration Order

1. **View sync** (`/consensus/view/{view}`) - Read-only catch-up, lowest risk
2. **Timeout vote** (`/consensus/timeout_vote`) - Fire-and-forget broadcast
3. **TC broadcast** (`/consensus/tc`) - Similar pattern to timeout vote
4. **QC broadcast** (`/qc`) - Commits state, higher stakes
5. **Ballot submission** (`/ballot`) - Core voting, requires response
6. **Transaction forwarding** (`/consensus/propose`) - Leader forwarding

### Phase 3a: View Sync

- `src/consensus/rpc.rs` — Domain RPC module: request/response types, server handlers, client callers
- `src/net/protocol.rs` — `ViewDataFetch`/`ViewPoll` request variants, `ViewDataFetchResponse`/`ViewPollResponse` response variants
- `src/net/handler.rs` — Dispatches to `consensus::rpc` handlers
- `src/consensus/routes.rs` — `fetch_view()` uses iroh instead of HTTP
- `src/consensus/functions.rs` — `poll_subset_for_max_view()` uses iroh instead of HTTP

View poll sends only the view number (not the full `ConsensusState`), reducing wire overhead.
HTTP endpoints (`GET /consensus`, `GET /consensus/view/{view}`) preserved for external consumers.

### Key Concerns

- **Connection failures mid-round**: Need graceful degradation, not panics
- **Message ordering**: Consensus assumes certain ordering guarantees
- **Warm connections**: Pool should be warm before consensus starts
- **Broadcast patterns**: Some messages go to all validators in parallel
- **Catch-up dispatch**: Message-driven catch-up in `handler.rs` replaces the old HTTP middleware. Before dispatching any consensus message, extracts the message's view via `IrohRequest::consensus_view()`. If ahead of ours, performs cross-view catch-up. For Lock-phase ballots, also triggers intra-view sync if missing the Propose QC. Uses lightweight `get_consensus_progress()` (one join, two integers) instead of full `get_consensus()`. Zero overhead on the happy path.

### Checklist

- [x] View sync over iroh
- [x] Timeout vote over iroh
- [x] TC broadcast over iroh
- [x] QC broadcast over iroh
- [x] Ballot submission over iroh
- [x] Transaction forwarding over iroh
- [x] Message-driven catch-up dispatch (replaces HTTP middleware)

**After each migration:** Run full test suite, verify divergence = 0

**Validation:**
- All existing integration tests pass
- `device-token-consistency` passes
- `documentprovider-write-consistency` passes

---

## Phase 3f: Consensus Barrier Testing Infrastructure
**Status:** [x] Complete

### Overview

Add conditional barriers at key consensus stages to enable controlled-timing tests for edge cases that are impossible to reproduce on the happy path. The full consensus pipeline must be on iroh first (transaction forwarding included) so barriers give end-to-end coverage.

### Motivation

Consensus safety is the hardest invariant to verify. Existing integration tests validate the happy path, but dangerous bugs hide in edge cases: missed phases, partial sync, stale state, race conditions between QC/TC application. These scenarios require controlled timing to reproduce — the system is designed to process consensus rounds as fast as possible, leaving no natural window to inject failures.

### Design

Conditional `AtomicBool` + `tokio::sync::Notify` barriers in `AppState`, activated only when `test_mode` is true (zero overhead in production):

- `before_ballot_dispatch` — hold a node before it processes an incoming ballot
- `after_propose_qc_broadcast` — hold after Propose QC is sent but before Lock ballot
- `before_tc_gst_wait` — hold before TC enters GST wait (before lock acquisition)
- `before_tc_application` — hold before timeout certificate advances the view
- `before_lock_qc_broadcast` — hold after Lock QC is formed but before broadcast/DB write

Orchestrator tests control the flow: hold a barrier on one node, let other nodes progress, release and verify catch-up/sync behavior.

### Test Scenarios

- **Intra-view sync**: hold node through Propose phase, release during Lock phase, verify it fetches missing Propose QC before processing Lock ballot
- **Cross-view catch-up**: hold node through entire view(s), release, verify message-driven catch-up fires and node rejoins
- **Lock QC vs TC safety (Scenario A)**: hold Lock QC broadcast + TC GST wait, let TC form on followers, release Lock QC first → Lock QC wins, TC rejected by Layer 2 staleness check
- **TC commits before Lock QC (Scenario B)**: hold only Lock QC broadcast, let TC fully commit on followers, then release Lock QC → diagnostic test for metadata divergence
- **Cascade timeout propagation**: hold one node, let others timeout, release and verify cascade

### Checklist

- [x] Add `ConsensusBarriers` struct to `AppState`
- [x] Insert barrier points at consensus stage boundaries (5 barriers)
- [x] Orchestrator test: barrier basic (hold/release between Propose and Lock)
- [x] Orchestrator test: cross-view catch-up (node paused for full view)
- [x] Orchestrator test: Lock QC vs TC race (Scenario A — Lock QC wins)
- [x] Orchestrator test: TC-late diagnostic (Scenario B — TC commits first)
- [x] Run and verify all barrier tests on orchestrator mesh

**Validation:** All barrier tests pass, existing tests unaffected (barriers are no-ops when not held)

---

## Phase 4: Fragment Transfers
**Status:** [ ] Not Started

Large data streaming over iroh:

- [ ] Fragment fetch (`GET /fragments/{hash}`)
- [ ] Fragment store (`POST /fragments/{hash}`)
- [ ] Use iroh streaming (no 4MB buffer)

**Validation:**
- `file-upload-consistency` passes
- `multi-size-file-consistency` passes
- `fragment-distribution` passes

---

## Phase 5: Deprecate HTTP Inter-Node
**Status:** [ ] Not Started

Remove HTTP for node-to-node communication:

- [ ] Remove HTTP inter-node routes
- [ ] Remove `reqwest` client creation for inter-node calls
- [ ] Keep HTTP for client-facing APIs (or migrate later)
- [ ] Update orchestrator container networking (iroh handles NAT)

**Validation:**
- Full test suite passes
- No HTTP inter-node traffic in logs

---

## Connection Architecture

Single connection per peer, multiple streams:

```
Node A ────── [ONE QUIC Connection] ────── Node B
            ├── Stream 1: ballot vote
            ├── Stream 2: QC broadcast
            ├── Stream 3: fragment upload (4MB)
            └── Stream N: ...
```

ALPN negotiated once at connection establishment. All message types share the connection, differentiated by message envelope.

---

## Android Client Implications

iroh-ffi has experimental Kotlin bindings. Options for mobile:

1. **Wait for better FFI** - n0 actively working on this
2. **Relay-only for mobile** - Android connects via DERP relay (simpler)
3. **HTTP gateway** - Keep HTTP for mobile, nodes speak iroh to each other

Recommend option 3 initially - focus on node-to-node iroh first, extend to mobile when FFI matures.