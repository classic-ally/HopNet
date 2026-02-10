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
**Status:** [ ] Not Started

### Overview

Migrate fragment health checks to iroh - first real message type. Low-risk (read-only), validates the migration pattern.

### Changes

- Add `FragmentHealthCheck` / `FragmentHealthResponse` to protocol enums
- Update fragment discovery to use iroh instead of HTTP for health checks
- Remove HTTP `/fragments/{hash}/health` endpoint (or keep as fallback initially)

### Checklist

- [ ] Add protocol variants for fragment health
- [ ] Update `src/files/discovery.rs` to use iroh transport
- [ ] Run `fragment-distribution` test to validate

**Validation:**
- `orchestrator test --test fragment-distribution` passes
- Divergence = 0

---

## Phase 3: Consensus Messages
**Status:** [ ] Not Started

### Overview

Migrate consensus messages one at a time, ordered by risk. Test after each migration.

### Migration Order

1. **View sync** (`/consensus/view/{view}`) - Read-only catch-up, lowest risk
2. **Timeout vote** (`/consensus/timeout_vote`) - Fire-and-forget broadcast
3. **TC broadcast** (`/consensus/tc`) - Similar pattern to timeout vote
4. **QC broadcast** (`/qc`) - Commits state, higher stakes
5. **Ballot submission** (`/ballot`) - Core voting, requires response
6. **Transaction forwarding** (`/consensus/propose`) - Leader forwarding

### Key Concerns

- **Connection failures mid-round**: Need graceful degradation, not panics
- **Message ordering**: Consensus assumes certain ordering guarantees
- **Warm connections**: Pool should be warm before consensus starts
- **Broadcast patterns**: Some messages go to all validators in parallel

### Checklist

- [ ] View sync over iroh
- [ ] Timeout vote over iroh
- [ ] TC broadcast over iroh
- [ ] QC broadcast over iroh
- [ ] Ballot submission over iroh
- [ ] Transaction forwarding over iroh

**After each migration:** Run full test suite, verify divergence = 0

**Validation:**
- All existing integration tests pass
- `device-token-consistency` passes
- `documentprovider-write-consistency` passes

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