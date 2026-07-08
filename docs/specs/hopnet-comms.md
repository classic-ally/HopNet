# RFC-017: hopnet-comms + Seam Completion

**Status**: Implemented (stages 1a–7 complete, 2026-07-08)
**Depends on**: RFC-013 (Malachite consensus), RFC-014 (storage substrate),
RFC-015/016 (projection modularity + host API)

## Motivation

Two goals, one RFC. First: inter-node communication was host-owned glue —
a monolithic request/response envelope in `src/net/`, with every
subsystem's wire vocabulary tangled into one enum and the iroh dependency
(and its fork) spread through the host. Second: the post-RFC-016 seam
audit found encapsulation holes — the host's consensus path decoded drive
envelopes, the fragment protocol was split across host and storage, host
jobs conflated substrate decisions with plumbing, and host SQL read
drive tables. The NORTH STAR (recorded here deliberately): projection
crates interface through the hopnet-projection API and nothing else —
"store this blob", "CRUD this record" — with the replicated state
machine, storage substrate, and network interlaced beneath them.

## hopnet-comms

The base-level shim around iroh owning inter-node communication. Two
faces:

- **Default features: zero-dependency vocabulary.** `PeerRef{node_id,
  pubkey}`, the `CommsError/TransportError/ProtocolError` taxonomy,
  client seams (`Rpc`, `Broadcast`), the host-injection seam
  (`PeerDirectory`), and the scope-handler contracts (`RpcHandler`,
  `StreamHandler`, `FrameSink`). Safe for any crate; hopnet-storage deps
  exactly this face.
- **`iroh` feature: the transport.** `IrohComms` — connection lifecycle
  and cache, framing, receiver-side dedup, `rpc_req` tracing spans, the
  dedicated 3-worker net runtime (never-block discipline), and the
  fork's `before_registration` hook. ONLY the host links it. Fork
  containment, stated honestly: the `[patch.crates-io]` entry must stay
  in the workspace-root manifest (a cargo rule), but the only
  `[dependencies]` naming iroh in the workspace is hopnet-comms'.

### Envelope vs payload

Comms decodes the ENVELOPE only — request id, scope, framing — and
dispatches `(peer, bytes)` to the single handler registered for that
scope. Payload encode/decode belongs to the scope's owning module.
Wire format:

```
request stream :  [8B request_id LE][1B scope_len][scope utf8][4B payload_len LE][payload]
response stream:  repeated frames of [4B len LE][bytes]   (rpc = exactly one frame)
```

### Scope registry

One handler per scope namespace ("consensus", "txforward", "storage",
"metrics", "setup"); message names live inside each module's own enum,
so cross-module name collisions are impossible by construction. A
duplicate scope registration — or claiming the comms-reserved "ping"
scope — panics at registration (the dispatch-table tripwire
philosophy). Registration is runtime (handlers hold live state); the
host builds the map at startup via `net::scopes::build_registry`, shared
with the integration tests. Projections get NO network scopes — the
vision says they never touch the network.

| Scope | Kind | Payload enum (owner) |
|---|---|---|
| ping | comms-internal | nonce echo (liveness; `IrohComms::ping → rtt`) |
| consensus | rpc | ConsensusNetRequest/Response (consensus/malachite/gossip.rs) — gossip intake + decided-value sync |
| txforward | streamed | TransactionForwardRequest + ForwardReply frames (consensus/rpc.rs) — two-phase ACK protocol |
| storage | rpc | FragmentRequest/Response (hopnet_storage::rpc) |
| metrics | rpc | MetricsRequest/Response (metrics/rpc.rs) |
| setup | rpc | SetupRequest/Response (setup.rs) — JoinDeliver |

### Semantics (ported verbatim from the old transport)

`rpc`: random u64 request id, one retry on retryable errors with
connection eviction, SAME id reused so the receiver dedups (response-
byte cache, rpc scopes only, 300s TTL). `open_call`: multi-frame, no
auto-retry, no dedup — streamed protocols own idempotency (txforward:
the nonce table). `broadcast`: spawn-per-peer fire-and-forget, fresh id
per send, single ack frame, failures at debug. Timeouts cover stream IO;
the connect budget is separate (`CallOptions.connect_timeout` carries
the forward path's tight 2s). Constants unchanged: 8MB frame cap, 10s
connect, 500ms/30s fragment timeouts, 3s publish.

### Spawn policy — comms is dumb

Comms spawns per connection/stream on the net runtime, decodes the
envelope, resolves the peer through the directory, and invokes the
handler INLINE. Handlers own their runtime hops: gossip intake and
metric echoes run inline (pure channel/CPU); txforward + decided-fetch
hop to the consensus queue runtime; storage/setup hop to the main
runtime. The load-bearing invariant survives structurally:
consensus-liveness traffic can never be starved by API load.

### Peer identity

`PeerDirectory` is host-injected: `is_known` (the before-registration
gate — unknown peers are rejected before path registration, no IP
disclosure) and `node_id` (inbound attribution). The host impl
(`net::directory::HostPeerDirectory`) owns the bincode-encoded pubkey
lookup against the nodes table and the setup-mode bypass — comms never
learns the DB format or what "setup" means.

## Seam completions (stages 2–7)

- **Fragment protocol → hopnet-storage** (`hopnet_storage::rpc`):
  FragmentRequest/Response, `serve()` dispatch, timeouts, and
  `RpcTransport<R: comms::Rpc>` implementing the engine's `Transport`
  seam. Zero iroh in storage; zero fragment knowledge in the host.
- **Height read → hopnet-projection**: `current_height(conn)` wraps
  hopnet-consensus's canonical reader; the three SQL copies died.
  Conn-based (not a capability method) because projections read the
  height inside consensus-apply transactions.
- **`Projection::committed_blob_ids(function, payload)`**: the
  distribution kick decodes in the owning projection; `on_decided`
  loops manifests. Pure decode on the shell thread.
- **`Projection::user_data_size_bytes(caps, user_id)`**: takeout/import
  quota sizing sums across manifests; drive owns its sizing SQL.
- **Maintenance descent** (hopnet-storage): inventory differential
  (height passed in), rebalance candidates + `EngineHandle::repair_blobs`,
  orphaned-fragment scan/cleanup (`fragstore::scan_fragments` +
  `maintenance` module), `DeleteOrphanedDataBlocksPayload` wire type.
  Host drivers submit through the `TxSubmitter` seam.
- **`src/capabilities.rs`** (was drive_host.rs): `CapabilityHost`
  implements the generic projection capabilities;
  `build_capabilities()` assembles the bundle. Nothing in it is
  drive-specific.

## Stays host — justified

- `batch_query_fragment_inventory`: joins the host-owned `nodes` table;
  already behind `StateReader::fragment_sources`.
- `find_orphaned_data_blocks`: iterates the projection-layer
  `DataBlockReferenceProvider` registry, which storage (below the seam)
  can never see.
- Availability classification: reads the host-owned `metrics` table.
- The `WriteAdmission` host impl consults takeout's import flag: the
  host is the composition root and legitimately deps takeout.
- Takeout's service wiring (mounts, cron, resume hooks, barriers,
  `AppState.takeout_runtime`): a projection-agnostic service whose state
  is not expressible from generic capabilities; no Service trait at n=1.

## Deferred north star (explicit residual)

Drive still deps hopnet-storage directly: `api::put` in the upload
path, `BlobInsertOp` embedded in its consensus envelopes
(bincode-frozen wire shape), the apply-path calls, and the blob-key
crypto. These are substrate VOCABULARY — a library dependency, not a
host capability — and hoisting put behind a projection API without
redesigning the envelope wire shape only relabels the edge. A future
RFC should have hopnet-projection re-export a storage client vocabulary
so a projection's manifest lists only hopnet-projection. Until then the
acceptance lens stands: every new seam decision is judged by whether a
projection crate deps anything besides hopnet-projection.

## Stages (as-built)

| Stage | Commit | Scope |
|---|---|---|
| 1a | 71ec162 | hopnet-comms crate (vocabulary + IrohComms + 7 loopback tests) |
| 1b | 4e44d59 | Host cutover: envelope dissolved into scopes; net/ = directory + scopes + routes; iroh out of the host manifest |
| 2 | 3b8c02c | Fragment protocol → hopnet_storage::rpc over comms::Rpc |
| 3 | 6dbcd1e | Height consolidation via hopnet_projection::current_height |
| 4 | bc9fe99 | committed_blob_ids hook (last drive knowledge out of the consensus path) |
| 5 | dbc45d0 | Maintenance decision logic → hopnet-storage per disposition table |
| 6 | b2447ea | user_data_size_bytes hook (last host SQL into drive tables) |
| 7 | (with docs) | drive_host.rs -> capabilities.rs rename |

Every stage: workspace tests + all bins + snapshotter capture/compare
(IDENTICAL at every stage) + orchestrator subset + divergence check.
Stage 1b's wide gate: 11/11 including iroh-reject-unknown (hook
survival), consensus-queue-cross-node (two-phase forward), and both
barrier tests.

## Known flake (tracked)

`malachite_mesh_decides_and_laggard_syncs_over_loopback_iroh` (lib
test) fails intermittently (~25%) even in isolation — verified at the
same rate on pre-RFC-017-Stage-4 commits; a timing-sensitivity in the
laggard-sync scenario, not a comms regression. Mesh-level orchestrator
gates cover the same path deterministically.
