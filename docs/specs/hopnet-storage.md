# RFC-014: Distribution Substrate (hopnet-storage)

Status: DESIGN — contract defined now; the code currently lives in the main
crate (`src/files/distribution.rs`, `src/files/placement.rs`, fragment RPC)
and is being shaped in place. Extraction into a `hopnet-storage` crate is the
planned follow-up after the consensus migration (RFC-013) merges.

## Purpose

Provide **distributed storage on top of distributed state-machine
guarantees** — durable, location-transparent, content-verified blobs —
without implying any particular rendering of that storage. Filesystems
(inodes/paths), photo libraries (EXIF/albums/derivatives), document stores:
all are *projections* that map their domain objects onto the same substrate.

```
┌─────────────────────────────────────────────────────────┐
│ Projections: fs (RFC-002), photos, takeout/import, …    │
│   own their metadata tables + transaction handlers      │
│   reference blobs by id; never touch fragments          │
├─────────────────────────────────────────────────────────┤
│ Distribution substrate (this RFC)                       │
│   control plane: blob lifecycle via consensus txs       │
│   data plane: fragment bytes over iroh, hash-verified   │
│   engine: global bounded distribution + repair workers  │
├─────────────────────────────────────────────────────────┤
│ hopnet-consensus (RFC-013)                              │
│   total order, atomic apply, durability, membership     │
└─────────────────────────────────────────────────────────┘
```

## The contract

### Model

- A **blob** is an opaque, immutable, encrypted byte sequence identified by a
  stable id (`data_block_id`), Reed-Solomon-encoded into **fragments**
  identified by content hash (blake3). Chunked RS for streaming (RFC-002's
  encoding survives unchanged — it moves here).
- **Placement** is deterministic: `f(file_hash, validator_set AND node
  metrics at placement_height, fragment_index)` — metrics-scored node
  selection plus a deterministic shuffle, then modulo by fragment index. Any
  node can compute where every fragment belongs from replicated state alone
  (metrics are replicated). No placement gossip, no manifest servers.

### Control plane (consensus transactions owned by this layer)

| Tx | Contents | Notes |
|---|---|---|
| `blob_insert` | blob id, fragment hashes, RS parameters, origin node | registers existence; fragments are `stored_locally` at origin |
| `placement_commit` | `Vec<(blob_id, placement_height)>` | **batched**: one tx per settling window, never per blob |
| `blob_delete` | blob id, ref owner | refcount decrement; physical cleanup at zero |
| repair/health txs | re-placement after node loss | tier-1 repair (existing) |

State tables (`data_records`, fragment inventory, placements) are replicated
through the ordinary `Application::apply_block` path — the substrate is a
consumer of the consensus layer's one-transaction atomicity, never a peer of
it.

### Data plane

Fragment transfer over iroh (`FragmentStore`/`FragmentFetch`/health checks),
hash-verified on receipt, idempotent (a re-sent fragment is a no-op). Data
never rides through consensus; only *facts about* data do.

### Distribution engine (the active component)

- **Event-driven**: work starts when the substrate observes its own
  `blob_insert` apply (post-decide notification) — never by polling
  replicated state.
- **Global bounded workers**: one process-wide worker pool sized to mesh
  bandwidth, draining a work queue of (blob, fragment) items across ALL
  in-flight blobs. Concurrency scales with the mesh, not with upload count.
- **Batched placement commits**: completed placements accumulate and flush
  as one `placement_commit` per window (bounded delay), so N uploaded files
  cost ~1 follow-up consensus tx, not N.
- Pool-connection discipline: checkouts are brief and never held across
  network sends (the conn-lifecycle rule; see the consensus-pipelining
  post-mortem).

### Guarantees exported upward

- **Durable(h)**: once `placement_commit` for a blob is decided at height h,
  the blob is reconstructible from any k of n fragments whose placement is
  derivable from state at h.
- **Location transparency**: `get(blob_id)` works from any node — local
  fragments, else placement-directed fetch, else brute-force mesh fetch
  (existing download path).
- **Content integrity**: every fragment verified against its hash at every
  hop; corruption surfaces as absence (repair handles it), never as data.

### API to projections (target shape at extraction)

```rust
trait BlobStore {
    /// Register + begin distributing. Returns after the blob_insert commits;
    /// distribution and placement_commit proceed asynchronously.
    async fn put(&self, bytes: impl Stream, policy: RsPolicy) -> Result<BlobId>;
    async fn get(&self, id: BlobId) -> Result<impl Stream>;
    /// Refcounted: the blob dies when its last reference does.
    async fn attach(&self, id: BlobId, owner: RefTag) -> Result<()>;
    async fn release(&self, id: BlobId, owner: RefTag) -> Result<()>;
    fn events(&self) -> broadcast::Receiver<BlobEvent>; // Committed, Durable(h), …
}
```

### What the substrate deliberately does not know

- Paths, filenames, directory semantics (fs projection).
- Users, sessions, sharing, access control *policy*.
- Photo metadata, MIME types, thumbnails/derivatives.
- Encryption keys: the substrate stores ciphertext and verifies hashes; key
  custody (per-blob keys, key wrapping, session SIV) belongs to projections.
  Two projections can encrypt differently without the substrate noticing.

**Known gap (validated 2026-07-06):** encryption-obliviousness is TRUE for
the data plane today (transfer/placement/repair touch only ciphertext +
hashes) but NOT yet for the put/get pipeline: encryption is per-fragment,
interleaved with RS encoding, and keyed by substrate-minted `fragment_id`s —
so `put(opaque bytes)` does not match the current crypto format. The
extraction must choose: change the crypto format (encrypt the stream once,
substrate chunks ciphertext) or expose a projection-supplied per-fragment
transform hook. Additionally `data_blocks.file_hash` is plaintext-derived
AND seeds placement; a clean substrate seeds placement from the blob id (or
a ciphertext hash). Both are design decisions, not mechanical moves.

## Migration state / cut lines

Already true today: fragment transfer, placement math, RS encoding, repair,
and health checks are consensus-agnostic. Being fixed on the
consensus-malachite branch (shaping-in-place):

- [x] identified: per-file placement txs → batched `placement_commit`
- [x] identified: poll-until-distributable → post-commit event kick
- [x] identified: 10-workers-per-file spawns → global bounded engine

Remaining for extraction (post-merge follow-up; cut lines VALIDATED against
code 2026-07-06 — corrections below supersede earlier claims):

1. Pure code + types to the crate first: placement.rs (pure), RS
   encode/decode, fragment file I/O, hash types (S).
2. Split `blob_insert` out of `insert_files` AND out of `ModifyItemHandler`
   (a second combined inode+blob handler this RFC originally missed). The
   table seam is clean (data_blocks + fragment_hashes vs inodes +
   file_access); the payload seam is not (`DataRecord` embeds
   file_access_entries; `fragment_id` is substrate-minted but feeds
   projection key derivation — it stays visible across the boundary). Fold
   an initial `attach` into blob_insert and batch it with the inode tx so a
   zero-ref window never exists (M).
3. Ownership: the RFC's original claim ("implicit via FileAccess") was
   WRONG — liveness is already decided by the `DataBlockReferenceProvider`
   mark-and-sweep seam (src/reference_providers.rs), which anticipates
   multiple projections by name. v1 extraction can keep the provider seam
   as the contract (S); attach/release refcounting replaces the provider
   subqueries + retention grace window + takeout's cleanup-blocking gate as
   a follow-up (M). Release call sites: delete_files, modify_item content
   replacement, share removal.
4. Carve `fragment_hashes.stored_locally` out of the replicated-table
   story: it is a per-node LOCAL column inside a consensus-replicated table
   (each node writes its own value during apply; the write-gate drain also
   writes it) — the invariant most likely to bite during extraction.
5. Fragment RPC + engine behind trait seams (FragmentStorage / Transport /
   StateReader (validators+metrics@height) / TxSubmitter / LocalStateSink,
   mirroring hopnet-consensus's seam style); requires net-protocol
   modularization of the monolithic IrohRequest enum (L).
6. Factor `get()` (fragment discovery + reconstruction) out of the fs
   projection's functions.rs — the largest untangling job — and settle the
   encryption-format / placement-seed design decisions there (L). Note:
   `get_file_fragments` today JOINs substrate + projection tables in one
   query; a substrate-owned query API replaces that. Note: network
   rebalancing is currently DISABLED (needs file_hash + local_index which
   the rebalancer lacks) — repair-completeness is part of this step, not
   already-true.
7. Photos ingress (`crates/ingress-*`) becomes the second projection — a
   separate post-substrate milestone, not an extraction step. Its blobs
   table already carries ref_count (validates the attach/release shape);
   the gaps are plaintext→ciphertext key custody and a photo-metadata tx
   family.
