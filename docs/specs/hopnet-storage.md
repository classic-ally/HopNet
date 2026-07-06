# RFC-014: Distribution Substrate (hopnet-storage)

Status: DESIGN APPROVED (2026-07-06) — extraction IN PROGRESS on the
`consensus-malachite` branch, pre-merge (nothing deployed; fresh genesis makes
schema/format changes free). Living implementation plan:
`~/.claude/plans/storage-substrate-extraction.md`. Supersedes the 2026-07-06
draft of this RFC where they conflict (encryption ownership inverted,
placement seed changed, key custody added).

## Purpose

Provide **distributed storage on top of distributed state-machine
guarantees** — durable, location-transparent, content-verified, encrypted
blobs — without implying any particular rendering of that storage.
Filesystems (inodes/paths), photo libraries (EXIF/albums/derivatives),
document stores: all are *projections* that map their domain objects onto the
same substrate.

```
┌─────────────────────────────────────────────────────────┐
│ Projections: fs (RFC-002), photos, takeout/import, …    │
│   own their metadata tables + transaction handlers      │
│   reference blobs by id; think in WHO can access,       │
│   never in ciphers; never touch fragments               │
├─────────────────────────────────────────────────────────┤
│ Distribution substrate (this RFC)                       │
│   control plane: blob lifecycle via consensus txs       │
│   data plane: fragment bytes over iroh, hash-verified   │
│   key custody: per-blob keys, recipient wrapping        │
│   engine: global bounded distribution + repair workers  │
├─────────────────────────────────────────────────────────┤
│ hopnet-consensus (RFC-013)                              │
│   total order, atomic apply, durability, membership     │
└─────────────────────────────────────────────────────────┘
```

## The contract

### Model

- A **blob** is an immutable byte sequence identified by a stable random id
  (`BlobId`, UUIDv7 — today's `data_block_id`). The substrate encrypts it
  (per-fragment ChaCha20-Poly1305), Reed-Solomon-encodes the ciphertext into
  **fragments** identified by content hash (blake3 over ciphertext). Chunked
  RS for streaming (RFC-002's encoding survives unchanged — it moves here).
- **Placement** is deterministic: `f(blob_id, validator_set AND node metrics
  at placement_height, fragment_index)` — metrics-scored node selection plus
  a deterministic shuffle seeded by `blake3(blob_id)`, then modulo by
  fragment index. Any node can compute where every fragment belongs from
  replicated state alone, with **zero plaintext-derived input**. No placement
  gossip, no manifest servers.
- **Access** is a recipient set: the per-blob key is wrapped (ephemeral
  X25519 + ChaCha20-Poly1305) to each recipient's public key. Projections
  decide WHO; the substrate performs every cryptographic operation.

### Key custody (substrate-owned)

The substrate owns encryption end-to-end. Projections never see keys,
ciphers, or wrap formats — they express access as sets of X25519 pubkeys.

- **Per-blob key**: 32 bytes, minted at `put`. Per-fragment keys/nonces
  derive from it plus a crate-private `fragment_id`
  (`blake3::derive_key("hopnet chunk_key" / "hopnet chunk_nonce")` — the
  existing cipher format, moved down a layer, byte-for-byte compatible).
- **Wraps** (`blob_access` rows): fresh ephemeral X25519 per wrap;
  `wrap_key = blake3::derive_key("hopnet-storage key_wrap v1", dh_shared)`;
  `wrap_nonce = blake3::derive_key("hopnet-storage wrap_nonce v1",
  blob_id ‖ recipient_pubkey ‖ ephemeral_pubkey)[..12]`;
  `wrapped_key = ChaCha20Poly1305(wrap_key).encrypt(wrap_nonce,
  per_blob_key)` (48 bytes). Keyed by **pubkey, not user id** — the substrate
  is user-agnostic; projections map user→pubkey.
- **Capability seam**: readers prove access through a `RecipientKey` trait —
  `{ pubkey(), dh(ephemeral_pubkey) -> [u8;32] }` — a DH oracle. User X25519
  private keys never cross into the substrate; the substrate receives only
  per-wrap shared secrets (single-use: each wrap has a fresh ephemeral).
  Wrap format and `blob_access` layout are crate-private.
- **Mesh keypair** (substrate primitive for "all users" access): one
  mesh-wide X25519 pair. Generated at genesis; the pubkey and the
  privkey-wrapped-to-member-pubkeys both ride the genesis **transaction**
  (never bare meta writes — joiners reproduce state only through handler
  replay). New members get a wrap at `insert_user`: the creating user's
  session unwraps the mesh privkey via its own grant and wraps it to the new
  user, atomically with the user row (induction from genesis). All-users
  blobs carry ONE wrap targeting the mesh pubkey — no rewrap churn on member
  add. Rotation is deferred; until it exists, mesh-wrapped blobs are
  readable by all *past* members (`key_version` column reserves schema room).
- **Integrity hash**: `blake3::keyed_hash(blake3::derive_key(
  "hopnet-storage integrity v1", per_blob_key), plaintext)` — whole-blob,
  verified post-decrypt on full reads. Replaces
  `file_hash = blake3(plaintext ‖ data_block_id)`, which was a plaintext
  **confirmation oracle** (public salt: any holder of replicated state could
  confirm guessed plaintexts). Only key holders can verify — exactly who
  runs the check. Fragment hashes remain public ciphertext hashes (per-hop
  verification unchanged).
- **Dedup: deferred entirely** (no tag column). Note for the future: any
  cross-principal dedup identifier is definitionally a plaintext-equality
  oracle to that principal set; if added, scope it (per-user tag safest) and
  implement as control-plane attach, not storage-plane.
- **rekey (reserved revocation primitive)**: rekey mints a **new blob id** —
  decrypt via an authorized capability, `put` under a fresh key to the
  reduced recipient set, projection swaps its reference, old blob released.
  Consistent with content-update-is-a-new-blob; placement reshuffles with
  the new id, so a revoked party's knowledge of fragment locations goes
  stale. (Simple wrap-row removal — `remove_recipients` — is policy-only:
  removed recipients may have cached the key.)

### Control plane (consensus transactions owned by this layer)

| Tx | Contents | Notes |
|---|---|---|
| `blob_insert` | blob record (fragments, RS params, sizes, integrity hash) + `Vec<BlobAccess>` wraps | rides the SAME wire tx as the projection's inode op (one tx, one handler, two halves → atomic; no zero-ref window) |
| `placement_commit` | `Vec<(blob_id, placement_height)>` | **batched**: one tx per settling window, never per blob |
| `blob_access_add` / `remove` | wrap rows | sharing = add_recipients; rides the projection's share tx |
| `delete_orphaned` | blob ids | mark-and-sweep via the `DataBlockReferenceProvider` seam (v1 ownership model; attach/release refcounts later) |
| `self_check_fragments` | fragment attestations | inventory/attestation family (existing; substrate-owned) |
| mesh key grant | wrapped mesh privkey for a new member | rides `insert_user` |

Substrate state tables are replicated through the ordinary
`Application::apply_block` path — the substrate is a consumer of the
consensus layer's one-transaction atomicity, never a peer of it. The
substrate exposes **sync apply functions** (`apply_blob_insert`,
`apply_placement_commit`, `apply_delete_orphaned`, `apply_self_check`,
`apply_blob_access_*`) taking `&rusqlite::Transaction`; the main crate keeps
thin inventory-registered shim handlers (envelope decode, authorization,
projection half) — the same crate/host relationship as hopnet-consensus.

`fragment_hashes.stored_locally` is a **node-local column inside a
replicated table** (excluded from divergence hashing): each node records its
own disk state during apply and via the write-gate drain. Both writers are
crate-owned functions; the invariant is documented at the write sites. It
stays a column (a separate node-local table would force a JOIN on the
hottest read path).

### Data plane

Fragment transfer over iroh (`FragmentStore`/`FragmentFetch`/health checks),
hash-verified on receipt, idempotent (a re-sent fragment is a no-op). Data
never rides through consensus; only *facts about* data do. The transport is
a crate trait (`Transport`, crate-owned `PeerRef` — no iroh types); the main
crate implements it over `IrohTransport`. Server-side handlers delegate to
crate `serve_*` functions.

### Distribution engine (the active component)

- **Event-driven**: `HopNetApplication::on_decided` scans decided blocks for
  blob ops and pushes blob ids to the engine's work queue
  (`notify_blob_committed`, non-blocking try_send — post-commit hooks must
  never add shell-adjacent latency). No polling, no per-file spawns.
- **Global bounded workers**: one process-wide worker pool sized to mesh
  bandwidth, draining a single work queue of (blob, fragment) items across
  ALL in-flight blobs. Concurrency scales with the mesh, not upload count.
- **Batched placement commits**: completed placements accumulate and flush
  as one `placement_commit` per window (750ms / 100-entry cap, dedup
  keep-latest, ≤3 transient retries), so N uploaded files cost ~1 follow-up
  consensus tx, not N.
- **Repair**: placement seeded by blob_id makes the (previously disabled)
  rebalancer computable from `fragment_hashes` alone — tier-1 repair =
  recompute placement at current height, fetch-and-forward misplaced
  fragments, batch a placement re-commit. (Correction to the earlier draft:
  no repair existed in the main crate; this is new capability, not a move.)
- Pool-connection discipline: checkouts are brief, one scoped snapshot per
  placement computation, never held across network sends.
- The engine does not own a runtime; the host passes handles (queue_rt for
  control-plane tasks, main runtime for data-plane workers).

### Host seams (crate traits)

`Transport` (data plane, PeerRef-keyed) · `StateReader` (one-snapshot
`placement_inputs[_at(height)]`: height + validators + metrics) ·
`TxSubmitter` (control-plane submission; signing keys stay host-side;
Rejected vs Transient distinguished) · `LocalStateSink` (stored_locally
marks via the write-gate drain). Pure modules (crypto, RS, placement, batch
policy, store applies) are sync and testable without tokio; the engine is a
feature-gated tokio component whose *decisions* delegate to the pure
modules. (The consensus crate's sans-io HostCore pattern deliberately does
NOT transplant — distribution is I/O-shaped; there is no WAL/replay: engine
recovery is idempotent re-work.)

### Guarantees exported upward

- **Durable(h)**: once `placement_commit` for a blob is decided at height h,
  the blob is reconstructible from any k of n fragments whose placement is
  derivable from state at h.
- **Location transparency**: `get(blob_id)` works from any node — local
  fragments, else placement-directed fetch, else inventory-hinted, else
  brute-force mesh fetch.
- **Content integrity**: every fragment verified against its ciphertext
  hash at every hop; whole-blob keyed integrity hash verified post-decrypt
  on full reads; corruption surfaces as absence (repair), never as data.
- **Confidentiality**: replicated state + fragments contain no
  deterministic function of plaintext computable without a per-blob key —
  a party holding the full replicated DB and every fragment cannot confirm
  a guessed plaintext, link equal-content blobs, or decrypt without a wrap
  it can open.

### API to projections (target shape)

```rust
pub trait RecipientKey: Send + Sync {
    fn pubkey(&self) -> XPubKey;
    fn dh(&self, ephemeral_pubkey: &XPubKey) -> [u8; 32]; // privkey never crosses
}

impl BlobStore {
    /// Mint key, encrypt+RS locally, wrap to recipients. Prepare-shaped:
    /// returns the payload the caller batches into ITS consensus tx
    /// (blob + inode + access in one tx). Distribution kicks post-decide.
    async fn put(&self, plaintext: impl AsyncRead, len: u64,
                 recipients: &[XPubKey], policy: RsPolicy)
        -> Result<PutReceipt, BlobError>; // { blob_id, integrity_hash, record, access }

    /// Streaming, range-aware. Unwrap via capability, decrypt, verify.
    async fn get(&self, id: BlobId, reader: &dyn RecipientKey,
                 range: Option<(u64, u64)>) -> Result<BlobStream, BlobError>;

    /// Sharing: unwrap via `via`, wrap to new recipients; entries ride the
    /// caller's share tx.
    async fn add_recipients(&self, id: BlobId, via: &dyn RecipientKey,
                            new: &[XPubKey]) -> Result<Vec<BlobAccess>, BlobError>;

    /// Policy-only removal (no key rotation — see rekey).
    fn remove_recipients_payload(&self, id: BlobId, recipients: &[XPubKey])
        -> RemoveRecipientsPayload;

    /// RESERVED: revocation. New blob id; projection swaps reference.
    async fn rekey(&self, id: BlobId, via: &dyn RecipientKey,
                   recipients: &[XPubKey]) -> Result<PutReceipt, BlobError>;
}
```

Empty content is a projection concern (`data_id = NULL`); the substrate
rejects zero-length puts.

### What the substrate deliberately does not know

- Paths, filenames, directory semantics (fs projection — including path SIV
  encryption, which is user-session-keyed and stays projection-side).
- Users, sessions, sharing *policy*, access-control decisions. The
  substrate enforces access cryptographically; projections decide who is in
  the recipient set and when it changes.
- Photo metadata, MIME types, thumbnails/derivatives.

### Trust model and accepted leaks

HopNet is **server-side encrypted, not client-E2E**: the origin node sees
plaintext during put/get, and nodes serving a user's session can derive that
user's X25519 privkey. The crypto layer's adversary is other mesh nodes
acting as fragment hosts and offline possession of replicated state +
fragments — both see only ciphertext, ciphertext hashes, keyed integrity
hashes, and wrapped keys. It is not a defense against a compromised
origin/session node.

Accepted leaks: exact file sizes (and fragment/chunk counts); recipient
lists (`blob_access` = the sharing graph, public replicated state); UUIDv7
ids leak creation timestamps; SIV path encryption is deterministic per user
(equal names → equal ciphertext segments; tree shape visible); access
patterns (who fetches which fragments when); mesh-wrapped blobs readable by
all past members until rotation exists.

## Migration state / stages

Extraction runs on the `consensus-malachite` branch, pre-merge. Stage-7
consensus gates (bench trio, full app-suite, soak) run ONCE, after Stage F —
user decision 2026-07-06. Detailed staged plan with cut lines, tests, and
risks: `~/.claude/plans/storage-substrate-extraction.md`.

- Stage A — crate scaffold + pure code moves (placement, chunk crypto,
  fragment I/O, wrap primitives). [ ]
- Stage B — key custody: blob_access schema, mesh keypair, keyed integrity
  hash, blob_id placement seed. The one-time format change. [ ]
- Stage C — control-plane split: substrate apply functions, envelope
  reshape (blob ops out of `DataRecord`/`Vec<Inode>`), share-propagation
  retarget. [ ]
- Stage D — stored_locally settlement (crate-owned write paths, invariant
  docs). [ ]
- Stage E — engine + fragment RPC behind seams; on_decided kick; global
  work queue. [ ]
- Stage F — get() untangling out of the fs projection; rebalancer
  re-enabled as tier-1 repair. [ ]
- Stage G — photos ingress becomes projection #2 (post-merge; gets
  encryption for free — it currently stores plaintext; dep-pin unification
  with the sqlx workspace must be verified first). [ ]
