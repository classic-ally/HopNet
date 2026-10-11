# RFC-027: Sessions on Untrusted Nodes — Client-Held Keys, Signed Ingest, Module-Owned Reads

**Status**: Draft (2026-10-11)
**Supersedes**: RFC-012 (device token sessions)
**Depends on**: RFC-014 (storage substrate), RFC-016 (projection host API),
RFC-017 (hopnet-comms), RFC-025 (ALPN classes)
**Amends**: RFC-006 (Node Blindness), RFC-011 (thin-client dispatch),
RFC-014 (trust model), RFC-016 (read scopes, signable tx types),
RFC-023 (client compat), RFC-025 (client ALPN class)
**Tracking**: #12 (#116–#120)

## Motivation

- **As built, HopNet is server-side encrypted, not E2E**
  - stated plainly in `docs/specs/hopnet-storage.md:249-255`
  - fragment hosts see ciphertext; the node serving a session sees everything
  - RFC-006's "Node Blindness" describes a goal, not the implementation
- **The session node holds the user's key, three ways**
  - passphrase sign-in: Argon2id unwrap on the node, kept in a
    `SessionEntry` for 1–24 h (`src/auth.rs:147-212`)
  - device tokens: the device secret, sent on every request, unwraps
    the key (`docs/specs/device-token-sessions.md:56-82`)
  - macOS GUI: raw owner key in the Keychain (`src/auth.rs:216-225`)
- **The node also signs on the user's behalf**
  - `submit_transaction` signs with the session key (`src/photos/routes.rs:69`)
  - client-side encryption alone fixes nothing: whoever signs holds the key
- **Native clients send plaintext, and sometimes keys**
  - ingress: plaintext + raw blob key in `x-hopnet-blob-key`
    (`crates/ingress-publisher/src/dispatch.rs:320`)
  - mount: plaintext multipart (`hopnet-mount/src/http_transport.rs:401-435`)
  - Hop Drive: always remote, every read and write in plaintext
- **Remote clients are no longer hypothetical**
  - multi-user mesh: sign-in on any node, device tokens valid mesh-wide
  - Hop Drive over pinned HTTPS; mount's remote door (RFC-018); ingress `--url`
  - an iroh client transport (NAT traversal, relays) widens the set of
    nodes a client might talk to
- **Integration surfaces have grown per module**
  - `/integrations/{fileprovider,documentprovider,mount}`, `/photos/client/*`:
    each with its own auth gate and its own upload route
  - authorization lives in HTTP handlers, not consensus
    (`DEVICE_TX_FUNCTIONS`, responsibility gate, `src/photos/routes.rs:1160-1192`)
- **Half the pieces already exist**
  - encrypt-then-RS: repair is already keyless (`hopnet-storage/spec/durability-policy.md:204-209`)
  - ingress already mints and wraps blob keys and encrypts metadata
    client-side (`hopnet-photos-core/src/publisher.rs:330`, `crates/ingress-publisher/src/flow.rs:117`)
  - `PhotoDispatch`: opaque tx payloads, local + HTTP impls (`hopnet-photos-core/src/dispatch.rs:18`)
  - `device_tokens` is already replicated consensus state

## Threat Model

- **Adversary: the node a client talks to** (session or ingest node)
  - a mesh member run by someone else, or a compromised one
  - full control of its process, its disk, and every response it sends
- **It must not be able to**
  - read the content, paths, or metadata of client-encrypted data
  - obtain anything that unwraps or stands in for a user key
  - forge, alter, or replay a client-signed transaction
  - redirect a blob key to a recipient the user didn't choose
- **It still can, and this RFC accepts that**
  - drop, delay, or reorder client writes (availability, censorship)
  - omit or roll back read results: AEAD stops forgery, not omission
  - claim receipt of a blob, then lose it: bounded by ack levels and
    attestation (§Blob Ingest)
  - observe traffic metadata: sizes, timing, which blobs a device touches
- **Unchanged from RFC-014**
  - fragment hosts and offline holders of state + fragments see ciphertext only
  - accepted leaks stay accepted: exact sizes, chunk counts, recipient
    lists (`blob_access` = the sharing graph), UUIDv7 timestamps
- **Trusted**
  - the client device and its platform keystore
  - the user's key, and keys pinned at pairing
  - consensus as a whole: one malicious node cannot make honest
    validators accept an invalid transaction

## Non-Goals

- **The web UI**
  - its JS/WASM is served by the very node it would need to distrust
  - stays on server-side mode; any future in-browser crypto is
    honest-but-curious protection only
- **Co-resident clients** (Tauri GUI, FileProvider over loopback)
  - their node is the user's own machine
  - adopt the device model (§Device Identity) for uniformity, not confidentiality
- **Availability and censorship resistance**
  - a client can retry through another node (§Transport); not a guarantee
- **Hiding traffic metadata**
- **Encoding evolution** (new RS parameters, fountain codes)
  - not designed here; constrained only by invariant I3
- **Migrating existing server-side data**
  - stays readable as is; moving it to client mode is a client action,
    never a node one
- **Verifiable recipient keys**
  - shared operations accept recipient-list poisoning (I5)
  - closed by a follow-up RFC
- **Users who also use server-side mode on a node they do not own**
  - a passphrase sign-in there hands over the key, as today
  - client mode protects a path, not a user
- **Read-scoped keys**
  - a layered key hierarchy per module or library; device scope
    (§Device Identity) limits writes only

## Invariants

Every mechanism below preserves these; each section names the ones it
leans on. They apply to client mode; server-side mode (the web UI) is
exempt by construction.

- **I1: No key material reaches a node**
  - nothing a client sends unwraps, derives, or stands in for a user
    key: no passphrase, no device secret, no raw blob key
  - blob keys travel only wrapped to recipients' X25519 pubkeys
- **I2: The client computes everything it signs**
  - never signs bytes a node assembled, or values it cannot recompute
  - the one node-supplied input it acts on is recipient pubkeys, for
    shared operations only (I5)
- **I3: Redundancy is derived and substrate-owned**
  - the user signs content identity: the originals' hashes, the keyed
    integrity hash
  - redundancy (today's RS parity, any future code) is a function of
    the signed originals: verifiable by recomputation, generated and
    regenerated by nodes with no key and no user signature
  - requires systematic codes: originals are stored verbatim
- **I4: Nodes relay, consensus authorizes**
  - every authorization decision (delegation, scope, responsibility)
    is checked at apply, by every validator
  - the transport layer may refuse early, as a cheap filter; it never grants
- **I5: Self-only operations need no key-bearing read**
  - the client derives its own X25519 pubkey from the key it holds;
    personal publishes and unshared files never ask a node for a pubkey
  - operations that wrap to others (shared-library publishes, editing
    a shared file, creating a share) read recipient pubkeys from the
    node: **a node can poison that list, and this RFC accepts it**
  - bounded: one blob at a time, only for operations through that
    node, and visible after the fact in replicated `blob_access`
  - closed by a follow-up RFC: recipient keys bound by signatures
    rooted in each user's identity key
- **I6: Durable before acknowledged**
  - a node acknowledges a blob only once every fragment, and the
    directory entry naming it, is fsynced
  - a client keeps its source until the blob is confirmed (placement
    confirmed), not merely committed

## Device Identity and Delegation

Leans on I1, I4.

- **A device record is a delegation, nothing more**
  - RFC-012 made the token a second passphrase: a bearer secret that
    unwraps `wrapped_user_key`, because the node had to derive the user
    key. Client mode removes that need, and with it both
  - the record: device id, user id, device pubkey, encrypted name, modules
  - created by `register_device` (`src/devices/handlers.rs:15`), a
    transaction signed by the user's identity key: if the row exists,
    the user agreed to it
  - the passphrase-wrapped `encrypted_privkey` becomes the only wrapped
    copy of the user key in replicated state
- **One key per device**
  - the device pubkey is its iroh `NodeId`: authenticates connections
    (stable and visible to relays, as two-way dialing requires) and
    signs `SignedTx` envelopes and blob-ingest requests
- **What a device holds, and what it doesn't**

  | Key | On the device | Why |
  |---|---|---|
  | device key | yes | its own identity: transport, signatures |
  | user X25519 private key | yes | decrypt anything wrapped to the user |
  | user SIV path key | yes | encrypt and decrypt paths |
  | user Ed25519 identity key | **no**: discarded after pairing | |

  - X25519 and SIV derive one-way from the identity key
    (`src/auth.rs:342-375`): holding them reveals nothing about it
  - so a device reads everything the user can read, but signs only as
    itself
- **Why the device signs with its own key**
  - if it held the identity key, a stolen device would be the root:
    it could register a new device, and revocation would mean nothing
  - with its own key, revocation is real for writes: the root never
    leaves pairing
- **Scope: modules, enforced at apply** (I4)
  - `modules` lists the modules whose transactions this device may
    sign, e.g. `{photos}` for the ingress daemon
  - validators check the transaction's module is in the set; the
    module is already known at dispatch (RFC-016)
  - `identity` can never be in the set: device registration and
    revocation, and changes to the user record, need the identity key
  - purpose: blast radius, not confidentiality. A buggy or compromised
    ingress daemon cannot touch drive; it can still read it (one
    X25519 key decrypts everything; read scoping is a non-goal)
  - finer restrictions inside a module stay module state (e.g.
    `photo_ingress_responsibility`)
- **Accepting a device-signed transaction**
  - resolve the device pubkey in replicated state at apply: present,
    not revoked, module in scope, nonce unseen
  - a revocation and a transaction in the same block: block order decides
  - the device table is in the identity snapshot section, so regenesis
    carries it
- **Pairing**
  - path A, passphrase on the device: fetch `encrypted_privkey`, unwrap
    locally (Argon2id), derive X25519 and SIV, sign `register_device`,
    discard the identity key
    - a node cannot fake `encrypted_privkey`: without the passphrase
      it cannot produce a wrap that authenticates, so a successful
      unwrap also authenticates the identity
  - path B, approval from your own node: a server-side session there
    signs `register_device` and hands over the derived keys on the
    pinned channel
  - either way the client pins the user's identity pubkey
- **Key storage per platform**
  - Apple: Keychain; the Secure Enclave has no Ed25519, so keys are
    encrypted at rest under an enclave-held key
  - Android: encrypted under a Keystore-held key
  - Linux: Secret Service, falling back to a `0600` file
- **Revocation**
  - `revoke_device`, signed by the identity key (passphrase, or your own node)
  - from the revoking height: signatures refused, connections refused
  - **does not revoke read access already granted**: the device kept
    the X25519 key. Real revocation means rotating it, out of scope (related: #36)
- **Coexistence with RFC-012**
  - legacy tokens keep working, server-side mode, during migration
  - a record is either legacy or delegated, never both
  - migrating means re-pairing, then revoking the legacy token

## Blob Ingest

Storage-owned, module-agnostic. Leans on I1, I3, I6.

- **The client produces the stored format itself**
  - the frozen layout: 40 MB chunks of 10 × 4 MiB original fragments,
    padded (`hopnet-storage/src/rs.rs`)
  - per-fragment AEAD, key and nonce derived from the blob key + a
    client-minted `fragment_id` (`hopnet-storage/src/crypto.rs:22,36`)
  - the keyed integrity hash over the plaintext (`crypto.rs:371-387`);
    in client mode only the client checks it, on read
  - client-minted ids: a collision or replay only produces orphans, as
    for blob ids today (`src/photos/routes.rs:1041-1044`)
- **The client computes the whole manifest**
  - original hashes, plus parity hashes from the same RS code, run
    locally (CPU only)
  - so the manifest format is unchanged, and the client signs nothing
    it hasn't computed (I2)
  - I3 means later encodings of the blob never need this signature again
- **It uploads only the originals**
  - 1× uplink instead of 3×
  - the node regenerates parity through the re-encode path, which
    already verifies against manifest hashes before writing
    (`hopnet-storage/src/engine/reencode.rs:1-8`)
  - any mismatch rejects the upload; nothing is stored or acknowledged
  - the request is signed by the device key; the manifest itself is
    signed later, in the referencing transaction (§Transaction Ingest)
- **RS determinism is a frozen format, like the cipher**
  - originals-only upload holds only if every node and client computes
    byte-identical parity
  - already relied on by keyless repair across x86 and aarch64 nodes,
    but nothing pins it today
  - prerequisite: golden vectors in CI on both architectures, each SIMD
    backend forced in turn, and `reed-solomon-simd = "=3.0.1"`
- **Durability before acknowledgement** (I6)
  - prerequisite: `fragstore::store_fragment` fsyncs the fragment and
    its directory before returning (`hopnet-storage/src/fragstore.rs:79-107`
    has no fsync today)
  - three ack levels, readable per blob:
    - `received`: fsynced and held by the upload ledger
    - `committed`: a referencing transaction is decided
    - `confirmed`: `confirm_placement` landed
  - the client keeps its source until `confirmed`
- **Retention: unchanged, and already key-free**
  - the upload ledger records the batch before files are written and
    holds it 14 days (`LOCAL_UPLOAD_RETENTION_SECS`,
    `hopnet-projection/src/host.rs:189-230`)
  - never-confirmed blobs protect every copy on every node
    (`protection.rs`); the origin releases surplus only after
    `confirm_placement` and another member's attestation
    (`eviction.rs:48-55`)
- **Space**
  - the origin still stores all 30 shards (3×) until handoff; admission
    reserves that up front (`admission.rs:61-103`)
  - per-device budget for unreferenced bytes: node-local admission
    policy, not consensus. Ledger rows gain `device_id`; past the
    budget, `InsufficientSpace`
  - admission probe: "can you take N bytes?", exposing `check_ingest`
    (`admission.rs:109`), non-binding, so a client can pick another
    node first
  - capacity reporting is unchanged: statvfs already counts held bytes

## Transaction Ingest

Host-owned, module-agnostic. Leans on I2, I4.

- **What exists** (`src/consensus/types.rs:50-56`)
  - `Transaction { rpc: {function, payload}, submitter, user, nonce }`
  - `submitter`: the relaying node's own signature; `user`: the user's
    signature, made today by the node holding the session key
  - nonce dedup and a 50-minute staleness check at validation
    (`src/consensus/dispatch.rs:82,95-135`); `txforward` relays between nodes
- **Gap: the nonce is not signed**
  - both signatures cover `rpc` only (`RpcCall::sign`)
  - a relay can re-wrap a client-signed `rpc` with a fresh nonce and
    replay it (a delete, an edit), as often as it likes
  - harmless while the node holds the user key anyway; in client mode
    it is the replay hole
- **Every signature covers `(function, payload, nonce, mesh id)`**
  - `submitter` and `authority` alike, server-side mode included: the
    hole closes everywhere, not only for devices
  - client-minted UUIDv7 nonce: dedup and expiry (`MAX_TRANSACTION_AGE`)
    for free, once signed
  - mesh id (RFC-025's mesh magic): no cross-mesh replay
  - not the epoch: a nonce already expires in 50 minutes, and binding the
    epoch would void legitimate transactions in flight at a boundary
  - a block-format change: frozen predecessor spec, can land ahead of
    the rest of this RFC
- **`user` becomes `authority`**
  - `submitter` is who relayed; `authority` is on whose behalf it acts
  - one slot, two kinds:
    - `Authority::Identity { user_id, signature }`: the identity key
    - `Authority::Device { device_id, signature }`: resolved to its
      user at apply
  - `None` stays a node-only transaction
  - identity-module transactions require `Authority::Identity`
  - the relaying node's `submitter` signature attests who relayed; it grants nothing
- **Validation at apply** (I4)
  - for `Authority::Device`: device present, not revoked, module in
    scope (§Device Identity); nonce unseen for every transaction
  - checks that live in HTTP handlers today move into module handlers:
    `DEVICE_TX_FUNCTIONS` is subsumed by module scope; the ingress
    responsibility gate (`src/photos/routes.rs:1160-1192`) moves into
    the photos handler
- **Client assembly needs preconditions**
  - the client builds against state that may be stale; today the node
    reads fresh state in-process
  - payloads that depend on current state carry an expected version
    (an inode's version, its parent); handlers reject deterministically
- **Rejections are structured**
  - submit returns `decided` or `rejected { code }`, not HTTP status strings
  - codes are part of the client contract
- **The envelope is a client contract**
  - clients lag releases; node-to-node `txforward` stays locked
  - the envelope and each module's signable payloads get a compat
    generation window (RFC-025 pattern)
- **Submission**
  - `Vec<SignedTx>` to any node, which enqueues or forwards it like `txforward`
  - not atomic: each transaction is decided independently

## Reads

Module-owned; fragments are storage-owned. Leans on I5.

- **Each module owns its read vocabulary**
  - declared through the projection API, next to its tx envelopes
    (extends RFC-016's `mounts()`)
  - with its own compat generation (§Transport): a module evolves its
    reads without touching the others
- **Shape: change feeds for freshness, enumeration for content**
  - every drive client already syncs a feed by height: FileProvider's
    sync anchor (`hopnet-drive/src/http/fileprovider.rs:187-207`),
    mount's and Hop Drive's SSE poke → `/changes` loop
    (`docs/specs/hopnet-mount.md:81-97`, `docs/specs/hop-drive-android.md:136-153`);
    photos' sync feed (`hopnet-photos-core/src/dispatch.rs:29`)
  - enumeration by parent works without plaintext: inode ids and
    structure are cleartext, names are SIV-encrypted
  - SIV is deterministic: a lookup by name encrypts the name
    client-side, and the node matches ciphertext
  - a replica is the client's choice (FileProvider's framework keeps
    one for us); none is required. Search beyond exact names would need
    one: a local index of decrypted names
  - reads return state as stored: encrypted names and metadata, wrapped
    keys. The client decrypts
- **Fragment reads: storage-owned, ciphertext only**
  - fetch by fragment hash; checked against the manifest's blake3,
    then per-fragment AEAD, then the keyed integrity hash once whole
  - ranged reads align to chunks: the client fetches the chunks it
    needs and slices locally (today the node decrypts ranges,
    `hopnet-storage/src/api.rs:454-543`)
- **What a read can be trusted for**
  - forgery: no. AEAD and hashes catch substituted content
  - completeness and freshness: no. A node can omit rows or serve old
    state; that's availability, accepted in §Threat Model
  - cheap regression check: the client remembers the highest height it
    has seen per feed (every drive client already stores the anchor)
    and refuses to go backwards on a given node
  - recipient pubkeys: the accepted gap of I5

## Transport

Leans on I4.

- **One client surface, two bindings**
  - the vocabulary (tx ingest, blob ingest, each module's reads) is
    defined once, behind a transport-agnostic trait; `PhotoDispatch`
    already has this shape, with local and HTTP impls
    (`hopnet-photos-core/src/dispatch.rs:18`)
  - HTTP: existing loopback and pinned HTTPS (RFC-022); the first binding
  - iroh: the target for remote clients; adds NAT traversal and relays,
    and replaces the SPKI pin with dialing a pinned `NodeId`
- **iroh: a third ALPN family, next to RFC-025's two**
  - `hopnet/<magic>/client/<owner>/<G>`: `<owner>` is `tx` (host),
    `blob` (storage), or a module (`drive`, `photos`); `<G>` is that
    owner's generation
  - `tx/<G>` covers the envelope only: each module's signable payloads
    are opaque to the host, so their generation is the module's
  - accept tiers as in RFC-025: served, retired (structured reject
    naming the floor: the client shows "update required", as RFC-023's
    426), unknown
  - the magic is learned at pairing; as in RFC-025, defence in depth,
    not authorization
- **Generations: RFC-025's contract, unchanged**
  - minted only when an owner's vocabulary changes, never per release:
    a release that touches nothing on the wire keeps every `G`
  - window `[G-1, G]`, as for nodes: a client breaks only after missing
    two vocabulary changes, and gets the retired tier's structured reject
  - the contract checks shape; a change of meaning on the same bytes
    mints too, by convention, since CI cannot see it
- **Authentication**
  - iroh: the connection is authenticated by the device's `NodeId`
    (= its registered pubkey); the node looks it up in the device
    table, refusing unknown or revoked ones through the fork's
    auth-before-register hook (`Cargo.toml:178`)
  - HTTP: no bearer secret. The device signs a node challenge and gets a
    short-lived, node-local session token, which unwraps nothing (I1)
  - either way a cheap filter: authority comes only from signatures
    checked at apply (I4)
- **Token rollover is invisible to callers**
  - an expired token gets a typed `token_expired` rejection, never a
    generic 401
  - the client re-runs the challenge and retries the same request
  - retrying is safe: a resubmitted committed transaction returns
    `Committed` (nonce dedup, §Transaction Ingest), and uploads are
    idempotent by blob id and fragment hash
  - expiry is checked when a request starts: a long upload is never cut
    off midway
- **Which node to talk to**
  - the client pins at least one node at pairing (`NodeId`, or SPKI over HTTP)
  - other nodes' addresses may come from reads: a poisoned list costs
    availability only
- **Unchanged**
  - node-to-node scopes (locked and compat) and the web UI, which stays
    HTTP and server-side mode

## Coexistence

Leans on I1.

- **One data format, two ways to reach it**
  - a client-written blob is byte-for-byte what a node writes: same
    cipher, same manifest, same wraps to the user's X25519 key (§Blob Ingest)
  - so client mode is a property of a session, not of data: there is
    no per-blob flag
  - a node holding a session for that user (web UI, server-side mode)
    reads and writes client-written data, and the reverse
- **Node-side features need a session, not a format**
  - photo grants and ranges: unwrap with the session's X25519
    (`src/photos/routes.rs:1590-1592`)
  - node-side photo sidecar hydration (`docs/specs/photos.md:28-36`)
  - takeout export: reconstructs plaintext with the per-blob key and
    hashes `blake3(plaintext ‖ blob_id)` (`hopnet-takeout/src/export.rs:466-470`)
  - resuming a stranded import at sign-in (`src/auth.rs:216`)
  - S3 standalone: its secret carries the decryption keys
    (`docs/specs/s3-compatibility.md:73-84`); server-side by design
  - all keep working, on all data, wherever the user has a session; a
    delegated device never creates one
- **What actually goes away: background sessions from device tokens**
  - RFC-012 bootstrapped sessions from device tokens, with no user
    present; delegated devices create none
  - work that needed such a session runs on the client instead, or
    waits for a user session
  - today's consumers (RFC-012 §Device classes): FileProvider,
    DocumentProvider, ingress, all clients of this RFC
  - what a node still does for a device in the background: push change
    notifications relevant to it (§Reads), nothing that needs a key
- **Both on one account**
  - legacy tokens and delegated devices coexist, per device (§Device Identity)
  - signing in on the web UI of a node you don't own still hands it
    your key (§Non-Goals)
- **Formats and releases**
  - the envelope change (signed nonce, `authority`): block format,
    frozen predecessor spec
  - the device record (identity section): `format_version` bump,
    frozen spec, `resolve_import_plan` arm (release rule 3)
  - `device_id` on upload ledger rows: node-local table, same rule
  - each one crosses through the release-crossing gate

## Phasing

Each phase ships through the release-crossing gate.

- **Phase 0: prerequisites, independent of the rest**
  - fsync in `fragstore::store_fragment`, fragment and directory (I6)
  - RS golden vectors in CI on x86 and aarch64, `reed-solomon-simd = "=3.0.1"`
  - every signature covers `(function, payload, nonce, mesh id)`;
    `user` becomes `authority` (block format, frozen spec)
  - each closes a gap in today's system on its own
- **Phase 1: device identity, both ingests, ingress**
  - delegated device records, `Authority::Device`, module scope at apply
  - blob ingest: client-produced format, originals-only upload, ack
    levels, per-device budget, admission probe
  - ingress adopts it over HTTP: write-only, already mints and wraps
    keys, already encrypts metadata
  - first real E2E: personal-library publishes
- **Phase 2: drive-core, on the node first**
  - extract drive's assembly into a pure core, the way `photos-core`
    was; the node runs it for server-side sessions
  - preconditions and structured rejections, proven while the node is
    still trusted
- **Phase 3: Hop Drive in client mode, over pinned HTTPS**
  - the largest exposure today: always remote, plaintext in both directions
  - needs the pure crypto + RS crate behind UniFFI
  - reads return encrypted names; decryption and path encryption move
    to the client
- **Phase 4: mount in client mode**
  - Rust, uses drive-core directly
  - matters once mount targets a remote node (RFC-018's open door)
- **Phase 5: the iroh binding**
  - the client ALPN family, `NodeId` authentication
  - Hop Drive first: reach without port forwarding
- **Phase 6: co-resident clients, then retirement**
  - FileProvider adopts the device model, for uniformity
  - legacy RFC-012 tokens retired once no supported client uses them

## Open Questions

- **Argon2id at 1 GiB on Android** (pairing path A): may get the app
  killed on modest phones; path B only on mobile, or measure and accept?
- **Changing a device's `modules`**: re-pairing, or an
  `update_device_scope` transaction signed by the identity key?
  Leaning re-pairing first: one less transaction to support
- **Per-device budget default** for unreferenced uploads (§Blob Ingest):
  a fixed size, or a share of free space above the upload floor?
- **Session token lifetime** on HTTP (§Transport): short enough to
  bound a leaked token, long enough that rollover stays rare
- **Pairing path B's channel**: how your own node hands the derived
  keys to the new device (QR plus pinned transport, or a short code)
- **Refusing passphrase sign-in on nodes a user doesn't own**: a per-user
  opt-in that would close the gap in §Non-Goals for users who want it

## Follow-ups

- **Verifiable recipient keys**: X25519 keys bound by identity-key
  signatures, signed membership; closes I5's accepted gap
- **Layered keys**: read scoping per module or library
- **User X25519 rotation**: real revocation of read access (related: #36)
- **Encoding evolution**: new RS parameters, fountain codes, within I3
