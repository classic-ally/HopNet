# Apple Photos Ingress

## Summary

A native ingress daemon that reconciles an Apple Photos library — including iCloud Photos and iCloud Shared Photo Library content — into HopNet. The first cut targets macOS, but the design is structured around primitives that map cleanly to iOS (`PhotoKit`, `PHCloudIdentifier`, `PHAssetResource`) so a future delta-upload daemon on iOS can share the same state model and resume the same dedup contract.

State (asset identity, ingest pipeline status, library partitioning, publish ledger) lives in a local SQLite database on the ingesting device. Materialized photo bytes stage briefly in a **transient spool** under the daemon's data directory, are published into HopNet as encrypted data blocks through consensus, and are deleted from the spool once the publish is consensus-decided. **HopNet is the archive of record**: fragments, distribution, per-recipient encryption, and the gallery all live mesh-side (`photos.md`).

Historical note: this spec originally defined a content-addressed archive on user-owned storage (a NAS share) as the destination — a deliberate stopgap to get data out of iCloud before HopNet could hold it. That archive layer (per-library blob trees on a storage root, sidecar JSON files, remote replication, snapshot/recovery machinery, the NAS viewer) has been removed; the daemon is now purely the PhotoKit→HopNet on-ramp.

## Motivation

### Why ingress, why now

- **Threat model**: Apple Advanced Data Protection is under sustained legislative pressure in multiple jurisdictions. iCloud is not a reliable long-term home for an encrypted-only-to-Apple photo library, and any forced-disclosure regime that compels Apple to disable ADP would silently demote that library to provider-readable.
- **Capacity**: Mac internal storage often cannot hold the full iCloud Photos library. The spool must stay bounded — bytes reside locally only between fetch and decided publish.
- **Dedup correctness across runs and devices**: Re-running the daemon on the same device, restarting after a crash, or running a future iOS daemon against the same iCloud library must not produce duplicate uploads or duplicate logical photo records (consensus `cloud_fingerprint` + ingress responsibility enforce this mesh-side; the local identity model enforces it daemon-side).

### Why a separate daemon, not just a script

PhotoKit's `PHPhotoLibraryChangeObserver` and `PHAssetResourceManager` model assume a long-lived process that holds library access. Reconciliation is not a one-shot import — it is a continuous activity that watches for new captures, edits, deletes, and shared-library membership changes, and a daemon is the right shape for that. A periodic cron job would miss observer-driven change events between runs and would re-enumerate the full library each invocation, which is wasteful on a 50k+ asset library.

### Scope boundaries

This document defines:

- Local SQLite schema on the ingesting device
- The transient spool layout and its eviction contract
- Library partitioning and routing rules
- Asset discovery, resource enumeration, dedup, and write pipeline
- The HopNet publish queue and spool eviction
- Failure handling: iCloud download failures, partial writes, daemon crashes, unreachable node
- Recovery model: a lost `state.db` is rebuilt by re-scanning PhotoKit; mesh-held photos re-associate via adoption (consensus `cloud_fingerprint`), so nothing re-uploads

This document explicitly does not define:

- The mesh-side photos model — encryption, fragments, gallery, sharing (`photos.md`)
- iOS implementation details (mentioned only where they constrain Mac design choices)

## Architecture Overview

### Components

The daemon is a single long-lived process on macOS, structured as a Swift PhotoKit shim layered over a Rust core. The split is dictated by platform constraints: PhotoKit can only be driven from Swift (or Objective-C), but everything downstream of asset enumeration — hashing, dedup, SQLite, spool I/O, publishing — is platform-agnostic.

Roughly:

```
┌──────────────────────────── macOS device ────────────────────────────┐
│                                                                      │
│   ┌──────────────────────────────────────────────────────────────┐   │
│   │ ingress daemon (LaunchAgent, single process)                 │   │
│   │                                                              │   │
│   │   Swift layer                                                │   │
│   │     - PhotoKit asset enumeration + change observer           │   │
│   │     - PHAssetResourceManager streaming (incl. iCloud pull)   │   │
│   │     - Library-scope detection (personal vs shared)           │   │
│   │                                                              │   │
│   │   Rust core (ingress-core crate)                             │   │
│   │     - BLAKE3 hashing                                         │   │
│   │     - Dedup decision (cloud_id → content_hash → new)       │   │
│   │     - SQLite state store (incl. publish ledger + capsule)    │   │
│   │     - Atomic spool writer                                    │   │
│   │     - Pipeline scheduler + retry/backoff                     │   │
│   │     - HopNet publish queue + spool eviction                  │   │
│   └──────────────────────────────────────────────────────────────┘   │
│                                                                      │
│   ┌─────────────────────────────────┐                                │
│   │ ingress-cli (status / config)   │  reads state.db via Rust core  │
│   └─────────────────────────────────┘                                │
│                                                                      │
│   Local-only filesystem state:                                       │
│     ~/.local/share/hopnet-photo-ingress/                             │
│       state.db                  (authoritative ingest state)         │
│       spool/blobs/<aa>/<bb>/<full-blake3>.<ext>   (transient bytes)  │
│       spool/blobs/.partial/     (in-flight write temps)              │
│       drain.lock                (exclusive run lock)                 │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
                                  │
                                  │  HTTP (device-token auth)
                                  ▼
┌──────────────────────────── HopNet node ─────────────────────────────┐
│   /api/photos/client/* — resolve, data-block upload, transaction     │
│   relay, committed probe. photo_add commits through consensus;       │
│   fragments distribute mesh-wide; the gallery serves bytes back.     │
└──────────────────────────────────────────────────────────────────────┘
```

### Data flow at a glance

1. PhotoKit emits a change event (or initial enumeration produces a backlog of assets).
2. The Swift layer iterates `PHAsset` records, extracting per-asset identifiers and library scope, and hands each off to the Rust core as a structured `AssetDescriptor`.
3. The Rust core consults `state.db` for prior ingest state, deciding whether the asset is new, already complete, partially complete, or in need of metadata-only update.
4. For new or incomplete assets, the Rust core requests resource bytes from the Swift layer one resource at a time (`original`, `paired_video`, `raw_alternate`, etc.). The Swift layer drives `PHAssetResourceManager` with `isNetworkAccessAllowed=true` to pull originals from iCloud when needed.
5. Bytes stream through a BLAKE3 hasher and into a temp file in the spool, then are atomically renamed to their final content-addressed path. If the hash matches a live spool entry, the temp file is discarded and the existing bytes are reused.
6. After all resources for a photo are written, the Rust core commits the final `photos` + `photo_resources` rows and the publish-metadata capsule (`descriptor_json`) in `state.db`.
7. The publish tick claims completed photos, streams their spool bytes into the node as encrypted data blocks, and submits `photo_add` (decided by consensus before the submit returns).
8. Eviction deletes spool bytes whose every referencing photo is decided; the spool stays bounded to in-flight work.

### Process model

- Single LaunchAgent, label `com.hopnet.desktop.photo-ingress`, bundled inside HopNet.app (`Contents/MacOS/photo-ingress` + `Contents/Library/LaunchAgents/<label>.plist`) and lifecycle-managed via `SMAppService.agent(plistName:)` — registration/unregistration is driven by the app's owner-only `/api/photo-ingress/{enable,disable,status}` routes (the future settings pane's backing API). Login autostart and crash restart (`KeepAlive`) belong to launchd.
- Provisioning travels through the keychain service `com.hopnet.desktop.photo-ingress` and is the device token alone: the enable route mints it (`api_key` + `base_url`); the daemon needs no other configuration — the spool is data-dir-derived and the personal library self-creates at startup via the ensure-only `ensure_personal_library` FFI (a deliberate narrow reversal of Phase 6's "libconfig is CLI-only" — the GUI app cannot link ingress-core (the crates/ workspace split), so the daemon is the only process that can create it; bind/rename/set-retention stay CLI-only). Token minting is enablement-gated: login only self-heals an existing provision, and setup mints nothing.
- One in-process tokio runtime hosting: PhotoKit observer callback, resource fetch workers, hash workers, spool writers, and the SQLite executor.
- Bounded concurrency: a configurable number of parallel resource fetches and a separate configurable number of parallel spool writes. Defaults are conservative (e.g. 4 fetches, 4 writes).
- All long-running operations are interruptible via cooperative cancellation; the daemon is expected to be SIGTERM-clean on user logout or system shutdown.
- **Lazy coupling to the HopNet node**: the daemon's lifecycle is its own — a periodic publish tick pushes completed photos into a node over HTTP (see §HopNet publish queue), and when the node is unreachable the tick PARKS (no retry budget consumed, observation and ingest continue untouched) rather than tearing anything down. Reachability edges are logged once (`node_unreachable` / `node_regained`), not per tick. The GUI node binds an ephemeral loopback port per launch, so after an unreachable pass the daemon re-reads the keychain credentials and rebuilds its HTTP client if they changed (`RefreshingPublisher`) — an app relaunch heals on the next tick without a daemon restart, and the steady state does zero keychain traffic.

### Why this shape

- **Swift shim, Rust core**: minimum platform-specific surface area. The Rust core is the artifact that survives the move to iOS and the eventual fold-in to HopNet's photos crates.
- **SQLite local, spool local**: everything the daemon touches lives on the local disk; the mesh is reached only over HTTP. (The archive era's SMB-mount hazards — fsync semantics, mount loss — are gone with the mount.)
- **Library partitioning in the ledger, not on disk**: the spool is a single hash-addressed tree; personal-vs-shared partitioning lives in the `blobs` ledger keys and the photo rows, where the publish path (and the mesh's access model) actually consume it.
- **Spool, not archive**: local bytes exist only to decouple the expensive PhotoKit/iCloud fetch from the publish (exact-length upload, cheap retry, crash resume). Once consensus decides the publish, the local copy adds no fault tolerance — the node often shares the disk — and eviction removes it.
## Asset Identity Model

Identity is the load-bearing concept for ingress. Everything downstream — dedup, change observation, cross-device coordination, publish idempotency — assumes a clear answer to "is this the same photo as one I've seen before?" The model uses three layered identifiers, each with a distinct purpose.

### The three identifiers

| Identifier | Source | Scope | Stability | Role |
|---|---|---|---|---|
| `cloud_id` | `PHCloudIdentifier` from PhotoKit | iCloud account | Stable across devices and reinstalls for as long as the asset exists in iCloud Photos | Primary dedup key. The lookup answer to "have I seen this asset on any device tied to this iCloud account?" |
| `content_hash` | BLAKE3 of resource bytes | Universal | Stable forever as a property of the bytes | Secondary dedup key for local-only assets, re-imports, and assets that lost their `cloud_id` association. Also blob storage address. |
| `photo_id` | UUIDv7 minted by the daemon at first discovery | Daemon-local (until publish) | Stable for the daemon's logical record, independent of any external identifier | Internal primary key. The identifier the daemon and `state.db` agree on. **Becomes the consensus `photos.id`** when this device publishes the photo first; when the mesh already holds it, the mesh's id is adopted into `consensus_photo_id` instead (see §HopNet publish queue). |

#### Why all three

Each identifier covers a failure mode of the others:

- `cloud_id` alone is insufficient because assets may not have one. Local-only assets (camera roll on a device not signed into iCloud Photos, or assets created before iCloud upload completes) return `nil` from the cloud identifier mapping. Deleting an asset from iCloud and re-adding the same file yields a new `cloud_id` — even though the bytes are identical.
- `content_hash` alone is insufficient as a discovery key because it requires materializing the bytes first. PhotoKit can hand us a `PHAsset` reference with metadata in microseconds but pulling the original resource from iCloud may take seconds (and quota). A dedup decision based solely on hash forces every asset through a full download just to decide whether to skip it.
- A daemon-local `photo_id` alone has no cross-device meaning. Two daemons against the same iCloud library would each mint distinct `photo_id`s for the same photo and produce divergent records.

The combination resolves each failure: `cloud_id` makes pre-download dedup possible in the common case (asset is in iCloud and unchanged); `content_hash` catches the cases where `cloud_id` lies or is absent; `photo_id` gives the daemon a stable internal handle that doesn't change when an asset's `cloud_id` evolves (e.g. when a local-only asset is later uploaded to iCloud and gains one).

### Match precedence on discovery

When the Swift layer emits an `AssetDescriptor` for a `PHAsset`, the Rust core resolves identity in a strict order:

```
1. If asset has a cloud_id AND that cloud_id exists in photos(cloud_id):
   → match: reuse photo_id, update local_id if changed, fast-path skip
     (no byte download, no hash computation needed unless metadata changed)

2. Else, download just enough to obtain the original-resource bytes,
   hash them, and check photo_resources(content_hash) for an entry with
   resource_type=0 (original):
   2a. If content_hash matches AND existing photo has no cloud_id:
       → late-binding: this is a previously-local asset that just gained
         a cloud_id, OR a re-import on another device. Reuse photo_id,
         add cloud_id to the existing photos row, link the new local_id.
   2b. If content_hash matches AND existing photo has a different cloud_id:
       → distinct logical photo with byte-identical original.
         Treat as new (mint new photo_id), reuse the existing blob via
         refcount increment.
   2c. No match:
       → new photo. Mint photo_id (UUIDv7), proceed with full ingest.
```

The first branch is the fast path and accounts for the steady-state case. Steps 2a–2c collectively run only on first-discovery on a given device, or on re-import scenarios.

#### Why 2b creates a new photo_id rather than merging

If two distinct `cloud_id`s have byte-identical originals, they are two records in iCloud — typically because the user explicitly added the same image to the library twice (e.g. saving the same screenshot, importing from two albums, or the same content existing in both personal and shared scopes). PhotoKit treats them as distinct assets with distinct identifiers, and the daemon mirrors that decision. The blob is shared at storage-layer refcount, so this is cheap, but the logical records remain separate.

### `photo_id` lifecycle

`photo_id` is minted at the moment a `PHAsset` first enters the Rust core's view, before any bytes are downloaded. This guarantees:

- Every asset the daemon has seen has a queryable record in `photos`, regardless of ingest status (downloading, hashing, written, failed).
- Pipeline state is column-level updates on the `photos` row, not row-level insertions/deletions. Restarts and crashes leave a consistent picture: a row with `materialized_at = NULL` means "discovered, not yet materialized", not "deleted and re-discovered."
- The CLI status tool can report inflight ingest progress without inferring it from absence of state.

A `photo_id` is never reissued. Once minted for a given `(cloud_id, content_hash)` correspondence on this device, it persists through:

- Asset edits in Photos (new `photo_resources` row at `resource_type=edited`, same `photo_id`)
- Re-imports producing the same content_hash (linked into existing `photo_id` per rule 2a)
- Library scope changes — though see the note on Library Partitioning regarding shared-library boundary crossings, which are treated as distinct photos
- The daemon being uninstalled and reinstalled, provided `state.db` survives (a lost `state.db` means a re-scan minting fresh `photo_id`s, with mesh identities re-associated via adoption; see the Recovery section)

### Cross-device convergence

When a future iOS daemon, or a second Mac, ingests the same iCloud library, the goal is that the two daemons produce **compatible** records — not bit-identical state, but state that can be merged without conflict during the RFC-011 consensus migration.

- `cloud_id`s match by construction (Apple's invariant).
- `content_hash`es match by construction (BLAKE3 is deterministic).
- `photo_id`s do **not** match — each daemon mints its own UUIDv7 at first discovery. This is acceptable because `photo_id` is daemon-internal until publish, at which point the discrepancy is resolved by the mesh itself: the publish pass's resolve pre-pass maps `cloud_id` → cloud fingerprint → any already-committed consensus id, and a second daemon **adopts** the mesh's id into `consensus_photo_id` instead of publishing a duplicate (see §HopNet publish queue and RFC-011 §Cloud Fingerprint).

This trade-off — distinct local `photo_id`s, deterministic `cloud_id` / `content_hash` — is deliberate. Forcing daemons to share `photo_id`s pre-publish would require an online coordination primitive (some daemon must "own" first-discovery), which the daemon explicitly does not have. The mesh IS that coordination point once publishing exists: first committed publish wins the id, everyone else adopts.

### Group identifiers

Cross-asset groupings (bursts, panoramas, HDR brackets, stacks) need a stable `group_id` that converges across devices without coordination. The daemon derives `group_id` deterministically from PhotoKit-provided grouping identifiers:

```
group_id = BLAKE3("ingress-v1/burst/" || burstIdentifier).hex()[..32]
```

Domain prefixes (`burst`, `stack`, `panorama`, `hdr`) prevent accidental collisions between group types that happen to share an underlying PhotoKit identifier. The `ingress-v1/` namespace allows for future revision of the derivation scheme without retroactively renaming existing groups; a `v2` scheme would coexist by writing a separate `group_id_v2` column or by versioning the entire `group_id` value.

This derivation is **one-way**: given a `group_id` and no access to the original PhotoKit state, an observer cannot recover the `burstIdentifier`. The opaque PhotoKit identifier never leaves the daemon. The cost is that the `group_id` is itself a stable identifier that observers with access to multiple `photo_id`s sharing it can use to correlate "these photos are part of the same burst" — but this is an inherent property of any grouping mechanism and is not specific to the hash derivation. Migration to RFC-011 can remap `group_id`s to fresh per-library UUIDs if even this level of correlation is undesired in the consensus model.

### Edge cases captured by the model

| Scenario | Identity outcome |
|---|---|
| Asset edited in Photos.app | Same `photo_id`. New `photo_resources` row for `resource_type=edited` (plus `edited_paired_video` for Live Photos) with new `content_hash`es. Original resource rows unchanged. |
| Asset deleted from Photos and re-added from a backup | New `cloud_id` per PhotoKit. Rule 2b applies: new `photo_id`, blob refcount incremented on the shared original `content_hash`. |
| Asset deleted from Photos and not re-added | Existing `photo_id` retains a `deleted_at` tombstone; blob refcount decremented at retention expiry; see Retention. |
| Asset is local-only on day 1, then iCloud upload completes on day 2 | Day 1: discovered with `cloud_id = NULL`, ingested via content_hash path, `photo_id` minted. Day 2: PhotoKit reports a new `cloud_id` on the change observer; rule 2a applies (late binding), the existing `photo_id`'s `cloud_id` column is populated. |
| Same content in personal and shared library scopes simultaneously | Two distinct `PHAsset`s with distinct `cloud_id`s. Two `photo_id`s. Two `photos` rows in different libraries, two `blobs` ledger rows — but one spool file (see Dedup namespace per library). |
| Burst frames | Distinct `photo_id`s, distinct `cloud_id`s per frame. Shared `group_id` derived from `burstIdentifier`. One frame marked `is_group_pick = 1` per PhotoKit's "user pick" hint. |
| Live Photo | Single `photo_id`. Two `photo_resources` rows: `original` (HEIC still) and `paired_video` (MOV). |
| RAW + JPEG paired capture | Single `photo_id`. Two `photo_resources` rows: `original` (typically JPEG, the user-visible representation) and `raw_alternate` (the RAW companion). |

## Library Partitioning

Photos are partitioned at the top level by **library** — a logical bucket corresponding to an access-control boundary. The two libraries this MVP targets are:

- `personal` — photos in the user's personal iCloud Photos library, visible only to that account.
- `shared` — photos in an iCloud Shared Photo Library, visible to all participants of that shared library.

Additional libraries can be defined (for example a second shared library, or a non-iCloud "imported" library populated from manual file drops), but the data model treats each as a distinct partition with its own dedup-ledger namespace.

### Why partition at all

The partition is the access-control boundary the mesh consumes. Personal-partition photos publish as the owner's personal consensus photos; shared-partition photos publish into a **mesh shared library** (RFC-011 Phase 3 multi-participant model) once the operator binds the local shared library to its consensus `shared_libraries` UUID (`library set-mesh-id`). An UNBOUND shared library has no publish target and is excluded from the publish claim — shared photos must never leak into the personal namespace (publishing them as personal-consensus records would create exactly the dedup and ownership debt the historical personal-only gate existed to avoid).

Daemon-side, the partition also keeps lifecycle arithmetic per-library: refcounts, retention windows, and library transitions are all ledger operations scoped by `library_id`. The daemon never reasons about who can read what — it records which partition an asset belongs to and lets the publish path (and eventually the mesh's sharing model) consume that fact.

### Library scope detection

Spike-verified reality (see `spikes/photokit/FINDINGS.md`): PhotoKit on macOS has **no public per-asset indicator** of iCloud Shared Photo Library membership. SPL assets appear in default fetches reporting `sourceType = typeUserLibrary`, indistinguishable from personal assets via documented API. The public `typeCloudShared` source type identifies only legacy iCloud Shared Albums, which are excluded from ingest scope entirely (downscaled copies, not part of the library proper — and conveniently absent from default fetches).

Detection therefore uses the undocumented KVC-readable `PHAsset` property `participatesInLibraryScope` (Bool) — verified exact against a 36k-asset library with a 10k-asset shared library. This is a private-API dependency of the same tier as the `fileSize` key used by storage-aware admission. Failure mode is specified: if the key returns nil (removed in a future macOS), the daemon treats it as a **hard error and stops ingest** — it must never default to personal, which would silently route shared photos into the personal partition, where the publish path would upload them as personal-consensus photos.

The Swift layer reads this property when constructing the `AssetDescriptor` and propagates it as an enum: `Personal` or `Shared`. iCloud supports at most one Shared Photo Library per account and PhotoKit exposes no scope identifier, so the signal is binary — the shared library's `scope_binding` is a fixed marker value (`icloud-shared-library`) rather than a PhotoKit-provided identifier. The `libraries` schema retains the general scope-binding shape for future non-PhotoKit library sources.

### Routing rule

On every asset discovery — including change-observer events for previously-seen assets — the Rust core consults the descriptor's library scope and:

1. Resolves the scope identifier to a configured `library_id` in `state.db`.
2. If no `library_id` is configured for that scope, the asset is recorded with a special `library_unmapped` sentinel and a soft error is emitted; the daemon logs a CLI prompt inviting the user to configure the library before ingest can proceed for that asset.
3. Otherwise, all subsequent operations (dedup queries, refcount adjustments, publish claims) use the resolved `library_id`.

### Asset migrating between libraries

PhotoKit supports moving an asset from the personal library into a shared library and vice versa. When this happens, the daemon observes a change event for the asset and the previously-recorded `library_id` for the asset's `cloud_id` no longer matches the current PhotoKit scope.

The daemon treats a library transition as a **ledger-only move**. The `photo_id` is retained and no bytes move — the spool is a single content-addressed tree shared by all libraries — so the whole transition is one SQLite transaction:

```
For each photo_resources row R of the transitioning photo
    (where R.content_hash is set):
  1. Increment refcount on (dst_library_id, content_hash) in blobs,
     inserting the row if absent. An inserted row inherits the source
     row's evicted_at stamp — file presence is a per-hash fact, not a
     per-library one.
  2. Decrement refcount on (src_library_id, content_hash) in blobs;
     delete the src row when it reaches 0. The file is untouched.

Then:
  3. Update photos.library_id = dst_library_id.
  4. Record a library_transition entry in the ingest log with both ids.
```

`photo_resources` rows stay keyed by `(photo_id, resource_type)` and need no library-aware updates; the spool path derives from `content_hash` alone. Pending resources ride along logically and fetch into the shared spool as usual. There is no filesystem step, and therefore no crash window beyond ordinary transaction atomicity.

An earlier revision physically relocated bytes between per-library subtrees ("hard move") because each library lived under its own storage root carrying its own filesystem ACL. With HopNet as the archive of record and a single local spool, per-library physical placement has no consumer: access control is the mesh's job, and the spool is visible to nothing but the daemon.

### Library configuration

A library is fully described by these pieces of configuration, persisted in `state.db`:

- `library_id` — short, stable identifier, generated at creation and immutable (see the `libraries` notes). Scopes the dedup ledger and is the foreign key on `photos` and `blobs`.
- `display_name` — UI string for the CLI.
- `scope_binding` — for shared libraries, the PhotoKit scope identifier this `library_id` is bound to. Personal libraries have no scope binding.
- `mesh_library_id` — the consensus `shared_libraries` UUID a shared library publishes into (`library set-mesh-id`; NULL = no publish target, excluded from the publish claim). Requires `scope_binding`, and scope detach is refused while set — personal libraries publish to the personal partition by definition, so a NULL-scope row never carries a mesh target.
- `retention_days` — soft-delete grace before hard-delete cleanup.

There are no storage paths to configure. All bytes stage in the shared spool under the data dir, and HopNet holds the archive; the personal library self-creates at daemon startup (`ensure_personal_library`), so a fresh install needs zero library configuration before ingest begins.

The Swift layer is told which PhotoKit scope identifier maps to which `library_id` via the `scope_binding` value. This decoupling lets the user rename a library's display name without breaking the PhotoKit binding (the `library_id` itself is generated and immutable — see the `libraries` notes), and lets the user opt out of shared-library ingest entirely by simply not binding its scope. Unbinding is refused while another NULL-scope row exists — the personal routing rule picks the NULL-scope row, so a second one would route personal photos arbitrarily.

*Phase 6 note:* shared-library configuration lives in the Rust `ingress-cli` (`library add/list/bind/rename/set-retention/set-mesh-id`); the FFI's only config surface is the zero-argument personal-library ensure. Every config write takes the exclusive run lock (refused while the daemon runs, with the holder's pid) and runs the Tier-1 refcount repair on an unclean reclaim.

### Dedup namespace per library

Dedup **accounting** is scoped to a single `library_id`: a hash `H` referenced from `personal` and from `shared` is two rows in `blobs`, each with its own refcount. The per-library ledger survives from the per-subtree era because it is what keeps library transitions, retention, and hard-delete pure per-library refcount arithmetic.

Dedup **bytes** are global: the spool stores at most one file per content hash, shared by every library that references it (a cross-library duplicate streams twice but the second rename lands on the same content-addressed path). The file-level rule is **hash liveness**: a spool file may be deleted only when no `blobs` row for its hash, in *any* library, remains unevicted. Every unlink site — eviction, re-edit supersede, revert, hard delete, orphan repair — gates on this check; per-library refcounts alone are never sufficient license to unlink.

A photo that exists in both `personal` and `shared` — two distinct `PHAsset`s with distinct `cloud_id`s but byte-identical originals — is therefore two `photos` rows and two ledger rows over one spool file, and each partition publishes on its own schedule (its own scope of the partitioned publish pass); the file survives until both are decided.

## Local State Schema (`state.db`)

`state.db` is the authoritative ingest state, a SQLite database at `~/.local/share/hopnet-photo-ingress/state.db`, accessed only by the daemon and the CLI (via the Rust core). It shares the local data dir with the spool.

Schema shapes are chosen for the RFC-011 publish contract: columns that flow into the mesh use the same names, types, and semantics as their `photos.md` counterparts, so the publish mapping copies them without transformation. Ingress-only columns (identity plumbing, pipeline state, refcounts) never leave the device.

### `libraries`

```sql
CREATE TABLE libraries (
    library_id           TEXT PRIMARY KEY,   -- generated two-word id, e.g. 'crisp_harbor'
    display_name         TEXT NOT NULL,      -- UI string for the CLI
    scope_binding        TEXT UNIQUE,        -- PhotoKit shared-library scope identifier; NULL for personal
    retention_days       INTEGER NOT NULL DEFAULT 30,  -- soft-delete grace before hard-delete cleanup
    created_at           TEXT NOT NULL,      -- ISO 8601
    mesh_library_id      TEXT                -- consensus shared_libraries UUID (publish target); NULL = none
);
```

Notes:

- `library_id` is **generated, not user-chosen** — library creation mints a two-word id from an embedded wordlist (`crisp_harbor`) and it is **immutable** thereafter; `display_name` is the mutable human-facing label (`library rename` edits only it). A user-supplied id duplicated `display_name`'s job while carrying identity weight it could never shed; generating it removes both the naming decision and the id-rename problem. An `--id` override remains for scripts and tests, validated as lowercase `[a-z0-9_]`. (Historically the id was also an on-disk path component; the spool is content-addressed and library-agnostic, so the id now scopes only the ledger.)
- `scope_binding` is `UNIQUE`: a PhotoKit scope maps to at most one library. SQLite permits multiple NULLs, so this does not constrain personal or future non-PhotoKit libraries.
- `retention_days` is per-library — a shared library may warrant a longer window than a personal one. The hard-delete cleanup job reads the owning library's value on each run; changing it applies from the next run (see the retention edge-case table in Deletion and Retention).
- **Exactly one personal library in the MVP**, created automatically at daemon startup (`ensure_personal_library`). PhotoKit exposes a single system photo library per account, so there is one row with `scope_binding IS NULL`. A future non-PhotoKit "imported" library (manual file drops) would be a second NULL-scope row; the schema requires no change, only routing rules.
- **No `library_unmapped` sentinel row.** An asset whose PhotoKit scope has no configured binding is recorded in `photos` with `library_id = NULL` (see the `photos` table); ingest is blocked for that asset until the user binds the scope. The unmapped state is an absence, not an entity.

### `photos`

```sql
CREATE TABLE photos (
    photo_id          TEXT PRIMARY KEY,    -- UUIDv7, minted at first discovery
    library_id        TEXT,                -- FK libraries; NULL = unmapped scope, ingest blocked
    cloud_id          TEXT UNIQUE,         -- PHCloudIdentifier; NULL for local-only assets
    local_id          TEXT,                -- PHAsset.localIdentifier; device-scoped convenience handle

    -- Cross-asset grouping (RFC-011-compatible, copied verbatim at migration)
    group_id          TEXT,
    group_type        INTEGER,             -- RFC-011 values: 0=burst, 1=stack, 2=panorama_frames, 3=hdr_bracket
    group_index       INTEGER,
    is_group_pick     INTEGER NOT NULL DEFAULT 0,

    -- Pipeline state (ingress-only, dropped at migration)
    discovered_at     TEXT NOT NULL,       -- ISO 8601, when the asset first entered the Rust core's view
    asset_modified_at TEXT,                -- PHAsset.modificationDate at last successful sync
    materialized_at   TEXT,                -- NULL = not all resources written yet
    descriptor_json   TEXT,                 -- descriptor capsule; persisted at materialization
                                           --   (see §Descriptor capsule and the publish document)

    -- HopNet publish queue (see §HopNet publish queue)
    published_at          TEXT,            -- NULL = not yet published into HopNet; set once, never reset
    publish_attempts      INTEGER NOT NULL DEFAULT 0,
    publish_next_retry_at TEXT,
    publish_last_error    TEXT,
    consensus_photo_id    TEXT,            -- set when ADOPTED (mesh already held the asset);
                                           -- consensus identity = COALESCE(consensus_photo_id, photo_id)

    -- Tombstone (RFC-011-compatible; deleted_by deliberately absent, see notes)
    deleted_at        TEXT,                -- ISO 8601, NULL when active

    -- Mesh convergence of the tombstone (see §Propagation to the mesh).
    -- What the mesh has been told, as against deleted_at's "what Photos
    -- believes"; the two disagreeing IS the propagation queue. RESETTABLE,
    -- unlike published_at — a restore clears it so the next delete queues.
    tombstone_published_at         TEXT,
    tombstone_publish_attempts     INTEGER NOT NULL DEFAULT 0,
    tombstone_publish_next_retry_at TEXT,
    tombstone_publish_last_error   TEXT,

    FOREIGN KEY (library_id) REFERENCES libraries(library_id)
);

CREATE INDEX idx_photos_library ON photos(library_id);
CREATE INDEX idx_photos_pending ON photos(materialized_at) WHERE materialized_at IS NULL;
CREATE INDEX idx_photos_deleted ON photos(deleted_at) WHERE deleted_at IS NOT NULL;
CREATE INDEX idx_photos_group ON photos(group_id) WHERE group_id IS NOT NULL;
CREATE INDEX idx_photos_unpublished ON photos(photo_id)
    WHERE published_at IS NULL AND materialized_at IS NOT NULL;
CREATE INDEX idx_photos_tombstone_pending ON photos(photo_id)
    WHERE published_at IS NOT NULL
      AND ((deleted_at IS NOT NULL AND tombstone_published_at IS NULL)
        OR (deleted_at IS NULL AND tombstone_published_at IS NOT NULL));
```

Notes:

- **`cloud_id` is globally `UNIQUE`, not per-library.** Apple guarantees distinct `cloud_id`s for distinct assets — the same content existing in both personal and shared scopes is two `PHAsset`s with two `cloud_id`s, so that case never conflicts. Global uniqueness is load-bearing for library transitions: a move between libraries preserves the `cloud_id`, and the daemon detects the move precisely because a known `cloud_id` arrives with a different scope. Per-library uniqueness would make a transition indistinguishable from a new photo in the destination plus an orphan in the source. If Apple's invariant is ever violated, the constraint fails loudly rather than letting state diverge silently. Per-library dedup scoping (see Library Partitioning) applies to `content_hash`, not `cloud_id`.
- `local_id` is a convenience handle for PhotoKit fetch calls, not an identity key — `PHAsset.localIdentifier` is device-scoped and can change across library rebuilds. It is updated opportunistically whenever the asset is observed (match-precedence step 1).
- `library_id = NULL` means the asset's PhotoKit scope has no configured binding (see `libraries` notes). The row exists so discovery is never lost, but the pipeline skips it until the scope is bound.
- `materialized_at` is the pipeline's photo-level completion marker: set (in the same transaction as the final `photo_resources` state update) only once every enumerated resource for the photo has been written and committed. Per-resource fetch/retry state lives on `photo_resources`, since resources fail independently (a Live Photo's video download can fail while its still succeeds).
- `asset_modified_at` powers the fast path's "unless metadata changed" check: if the incoming descriptor's `PHAsset.modificationDate` equals the stored value, the observer event is a no-op; if newer, the descriptor capsule is refreshed and resource-level changes are re-enumerated.
- **There is no `deleted_by` column.** The daemon has a single implicit local user and no consensus `user_id` to record — storing a sentinel would be fabricating data. Future tombstone propagation assigns the mesh's `deleted_by` from the publishing user at transaction time.
- `descriptor_json` is the **descriptor capsule**: the PhotoKit-computed metadata (media type/subtypes, favorite, capture metadata) that the publish document needs and that no other column stores. Written at materialization and refreshed on metadata-only changes; NULL means "materialized before the column existed" and self-heals via the reconciliation scan (see §Descriptor capsule and the publish document).
- `published_at` is the HopNet publish terminal state and the retry ledger's clear signal (see §HopNet publish queue). It is set once and **never reset**, because a re-publish of the same `photo_id` is hard-rejected by consensus (re-edit propagation is a future content-update transaction). It is also the **eviction predicate**: a spool blob whose every referencing photo is decided — `published_at` set, by upload or adoption — is deletable (see §HopNet publish queue).

### `photo_resources`

```sql
CREATE TABLE photo_resources (
    photo_id         TEXT NOT NULL,        -- FK photos
    resource_type    INTEGER NOT NULL,     -- RFC-011 values: 0=original, 1=edited, 2=paired_video,
                                           --   3=adjustment_data, 4=raw_alternate, 5=thumbnail_small,
                                           --   6=thumbnail_medium, 7=edited_paired_video
    content_hash     TEXT,                 -- BLAKE3 hex of resource bytes; NULL until fetched+hashed
    ext              TEXT,                 -- canonical extension for the resource's UTI (heic, jpg, mov, dng)
    size_bytes       INTEGER,              -- recorded at write time; data_block creation needs it at migration

    -- Per-resource pipeline state (ingress-only)
    written_at       TEXT,                 -- ISO 8601; NULL = not yet durably on the storage root
    retry_count      INTEGER NOT NULL DEFAULT 0,
    next_retry_at    TEXT,                 -- backoff deadline; NULL = not awaiting retry
    last_error       TEXT,

    PRIMARY KEY (photo_id, resource_type),
    FOREIGN KEY (photo_id) REFERENCES photos(photo_id)
);

CREATE INDEX idx_photo_resources_hash ON photo_resources(content_hash) WHERE content_hash IS NOT NULL;
```

Notes:

- **`resource_type` uses RFC-011's enum values verbatim** (see `photos.md` Resource Types). Thumbnail types (5, 6) are **daemon-generated JPEG renditions** (~256px small, ~1024px medium, video assets get poster frames), requested from `PHImageManager` — Apple decodes HEIC/video for free and the renditions come from the local preview cache (no iCloud round trip). They exist so gallery clients (which cannot decode HEIC in a browser) always have a decodable resource, and they flow the normal pipeline: spool, resource rows, publish.
- **PhotoKit → ingress resource mapping** (spike-verified against a real library):

  | `PHAssetResourceType` | Ingress `resource_type` |
  |---|---|
  | `photo` (1) / `video` (2) | `original` (0) |
  | `fullSizePhoto` (5) / `fullSizeVideo` (6) | `edited` (1) |
  | `pairedVideo` (9) | `paired_video` (2) |
  | `adjustmentData` (7) | `adjustment_data` (3) |
  | `alternatePhoto` (4) | `raw_alternate` (4) |
  | HopNet sentinel (1005)¹ | `thumbnail_small` (5) |
  | HopNet sentinel (1006)¹ | `thumbnail_medium` (6) |
  | `fullSizePairedVideo` (10) | `edited_paired_video` (7) |

  ¹ Synthetic, not `PHAssetResource`-backed: `DescriptorExtraction.swift` appends the two sentinel descriptors to every asset, and the fetcher renders them via `PHImageManager` (synchronous delivery — async returns nil-image/nil-error in the daemon's non-app context — with an ImageIO downscale fallback). Sentinels live at 1000 + RFC value because Apple's real namespace (1–12) collides with the RFC integers: PH 5/6 mean `fullSizePhoto`/`Video`. Their descriptor `fileSize` is a constant admission estimate (64 KiB / 512 KiB), never a re-edit signal.

  Edits never mutate the `photo`/`pairedVideo` resources — edited renders appear as separate `fullSize*` resources, and the presence of `adjustmentData` is the "this asset has edits" signal. An edited Live Photo therefore carries five resources (original still, original motion, adjustment plist, edited still, edited motion).
- **No `status` column.** Per-resource pipeline state is derivable: `content_hash IS NULL` = not yet fetched; `written_at IS NULL AND next_retry_at IS NOT NULL` = failed, awaiting backoff; `written_at IS NOT NULL` = durably written. The CLI computes human-readable status from these columns; a stored enum would be a second source of truth that can drift.
- **Spool paths are not stored.** A resource's spool path is derived at read time as `<data_dir>/spool/blobs/<aa>/<bb>/<content_hash>.<ext>` — library-agnostic, which is what makes library transitions a pure refcount-plus-`photos.library_id` operation with no per-resource updates.
- **Resource lifecycle mirrors PhotoKit's current state; no version history.**
  - *First edit* (asset gains an edited rendition): a new row with `resource_type = 1` appears alongside the untouched `original` row (for Live Photos, an `edited_paired_video = 7` row appears as well). This is additional current resources, not history.
  - *Re-edit* (asset's edited rendition is replaced): the `edited` row is updated in place with the new `content_hash`. In the same transaction — the **write-commit transaction of the replacement bytes**, not the classification event (the new bytes must be fetched first; between classification and commit the row sits in the superseded-pending state, see §Per-resource state machine) — the superseded blob's refcount is decremented (unlinking the file at 0 only if the hash is no longer live in any library's ledger) and the new blob's refcount is incremented. Detection is a `fileSize` compare (descriptor vs stored `size_bytes`) on written edit-mutable rows; equal or absent sizes are assumed unchanged, and a false positive (changed size, identical bytes) nets to a refcount no-op. Superseded edit renditions are not retained — the daemon archives the current iCloud state; version history is RFC-011's operation log's job post-migration.
  - *Revert to original* (user discards edits): the `edited` row (and `adjustment_data` row, if PhotoKit drops it) is deleted, with the same refcount decrement semantics.
  - The `original` row is never overwritten in any of these flows.
  - *Thumbnail regeneration*: written thumbnail rows (5, 6) reopen whenever the photo's edit-mutable set changes — first edit, re-edit, or revert — because the renditions render the *current* primary display. They are deliberately excluded from the `fileSize` re-edit compare (their descriptor size is a constant admission estimate; comparing it against real stored bytes would reopen them on every delivery). Metadata-only refreshes never touch them.
  - *Backfill*: photos ingested before the daemon generated renditions never re-deliver descriptors (the reconciliation scan probes unchanged photos `Done`), so a schema migration mints pending 5/6 rows for materialized, library-bound, PhotoKit-addressable photos and clears their `materialized_at`. Tombstoned photos are skipped (the restore delivery heals them); unmapped-scope photos heal at adoption. Already-**published** photos re-materialize with thumbnails but do not re-publish — `published_at` is terminal until content-update propagation lands (future phase).
  - *Thumbnail failure blocks materialization* (and therefore publish) at the retry cap, the same policy as any resource; the next scan's gave-up reset re-arms them.
- **`adjustment_data` (type 3) is captured.** `PHAdjustmentData` is the reversible-edit recipe and cannot be re-derived once the Photos library is gone; RFC-011 expects it for edit reconstruction. The payload is a small non-image blob; it flows through the same content-addressed write path with an extension derived from its UTI.
- A resource row is created for every resource enumerated on the asset at discovery time, before any bytes are fetched — mirroring the `photos` row's mint-before-materialize rule, so inflight per-resource progress is queryable.

### `blobs`

```sql
CREATE TABLE blobs (
    library_id       TEXT NOT NULL,        -- FK libraries
    content_hash     TEXT NOT NULL,        -- BLAKE3 hex
    ext              TEXT NOT NULL,        -- extension of the file on disk
    size_bytes       INTEGER NOT NULL,
    ref_count        INTEGER NOT NULL,     -- number of photo_resources rows referencing this blob
    written_at       TEXT NOT NULL,        -- ISO 8601, when the atomic rename landed
    evicted_at       TEXT,                 -- ISO 8601; spool file reclaimed after decided publish

    PRIMARY KEY (library_id, content_hash),
    FOREIGN KEY (library_id) REFERENCES libraries(library_id)
);
```

Notes:

- **`ref_count` invariant**: `ref_count` equals the number of `photo_resources` rows whose `content_hash` matches and whose parent photo's `library_id` matches — i.e. it is fully recomputable via a JOIN through `photos`. Every increment/decrement happens in the same SQLite transaction as the `photo_resources` change that caused it. The invariant counts **rows, not files**: eviction deletes the file and stamps `evicted_at` without touching `ref_count`, so the Tier-1 recount (which recomputes from resource rows) never fights a deliberate eviction. A decrement-based eviction would be silently reverted by the next recount — this is why the stamp exists.
- **Eviction is a stamp, not a row delete.** `evicted_at` means "the mesh holds every referencing photo; the spool file was reclaimed." The row (and its refcount) survives so recounts stay truthful and dedup stays correct: a later materialization that hits an evicted row re-places the bytes and clears the stamp instead of treating ledger presence as file presence. The file-unlink gate is spool-wide hash liveness (see §Dedup namespace per library).
- **Why a stored count rather than deriving it**: a declared FK from `photo_resources` to `blobs` is not expressible (the blob key includes `library_id`, which lives on `photos`, not `photo_resources`). More importantly, the count gates eager, irreversible filesystem deletes (re-edit supersede, revert, hard-delete cleanup) — `UPDATE ... SET ref_count = ref_count - 1 ... RETURNING ref_count` is a single atomic operation with no scoping JOIN to get subtly wrong at any call site. The redundancy is deliberate: a stored count plus a recount JOIN are two independent answers that must agree, so refcount drift from a crash or bug is detectable and repairable (recovery and a CLI `fsck`-style check recompute and diff). RFC-011's `DataBlockReferenceProvider` derives instead because its cleanup is a lazy background sweep spanning multiple modules; ingress deletes eagerly within one module, which favors the counter.
- **`ext` is an attribute, not part of the key.** Identical bytes imply the same UTI in practice; in the pathological case of the same content arriving under two different UTIs, the first writer's extension wins and a warning is logged.
- **Single-writer invariant**: the spool, like `state.db`, is device-local under one data dir, and the exclusive `drain.lock` ensures one writing process. A second Mac or future iOS daemon ingesting the same iCloud library has its own spool and converges at the mesh via the resolve/adoption pre-pass (see Cross-Device Convergence), not by sharing files. In-process, at most one inflight materialization runs per `(library_id, content_hash)`, so two photos sharing a blob don't race the same temp-write-rename.
- No integrity/scrub column (`verified_at`) — spool residence is transient (bounded by publish latency), and post-decide durability is the mesh's job (RS-encoded fragments, mesh-side repair). Bit-rot in a file that lives locally for minutes-to-days is not a design concern.

### `ingest_log`

```sql
CREATE TABLE ingest_log (
    id           INTEGER PRIMARY KEY,      -- rowid, monotonic
    at           TEXT NOT NULL,            -- ISO 8601
    event_type   TEXT NOT NULL,            -- see event list below
    photo_id     TEXT,                     -- NULL for non-photo-scoped events
    detail       TEXT                      -- JSON, event-specific payload
);

CREATE INDEX idx_ingest_log_photo ON ingest_log(photo_id) WHERE photo_id IS NOT NULL;
```

The ingest log is **authoritative for nothing**. No code path reads it to make a decision; deleting the table changes no behavior. State tables answer "what is"; the log answers "what happened" — it is the black-box recorder for a daemon that deletes irreplaceable data. Its primary consumer is forensics: after a hard delete, the state tables retain no trace of the photo, and the log is the only artifact that can answer "where did my photo go?" (`deletion_observed` on March 3, `hard_delete` on April 2). Secondary consumers are CLI history views and incident debugging (publish parks, ingest stalls). It does not migrate to RFC-011 — deletion state flows through `photos` columns, not log replay.

Logging rule: an event is logged if it **destroys bytes** or **explains a stall**. Per-retry fetch failures are never logged — `retry_count` / `last_error` on `photo_resources` carry current failure state, and a flaky iCloud connection over a 50k-asset library would flood the log.

| Event | photo_id | detail |
|---|---|---|
| `deletion_observed` | yes | — |
| `restore_observed` | yes | — |
| `hard_delete` | yes | resources + hashes removed |
| `library_transition` | yes | `{src, dst}` library ids |
| `blob_superseded` | yes | old/new hash on re-edit (old bytes deleted) |
| `resource_gave_up` | yes | resource_type, final error (retries exhausted) |
| `scan_started` / `scan_completed` | no | counts |
| `spool_evicted` | no | blobs + bytes reclaimed by a publish pass or cleanup sweep |
| `publish_adopted` | yes | mesh-held consensus id adopted without upload |
| `publish_not_responsible` / `responsibility_regained` | no | edge-triggered responsibility standing |
| `publish_descriptor_missing` | yes | NULL capsule at publish claim (self-heals via scan) |
| `storage_low` / `storage_recovered` | no | free bytes, reserve floor (data-dir disk) |
| `node_unreachable` / `node_regained` | no | publish reachability edges |
| `scope_unmapped` | no | PhotoKit scope id encountered with no binding |

Shape is deliberately loose — `event_type` as TEXT (readable in a `sqlite3` shell, new types cost nothing), `detail` as freeform JSON — because nothing downstream parses it. Rows older than 180 days are pruned by the hourly cleanup job.

## Descriptor capsule and the publish document

There are no metadata files. The PhotoKit-derived metadata that once lived in per-photo sidecar JSON documents lives in `photos.descriptor_json` — the **descriptor capsule** — and the JSON document HopNet receives at publish time is composed on the fly from the capsule plus the live DB rows.

### Descriptor capsule

Persisted at materialization and refreshed on metadata-only changes, the capsule carries exactly the fields that are PhotoKit-computed and stored on no other column:

- `media_type` — `"image" | "video" | "live_photo"`, matching RFC-011's values.
- `media_subtypes` — PhotoKit's computed subtype flags (`hdr`, `screenshot`, `panorama`, `slomo`, …); PhotoKit-derived, not recoverable from EXIF alone.
- `favorite` — PhotoKit-only state, mapping to RFC-011's `photo_favorites`.
- `capture` — capture date as PhotoKit resolves it, dimensions, orientation, duration, camera make/model, GPS. These mirror RFC-011's sidecar `photo_index` columns one-to-one, so the publish mapping constructs the metadata document without transformation.

Everything else the publish document needs — identity, tombstone state, grouping, the resource list — stays authoritative on `photos`/`photo_resources` rows. This is why tombstone, restore, favorite refresh (which rewrites the capsule), and library transitions are plain row updates with no document rewrite: no document exists until publish composes one.

A photo materialized before the capsule column existed has a NULL capsule; the reconciliation scan detects this and requests a full descriptor re-delivery to backfill it. Publish skips a NULL-capsule photo without burning an attempt (`publish_descriptor_missing`).

Anything re-extractable from original bytes is excluded — notably the **full EXIF dump**: raw EXIF lives inside the original blobs, which the mesh holds durably. Exotic EXIF can be re-extracted from mesh-served bytes on demand; the capsule carries only the curated fields.

### Publish document

`Sidecar::compose` (the name survives its file-format origins) assembles the document each publish pass from the capsule and the committed rows:

```json
{
  "schema": "hopnet-photo-ingress/v1",
  "photo_id": "01912e5a-7b3c-7f21-a4d8-3e9f12ab34cd",
  "library_id": "personal",
  "cloud_id": "ABC123…",
  "ingested_at": "2026-07-02T14:03:11Z",
  "deleted_at": null,

  "captured_at": "2019-08-14T16:22:03+02:00",
  "media_type": "image",
  "media_subtypes": ["hdr"],
  "pixel_width": 4032,
  "pixel_height": 3024,
  "orientation": 6,
  "duration_ms": null,
  "camera": { "make": "Apple", "model": "iPhone 15 Pro" },
  "location": { "lat": 45.5017, "lon": -73.5673 },
  "favorite": false,

  "group": { "id": "3f9a…", "type": "burst", "index": 3, "is_pick": true },

  "resources": [
    { "type": "original", "content_hash": "ab34…", "ext": "heic", "size_bytes": 2841923 },
    { "type": "paired_video", "content_hash": "9c1f…", "ext": "mov", "size_bytes": 1204833 }
  ]
}
```

Field notes:

- `schema` versions the format; a `v2` reader handles `v1` documents, never the reverse requirement.
- `location` is present only when the asset has GPS data; `camera` fields are null for assets without camera metadata (screenshots, imports).
- `resources` lists every durably-written resource with `type` as the RFC-011 enum *name* (not integer — the document optimizes for human readability).
- The document reflects committed state at compose time. There is no stored copy to drift, replicate, or rewrite — publish composes fresh every pass.

### Explicitly excluded

- **Full EXIF dump** — re-extractable from the originals the mesh holds (see above).
- **Album membership** — deliberately out of scope for the MVP. RFC-011's album support is Phase 4, and PhotoKit retains album structure, so a later revision can backfill albums from the live Photos library at any time; the only unrecoverable scenario is the Photos library itself dying before that lands.

## Ingest Pipeline

This section specifies the invariants a correct pipeline must preserve — state transitions, transaction boundaries, crash windows, and admission rules. Worker topology, queue scheduling, backoff parameters, and concurrency defaults are implementation details; the spec constrains only their observable behavior.

### Discovery and the work queue

Two discovery modes feed the same downstream path:

- **Observer events** (runtime): `PHPhotoLibraryChangeObserver` callbacks, translated to `AssetDescriptor`s.
- **Reconciliation scan** (startup + periodic): full `PHAsset` enumeration diffed against `state.db`. Catches everything missed while the daemon was offline — including deletions, which are detectable only this way (asset present in `state.db`, absent from PhotoKit).

Both modes are idempotent: every descriptor resolves through match precedence (see Asset Identity Model), so duplicate or re-delivered events are no-ops.

**`state.db` is the work queue.** `photos` and `photo_resources` rows are minted at discovery, before any bytes move; the pipeline pulls pending work from `state.db` (`materialized_at IS NULL`, `written_at IS NULL`, gated by `next_retry_at`). PhotoKit is never re-enumerated to find work — a restart resumes exactly where the daemon left off by querying pending rows.

### Change classification

Every discovery event resolves to exactly one of five kinds. All state transitions and transaction boundaries hang off this taxonomy:

| Kind | Trigger | Actions |
|---|---|---|
| New photo | No identity match | Mint `photo_id` + resource rows, enqueue fetches |
| Resource change | Resource set or `edited` bytes differ | New/updated/deleted resource rows; re-edit and revert follow the lifecycle in `photo_resources` notes (refcount swap in same tx) |
| Metadata-only | `asset_modified_at` newer, resources unchanged | Update `photos` row + descriptor capsule; no byte movement |
| Scope change | Known `cloud_id`, different library scope | Ledger-only move (see Library Partitioning) |
| Deletion | Asset absent / inaccessible | Tombstone (see Deletion and Retention) |

### Per-resource state machine

A resource has three persistent states, keyed on `written_at`:

- **pending** — `content_hash IS NULL`, `written_at IS NULL`. Retry metadata (`retry_count`, `next_retry_at`, `last_error`) annotates this state; a resource that exhausted retries is still `pending`, terminally, with a `resource_gave_up` log event.
- **superseded-pending** *(Phase 4 amendment)* — `content_hash` set, `written_at IS NULL`. A re-edit *reopens* the row: `written_at` and retry state clear, but the old `content_hash` is **retained as the superseded pointer** so the blob refcount swap (decrement old, increment new, `blob_superseded` log) commits atomically with the replacement bytes at write time. This preserves both the no-byte-loss window (the old render's refcount survives until the new render is durable) and the `ref_count` recount invariant (the row still references the old blob until the swap). The work queue keys on `written_at IS NULL`, so this state is pending for scheduling purposes.
- **written** — `content_hash` and `written_at` both set, always in the same transaction.

There is deliberately no persisted intermediate ("fetched", "hashed"): bytes stream once through the BLAKE3 hasher into the temp file simultaneously, so the hash is only known at the moment the blob is already durably placeable — the two facts commit together or not at all. A crash mid-stream leaves only an orphan temp file and an untouched `pending` (or superseded-pending) row.

### Write path

1. **Admission** — storage-aware check (below) before a fetch slot is granted.
2. **Stream** — `PHAssetResourceManager.requestData` (with `isNetworkAccessAllowed = true`) delivers chunks in memory; each chunk feeds the BLAKE3 hasher and appends to `<data_dir>/spool/blobs/.partial/<photo_id>.<resource_type>`. The temp is named by `(photo_id, resource_type)` — the hash isn't known yet, and this naming makes per-resource inflight exclusivity structural. Exception: a brand-new photo's original streams *before* any `photo_id` exists (identity rules 2a–2c resolve from its hash), so pre-mint originals use a fresh probe token (`probe-<uuid>`) under the same `.partial/` directory, swept identically at startup.
3. **Dedup decision** — stream complete, hash known. Query `blobs(library_id, content_hash)`:
   - **Hit on a live row**: delete the temp. Transaction: increment `ref_count`, set `content_hash`/`ext`/`size_bytes`/`written_at` on the resource row.
   - **Hit on an evicted row**: the file is gone, but the bytes just streamed — place them (fsync + rename, as a miss) and clear `evicted_at` in the same transaction as the refcount increment. The next publish pass re-evicts once decided.
   - **Miss**: fsync the temp, rename to `spool/blobs/<aa>/<bb>/<hash>.<ext>`. Transaction: insert `blobs` row with `ref_count = 1`, update the resource row as above.
4. **Photo completion** — if this was the photo's last unwritten resource, the same transaction sets `photos.materialized_at`, and the descriptor capsule (`photos.descriptor_json`) is persisted from the drain-time descriptor — the photo is now publishable.

**Ordering invariant: filesystem durability precedes database commit.** A committed row never references bytes that might not exist. The crash windows this leaves are all benign:

| Crash point | On-disk result | Repair |
|---|---|---|
| Mid-stream | Orphan `.partial` file, `pending` row | Startup sweep deletes all `.partial` files (nothing ever references them); row retries |
| After rename, before commit | Blob file with no `blobs` row | Orphan scan (see Recovery); refetch takes the dedup-hit path if the file is re-created first, or rewrites it — content addressing makes both idempotent |
| After commit | Consistent | Nothing to do |

**Temps live inside the spool** (`spool/blobs/.partial/`), on the same filesystem as their destination, so the finalize rename is atomic. The cost of a dedup hit is having streamed the bytes from iCloud before discarding them; this is accepted because the `cloud_id` fast path catches already-ingested assets before any download, making write-stage dedup hits rare in steady state.

### Storage-aware admission

Before a fetch is admitted, the scheduler checks that the write can complete:

- **Expected size** comes from `PHAssetResource`'s `fileSize` attribute — an undocumented KVC key (`value(forKey: "fileSize")`), reliable in practice but treated as advisory: it may be absent or zero for assets not yet downloaded from iCloud. Unknown sizes are assumed to be a configurable pessimistic estimate (default: the largest asset observed so far in this library).
- **Free space** on the data-dir filesystem (`state.db` and the spool share it), minus the summed expected sizes of already-inflight writes, must stay above a configurable reserve floor (default 10 GiB). Breach pauses admission (inflight writes finish), emits `storage_low`, and admission resumes with `storage_recovered` once the check passes again. **This is what bounds the spool**: admission stops before the disk crosses the floor, and eviction of decided publishes is what frees it — the steady-state spool footprint is the publish queue's in-flight window, not the library size.
- **PhotoKit cache headroom**: PhotoKit stages iCloud downloads in its own cache on the same local disk — this cannot be opted out of, and the daemon never sees those files. Required extra headroom is approximately `fetch_concurrency × largest asset`; the bounded fetch pool is the control knob.

### Concurrency and cancellation

- Bounded fetch and write pools (defaults 4/4, configurable; see Process model).
- At most one inflight materialization per `(library_id, content_hash)` (see `blobs` notes) and per `(photo_id, resource_type)` (structural, via temp naming).
- SIGTERM triggers cooperative cancellation: inflight streams are abandoned (their temps swept at next startup), SQLite transactions are never interrupted mid-flight (they are fast and atomic anyway).
- Every step is re-runnable. Idempotency falls out of content addressing plus match precedence — re-fetching a written resource is a dedup hit; re-processing a discovery event is a no-op.

### HopNet publish queue

The daemon-loop tick (`ingress-core/src/publish.rs`; concrete publisher in `crates/ingress-publisher`) that pushes completed photos into a HopNet node over the thin-client routes — the step that makes HopNet the archive of record: consensus-committed, RS-encoded, mesh-distributed storage.

- **Claim predicate**: `published_at IS NULL AND materialized_at IS NOT NULL AND deleted_at IS NULL AND publish_attempts < cap AND (publish_next_retry_at IS NULL OR due)`, joined to libraries with a **publish target** — `scope_binding IS NULL OR mesh_library_id IS NOT NULL` (personal always publishes; a shared library only once mesh-bound; an unbound shared library published as personal-consensus photos would be exactly the dedup debt the old personal-only gate avoided). Small batches (default 4), claimed photos registered **inflight** for the pass so PhotoKit events for them defer — the same machinery that protects photo_tasks also excludes supersede/hard-move races on the streaming blob reads.
- **The pass is partitioned by publish scope** — the photo's library's `mesh_library_id` (NULL = personal partition), personal first, then mesh ids in sorted order, each scope running its own resolve → adopt → gate → publish sequence. Responsibility standing, parking, and resolve-failure attempt burning are all **per-scope**: a kicked member's 403ing shared library backs off toward `gave_up` without starving the personal queue, and losing the personal claim holds personal photos while the shared library keeps draining. Node unreachability is the one whole-pass condition — the first unreachable scope parks everything (no attempts anywhere). The edge logs (`publish_not_responsible` / `responsibility_regained`) carry a `library` field.
- **One spawned pass task** (unlike the inline replication tick): publishing streams multi-GB originals; inline it would stall event routing for the duration.
- **Metadata source is the descriptor capsule plus live rows** — `Sidecar::compose` builds the publish document per pass (see §Descriptor capsule and the publish document), so the document always reflects committed state with no stored copy to drift. A NULL capsule (pre-column photo awaiting scan backfill) skips that photo without burning an attempt (`publish_descriptor_missing`) and the batch continues.
- **A missing spool file at publish time clears the resource's `written_at`**, re-entering it into the fetch queue — belt-and-braces self-healing for a file lost outside the daemon's control, rather than burning publish attempts against bytes that cannot appear.
- **The daemon-minted `photo_id` IS the consensus `photos.id`** for photos this device publishes itself: `PublishRequest.photo_id` carries it verbatim, so the `SourceIdentity → photo_id` persistence half of the publisher idempotency contract is satisfied by construction. The one exception is **adoption** (below), where the mesh's pre-existing id is recorded in `consensus_photo_id` — the photo's consensus identity is always `COALESCE(consensus_photo_id, photo_id)`.
- **Resolve pre-pass (consensus photo identity)**: each scope opens with one batched `POST /api/photos/client/resolve` carrying that scope's claimed `cloud_id`s plus the scope's `library_id` (absent = personal). The node (which holds the user key at device-auth time; the daemon holds none) returns per cloud_id the keyed **cloud fingerprint**, any already-committed consensus photo id, and this device's responsibility standing **in that scope**. The fingerprint key is scope-selected (RFC-011 §Cloud Fingerprint): the per-user key for personal, the **library-scoped key** (blake3 derive from the shared library key — every member derives the same one) for shared, which is what makes one member's resolve match ANOTHER member's committed photos. Two members' daemons ingesting the same iCloud Shared Photo Library therefore converge: the second daemon adopts instead of re-uploading, and if both race the same asset, the loser's `photo_add` fails deterministically on the `(library_id, cloud_fingerprint)` UNIQUE index, re-resolves next pass, and adopts the winner's row.
  - *Adoption*: a committed hit on a different id means the mesh already holds this asset (published by another device, or by a previous state.db of this one — the handoff case). The pass stamps `published_at` + `consensus_photo_id` WITHOUT uploading (`publish_adopted` log, `adopted` counter). A hit on the photo's **own** id is an earlier ambiguous submit that actually landed — counted `already_published`, `consensus_photo_id` stays NULL. Adoption runs in **every** responsibility standing; it is read-only node-side and what makes a designation handoff a cheap sweep instead of a full re-upload.
  - *Responsibility gate*: if this device is not the holder for the scope (`other` | `unclaimed`), that scope's remaining photos are HELD under a distinct `parked_responsibility` state — edge-logged per scope, zero attempts consumed. Responsibility is **per (user, scope)**: `POST /api/photos/ingress/claim {device_id, library_id?}` (JWT-only; a shared-scope claim requires applied membership, and a kick dissolves the target's scope claim). Each member of a shared library claims independently for their own devices — cross-member dedup is the fingerprint's job, not responsibility's. The device transaction route rejects the claim tx kind and 403s mutations for any scope the device doesn't hold (decode-at-admission, exact per scope — holding the personal claim never admits shared writes or vice versa); the fingerprint UNIQUE pair is the correctness backstop behind that. A daemon never claims for itself; the enablement UI (or curl) designates deliberately.
  - *Fingerprint threading*: resolve-returned fingerprints ride each `PublishItem` into the `photo_add` payload, so the mesh row carries the dedupe key. `cloud_id`-NULL photos (local-only assets) skip the resolve batch and publish fingerprint-less — exempt from dedupe.
  - *Failure classes*: an unreachable resolve parks exactly like an unreachable publish; other resolve failures burn ONE attempt per claimed photo (bounded backoff toward `gave_up` instead of silent spinning).
- **Confirm-then-retry**: consensus hard-rejects duplicate photo ids (proposer preflight), so every publish attempt probes `GET /api/photos/client/committed/{photo_id}` first — 200 resolves an earlier ambiguous failure as already-published (stamp, never re-submit); 404 makes the same-id submit safe. Ambiguous outcomes (opaque 500s, submit timeouts) classify as transient; the next tick's probe disambiguates. (For fingerprinted photos the resolve pre-pass usually answers first via self-resolution; the probe remains the path for `cloud_id`-NULL photos.)
- **Retry ledger**: transient failures back off exponentially (base 60s, max 6h) up to `publish_attempts = cap` (terminal until operator reset); permanent rejections (mapping/validation, malformed fingerprints) jump straight to the cap. Node unreachability (connect/timeout/HTTP 503 shedding) consumes **no** attempts — the pass parks.
- **Auth**: an RFC-012 device token (`{device_id}.{secret}`), so the daemon can target any node holding the consensus state, and revoking the device row kills its access mesh-wide. The node derives the device identity for the responsibility gate from the same token.
- **Eviction rides the pass.** After each publish pass — and again on the hourly cleanup tick, which catches strays — blobs whose every referencing photo in the library is decided (`NOT EXISTS … published_at IS NULL`) are stamped `evicted_at` and their spool files deleted under the spool-wide hash-liveness gate. Stamp first, then unlink: a crash between the two leaves a lingering file that fsck classifies as a benign orphan, never byte loss. `spool_evicted` is logged when a pass reclaims anything. This is the point of the spool — residence is bounded by publish latency, not library size.
- **Driver exit codes** (`ingress-publish-e2e publish`): 0 drained, 2 unreachable-park, 3 = SOME scope responsibility-parked — since the pass is scope-partitioned, healthy scopes were still drained first (read `published`/`parked_responsibility` in the JSON, not just the code).
- **Kick mid-stream**: a member removed from the mesh library loses the scope on both ends — the remove handler dissolves their responsibility row, and their daemon's next scoped resolve 403s (`library_not_member`), burning attempts toward `gave_up` for that scope only. No ingress-side reaction beyond the backoff this cycle; clearing the local mesh binding stops the attempts.
- **Out of scope (this phase)**: re-edit propagation (a re-materialized published photo is NOT re-enqueued; content updates need their own transaction type), tombstone propagation (designed but not built — see §Propagation to the mesh), and favorites (Phase 4). Shared-library publish landed with the scope-partitioned pass above — the historical "shared libraries (Phase 3)" exclusion is closed.

## Deletion and Retention

The daemon mirrors PhotoKit deletions into its own state, but does not delete rows (or any still-spooled bytes) immediately. A retention window allows recovery from accidental deletes — both at the user's "oh wait" level and at the level of unexpected PhotoKit observer churn during library reorganizations.

The model mirrors `photos.md`'s 30-day soft-delete retention so that migration into the consensus layer carries the deletion state forward unchanged.

### Trigger

The PhotoKit change observer emits an event when a `PHAsset` becomes inaccessible to the API. In practice this fires when the user moves the asset to Photos's "Recently Deleted" album, not when the asset is purged from Recently Deleted 30 days later. The daemon treats this single event as the deletion trigger and does not attempt to distinguish "soft delete in Photos" from "permanent delete in Photos." The 30-day window described below stacks on top of whatever grace Apple's own Recently Deleted provides, which is a feature rather than a bug.

### Tombstone

When a deletion event fires for an asset the daemon has previously ingested:

1. `photos.deleted_at` is set to the current timestamp.
2. No deleting actor is recorded — the daemon has a single implicit user; RFC-011's `deleted_by` is assigned to the importing user at migration (see the `photos` schema notes).
3. `photo_resources` rows are **not** touched.
4. `blobs.ref_count` values are **not** decremented.
5. A `deletion_observed` event is recorded in the ingest log.

Nothing else happens — the tombstone is a single-row update. Steps 3 and 4 mean the refcount remains accurate to the (still-existing) `photo_resources` rows. The photo disappears from active queries (`WHERE deleted_at IS NULL`) but is fully restorable until the retention window expires.

Spool interplay: a tombstoned photo that never published counts as a live reference (`published_at IS NULL`), so its spool bytes are **not** evicted — they survive locally through the retention window and are reclaimed at hard delete. A tombstoned photo that already published keeps its rows through retention like any other, but its bytes follow the ordinary eviction rule; the mesh copy is unaffected until propagation runs (see §Propagation to the mesh — designed, not yet built).

This matches the `photos.md` reference provider's behavior: a soft-deleted `photos` row keeps its `photo_resources` rows alive, which in turn keep their data blocks alive. The daemon's `blobs.ref_count` is the analogue of the consensus layer's reference-provider check.

### Restore inside the window

If PhotoKit subsequently emits a change event indicating the asset is alive again — typically because the user un-deleted it from Recently Deleted — the daemon resolves identity by `cloud_id`, finds the tombstoned `photos` row, and:

1. Clears `photos.deleted_at`.
2. Records a `restore_observed` event in the ingest log.

No byte movement is required because nothing was moved on the original delete. Restore is atomic at the SQLite level: a single update statement on the `photos` row.

If the asset has been deleted in PhotoKit and then re-imported as a fresh asset (new `cloud_id`), it is **not** a restore — it is a new photo, even if the bytes are identical. Rule 2b from the Asset Identity Model applies: a new `photo_id` is minted and the existing blob's refcount is incremented.

### Propagation to the mesh

**Status: designed, not implemented.** Both preceding subsections describe purely local state — a delete or restore observed from PhotoKit never reaches HopNet today, so the mesh keeps serving a photo the user has discarded (and keeps hiding one they have recovered). This subsection specifies the convergence mechanism; the work is tracked separately.

The mesh side needs nothing new. `photo_delete` and `photo_restore` are registered handlers, and both are already in `DEVICE_TX_FUNCTIONS` — a daemon may submit them over `POST /api/photos/client/transaction` under its existing device token, subject to the per-scope responsibility gate that admits every other photo-targeting transaction. Delete authorization is already the uploader **or any member of the photo's shared library**, matching Apple's Shared Photo Library semantics where any participant may delete. The delete handler is idempotent on a missing photo. What is missing is entirely on the daemon: nothing turns a local tombstone into a submission.

#### Why a marker column is required

`published_at` doubles as both state and queue marker: `publishable_photos` selects `published_at IS NULL`, and stamping it removes the row from the queue permanently. That works because publication is **monotonic** — a photo goes unpublished to published exactly once, in one direction, and a nullable timestamp captures a one-way door completely.

Deletion is **cyclic**. A user may delete, restore from Recently Deleted, and delete again without limit. A queue selecting `published_at IS NOT NULL AND deleted_at IS NOT NULL` has no off-switch: it would re-submit `photo_delete` for every tombstoned photo on every tick, through the retention window and beyond, until hard delete finally removes the row. Idempotency on the mesh side keeps this correct but not cheap — it is one consensus transaction per deleted photo per tick.

So propagation state needs its own column, `tombstone_published_at`. This is consistent with the convention in §`photo_resources` that per-resource state is derivable from nullable timestamps rather than a stored `status` enum: a timestamp *is* the state, and doubles as the audit record. The prohibition is on enums shadowing timestamps, not on markers as such.

#### The two-column state machine

`deleted_at` records what Apple Photos believes. `tombstone_published_at` records what the mesh has been told. The queue is the delta.

| `deleted_at` | `tombstone_published_at` | State | Action |
|---|---|---|---|
| NULL | NULL | Live; mesh agrees | none |
| set | NULL | Deleted locally, mesh not told | submit `photo_delete`, then stamp |
| set | set | Deleted; mesh converged | none |
| NULL | set | Restored locally, mesh still tombstoned | submit `photo_restore`, then clear |

The restore queue is the fourth row and costs nothing extra — the same column drives both directions.

**`tombstone_published_at` must be resettable**, unlike `published_at`. A successful restore clears it, returning the row to the first state. If a restore left it set, the next delete would land in the third state and never propagate, leaving the mesh holding a photo Photos has discarded. This is a deliberate deviation from its neighbour in the same table, whose "set once, never reset" rule exists for the opposite reason (a re-publish of the same `photo_id` is hard-rejected by consensus).

The propagation queue carries its own retry ledger rather than reusing `publish_attempts` / `publish_next_retry_at` / `publish_last_error`. Publish success resets that ledger, so the columns are technically free — but a photo that struggled to publish, succeeded, and then failed to propagate its delete would carry a blended failure history under a `publish_last_error` string describing the wrong operation.

#### Marker as cache, resolve as repair

`tombstone_published_at` is a local memo, not the truth. The truth is the mesh's `photos.deleted_at`, and the daemon already has a seam that reaches it: the resolve pre-pass deliberately resolves tombstoned rows (see §HopNet publish queue and the no-`deleted_at` rationale on the by-fingerprint lookups), it simply returns the committed id alone today. Extending that projection to carry the mesh's tombstone state lets a daemon whose marker is stale or absent — a rebuilt `state.db`, a Tier 3 re-scan — reconverge instead of diverging permanently.

This mirrors publication exactly: `published_at` is the fast local marker, and adoption-by-fingerprint is the repair path when it is missing. One idea applied twice rather than two idioms side by side.

#### Relationship to re-edit propagation

Re-edit propagation is the same *pattern* at a different granularity, and should reuse the vocabulary rather than invent a parallel one. Both are convergence between local desired state and remote known state, with a marker recording last-converged and the delta forming the queue. Three things differ, and they are what make it separate work rather than a second instance:

- **Granularity.** A tombstone is a property of the photo. An edit is a property of its resources — `photo_edit_content` carries `resources: Vec<PhotoResourceOp>`, so the marker belongs on `photo_resources`, not `photos`.
- **Cardinality.** Deletion is a bit, so a timestamp suffices to say "told." An edit is a *value*: the marker must record *which* version the mesh holds, comparing against the resource's current `content_hash`. A bare timestamp cannot express that.
- **Payload.** `photo_delete` carries only photo ids, so propagation is transaction-only. `photo_edit_content` carries data blocks that must be uploaded first, which means re-edit propagation drives the entire fetch/encrypt/upload pipeline, not just a submission. This is the bulk of the difference in cost.

The bookkeeping generalizes; the work does not.

### Hard-delete cleanup

A periodic cleanup job runs on a configurable interval (default: once per hour) and processes photos whose retention window has expired:

```
For each photo P with deleted_at IS NOT NULL
                AND datetime(deleted_at, '+30 days') < datetime('now'):

  Inside a single SQLite transaction:
    1. For each photo_resources row R of P:
         decrement blobs(library_id, content_hash).ref_count
         record (content_hash, ext) for post-tx cleanup
            if the decremented count is zero (row deleted at zero)
    2. Delete photo_resources rows for P.
    3. Delete photos row for P.
    4. Record a hard_delete event in the ingest log (in-tx — see errata).

  After the transaction commits:
    5. For each recorded (content_hash, ext) whose count reached zero
       AND whose hash is no longer live in any library's ledger,
       delete the spool file at spool/blobs/<aa>/<bb>/<hash>.<ext>.
       (Already-evicted blobs have no file; the unlink is a no-op.)
```

The transaction commits before the filesystem unlink because SQLite is fast and authoritative, and the crash window this leaves is benign: an orphan spool file with no live `blobs` row, which `fsck` reports and `--repair` deletes.

The cleanup job is idempotent: re-running it produces no additional effect once a photo has been fully hard-deleted.

*Implementation errata (Phase 5):* the `hard_delete` log event commits **inside** the step-1–3 transaction, not after the filesystem operations — a crash between the transaction and fs cleanup must never leave a vanished photo with a silent black box, which is the exact forensic failure the ingest log exists to prevent. The detail (resources, hashes, reaped blobs) is fully known pre-fs. Each photo is its own transaction; retention cutoffs are computed Rust-side (`now − retention_days`) and bound as parameters, reading each library's `retention_days` fresh per run. Hard deletes are batch-capped per run (default 500) — a whole-library expiry processes across consecutive runs rather than stalling the daemon loop.

### Edge cases

| Scenario | Behavior |
|---|---|
| Soft-deleted photo's blob is also referenced by an active photo | Refcount stays > 0 during cleanup; blob file is preserved. Only the tombstoned photo's `photos` and `photo_resources` rows are removed at hard-delete time. |
| Daemon offline when PhotoKit deletes an asset | On next observer reconciliation, the asset is reported as absent from PhotoKit while still present in `state.db`. Daemon synthesizes a deletion event and tombstones the photo. The `deleted_at` timestamp is the reconciliation moment, not the original PhotoKit delete moment, so the retention window starts from when the daemon noticed. |
| Daemon offline for longer than 30 days | Same as above — tombstone is created with `deleted_at = now`, full 30-day window applies. No assumption that the user's PhotoKit-side grace already elapsed. |
| User deletes the entire iCloud Shared Photo Library | Every shared-library asset transitions to a deletion event on the change observer; daemon tombstones them all. After 30 days, cleanup hard-deletes the rows and any still-spooled bytes. The `libraries` row for the shared library remains until the user explicitly removes it via CLI. |
| Photo deleted, then user attempts to re-import the same image file as a new asset | Rule 2b: new `cloud_id`, new `photo_id`. The old tombstoned record proceeds through normal hard-delete on schedule; the new record begins fresh. Blob refcount handles the byte-level overlap (single blob, refcount 2 during overlap, refcount 1 after the tombstone expires). |
| Retention window changed (config edited from 30 to 60 days) | Cleanup job uses the new value on its next run. Photos already past the old window are not retroactively hard-deleted by re-extending; they may have already been processed. |
| Unmapped tombstone (`library_id IS NULL`, scope never bound) | Hard-deleted after a fixed 30-day default (no per-library config exists for it); degenerates to row deletion + log — an unmapped photo holds no bytes. |
| Tombstoned photo with a superseded-pending row (re-edit reopened, never refetched) | The row's retained `content_hash` still holds a refcount and is decremented at hard delete — same hash-gate rule as the revert path. |

### Why this matches RFC-011

The daemon's deletion model is intentionally identical in shape to `photos.md`'s soft-delete approach: tombstone on the `photos` row, retain referenced data through a refcount-style check, hard-delete after a fixed retention window. The mapping is direct:

| Daemon | RFC-011 consensus |
|---|---|
| `state.db.photos.deleted_at` | `photos.deleted_at` |
| (not stored) | `photos.deleted_by` — assigned from the publishing user when tombstone propagation lands |
| `state.db.blobs.ref_count` | `DataBlockReferenceProvider` check on `photo_resources` |
| Periodic cleanup of tombstones past retention | RFC-011's cleanup job for expired tombstones |

The 30-day window is the same value in both layers, so when tombstone propagation to the mesh lands (§Propagation to the mesh), in-flight tombstones carry over as-is.

## Failure Handling

The spec constrains failure *semantics* — what state each failure class may and may not leave behind. Detection mechanics, timeout values, and backoff parameters are implementation details.

| Failure | Semantics |
|---|---|
| Spool disk pressure (reserve floor breached) | Daemon-wide admission pause (`storage_low`); inflight writes finish, pending rows are untouched, no retry counts are consumed. Resumes with `storage_recovered` once eviction (or the user) frees space. |
| iCloud fetch failure | Per-resource exponential backoff via `retry_count` / `next_retry_at`. After a configurable retry cap, the resource goes terminally pending with a `resource_gave_up` event. Terminal resources are automatically re-enqueued by the next reconciliation scan — transient iCloud outages self-heal without operator action. |
| Local disk pressure (`CloudPhotoLibraryErrorDomain` code 1005) | `cloudphotod` refuses downloads below a local-headroom threshold (spike-verified: instant failure, no network attempt; resolves when space is freed). Classified as a daemon-wide pause like `storage_low`: fetch admission stops, no retry counts are consumed, admission resumes when headroom recovers. Treating it as a per-resource failure would spin the retry budget uselessly. |
| Partial blob write (crash mid-stream) | Covered by the crash-window table in the Ingest Pipeline: orphan `.partial` temps are swept at startup; a renamed-but-uncommitted blob is reconciled by the orphan scan. No committed row ever references unverified bytes. |
| HopNet node unreachable | The publish pass **parks** (connect/timeout/503 shedding consume no attempts); observation and ingest continue untouched. Reachability edges log once (`node_unreachable`/`node_regained`). Responsibility loss parks separately (`parked_responsibility`) with its own edge events. |
| PhotoKit authorization revoked | Hard stall: the daemon stops all PhotoKit interaction, logs a loud event, and the CLI surfaces the condition. No state is modified — in particular, an empty enumeration due to lost authorization must not be interpreted as mass deletion. |
| Local `state.db` corruption | Disaster case; wipe and re-ingest — PhotoKit re-delivers the library and the mesh resolve pre-pass adopts everything already committed, so nothing re-uploads (see Recovery). |

The PhotoKit-authorization row deserves emphasis as a general rule: **absence of evidence from PhotoKit is only evidence of deletion when the API is healthy.** Any scan that could synthesize deletion events must verify library authorization and non-empty enumeration sanity before tombstoning anything.

Durability boundaries: spool writes fsync before rename (`F_FULLFSYNC` on macOS — plain fsync does not defeat the drive's write cache); once a publish is consensus-decided, durability is the mesh's job and the local copy is disposable by design.

## Recovery

Recovery is tiered: the daemon repairs benign inconsistencies automatically, audits on demand, and treats a lost `state.db` as a **re-ingest, not a restore** — HopNet holds the archive, and PhotoKit plus the mesh's adoption path rebuild the daemon's world from scratch. The daemon never decides on its own that `state.db` is disposable.

### Tier 1 — automatic startup reconciliation

On every start:

1. Sweep all `.partial` temp files (nothing ever references them).
2. If the previous shutdown was unclean (stale pid/lock in `run/`): recount `blobs.ref_count` from the JOIN through `photos`/`photo_resources`, diff against stored values, repair and log any drift.

Startup repair never deletes spool files — orphan deletion is deliberately excluded from the automatic tier. The recount also never touches `evicted_at`: it recomputes counts from resource **rows**, and an evicted blob's rows still exist, so repair and eviction cannot fight (this is why eviction is a stamp rather than a decrement — see the `blobs` notes).

*Implementation notes (Phase 5):* the `drain.lock` file is pid-stamped. A starting process finding a lock held by a dead pid (or an empty/unparseable file) reclaims it and treats the start as unclean, running the recount before any work is admitted — repaired counts gate the irreversible file deletes. A live-pid lock is a hard error. Repair is row-level only: count mismatches are updated, zero-recount `blobs` rows deleted (the file becomes fsck's benign orphan class), missing rows inserted from a referencing resource row (file existence deliberately unchecked — a missing file is fsck's loud byte-loss class). Rows are counted by `content_hash` regardless of `written_at` (superseded-pending rows still reference their old blob). One `refcount_repaired` event per run, drift only.

### Tier 2 — `ingress-cli fsck`

On-demand invariant audit across `state.db` and the spool, in one spool-wide blob-tree walk:

- Recount refcounts (as tier 1) and report drift.
- **Missing spool files**: a **live** (unevicted) `blobs` row must have its file on disk. A miss means byte loss (or manual tampering) and is reported loudly — not repairable from local state; the resource must be re-fetched from PhotoKit if the asset still exists there. An **evicted** row with no file is the normal post-publish state and is clean; an evicted row whose file *lingers* (crash between stamp and unlink) is a benign orphan.
- **Orphan spool files**: files with no live row — the rename-before-commit crash window, a crashed hard delete, or a stamp-then-unlink eviction crash. Deleted only under `--repair` — this is the one destructive repair, which is why it lives here and not in tier 1.
- Ext-mismatched and foreign (unparseable-name / wrong-depth) files are reported but never deleted — the delete gate is exact-match orphans only.

*Implementation notes:* the default run is **read-only** (read-only pool, no lock, logs nothing) so it can audit beside a live daemon; a live `drain.lock` prints an in-flight-work banner since transient states (a renamed-but-uncommitted blob) read as findings. `--repair` takes the exclusive run lock, applies the refcount repair, deletes exact-match orphans (logged as `fsck_orphans_deleted`), and runs Tier-1 first on an unclean reclaim — the reclaim signal must never be swallowed. Exit codes follow fsck(8): 0 clean, 1 findings remain, 2 operational error. A read-only SQLite connection cannot run WAL recovery, so read paths can fail against a hot `-wal` left by a crashed daemon (`SQLITE_READONLY_RECOVERY`); when no live pid holds `drain.lock`, the CLI falls back to a normal read-write open (safe: no live writer, migrations are a no-op) with a printed note.

### Tier 3 — re-scan and adoption

There is no restore tool, no state snapshot, and no archive-tree rebuild. A dead Mac or lost `state.db` is recovered by re-ingesting:

1. Enable the daemon on the (new) machine; `ensure_personal_library` recreates the library row, and the keychain re-provisions via the enable route.
2. The first reconciliation scan re-enumerates PhotoKit and mints fresh rows; materialization re-streams from iCloud into the spool as ordinary pipeline work.
3. The publish pass's **resolve pre-pass** asks the node, per `cloud_id`, whether the mesh already holds each asset (keyed cloud fingerprint — RFC-011 §Cloud Fingerprint). Everything already committed is **adopted**: stamped `published_at` + `consensus_photo_id` with zero uploads. A full recovery re-uploads only what the mesh never received, and adopted photos evict on the same pass — the recovery's spool footprint is as transient as steady-state ingest.

Lost with the old `state.db`: retry state and the ingest log (both disposable) and the daemon-minted `photo_id`s for never-published photos (their mesh identities, where they exist, come back via adoption). This replaces the archive era's snapshot-restore and sidecar-tree-rebuild machinery outright — the mesh, not the daemon, is the durable record, and the verification step is the same reconciliation scan the daemon runs every startup anyway.

## Implementation Phases

Structure: a Swift executable (the LaunchAgent) linking `ingress-core` (Rust, in the HopNet monorepo at `crates/ingress-core/`) as a static library via UniFFI. The tokio runtime lives on the Rust side; Swift pushes `AssetDescriptor`s in, Rust calls back out for resource byte streams. The CLI is a pure-Rust binary reading `state.db` directly — no PhotoKit dependency. Chunk streaming across the FFI boundary uses large (~1 MB) buffers, not per-read chunks.

### Phase 0: PhotoKit Spike [x] — see `spikes/photokit/FINDINGS.md`
- Throwaway Swift script against a real library; falsifies spec assumptions before the architecture hardens
- Batch `PHCloudIdentifier` mapping at full-library scale (performance unknown)
- Shared-library scope property detection
- `fileSize` KVC attribute availability (documented-behavior check for the undocumented key)
- Stream one iCloud-remote original via `PHAssetResourceManager` (network fetch, chunk delivery)
- TCC / Photos authorization flow for a daemon-shaped process

### Phase 1: ingress-core Skeleton [x] — `crates/ingress-core/` (standalone workspace; sqlx 0.9 pilot)
- Schema DDL + migrations for `state.db` (all five tables)
- State store and match-precedence engine (identity resolution rules 1, 2a–2c)
- Sidecar serialization
- Unit tests against fixture `AssetDescriptor`s — no Mac, no PhotoKit

### Phase 2: Bridge and Vertical Slice [x] — `crates/ingress-ffi/`, `apple/PhotoIngress/`
- UniFFI bindings: descriptor ingestion, chunk streaming (blocking session API, Rust-owned runtime)
- Blob write path in ingress-core: streaming writer, dedup-hit discard, crash-window-safe finalize
- One real asset end-to-end: fetch → hash → temp → rename → sidecar → committed rows (verified against a Shared Photo Library Live Photo, b3sum-exact)
- The resource-fetch callback interface moved to Phase 3 — the scheduler's concurrency model shapes that trait's signature

### Phase 3: Pipeline [x] — `crates/ingress-core/src/scheduler/`, seed/drain subcommands
- Scheduler with a bounded fetch pool (single knob: writes structurally ride the fetch path — sync sink writes are the backpressure; a write pool would mean local spooling, which §Write path rejects)
- Resource-fetch foreign trait (`PhotoResourceFetcher`: `descriptor_for` + `fetch_resource` into a scheduler-owned sink; blocking, spawn_blocking under a semaphore)
- Retry/backoff (exponential, per-resource columns), terminal give-up at a flag-configured cap (`retry_count >= cap`, still pending; raising the cap on a later run revives gave-up rows)
- Storage-aware admission (statvfs vs reserve floor + inflight expected sizes; descriptor `fileSize` with a max-blob pessimistic fallback). 1005 = daemon-wide pause with timed re-probe (cloudphotod's threshold is unobservable)
- SIGTERM-clean cancellation (verified live against a mid-download 1.6 GB video; PhotoKit's `cancelDataRequest` does NOT reliably fire completion handlers — the Swift fetcher poll-waits and abandons on cancel), startup `.partial` sweep, exclusive `drain.lock`
- Unmapped photos adopt their library on re-delivery (rule-1 branch); pending rows become drain-eligible
- Seed mints photo + pending resource rows at discovery (aligns with §Discovery); rule 2a consequently runs at drain time as a **late-binding merge** — the existing cloud_id-less photo survives (keeps photo_id + blobs), the seed-minted provisional row is deleted before anything on disk references it (original streams first), logged as `late_binding_merge`
- Drain-time descriptors come fresh from `descriptor_for` per admitted photo (never persisted; serves sidecar fields, ext derivation, and admission sizes)

### Phase 4: Discovery [x] — `classify.rs`, `scan.rs`, `transition.rs`, `scheduler/daemon.rs`, `daemon` subcommand
- Change classification: pure planner (`plan_changes`) + applier (`apply_change`) covering all five kinds; observer events and scan re-deliveries funnel through the same idempotent path. NoOp is the hot path (redundant PhotoKit delivery). Original-class rows are never removed by a diff (`original_disappeared` invariant event).
- Tombstone/restore pulled forward from Phase 5 (deletion synthesis needs them): guarded single-tx updates + `deletion_observed`/`restore_observed`; sidecar `deleted_at` via read-modify-write (see §Tombstone step 5).
- Re-edit = fileSize compare → superseded-pending reopen; refcount swap commits with the replacement bytes (see §Per-resource state machine amendment). Revert deletes rows + reaps blobs (row deleted at refcount 0 — a lingering file is the benign fsck class, a lingering row is the loud one).
- Hard moves per §Asset migrating, copy-before-tx (see errata there); sidecar relocates between library roots; pending resources ride along logically and fetch into the dst root.
- Reconciliation scan: light-probe protocol (`scan_asset` — identity/scope/date only, NO per-asset resource enumeration; full descriptors only for probe misses). Seen-marking at probe time so deletion synthesis is immune to event-queue lag. Health guard: zero enumeration vs non-empty store skips synthesis (lost TCC must never mass-tombstone). Gave-up retry state resets at scan close (spec §Failure Handling).
- Daemon loop: drain that never exits; Swift pushes observer events (unbounded channel + wake). Events for inflight photos DEFER into a per-photo FIFO and those photos are not admitted — structural prevention of the finalize-vs-move race (verified by a barrier-pinned test). `CancelToken` gained an async `cancelled()` waker — a live SIGTERM test caught the idle `select!` sleeping through cancellation.
- AssetUnavailable is NOT inline-reclassified as deletion (local_id ≠ identity; API-health rule) — observer removals + scan synthesis are the two deletion detectors.
- Verified on-device: cold-start daemon scan seeds+drains 25 assets (21 personal / 4 shared, b3sum-exact); live favorite → metadata-only sidecar refresh, zero fetches; idempotent restart rescan (probed=25, needed_full=0); SIGTERM → prompt report + lock release. EXIF pass (orientation/camera/UTC offset) deliberately deferred.

### Phase 5: Lifecycle [x] — `cleanup.rs`, `recovery.rs`, `runlock.rs`, `cleanup` subcommand
- Hourly cleanup job, integrated into the daemon loop (serialized with event application; photo_tasks unaffected) plus a one-shot `photo-ingress cleanup` subcommand sharing the exclusive run lock (no PhotoKit — runs without authorization). Hard deletes per the 7-step procedure with the in-tx `hard_delete` errata (see §Hard-delete cleanup); per-library retention read fresh per run; unmapped tombstones on the 30-day default; batch-capped. Log pruning (180d). Daily snapshots via `VACUUM INTO` (see §State snapshots errata) to `<blob_root>/state-snapshots/`, keep 7, filename-derived due-check.
- `sidecar_replicated_at` dirty-set drain: every sidecar-rewrite trigger NULLs the flag in the same transaction as its state change (completion, metadata refresh, revert/resource-change, tombstone, restore, hard move); the drain runs on its own faster cadence (default 60s, batch 500), skips photos with a live fetch task (stamp-vs-rewrite race), and edge-logs `mount_lost`/`mount_regained` (per-process edge: the standalone subcommand re-logs per run).
- Tier-1 startup repair: pid-stamped `drain.lock`, dead-pid reclaim = unclean start → refcount recount/repair before any work (see §Recovery Tier 1 notes). Reused by Phase 6 fsck.
- Hard-move spec-gap fix: the source library's REMOTE sidecar copy is removed at transition (see §Asset migrating note).
- Drive-by fix: the revert path's blob decrement now gates on `content_hash` (not `written_at`) — a revert landing while a re-edit sat superseded-pending leaked the old blob's refcount and stranded its file forever.
- Verified on-device: 5-asset seed/drain → remote sidecars replicated + stamped; retention-0 hard delete removes rows, blob file, local AND remote sidecars with the in-tx log; mount break/restore → stall flag + `mount_lost`, drain resumes on recovery; daily snapshot once (same-day rerun no-op, snapshot opens as a valid db); `kill -9` → stale lock reclaimed by the next run; `cleanup` vs a live daemon → refused with the holder's pid. 130 Rust tests.

### Phase 6: CLI [x] — `crates/ingress-cli/`, core modules `status.rs`/`fsck.rs`/`recover.rs`/`libconfig.rs`
- Pure-Rust `ingress-cli` binary (clap), no PhotoKit; all logic in ingress-core modules (testable without the binary, `cleanup::run_standalone` pattern). `--data-dir` defaults to the canonical `~/.local/share/hopnet-photo-ingress`. Human tables by default, `--json` on `status`/`fsck`/`library list`. Exit codes: 0 clean, 1 fsck findings remain, 2 error.
- `status` — per-library counters (active/pending/tombstones/dirty-sidecars/blob count+bytes), pipeline posture (fresh vs awaiting-retry vs gave-up at `--retry-cap`, unmapped count, earliest retry), and `status <photo_id|cloud_id>` drill-down (row state, per-resource blob paths with existence bits, sidecar path, newest-first log tail). Reads through a new `StateStore::open_read_only` (WAL concurrent reader; schema-version guard; see the read-only caveat under §State snapshots).
- `fsck` per §Tier 2 with its Phase 6 notes: read-only default + `--deep` + `--repair`; `recovery.rs` split into a pure recount/diff half (shared with the default run) and the repairing half (`repair_refcounts`, API unchanged).
- `recover` per §Tier 3 with its Phase 6 notes: snapshot-first across `--root`s, sidecar-tree rebuild from `--library` specs, local-sidecar hydration, `--force` aside-move, per-root inventory error; blob-only deferred.
- Library config moved wholesale from Swift `setup` to `ingress-cli library add/list/bind/rename/set-retention`: generated immutable two-word ids (see §libraries errata), single-personal + single-shared-marker invariants enforced with friendly errors, unbind-ambiguity guard, no-remote-backup warning relocated from setup, all writes lock-guarded + Tier-1-on-unclean + logged (`library_added`/`library_bound`/`library_renamed`/`retention_changed`). FFI `add_library` removed (bindings regenerated); `setup` = data dir + Photos authorization only.
- Verified on-device (scratch env, 5 real assets): generated-id adds, seed/drain, status views against live rows, replication then `fsck --deep` clean, planted orphan + deleted blob → exit 1 with BYTE LOSS banner, `--repair` deletes only the orphan, snapshot recover into a fresh data dir (5 photos + hydration), sidecar-tree recover catching the destroyed blob in verification, `--force` interlock, live-daemon coexistence (read-only status, fsck banner, config writes refused with holder pid). 139 Rust tests across the workspace.

### Phase 7: Hardening [~]
- LaunchAgent packaging, login autostart [x] (2026-07-31) — daemon bundled into HopNet.app (`Contents/MacOS/photo-ingress`, signed individually with hardened runtime + the photos-library entitlement, no profile) with the SMAppService agent plist at `Contents/Library/LaunchAgents/com.hopnet.desktop.photo-ingress.plist` (build stages 1b/3b); bundle id moved `app.hopnet.photo-ingress` → `com.hopnet.desktop.photo-ingress` (nested-code convention; TCC grants re-prompt once). Owner-only `/api/photo-ingress/{enable,disable,status}` routes on the GUI node provision keychain (token + blob_root) and drive registration; the daemon self-defaults `--data-dir`, owns its log (`--log-to-data-dir`), and auto-binds the personal library at startup via the ensure-only `ensure_personal_library` FFI (narrow reversal of Phase 6's FFI removal — the GUI can't link ingress-core across the workspace split). Stale node URL after a GUI relaunch heals via `RefreshingPublisher` (keychain re-read only after an unreachable pass). Enablement-gated minting: setup no longer provisions, login only self-heals. Smoke finding: the main app must be UNSANDBOXED — `SMAppService.agent` registration is EPERM from a sandboxed process (only `loginItem` is sandbox-compatible); `entitlements-app.plist` documents this. Live smoke (2026-08-01) verified the full enable → TCC consent → migrate → auto-bind → disable → re-enable cycle against the production `state.db` (migration `1785600000 consensus adoption`, AlreadyExists auto-bind with blob-root mismatch warning, SIGTERM-clean shutdown, zero publishes — explicit-claim parking held). Two more findings baked into the pipeline: (1) TCC attributes the bundled daemon to its CONTAINING bundle, and under the hardened runtime tccd denies Photos without prompting unless the attributed identity — the main app — carries `com.apple.security.personal-information.photos-library` (both entitlement files now have it; stage 3b stamps the matching `NSPhotoLibraryUsageDescription` onto the app's Info.plist; the grant is keyed on `com.hopnet.desktop`, so daemon re-signs don't reset it). (2) A stale BTM record from the earlier sandboxed registration attempts poisons all later registrations with EPERM even after unsandboxing — invisible in Login Items, fixed only by `sfltool resetbtm` + reboot (documented here because any tester who ran a sandboxed build hits it). The route orchestration now lives in platform-independent `photo_ingress::flow` behind a `ProvisioningDeps` seam — Linux CI pins the sequencing invariants (keychain-before-register, device-id-capture-before-wipe, owner gate, best-effort disable) with mock tests; macOS `routes.rs` is axum glue plus the real SMAppService/keychain/consensus impl.
- Long-run soak against a full-size library — shared library (10,140 photos / 170 GiB) drained clean end-to-end; personal (~26k) in progress
- Mount-loss / storage-low behavior under real SMB conditions [x] — vanished blob root (`statvfs` ENOENT/ENOTDIR) now maps to `StorageUnavailable` and routes into the `storage_low` pause-and-poll path instead of burning per-resource retries (one overnight unmount had driven 5,961 resources to gave-up). See §Storage-aware admission errata.

### Phase 8: Interim viewer [x] — retired (crate deleted in the Phase 9 transplant; browsing is HopNet's gallery)
- Freestanding read-only web viewer on the storage host: sidecar-tree index (own SQLite, incremental) + Renderer (libheif HEIC→JPEG, ffmpeg video poster, `image` for JPG/PNG; RAW download-only) + Axum REST + pocket-id OIDC session; forked Svelte frontend (virtualized photo grid + lightbox), reusing HopNet primitives/tokens.
- Decode spike [x]: `libheif-rs 1.1` (real HEIC→JPEG) and `ffmpeg-next 7.1` (HEVC `.mov` poster) validated under Nix. Pin `ffmpeg_7` (ffmpeg 8 drops `avfft.h`, breaks `ffmpeg-sys-next`).
- Backend [x]: sidecar-tree index (keyset paging, mtime-incremental refresh, membership sweep); pocket-id OIDC (auth-code+PKCE, server-side `tower-sessions` cookie) with per-library group authorization (`access.groups` ∩ token `groups`); REST (`/api/libraries`, `/api/photos`, `/api/photos/{id}`, `/photos/histogram` month buckets, `/thumb`, `/display`, `/resource/{type}` range passthrough); tri-state browse filters (`video`/`live`/`raw`/`favorite`, absent=any true=only false=exclude; `raw` via EXISTS on `raw_alternate` resource) shared by list + histogram; Renderer with content-hash JPEG cache. TLS forced to rustls/ring (no openssl/aws-lc). Live OIDC smoke passed against real pocket-id; render validated on real HEIC + HEVC. Needs C-lib devshell (`shell.nix`).
- Frontend [x]: Svelte grid (uniform square cells, true aspect ratio, keyset infinite scroll — visibility-effect load loop + Load More fallback) + lightbox + filter dropdown (media checkmarks + tri-state subfilters) + library dropdown (one-or-many selection; multi fuses into one timeline via CSV `library` param → `IN` clause, cursor already a global total order; shared-library assets badged in fused view; per-library photo/video count breakdown via `COUNT(*) FILTER`) + month histogram rail (hover-expand, resizes grid) + pointer-following hover preview (thumb layer upgraded in place to display rendition); Storybook mocks for all presentational components. Auth UX: any 401 (boot or mid-session expiry) shows a login page (HopNet `SetupPane` card + logo, single button → `/auth/login` OIDC hand-off) instead of hard-redirecting. `LibraryEntry.shared` config flag marks shared libraries.
- Windowed browse [x]: `/api/photos` gains `dir=older|newer` (keyset comparison + scan order flip together; `newer` pages ASC then reverses, so items are always newest-first) and `PhotoSummary.sort_ms` so the client synthesizes continuation cursors from its window edges. Frontend grid is a sliding window (cap 600, evicts the far end with measured scroll-height compensation, `overflow-anchor: none`); histogram rail is scroll-synced (current month highlighted via sticky day headers) and click-jumps re-anchor the window at the month boundary — scrolling both directions from there. Rail hover zooms the grid out via transform (constant layout width → no reflow, no scroll loss).
- Deployed [x]: on thor via nix-config (`photo.bentley.sh`, agenix OIDC secret, SPA embedded in the binary via `include_dir!`).

### Phase 9: HopNet transplant [x] — archive → transient spool (2026-08)
- Descriptor capsule (`photos.descriptor_json`) replaces sidecar files as the publish metadata source; publish composes the document per pass (`Sidecar::compose`); NULL capsules backfill via the reconciliation scan
- Sidecar layer deleted wholesale: writer/walker/heal, remote replication + `sidecar_replicated_at`, fsck sidecar checks, tombstone/restore/move document rewrites
- Spool eviction on decided publish: `blobs.evicted_at` stamp + file unlink under the spool-wide hash-liveness gate; rides the publish pass and the cleanup tick; a re-fetch of an evicted hash re-places bytes and clears the stamp
- Structural spool: single content-addressed tree at `<data_dir>/spool/blobs/`, per-library storage roots dropped from `libraries` (table rebuild), admission re-pointed at the data-dir disk, library transitions ledger-only
- Provisioning shrank to the device token: zero-arg `ensure_personal_library`, enable route takes no body, `blob_root`/`sidecar_root_remote` purged from FFI, keychain, status, and CLI (legacy keychain accounts wiped on disable)
- Demolition: `crates/ingress-server` (interim viewer), state snapshots, Tier-3 `recover`, mount-loss pause class, `tests/recover.rs`
- Cutover is a fresh iCloud re-pull plus adoption, not a data migration; the old NAS archive stays as an untouched cold copy

## Changelog

- **2026-08 — spool transplant.** Reversed two founding decisions. (1) *"No local spooling"*: the original write path rejected a local staging tier because user-owned network storage was the destination; with HopNet as the archive of record, the local spool **is** the staging tier — bounded by admission, drained by eviction on consensus-decided publish. (2) *"User-owned storage roots are the archive"*: the per-library blob roots, sidecar trees, remote replication, snapshots, and the recover/viewer machinery all existed to make an intermediate NAS archive durable, and were deleted once consensus-committed storage subsumed that role. Phase records 0–8 describe the pre-transplant design; where they conflict with the body text, the body text is current.