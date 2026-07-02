# Apple Photos Ingress

## Summary

A native ingress daemon that reconciles an Apple Photos library — including iCloud Photos and iCloud Shared Photo Library content — into a content-addressed blob store on user-owned storage. The first cut targets macOS, but the design is structured around primitives that map cleanly to iOS (`PhotoKit`, `PHCloudIdentifier`, `PHAssetResource`) so a future delta-upload daemon on iOS can share the same state model and resume the same dedup contract.

State (asset-to-blob mappings, ingest pipeline status, library partitioning) lives in a local SQLite database on the ingesting device. Materialized photo bytes are written to a configurable storage root — typically a network share backed by a user-controlled server — partitioned by library membership (personal vs. shared). The PhotoKit-derived metadata needed to rebuild ingest state and populate the future consensus metadata model is preserved in per-photo sidecar JSON, sized for forward portability into the full HopNet photos system described in `photos.md`.

This spec is **interim**. It exists because the full HopNet photos system is a multi-quarter effort, and a reliable off-iCloud copy of photo libraries are needed in advance of that work. The state and on-disk structures defined here are designed to migrate cleanly into the consensus model when it lands; nothing captured by this daemon should need to be re-derived during migration.

## Motivation

### Why ingress, why now

- **Threat model**: Apple Advanced Data Protection is under sustained legislative pressure in multiple jurisdictions. iCloud is not a reliable long-term home for an encrypted-only-to-Apple photo library, and any forced-disclosure regime that compels Apple to disable ADP would silently demote that library to provider-readable.
- **Capacity**: Mac internal storage often cannot hold the full iCloud Photos library. Materialization must target network-attached storage that the user controls.
- **Forward portability**: When the full RFC-011 photos module lands, the daemon's output must be ingestible as-is — no metadata recomputation, no re-hashing, no manual re-tagging. Asset identity, grouping, resource enumeration, and capture metadata must all survive the migration.
- **Dedup correctness across runs and devices**: Re-running the daemon on the same device, restarting after a crash, or running a future iOS daemon against the same iCloud library must not produce duplicate blobs or duplicate logical photo records.
- **No vendor lock-in on the staging format**: Sidecar JSON + content-addressed blobs is a format that survives the absence of the daemon. If the project is abandoned, the user still has a structured, queryable archive.

### Why a separate daemon, not just a script

PhotoKit's `PHPhotoLibraryChangeObserver` and `PHAssetResourceManager` model assume a long-lived process that holds library access. Reconciliation is not a one-shot import — it is a continuous activity that watches for new captures, edits, deletes, and shared-library membership changes, and a daemon is the right shape for that. A periodic cron job would miss observer-driven change events between runs and would re-enumerate the full library each invocation, which is wasteful on a 50k+ asset library.

### Scope boundaries

This document defines:

- Local SQLite schema on the ingesting device
- On-disk blob and sidecar layout on the storage root
- Library partitioning and routing rules
- Asset discovery, resource enumeration, dedup, and write pipeline
- Failure handling: mount loss, iCloud download failures, partial writes, daemon crashes
- Recovery model: rebuilding state from on-disk blobs and sidecars if the local SQLite is lost

This document explicitly does not define:

- The encryption model for blobs (deferred to FileVault / ZFS native encryption at the MVP stage; per-recipient encryption is in `photos.md`)
- Multi-user sharing of materialized data (also `photos.md`)
- Replication of blobs across multiple storage roots (out of scope; user provides storage redundancy via ZFS, RAID, off-site backup, etc.)
- iOS implementation details (mentioned only where they constrain Mac design choices)

## Architecture Overview

### Components

The daemon is a single long-lived process on macOS, structured as a Swift PhotoKit shim layered over a Rust core. The split is dictated by platform constraints: PhotoKit can only be driven from Swift (or Objective-C), but everything downstream of asset enumeration — hashing, dedup, sidecar serialization, SQLite, storage I/O — is platform-agnostic.

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
│   │     - SQLite state store                                     │   │
│   │     - Sidecar JSON serialization                             │   │
│   │     - Atomic blob writer (over POSIX, including SMB mount)   │   │
│   │     - Pipeline scheduler + retry/backoff                     │   │
│   └──────────────────────────────────────────────────────────────┘   │
│                                                                      │
│   ┌─────────────────────────────────┐                                │
│   │ ingress-cli (status / re-scan)  │  reads state.db via Rust core  │
│   └─────────────────────────────────┘                                │
│                                                                      │
│   Local-only filesystem state:                                       │
│     ~/.local/share/hopnet-photo-ingress/                             │
│       state.db                  (authoritative ingest state)         │
│       sidecars/<library>/...    (hot-path metadata, queried often)   │
│       run/                      (pid, locks)                         │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
                                  │
                                  │  SMB (or any POSIX mount)
                                  ▼
┌─────────────────────── user-controlled storage ──────────────────────┐
│                                                                      │
│   Per-library roots:                                                 │
│     /<root>/<library>/blobs/<aa>/<bb>/<full-blake3>.<ext>            │
│     /<root>/<library>/blobs/.partial/    (in-flight write temps)     │
│     /<root>/<library>/sidecars/<YYYY>/<MM>/<photo_id>.json (backup)  │
│     /<root>/<library>/state-snapshots/state.db.<timestamp>.sqlite3   │
│                                                                      │
│   <ext> is the canonical extension for the resource's UTI            │
│   (e.g. .heic, .jpg, .mov, .dng). Bytes are stored unmodified;       │
│   files remain directly openable in Finder/Preview.                  │
│                                                                      │
│   Server-side encryption: out of scope for this daemon.              │
│   Expected to be provided by FileVault on the Mac side and           │
│   filesystem-level encryption (e.g. ZFS native encryption) on        │
│   the storage side.                                                  │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

### Data flow at a glance

1. PhotoKit emits a change event (or initial enumeration produces a backlog of assets).
2. The Swift layer iterates `PHAsset` records, extracting per-asset identifiers and library scope, and hands each off to the Rust core as a structured `AssetDescriptor`.
3. The Rust core consults `state.db` for prior ingest state, deciding whether the asset is new, already complete, partially complete, or in need of metadata-only update.
4. For new or incomplete assets, the Rust core requests resource bytes from the Swift layer one resource at a time (`original`, `paired_video`, `raw_alternate`, etc.). The Swift layer drives `PHAssetResourceManager` with `isNetworkAccessAllowed=true` to pull originals from iCloud when needed.
5. Bytes stream through a BLAKE3 hasher and into a temp file on the storage root, then are atomically renamed to their final content-addressed path. If the hash matches a blob already present for the target library, the temp file is discarded and the existing blob is reused.
6. After all resources for a photo are written, the Rust core writes the sidecar JSON locally, replicates it to the storage root (best-effort, asynchronous), and commits the final `photos` + `photo_resources` rows in `state.db` in a single transaction.
7. The pipeline advances; the daemon continues observing for further changes.

### Process model

- Single LaunchAgent, label `app.hopnet.photo-ingress`, autostarted on user login.
- One in-process tokio runtime hosting: PhotoKit observer callback, resource fetch workers, hash workers, blob writers, sidecar writers, and the SQLite executor.
- Bounded concurrency: a configurable number of parallel resource fetches and a separate configurable number of parallel blob writes. Defaults are conservative (e.g. 4 fetches, 4 writes) and tuned per network.
- All long-running operations are interruptible via cooperative cancellation; the daemon is expected to be SIGTERM-clean on user logout or system shutdown.

### Why this shape

- **Swift shim, Rust core**: minimum platform-specific surface area. The Rust core is the artifact that survives the move to iOS, the move to a future Linux ingress (e.g. reading from an exported folder), and the eventual fold-in to HopNet's `photos-core` crate described in `photos.md`.
- **SQLite local, blobs remote**: SQLite over a network mount is hazardous (lock semantics differ, fsync semantics differ across SMB/NFS implementations) and is treated as a hard rule. Blobs are large, content-addressed, write-once, and tolerate the higher per-operation latency of a network mount.
- **Per-library on-disk partitioning**: the storage root expects per-library subtrees so that filesystem-level ACLs on the storage side can independently constrain access to personal vs shared content. Cross-library blob sharing via symlink or hardlink is rejected to keep the access-control story simple and auditable.
- **Sidecars duplicated (local hot path + remote backup)**: small JSON, queried frequently during gallery operations and rescans, but also needed in disaster recovery from blob-only state. Both copies are cheap; the storage cost is dominated by blob bytes.

## Asset Identity Model

Identity is the load-bearing concept for ingress. Everything downstream — dedup, change observation, cross-device coordination, sidecar correctness — assumes a clear answer to "is this the same photo as one I've seen before?" The model uses three layered identifiers, each with a distinct purpose.

### The three identifiers

| Identifier | Source | Scope | Stability | Role |
|---|---|---|---|---|
| `cloud_id` | `PHCloudIdentifier` from PhotoKit | iCloud account | Stable across devices and reinstalls for as long as the asset exists in iCloud Photos | Primary dedup key. The lookup answer to "have I seen this asset on any device tied to this iCloud account?" |
| `content_hash` | BLAKE3 of resource bytes | Universal | Stable forever as a property of the bytes | Secondary dedup key for local-only assets, re-imports, and assets that lost their `cloud_id` association. Also blob storage address. |
| `photo_id` | UUIDv7 minted by the daemon at first discovery | Daemon-local (until migration) | Stable for the daemon's logical record, independent of any external identifier | Internal primary key. The identifier the daemon, sidecars, and `state.db` all agree on. Will become the consensus `photos.id` if migrated to RFC-011 without remapping. |

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
- The daemon being uninstalled and reinstalled, provided `state.db` survives (recovery from blob-only state may mint fresh `photo_id`s; see the Recovery section)

### Cross-device convergence

When a future iOS daemon, or a second Mac, ingests the same iCloud library, the goal is that the two daemons produce **compatible** records — not bit-identical state, but state that can be merged without conflict during the RFC-011 consensus migration.

- `cloud_id`s match by construction (Apple's invariant).
- `content_hash`es match by construction (BLAKE3 is deterministic).
- `photo_id`s do **not** match — each daemon mints its own UUIDv7 at first discovery. This is acceptable because `photo_id` is daemon-internal until consensus migration, at which point the migrator resolves the device-to-device `photo_id` discrepancy by joining on `cloud_id` (primary) and `content_hash` (fallback) to identify pairs and pick a canonical id.

This trade-off — distinct local `photo_id`s, deterministic `cloud_id` / `content_hash` — is deliberate. Forcing daemons to share `photo_id`s pre-consensus would require an online coordination primitive (some daemon must "own" first-discovery), which the MVP explicitly does not have. The migrator handles the merge at the moment cross-device coordination becomes available.

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
| Same content in personal and shared library scopes simultaneously | Two distinct `PHAsset`s with distinct `cloud_id`s. Two `photo_id`s. Two `photos` rows in different libraries. Storage-layer dedup (ZFS or refcount) handles byte sharing. |
| Burst frames | Distinct `photo_id`s, distinct `cloud_id`s per frame. Shared `group_id` derived from `burstIdentifier`. One frame marked `is_group_pick = 1` per PhotoKit's "user pick" hint. |
| Live Photo | Single `photo_id`. Two `photo_resources` rows: `original` (HEIC still) and `paired_video` (MOV). |
| RAW + JPEG paired capture | Single `photo_id`. Two `photo_resources` rows: `original` (typically JPEG, the user-visible representation) and `raw_alternate` (the RAW companion). |

## Library Partitioning

Photos are partitioned at the top level by **library** — a logical bucket corresponding to an access-control boundary. The two libraries this MVP targets are:

- `personal` — photos in the user's personal iCloud Photos library, visible only to that account.
- `shared` — photos in an iCloud Shared Photo Library, visible to all participants of that shared library.

Additional libraries can be defined (for example a second shared library, or a non-iCloud "imported" library populated from manual file drops), but the data model treats each as a distinct partition with its own storage subtree and its own dedup namespace.

### Why partition at all

The on-disk storage root is expected to be a multi-user server share where filesystem-level ACLs constrain access. Personal photos sit under a path readable only by the owning user account on the server; shared photos sit under a path readable by all shared-library participants. Mixing the two in a single content-addressed pool would either require every shared participant to be granted read access to every personal photo (unacceptable) or require some cryptographic per-blob access mechanism, which is exactly the ceremony this MVP defers.

Partitioning by library makes the access story trivial: server-side ACLs on the per-library subtree are the access story. The daemon never needs to reason about who can read what — it routes bytes to the correct subtree and stops there.

### Library scope detection

Spike-verified reality (see `spikes/photokit/FINDINGS.md`): PhotoKit on macOS has **no public per-asset indicator** of iCloud Shared Photo Library membership. SPL assets appear in default fetches reporting `sourceType = typeUserLibrary`, indistinguishable from personal assets via documented API. The public `typeCloudShared` source type identifies only legacy iCloud Shared Albums, which are excluded from ingest scope entirely (downscaled copies, not part of the library proper — and conveniently absent from default fetches).

Detection therefore uses the undocumented KVC-readable `PHAsset` property `participatesInLibraryScope` (Bool) — verified exact against a 36k-asset library with a 10k-asset shared library. This is a private-API dependency of the same tier as the `fileSize` key used by storage-aware admission. Failure mode is specified: if the key returns nil (removed in a future macOS), the daemon treats it as a **hard error and stops ingest** — it must never default to personal, which would silently route shared photos into the personal library subtree and violate the partitioning ACL story.

The Swift layer reads this property when constructing the `AssetDescriptor` and propagates it as an enum: `Personal` or `Shared`. iCloud supports at most one Shared Photo Library per account and PhotoKit exposes no scope identifier, so the signal is binary — the shared library's `scope_binding` is a fixed marker value (`icloud-shared-library`) rather than a PhotoKit-provided identifier. The `libraries` schema retains the general scope-binding shape for future non-PhotoKit library sources.

### Routing rule

On every asset discovery — including change-observer events for previously-seen assets — the Rust core consults the descriptor's library scope and:

1. Resolves the scope identifier to a configured `library_id` in `state.db`.
2. If no `library_id` is configured for that scope, the asset is recorded with a special `library_unmapped` sentinel and a soft error is emitted; the daemon logs a CLI prompt inviting the user to configure the library before ingest can proceed for that asset.
3. Otherwise, all subsequent operations (blob path resolution, sidecar path resolution, dedup queries, refcount adjustments) use the resolved `library_id`.

### Asset migrating between libraries

PhotoKit supports moving an asset from the personal library into a shared library and vice versa. When this happens, the daemon observes a change event for the asset and the previously-recorded `library_id` for the asset's `cloud_id` no longer matches the current PhotoKit scope.

The daemon treats a library transition as a **hard move**. The `photo_id` is retained, but bytes are physically relocated so that the photo's full state lives entirely under the destination library's subtree:

```
For each photo_resources row R of the transitioning photo:
  1. Resolve src_path = <src_blob_root>/<aa>/<bb>/<hash>.<ext>
  2. Resolve dst_path = <dst_blob_root>/<aa>/<bb>/<hash>.<ext>
  3. Increment refcount on (dst_library_id, content_hash) in blobs;
     if previous refcount was 0, copy src_path → dst_path
     (write to .partial temp, fsync, rename).
  4. Decrement refcount on (src_library_id, content_hash) in blobs;
     if refcount reaches 0, delete src_path.
  5. Update photos.library_id = dst_library_id.
  6. Update sidecar's library_id field and rewrite the sidecar JSON
     (local copy and remote backup).
  7. Record a library_transition entry in the ingest log with both ids.
```

`photo_resources` rows stay keyed by `(photo_id, resource_type)` and need no library-aware updates; the blob path is reconstructed from `photos.library_id` and `content_hash` at read time.

Step 3's refcount check matters: if another photo in the destination library already shares this blob (for example the user previously imported the same content into the shared library independently), the bytes are already on disk and the copy is skipped — only the refcount increments.

Step 4's refcount check matters symmetrically: if another photo in the source library still references the blob (unlikely but possible), the file is not deleted; only the refcount decrements.

The refcount updates (steps 3 and 4) and the `photos` row update (step 5) run inside a single SQLite transaction. The filesystem operations (copy, delete) are bracketed by but not part of that transaction. If a copy or delete fails mid-way, the daemon recovers using the rules described in the Recovery section: refcounts in `state.db` are the authoritative reference state, and the on-disk presence of a blob is reconciled against them on startup.

*Implementation errata (Phase 4):* all copies run **before** the transaction, not interleaved per step 3's numbering. A dst refcount committed ahead of its copy could reference bytes that never arrived — which fsck classifies as byte loss — whereas copy-first leaves only benign orphan files on a crash, consistent with the write path's durability-precedes-commit invariant. Source-file deletes still run after the transaction.

#### Why hard move, not soft

An earlier draft considered leaving blobs in their original library's subtree after a transition and updating only the logical `library_id`. That approach was rejected: it would leave a `shared`-scope photo with bytes physically located under `/<root>/personal/blobs/`, which the personal-library-only ACL would correctly deny to shared-library participants. The photo's access state would become incoherent. Hard move keeps the invariant that **a photo's bytes are always under its current `library_id`'s subtree**, and this invariant is what makes the server-side ACL story work without app-level coordination.

### Storage root configuration

A library is fully described by these pieces of configuration, persisted in `state.db`:

- `library_id` — short, stable, human-readable identifier (`personal`, `shared_household`, etc.). Used as the path component on disk and as the foreign key on `photos` and `blobs`.
- `display_name` — UI string for the CLI.
- `blob_root` — absolute path on the ingesting device's filesystem to the per-library subtree on the storage root. Typically a path under a mounted network share (for example `/Volumes/photos-personal`).
- `sidecar_root_local` — derived path under `~/.local/share/hopnet-photo-ingress/sidecars/<library_id>/`; hot-path location for sidecars.
- `sidecar_root_remote` — optional path under the storage root for the periodic sidecar backup. Often a sibling of `blob_root`.
- `scope_binding` — for shared libraries, the PhotoKit scope identifier this `library_id` is bound to. Personal libraries have no scope binding.

The configuration is editable via the CLI but is not edited by the daemon itself — changing where bytes are written is a deliberate user action that requires the daemon to be stopped, the configuration to be edited, and (if the new root differs from the old) a one-time migration to be run.

The Swift layer is told which PhotoKit scope identifier maps to which `library_id` via the `scope_binding` value. This decoupling lets the user rename a `library_id` without breaking the PhotoKit binding, and lets the user opt out of shared-library ingest entirely by simply not binding its scope.

### Dedup namespace per library

All dedup logic — both `cloud_id` lookups and `content_hash` lookups — is scoped to a single `library_id`. A blob with hash `H` in `personal` is a distinct row in `blobs` from a blob with the same hash in `shared`. This follows from the access-control argument: the two blobs must live on physically separate storage subtrees with separate ACLs, so the application layer cannot share their identity.

Practical consequence: a photo that exists in both `personal` and `shared` — two distinct `PHAsset`s with distinct `cloud_id`s but byte-identical originals — results in two `photos` rows (one per `library_id`, each with its own `photo_id`), two writes of the same bytes to two different subtrees, and one stored copy on disk if ZFS native dedup is enabled on the storage server, or two copies otherwise.

The daemon makes no attempt to detect or coordinate this case at the application layer. Cross-library byte-identical content is a storage-layer concern.

## Local State Schema (`state.db`)

`state.db` is the authoritative ingest state, a SQLite database at `~/.local/share/hopnet-photo-ingress/state.db`, accessed only by the daemon and the CLI (via the Rust core). It never lives on the network mount — see "SQLite local, blobs remote" in the Architecture section.

Schema shapes are chosen for the RFC-011 migration contract: columns that survive migration use the same names, types, and semantics as their `photos.md` counterparts, so the migrator copies them without transformation. Ingress-only columns (identity plumbing, pipeline state, refcounts) are dropped at migration.

### `libraries`

```sql
CREATE TABLE libraries (
    library_id           TEXT PRIMARY KEY,   -- 'personal', 'shared_household'; also the on-disk path component
    display_name         TEXT NOT NULL,      -- UI string for the CLI
    blob_root            TEXT NOT NULL,      -- absolute path on the ingesting device, e.g. /Volumes/photos-personal
    sidecar_root_remote  TEXT,               -- backup root on the storage side; NULL = no remote sidecar backup
    scope_binding        TEXT UNIQUE,        -- PhotoKit shared-library scope identifier; NULL for personal
    retention_days       INTEGER NOT NULL DEFAULT 30,  -- soft-delete grace before hard-delete cleanup
    created_at           TEXT NOT NULL       -- ISO 8601
);
```

Notes:

- `library_id` doubles as the on-disk path component (`/<root>/<library_id>/blobs/...`), so it must be filesystem-safe: lowercase, `[a-z0-9_]`, no path separators. Enforced by the CLI at configuration time.
- `sidecar_root_local` is **not** stored. It is always derived as `~/.local/share/hopnet-photo-ingress/sidecars/<library_id>/`; storing it would invite drift between the stored value and the derivation rule.
- `scope_binding` is `UNIQUE`: a PhotoKit scope maps to at most one library. SQLite permits multiple NULLs, so this does not constrain personal or future non-PhotoKit libraries.
- `retention_days` is per-library — a shared library may warrant a longer window than a personal one. The hard-delete cleanup job reads the owning library's value on each run; changing it applies from the next run (see the retention edge-case table in Deletion and Retention).
- **Exactly one personal library in the MVP.** PhotoKit exposes a single system photo library per account, so there is one row with `scope_binding IS NULL`, conventionally `library_id = 'personal'`. A future non-PhotoKit "imported" library (manual file drops) would be a second NULL-scope row; the schema requires no change, only routing rules.
- **No `library_unmapped` sentinel row.** An asset whose PhotoKit scope has no configured binding is recorded in `photos` with `library_id = NULL` (see the `photos` table); ingest is blocked for that asset until the user binds the scope. The unmapped state is an absence, not an entity — this keeps `libraries` free of placeholder rows that would need fake `blob_root` values.
- `sidecar_root_remote = NULL` disables the remote sidecar backup for the library. The CLI warns loudly on this configuration: without remote sidecars, recovery from a lost Mac degrades to blob-only rebuild, which loses all PhotoKit-derived metadata (capture grouping, library scope, favorites, edit relationships) and mints fresh `photo_id`s.

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
    sidecar_replicated_at TEXT,            -- NULL = local sidecar newer than remote copy (replication pending)

    -- Tombstone (RFC-011-compatible; deleted_by deliberately absent, see notes)
    deleted_at        TEXT,                -- ISO 8601, NULL when active

    FOREIGN KEY (library_id) REFERENCES libraries(library_id)
);

CREATE INDEX idx_photos_library ON photos(library_id);
CREATE INDEX idx_photos_pending ON photos(materialized_at) WHERE materialized_at IS NULL;
CREATE INDEX idx_photos_deleted ON photos(deleted_at) WHERE deleted_at IS NOT NULL;
CREATE INDEX idx_photos_group ON photos(group_id) WHERE group_id IS NOT NULL;
```

Notes:

- **`cloud_id` is globally `UNIQUE`, not per-library.** Apple guarantees distinct `cloud_id`s for distinct assets — the same content existing in both personal and shared scopes is two `PHAsset`s with two `cloud_id`s, so that case never conflicts. Global uniqueness is load-bearing for library transitions: a move between libraries preserves the `cloud_id`, and the daemon detects the move precisely because a known `cloud_id` arrives with a different scope. Per-library uniqueness would make a transition indistinguishable from a new photo in the destination plus an orphan in the source. If Apple's invariant is ever violated, the constraint fails loudly rather than letting state diverge silently. Per-library dedup scoping (see Library Partitioning) applies to `content_hash`, not `cloud_id`.
- `local_id` is a convenience handle for PhotoKit fetch calls, not an identity key — `PHAsset.localIdentifier` is device-scoped and can change across library rebuilds. It is updated opportunistically whenever the asset is observed (match-precedence step 1).
- `library_id = NULL` means the asset's PhotoKit scope has no configured binding (see `libraries` notes). The row exists so discovery is never lost, but the pipeline skips it until the scope is bound.
- `materialized_at` is the pipeline's photo-level completion marker: set (in the same transaction as the final `photo_resources` state update) only once every enumerated resource for the photo has been written and committed. Per-resource fetch/retry state lives on `photo_resources`, since resources fail independently (a Live Photo's video download can fail while its still succeeds).
- `asset_modified_at` powers the fast path's "unless metadata changed" check: if the incoming descriptor's `PHAsset.modificationDate` equals the stored value, the observer event is a no-op; if newer, sidecar metadata is refreshed and resource-level changes are re-enumerated.
- **There is no `deleted_by` column.** The daemon has a single implicit local user and no consensus `user_id` to record — storing a sentinel would be fabricating data. The migrator sets RFC-011's `photos.deleted_by` to the importing user's `user_id` for any tombstone that flows through migration.
- `sidecar_replicated_at` is the remote-sidecar dirty flag: every local sidecar rewrite sets it to `NULL` (in the same transaction as the state change that triggered the rewrite); successful replication to `sidecar_root_remote` stamps it. The daemon drains the dirty set (`WHERE sidecar_replicated_at IS NULL`) whenever the mount is available. This matters because metadata-only rewrites (tombstone, restore, favorite) do not require the mount and can accumulate while it is down — without a durable marker, a Mac dying with an unreplicated tombstone would resurrect the deleted photo in disaster recovery.

### `photo_resources`

```sql
CREATE TABLE photo_resources (
    photo_id         TEXT NOT NULL,        -- FK photos
    resource_type    INTEGER NOT NULL,     -- RFC-011 values: 0=original, 1=edited, 2=paired_video,
                                           --   3=adjustment_data, 4=raw_alternate, 7=edited_paired_video
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

- **`resource_type` uses RFC-011's enum values verbatim** (see `photos.md` Resource Types). Thumbnail types (5, 6) are deliberately never stored — thumbnails are generated client-side from the primary display resource after migration, not archived by the daemon.
- **PhotoKit → ingress resource mapping** (spike-verified against a real library):

  | `PHAssetResourceType` | Ingress `resource_type` |
  |---|---|
  | `photo` (1) / `video` (2) | `original` (0) |
  | `fullSizePhoto` (5) / `fullSizeVideo` (6) | `edited` (1) |
  | `pairedVideo` (9) | `paired_video` (2) |
  | `adjustmentData` (7) | `adjustment_data` (3) |
  | `alternatePhoto` (4) | `raw_alternate` (4) |
  | `fullSizePairedVideo` (10) | `edited_paired_video` (7) |

  Edits never mutate the `photo`/`pairedVideo` resources — edited renders appear as separate `fullSize*` resources, and the presence of `adjustmentData` is the "this asset has edits" signal. An edited Live Photo therefore carries five resources (original still, original motion, adjustment plist, edited still, edited motion).
- **No `status` column.** Per-resource pipeline state is derivable: `content_hash IS NULL` = not yet fetched; `written_at IS NULL AND next_retry_at IS NOT NULL` = failed, awaiting backoff; `written_at IS NOT NULL` = durably written. The CLI computes human-readable status from these columns; a stored enum would be a second source of truth that can drift.
- **Blob paths are not stored.** A resource's blob path is reconstructed at read time as `<blob_root(photos.library_id)>/<aa>/<bb>/<content_hash>.<ext>` — this is what makes library transitions a pure refcount-plus-`photos.library_id` operation with no per-resource updates.
- **Resource lifecycle mirrors PhotoKit's current state; no version history.**
  - *First edit* (asset gains an edited rendition): a new row with `resource_type = 1` appears alongside the untouched `original` row (for Live Photos, an `edited_paired_video = 7` row appears as well). This is additional current resources, not history.
  - *Re-edit* (asset's edited rendition is replaced): the `edited` row is updated in place with the new `content_hash`. In the same transaction — the **write-commit transaction of the replacement bytes**, not the classification event (the new bytes must be fetched first; between classification and commit the row sits in the superseded-pending state, see §Per-resource state machine) — the superseded blob's refcount is decremented (deleting the file if it reaches 0) and the new blob's refcount is incremented. Detection is a `fileSize` compare (descriptor vs stored `size_bytes`) on written edit-mutable rows; equal or absent sizes are assumed unchanged, and a false positive (changed size, identical bytes) nets to a refcount no-op. Superseded edit renditions are not retained — the daemon archives the current iCloud state; version history is RFC-011's operation log's job post-migration.
  - *Revert to original* (user discards edits): the `edited` row (and `adjustment_data` row, if PhotoKit drops it) is deleted, with the same refcount decrement semantics.
  - The `original` row is never overwritten in any of these flows.
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

    PRIMARY KEY (library_id, content_hash),
    FOREIGN KEY (library_id) REFERENCES libraries(library_id)
);
```

Notes:

- **`ref_count` invariant**: `ref_count` equals the number of `photo_resources` rows whose `content_hash` matches and whose parent photo's `library_id` matches — i.e. it is fully recomputable via a JOIN through `photos`. Every increment/decrement happens in the same SQLite transaction as the `photo_resources` change that caused it.
- **Why a stored count rather than deriving it**: a declared FK from `photo_resources` to `blobs` is not expressible (the blob key includes `library_id`, which lives on `photos`, not `photo_resources`). More importantly, the count gates eager, irreversible filesystem deletes (hard-move relocation, re-edit supersede, hard-delete cleanup) — `UPDATE ... SET ref_count = ref_count - 1 ... RETURNING ref_count` is a single atomic operation with no scoping JOIN to get subtly wrong at any call site. The redundancy is deliberate: a stored count plus a recount JOIN are two independent answers that must agree, so refcount drift from a crash or bug is detectable and repairable (recovery and a CLI `fsck`-style check recompute and diff). RFC-011's `DataBlockReferenceProvider` derives instead because its cleanup is a lazy background sweep spanning multiple modules; ingress deletes eagerly within one module, which favors the counter.
- **`ext` is an attribute, not part of the key.** Identical bytes imply the same UTI in practice; in the pathological case of the same content arriving under two different UTIs, the first writer's extension wins and a warning is logged.
- **Single-writer invariant**: exactly one daemon instance writes a given library subtree on the storage root. This falls out of the design anyway — `state.db` (holding the authoritative refcounts) and PhotoKit `local_id`s are both device-local, so a second device cannot meaningfully share them. A second Mac or future iOS daemon ingesting the same iCloud library targets its own storage subtree, and the RFC-011 migrator merges records at consensus time (see Cross-Device Convergence). The daemon enforces only in-process exclusivity: at most one inflight materialization per `(library_id, content_hash)`, so two photos sharing a blob don't race the same temp-write-rename.
- No integrity/scrub column (`verified_at`) — bit-rot detection is delegated to the storage filesystem (ZFS scrub). If a future deployment targets storage without self-healing, a scrub job and column can be added then.

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

The ingest log is **authoritative for nothing**. No code path reads it to make a decision; deleting the table changes no behavior. State tables answer "what is"; the log answers "what happened" — it is the black-box recorder for a daemon that deletes irreplaceable data. Its primary consumer is forensics: after a hard delete, the state tables retain no trace of the photo, and the log is the only artifact that can answer "where did my photo go?" (`deletion_observed` on March 3, `hard_delete` on April 2). Secondary consumers are CLI history views and incident debugging (mount flaps, ingest stalls). It does not migrate to RFC-011 — deletion state flows through `photos` columns, not log replay.

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
| `mount_lost` / `mount_regained` | no | library ids affected |
| `storage_low` / `storage_recovered` | no | free bytes, reserve floor, library id |
| `scope_unmapped` | no | PhotoKit scope id encountered with no binding |

Shape is deliberately loose — `event_type` as TEXT (readable in a `sqlite3` shell, new types cost nothing), `detail` as freeform JSON — because nothing downstream parses it. Rows older than 180 days are pruned by the hourly cleanup job.

## Sidecar Format

One JSON document per photo, written locally (hot path) and replicated to `sidecar_root_remote` (backup), at `<root>/<YYYY>/<MM>/<photo_id>.json` keyed by `captured_at` year/month (falling back to `ingested_at` when capture date is unknown).

The sidecar exists for exactly three things, and every field must justify itself against them:

1. **PhotoKit-computed values that raw EXIF lacks** — capture date as PhotoKit resolves it, media type/subtypes, favorite state, burst grouping and pick.
2. **High-level query parameters** — camera, location, date, resolution — so galleries and rescans never open blob bytes.
3. **DB-state repopulation** — if the Mac (and `state.db`) dies, the sidecar tree plus the blob tree on the surviving storage must be sufficient to rebuild `photos`, `photo_resources`, and recount `blobs.ref_count`. The `resources` array is the load-bearing field here: it is the only off-device record of the photo-to-blob mapping.

Anything re-extractable from surviving blob bytes fails the test and is excluded — notably the **full EXIF dump**: raw EXIF lives inside the original blobs, which survive the dead Mac by definition. Recovery or migration can re-extract exotic EXIF from blobs on demand; the sidecar carries only the curated fields.

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
- Curated metadata fields mirror RFC-011's sidecar `photo_index` columns one-to-one (`date_taken`, dimensions, orientation, media type, duration, camera, GPS), so the migrator constructs the `encrypted_metadata` blob from these fields without transformation. `media_subtypes` carries PhotoKit's computed subtype flags (`hdr`, `screenshot`, `panorama`, `slomo`, …) — PhotoKit-derived, not recoverable from EXIF alone.
- `media_type` is `"image" | "video" | "live_photo"`, matching RFC-011's sidecar values.
- `location` is present only when the asset has GPS data; `camera` fields are null for assets without camera metadata (screenshots, imports).
- `favorite` is included: PhotoKit-only state, maps to RFC-011's `photo_favorites` at migration.
- `resources` lists every written resource with `type` as the RFC-011 enum *name* (not integer — sidecars optimize for human readability). Entries appear as each resource is durably written; a sidecar's resource list reflects committed state only.
- The sidecar is mutable and rewritten in place (temp + rename) on: resource set changes (edit, re-edit, revert), tombstone/restore (`deleted_at`), favorite toggles, and library transitions (`library_id`). The remote copy is refreshed asynchronously, best-effort, on the same triggers.

### Explicitly excluded

- **Full EXIF dump** — re-extractable from original blobs, per the three-purpose test above.
- **Album membership** — deliberately out of scope for the MVP. Albums are structurally additive to retrofit: a future revision adds a per-library `albums.json` (one document per library mapping album names to `photo_id` lists) with no changes to per-photo sidecars or existing `state.db` tables. The retrofit can backfill from the live Photos library at any time, since PhotoKit retains album structure; the only unrecoverable scenario is the Photos library itself dying before the retrofit lands. RFC-011's album support is Phase 4, so nothing downstream blocks on this.

## Ingest Pipeline

This section specifies the invariants a correct pipeline must preserve — state transitions, transaction boundaries, crash windows, and admission rules. Worker topology, queue scheduling, backoff parameters, and concurrency defaults are implementation details; the spec constrains only their observable behavior.

### Discovery and the work queue

Two discovery modes feed the same downstream path:

- **Observer events** (runtime): `PHPhotoLibraryChangeObserver` callbacks, translated to `AssetDescriptor`s.
- **Reconciliation scan** (startup + periodic): full `PHAsset` enumeration diffed against `state.db`. Catches everything missed while the daemon was offline — including deletions, which are detectable only this way (asset present in `state.db`, absent from PhotoKit).

Both modes are idempotent: every descriptor resolves through match precedence (see Asset Identity Model), so duplicate or re-delivered events are no-ops.

**`state.db` is the work queue.** `photos` and `photo_resources` rows are minted at discovery, before any bytes move; the pipeline pulls pending work from `state.db` (`materialized_at IS NULL`, `written_at IS NULL`, gated by `next_retry_at`). PhotoKit is never re-enumerated to find work — a restart resumes exactly where the daemon left off by querying pending rows.

### Change classification

Every discovery event resolves to exactly one of five kinds. All sidecar rewrites and transaction boundaries hang off this taxonomy:

| Kind | Trigger | Actions |
|---|---|---|
| New photo | No identity match | Mint `photo_id` + resource rows, enqueue fetches |
| Resource change | Resource set or `edited` bytes differ | New/updated/deleted resource rows; re-edit and revert follow the lifecycle in `photo_resources` notes (refcount swap in same tx); rewrite sidecar |
| Metadata-only | `asset_modified_at` newer, resources unchanged | Update `photos` row + rewrite sidecar; no byte movement |
| Scope change | Known `cloud_id`, different library scope | Hard move (see Library Partitioning) |
| Deletion | Asset absent / inaccessible | Tombstone (see Deletion and Retention) |

### Per-resource state machine

A resource has three persistent states, keyed on `written_at`:

- **pending** — `content_hash IS NULL`, `written_at IS NULL`. Retry metadata (`retry_count`, `next_retry_at`, `last_error`) annotates this state; a resource that exhausted retries is still `pending`, terminally, with a `resource_gave_up` log event.
- **superseded-pending** *(Phase 4 amendment)* — `content_hash` set, `written_at IS NULL`. A re-edit *reopens* the row: `written_at` and retry state clear, but the old `content_hash` is **retained as the superseded pointer** so the blob refcount swap (decrement old, increment new, `blob_superseded` log) commits atomically with the replacement bytes at write time. This preserves both the no-byte-loss window (the old render's refcount survives until the new render is durable) and the `ref_count` recount invariant (the row still references the old blob until the swap). The work queue keys on `written_at IS NULL`, so this state is pending for scheduling purposes.
- **written** — `content_hash` and `written_at` both set, always in the same transaction.

There is deliberately no persisted intermediate ("fetched", "hashed"): bytes stream once through the BLAKE3 hasher into the temp file simultaneously, so the hash is only known at the moment the blob is already durably placeable — the two facts commit together or not at all. A crash mid-stream leaves only an orphan temp file and an untouched `pending` (or superseded-pending) row.

### Write path

1. **Admission** — storage-aware check (below) before a fetch slot is granted.
2. **Stream** — `PHAssetResourceManager.requestData` (with `isNetworkAccessAllowed = true`) delivers chunks in memory; each chunk feeds the BLAKE3 hasher and appends to `<blob_root>/blobs/.partial/<photo_id>.<resource_type>`. The temp is named by `(photo_id, resource_type)` — the hash isn't known yet, and this naming makes per-resource inflight exclusivity structural. Exception: a brand-new photo's original streams *before* any `photo_id` exists (identity rules 2a–2c resolve from its hash), so pre-mint originals use a fresh probe token (`probe-<uuid>`) under the same `.partial/` directory, swept identically at startup.
3. **Dedup decision** — stream complete, hash known. Query `blobs(library_id, content_hash)`:
   - **Hit**: delete the temp. Transaction: increment `ref_count`, set `content_hash`/`ext`/`size_bytes`/`written_at` on the resource row.
   - **Miss**: fsync the temp, rename to `blobs/<aa>/<bb>/<hash>.<ext>`. Transaction: insert `blobs` row with `ref_count = 1`, update the resource row as above.
4. **Photo completion** — if this was the photo's last unwritten resource, the same transaction sets `photos.materialized_at`. The sidecar is then written locally and queued for best-effort remote replication.

**Ordering invariant: filesystem durability precedes database commit.** A committed row never references bytes that might not exist. The crash windows this leaves are all benign:

| Crash point | On-disk result | Repair |
|---|---|---|
| Mid-stream | Orphan `.partial` file, `pending` row | Startup sweep deletes all `.partial` files (nothing ever references them); row retries |
| After rename, before commit | Blob file with no `blobs` row | Orphan scan (see Recovery); refetch takes the dedup-hit path if the file is re-created first, or rewrites it — content addressing makes both idempotent |
| After commit | Consistent | Nothing to do |

**Temp files live on the destination filesystem** — atomic rename cannot cross filesystems, so temps cannot live in local `run/`. The cost of a dedup hit is having streamed the bytes over the network before discarding them; this is accepted because the `cloud_id` fast path catches already-ingested assets before any download, making write-stage dedup hits rare in steady state. The alternative (spool locally, hash, then copy on miss) costs strictly more total I/O and adds local disk pressure.

### Storage-aware admission

Before a fetch is admitted, the scheduler checks that the write can complete:

- **Expected size** comes from `PHAssetResource`'s `fileSize` attribute — an undocumented KVC key (`value(forKey: "fileSize")`), reliable in practice but treated as advisory: it may be absent or zero for assets not yet downloaded from iCloud. Unknown sizes are assumed to be a configurable pessimistic estimate (default: the largest asset observed so far in this library).
- **Free space** on the `blob_root` filesystem, minus the summed expected sizes of already-inflight writes, must stay above a configurable reserve floor (default 10 GiB). Breach pauses admission (inflight writes finish), emits `storage_low`, and admission resumes with `storage_recovered` once the check passes again.
- **Local disk headroom**: PhotoKit stages iCloud downloads in its own cache on the Mac's local disk — this cannot be opted out of, and the daemon never sees those files. Required local headroom is approximately `fetch_concurrency × largest asset`; the bounded fetch pool is the control knob.

### Concurrency and cancellation

- Bounded fetch and write pools (defaults 4/4, configurable; see Process model).
- At most one inflight materialization per `(library_id, content_hash)` (see `blobs` notes) and per `(photo_id, resource_type)` (structural, via temp naming).
- SIGTERM triggers cooperative cancellation: inflight streams are abandoned (their temps swept at next startup), SQLite transactions are never interrupted mid-flight (they are fast and atomic anyway).
- Every step is re-runnable. Idempotency falls out of content addressing plus match precedence — re-fetching a written resource is a dedup hit; re-processing a discovery event is a no-op.

The daemon mirrors PhotoKit deletions into its own state, but does not delete bytes from the storage root immediately. A retention window allows recovery from accidental deletes — both at the user's "oh wait" level and at the level of unexpected PhotoKit observer churn during library reorganizations.

The model mirrors `photos.md`'s 30-day soft-delete retention so that migration into the consensus layer carries the deletion state forward unchanged.

### Trigger

The PhotoKit change observer emits an event when a `PHAsset` becomes inaccessible to the API. In practice this fires when the user moves the asset to Photos's "Recently Deleted" album, not when the asset is purged from Recently Deleted 30 days later. The daemon treats this single event as the deletion trigger and does not attempt to distinguish "soft delete in Photos" from "permanent delete in Photos." The 30-day window described below stacks on top of whatever grace Apple's own Recently Deleted provides, which is a feature rather than a bug.

### Tombstone

When a deletion event fires for an asset the daemon has previously ingested:

1. `photos.deleted_at` is set to the current timestamp.
2. No deleting actor is recorded — the daemon has a single implicit user; RFC-011's `deleted_by` is assigned to the importing user at migration (see the `photos` schema notes).
3. `photo_resources` rows are **not** touched.
4. `blobs.ref_count` values are **not** decremented.
5. The sidecar's `deleted_at` field is set and the sidecar JSON is rewritten in place. This is a **read-modify-write of the existing document** — the asset no longer exists in PhotoKit, so recomposition from a descriptor is impossible; and since the sidecar path is keyed on `captured_at` (not persisted in `state.db`), the document is located by a two-level `YYYY/MM` walk. A photo that never materialized has no sidecar; the step is skipped silently.
6. A `deletion_observed` event is recorded in the ingest log.

Steps 3 and 4 mean the bytes stay on disk and the refcount remains accurate to the (still-existing) `photo_resources` rows. The photo disappears from active queries (`WHERE deleted_at IS NULL`) but is fully restorable until the retention window expires.

This matches the `photos.md` reference provider's behavior: a soft-deleted `photos` row keeps its `photo_resources` rows alive, which in turn keep their data blocks alive. The daemon's `blobs.ref_count` is the analogue of the consensus layer's reference-provider check.

### Restore inside the window

If PhotoKit subsequently emits a change event indicating the asset is alive again — typically because the user un-deleted it from Recently Deleted — the daemon resolves identity by `cloud_id`, finds the tombstoned `photos` row, and:

1. Clears `photos.deleted_at`.
2. Clears the sidecar's `deleted_at` field and rewrites the sidecar JSON.
3. Records a `restore_observed` event in the ingest log.

No blob movement is required because nothing was moved on the original delete. Restore is atomic at the SQLite level: a single update statement on the `photos` row.

If the asset has been deleted in PhotoKit and then re-imported as a fresh asset (new `cloud_id`), it is **not** a restore — it is a new photo, even if the bytes are identical. Rule 2b from the Asset Identity Model applies: a new `photo_id` is minted and the existing blob's refcount is incremented.

### Hard-delete cleanup

A periodic cleanup job runs on a configurable interval (default: once per hour) and processes photos whose retention window has expired:

```
For each photo P with deleted_at IS NOT NULL
                AND datetime(deleted_at, '+30 days') < datetime('now'):

  Inside a single SQLite transaction:
    1. For each photo_resources row R of P:
         decrement blobs(library_id, content_hash).ref_count
         record (library_id, content_hash, ext) for post-tx cleanup
            if the decremented count is zero
    2. Delete photo_resources rows for P.
    3. Delete photos row for P.

  After the transaction commits:
    4. For each recorded (library_id, content_hash, ext) whose
       count reached zero, delete the blob file at
       <blob_root>/<aa>/<bb>/<hash>.<ext>.
    5. Delete the sidecar JSON locally
       (<sidecar_root_local>/<YYYY>/<MM>/<photo_id>.json).
    6. Delete the sidecar JSON on the remote backup root (best effort).
    7. Record a hard_delete event in the ingest log.
```

The transaction commits before the filesystem operations because:

- SQLite transactions are fast and authoritative.
- Filesystem operations over a network mount are slow and can fail (mount lost, server restarting, ACL transient issues).
- If the daemon crashes between transaction commit and filesystem cleanup, the result is orphan blob files on disk with no `blobs` row referencing them. The Recovery section describes a startup scan that reconciles this: any file under `<blob_root>` whose `(library_id, content_hash)` has no corresponding `blobs` row is an orphan and is deleted.

The cleanup job is idempotent: re-running it produces no additional effect once a photo has been fully hard-deleted.

### Edge cases

| Scenario | Behavior |
|---|---|
| Soft-deleted photo's blob is also referenced by an active photo | Refcount stays > 0 during cleanup; blob file is preserved. Only the tombstoned photo's `photos` and `photo_resources` rows are removed at hard-delete time. |
| Daemon offline when PhotoKit deletes an asset | On next observer reconciliation, the asset is reported as absent from PhotoKit while still present in `state.db`. Daemon synthesizes a deletion event and tombstones the photo. The `deleted_at` timestamp is the reconciliation moment, not the original PhotoKit delete moment, so the retention window starts from when the daemon noticed. |
| Daemon offline for longer than 30 days | Same as above — tombstone is created with `deleted_at = now`, full 30-day window applies. No assumption that the user's PhotoKit-side grace already elapsed. |
| User deletes the entire iCloud Shared Photo Library | Every shared-library asset transitions to a deletion event on the change observer; daemon tombstones them all. After 30 days, cleanup hard-deletes the rows and the bytes. The `libraries` row for the shared library remains until the user explicitly removes it via CLI. |
| Photo deleted, then user attempts to re-import the same image file as a new asset | Rule 2b: new `cloud_id`, new `photo_id`. The old tombstoned record proceeds through normal hard-delete on schedule; the new record begins fresh. Blob refcount handles the byte-level overlap (single blob, refcount 2 during overlap, refcount 1 after the tombstone expires). |
| Retention window changed (config edited from 30 to 60 days) | Cleanup job uses the new value on its next run. Photos already past the old window are not retroactively hard-deleted by re-extending; they may have already been processed. |

### Why this matches RFC-011

The daemon's deletion model is intentionally identical in shape to `photos.md`'s soft-delete approach: tombstone on the `photos` row, retain referenced data through a refcount-style check, hard-delete after a fixed retention window. The mapping at migration time is direct:

| Daemon | RFC-011 consensus |
|---|---|
| `state.db.photos.deleted_at` | `photos.deleted_at` |
| (not stored) | `photos.deleted_by` — migrator assigns the importing user's user_id |
| `state.db.blobs.ref_count` | `DataBlockReferenceProvider` check on `photo_resources` |
| Periodic cleanup of tombstones past retention | RFC-011's cleanup job for expired tombstones |
| Sidecar deletion | Sidecar rebuild on next consensus sync |

No daemon state needs to be invented or re-derived during migration. The 30-day window is the same value in both layers; in-flight tombstones flow through the migration as-is.

## Failure Handling

The spec constrains failure *semantics* — what state each failure class may and may not leave behind. Detection mechanics, timeout values, and backoff parameters are implementation details.

| Failure | Semantics |
|---|---|
| Storage mount lost | Pipeline pauses for all libraries on that mount. Pending rows are untouched — mount loss is not a resource failure and consumes no retries. `mount_lost` event; on regain, `mount_regained`, admission resumes, and the sidecar dirty set is drained. |
| iCloud fetch failure | Per-resource exponential backoff via `retry_count` / `next_retry_at`. After a configurable retry cap, the resource goes terminally pending with a `resource_gave_up` event. Terminal resources are automatically re-enqueued by the next reconciliation scan — transient iCloud outages self-heal without operator action. |
| Local disk pressure (`CloudPhotoLibraryErrorDomain` code 1005) | `cloudphotod` refuses downloads below a local-headroom threshold (spike-verified: instant failure, no network attempt; resolves when space is freed). Classified as a daemon-wide pause like `storage_low`, but for the *local* disk: fetch admission stops, no retry counts are consumed, admission resumes when headroom recovers. Treating it as a per-resource failure would spin the retry budget uselessly. |
| Partial blob write (crash, mount drop mid-stream) | Covered by the crash-window table in the Ingest Pipeline: orphan `.partial` temps are swept at startup; a renamed-but-uncommitted blob is reconciled by the orphan scan. No committed row ever references unverified bytes. |
| Remote sidecar replication failure | Best-effort and asynchronous by design; durability comes from the `sidecar_replicated_at` dirty flag, drained whenever the mount is up. Metadata-only rewrites (tombstone, favorite) accumulate safely while the mount is down. |
| PhotoKit authorization revoked | Hard stall: the daemon stops all PhotoKit interaction, logs a loud event, and the CLI surfaces the condition. No state is modified — in particular, an empty enumeration due to lost authorization must not be interpreted as mass deletion. |
| Local `state.db` corruption | Disaster case; recover from the most recent state snapshot (see Recovery). |

The last row of the table deserves emphasis as a general rule: **absence of evidence from PhotoKit is only evidence of deletion when the API is healthy.** Any scan that could synthesize deletion events must verify library authorization and non-empty enumeration sanity before tombstoning anything.

fsync over SMB is weaker than local fsync — the daemon issues it, but the storage server's write cache is the real durability boundary. This is accepted: the backstop for silent byte loss is `fsck`'s blob-existence check plus filesystem-level integrity (ZFS) on the storage side.

## Recovery

Recovery is tiered: the daemon repairs benign inconsistencies automatically, audits on demand, and rebuilds from the storage root only as an explicit, operator-initiated disaster action. The daemon never decides on its own that `state.db` is disposable.

### Tier 1 — automatic startup reconciliation

On every start:

1. Sweep all `.partial` temp files (nothing ever references them).
2. If the previous shutdown was unclean (stale pid/lock in `run/`): recount `blobs.ref_count` from the JOIN through `photos`/`photo_resources`, diff against stored values, repair and log any drift.

Startup repair never deletes blob files — orphan deletion is deliberately excluded from the automatic tier.

### Tier 2 — `ingress-cli fsck`

On-demand invariant audit across `state.db`, the sidecar trees, and the blob trees:

- Recount refcounts (as tier 1) and report drift.
- **Missing blobs**: every `blobs` row must have its file on disk. A miss means byte loss (or manual tampering) and is reported loudly — it is not repairable from local state; the resource must be re-fetched from PhotoKit if the asset still exists there.
- **Orphan blobs**: files under `blobs/` with no corresponding `blobs` row (the crash window between rename and commit, or leftovers from a crashed hard-delete). Deleted only under `--repair` — this is the one destructive repair, which is why it lives here and not in tier 1.
- Sidecar consistency: every non-`NULL`-library photo has a local sidecar whose contents match its rows; remote copies match `sidecar_replicated_at` claims.

### Tier 3 — `ingress-cli recover`

Explicit rebuild of `state.db` from a storage root, for a dead Mac or lost local disk. Two sources, in preference order:

1. **State snapshot** (`state-snapshots/state.db.<timestamp>.sqlite3`): restore the newest snapshot, then let normal startup plus a reconciliation scan close the gap — PhotoKit re-delivers everything that changed since the snapshot, and match precedence makes re-delivery idempotent. Complete recovery: `photo_id`s, tombstones, pipeline state.
2. **Sidecar-tree rebuild** (no usable snapshot): walk `sidecars/<YYYY>/<MM>/*.json` per library, reconstruct `photos` and `photo_resources` rows from each document, rebuild `blobs` by recounting resource references, and verify each referenced blob file exists. `photo_id`s survive — they are in the sidecars. Lost: retry state and the ingest log (both disposable) and `local_id`s, which are device-scoped and would be useless on a replacement Mac anyway — the first reconciliation scan re-links every asset by `cloud_id` (match-precedence step 1) and repopulates them.

Only if both sources are absent (blobs survived, sidecars did not) does recovery degrade to the blob-only case: fresh `photo_id`s, metadata re-derived from blob bytes where possible. This is the scenario the remote sidecar backup exists to prevent — see the `sidecar_root_remote` warning in the `libraries` notes.

After any tier-3 recovery, the daemon's first reconciliation scan doubles as verification: assets still in PhotoKit re-resolve by `cloud_id`, and discrepancies surface as ordinary pipeline work rather than recovery-specific logic.

### State snapshots

Written by the periodic cleanup job: once per day per library root (on the first run after the day rolls over), via the SQLite backup API to `state-snapshots/state.db.<timestamp>.sqlite3`, keeping the newest 7. Snapshot staleness is benign — tier 3 reconciles the gap from PhotoKit — so no tighter cadence is warranted. Snapshot cadence is unrelated to ingest freshness: blobs and sidecars replicate continuously as changes happen.

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

### Phase 5: Lifecycle [ ]
- Hourly cleanup job: hard deletes, refcount-zero blob removal, log pruning, daily state snapshots
- `sidecar_replicated_at` dirty-set drain

### Phase 6: CLI [ ]
- `status` (library, pipeline, and per-photo views)
- `fsck` with `--repair`
- `recover` (snapshot-first, sidecar-tree fallback)
- Library configuration commands (add/bind/rename, retention)

### Phase 7: Hardening [ ]
- LaunchAgent packaging, login autostart
- Long-run soak against a full-size library
- Mount-loss / storage-low behavior under real SMB conditions