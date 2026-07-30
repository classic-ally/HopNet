# RFC-018: hopnet-mount — Linux Filesystem Integration

**Status**: Draft
**Depends on**: RFC-012 (device-token sessions), RFC-014 (storage substrate),
RFC-015/016 (projection modularity + host API), RFC-009 (Apple FileProvider,
as architectural precedent)
**Related**: issue #23 (storage-layer blob pinning — deferred, out of scope)

## Motivation

macOS integration exists (RFC-009: FileProvider appex speaking HTTP to the
local node); Android has a DocumentsProvider; Linux has nothing. Goal:
HopNet appears as an ordinary directory on Linux, files materialize on
demand, and IO on already-local content runs at native speed.

Linux has no FileProvider/Cloud Files API equivalent, so the interception
mechanism is a design choice.

fanotify pre-content (FAN_PRE_ACCESS, kernel 6.14) — evaluated as primary
mechanism, rejected for now (kernel state as of 2026-07):

- FAN_PRE_MODIFY never merged — no pre-write interception
- mmap page-fault hook merged, regressed, reverted — full population
  required at mmap() time
- directory pre-content events (lazy namespace) unmerged
- restartable permission events (safe daemon restart) unmerged
- CAP_SYS_ADMIN required — root daemon, at odds with per-user session keys
- fails silent — dead daemon auto-allows events; unhydrated placeholders
  read as zeros

FUSE — chosen mechanism; inverts each of the above:

- unprivileged (fusermount3), daemon runs in the user session
- fails loud (ENOTCONN), recoverable by supervised remount
- no placeholder files — namespace served live, nothing to read as zeros
- mmap works in normal FUSE mode (page faults become FUSE reads)
- works on every currently-supported distro kernel
- passthrough (6.9+) + io_uring transport (6.14) close the historical
  performance gap for local content

fanotify remains a possible future backend behind the same daemon once the
kernel side matures.

Discipline, not requirement: keep the daemon core (VFS surface, hydration,
cache, staging) cleanly separated from HopNet-specific plumbing so the
layer stays highly reusable by other sync backends. Reusability guides
boundaries but is not a deliverable.

## Architecture

`hopnet-mount`: user-session daemon binary, new workspace crate.

```
┌────────────┐  FUSE (/dev/fuse, fuser crate)   ┌──────────────────────┐
│   kernel    │◄────────────────────────────────►│ hopnet-mount daemon  │
│  VFS layer  │  passthrough fd (hydrated files) │  · FUSE dispatch     │
└────────────┘                                   │  · id map/attr cache │
                                                 │  · sparse cache mgr  │
                                                 │  · write staging     │
                                                 │  · /watch subscriber │
                                                 └──────────┬───────────┘
                                                            │ HTTP, device token
                                                 ┌──────────▼───────────┐
                                                 │ hopnet node (axum)   │
                                                 │ /api/integrations/…  │
                                                 └──────────────────────┘
```

- out-of-process from the node — same shape as the macOS appex
- HTTP to `127.0.0.1:{port}` with a device token (RFC-012) — no prior
  login needed; leaves the door open to mounting a remote node
  (thin-client, out of scope)
- components:
  - **FUSE dispatch** (`fuser`) — namespace ops (lookup/getattr/readdir)
    answered from node metadata; no on-disk placeholder tree, so no
    placeholder-consistency problem
  - **id map + attr cache** — the in-memory state, deliberately small:
    - u64 st_ino ⇄ inode UUID allocation table (FUSE needs stable u64s;
      table is daemon-lifetime, rebuilt on restart)
    - per-inode attr entries (type, size, times, parent) tagged with the
      consensus height they were read at
    - readdir is NOT mirrored — directory listings always ask the node;
      the node's SQLite is local and fast, and this keeps the honest-state
      surface minimal
  - **sparse content cache** — per-blob decode buffer (see Hydration)
  - **write staging** — accumulate until release/fsync (see Writes)
  - **/watch subscriber** — long-lived change-push connection; on poke:
    `/changes?since_height=N`, drop/refresh affected attr entries, issue
    kernel invalidations (fuser notify) for touched inodes and parent dirs
- cache policy: generous, cache-until-poked
  - long kernel entry/attr TTLs; proactive invalidation on the changes feed
  - poke correctness is therefore load-bearing
  - test suite MUST cover poke-driven invalidation end-to-end: mutation on
    node → poke → kernel cache invalidated → fresh stat/readdir observes
    the change
  - MUST include watch-connection drop/reconnect gaps — no divergence
    window on reconnect (resync from last anchor height)
- synthesized POSIX metadata: 0644/0755, mounting user's uid/gid, times
  from server-derived UUIDv7 timestamps; no symlinks/hardlinks/xattrs in v1

## Node HTTP Surface

New mount `/api/integrations/mount`, `AuthClass::DeviceToken`, declared in
`DriveProjection::mounts()` alongside fileprovider/documentprovider.

- new surface, tightly coupled to this consumer as needs evolve; the
  shipped fileprovider/documentprovider surfaces stay frozen
- modelled on documentprovider: UUID-native identifiers, no sentinel
  strings; route handlers are thin glue over the same db/upload/download
  functions
- id-based navigation, not path-based:
  - FUSE resolves component-by-component — lookup(parent, name) maps 1:1
  - inode UUIDs are rename-stable; path addressing invalidates whole
    subtrees on ancestor rename
  - deterministic AES-SIV path segments make (parent_id, name) an exact
    index hit server-side
  - the changes feed is already id-based
- routes:
  - `GET /enumerate?parent_id=` — children, paged
  - `GET /lookup?parent_id=&name=` — single-child resolution; avoids
    enumerating large directories for one lookup
  - `GET /item?id=`
  - `GET /changes?since_height=N` — existing height-anchored delta feed
  - `GET /watch` — SSE change push
    - content-free "something changed" poke; daemon follows with /changes
    - heartbeat comment frames; on drop/reconnect the daemon resyncs from
      its last anchor height (no divergence window)
  - `GET /download?id=` — MUST honor HTTP Range
    (`reconstruct_file_range` already supports it; unlike the
    fileprovider route, this surface uses it)
  - `POST /create`, `PATCH /modify`, `DELETE /delete`
    - strict consistency: respond only after the transaction is decided
      by consensus and applied locally (existing barrier infra)
    - no fire-and-forget variant on this surface — one behavior to
      validate, no divergence risk from premature success
  - `GET /health` — unauthenticated readiness (same contract as the
    fileprovider health route)
- plumbing notes:
  - `/watch` lives in this mount's own router; needs a subscribe seam on
    the `ChangeNotifier` capability (broadcast channel) so projection
    routers can subscribe — small RFC-016 host-API addition; the macOS
    signal becomes just another subscriber
  - mutation handlers reuse the validate-then-submit flow, then barrier
    on the decided height before responding

## Hydration & Content Cache

Per-blob sparse files in `$XDG_CACHE_HOME/hopnet/content/{blob_id}`.

- mechanics:
  - `truncate()` to logical size at creation — allocates nothing
  - `pwrite()` ranges as fetched; presence bitmap + LRU per blob
  - read at offset → covering segments → absent ones fetched via Range
  - single-flight: concurrent overlapping reads coalesce into one fetch
    per segment (per-segment inflight tracking with waiters)
  - daemon prefetches ahead on sequential-access detection, on top of
    kernel readahead
- segment size = storage chunk (40 MB), aligned:
  - the substrate reconstructs at chunk granularity — any range touching
    a chunk costs a full-chunk reconstruction (10 fragments ≈ 40 MB of
    mesh traffic) before bytes stream
  - sub-chunk segments would save only loopback/disk bytes, not mesh
    traffic; not worth the extra requests
  - partial reads of large files (container metadata sniffs, seeks) are
    real, but their marginal cost is chunk-granular regardless
- whole-file fast path: files ≤ 1 chunk (the common case) hydrate whole
  into a plain file — no bitmap machinery, immediately
  passthrough-eligible; the sparse-window path is for large files only
- eviction — disk-pressure driven, no fixed byte cap:
  - monitor free space on the cache filesystem; under pressure, punch
    least-recently-read segments (`fallocate(PUNCH_HOLE)`) until relieved
  - ENOSPC on a cache write → punch, retry
  - logical size never changes (hole punch, never truncate) — large
    files behave as a rolling window that follows the read head
  - pinned-for-offline content is NOT the cache's concern (issue #23) —
    the cache may always punch anything
- ephemeral: no persisted bitmap, no crash-consistency obligations;
  rebuild conservatively (or discard) on restart — correctness never
  depends on cache state
- statfs/statvfs reports node-side numbers, not local cache state:
  - total = user-data volume while the placement curve still tolerates
    >= 2 node failures (the resilience pane's capacity number); free =
    total − consumed
  - v1 may read the existing view; migrates to the shared
    available-capacity abstraction (issue #24)
- passthrough acceleration (optional, later phase):
  - only when a file's content is provably complete, and only on a
    subsequent open — passthrough is all-or-nothing per open
  - raw-ioctl registration quarantined in one feature-gated module — the
    crate's only `unsafe`; kernel probe + graceful fallback to
    daemon-mediated reads
  - v1 ships without it; added as a measured optimization

## Writes & Consistency

- staging — durable, separate from the content cache:
  - per-file plaintext staging files in `$XDG_DATA_HOME/hopnet/staging/`
  - dirty data must survive daemon restarts: startup scans staging and
    resumes/retries uploads of orphans
  - never punched — disk-pressure eviction applies to the content cache
    only
- copy-up: opening an existing file for write (without O_TRUNC) hydrates
  full content into staging first — upload is whole-file, so a small edit
  of an unhydrated file costs full hydrate + full re-upload
  - substrate limitation shared with macOS; chunk-delta modify tracked in
    issue #25, explicit non-goal here
- op mapping:
  - `mkdir` → create folder; `create`+writes → staged, uploaded on release
  - `rename` → modify (inode UUIDs are rename-stable)
  - `unlink`/`rmdir` → delete; non-recursive rmdir, 409 → ENOTEMPTY
  - metadata ops are strict-inline (respond after consensus decides,
    per Node HTTP Surface)
- upload semantics — two-tier durability:
  - `release()` (close) kicks a background upload and returns; the file
    stays readable locally from staging (read-your-writes); durable
    staging means a crash before upload completes loses nothing
  - `fsync()` is the strict barrier: returns only after upload complete
    AND the modify transaction is decided by consensus
  - matches the POSIX contract — only fsync promises persistence — and
    avoids serializing bulk copies on per-file consensus rounds
- conflict policy — LWW + height-based rollback (no conflict copies):
  - detection: staged-base height vs current item height at upload time;
    always detected and logged
  - clobbered content is the inode's content at a prior consensus
    height; rollback/restore is provided by version-aware GC + blob
    retention (issue #26, an RFC-002 slice)
  - until retention lands, behavior equals today's macOS client (LWW,
    old blob orphaned) — but detection/logging ships from v1
  - rationale: heights are already the system's version identifiers;
    "(conflicted copy)" files pollute shared namespaces and teach users
    a behavior we intend to remove

## Provisioning & Lifecycle

- credentials: device token (RFC-012) in Secret Service (libsecret);
  fallback 0600 file under `$XDG_CONFIG_HOME/hopnet/`
- provisioning — both flows, chosen by deployment shape:
  - zero-touch (GUI mode, node in the same user session): node
    auto-registers a "Mount" device on login and writes credentials into
    the user's Secret Service — Linux arm of macOS
    `ensure_fileprovider_device_token`
  - manual (`hopnet-mount login`): paste `device_id.secret` from the
    DevicesPane UI — the Android model; required because headless nodes
    commonly run as a different user than the interactive session
- endpoint discovery:
  - headless: fixed :34632, nothing to do
  - GUI: ephemeral port → node writes `$XDG_RUNTIME_DIR/hopnet/endpoint`
    on startup (Linux arm of the macOS Keychain base_url refresh)
  - manual flow accepts an explicit URL at login time
- readiness: `/health` distinguishes connection-refused ("node not
  running") from `not_ready` ("running, not set up"); the daemon
  surfaces the difference rather than generic EIO
- lifecycle:
  - systemd user unit `hopnet-mount.service`; mountpoint `~/HopDrive`
    by default
  - on start: clean up stale mounts from a previous crash
    (`fusermount3 -uz`); on stop: unmount
  - node-unreachable: cached attrs keep answering; ops requiring the
    node fail with EIO after a short bounded retry — never indefinite
    hangs

## Shares

Shared-with-me content requires no mount-side handling. Accepting a share
places an ordinary inode in the recipient's own namespace (recipient-chosen
path, recipient's SIV key) whose data_id points at the shared blob; content
is live-linked (any member's modify propagates the new blob id to all
members' inodes and re-wraps access), metadata is per-user. There is no
read-only share concept. The mount therefore enumerates, reads, and writes
shared files identically to owned files. Pending share invitations
(incoming_shares) are not inodes and are not surfaced — accept/decline
stays in the SPA.

## Non-Goals

Each tracked, not forgotten:

- blob pinning / offline guarantees — issue #23 (substrate pin table
  exists; fetch-and-hold and surfaces missing)
- version retention + rollback machinery — issue #26 (mount ships
  conflict detection/logging only)
- chunk-delta modify — issue #25 (whole-file rewrite accepted)
- shared available-capacity abstraction — issue #24 (v1 statfs reads the
  existing view)
- thin-client remote mount (co-located node assumed; pinning semantics
  differ for remote mounts — revisit there)
- fanotify and GVfs/KIO backends (future, behind the same daemon)
- symlinks, hardlinks, xattrs
- trash semantics (macOS parity: unsupported)
- share invitation management (SPA concern)

## Implementation Phases

- [ ] Phase 1 — node surface: `/api/integrations/mount` routes, `/watch`
      SSE + ChangeNotifier subscribe seam, strict mutation barrier
- [ ] Phase 2 — read-only mount: fuser skeleton, id map/attr cache,
      daemon-mediated Range reads, sparse cache + whole-file fast path,
      disk-pressure eviction, poke-invalidation test suite
- [ ] Phase 3 — writes: durable staging, copy-up, background upload +
      fsync barrier, conflict detection/logging
- [ ] Phase 4 — provisioning & lifecycle: Secret Service + `login`,
      endpoint file, systemd user unit, statfs
- [ ] Phase 5 — passthrough acceleration, measured against the Phase 2
      baseline
- [ ] Phase 6 — desktop polish: Nautilus/Dolphin badges, context menus;
      packaging (Flatpak, deb, rpm, nix module)

Testing mirrors RFC-010: `HOPNET_EPHEMERAL_DB=1`, a test-mode route
minting a throwaway device token, poke counters (the signal-count
pattern); integration tests against a live mountpoint; pjdfstest subset
later. The poke-invalidation suite from the Architecture section is the
load-bearing one and lands with Phase 2, not later.

Both sides of the daemon⇄node boundary are testable in isolation, plus
together as a stack:

- **node surface alone** — the mount routes are plain axum handlers;
  HTTP tests against an ephemeral-DB node, no FUSE involved
- **daemon core alone** — the daemon talks to the node through a
  transport trait; a mock transport records every request/upload/
  invalidation the core *emits* and scripts the node's responses
  (including poke sequences and failure injection: dropped /watch
  connections, 503s, mid-upload crashes). Cache, staging, single-flight,
  and invalidation logic get deterministic tests with no kernel mount
  and no node. This seam is the same boundary the reusability
  discipline keeps HopNet-agnostic — testability and transplantability
  are the same cut
- **full stack** — real node + real mountpoint, POSIX ops observed
  end-to-end (the RFC-010-style harness above)

## Open Questions

1. `/watch` auth over long-lived SSE — device-token bootstrap sessions
   are short-TTL; reconnect-with-reauth cadence vs keepalive semantics.
2. Poke coalescing — a burst of consensus commits should not produce N
   pokes; debounce server-side or client-side.
3. Prefetch depth for sequential-access detection — measure, don't guess.
4. Pin/unpin surfaced through the mount (xattr command, sidecar CLI, or
   file-manager action) once issue #23 lands.
