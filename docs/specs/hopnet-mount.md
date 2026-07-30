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
  - long kernel entry/attr TTLs; proactive invalidation on the changes
    feed. As of S4 the TTLs are pure backstops: freshness is poke-driven
    (consensus commit → broadcast → SSE poke → /changes sync → attr-cache
    refresh + kernel inval_entry/inval_inode)
  - poke coalescing (resolved open question): server side, a lagged SSE
    subscriber gets one poke (pokes are idempotent); daemon side, bursts
    drain into a single /changes sync
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
  - `GET /enumerate?parent_id=&cursor=` — children; stable cursor
    pagination (last-seen id, not page numbers), so FUSE readdir
    cookies resume sanely across concurrent mutation
  - `GET /lookup?parent_id=&name=` — single-child resolution; avoids
    enumerating large directories for one lookup
  - `GET /item?id=`
  - `GET /changes?since_height=N` — existing height-anchored delta feed
    - S4 fixed a pre-existing node bug this contract exposed: mutation
      handlers stamped modification_log from last_decided_height read
      DURING apply (lags the deciding block; also node-local, so
      live-apply vs catch-up could diverge). Heights now stamp the
      deciding block via HandlerCtx.height. Replicated-state derivation
      change: all mesh nodes must upgrade together
  - `GET /watch` — SSE change push
    - content-free "something changed" poke; daemon follows with /changes
    - heartbeat comment frames; on drop/reconnect the daemon resyncs from
      its last anchor height (no divergence window)
  - `GET /download?blob_id=` — blob-addressed, MUST honor HTTP Range
    (`reconstruct_file_range` already supports it; unlike the
    fileprovider route, this surface uses it)
    - blob-addressed (authorized via blob_access), not inode-addressed:
      POSIX open-handle semantics require snapshot-at-open — an open fd
      keeps reading the blob current at open() even if a (possibly
      remote) modify swaps the inode's data_id mid-read;
      inode-addressed download would tear content under the handle
    - scope (learned in S5): the snapshot pins what the DAEMON serves per
      handle; the kernel page cache is per-inode, so after inval_inode a
      fresh read can populate pages an older fd then sees — local
      overwrite-in-place visibility, consistent but not per-fd isolated.
      True isolation would need direct_io (page-cache and mmap costs) —
      deliberately not taken
    - consequence: a displaced blob under an open handle races RFC-007
      orphan cleanup — same keep-set family as pins/version retention
      (issues #23/#26)
  - `POST /create` (multipart: parent_id + folder_name | file_{size}),
    `PATCH /modify` (JSON rename/move), `DELETE /delete` (JSON)
    - strict consistency: respond only after the transaction is decided
      by consensus and applied locally — implemented on the queue's
      per-tx oneshot (settled from local committed_tx_nonces), NOT the
      test barriers (S6 correction: those are pause points, not height
      waiters)
    - responses carry the fresh post-apply item + decided height, the
      daemon's read anchor (fast-forward without treating its own write
      as a remote change)
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

## Implementation Slices

Boundary authored from the FUSE seat: S1 shapes the transport trait by
writing real FUSE callbacks against a mock; the node surface then
implements that contract. Each slice is PR-sized and lands green.

- [x] S1 — crate + transport trait + mock + fuser namespace skeleton;
      async bridge decided (2026-07-30)
- [x] S2 — node read surface: enumerate/lookup/item/blob download/
      changes/health (2026-07-30)
- [x] S3 — HTTP transport joins the halves; real tree browses read-only
      (2026-07-30)
- [x] S4 — /watch SSE + subscribe seam + kernel invalidation + poke
      test suite (2026-07-30; includes the modification-height stamping
      fix — deciding block height, not lagging last_decided)
- [x] S5 — content reads: sparse cache, whole-file fast path,
      snapshot-at-open, disk-pressure eviction (2026-07-30)
- [x] S6 — submit-and-wait-decided + strict mutation routes (node-side)
      (2026-07-30). The pipeline's per-tx oneshot already waited for the
      local commit on the proposer path; S6 threads the decided height
      through it (TxGateway::submit_batch_decided) and FIXES the
      forwarder path, which resolved on the remote proposer's Committed
      ACK before local apply — mutations now respond only after applied
      HERE, on every path. Timeout is a distinct outcome-unknown error
      (504), never success. Follow-up noted: fileprovider/
      documentprovider mutations still use the legacy non-height wait;
      migrating them is a small, separate change.
- [ ] S7 — writes: staging, copy-up, release/fsync tiers, conflict
      logging, startup recovery
- [ ] S8 — provisioning & lifecycle: secrets, login, endpoint file,
      systemd unit, statfs
- [ ] S9 — passthrough quarantine module + measurement
- [ ] S10 — desktop badges, context menus, packaging

Testing mirrors RFC-010: `HOPNET_EPHEMERAL_DB=1`, a test-mode route
minting a throwaway device token, poke counters (the signal-count
pattern); integration tests against a live mountpoint; pjdfstest subset
later. The poke-invalidation suite from the Architecture section is the
load-bearing one and lands with S4, deliberately before the content
machinery.

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
   (S4 v1: auth at request time only; connection outlives the session.)
2. Prefetch depth for sequential-access detection — measure, don't guess.
3. Pin/unpin surfaced through the mount (xattr command, sidecar CLI, or
   file-manager action) once issue #23 lands.
