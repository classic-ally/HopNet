# Hop Drive — Android SAF Client

**Status**: v1 implemented (2026-08)
**Depends on**: RFC-022 (pinned HTTPS), RFC-012 (device token sessions), RFC-018 (mount surface)

## Summary

`android/HopDrive` (`app.hopnet.drive`) exposes a paired HopNet node
through Android's Storage Access Framework: a DocumentsProvider whose
every call speaks live to the node — no local store, no sync engine.
Pairing is QR payload v1 (or manual entry); transport is pinned HTTPS
authenticated by SPKI fingerprint plus device token.

## Design decisions

### Split by surface: read DP, write mount

Reads (`queryDocument`/`queryChildDocuments`/`openDocument r`) use
`/api/integrations/documentprovider/*`. Every mutation uses
`/api/integrations/mount/*` instead, because the mount surface is
**strict-wait**: responses return only after the consensus transaction
is decided and applied on the serving node, carrying the authoritative
post-apply item. Consequences:

- `createDocument` gets its inode id synchronously (SAF requires an id
  immediately) — no re-enumerate polling anywhere in the app.
- Name collisions are clean 409s → SAF-conventional mangling
  (`report.txt` → `report (1).txt`).
- Content writes to existing documents (`FLAG_SUPPORTS_WRITE`) ride
  `PUT /mount/content`; the DP upload route cannot express this (its
  inode INSERT 500s on a path collision).
- Recursive folder delete comes from `DELETE /mount/delete`.

### Live-only, ephemeral state

The only client-side state besides the pairing is an in-process
child→parent map (fed by every response) that keeps `isChildDocument`
off the network, and per-write temp files in the cache dir. Transient
failures surface as `EXTRA_ERROR` cursor banners; unpaired = no SAF
root at all (the root appears via a roots-URI notify at pair time).

### Proxy file descriptors

Per-open `HandlerThread` (network must never run on the main looper).
Reads stream sequentially and reopen with `Range: bytes={offset}-` on
seek — the reason the DP download endpoint grew Range support.
Writes buffer to a cache temp file and upload once at `onRelease`;
`onFsync` only marks dirty (a per-fsync upload would mint a consensus
blob per sync). Two documented windows:

- the writer's `close()` returns before `onRelease` uploads, so
  immediate read-back needs the parent-notify or a short wait;
- a release-time upload failure cannot reach the already-closed writer:
  it is logged loudly and the temp file kept. A retry queue is future
  work.

### Trust

`SpkiPinningTrustManager`: SHA-256 over the leaf certificate's
SubjectPublicKeyInfo DER compared against the pinned fingerprint —
no chain, validity, or hostname checks (the pin IS the trust,
RFC-022). Pairing lives in app-private SharedPreferences with
`allowBackup=false`; Keystore wrapping is future hardening. QR
scanning is ZXing core over CameraX frames (rowStride-aware Y-plane
copy) — deliberately no Google Play Services dependency.

## Verification

- `PairingPayloadTest` (JVM): payload v1 acceptance/rejection.
- `LiveNodeTest` (instrumented, run against a real node):
  `./gradlew connectedDebugAndroidTest` with
  `-Pandroid.testInstrumentationRunnerArguments.{host,port,spki,token}`;
  covers root exposure and the full lifecycle (create, write, ranged
  read-back, rename, move, recursive delete) with same-UID provider
  access. On NixOS build with
  `-Pandroid.aapt2FromMavenOverride=$ANDROID_HOME/build-tools/<v>/aapt2`
  and pin `buildToolsVersion` to the nix-provisioned SDK.

## Change feed (push refresh)

`net/WatchLoop.kt` — one daemon thread holding the node's SSE poke
stream (`GET /mount/watch`), started lazily by any provider entry point
and self-stopping after 3 idle minutes; the provider process only lives
while clients hold cursors, so the connection's cost is bounded to
active browsing (no service). Shape mirrors the FUSE daemon's watcher:
sync on every (re)connect, coalesce poke bursts (75 ms drain — also
skips the node's poke-fires-pre-commit window), anchor on each delta's
`height` (first-connect sentinel exactly `i64::MAX`; the server's
height bit-cast makes larger values match everything). The delta's
`items`/`deleted_ids` map to `notifyChange` on affected children URIs,
using the shared child→parent map for move/delete old-parent
invalidation. Pokes are node-scoped; `/changes` filters per-user.
`/changes` reads its delta and height anchor in one SQLite snapshot
(fix landed with this feature) so a poke-driven client can trust the
anchor. Each open watch holds one API concurrency slot — fine at
personal-device scale.

**Cached-app freezer constraint** (verified on a Pixel 9a): while
another app (DocumentsUI) browses the provider, our process is cached
and Android freezes it — threads stop and sockets abort, so the SSE
connection cannot be held. The loop therefore degrades to
sync-on-interaction: every binder call (any navigation or query) thaws
the process, the loop reconnects and catches up immediately, and its
notifyChange refreshes the open listing. True dwell-on-screen push
works only while the process is unfrozen — the Hop Drive app in the
foreground, or the post-interaction grace window. Fixing this fully
would require a foreground service (rejected: persistent notification)
or FCM (rejected: no Google Play Services); the failed reconnects while
frozen are harmless (backoff caps at 30s and the process is asleep for
most of it).

## Future work

- [ ] Retry queue for release-time upload failures
- [ ] Keystore-wrapped pairing storage
- [ ] Camera E2E on a physical device (emulator uses manual pairing)
