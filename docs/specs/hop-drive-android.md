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

## Future work

- [ ] Push-driven refresh via the mount `/changes` + `/watch` feed
      instead of notify-on-own-mutation only
- [ ] Retry queue for release-time upload failures
- [ ] Keystore-wrapped pairing storage
- [ ] Camera E2E on a physical device (emulator uses manual pairing)
