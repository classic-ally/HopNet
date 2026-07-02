# PhotoKit Spike Findings

Environment: macOS 26.3.1, Swift 6.3.3, library of 36,396 assets (35,185 images,
1,211 videos), iCloud Photos with Optimize Mac Storage (only ~1.5% of sampled
resources locally available).

Build note: nix sets SDKROOT/DEVELOPER_DIR globally to a pinned apple-sdk that
mismatches system Swift. Build with:
`DEVELOPER_DIR=/Applications/Xcode.app/Contents/Developer SDKROOT=$(xcrun --sdk macosx --show-sdk-path) swift build`

## Confirmed spec assumptions

### PHCloudIdentifier batch mapping scales (spec: fast path viability)
- `cloudIdentifierMappings(forLocalIdentifiers:)` over ALL 36,396 assets: **2.08s**, zero failures.
- Chunk of 1,000: 39ms. Reverse (`localIdentifierMappings(for:)`, recovery re-link path): 1,000 in 76ms.
- `stringValue` round-trips exactly; 69-char stable format (`UUID:001…`).
- Conclusion: full-library cloud-id mapping is cheap enough to run on every
  reconciliation scan. Fast path confirmed viable.

### fileSize KVC works, including for iCloud-remote assets (spec: storage-aware admission)
- 768 resources sampled across 506 assets: `value(forKey: "fileSize")` returned
  **nonzero for 100%**, including 760 resources with `locallyAvailable = false`.
- Bonus: undocumented `locallyAvailable` KVC key also works — usable signal for
  "this fetch will hit the network".
- Conclusion: expected-size admission works even for unmaterialized assets.
  Pessimistic-estimate fallback still specced but appears rarely needed.

### Resource enumeration matches the schema (mostly — see gap below)
- Live Photo: `photo` (1) + `pairedVideo` (9). Matches spec.
- Burst: `representsBurst = true`, `burstIdentifier` present. Matches.
- RAW+JPEG: `photo` (1, JPEG) + `alternatePhoto` (4, Sony ARW). Matches `raw_alternate`.
- adjustmentData (7) is a plist (`com.apple.property-list`, `Adjustments.plist`) —
  small, `.plist` ext as the spec guessed.

## Gap found: edited Live Photo has FIVE resources

An edited Live Photo enumerates:

| PHAssetResourceType | Meaning | Ingress mapping |
|---|---|---|
| 1 `photo` | original still | `original` (0) |
| 7 `adjustmentData` | edit recipe plist | `adjustment_data` (3) |
| 9 `pairedVideo` | original motion | `paired_video` (2) |
| 5 `fullSizePhoto` | **edited still render** | `edited` (1) |
| 10 `fullSizePairedVideo` | **edited motion render** | **NO SLOT — enum gap** |

- "Edited" current rendition is `fullSizePhoto` (5) / `fullSizePairedVideo` (10),
  not a mutation of the original resources. Originals stay intact (good).
- **RFC-011's resource enum has no value for an edited paired video.** Both specs
  need a new resource type (proposal: `edited_paired_video = 7`) or the edited
  motion render is silently dropped / would clobber the original `paired_video`.
- Detection of "has edits": presence of `adjustmentData` (7) among resources.

## Inconclusive / open

### Shared-library scope detection — RESOLVED (private API required)

Ground truth from Photos.app: personal = 25,314 photos + 942 videos;
Shared Photo Library = 9,871 photos + 269 videos. Total 36,396.

Probe sequence:

1. Default fetch: 36,396 assets — **SPL assets are included in default fetches
   and report `sourceType = typeUserLibrary`**. The public `sourceType` API
   cannot distinguish SPL from personal on macOS.
2. `typeCloudShared`-only fetch: 7,069 assets = exactly the sum of 19 legacy
   iCloud Shared Albums (`.albumCloudShared` collections). `typeCloudShared`
   means *legacy shared albums*, not the Shared Photo Library. These are also
   excluded from default fetches.
3. ObjC-runtime property sweep over PHAsset found the discriminator:
   **`participatesInLibraryScope`** (undocumented, KVC-readable, Bool).
   Full-library verification: exactly 10,140 true (9,871 photos + 269 videos) —
   digit-for-digit match with Photos.app. Legacy shared-album assets: 0/7,069 true.

Conclusions for the daemon:

- Scope detection = `asset.value(forKey: "participatesInLibraryScope") as? Bool`.
  **Private API dependency** — same tier as the `fileSize` KVC key. Failure mode
  if Apple removes it: KVC returns nil, which must be detected and treated as a
  hard error (fail loud), never defaulted to personal — silent default would
  route shared photos into the personal library subtree.
- Default fetch (no `includeAssetSourceTypes`) is exactly the right asset
  universe for the daemon: personal + SPL included, legacy shared albums
  excluded for free.
- **Decision (user)**: legacy iCloud Shared Albums are OUT of scope for ingest.
  They are typically downscaled copies and not part of the library proper.
- iCloud supports at most ONE Shared Photo Library per account, and PhotoKit
  exposes no scope identifier — the signal is binary. Descriptor enum is
  `Personal | Shared`, and `scope_binding` for the shared library is a fixed
  marker value rather than a real PhotoKit identifier. The multi-shared-library
  generality in the `libraries` schema remains for future non-PhotoKit sources.

### Full-library resource enumeration is expensive
- Calling `PHAssetResource.assetResources(for:)` per asset while scanning:
  ~20s for a partial scan (stopped early). Fine for one-time ingest;
  NOT fine per reconciliation scan.
- Design consequence (already consistent with spec): reconciliation relies on
  `cloud_id` fast path + `modificationDate`; resource re-enumeration only for
  assets that changed.

## Streaming — works; code 1005 = local disk pressure (verified both ways)

Initial state: all three download paths (`PHAssetResourceManager.requestData`,
`.writeData(toFile:)`, `PHImageManager.requestImageDataAndOrientation`) failed
instantly (~5–18ms, no network attempt) with `CloudPhotoLibraryErrorDomain
Code=1005 "(null)"` across 5 assets spanning 2016–2025. Ruled out: Bash sandbox,
bare-binary vs .app identity, per-asset corruption.

**Root cause, verified by before/after: local disk at 100% (3.4 GB free).**
`cloudphotod` refuses downloads below a local-headroom threshold. After freeing
~140 GB (deleting Rust `target/` caches), the identical probe succeeded 5/5.
The error code is publicly undocumented; the signature is instant local failure
with no network attempt.

Streaming behavior (healthy disk):

- ~1–4 MB originals download in 0.7–1.6s each.
- `requestData` delivers chunks of exactly **1 MiB** (last chunk short) — the
  natural FFI buffer size for the Swift→Rust boundary is 1 MiB as-is.
- `progressHandler` fires (~5 reports over a 2 MB file), usable for CLI display.
- **Delivered byte count matches the `fileSize` KVC value exactly** — admission
  sizing can trust `fileSize` as byte-accurate, not just advisory.
- PhotoKit caches the download: immediate re-request of the same resource
  completed in 2.3ms (local cache hit, no re-download).

Daemon consequences:
- Code 1005 is **local disk pressure, not a per-resource failure**: pause fetch
  admission (like `storage_low`), consume no retry counts, resume when headroom
  returns. A retry loop would spin uselessly.
- The daemon's local-headroom admission check (fetch_concurrency × largest
  asset) is not merely advisory — the OS enforces its own threshold with 1005.

Harness lessons: (1) some PhotoKit completion handlers deliver on the main
queue — a CLI harness must never block the main thread (use `dispatchMain()` +
background queue), or it deadlocks. (2) Swift `print` block-buffers when stdout
is a pipe — long-running probe output is invisible until flush (rescued once via
`lldb -p PID` + `expr fflush(0)`; use `setvbuf`/line buffering in long-running
harnesses). Each cost one debugging cycle.

## Change observer — per-asset incremental diffs (all five change kinds mapped)

Live test: observer registered on a full-library fetch result while the user
performed a scripted action sequence in Photos.app. Every action produced
`hasIncrementalChanges=true` with per-asset diffs — no full-reload events, no
rescan needed. Mapping to the spec's change classification:

| Photos.app action | Observer delivery |
|---|---|
| Favorite / unfavorite | `changed=1`, `isFavorite` readable directly on the changed asset. Metadata-only. |
| Edit (crop) a Live Photo | Several `changed` events during save; resource set transitions `[1,9]` → `[1,7,10,9,5]` — the five-resource edited Live Photo shape, confirmed live. |
| Revert to original | `changed=1`, resource set back to `[1,9]`. Edited renders + adjustment data disappear. |
| Delete | `removed=1` — the asset leaves the default fetch universe when it moves to Recently Deleted. Matches the spec's deletion trigger ("becomes inaccessible"). |
| Restore from Recently Deleted | `inserted=1` with the same `localIdentifier` — **restore looks like a brand-new asset**. Identity resolution (cloud_id match precedence) is what turns this back into the existing photo; a naive insert-handler would duplicate it. |
| Drag new file into Photos | `inserted=1` followed by `changed` events as ingest settles. |
| Move to Shared Library (and back) | **`changed` event, not remove+insert** — the asset stays in the fetch result; only its scope flips. Confirms the spec's scope-change detection design: known cloud_id arriving with a different `participatesInLibraryScope` value. |

Operational notes:

- **Events are redundant**: a single action can fire 2–4 near-identical events;
  `modificationDate` sometimes bumps with no visible state change. The pipeline's
  idempotent classification (match precedence + state diff) is required, not
  optional.
- Frequent "unrelated" change notifications fire for collection-level changes
  that produce no `changeDetails` for the asset fetch result — must be tolerated
  silently.
- Changed assets are handed over with current state; per-asset resource
  re-enumeration on change is cheap (only scanning all assets is expensive).
- Shared-library sync produces spontaneous `changed` events for assets the user
  didn't touch (other members' devices, sync state) — reinforces that observer
  events are hints to reconcile, not authoritative deltas.

## Not yet probed

- Burst frame default-fetch behavior (`includeAllBurstAssets`) — do non-pick
  frames appear in a default fetch?
- Offline (network-down) error shape for `requestData` — distinguishable
  from 1005?
