# RFC-016: Projection Host API

**Status**: Implemented (stages 1–6 complete, 2026-07-08)
**Depends on**: RFC-014 (storage substrate), RFC-015 (projection modularity)
**Amended by**: RFC-019 (snapshot seam — `snapshot_section` /
`node_local_tables`, 2026-08-01)

## Motivation

RFC-015 made projections crates, but not plug-and-play: adding or
removing one still touched ~6 host files (schema install chain, boot
tripwire, five named router mounts, seam adapter construction, takeout's
exporter vector, the work-scheduler match). This RFC collapses every
host integration point onto one registry, so we — and third parties —
target a single API to build projections over the replicated state
machine + storage substrate. Adding a projection to the host is now:
implement `hopnet_projection::Projection`, add ONE entry in
`src/projections.rs::manifests()`.

## Layering decision (the dependency-direction fork)

`BlobStreamer` is defined over `hopnet_storage::store::BlobManifest` and
`hopnet_storage::StorageError`, so hosting it in hopnet-projection
requires **projection → storage**. The competing idea — the storage
substrate's tx handlers registering themselves from hopnet-storage via
the inventory seam — requires storage → projection. Both cannot hold.

Chosen: **projection → storage**.

```
hopnet (host: composition root)
   │
   ├─► hopnet-drive ─┐        hopnet-takeout ─┐
   │                 ▼                        ▼
   ├─────────► hopnet-projection (seam crate + host capabilities)
   │                 │
   ├─────────► hopnet-storage (RFC-014 substrate; NEVER deps projection)
   │                 │
   └────────────► hopnet-common        hopnet-consensus (height reads)
```

Consequences, recorded deliberately:

- The full `HostCapabilities` bundle (including blob streaming) lives
  below every projection — the point of the RFC.
- hopnet-storage stays dependency-light (the most reusable crate).
- **The 3 storage tx handlers stay HOST-side** (`src/storage_host/`),
  not in hopnet-storage. This is not debt: their real apply logic
  already lives in `hopnet_storage::store` (RFC-014); the host shims are
  decode-authz-delegate only, and the remainder CANNOT descend —
  `delete_orphaned_data_blocks_consensus` gates on
  `has_active_takeout_tx` inside the consensus-deterministic GC path
  (storage can never dep takeout), and `&'static dyn` inventory handlers
  cannot capture runtime closures to inject that check. The boot
  tripwire covers them via a named `storage_host::handlers::TX_FUNCTIONS`
  check beside the manifest loop.

## The manifest

```rust
// hopnet-projection
pub trait Projection: Send + Sync {
    fn name(&self) -> &'static str;
    fn tx_functions(&self) -> &'static [&'static str];
    fn install_schema(&self, conn: &rusqlite::Connection) -> Result<(), rusqlite::Error>;
    // Runtime surface — default-empty; caps passed per call, never stored:
    fn exporter(&self, caps: &HostCapabilities) -> Option<Arc<dyn ProjectionExporter>>;
    fn mounts(&self, caps: &HostCapabilities) -> Vec<Mount>;
    fn work(&self, caps: &HostCapabilities, subsystem: &str, key: String)
        -> Option<BoxFuture<'static, ()>>;
    // RFC-017 additions:
    fn committed_blob_ids(&self, function: &str, payload: &[u8]) -> Vec<BlobId>;
        // distribution kick — pure decode of the projection's OWN envelopes
    fn user_data_size_bytes(&self, caps: &HostCapabilities, user_id: i32)
        -> BoxFuture<'static, Result<u64, String>>;
        // takeout/import quota sizing, summed across manifests
    // RFC-019 additions:
    fn snapshot_section(&self) -> Option<&'static SectionSpec>;
        // this projection's slice of the canonical state snapshot —
        // covered tables for divergence hashing + epoch export
    fn node_local_tables(&self) -> &'static [&'static str];
        // tables outside the snapshot universe; the host's registry test
        // pins covered ∪ node-local == sqlite_master
}
```

RFC-017 also added `hopnet_projection::current_height(conn)` — the
canonical consensus-height reader (wraps hopnet-consensus's SQL); the
host adapter is now `src/capabilities.rs` (`CapabilityHost` /
`build_capabilities()` — renamed from drive_host: nothing in it is
drive-specific). See [RFC-017](hopnet-comms.md).

Manifests are **unit structs** (`&'static dyn Projection`). This is
load-bearing: schema install and the boot tripwire run BEFORE the host's
AppState (and therefore HostCapabilities) exists — from `main` and the
consensus test harness alike. Runtime methods take `&HostCapabilities`
per call; construction is cheap Arc clones.

```rust
// src/projections.rs — the whole host diff for a new projection
pub fn manifests() -> &'static [&'static dyn Projection] {
    &[&hopnet_drive::DriveProjection, &hopnet_takeout::TakeoutProjection]
}
```

The registry drives:

1. **Schema chain** (`src/db/shared.rs`): named
   `hopnet_storage::store::install_schema` first (substrate is below the
   seam), then each manifest in slice order — which IS the FK order.
2. **Boot tripwire** (`src/lib.rs::assert_projection_registrations`):
   every manifest's `tx_functions` ⊆ DISPATCH_TABLE, plus the named
   storage_host check, plus reference-provider count ≥ 1.
3. **Router mounts** (`src/main.rs`): see below.
4. **Takeout translators** (`src/takeout_host.rs`):
   `manifests().iter().filter_map(|m| m.exporter(&caps))`.
5. **Background work** (`src/handlers.rs::HostWorkScheduler`): unknown
   subsystems are offered to each manifest; the first `Some(future)` is
   spawned on the MAIN runtime (consensus apply runs on the malachite
   shell's time-only runtime — IO work must not land there).
6. **Snapshot sections** (`src/db/snapshot.rs`, RFC-019 S2): the named
   substrate trio (host, consensus, storage) first, then
   `manifests().filter_map(snapshot_section)`; `node_local_tables()`
   unions likewise. Adding a projection's state to the divergence
   universe and the epoch snapshot is zero host lines.

## HostCapabilities

```rust
pub struct HostCapabilities {   // Clone
    pub db_pool: r2d2::Pool<SqliteConnectionManager>,
    pub fragments_dir: String,
    pub test_mode: bool,
    pub node_id: Arc<OnceCell<i32>>,
    pub sessions: Arc<dyn SessionAccess>,     // SIV keys + x25519; ed25519 never crosses
    pub txs: Arc<dyn TxGateway>,              // host-signed consensus submission
    pub blobs: Arc<dyn BlobStreamer>,         // type-erased api::get
    pub notify: Arc<dyn ChangeNotifier>,      // post-apply change signal
    pub write_admission: Arc<dyn WriteAdmission>,  // import gate
}
```

Drive's `DriveState` is now `pub type DriveState = HostCapabilities;`.
The `write_gate` axum middleware lives in hopnet-projection, typed over
HostCapabilities — every projection's write sub-router reuses it
(missing user → 401, denied → 409, check failure → 500).

`WriteAdmission`'s host impl queries takeout's per-user import flag
directly. That is correct, not a leak: the host is the composition root
and legitimately deps takeout. The RFC-015-era bug was hopnet-drive
OWNING the trait; projections now only ever see the seam.

## Mounts

```rust
pub enum AuthClass { UserJwt, DeviceToken }
pub struct Mount {
    pub prefix: &'static str,   // FULL path; always nested
    pub auth: AuthClass,
    pub router: axum::Router,   // Router<()>
}
```

Routers own their internal layers (write gate, body limits — the drive's
files and fileprovider routers carry their own 5GB limits). The host
adds exactly: the declared auth class's middleware, then its global
layers (DB-capacity gate, load-shed/concurrency, trace, CORS-debug).

axum 0.8 detail that shaped `main.rs`: `Router<AppState>::nest/merge`
only accepts `Router<AppState>`, so the host closes its own routes over
AppState (`with_state`) FIRST, then the mount loop nests each projection
`Router<()>` under its auth layer, then the global layers wrap the
combined router. Global-layer order is unchanged (they always applied
after all merges).

## Deliberately NOT generalized

- **Takeout's service wiring** stays named host code: /takeout +
  maintenance mounts, apalis cron, startup resume scan, login-resume
  hooks (GUI auto-login + device-token session bootstrap), barrier
  registration, `AppState.takeout_runtime`, and the two named
  work-scheduler arms (they need TakeoutState + a post-commit
  row-visibility retry). Takeout is a projection-agnostic SERVICE whose
  state (translators collected from OTHER projections, host SQL hooks)
  is not expressible from generic capabilities; a Service trait at n=1
  would obscure load-bearing detail. Takeout still implements the
  static trio of `Projection` (it has handlers + schema), so the
  tripwire and install chain cover it through the same loop.
- **Storage handlers + GC apply fns** stay in `src/storage_host/`
  (rationale above; the fns live in `storage_host/db_apply.rs` beside
  their only callers).
- **Pure db shims** (`hopnet::db::{files,shares,fileprovider,
  documentprovider}`) stay — the snapshotter consumes them; deleting is
  churn with no photos payoff.

## Stages (as-built)

| Stage | Commit | Scope |
|---|---|---|
| 1 | 40411e8 | BlobStreamer/WriteAdmission + error types descend to hopnet-projection |
| 2 | 34a486a | HostCapabilities; DriveState → alias; write_gate descends |
| 3 | 45d6a31 | Projection trait; manifests() registry; schema + tripwire + exporter loops; storage TX_FUNCTIONS |
| 4 | a901ba4 | Mount/AuthClass; drive's four mounts via loop; main.rs tail restructure |
| 5 | fa445fb | Projection::work dispatch fallthrough |
| 6 | 9070521 | src/files → src/storage_host; GC apply fns beside handlers |
| 7 | (this doc) | Docs + final smoke |

Every stage: workspace tests + all bins + snapshotter capture/compare
(IDENTICAL at every stage) + orchestrator subset + divergence check,
committed only when green. Stage 4 (the middleware restructure) passed
the full 11-test drive suite at 284/284 checks.

## Adding a projection (the recipe photos follows)

1. New crate depending on hopnet-projection (+ hopnet-storage for blob
   payloads): envelopes, `inventory::submit!` handlers +
   DataBlockReferenceProvider, `TX_FUNCTIONS`, a
   `db::install_schema/uninstall_schema` unit, axum routers over
   `HostCapabilities`, a `ProjectionExporter` if takeout should cover it.
2. Export a unit-struct manifest implementing `Projection` (static trio
   mandatory; exporter/mounts/work as needed).
3. Add ONE line to `src/projections.rs::manifests()` (slice position =
   schema FK order). That's the whole host diff — schema install,
   tripwire, mounts, takeout translation, and work dispatch all follow.
