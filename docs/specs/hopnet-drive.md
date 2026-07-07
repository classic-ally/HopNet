# RFC-015: Projection Modularity — hopnet-drive, hopnet-projection, hopnet-takeout

**Status**: Implemented (stages D0–D5 complete, 2026-07-08)
**Depends on**: RFC-013 (Malachite consensus), RFC-014 (storage substrate)

## Motivation

RFC-014 left the fs projection (inodes, encrypted paths, shares, provider
surfaces, takeout) tangled through the main crate. This RFC makes
projections modularly addable/removable: each owns its transaction
envelopes, consensus handlers, schema unit, HTTP surface, and takeout
translator. Adding a projection (photos is next) means adding a crate;
removing one removes its behavior, tables, and export sections cleanly.

## Architecture

```
hopnet (host: AppState, auth/sessions, users, devices, admin, net/iroh,
        consensus shims, storage maintenance/GC, bins, macOS glue)
   │
   ├─► hopnet-drive    (fs projection: inodes, path SIV, drive DB fns,
   │        │           envelopes + handlers, shares, FP/DP routers,
   │        │           schema unit, DriveExporter)
   ├─► hopnet-takeout  (projection-agnostic export/import: manifest v2,
   │        │           archive, work tables, resume, import gate core)
   │        ▼
   ├─► hopnet-storage  (RFC-014 substrate)
   │        │
   │   hopnet-projection (the seam crate: every contract below)
   │        │
   └────────┴──► hopnet-common          hopnet-consensus (height reads)
```
No dependency cycles; the host depends on everything, projections depend
only downward. The main crate keeps thin re-export shims at old paths so
bins (snapshotter, orchestrator) never churn.

## The seam crate: hopnet-projection

Owns every contract that must live BELOW both the host and all
projections (registries use `inventory` — cross-crate link-time
registration, guarded by a boot tripwire in the host asserting each
projection's `TX_FUNCTIONS ⊆ DISPATCH_TABLE` and provider count ≥ 1).

- **TransactionHandler** — narrowed handler contract:
  `process(&TxMeta, execute, &HandlerCtx, &rusqlite::Transaction)`.
  `TxMeta{function, payload, submitter_node, user_id}` is a
  signature-verified view; AppState and the full Transaction never cross.
  `HandlerCtx{fragments_dir, node_id, notifier, work}`.
- **ChangeNotifier** — post-apply change signal (host impl owns macOS
  FileProvider refresh + test_mode gating).
- **WorkScheduler** — handlers ENQUEUE named background work
  (`schedule(subsystem, key)`, execute-phase only); the host routes keys
  ("takeout.materialize", "takeout.cleanup") onto the MAIN runtime.
  Rationale: consensus apply runs on the malachite shell's
  single-threaded, time-only runtime — in-apply tokio::spawn used to
  land background work THERE, interleaving with consensus. AppState
  carries the main runtime Handle for this. schedule() fires pre-commit;
  scheduled tasks tolerate not yet seeing applied rows (brief retry).
- **DataBlockReferenceProvider** — GC registry: what blobs a projection
  still references (orphan cleanup consults all providers).
- **Host capabilities** (implemented by the host, consumed by
  projections/services): SessionAccess → UserSession{siv_key, siv_nonce,
  x25519_privkey} (ed25519 never crosses); TxGateway → sign-and-submit
  TxSpec batches (TxSigner::{Node, User}); shared Barriers,
  commit_timed histogram, CustomDateTime.
- **ProjectionExporter** — the takeout translator (below).

## hopnet-drive (fs projection)

- Model: Inode with `InodeOwner::Id(i32)` — a single-variant enum
  bincode-identical to the legacy `Either::Left` encoding (golden-byte
  test pins the wire; `Either::Right` was never produced and now fails
  decode).
- Schema unit: `db::install_schema/uninstall_schema/TABLES` (inodes,
  modification_log, incoming_shares, shares). Install chain: host DDL →
  hopnet-consensus → hopnet-storage → hopnet-drive → hopnet-takeout
  (order = FK direction; nothing FKs INTO a projection, so uninstall is
  a clean unit).
- Envelopes: DriveInsertPayload{blob_ops, inodes}, ModifyItemPayload,
  DriveContentUpdate, DeleteFilesPayload, share payloads — drive-owned
  wire types embedding hopnet-storage sub-payloads.
- Handlers registered from the crate: insert_files, modify_item,
  delete_files, share_file, accept_share, decline_share, unshare
  (+ FilesystemReferenceProvider).
- HTTP: files/shares/fileprovider/documentprovider routers over
  `DriveState{db_pool, fragments_dir, test_mode, node_id, sessions, txs,
  blobs, notify, write_admission}`; the host nests them under its
  JWT/device-token layers. BlobStreamer type-erases
  `hopnet_storage::api::get`; WriteAdmission consults the takeout import
  gate. Drive SQL may READ host tables (users) — the boundary is code
  ownership, not SQL isolation.
- Stays host: storage maintenance routes/GC jobs, FP health/test/signal
  handlers, domain/keychain objc glue, session/user/device management.

## hopnet-takeout (projection-agnostic export/import)

- **Manifest v2**: `{version: 2, takeout_id, created_at,
  source_username, projections: {name: {total_files, total_folders,
  total_bytes, entries: [{logical_path, kind, size, blob_id,
  content_hash, metadata}]}}}`. Archive: `manifest.json` +
  `{projection}/<logical_path>`. Per-entry `content_hash =
  blake3(plaintext ‖ blob_id)`, computed by the CORE while streaming.
- **ProjectionExporter** (per projection, host-registered as
  `Arc<dyn ProjectionExporter>` — impls hold runtime state):
  `enumerate(user)` → streaming entries (photos-scale);
  `open(user, entry)` → plaintext byte stream;
  `import_entry(user, entry, staged_path)` per entry +
  `flush(user)` per section (batching hook; drive's flush is a no-op —
  it submits per-entry consensus txs, batching is a later optimization).
  `ExportEntry.metadata` follows the sidecar principle: enough to
  reconstruct the projection's state from entries alone.
  `export_handle` is exporter-private (serde-skipped).
- **Skip-unknown-sections contract**: importing a manifest section with
  no registered translator marks that section's rows Skipped
  (`no_translator`) and never fails the import — a drive-only mesh
  imports the drive section of a drive+photos archive.
- Core owns: work tables (`takeout_entries_{id}`, `import_paths_{id}`
  with a projection column), archive/manifest, HTTP routes, resume
  registry + startup scan + login-hook resume, cron sweep, the import
  write gate. Enumeration happens in the scheduled materialize task
  (one-SQL-moment snapshot), NOT inside consensus apply.
- `TakeoutHooks` (host contract): import_completed (onboarding tx),
  plus storage-metric reads whose formulas stay in core.

## Extraction stages (as-built)

| Stage | Commit | Scope |
|---|---|---|
| D0 | 8969f10 | hopnet-projection crate; all 24 handlers narrowed; dispatch adapters; boot tripwire groundwork |
| D1 | b14476b | Schema seam: storage + drive install units; host chains installs |
| D2 | 610818a, b81d0c4 | Pure moves: Inode (owner narrowed, golden-pinned), DB layer, path SIV, FileError, envelopes |
| D3 | 41b39d8 | Handlers + reference provider cross the boundary; attestation vocab → storage; tripwire live |
| D4 | 426373c | HTTP surface behind five host seams; DriveHost adapter |
| D5 | d5dc88c, aa6a8d8 | WorkScheduler (Any-hatch dies; shell-runtime spawn hazard fixed); hopnet-takeout + manifest v2 + DriveExporter |
| D6 | (this doc) | Docs + full app-suite gate |

Every stage: workspace tests + all bins + snapshotter capture/compare
(IDENTICAL at every stage) + a per-stage orchestrator subset + divergence
check, committed only when green.

## Adding a projection (the recipe photos follows)

1. New crate depending on hopnet-projection (+ hopnet-storage for
   blobs): define envelopes, implement + `inventory::submit!` handlers
   and a DataBlockReferenceProvider, export `TX_FUNCTIONS`.
2. Ship `db::install_schema/uninstall_schema/TABLES`; host adds it to
   the install chain and the boot tripwire.
3. Expose axum routers over a projection state struct wired from the
   host-implemented seams.
4. Implement ProjectionExporter; host registers it — takeout support is
   automatic, including graceful skips on meshes without the projection.
