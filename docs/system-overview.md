# HopNet System Overview

## Vision
**"A distributed filesystem designed for everyone, from enthusiast to enterprise"**

HopNet provides secure, reliable, performant file storage across networked devices using Byzantine fault-tolerant consensus and Reed-Solomon erasure coding. Users can create private networks spanning their devices, share with friends and family, or deploy enterprise-wide distributed storage solutions.

## Core Systems

### 1. Consensus System ([RFC-013](specs/malachite-consensus.md) — plan in `~/.claude/plans/spicy-imagining-mango.md`)
**Status**: Migrated to the Malachite (Tendermint) engine; final gates (shared with the RFC-014 extraction) before merge

Byzantine fault-tolerant consensus via the `hopnet-consensus` crate: Malachite's
Quint-verified Tendermint core (`arc-malachitebft-core-consensus`, tier-1 effect
API) driven by a sans-io host with SQLite WAL + one-transaction decides. The
bespoke HotStuff-2-style engine (RFC-001, retired) was deleted at Stage 5b after
an audit found a view-change safety hole. See RFC-013 for the full design
(quorum-profile theorems, on-demand heights, WAL/replay, decided-value sync).

- [x] Tendermint consensus (Malachite engine, PartsOnly proposals, Rule-8 validation before voting)
- [x] Decided-block storage + WAL crash recovery in one SQLite transaction
- [x] BFT (2/3) and CFT majority (1/2) quorum profiles, genesis-fixed per mesh
- [x] On-demand heights: idle meshes are fully quiescent; wake on local work or peer messages
- [x] Decided-value sync (replaces view catch-up); trusted height-0 join bootstrap
- [x] Event-driven transaction queue (PendingPool + engine driver, proposer forwarding)
- [x] Transient-storage-error classification (2026-08-16): `DatabaseError::Transient`
      threaded from the DB layers through every handler crate; SQLITE_BUSY during the
      proposer's preflight restages the transaction (bounded, then 503) instead of
      rejecting it (the 409→EEXIST rsync data-loss path), and block validation returns
      Undetermined + retries on a bounded IMMEDIATE transaction instead of voting
      Invalid (ends the false SyncInvalid determinism alarms)
- [x] Shell-wedge fix (2026-08-17): the third instance of the transient-error defect
      class — a BUSY *acquiring* the validation retry transaction became a fatal
      `HostError` and stopped the consensus shell without tripping the abort guard
      (zombie: HTTP up, `/health` Ready, chain dead; two of three nodes within 45 min
      under rsync load). `StoreError::is_transient` + a `Storage::error_is_transient`
      seam classify all four validation sites into the existing Undetermined backstops;
      the shell's fatal arm now aborts the process so supervision restarts it; the
      shell exposes a liveness flag that `/health` folds into Ready/NotReady alongside
      a new `consensus_height` payload field; the feature-gated shell test suite now
      runs in CI with an end-to-end contention-wedge guard
- [x] Deterministic simulation + seeded fault fuzzing (200-seed safety corpus, wake-rule tests)
- [x] Validator set management with height-based activation
- [x] Performance metrics integration for node reliability (latency + throughput measurement complete)
- [x] Orchestrator test suite rebuild (Stage 6: self-hosted iroh relay, leader-down, CFT meshes, barrier taps)
- [~] Post-extraction gates: bench trio + full app-suite (streaming 36.6 MB/s and
      pragma benches pass; consensus-queue-throughput 68.7% vs the 80% bar —
      admission tuning is the open follow-up)
- [x] Node health monitoring and automatic validator management —
      MODEL-CHECKED + IMPLEMENTED.
      [RFC-CONSENSUS-001](../hopnet-consensus/spec/validator-membership.md)
      (2026-07-16, Quint/Apalache) specifies the policy;
      [RFC-CONSENSUS-002](../hopnet-consensus/spec/implementation-plan.md)
      (2026-07-17, S0–S6) implements it: voluntary leave, a per-peer
      evidence layer (deadline probes, cert-participation refresh),
      unreachability vote-out with Live-origin subjective attestation,
      mesh-initiated batch seating (parity/posture/proven-quorum
      ceiling), and the AUTO quorum profile (per-height thresholds via
      driver replacement at the seam). All orchestrator gates green
      (graceful-leave, evidence-observe, vote-out-after-kill,
      mesh-growth, auto-seam, consensus-bft-quorum-loss), divergence
      clean.
- [~] Regenesis: epoch compression + coordinated upgrades — SPECIFIED
      ([RFC-019](specs/regenesis.md), 2026-07-30): operator-initiated
      epoch boundaries (drain → seal → certified snapshot → fresh
      genesis at a CONTINUOUS height) discharging catchup cost and
      history storage; the decide certificate of the final block IS
      the snapshot certificate. Landed: S0 app-layer u64 heights
      (lossless SQLite bit-cast mapping); S1 canonical snapshot
      serializer (domain-tagged binary encoding, per-module
      SNAPSHOT_SECTION registry pinned against sqlite_master,
      decided_blocks divergence-checked but never exported, golden
      tests) with the orchestrator divergence check rebuilt on the
      manifest's top hash — and now a real gate that fails the run on
      divergence; S2 Projection snapshot seam (RFC-016 amendment:
      projections declare their own sections) + fresh-DB artifact
      import, byte-identical roundtrip gate green; S3 staged versions
      (CalVer integer codes, Cargo.toml authoritative; per-node
      running/staged attestation columns via the node_staged_version
      tx) + v1 git-release upgrade provider (reports
      available-but-unstaged from the Forgejo releases API) +
      /api/views/upgrade-readiness advisory; S4 epoch-boundary model
      (`epoch_policy` in validator_membership.qnt: the restart lands
      inside the membership machine's inductive invariant, so the
      whole safety matrix transfers across the boundary depth-free;
      seal safety via inductively-checked ghost) + the seal contract
      note (hopnet-consensus/spec/regenesis-seal-contract.md); S5
      boundary protocol — regenesis_start/commit/abort txs over the
      regenesis_state singleton (seated-validator authz, staged-target
      precondition), the layered freeze (503 + structured refusal →
      queue-chokepoint admissibility seam → vote-time solo/binding/
      vote-iff-match/sealed checks → engine halt at terminal H),
      drain watcher + proposer commit injection (snapshot_hash =
      blake3 of the artifact bytes), durable seal marker + byte-
      certified artifact next to the database; evidence at four
      layers (handler/vote-time units, seeded sim drain+seal
      scenarios, real-engine loopback e2e, orchestrator scenario);
      S6 genesis construction + restart — deterministic
      EpochGenesisRecord in a genesis Block at boundary height H
      (chain_id(N+1) = its hash; the node-local decide certificate
      rides BESIDE the genesis in the forever-kept lineage record),
      pre-pool boot transition (`regenesis::boot`: VERSION/LINEAGE/
      IMPORT/CARRY gates, one-transaction fresh-DB build, atomic
      swap, rollback window until the first epoch-2 decide), derived
      restart (exit 75 at version match; awaiting-upgrade parked
      ALIVE otherwise, surfaced on the status view), (epoch, version)
      handshake on the status probe + DecidedFetch, test-gated
      version-override seams; evidence: boot/genesis units incl. a
      canonical-bytes golden, in-process single-node transition
      roundtrip (epoch 2 decides H+1 and seals itself again),
      orchestrator regenesis-restart (full cycle, exit 75, epoch-2
      coherence) + regenesis-awaiting-upgrade (parked-alive, per-node
      swap, liveness gate).
  - S7 — stragglers + rejoin: a `regenesis` comms scope serving
      lineage records and the boundary snapshot in resumable 4MiB
      chunks (engineless, so parked and sealed nodes rescue
      stragglers); hop-by-hop lineage-chain verification with the
      weak-subjectivity overlap rule (thresholds from the active
      quorum profile, never hardcoded); stage-then-restart for a
      straggler, reusing the S6 boot rebuild, with the version gate
      ahead of anything schema-touching; in-process epoch join for
      fresh nodes, which subsumes the height-0 bootstrap; a manual
      re-trust route for churn past the overlap window; post-import
      fragment reconcile. Evidence: overlap/chain units, staged-boot
      gate battery, an in-process straggler rejoin over real comms
      ending in byte-identical state, and the orchestrator
      straggler-rejoin / diverged-node-rebuild scenarios.
  - S8 — upgrade epoch end-to-end: the version-bump flow was already
      covered by S6's awaiting-upgrade scenario, and a second real
      image was rejected as theatre for a no-op bump (identical
      schemas — nothing for it to catch). The substance was the
      rollback window, which had never been exercised and whose
      documented procedure did not work: restoring the retained
      database by hand leaves the sealed marker and committed phase
      in place, so the next boot either re-crosses the boundary or
      parks with no consensus. Rollback is now a marker the boot path
      honours ahead of every other path, covering the crossed and the
      never-crossed node alike, resumable across a crash, and exposed
      as an authenticated route. Evidence: boot units per arrangement
      and per crash point (incl. a regression pinning the old
      re-cross), and the orchestrator regenesis-rollback drill —
      abandon mesh-wide, prove the mesh RUNS again, then prove the
      window is refused once the next epoch decides.
  - **RFC-019 is complete**, with one round of post-review remediation on
    the joiner's trust boundary. A lineage record is PEER DATA and only
    its certificate is evidence, so every record field a joiner acts on is
    now cross-checked against the boundary block's `regenesis_commit`
    (`snapshot_hash`, `seal_height`, `target_version_code` all live inside
    the certified block for exactly this reason). Operator re-trust
    SUBSTITUTES an out-of-band chain-id fingerprint for the overlap rule
    instead of waiving it — waiving left each hop verified against the
    validator set declared inside its own record, i.e. self-certifying.
    And the sealed marker is derived from the committed phase, so a crash
    between the decide and the seal work can no longer leave a node
    running on a retired chain.
  - **RFC-020 redesigned post-regenesis**
    ([RFC-020](specs/module-versioning.md), Draft 2026-08-16): schema
    evolution via per-module append-only migration chains — replay is
    the only installer, epoch boundaries fast-forward, and a one-time
    cutover exits the `initialize` regime. Implementation not
    started; ships as one PR (S1–S6 staged), merged only when the
    cutover slice is ready.
  - **RFC-025 drafted**
    ([RFC-025](specs/rpc-version-enforcement.md), Draft 2026-08-17):
    RPC version enforcement — locked scopes refuse mixed-version
    peers at the ALPN (mesh magic + exact CalVer), compat scopes
    (status, regenesis, ping) serve a two-generation window so
    stragglers stage across boundaries; typed refusals replace
    silent bincode misdecode. Implementation not started; one PR
    (S1–S6), the following release is the enforcement cutover.

### 2. Storage Substrate ([RFC-014](specs/hopnet-storage.md)) + File Storage ([RFC-002](specs/file-storage.md))
**Status**: Substrate extraction COMPLETE (stages A–F, 2026-07-07) — the `hopnet-storage`
crate owns blobs end to end; the fs layer (RFC-002) is a projection over it

The distribution substrate: durable, location-transparent, ENCRYPTED blobs over the
consensus state machine. `hopnet-storage` owns the ciphers (format-frozen,
golden-vector pinned), key custody (pubkey-keyed `blob_access` wraps, mesh-wide
keypair for all-users access), the Reed-Solomon fragment format, deterministic
placement (blob_id-seeded), the distribution engine, the download path (`api::get`:
inventory → placement-directed → gossip discovery ladder, RS reconstruction, keyed
integrity verify), and tier-1 pull-only repair. The main crate reaches it through
four host seams (Transport / StateReader / TxSubmitter / LocalStateSink) and sync
apply functions inside consensus handlers.

- [x] Substrate key custody: v1 pubkey wraps, mesh keypair (genesis + insert_user
      grants), keyed integrity hash (plaintext-confirmation oracle fixed)
- [x] Control-plane apply functions + drive-scoped envelopes; stored_locally
      settlement through crate-owned writers only
- [x] Distribution engine behind seams (event-driven on_decided kick, global
      workers, batched placement commits) + fragment RPC serve half
- [x] api::get + blob manifest reads + tier-1 repair (rebalancer re-enabled)
- [~] Photos ingress as projection #2 (post-merge; encryption for free) —
      Phase 1 schema + handlers + distribution hook + cleanup landed
      2026-07-28: `hopnet-photos` crate (RFC-016 manifest) installs 9
      consensus-tracked tables with zero plaintext photo metadata (group
      fields folded into `encrypted_metadata`); `photo_add`/`photo_delete`/
      `photo_restore`/`photo_cleanup_expired` consensus handlers;
      `committed_blob_ids` hook for fragment distribution; `PhotosReferenceProvider`
      pins blobs with UUIDv7-timestamp-filtered edit-history retention;
      daily randomized apalis cron (`photos_host::spawn_tombstone_cleanup_worker`)
      scans expired tombstones, batches 50 IDs per tx, handler verifies 30d
      window deterministically via `scan_cutoff` in payload. 39 total tests.
      `Projection::tables` trait method feeds the divergence checker from
      each crate's `db::TABLES` const — no hardcoded table list to drift.
      `register_uuid_extract_timestamp` moved to `hopnet-common::db_impl`.
- [~] Photos server-side wiring (Track C, 2026-07-28): per-user sidecar
      (`hopnet-photos-core::sidecar::SidecarDb`) with X25519-recipient
      metadata decryption; `src/photos::PhotosHost` enables/disables
      sidecars and runs a 30 s sync worker over `read_photo_changes`;
      axum routes: `/api/photos/{gallery,recently-deleted,{id},
      transaction,sync,sidecar/{status,enable,disable,reinit}}`.
- [x] Photos shared ingest model: `hopnet-photos-core::asset` defines
      source-independent identities, RFC-011 resource kinds, resource
      descriptors, and validation.
- [x] Photos content serving + gallery (2026-07-30): `GET /api/photos/
      {id}/resource/{type}` streams decrypted resource bytes — blob_access
      wrap as the read grant (404/403 split), soft-deleted photos still
      serve during the 30-day window, Range/206 support, ETag
      `"{data_block_id}"` + private/immutable caching with 304
      revalidation; keyset browse page + month histogram endpoints;
      ingress viewer components folded into the main frontend (windowed
      grid, histogram rail, filters, lightbox) over a module-scope
      authenticated blob cache keyed by data_block_id; manual multipart
      ingest route `POST /api/photos` runs the full publisher server-side
      (2026-07-30). Seeding suite (2026-07-30): `photo-seeder` bin +
      `hopnet::dev_seed` (deterministic synthetic photos over HTTP,
      targets a dev node or mesh nodes), `photos-upload-consistency`
      orchestrator test (first cross-node validation of the photos
      pipeline), `orchestrator creds` for browser sign-in. Deferred:
      favorites (Phase 4), video Range streaming. (Shared libraries have
      since landed — see the Phase 3 entries below.)
- [x] Photo publisher (2026-07-30): `hopnet-photos-core::publisher`
      turns a validated `PhotoAsset` + byte streams into an encrypted
      `photo_add` — exact-length streaming upload (no staging copy),
      per-recipient key wrapping, `PartialPublish` reconciliation
      contract; `PhotoDispatch` gains the upload pipe
      (`upload_data_block`, `fetch_library_members`), implemented on the
      node-local `Submitter` with `uploaded_by` derived from
      authenticated dispatch state.
- [x] Thin-client dispatch routes + ingress publisher adapter, Rust
      slice (2026-07-30): device-token-authenticated
      `/api/photos/client/*` surface (membership, streaming data-block
      upload with inline declared-length enforcement, transaction relay,
      committed-state confirm probe); ingress-core publish queue
      (`published_at` ledger, park-on-unreachable daemon tick, inflight
      deferral); `crates/ingress-publisher` (sidecar→PhotoAsset mapping,
      `HttpDispatch`, confirm-then-retry `NodePublisher`, e2e driver
      bin); `photos-ingress-publish` orchestrator scenario. Swift/FFI
      wiring landed 2026-07-30 (publish credentials via FfiDaemonOptions,
      keychain provisioning from the app, soak-verified on a real
      library); SMAppService packaging landed 2026-07-31 (below);
      buffer-mode retention landed as the spool transplant (below).
- [x] Thumbnail renditions (2026-07-30): the ingress daemon generates
      ~256px/~1024px JPEG renditions per photo (PHImageManager,
      synchronous delivery + ImageIO fallback; video poster frames) as
      RFC-011 resources 5/6 via synthetic sentinel descriptors
      (1005/1006); written thumbnails reopen on edit-set changes; backfill
      migration re-queues pre-rendition archives. Gallery cells degrade to
      placeholders on img decode errors. Soak-verified end-to-end with a
      real library (HEIC + video cells render). Deferred: non-PhotoKit
      import paths must generate their own thumbnails when import lands.
- [x] Consensus photo identity (2026-07-31): `photos.cloud_fingerprint`
      (per-user keyed HMAC of the PHCloudIdentifier, computed node-side
      at `POST /api/photos/client/resolve`, enforced by a partial UNIQUE
      pair) + `photo_ingress_responsibility` (explicit JWT-only
      claim/transfer via `/api/photos/ingress/claim`, device tx route
      gates on holder) + ingress remote adoption (resolve pre-pass stamps
      mesh-held photos as adopted with `consensus_photo_id`, zero
      uploads; non-holders adopt but park mutations without burning
      retries). Wire break: `PhotoAddEntry` amended in place
      (pre-release), dev meshes reseeded. `photos-ingress-identity`
      orchestrator scenario covers the dual-device shape end to end.
      Deferred: import-time fingerprints for non-PhotoKit paths.
      (Library-scoped fingerprint keys have since landed — see the
      ingress shared-library publish entry below; the cross-participant
      PHCloudIdentifier agreement still wants a two-account spike before
      real-world cutover of a multi-member SPL.)
- [x] Photo-ingress packaging + provisioning plumbing (2026-07-31): the
      daemon ships inside HopNet.app (build stages 1b/3b embed
      `Contents/MacOS/photo-ingress` + the SMAppService agent plist,
      signed individually, hardened runtime + photos-library entitlement;
      bundle id now `com.hopnet.desktop.photo-ingress`). Owner-only
      `/api/photo-ingress/{enable,disable,status}` routes provision the
      keychain (device token + blob_root) and drive
      registration/unregistration — the future settings pane's backing
      API, enablement-gated minting (setup/login no longer mint
      unconditionally). Daemon self-provisions at startup: canonical
      `--data-dir` default, `ensure_personal_library` auto-bind from
      keychain blob_root (closes the fresh-state `scope_unmapped` trap),
      `--log-to-data-dir` log ownership, and `RefreshingPublisher`
      credential re-read after unreachable passes (GUI ephemeral-port
      relaunch heals without a daemon restart). Settings pane UI is the
      next slice. Live smoke on the macbook (2026-08-01) passed the full
      enable/disable/re-enable cycle on the production state.db; fixes it
      forced: app unsandboxed (SMAppService EPERM), photos-library
      entitlement + usage string on the MAIN app (TCC attributes the
      bundled daemon to its containing bundle; hardened runtime denies
      without prompt otherwise), and a GUI auto-login startup panic
      (tokio `blocking_write` inside the runtime) that killed the HTTP
      server on every release launch with a stored session key. The route
      orchestration has since been extracted into `photo_ingress::flow`
      behind a `ProvisioningDeps` seam, so its ordering/owner invariants
      (creds-before-register, capture-before-wipe, owner-only 403) are
      pinned by mock tests in Linux CI.
- [~] Shared-library membership lifecycle, mesh side (RFC-011 Phase 3,
      2026-08-01): create_shared_library + invite/accept/decline/
      remove_member consensus txs (JWT-only; DEVICE list unchanged) with
      per-member wrapped library key (name encrypted under it; seam for
      the library-scoped fingerprint key); consent-pattern invites carry
      the invitee's wrap so accept works inviter-offline. Access
      distribution is a per-user CONVERGENCE worker (session-lifetime,
      beside the sidecar sync worker): grant/revoke deltas toward
      members ∪ invitees, OR-IGNORE handlers make racing workers
      harmless, ConvergeLane seam reserves the future key-rotation lane.
      Reads are membership-gated for shared photos (Design B — pre-staged
      invitee wraps inert until accept; kick = instant API revocation);
      sidecar rematerializes on membership diff + photo_view_changes
      signals (join backfill / leave purge) instead of photo_changes
      bumps. JWT routes /api/photos/libraries*, ingest route gains a
      library_id target.
- [x] Ingress shared-library publish (2026-08-02) — **closes the iCloud
      cutover gate**: ingress `libraries` gains `mesh_library_id`
      (`library set-mesh-id`, scope-bound-only; detach refused while
      mesh-bound), claim predicate becomes has-publish-target, and the
      publish pass partitions by scope (per-scope resolve/responsibility/
      parking/attempt-burn — a kicked member's 403ing library can't
      starve the personal queue; unreachable still parks the whole pass).
      Responsibility reshaped to per-(user, library) with a split
      partial-UNIQUE pair; shared claims are membership-checked and
      dissolved on kick; the thin-client tx gate checks the exact scopes
      a tx touches. Resolve accepts a library_id and derives the
      LIBRARY-SCOPED fingerprint key from the shared library key (frozen
      context, pinned vector) so every member computes identical
      fingerprints — idx_photos_fp_shared finally dedupes cross-member;
      `photos-ingress-shared` scenario proves publish → cross-member
      byte-exact reads → second member's daemon ADOPTS instead of
      re-uploading → per-library eviction, e2e. Cutover ops (wipe,
      re-pull, bind, claim) can now cover both partitions.
- [x] Ingress edit + metadata propagation (2026-08-02): editing a photo in
      Apple Photos now reaches the mesh — crops, adjustments, reverts, and
      metadata-only refreshes alike. Same vocabulary as tombstones, keyed
      on a VALUE rather than existence: `photo_resources.published_content_hash`
      is the bytes consensus holds and `photos.published_asset_modified_at`
      the metadata it was composed from, each disagreeing with its live
      counterpart forming half the queue. A revert has no upsert that can
      express an absence, so the envelope gained a removal list and the
      local row is retired to a marker until it propagates — a hard delete
      would take the marker with it and make the divergence invisible.
      Both edit envelopes also gained `metadata_access`: they carried a
      ciphertext with no way to ship the wraps of its key, which assumed a
      writer holding a member private key — true of a browser client, false
      of the daemon, and replacing the ciphertext without new wraps makes a
      photo's metadata undecryptable for everyone, silently. Publish and
      adoption now stamp the markers in the same transaction (and the
      migration backfills them), or "the mesh has it" and "the mesh has
      never been told" would be the same state and every pass would
      re-upload the archive. Spool eviction spares bytes an edit still
      owes the mesh (`edits_unpropagated` per library), since PhotoKit
      will not re-deliver an unchanged asset. The scope pass now runs
      publish → restore → edit → delete, because both edit handlers reject
      a photo the mesh believes is tombstoned. `photos-ingress-edit` proves
      edit, revert and metadata refresh across a 3-node mesh with zero
      divergence; `photos-ingress-shared` covers the cross-member case.
- [x] Ingress tombstone + restore propagation (2026-08-02): deleting a
      photo in Apple Photos now reaches the mesh, and pulling it back out
      of Recently Deleted does too. `photos.tombstone_published_at` (with
      its own retry ledger) records what consensus has been told; that
      disagreeing with `deleted_at` IS the queue, in both directions. The
      marker is deliberately RESETTABLE unlike `published_at` — delete →
      restore → delete is a legitimate cycle, and a set-once marker would
      strand the second delete. Rides the existing scope-partitioned pass
      under the same holder gate (the node's device-tx gate would reject a
      foreign-scope delete anyway) and runs AFTER publishing, so a photo
      added and deleted between passes reaches consensus before being
      tombstoned there; adopted photos propagate under
      `consensus_photo_id`. The mesh side needed nothing — photo_delete /
      photo_restore were already device-submittable, already gated, and
      already authorized for any library member. Hard delete now holds a
      published tombstone past its retention cutoff until the mesh knows
      (surfaced as `tombstones_unpropagated` per library), because reaping
      the row would strand the photo in HopNet with nothing left to repair
      it. `photos-ingress-tombstone` proves both directions across a
      3-node mesh with zero divergence.
- [x] Ingress transplant: archive → transient spool (2026-08-01): the
      daemon's independent archive on user-owned storage (blob roots,
      sidecar JSON files, remote replication, snapshots, Tier-3 recover,
      the `ingress-server` viewer crate) is deleted — HopNet is the
      archive of record. Bytes stage in a single content-addressed spool
      under the data dir and are evicted (`blobs.evicted_at` stamp +
      hash-liveness-gated unlink) once every referencing photo's publish
      is consensus-decided (`published_at`, incl. adoption). Publish
      metadata comes from the `photos.descriptor_json` capsule; the
      document is composed per pass (`Sidecar::compose`). Library
      transitions are ledger-only; per-library storage roots dropped
      from the schema; provisioning shrank to the device token (zero-arg
      `ensure_personal_library`, body-less enable route, blob_root
      purged from FFI/keychain/status). Recovery = re-scan + adoption.
      Cutover is a fresh iCloud re-pull; the old NAS archive stays as an
      untouched cold copy. RFC-011 ingress spec rewritten to match.
- [x] hopnet-drive + hopnet-projection + hopnet-takeout extraction
      ([RFC-015](specs/hopnet-drive.md)) COMPLETE (stages D0–D5,
      2026-07-08): narrowed handler seam, per-projection schema units,
      cross-crate handler/GC registration with boot tripwire, HTTP
      surface behind five host seams, projection-agnostic takeout with
      manifest v2 + per-projection translators (unknown sections skip
      cleanly — photos-ready)
- [x] Projection host API ([RFC-016](specs/projection-host-api.md))
      COMPLETE (stages 1–6, 2026-07-08): HostCapabilities bundle,
      static Projection manifests + one-line registry
      (src/projections.rs) driving schema chain / tripwire / router
      mounts / takeout translators / work dispatch; src/files renamed
      to src/storage_host. Adding photos = implement Projection + one
      manifests() entry
- [x] hopnet-comms + seam completion ([RFC-017](specs/hopnet-comms.md))
      COMPLETE (2026-07-08): inter-node communication extracted to the
      hopnet-comms crate (envelope + scoped dispatch; iroh + the fork
      contained to one manifest; per-subsystem payload enums); fragment
      protocol owned end-to-end by hopnet-storage over comms::Rpc;
      committed_blob_ids + user_data_size_bytes manifest hooks (zero
      drive knowledge left in host consensus/SQL paths); maintenance
      decision logic descended into hopnet-storage; canonical height
      reader in hopnet-projection. Deferred north star recorded:
      projection crates dep only hopnet-projection (blob-put vocabulary
      re-export is a future RFC)
- [x] Storage durability & placement policy — SPECIFIED
      ([RFC-STORAGE-001](../hopnet-storage/spec/durability-policy.md),
      2026-07-13; first module-scoped spec, formally modeled + checked
      in `hopnet-storage/spec/storage_policy.qnt`): copy classes
      (responsible/surplus + projection pins) with decentralized
      watermark GC, per-node decay tiers from absence history,
      capped-HRW placement (spread + minimal movement, quantified),
      re-encode as the core repair loop (keyless, chunk-flat,
      mesh-sharded), watermark urgency floor. Consensus deactivation
      spec owes the membership rely condition
- [x] Storage durability policy — IMPLEMENTED
      ([RFC-STORAGE-002](../hopnet-storage/spec/implementation-plan.md),
      2026-07-15): balanced capped rendezvous placement live (modulo
      deleted; single responsible node per class); metrics-derived
      decay-tier membership view (height-anchored scoring — no wall
      clock in any derivation) with genesis-seeded
      `hopnet_storage_policy` mesh config + `this_node` node settings;
      ciphertext re-encode repair (byte-verified against manifest
      hashes, repairer elected per chunk, urgent/lazy watermark
      urgency); pins + decentralized watermark eviction (disk ground
      truth, guard-carried safety); 5-min policy tick (view sync,
      repair scan, migration pull, eviction, weekly rolling scrub).
      Orchestrator: tier-membership, eviction-under-pressure,
      re-encode-after-departure (kill → decay → regenerate → download)
- [x] Consensus↔storage quorum single-sourced + active-profile watermark
      (2026-07-21): quorum math extracted to `hopnet_common::quorum`
      (one source of truth for both the consensus engine and the storage
      durability watermark — no duplication); the watermark fault budget
      `B(v) = v − quorum(profile, v)` now keys off the mesh's ACTIVE
      quorum profile (was hard-coded BFT, which under-buffered a
      majority/AUTO mesh at small v∈{3,5,6} — a real durability gap). The
      member-count basis is certified by the `storage_policy.qnt` burst /
      σ-tail lemmas; a sans-io `consensus_contract` test locks the parity,
      the seam, and the "consensus churn moves zero bytes" decoupling.

Reed-Solomon encoded file storage with encryption, chunked encoding, and fragment management.

- [x] **NEW**: Chunked Reed-Solomon encoding (40MB chunks, 10 original + 20 recovery per chunk)
- [x] **NEW**: Progressive streaming reconstruction with 25x TTFB improvement for large files
- [x] **NEW**: Per-chunk fast-path and Reed-Solomon reconstruction
- [x] **NEW**: Chunk-aware database schema (chunk_number, local_index compound primary key)
- [x] ChaCha20-Poly1305 fragment encryption with per-file keys
- [x] Blake3 fragment hashing and integrity verification
- [x] Local fragment storage with 2-level directory structure
- [x] Event-driven fragment distribution after upload completion
- [x] Distributed fragment discovery with accelerated fallback pattern
- [x] Work queue pattern for efficient concurrent fragment retrieval
- [x] Database consistency management with automatic state correction
- [x] Cryptographic authentication for all fragment transfer operations
- [x] Consensus-based file deletion with user ownership validation and proper error handling
- [ ] Fragment lifecycle management and garbage collection
- [ ] Automated maintenance reconciliation (route + scheduled job)
- [ ] Storage capacity monitoring and quota management
- [ ] Secure thumbnail generation for encrypted files
- [ ] Preview data extraction while maintaining encryption

### 3. Node Communication System ([RFC-003](specs/node-communication.md))
**Status**: Iroh transport complete — all inter-node communication uses iroh (QUIC/TLS). IP addresses removed from data model.

Pubkey-based peer-to-peer communication over iroh (QUIC/TLS). Nodes are addressed by Ed25519 public key, not IP address.

- [x] HTTP API with RESTful endpoints for admin/client operations
- [x] Dual Ed25519 signature authentication (node + user signatures)
- [x] Node registry with public keys (pubkey-based addressing)
- [x] Request routing (non-leaders forward to current leader)
- [x] Iroh transport layer with peer validation and connection caching
- [x] Consensus messages over iroh (Phase 3 complete)
- [x] Fragment data transfer over iroh (Phase 4 complete)
- [x] Metrics measurement over iroh RPC (Phase 5b complete)
- [x] Node bootstrap over iroh with setup-mode PeerValidator bypass (Phase 5c complete)
- [x] IP address and port removed from schema, structs, and all queries (Phase 5d complete)
- [ ] Network topology awareness and geographic information

### 4. Shard Synchronization System ([RFC-004](specs/shard-synchronization.md))
**Status**: Complete modulo placement with chunked RS support, automated recovery pending

Intelligent fragment distribution system optimizing for performance, reliability, and even distribution.

- [x] Consensus height-based versioning for deterministic placement
- [x] **NEW**: Modulo placement algorithm (local_index % num_validators) with perfect balance
- [x] **NEW**: File-level node selection with metrics-based deterministic shuffle
- [x] **NEW**: Local-index-aware placement ensuring consistent chunk distribution
- [x] Metrics-based scoring for optimal node selection (40% availability, 30% throughput, 20% latency, 10% stability)
- [x] Event-driven distribution with consensus integration
- [x] Self-skip optimization (avoid sending fragments to local node)
- [x] Retry logic with exponential backoff and connection timeouts
- [x] Inter-node authentication for secure fragment transfer
- [x] Fragment discovery protocol for download reconstruction
- [x] Manual network rebalancing with atomic data block processing
- [x] Dynamic timeout calculation based on fragment count (1GB/30min transfer rate)
- [x] Direct node-to-node fragment transfer without intermediary
- [ ] Background orphan recovery with adaptive thresholds
- [ ] Node reliability scoring and roaming device detection
- [ ] Automated background rebalancing and fragment migration
- [ ] User proximity optimization for shared files

### 5. User Interface System ([RFC-005](specs/user-interface.md))
**Status**: Progressed to max for current system set

Cross-platform desktop application providing file management and network administration.

- [x] Tauri-based desktop application (Windows, macOS, Linux)
- [x] Svelte frontend with reactive file browser
- [x] QR code-based network joining workflow
- [x] File upload/download with progress tracking
- [x] Node management and network status monitoring
- [x] User authentication and session management
- [ ] File preview system with secure thumbnail generation
- [ ] Native OS integration (Apple FileProvider, Windows Cloud Files API)
- [~] Linux filesystem integration — hopnet-mount FUSE daemon
      ([RFC-018](specs/hopnet-mount.md), spec 2026-07-30; S1–S9 of 10 shipped
      2026-07-31: read-write mount with strict namespace ops, poke-driven
      freshness, sparse content cache, durable write-back staging,
      provisioning/lifecycle (`hopnet-mount login`, Secret Service + file
      fallback, mesh-capacity statfs, systemd user unit via flake
      home-manager/NixOS modules), and kernel passthrough reads of fully
      cached files (~1.6× the daemon path, zero daemon wakeups; opt-in
      CAP_SYS_ADMIN wrapper in the NixOS module). Cross-node regression
      guard: orchestrator test `mount-cross-node-consistency` mounts via
      FUSE inside a node container and proves kernel IO converges across
      the mesh (2026-08-01). Cache hardening 2026-08-16: descriptors and
      per-blob states are LRU-bounded, so tree-sweeping reads (migration
      verification, grep -r, backups) no longer exhaust RLIMIT_NOFILE
      and wedge reads into silent EIO. Remaining: S10 desktop
      polish/packaging)
- [ ] Client API compatibility — CalVer identity + skew enforcement
      ([RFC-023](specs/client-compat.md), spec 2026-08-16: per-surface
      `min_client` in the RFC-016 manifest, per-request version header
      with mandatory 426 on DeviceToken surfaces, client-side
      `min_node` health probe; all slices S1–S4 shipped 2026-08-16 —
      workspace CalVer inheritance, min_client in the manifest with
      the DeviceToken coverage tripwire, the 426 version gate on
      every DeviceToken surface with versioned health probes, typed
      client-side 426 handling + min_node across mount/appex/ingress,
      and the `client-version-skew` orchestrator test)
- [x] Mount auto-upgrade — the nix client channel
      ([RFC-024](specs/mount-upgrade.md), spec 2026-08-16: follower
      forward-walk selection with the anchor lemma, hopnet-mount
      upgrade wrapper on timer/ExecStartPre/426 triggers,
      profile-symlink activation with exit 75; S1+S2 shipped
      2026-08-16 — `--min-node` + the `upgrade` subcommand (feed logic
      hoisted to hopnet-common), module reshape with profile exec /
      newest-wins seeding / phase-offset timer, daemon exit-75 gate
      and one-shot spawn; S3 shipped 2026-08-16 — the
      `mount-upgrade-vm-test` NixOS check crossing stage → flip →
      exit 75 into the new binary and pinning the held-state
      no-restart-loop, via the test-mode `HOPNET_MIN_CLIENT_OVERRIDE`
      gate seam)
- [~] macOS app as a first-class node
      ([RFC-026](specs/macos-app-node.md), Draft 2026-08-17):
      certified-artifact staging + launchd supervision for the signed
      app bundle — discharges RFC-021's deferred deployment class. S1
      shipped 2026-08-17 (version identity through the bundle: the
      workspace CalVer reaches `tauri.conf.json` via the Tauri
      crate-version fallback, both Info.plists via
      `scripts/macos/version.sh` check/write, and the zip name; the
      release workflow gates tag == version). S2 shipped 2026-08-17:
      `darwinModules.hopnet-desktop` (launchd user agent through the
      RFC-021 profile indirection, newest-wins seed wrapper, exit-75
      restarts via `SuccessfulExit = false`) plus three product fixes —
      tray-only `HOPNET_AUTOSTART` launches, a data-dir instance flock
      (loser exits 0, so a supervised agent never restart-loops against
      a Finder-launched copy), and a startup SMAppService healer that
      re-registers the ingress agent after a bundle move. Discovered en
      route: releases v2026.8.1–v2026.8.4 all carry zero assets (the
      u64-heights Swift breakage failed every CalVer release build), so
      the `hopnet-desktop` pin bump waits on the next release. S3
      implemented 2026-08-17 (e2e crossing pending): the `macos-app`
      provider stages the CI-certified artifact (asset-attached
      availability, sha256 sidecar + codesign + staple + honest-bytes
      verification, provenance last) and activates via the RFC-021
      profile flip; `ActivationEnv` generalizes the tick/boot/seal/view
      seams over both wrapper classes; activation unattended by default
      (tray-only relaunch). S3 e2e crossed 2026-08-17 on the macbook
      (isolated fake feed + one-node scratch mesh): one tick staged the
      certified 2026.8.99 artifact, regenesis_start → seal → unattended
      flip → exit 75 → epoch 2 decided on the new bundle, 3.4 s seal-to-
      cross; crash-loop probe clean. Remaining: S4 pin-bump automation.
- [ ] Advanced file operations (multi-select, context menus, drag-drop)
- [x] Network health dashboard — invariant-derived resilience pane (2026-07-25): the
      Network Resilience pane reports margins to the model-checked invariants rather than
      raw counters. **State Machine Replication** renders INV-NO-HARM as the identity
      `B(v) − (v − live) = headroom`, plus the seated/standby/unreachable split and the
      headroom bands from `membership::band`. **Data Replication** plots the observed
      durability frontier (blocks sorted by worst-case tolerance, cumulative raw bytes)
      against the even-spread ideal, so the gap is the measured cost of non-conformance —
      BRIDGE's precondition. Served by `GET /views/network-resilience`, a view-model route
      that owns no arithmetic: every figure comes from `hopnet_common::quorum`,
      `live_estimate`, `derive_view` or `db::resilience`. Replaces a pane that had drifted
      to `ceil(2v/3)` for quorum, wrong at every `v` divisible by 3.
- [x] Advanced file sharing controls and permissions (Phase 2a+2b backend, Phase 2c frontend)
- [ ] Responsive mobile interface for thin client operations

### 6. Security & Authentication ([RFC-006](specs/security.md))
**Status**: Core Complete, Extensions Planned

End-to-end encryption and comprehensive authentication system.

- [x] Ed25519 cryptographic identity for nodes and users
- [x] X25519 key derivation for file access control
- [x] AES-SIV encrypted file paths and metadata
- [x] JWT-based user session management
- [x] Per-file encryption keys with user access control
- [x] Consensus operation authentication and validation
- [x] TLS-only network surface with per-node self-signed cert and SPKI pairing ([RFC-022](specs/pinned-https.md)); plaintext HTTP is loopback-only
- [x] Android Hop Drive client — live SAF provider over pinned HTTPS with RFC-023 client compat (identity header, typed 426, upgrade banner/notification) and an automated e2e harness (`nix develop .#android` + `scripts/android/e2e.sh`) ([spec](specs/hop-drive-android.md))
- [ ] Advanced permission models (read-only, time-limited access)
- [ ] Key rotation and recovery mechanisms
- [ ] Audit logging and security monitoring
- [ ] Geographic compliance and data sovereignty controls
- [ ] Thin client architecture for mobile/constrained devices

### 7. Maintenance & Operations System ([RFC-007](specs/maintenance-operations.md))
**Status**: Orphaned data cleanup and manual rebalancing complete, automated recovery pending

Automated background processes ensuring network health and storage efficiency.

- [x] **NEW**: Threshold-based orphaned data block cleanup with UUIDv7 age prioritization and consensus deletion
- [x] **NEW**: Two-transaction approach for DuckDB foreign key constraint handling 
- [x] **NEW**: Opportunistic local fragment deletion during consensus execution
- [x] **NEW**: Manual network rebalancing trigger with placement height consensus updates
- [x] **NEW**: Atomic data block rebalancing (only update placement_height after all fragments migrate)
- [x] **NEW**: RPC fragment fetch instructions with dual Ed25519 authentication
- [ ] Availability-aware cleanup prioritization (redundant vs historical)
- [ ] Automated background network rebalancing for node join/leave events
- [ ] Lost shard recovery with Reed-Solomon reconstruction
- [ ] Redundant copy cleanup for download/rebalancing artifacts
- [ ] Fragment health monitoring and remediation
- [ ] Consensus state management and archival
- [ ] Fragment filesystem cleanup for orphaned files
- [ ] Job coordination using node ID proximity to minimize duplicate work

### 8. User Data Takeout & Import System ([RFC-010](specs/user-data-takeout.md))
**Status**: Takeout complete; Import backend (Phase 3) + Frontend MVP (Phase 4) complete; onboarding architecture in place for future steps.

Consensus-coordinated user data portability — symmetric export (takeout) and ingest (import) with onboarding-driven UX surfacing.

**Takeout (export):**
- [x] Consensus-tracked takeout lifecycle management with network-wide coordination
- [x] Rate limiting with one active takeout per user validation
- [x] Event-driven file materialization with real-time progress tracking
- [x] Streaming archive creation with tar.gz compression and encryption
- [x] Fragment retention coordination preventing cleanup during active takeouts
- [x] Automated maintenance jobs with batched consensus operations for expiration handling
- [x] Complete REST API endpoints (initiate, list, download, delete, can-create status)
- [x] Responsive frontend interface with selection-based bulk operations
- [x] Archive completeness guard — archiver fails loudly on missing staged entries, file count asserted against manifest before `Ready` (fixed nested-entry truncation where per-directory staging cleanup destroyed unarchived descendants)

**Import (ingest):**
- [x] Multipart upload with manifest validation + quota enforcement (`POST /takeout/import`)
- [x] Per-file extraction with hash verification + per-row consensus walk (Phase 3.4–3.5)
- [x] Concurrent-write gate middleware (`import_gate` 409s writes during active import)
- [x] Aggregate status counts route (`GET /takeout/import/status`); per-path debug route (`GET /takeout/import/paths`)
- [x] Owner-restart resume — startup scan + auth-event hook re-spawns stranded `Importing` rows (Phase 3.7)
- [x] Frontend `WelcomeModal` onboarding architecture with pluggable step registry; import as first step
- [x] Drag-drop upload, 1 Hz status polling, terminal summary card, write-affordance gating across BrowsePane / IncomingShares / ShareDetailsModal

**Onboarding bitfield:**
- [x] `users.onboarding_flags` u32 bitfield replicated via consensus; `OnboardingFlag` typed enum (`ImportOffered`, `ImportCompleted`); `PUT /users/me/onboarding`
- [ ] **Future**: Profile setup, recovery passphrase verification, multi-device pairing as additional onboarding steps

**Future enhancements:**
- [ ] Failure-state semantics: import completeness guard before `Completed`; `TakeoutStatus` failure variant for errored exports stuck in `Materializing` (issue #48)
- [ ] Incremental exports (changes since last takeout)
- [ ] Selective folder/file export capabilities
- [ ] Retry-failed-files UI; per-file failure report endpoint
- [ ] WebSocket-based real-time progress updates

### 9. Apple FileProvider Integration ([RFC-009](specs/apple-fileprovider.md))
**Status**: Phase 4b Complete ✅ + Comprehensive Testing Framework ✅ - Full Read/Write Support with Unified Change Tracking

Native macOS Finder and iOS Files app integration through Apple's FileProvider framework.

- [x] Swift-based FileProvider extension with native URLSession
- [x] HTTP API communication with device token authentication via Keychain ([RFC-012](specs/device-token-sessions.md))
- [x] Stable file identity using data_block_id for files, hex-encoded encrypted paths for folders
- [x] Read operations (enumerate, fetch metadata, download files)
- [x] Delete operations with recursive folder support
- [x] **NEW**: Unified modification log for comprehensive change tracking (all operations: create, modify, move, delete)
- [x] **NEW**: Efficient single-query incremental sync using LEFT JOIN pattern
- [x] **NEW**: Recursive folder modification dates showing most recent child activity
- [x] **NEW**: Parent folder inclusion in change queries for consistent FileProvider sync
- [x] **NEW**: Ancestor folder modification logging ensuring complete folder hierarchy change tracking
- [x] Process isolation maintaining consensus integrity
- [x] Fragment assembly and streaming for downloads
- [x] **Phase 2**: Write operations (createItem for files and folders)
- [x] **Phase 2**: Multipart upload integration with consensus via existing post_files endpoint
- [x] **Phase 2**: Folder identifier consistency fix using backend-generated encrypted identifiers
- [x] **Phase 2**: .allowsWriting capability for root container and folders
- [x] **Phase 3**: Enhanced metadata properties (creation dates, modification dates)
- [x] **Phase 3**: CustomDateTime deserialization supporting DuckDB native timestamps
- [x] **Phase 3**: Fallback logic using creation dates when modification dates are NULL
- [x] **Phase 3**: ISO 8601 timestamp formatting with Swift Date parsing
- [x] **Phase 4a**: Item modification (modifyItem for metadata-only rename/move operations)
- [x] **Phase 4a**: Folder identifier change handling via deletion_log tracking
- [x] **Phase 4a**: Trash detection with NSFeatureUnsupportedError for unsupported trashing
- [x] **Phase 4a**: Authentication header fix (Bearer token format)
- [x] **Phase 4a**: Consensus height boundary fix for deletion sync  
- [x] **Phase 4b**: Content modification (file content updates via modifyItem)
- [x] **Testing Framework**: Comprehensive FileProvider test suite with Swift executables and Rust orchestration
- [x] **Testing Framework**: Empty file support (handle both `file_size == Some(0)` and `file_size.is_none()`)
- [x] **Testing Framework**: Content verification for all file types with direct API download (bypasses system integration)
- [x] **Testing Framework**: Complete round-trip testing (create → enumerate → download → verify)
- [ ] **Next**: Explicit parent folder logging for complete change tracking (handle deletes/moves)
- [ ] Manual domain registration in HopNet settings
- [ ] Working sets support (recents, favorites, shared)
- [ ] Foundation for iOS thin client architecture
- [ ] Thumbnail generation and Quick Look integration

### 10. S3-Compatible API ([RFC-008](specs/s3-compatibility.md))
**Status**: Specification complete, implementation not started

S3-compatible API layer enabling standard S3 clients and SDKs to interact with HopNet.

- [ ] Virtual bucket layer mapping S3 buckets to encrypted paths
- [ ] Dual-mode credentials (secure proxy mode and portable standalone mode)
- [ ] AWS Signature v4 authentication
- [ ] Core S3 operations (ListBuckets, CreateBucket, GetObject, PutObject, etc.)
- [ ] Local proxy mode for secure key management
- [ ] Standalone mode for environments without local HopNet client
- [ ] Bucket-level access control and sharing
- [ ] Multipart upload support
- [ ] Pre-signed URLs for temporary access
- [ ] Integration with AWS CLI and standard S3 SDKs

### 11. Apple Photos Ingress ([spec](specs/apple-photos-ingress.md))
**Status**: Phases 0–6 complete; Phase 7 (hardening) in progress; Phase 8 retired; Phase 9 (HopNet transplant) complete — HopNet is the archive of record

PhotoKit→HopNet on-ramp daemon (personal + iCloud Shared Photo Library). Bytes stage in a transient content-addressed spool under the daemon's data dir, publish into the mesh as encrypted data blocks through consensus (device-token thin-client routes via `crates/ingress-publisher`), and are evicted once decided — the daemon-minted photo_id becomes the consensus id with no remapping, and mesh-held photos re-associate via adoption. Standalone Rust workspace at `crates/` (sqlx 0.9 pilot) + Swift PhotoKit shim at `apple/PhotoIngress/`. The original user-owned-storage archive layer (blob roots, sidecars, replication, snapshots, viewer) was a stopgap and has been deleted (Phase 9).

- [x] Phase 0: PhotoKit spike (identity, scope detection, streaming)
- [x] Phase 1: ingress-core skeleton (schema, match precedence)
- [x] Phase 2: UniFFI bridge + vertical slice (one asset end-to-end)
- [x] Phase 3: Pipeline (scheduler, retry/backoff, admission, SIGTERM)
- [x] Phase 4: Discovery (change classification, reconciliation scan, library moves, daemon loop)
- [x] Phase 5: Lifecycle (hard-delete cleanup, Tier-1 repair)
- [x] Phase 6: CLI (`ingress-cli`: status, fsck --repair, library config)
- [~] Phase 7: Hardening (packaging + provisioning + live smoke done; full personal-library soak remaining)
- [x] Phase 8: Interim viewer — retired; crate deleted in Phase 9, gallery lives in HopNet's frontend
- [x] Phase 9: HopNet transplant (descriptor capsule, spool eviction on decided publish, roots/sidecars/snapshots/recover demolished, token-only provisioning)

### Reliability Goals
- **Single node failure**: No data loss, minimal performance impact
- **Regional outage**: Full data availability through geographic redundancy
- **Network partition**: Graceful degradation with majority consensus
- **Roaming devices**: No impact on network performance or availability

### Scalability Targets
- **Current scope**: Validator-sized networks (<100 nodes)
- **Near-term**: Support up to 1000 nodes with varying participation in consensus (e.g. some nodes storage only)
- **Long-term**: Enterprise deployments with geographic distribution

## Technology Stack

### Backend
- **Language**: Rust (performance, safety, concurrency)
- **Database**: DuckDB (embedded analytics, complex queries)
- **Consensus**: Malachite (Quint-verified Tendermint), tier-1 effect API, sans-io host
- **Cryptography**: Ed25519, X25519, ChaCha20-Poly1305, Blake3
- **Networking**: HTTP with custom authentication

### Frontend
- **Framework**: Tauri (cross-platform desktop)
- **UI Library**: Svelte (reactive, lightweight)
- **Styling**: Modern CSS with dark theme
- **Build System**: Vite (fast development, optimized builds)

### Development
- **Version Control**: Git with conventional commits
- **Documentation**: Markdown
- **Testing**: Rust unit/integration tests, manual UI testing
- **Deployment**: Native binary distribution
- **Node storage locations**: every path a node writes to resolves through
  `src/paths.rs` — database and regenesis artifacts, TLS identity, and the
  fragment store, each with its own override. `HOPNET_EPHEMERAL_DB=1` routes
  all three into a disposable per-checkout, per-process tree under
  `HOPNET_EPHEMERAL_ROOT` (default `$TMPDIR`), reaped by the next ephemeral
  node via an flock'd owner file. Nothing derives its location from the
  database path.
- **Splitting storage across disks**: `HOPNET_DATA_DIR` (database, WAL,
  regenesis artifacts, photos sidecars) and `HOPNET_FRAGMENTS_DIR` (bulk
  content-addressed blobs) resolve independently, so a validator can keep the
  latency-critical database on fast storage while blobs stay on a bulk pool.
  Every synced database write gates the consensus round the node proposes: an
  fsync costs ~146 ms on spinning ZFS against ~0.55 ms on NVMe, which clients
  see as multi-second file operations whenever that node is proposer. The nix
  module exposes this as `services.hopnet.fragmentsDir`, defaulting under
  `dataDir` so existing deployments are unchanged. A boot guard refuses to
  start when the database claims locally-stored fragments but the store is
  missing or empty — the failure it prevents is a node starting before its
  bulk filesystem mounts and writing blobs to the wrong disk.

## Development Roadmap

### Phase 1A: Infrastructure Completion (Critical Path)
**Goal**: Resolve blocking dependencies preventing distributed operations

1. **Complete RFC-003 fragment transfer protocols** ✅ - Critical blocker resolved
   - Fragment transfer HTTP endpoints implemented (GET/POST /fragments/{hash}, health checks)
   - Authentication integrated with dual signature system
   - Fragment size validation and automatic hash verification
   
2. **Implement background metrics collection infrastructure** - New critical blocker identified  
   - Extend metrics table with consensus height and availability boolean columns
   - Automated background metrics collection with randomized scheduling (every 10 minutes)
   - Consensus transaction batching to minimize network overhead
   - Manual trigger API endpoint for debugging and testing
   
3. **Enable RFC-002 distributed fragment storage** - Foundation for distributed network
   - Implement distributed fragment placement using metrics-based node reliability scoring
   - Add cross-node fragment discovery and retrieval (depends on RFC-003 ✅)
   - Complete storage capacity monitoring and basic quota management

### Phase 1B: Native OS Integration
**Goal**: Enable seamless native OS file access

1. **RFC-009 Apple FileProvider Integration** - Native macOS/iOS file access (PHASES 1-3 COMPLETE ✅)
   - ✅ Implemented Swift FileProvider extension with full read/delete operations
   - ✅ Added scoped HTTP API endpoints with Keychain authentication
   - ✅ Created stable file identity system using data_block_id
   - ✅ Added fragment assembly streaming for downloads
   - ✅ **Phase 2 (Complete)**: Implemented createItem for file/folder creation with multipart upload
   - ✅ **Phase 3 (Complete)**: Added enhanced metadata properties (creation dates, modification dates) with DuckDB timestamp support
   - ✅ **Phase 4a (Complete)**: Implemented modifyItem for metadata-only rename/move operations
   - **Phase 4b (Design Complete, Ready for Implementation)**: Content modification with new data blocks approach
   
### Phase 1C: Basic Distributed Operations ✅ **COMPLETED**
**Goal**: Enable core distributed filesystem functionality

1. **RFC-004 fragment placement and discovery** - Smart fragment distribution ✅
   - ✅ Implemented modulo placement for deterministic, balanced distribution
   - ✅ Added node reliability scoring with metrics-based selection
   - ✅ Chunked Reed-Solomon implementation with progressive streaming
   
2. **RFC-007 maintenance and operations** - Network health and efficiency
   - Implement threshold-based fragment cleanup with UUIDv7 age tracking
   - Add availability-aware cleanup prioritization
   - Build network rebalancing system for topology changes
   - Create redundant copy cleanup for storage optimization
   
3. **Complete UI features for distributed operations** - User-facing distributed functionality  
   - Advanced file operations (multi-select, drag-drop) with distributed backend
   - Network health dashboard showing distributed node status
   - File sharing controls leveraging distributed storage
   
4. **Node performance monitoring** - Foundation for reliability scoring
   - Implement comprehensive node metrics collection
   - Add automatic node health scoring for placement decisions
   - [x] Build monitoring dashboard for network health (2026-07-25) — see the UI section.
     Two gaps remain deliberately unaddressed: **RECOVERY** (a validator dark past `t_out`
     and still seated means self-healing is stuck) has no time-axis surface, and
     **liveness** is unrepresented, so a mesh that stopped deciding still renders green.
     The latter needs the on-demand engine's own wake rules — `round.height > decided`
     together with `PendingPool::staged_len()` — since a quiescent mesh legitimately does
     not advance height and a rate metric alone cannot tell it from a wedged one.
     Partially closed 2026-08-17: `/health` now reports NotReady when the consensus
     shell is not running (the shell-wedge zombie shape) and carries the node's
     `consensus_height`, so cross-host height comparison no longer needs shell access;
     the mesh-stall predicate above (wedged-vs-quiescent) remains open.

### Phase 2: Performance & Reliability
- Advanced node reliability scoring with predictive capabilities
- Automated rebalancing and fragment migration
- Performance optimization for streaming use cases
- NAT traversal implementation for simplified network setup

### Phase 3: Enterprise Features
- Geographic compliance framework with user-provided regions
- Advanced security features (audit logging, key rotation)
- Large network scaling optimizations
- Mobile thin client application

### Phase 4: Advanced Capabilities
- Machine learning-based placement optimization
- Integration with cloud storage providers
- Advanced geographic redundancy with regulatory compliance
- Developer APIs and third-party integrations

### Phase 5: Enterprise Integration APIs
**Goal**: Enable enterprise adoption through standard APIs

**FileProvider Next Steps (Phase 4b-5):**
- Phase 4b (Implementation Ready): Content modification with stable inode IDs and new data blocks 
- Phase 5: Enable working sets, thumbnails, and Quick Look integration

**S3 Compatibility Layer:**
- Implement core S3 operations with AWS Signature v4 authentication
- Build virtual bucket layer with encrypted path mappings
- Create dual-mode credential system (proxy and standalone)
- Add multipart upload and pre-signed URL support
- Integrate with existing file sharing and permission architecture
- Validate compatibility with AWS CLI and major S3 SDKs
