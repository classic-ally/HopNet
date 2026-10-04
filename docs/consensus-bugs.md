# Consensus Bugs

## 1. ✅ Concurrent Block Creation Race
**Status**: FIXED (consensus mutex)
**Issue**: Multiple threads could create blocks simultaneously
**Location**: consensus_middleware
**Fix**: Added Mutex<()> guard before block creation

## 2. Missing Justify Fields
**Issue**: Blocks don't carry QC/TC for catch-up and partition recovery
**Impact**: Nodes can't recover from missed broadcasts
**Location**: BlockData struct, blocks table
**Fix**: Add `justify_qc` and `justify_tc` fields

## 3. ✅ Lock QC/TC Race Condition (Fork Vulnerability)
**Status**: FIXED (two-layer defense-in-depth)
**Issue**: Lock QC and TC can both form for same view with different arrival orders
**Impact**: Some nodes commit block, others don't → permanent state divergence
**Root cause**: Immediate commit on Lock QC arrival (1-chain commit rule)

**Attack scenario**:
```
t=60s:  Nodes A,B,C,D timeout
t=60.1s: Lock QC forms at leader
t=60.1s: TC forms from timeout votes
t=60.2s: Node A receives Lock QC → commits
t=60.2s: Nodes B,C,D receive TC → advance view without commit
→ DIVERGENCE: A committed, B,C,D didn't
```

**Fix**: Two-layer defense-in-depth mitigation

**Layer 1: Leader Abandonment** (proactive) - IMPLEMENTED
- Added `CertificateError::NetworkTimeout` variant
- `QuorumCertificate::create()` now checks timeout vote count via `TimeoutVoteCollector.get_vote_count()`
- If timeout_count >= quorum_threshold, refuses to create QC (both Propose and Lock phases)
- Uses `calculate_quorum_threshold()` for dynamic BFT/relaxed mode support
- Prevents honest-but-slow leader from completing QC after network timeout
- **Defends against**: Slow/crashed honest leader (95%+ of real failures)
- **Location**: `src/consensus/types.rs:478-502` (QuorumCertificate::create)

**Layer 2: Post-TC Bounded Wait** (reactive) - IMPLEMENTED
- **Follower side**: Added `skip_wait: bool` parameter to `apply_timeout_certificate()`
- Waits GST (500ms) before checking consensus state and applying TC
- If Lock QC applied during wait, view advances and TC becomes stale (rejected)
- Local timeout sites use `tokio::join!` to broadcast and apply in parallel for synchronization
- Catch-up uses `insert_tc_safe()` directly (no wait needed for historical replay)
- **Leader side**: Leader commits QC and broadcasts in parallel using `tokio::join!`
- Parallel broadcast minimizes window for state divergence by starting broadcast immediately
- Maximizes time for Lock QC to arrive at followers before their GST wait expires
- **Defends against**: Network reordering, asymmetric delays
- **Limitation**: Depends on GST timing assumption
- **Locations**:
  - Follower: `src/consensus/routes.rs:1428-1466` (apply_timeout_certificate)
  - Follower call sites: `src/consensus/jobs.rs:76-86`, `src/consensus/routes.rs:404-417`, `src/consensus/routes.rs:434`
  - Leader: `src/consensus/functions.rs:180-194` (QC1), `src/consensus/functions.rs:199-223` (QC2)

**Remaining attack vector**: Byzantine leader with network-level control can still cause divergence via selective delivery of Lock QC to subset of nodes. This requires:
- Malicious intent (not just crash/slow)
- Network control (selective message dropping)
- Precise timing coordination
- Detectable after-the-fact (victims have proof via Lock QC)
- **Risk assessment**: Low (requires sophisticated adversary)

**Long-term fix**: Migrate to HotStuff-2 (2-chain commit rule) eliminates all timing-based attacks but requires ~1000 LoC refactor and 2-view commit latency.

## 4. ✅ Early prepared_block_hash Setting
**Status**: FIXED (removed set_prepared parameter)
**Issue**: `prepared_block_hash` set when voting, should only set when Propose QC arrives
**Impact**: Incorrect HotStuff state machine progression
**Location**: insert_block() in db/consensus.rs, post_ballot route, Block::new_tip(), integrate_view()
**Fix**: Removed `set_prepared` parameter entirely from `insert_block()` and `insert_block_with_conn()`
- Block insertion now only stores block data, never modifies consensus state
- `prepared_block_hash` is ONLY set by `insert_qc_unsafe_tx()` when Propose QC arrives (correct HotStuff semantics)
- Updated all 3 call sites: leader block creation, validator voting, catch-up integration

## 5. ✅ No Double-Voting Protection
**Status**: FIXED (double-vote checks + vote tracking)
**Issue**: Node could vote twice in Propose phase for different blocks in same view
**Impact**: Byzantine behavior, could enable equivocation attacks
**Location**: Ballot::propose() for leaders, ballot.verify_proposal() + ballot.sign() for followers
**Fix**: Comprehensive double-vote protection for both roles:

**Schema changes:**
- Added `last_propose_vote_block_hash` field to `this_node` table
- Added `ProgressionErrorKind::DoubleVote` error variant
- Added `db::update_last_propose_vote()` function

**Leader protection (Ballot::propose()):**
- Checks `last_propose_vote_block_hash` before proposing
- Rejects different block in same view (double-vote attempt)
- Allows retry for same block (idempotent)
- Records vote after creating ballot
- Automatically creates vote signature internally

**Follower protection (ballot.verify_proposal() + ballot.sign()):**
- Checks `last_propose_vote_block_hash` before voting
- Rejects different block in same view (double-vote attempt)
- Allows retry for same block (idempotent)
- Records vote after signing

**View advancement cleanup:**
- `last_propose_vote_block_hash` is cleared when Lock QC advances to next view (db/consensus.rs:695)
- `last_propose_vote_block_hash` is cleared when TC advances to next view (db/consensus.rs:585)
- Allows leader to propose new blocks in the new view

**Note**: Lock phase doesn't need explicit tracking - `prepared_block_hash` already prevents voting on different blocks within a view

## 6. ✅ TC Doesn't Clear prepared_block_hash
**Status**: FIXED (insert_tc_safe)
**Issue**: When TC arrives, prepared_block_hash should be set to NULL (work abandoned)
**Impact**: Stale prepared state after timeout
**Location**: insert_tc_unsafe_tx() in db/consensus.rs:593-596
**Fix**: Added `prepared_block_hash = NULL` to TC processing UPDATE statement

## 7. ✅ TC Doesn't Process highest_qc
**Status**: FIXED (insert_tc_safe)
**Issue**: TC contains highest_qc field that should be extracted and inserted
**Impact**: Lose QC information during timeouts
**Location**: insert_tc_safe() in db/consensus.rs:603-672
**Fix**: Full validation pipeline:
1. Check if QC already exists → safe to proceed
2. If QC missing but block exists → verify QC cryptographically, then insert
3. If block missing → reject TC (requires catch-up first)
- Ensures consensus safety by validating justification before advancing view
- Opportunistically recovers missing QC when block available

## 8. ✅ Lock QC Without Propose QC
**Status**: FIXED (qc.verify)
**Issue**: qc.verify() doesn't check that local Propose QC exists before accepting Lock QC
**Impact**: Could accept Lock QC for block we never saw prepared
**Location**: qc.verify() in src/consensus/types.rs
**Fix**: Added Propose QC prerequisite check to qc.verify() - Lock QC validation now requires corresponding Propose QC to exist first, ensuring we never accept Lock QC for blocks we didn't see prepared

## 9. TOCTOU in Routes
**Status**: MITIGATED (consensus mutex)
**Issue**: /qc and /tc routes check state then modify separately
**Impact**: Race condition between check and modification
**Mitigation**: Consensus mutex prevents concurrent modifications
## 10. ✅ Retried Forward Duplicated in the Proposer's Pool
**Status**: FIXED 2026-09-27 (nonce-unique PendingPool + committed re-check on rejection)
**Issue**: A forwarder retries the moment a height decides without its transaction (`AckedDecided` → `RetryNow`), while the proposer still holds the original in its pending pool for a later block. The proposer's forward handler dedups against `committed_tx_nonces` only, so the retry became a second pool entry; preflight applies pool entries in order under savepoints, the first copy applied and the second was rejected `Permanent("ProcessingError")` (e.g. "pubkey already registered"). On a 3-validator mesh one rejection is final, so the API answered 500 while the transaction committed.
**Impact**: Any queue-submitted write could report failure after committing; seen as the fourth node's join in `regenesis-cutover`, likelier under the RFC-STORAGE-003 catch-up drain's transaction volume.
**Location**: `PendingPool::push` and `process_forward_results` in src/consensus/queue.rs
**Fix**: `PendingPool::push` joins a same-nonce entry's waiters (staged or in flight) instead of pushing a duplicate — `QueuedTransaction` carries `Notifiers`, a fan-out of every submitter waiting on it. On the forwarder, a `Rejected` verdict for a nonce already committed locally resolves through the settler as committed. Tests: `duplicate_nonce_joins_the_pooled_entry`, `notifiers_fan_out_and_ignore_dropped_receivers` (queue.rs), `rejection_of_a_locally_committed_nonce_resolves_as_committed` (consensus/tests/forward_dedup.rs).

## 11. ✅ Failed Decide Effect Swallowed by the Engine Macro
**Status**: FIXED 2026-10-01 (effect errors parked and returned by the host; transient decides retried)
**Issue**: Malachite's `process!` macro logs an `Err` from the effect handler ("Error when processing effect") and resumes the engine with `Resume::Continue`. A `HostError::Storage` from `Effect::Decide` — SQLITE_BUSY after the 5 s busy_timeout, a commit-time BUSY, or a handler's lock error lifted through `ApplyError` — therefore left the engine at Commit step believing the height decided, while the host never advanced `last_decided`, never queued `StartHeight(h+1)` and dropped every later sync value for the height as already decided. The shell's "storage errors are fatal" promise only covered errors returned from the input path.
**Impact**: A silent, permanent wedge of one validator. Production, 2026-09-27: the desktop node sat at height 64858 for four days (10,948 ProposedValue WAL entries for 64859) until a restart's WAL replay re-decided it; the same effect error fired 36× on the laptop and 58× on the desktop in the hour after the 2026.10.1 crossing under the disk-truth sweep's write load.
**Location**: `drive_once` / `handle_effect` in hopnet-consensus/src/host.rs; `StoreError` in hopnet-consensus/src/store.rs; `apply_block` in src/consensus/malachite/app.rs
**Fix**: The handler parks the first error in `HostCore::effect_error` and returns a marker to the macro; `drive_once` returns the parked error after `process!`, so `feed` propagates it and the shell aborts (supervision restarts, replay re-decides). Inside `Effect::Decide` a transient error (`Storage::error_is_transient`) re-runs the whole decide up to `DECIDE_RETRIES` (3) times with a 100/200/400 ms blocking backoff; `ApplyError` carries `transient`, mapped from a handler's `DatabaseError::Transient` and surfaced as `StoreError::ApplyTransient`, so contention inside a handler is retried rather than treated as a determinism failure. Tests: hopnet-consensus/tests/decide_errors.rs.
**Also exposed**: making effect errors visible surfaced a second silent loss on the same path — after a crash-replay the host restarted `wal_seq` at 0, so the first live WAL append at the replayed height violated the `(height, seq)` key and, swallowed, every post-restart vote at that height was published without its durable entry (the no-equivocation-across-crash guarantee held only for votes cast before the first crash). `start_height` now resumes `wal_seq` after the replayed entries and `wal_fetch` deletes a torn final row so the count is exact.

## 12. ✅ Fatal WAL-Append Busy Under the Proposer's Preflight Lock
**Status**: FIXED 2026-10-01 (every durability effect retries transient contention under one bounded budget; no effect runs past a failed one)
**Issue**: Entry 11 made a failed effect fatal, but only the decide got a retry. `Effect::WalAppend` persists the engine's own proposal and votes as a single autocommit INSERT on the consensus connection and relied entirely on the connection's 5 s `busy_timeout`. The proposer's `build_value` preflight dry-runs every candidate transaction inside an IMMEDIATE transaction on a second pool connection and holds the write lock for the whole dry-run; on a cold cache a 500-blob declare page took the hold past 15 s. The vote's WAL append waited 5 s, got `database is locked`, the host parked it, `drive_once` returned it, and the shell aborted the process. The same gap existed for `wal_reset`/`wal_fetch` at height start.
**Impact**: A deterministic crash loop. Production, 2026-10-01 17:37–18:03 UTC: thor was the proposer for height 82752; every boot ran the cold preflight, failed the vote append, aborted and dumped core — 24 restarts, the mesh pinned at 82752 with the desktop as the only live validator until the height moved and thor stopped being its proposer. systemd's core cap kept the disk safe; nothing else did. Also observed: after the first failed append the engine macro kept executing the input's later effects, so votes were signed and published with no WAL entry behind them before the abort — the equivocation window the WAL exists to close.
**Location**: `retry_transient`, `Effect::WalAppend`, `Effect::Decide`, `start_height`, `start_or_defer` and the `with:` arm of `drive_once` in hopnet-consensus/src/host.rs; the preflight in `build_value`, src/consensus/malachite/app.rs
**Fix**: One helper, `retry_transient`, wraps every durability effect — WAL append, WAL fetch/reset at height start, decide — and retries while `Storage::error_is_transient` holds: 7 attempts, each waiting out the 5 s busy_timeout, with 100·2^(n-1) ms between them capped at 3200 ms, ≈ 41 s of shell blocking worst case before the error is fatal (blocking votes that long is strictly better than aborting: consensus is stalled on the same lock either way). The WAL sequence number advances only after a successful insert, so a retried append reuses it. Once an effect has parked an error, `drive_once` refuses every later effect of the same input unexecuted, so nothing is signed or published past a failed append. The proposer side logs how long the preflight held the lock and stops admitting candidates past a two-second budget, restaging the rest for the next block (proposer-local block shaping; validation is untouched). Regression tests in hopnet-consensus/tests/wal_errors.rs: a lock held past one busy wait is survived; retries are bounded; no effect runs after a parked error; the height-start WAL reset retries.

## 13. ✅ Decided-Fetch Reply Exceeded the Wire Frame — a Lagging Node Could Never Sync
**Status**: FIXED 2026-10-01 (the fetch server bounds each reply by bytes; the client already accepts a short contiguous prefix)
**Issue**: The sync client asks a peer for 50 decided blocks per request and the server encodes every (block, certificate) pair in range, up to 100. The transport's 8 MB frame cap is enforced on receive only, so an oversized reply leaves the server and is refused at the client as `protocol error: message too large`. The client classifies that as a per-peer transport strike, strikes every peer in turn, and reports the sync exhausted — at debug level. After the v2026.10.2 crossing the chain carried its fattest blocks ever (8192-hash attestation pages at 270 KB, 500-blob declare pages), and fifty of them encoded to 10.9 MB.
**Impact**: A node more than one chunk behind across that stretch can never catch up. Production, 2026-10-01: the macbook (node 3, unseated) joined epoch 7 at the seal height 81948 and sat there for hours while its probes showed every peer 2,400 heights ahead; nothing at INFO explained it. Its sweep's self-check then carried a stale inventory count the chain had already moved past, and was rejected ("removal requires exact count: expected 238445, found 143040") — a symptom that first pointed at the wrong bug.
**Location**: `serve_decided_fetch` / `take_within_budget` in src/net/scopes.rs; the client in src/consensus/malachite/sync.rs; the frame cap in hopnet-comms/src/iroh_impl.rs
**Fix**: `DECIDED_FETCH_BYTE_BUDGET` (6 MB on the raw pair sum, headroom for bincode's envelope under the 8 MB cap): the server keeps the longest prefix of encoded pairs that fits, never fewer than one — a zero-pair reply would read as "the peer has nothing" and end a tip-mode sync as satisfied. The client needed no change: it feeds whatever contiguous prefix arrives and continues from the last height fed, with strikes reset on progress. Tests: three pure budget tests in scopes.rs and a seeded fat-block fetch in src/consensus/tests/sync_fetch.rs asserting a short, contiguous, under-frame reply.

## 14. ✅ WAL Sequence Reset Over Rows Appended During StartHeight Replay
**Status**: FIXED 2026-10-01 (the host sets the WAL sequence counter before it feeds StartHeight, not after)
**Issue**: `start_height` fetched the height's persisted WAL rows, fed `StartHeight` to the engine, and only then set the live sequence counter to the fetched-row count. But the engine replays the inputs it buffered for the new height *inside* StartHeight, and every replayed vote is a live WAL append. On a live resume the fetch returns nothing, so the counter snapped back to 0 over the rows the replay had just written; the next append reused `(height, 0)` and the `PRIMARY KEY (height, seq)` rejected it. A UNIQUE violation is not transient, so the fail-fast rule from entry 11 aborted the process. The trigger needs on-demand mode (every production node) plus buffered next-height votes at decide time — a lagging validator.
**Impact**: Production, 2026-10-01 19:44–20:06 UTC on v2026.10.3: thor, surviving BUSY waits of several seconds per append thanks to entry 12 while the chain ran ~24 heights a minute, decided each height already holding the next height's votes; 17 aborts in 22 minutes, the mesh deciding only in the gaps. Latent since the WAL host existed; invisible while the chain was quiet and BUSY aborts hid the lag.
**Location**: `start_height` in hopnet-consensus/src/host.rs
**Fix**: The counter is set to `entries.len()` right after the fetch, before StartHeight is fed, so the replay's appends continue from it and the post-feed assignment is gone. Test: `resume_counts_the_appends_made_while_replaying_buffered_votes` in hopnet-consensus/tests/wal_errors.rs — an on-demand core decides height 1, receives a vote for height 2 while still at 1 (buffered, no row), resumes (the replay persists seq 0), and the next live append must take seq 1 and decide height 2.

## 15. ✅ Proposals and Forward Batches Capped by Count, Not Bytes
**Status**: FIXED 2026-10-01 (byte budget on proposal takes and forward batches; oversized transactions refused at admission and never proposed)
**Issue**: Entry 13's root shape, on the live path. A proposal travels as ONE gossip frame (PartsOnly mode: `WireProposedValue` carries the whole block) and a forward batch as ONE RPC frame, and the transport refuses frames over 8 MiB on receive only. `PendingPool::take_for_proposal` and the forwarder's drain capped by count (100) alone, and no transaction size was checked anywhere — admission, forwarding or the `build_value` pre-filter. A block of a hundred few-hundred-KB entries (photo_add for multi-GB videos, attestation pages) is tens of MB: every validator drops the proposal, the round times out, and the proposer re-proposes the same entries; a refused forward returns NoAck and the same batch goes back into the holdback, which only ever grows.
**Impact**: Latent until the photo ingress daemon published one photo at a time; unserializing it (16 concurrent publishes) makes multi-video blocks routine. Observed precursor: block 82440 at 5.6 MB.
**Location**: `take_for_proposal`, `batch_processor`, `pre_validate` in src/consensus/queue.rs; the engine's take in src/consensus/malachite/engine.rs; the `build_value` pre-filter in src/consensus/malachite/app.rs
**Fix**: `BATCH_BYTE_BUDGET` (5 MiB, room under 8 MiB for what `build_value` appends and the envelope) by `tx_wire_bytes` (payload + function + a 512 B allowance): the proposal take and the forward drain keep the longest prefix within it, never fewer than one; held-back forwards beyond it wait for the next cycle ahead of newer work. `MAX_TX_PAYLOAD_BYTES` (4 MiB) is refused at `pre_validate`, and the `build_value` pre-filter rejects it permanently for anything that skipped admission (forwarded, older nodes). Compile-time asserts keep the two limits consistent with each other and the frame. Tests: `budgeted_prefix_respects_count_and_bytes`, `proposal_take_stops_at_the_byte_budget` in queue.rs.

## 16. ✅ Storage Infrastructure Errors Classified as Semantic Verdicts
**Status**: FIXED 2026-10-03 on `worktree-mesh-stall-fixes` (unreleased): every classifier shares one predicate for storage-availability codes
**Issue**: Only `SQLITE_BUSY` / `SQLITE_LOCKED` counted as transient. `DiskFull`, `CannotOpen`, `IoErr`, `ReadOnly` and `Protocol` fell through `db_err` / `DatabaseError::classified` / `StoreError::is_transient` as `ProcessingError`, which `validate_inner` turns into `ValidateFailure::Semantic` → `Validity::Invalid`, the sync path into `SyncInvalid` ("failed local validation despite a valid commit certificate"), and the preflight into a `Permanent` drop. The node's own reads inside `validate_inner` (parent lookup, key tables, nonce dedup, the vote-iff-match snapshot) went through the string-is-semantic `From` impls and had no classification at all.
**Impact**: Production, 2026-10-02 09:04 UTC: the macbook's disk filled (7 GB free of 927). SQLite returned `DiskFull`, then `CannotOpen` ("unable to open database file"). The node nil-voted on every live proposal at height 119836, rejected the certificate-backed sync value for it, dropped its own attestations as Permanent, and logged nothing consensus-related for the next 13 hours while its HTTP server kept answering (507 to the ingress agent once a second). Seated as a validator, it also cost the mesh a round timeout on every third height. With its inventory rows stale, no blob with a macbook class could ever satisfy the confirmation evidence rule: 0 of 68,689 blobs confirmed mesh-wide.
**Location**: `sqlite_code_is_infrastructure` in common/src/db_impl.rs; `DatabaseError::classified` in hopnet-projection/src/lib.rs; `db_err` in hopnet-storage/src/store.rs; `StoreError::is_transient` in hopnet-consensus/src/store.rs; `db_failure` in src/consensus/malachite/app.rs; `storage_err` in src/storage_host/handlers.rs
**Fix**: One predicate names the infrastructure codes (busy, locked, full, cantopen, ioerr, readonly, protocol) and the three classifiers use it, so validation goes Undetermined (IMMEDIATE retry, then the existing loud hold), preflight restages, and decide retries under its bounded budget before going fatal. `validate_inner`'s own reads classify through `db_failure`. `UpdatePlacementHeights` stops mapping contention to `ProcessingError`. Two silent-continue paths join the fatal posture: a failed `start_or_defer` aborts like any fatal step, and a failed engine spawn at boot exits with the restart code instead of serving HTTP with no chain. Tests: `disk_and_open_failures…` in hopnet-consensus/src/store.rs, `preflight_disk_full_is_bucketed_for_restage_then_proposes` / `validate_block_returns_undetermined_for_disk_full` in src/consensus/tests/transient_restage.rs, the predicate's own tests in common.
**Not fixed here**: the recovery once the disk frees is a hold-and-retry, not a restart; a node that stays unopenable holds at its height logging ERROR per attempt. The 09:05 silence itself (no sync attempts logged after the last rejection) is not fully explained.

## 17. ✅ Self-Check Apply Rejected Ordinary Concurrency as a Verdict
**Status**: FIXED 2026-10-03 on `worktree-mesh-stall-fixes` (unreleased): the apply is idempotent; the prompt report is blob-scoped
**Issue**: `apply_self_check` inserted belief rows with a plain `INSERT` into a table keyed `(fragment_hash, node_id)` and refused any report with removals unless the node's row count equalled the report's `previous_count` exactly. Two reports built from one snapshot — the pull's prompt report and the sweep's whole-node differential — collided on UNIQUE; any other apply moving the count failed the guard; both surfaced as `StorageError::Rs` ("reed-solomon coding error") → `ProcessingError` → a Permanent drop (and a nil vote at validation). The prompt report itself was the whole-node differential (two `EXCEPT` scans over ~768k rows) run on every pull that moved bytes, ~5×/min on the laptop, so it both manufactured the collisions and bounded pull throughput.
**Impact**: Production, 2026-10-02: 201 self-check transactions dropped in a day on the laptop (145 of them thor's), so the belief row an attestation stamps often never existed and confirmation evidence could not complete. Entry 13 had already met the exact-count rejection as a misleading symptom.
**Location**: `apply_self_check`, `compute_blob_inventory_differential` in hopnet-storage/src/store.rs; `blob_self_check_report` in hopnet-storage/src/traits.rs and src/storage_host/substrate_host.rs; the evidence block of `pull_owed` in hopnet-storage/src/engine/mod.rs
**Fix**: Additions are an upsert keeping the newest `self_verified_height`; removals are a per-row compare-and-swap on `verified_height` — replicated, stamped only by attestations — so a flag-derived removal never discards disk-verified evidence newer than its view. The CAS must not key on `self_verified_height`: it is excluded from the canonical snapshot, a joiner imports it NULL while members carry it, and a predicate over it would make row presence diverge between nodes. The count guard is gone (`previous_count` stays on the wire, compared only under DEBUG logging). The pull's prompt belief is one indexed, blob-scoped query (held classes of this blob with no row for this node — origin classes at birth included, which ingest never inventories); the whole-node differential remains the sweep's. Tests: `self_check_apply_is_idempotent_and_merges_concurrent_reports`, `self_check_removal_keeps_rows_verified_after_the_report`, `blob_inventory_differential_is_scoped_and_filtered` in store.rs; `self_check_handler_applies_the_same_report_twice` in src/storage_host/tests.rs; the pull tests in engine/mod.rs decode the belief payload.

## 18. ✅ Attestation Height Trusted from the Payload and Overwritten Downward
**Status**: FIXED 2026-10-03 on `fix-mesh-stall` (PR #93, unreleased): stamps are capped at the deciding height and only rise
**Issue**: `apply_attestation` set `verified_height` to the height carried in the `attest_fragments` payload, unchecked and unconditionally. The payload height is the submitter's word: a node could stamp a height past the chain tip and keep its rows inside the confirmation recency window indefinitely. And because the stamp overwrote, a sweep page committing late with an older height (pages commit one at a time over minutes, all carrying the height read once at the end of the walk) lowered a newer stamp from a prompt pull attestation, aging the row out of the window early.
**Impact**: No observed exploit; the downward overwrite contributed to rows falling out of the 1024-height window during the 2026-10-02 stall. The window is a consensus rule (`evidence_complete` runs inside `apply_confirm`), so both behaviours fed directly into which confirmations applied.
**Location**: `apply_attestation` in hopnet-storage/src/store.rs; `AttestFragmentsHandler` in src/storage_host/handlers.rs
**Fix**: The stamp is `MIN(payload height, ctx.height)` and is written as `MAX(COALESCE(verified_height, 0), stamp)`, so it never passes the height it is applied at and never moves down. A consensus-rule change, shipped at the 2026.10.5 epoch crossing. Tests: `attestation_never_lowers_a_stamp`, `attestation_height_is_clamped_to_the_deciding_height` in store.rs.

## 19. ✅ Index-Only Section Bump Without a Frozen Predecessor Broke Epoch Joins
**Status**: FIXED 2026-10-03 on `fix-storage-v3-import` (unreleased; 2026.10.5 carries the bug)
**Issue**: 2026.10.5 bumped the storage snapshot section from format 3 to 4 for storage step 0004, which adds an index only. The covered tables were unchanged, so no frozen v3 spec was kept and `resolve_import_plan` mapped storage@3 headers to the live spec. But `format_version` is written into the artifact and hashed: rebuilding a storage@3 artifact with the v4 spec re-serializes a storage@4 header, and the roundtrip gate refuses with "schema fingerprint mismatch (artifact roundtrip at artifact shape)".
**Impact**: Production, 2026-10-03: the epoch-10 seal was written by 2026.10.4 binaries (storage@3). Laptop, desktop and thor crossed by the sealed-file path, which never imports the artifact. The macbook, a straggler at height 123182, could only cross by the staged epoch join; on 2026.10.5 it failed the gate on every boot and restart-looped every ~20 s, writing a ~500 MB scratch database each time. Every straggler or new joiner into epoch 10 is affected.
**Location**: `SNAPSHOT_SECTION` / `PRE_SCAN_INDEX_SNAPSHOT_SECTION` in hopnet-storage/src/store.rs; `resolve_import_plan` in src/db/snapshot.rs
**Fix**: A frozen `PRE_SCAN_INDEX_SNAPSHOT_SECTION` (format 3, the same tables via a shared `STORAGE_TABLES`) and a `storage@3` arm in `resolve_import_plan` that imports at ordinal 3 and fast-forwards through step 0004. Every section format bump needs its frozen predecessor, even when only the version changes. Epoch 10 itself requires exactly 2026.10.5, so a joiner can only use the fix after the next crossing. Test: `storage_v3_artifact_builds_against_its_own_hash` in src/db/chains.rs (fails against the live spec).
**Guards against recurrence** (branch `feat-rolling-sweep`, landed with the next section bump, storage@5): `resolve_import_plan` refuses an ordinal with no frozen spec by name instead of building it with the live one; the tripwire `every_sealed_ordinal_imports_through_a_spec_of_its_own_version` requires every ordinal a release sealed (`OLDEST_SEALED` to head) to resolve to a spec of that version; a straggler written by the previous release (`src/regenesis/fixtures/previous-release-join/`, refreshed per release) must cross into the build through the staged-join boot path (`previous_release_straggler_crosses_into_this_build`); and the `release-crossing` orchestrator test is a required pre-tag gate (CLAUDE.md "Releases").

## 20. ✅ A Node Deleted Its Own Unconfirmed Upload (Join Reconcile, Then the Sweep)
**Status**: FIXED 2026-10-04 on `fix-join-keeps-unbacked-fragments` (targets 2026.10.11)
**Issue**: After a straggler imports the joined epoch's inventory, `reconcile_fragment_store` deleted every fragment file with no `fragment_hashes` row, with zero grace, on the claim that nothing is in flight during a join. But an upload writes its fragments to the receiving node's disk (`hopnet_storage::api::put`) before its transaction commits, and a straggler can join before that commit reaches it. The node's own upload then looks like an orphan. The rolling sweep had the same hole with a one-hour grace: a rowless file older than `SWEEP_ORPHAN_GRACE_SECS` was deleted from boot onwards, so stopping the reconcile alone only moved the loss to the next sweep rotation.
**Impact**: Production, 2026-10-01: photo ingress published two iCloud videos (2.54 GB, six blobs with thumbnails) to the macbook's node at ~17:17–17:40 UTC. The submits timed out client-side and the photos committed at height 82990. At 19:22 the macbook, a straggler, staged-joined the 10.3 epoch and the reconcile deleted 9,577 rowless files, including every fragment of both videos. Placement had never run, so the macbook was the only holder; the blobs are unrecoverable mesh-wide and show as the resilience pane's "lost" bucket. The originals survive in iCloud and the ingress spool.
**Location**: `reconcile_fragment_store` in src/regenesis/join.rs (called from the staged-join boot path in src/regenesis/boot.rs and the in-process join); `sweep_shard` step 3 in src/storage_host/jobs.rs over `hopnet_storage::sweep::diff`
**Fix**: Two parts. (1) The reconcile only re-marks backed fragments and counts unbacked files (logged as `unbacked`); it deletes nothing. (2) The rolling sweep, the one place a rowless file is deleted, consults a durable node-local ledger of this node's own uploads: `hopnet_storage_local_uploads` (storage step 0006, storage@6; storage@5 artifacts import through the frozen `PRE_UPLOAD_LEDGER_SNAPSHOT_SECTION`). Every `hopnet_storage::api::put` site — the photos `Submitter` and the drive's `process_uploaded_file` — records the returned fragment hashes under the blob id (`hopnet_projection::host::record_own_upload`) before the transaction is signed, and a put whose ledger write fails fails the upload. `reap_orphans` (src/storage_host/jobs.rs) deletes a rowless file past `SWEEP_ORPHAN_GRACE_SECS` only if the ledger does not name it; a ledgered file is held at any age (`orphans_held`, `orphan_bytes_held`) until the hold expires. A time window alone was rejected: a node restarting every release never lets one elapse, and a straggler's imported inventory may never carry the rows, so only the exact signal — this node uploaded it — is safe. The ledger is node-local and rides the staged-join copy like the sweep cursor. The hook runs inside `put` before each batch of fragment files is written (`api::put_with`, via `hopnet_projection::host::put_own_upload`), so an upload streaming for longer than the grace never has a file on disk without its ledger row; a put that fails part-way abandons its blob (holds released, rowless files unlinked) instead of holding its first chunks for the retention. A hold is never retired by its row landing: the sweep's orphan list is a snapshot taken before awaited work, so retiring on the row and deleting from the snapshot could take a file whose row landed in between, and a later staged join can import an inventory without the row. Holds last until the retention — 14 days (`LOCAL_UPLOAD_RETENTION_SECS`, `HOPNET_STORAGE_LOCAL_UPLOAD_RETENTION_SECS`), then the files are ordinary orphans (`uploads_expired`); a rowed fragment is ordinary for placement, eviction and surplus release meanwhile, since the ledger only shields orphan deletion. Every orphan unlink goes through `store::delete_unclaimed_fragments`, which re-checks the hash has no `fragment_hashes` row and no hold inside the write transaction, immediately before the unlink, in batches of 32 with the `stat`s done outside the lock — the sweep, the owner's purge and an abandoned put alike; the sweep's orphan step runs on the blocking pool and a busy database skips the step (`orphan_batches_skipped`) rather than failing the shard. The ledger hook retries transient failures (pool checkout, SQLITE_BUSY/LOCKED) with backoff for up to a minute before a put fails, and `put_own_upload`'s drop guard abandons an upload whose future a client disconnect dropped, the hook refusing the detached chunk task's next batch. The abandon's release and unlinks retry transient failures under the same budget; a batch whose ledger write was still retrying when the upload was abandoned is refused after its fresh holds are dropped (the abandon may have released the blob before that write committed), and the refusing call drops the last accepted batch's own holds before unlinking it, since those holds would otherwise make every re-check keep the file (`a_batch_abandoned_during_its_ledger_write_leaves_nothing_held`, `an_abandoned_upload_releases_its_last_batch_before_unlinking_it`, `an_abandon_retries_through_a_busy_database` in hopnet-projection/src/host.rs; `a_purge_skips_a_recently_finished_upload` in src/storage_host/jobs.rs). Stuck uploads are visible under `held` on `GET /maintenance/orphaned-fragments` by blob id (rowless computed at read time, grouped in SQL, capped at 1000 blobs with totals, counts not bytes), and the node's owner can release them early through `POST /maintenance/orphaned-fragments/purge-held` (on the blocking pool), which skips and reports (`skipped_recent`) a blob whose put is in flight in this process (an exact registry — a slow client can outlast any fixed age per chunk, so a stamp age alone could not tell) or whose newest hold is under a day old (`PURGE_MIN_AGE_SECS`; the registry alone was not enough, since it covers only the put while the rows land with a later transaction — the photos publisher uploads every resource of a photo before its one `photo_add`, a stalled mesh delays commits, and a transaction proposed before a restart can still commit after it — so purging a just-finished upload would unlink its only copy; a future-stamped hold stays purgeable, or a clock step would leave it neither expirable nor purgeable), and never deletes a file another upload still holds (the ledger is keyed per blob). Tests: `an_own_upload_older_than_the_grace_survives_the_sweep_until_its_rows_land` and `a_rowless_file_that_is_not_an_own_upload_is_still_deleted_after_the_grace` (src/storage_host/jobs.rs, fail without the ledger filter), `split_orphans_spares_own_uploads` (hopnet-storage/src/sweep.rs), `local_upload_ledger_records_retires_and_purges` (hopnet-storage/src/store.rs), `process_uploaded_file_records_every_fragment_in_the_upload_ledger` (src/storage_host/tests.rs), `storage_v5_artifact_builds_against_its_own_hash` (src/db/chains.rs), `reconcile_remarks_local_fragments_and_leaves_unbacked_files` (src/regenesis/join.rs). Open: why the macbook's 19:22 import lacked rows for blocks committed before the 85218 seal (`import_snapshot` may skip an unmappable section and still report success); and ingress adoption marks such a photo published without checking that its blocks are recoverable (`crates/ingress-core/src/publish.rs`). The two videos need re-uploading; the ledger's blob ids are the handle.

## 21. ✅ RPC Dedup Cache Held Every Served Response for Five Minutes
**Status**: FIXED 2026-10-04 on `fix-rpc-dedup-retention` (unreleased)
**Issue**: The receiver-side RPC dedup cache in hopnet-comms stored, under each rpc request id, a `OnceCell` holding the whole encoded response; a per-request timer removed it only after `DEDUP_TTL` (300 s). Nothing bounded it by bytes or count. The storage scope's fetch responses are whole fragments, so the cache held five minutes of every fragment a node served. The cache only exists so a sender retrying the same request id after a transport failure (one retry, after its rpc timeout and a redial) gets the first response instead of a second execution.
**Impact**: Production, thor: serving ~19 MB/s of fragments pinned ~5.7 GB of heap (malloc_stats ~5.3 GB in use; a jemalloc profile put 99% under `StorageScope` handle → `encode_to_vec`), and the node was OOM-killed repeatedly. Not a consensus fault, but it took a mesh node down.
**Location**: the `ScopeKind::Rpc` arm of `handle_stream` and `DEDUP_TTL` in hopnet-comms/src/iroh_impl.rs; `RpcHandler` in hopnet-comms/src/lib.rs; `StorageScope` in src/net/scopes.rs; `RegenesisScope` in src/regenesis/rpc.rs
**Fix**: A new `RpcHandler::dedup` method (default true) lets a handler opt out: no entry and no timer, so a retry runs the handler again.
- **Storage and regenesis opt out.** They serve the large, high-volume replies, and both are safe to run twice. Storage health and fetch are read-only. Storage store is content-addressed: the hash is checked, `AlreadyExisted` comes back when a valid copy is on disk, the write is a temp file plus atomic rename, and `mark_local` only sets a flag. Regenesis EpochInfo, LineageFetch and SnapshotChunk only read. SnapshotInfo may materialize the seal artifact, but only bytes whose blake3 matches the lineage record, through a temp file and rename.
- **The other scopes keep every entry, delivered or not.** These are consensus, setup, metrics and status. A successful `finish` only means the reply reached the local send buffer, so a reply lost on a dying connection looks delivered, and the sender's same-id retry must still find it.
- **The window runs from the reply.** `DEDUP_TTL` is 60 s from when the reply is written, or its write fails, or the stream task is dropped. Before, it ran from first sight. The longest rpc timeout among these scopes is 30 s (setup JoinDeliver), and the redial adds up to 10 s (`CONNECTION_TIMEOUT`), so the retry lands within ~40 s. That leaves 20 s of margin.
  - A 30 s TTL from first sight would have re-run a JoinDeliver whose JoinAck was lost. The coordinator would then get "already initialized" and give up on a node that had joined.
- **Same-id safety.** The TTL removal checks `Arc::ptr_eq` and holds only a `Weak`, so an older request's timer never evicts a newer entry for the same id.
- **Byte bound.** A reply over 1 MiB (`DEDUP_MAX_ENTRY_BYTES`) is still sent but never cached. Otherwise a catch-up's decided fetches (up to ~6 MiB each) would fill the budget and crowd out the small acks the cache is for. So every handler whose replies can exceed 1 MiB must be idempotent. Among the scopes that keep dedup, the only such reply is consensus DecidedFetch, a read of decided history; setup, metrics and status replies are small. Behind that, cached replies share a 64 MiB budget (`DEDUP_BUDGET_BYTES`), and a reply that would cross it is also sent but not cached.

Tests in iroh_impl.rs:
- `opted_out_handler_never_caches_a_large_response`
- `same_id_retry_after_reply_is_served_from_cache`
- `dedup_window_runs_from_the_reply` (fails with a 30 s or a first-sight TTL)
- `ttl_cleanup_spares_newer_entry_for_same_id` (keeps the older cell alive, so it fails without the `Arc::ptr_eq` check)
- `dedup_cache_never_exceeds_the_byte_budget` and `over_budget_reply_is_delivered_but_not_cached` (both fail without the budget)
- `large_replies_never_crowd_out_small_ones` (fails with the old 8 MiB entry cap)
- `reply_over_frame_cap_is_not_cached`
- `dedup_same_request_id_invokes_handler_once`

Test in src/net/scopes.rs: `only_storage_and_regenesis_skip_rpc_dedup`.

## 22. ✅ A Staged Join Re-Derived Every Fragment Flag by Reading the Whole Store
**Status**: FIXED 2026-10-04 on `fix-fast-join-reconcile` (unreleased; 2026.10.12 carries the bug)
**Issue**: A straggler's staged join copies its old database, whose `fragment_hashes.stored_locally` flags the sweep keeps correct, then `transplant_from_scratch` replaces `fragment_hashes` from the artifact. `stored_locally` is an excluded node-local column, so every row came back 0. `reconcile_fragment_store` then walked every unflagged row (all ~2M replicated rows, not just this node's), read and blake3-hashed each fragment that existed, and wrote one autocommit UPDATE per hit, with no progress logging (issue #105). It is blocking on purpose: starting consensus with the flags missing makes the self-check differential report a mass removal. A second hazard sat behind it: the reconcile ran after the swap and before staging was cleared, so a node stopped mid-reconcile restarted, saw its leftover staging for an epoch it had already reached, cleared it, and started consensus on partial flags.
**Impact**: Production, thor, 2026-10-04: thor missed the 2026.10.12 seal and joined epoch 17 by the staged join. On its HDD (~700k local fragments, ~225 GB) the reconcile read at ~1.9 MB/s, an estimated ~31 hours before the node could start consensus. Restarting it to escape would have started it on partial flags.
**Location**: `build_next_from_seal_with_splice` and `staged_join_transition` in src/regenesis/boot.rs; `transplant_from_scratch` in src/db/chains.rs; `reconcile_fragment_store` in src/regenesis/join.rs
**Fix**:
- **Flags carried.** `transplant_preserving_local_flags` (src/db/chains.rs) snapshots the old inventory's hashes and flags into a temp table in the build transaction, transplants, and re-flags by hash. No file is touched.
- **Existence-only, set-based reconcile.** With flags carried, only hashes the old inventory never knew (fragments from the missed epochs, e.g. this node's own upload whose commit it never saw, consensus-bugs 20) get one `stat` each; the rest are trusted, and the sweep's flagged-but-missing pass unflags any whose file has since vanished. With nothing carried (fresh node, in-process join) it walks the store by directory listing (`fragstore::list_shard_hashes`: no per-file stat, no content read), one transaction per shard. Content verification is deferred to the sweep's scrub and serve-time verification. All writes are batched through `commit_timed`; INFO lines at start, end, and every 16 shards or 60 s with an ETA. It still deletes nothing.
- **Interrupted reconciles.** `database.db.reconcile-pending` is written and fsynced before the joined database goes live (staged swap and in-process import) and removed only when the reconcile completes; staging is now cleared right after the swap. At boot, a surviving marker means the flags are partial: a staged join does not carry them, and before the engine starts the boot walks the store. Staging left behind for an epoch the database already reached (an older binary stopped mid-reconcile — thor's state) writes the marker too.
Tests: `staged_join_keeps_the_local_flags_of_fragments_it_already_held`, `interrupted_reconcile_marker_forces_the_full_walk`, `rerun_of_an_already_swapped_join_walks_the_store`, `pending_marker_stops_a_staged_join_trusting_its_flags` (src/regenesis/boot.rs); `reconcile_marks_present_files_without_reading_them`, `reconcile_with_carried_flags_checks_only_unknown_hashes`, `reconcile_commits_in_batches_not_per_row`, `reconcile_progress_is_due_every_sixteen_units_or_minute`, and the ignored `reconcile_timing_harness` (src/regenesis/join.rs); `shard_hash_listing_names_only_fragments` (hopnet-storage/src/fragstore.rs).
