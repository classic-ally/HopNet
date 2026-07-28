use hopnet_common::CustomUUID;

use crate::error::PhotosCoreError;

/// The dispatch boundary between photos-core's client logic and the
/// consensus/transport substrate (photos.md:628-654).
///
/// Two concrete impls exist downstream: a node-local dispatch that calls
/// straight into the consensus submit pipeline + local DB, and a thin-
/// client dispatch that POSTs over HTTP. photos-core itself is impl-agnostic.
///
/// Commit 2 ships the sync/hydration surface (fetch_photos_since,
/// submit_transaction). The upload pipe (upload_data_block, fetch_data_block,
/// fetch_library_members) is deferred to a later commit — the sidecar
/// hydration path (Commit 3) only needs these two methods.
#[async_trait::async_trait]
pub trait PhotoDispatch: Send + Sync {
    /// Submit a fully-formed, bincode-encoded photo transaction envelope
    /// to consensus. `tx_type` is the projection's transaction-type tag
    /// (e.g. "photo_add", "photo_delete"). The payload bytes are opaque
    /// to the dispatch — photos-core has already done all crypto + framing.
    async fn submit_transaction(
        &self,
        tx_type: &str,
        payload_bytes: Vec<u8>,
    ) -> Result<(), PhotosCoreError>;

    /// Incremental sync feed: every photo whose `photo_changes` row was
    /// upserted at a consensus height strictly greater than `height`.
    ///
    /// Returns a `SyncBatch` whose `high_water_mark` is the node's CURRENT
    /// decided height — the caller persists THAT as its cursor, NOT
    /// `max(changes.changed_at_height)`. The latter regresses on hard-delete:
    /// a cleanup batch can drop every row above the cursor, leaving the
    /// client replaying stale tombstones forever. The node-level cursor is
    /// monotonic.
    async fn fetch_photos_since(
        &self,
        height: u64,
    ) -> Result<SyncBatch, PhotosCoreError>;
}

/// One incremental sync response. Carries the changed photos since the
/// client's last cursor PLUS the node's current decided height, which the
/// client adopts as its new cursor.
#[derive(Debug, Clone)]
pub struct SyncBatch {
    /// Photos with `photo_changes.changed_at_height > cursor`. Ordered
    /// ascending by `changed_at_height` for deterministic replay.
    pub changes: Vec<PhotoChange>,
    /// The node's current decided consensus height. The client writes
    /// THIS value as its cursor, not `max(changes.changed_at_height)`.
    pub high_water_mark: u64,
}

/// One photo's worth of sync state. `state == None` means the photo row
/// was hard-deleted (tombstone expired + cleanup ran) — the `photo_changes`
/// row survives the cascade (NO FK), so offline clients still learn
/// the deletion.
#[derive(Debug, Clone)]
pub struct PhotoChange {
    pub photo_id: CustomUUID,
    /// The consensus height at which the `photo_changes` row was last
    /// upserted — the sync-feed ordering key.
    pub changed_at_height: u64,
    /// `Some` = photo row still present, decrypt + upsert into sidecar.
    /// `None` = hard-deleted, delete from sidecar.
    pub state: Option<EncryptedPhotoState>,
}

/// The encrypted, per-photo snapshot the sidecar needs to (1) decrypt
/// metadata via the calling user's `photo_metadata_access` wrap and
/// (2) upsert a `photo_index` row + resource pointers. Fields map 1:1
/// to the consensus DB's `photos` / `photo_metadata_access` /
/// `photo_resources` tables. `photo_id` is NOT duplicated here — it
/// lives on the parent `PhotoChange`.
#[derive(Debug, Clone)]
pub struct EncryptedPhotoState {
    // --- `photos` columns ---
    /// NULL = personal library.
    pub library_id: Option<CustomUUID>,
    pub uploaded_by: i32,
    pub encrypted_metadata: Vec<u8>,
    pub metadata_nonce: [u8; 12],
    /// ISO 8601, NULL when the photo is active (not soft-deleted).
    pub deleted_at: Option<String>,
    /// FK users.user_id, NULL when active. INTEGER in the DB.
    pub deleted_by: Option<i32>,

    // --- the calling user's `photo_metadata_access` row, if any ---
    /// `None` = no access row for this user (photo shared after they
    /// went offline, or access revoked). The sidecar records the photo
    /// as undecryptable and skips metadata upsert.
    pub ephemeral_pubkey: Option<[u8; 32]>,
    pub encrypted_metadata_key: Option<Vec<u8>>,

    // --- `photo_resources` rows for this photo ---
    /// (resource_type, data_block_id) pairs. Empty vec is valid
    /// (transient mid-cleanup state where resources cascade-deleted
    /// before the photo row).
    pub resources: Vec<(i32, CustomUUID)>,
}
