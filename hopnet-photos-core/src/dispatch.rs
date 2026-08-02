use hopnet_common::CustomUUID;

use crate::error::PhotosCoreError;

/// The dispatch boundary between photos-core's client logic and the
/// consensus/transport substrate (photos.md:628-654).
///
/// Two concrete impls exist downstream: a node-local dispatch that calls
/// straight into the consensus submit pipeline + local DB, and a thin-
/// client dispatch that POSTs over HTTP. photos-core itself is impl-agnostic.
///
/// The sync/hydration surface (fetch_photos_since, submit_transaction) and
/// the upload pipe (upload_data_block, fetch_library_members) are shipped.
/// The download pipe (fetch_data_block) is deferred to the thin-client
/// dispatch commit — node-side clients read content through the host's
/// `GET /api/photos/{id}/resource/{type}` route instead of the trait.
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
    async fn fetch_photos_since(&self, height: u64) -> Result<SyncBatch, PhotosCoreError>;

    /// Encrypt and store one resource's bytes as a data block on the storage
    /// substrate. `source` yields exactly `file_size` plaintext bytes (the
    /// publisher enforces this); the dispatch encrypts with `per_blob_key`,
    /// fragments, and persists. Fragment distribution is the substrate's
    /// concern, not the caller's.
    async fn upload_data_block(
        &self,
        blob_id: hopnet_storage::BlobId,
        source: Box<dyn tokio::io::AsyncRead + Unpin + Send>,
        file_size: usize,
        per_blob_key: chacha20poly1305::Key,
    ) -> Result<UploadedDataBlock, PhotosCoreError>;

    /// Resolve the recipients of a publish. The dispatch derives the acting
    /// user (`LibraryMembership::uploaded_by`) from its own authenticated
    /// state — callers never supply an identity. `library_id == None` is the
    /// personal library: membership is exactly `[uploaded_by]`.
    async fn fetch_library_members(
        &self,
        library_id: Option<CustomUUID>,
    ) -> Result<LibraryMembership, PhotosCoreError>;
}

/// One stored fragment of an uploaded data block. Mirrors the storage
/// engine's put outcome 1:1 so the trait stays free of the substrate's
/// `engine` feature. Serde: these DTOs are the wire shapes of the
/// thin-client dispatch routes (`/api/photos/client/*`).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct UploadedFragment {
    pub chunk_number: u32,
    pub local_index: u32,
    pub fragment_id: CustomUUID,
    pub fragment_hash: hopnet_common::Blake3Hash,
    pub recovery: bool,
}

/// Outcome of `PhotoDispatch::upload_data_block` — everything the publisher
/// needs to assemble a `BlobInsertOp` for the photo_add payload.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct UploadedDataBlock {
    pub integrity_hash: hopnet_common::Blake3Hash,
    pub fragments: Vec<UploadedFragment>,
    pub added_bytes: u8,
}

/// A recipient of a publish: consensus user id plus the X25519 public key
/// blob and metadata keys are wrapped to.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LibraryMember {
    pub user_id: i32,
    pub pubkey: hopnet_storage::x25519_dalek::PublicKey,
}

/// Recipients of a publish plus the dispatch-derived acting user.
/// `uploaded_by` comes from the dispatch's authenticated state (node-local:
/// the session behind the Submitter; thin client: the session key holder),
/// never from the publish caller.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LibraryMembership {
    pub uploaded_by: i32,
    pub members: Vec<LibraryMember>,
}

/// One incremental sync response. Carries the changed photos since the
/// client's last cursor PLUS the node's current decided height, which the
/// client adopts as its new cursor.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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

#[cfg(test)]
mod tests {
    use super::*;

    // Impact: these DTOs are the wire contract of the thin-client dispatch
    // routes — a silently changed field shape strands every daemon-side
    // deserializer at once.
    // Should: round-trip an uploaded data block through JSON with fragment
    // identity, hashes, and flags intact.
    #[test]
    fn uploaded_data_block_round_trips_through_json() {
        let block = UploadedDataBlock {
            integrity_hash: hopnet_common::Blake3Hash::from_bytes([0xAA; 32]),
            fragments: vec![UploadedFragment {
                chunk_number: 3,
                local_index: 11,
                fragment_id: CustomUUID::new(None),
                fragment_hash: hopnet_common::Blake3Hash::from_bytes([0xBB; 32]),
                recovery: true,
            }],
            added_bytes: 2,
        };

        let json = serde_json::to_string(&block).unwrap();
        let back: UploadedDataBlock = serde_json::from_str(&json).unwrap();

        assert_eq!(back.integrity_hash, block.integrity_hash);
        assert_eq!(back.added_bytes, block.added_bytes);
        assert_eq!(back.fragments.len(), 1);
        assert_eq!(back.fragments[0].chunk_number, 3);
        assert_eq!(back.fragments[0].local_index, 11);
        assert_eq!(
            back.fragments[0].fragment_id,
            block.fragments[0].fragment_id
        );
        assert_eq!(
            back.fragments[0].fragment_hash,
            block.fragments[0].fragment_hash
        );
        assert!(back.fragments[0].recovery);
    }

    // Should: round-trip library membership with the recipient X25519 key
    // bytes and the dispatch-derived uploader intact.
    #[test]
    fn library_membership_round_trips_through_json() {
        let pubkey = hopnet_storage::x25519_dalek::PublicKey::from([0x42u8; 32]);
        let membership = LibraryMembership {
            uploaded_by: 7,
            members: vec![LibraryMember { user_id: 7, pubkey }],
        };

        let json = serde_json::to_string(&membership).unwrap();
        let back: LibraryMembership = serde_json::from_str(&json).unwrap();

        assert_eq!(back.uploaded_by, 7);
        assert_eq!(back.members.len(), 1);
        assert_eq!(back.members[0].user_id, 7);
        assert_eq!(back.members[0].pubkey.as_bytes(), pubkey.as_bytes());
    }
}
