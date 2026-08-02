use crate::error::PhotosCoreError;
use hopnet_common::CustomUUID;
use hopnet_photos::envelopes::{
    MetadataAccessEntry, PhotoAddEntry, PhotoAddPayload, PhotoDeleteEntry, PhotoDeletePayload,
    PhotoEditContentEntry, PhotoEditContentPayload, PhotoEditMetadataEntry,
    PhotoEditMetadataPayload, PhotoFavoriteEntry, PhotoFavoritePayload, PhotoResourceOp,
    PhotoRestoreEntry, PhotoRestorePayload,
};
use serde::Serialize;

pub struct PhotoAddDraft {
    pub photo_id: CustomUUID,
    pub library_id: Option<CustomUUID>,
    pub uploaded_by: i32,
    pub encrypted_metadata: Vec<u8>,
    pub metadata_nonce: [u8; 12],
    pub resources: Vec<PhotoResourceOp>,
    pub metadata_access: Vec<MetadataAccessEntry>,
    pub operation_id: CustomUUID,
    /// Keyed HMAC of the asset's stable cross-device id (computed
    /// node-side via the resolve route). None = local-only asset.
    pub cloud_fingerprint: Option<[u8; 32]>,
}

pub fn build_photo_add(drafts: Vec<PhotoAddDraft>) -> PhotoAddPayload {
    PhotoAddPayload {
        entries: drafts
            .into_iter()
            .map(|d| PhotoAddEntry {
                photo_id: d.photo_id,
                library_id: d.library_id,
                uploaded_by: d.uploaded_by,
                encrypted_metadata: d.encrypted_metadata,
                metadata_nonce: d.metadata_nonce,
                resources: d.resources,
                metadata_access: d.metadata_access,
                operation_id: d.operation_id,
                cloud_fingerprint: d.cloud_fingerprint,
            })
            .collect(),
    }
}

/// A content edit: new blobs for changed resources, kinds a revert dropped,
/// and — when the metadata changed with them — a fresh ciphertext with the
/// wraps of the key it is under. Metadata rides the content transaction
/// rather than a second one so a crop's new dimensions land atomically with
/// the pixels they describe.
pub struct PhotoEditContentDraft {
    pub photo_id: CustomUUID,
    pub resources: Vec<PhotoResourceOp>,
    pub encrypted_metadata: Option<Vec<u8>>,
    pub metadata_nonce: Option<[u8; 12]>,
    pub metadata_access: Vec<MetadataAccessEntry>,
    /// Wire values of the `ResourceKind`s to drop.
    pub remove_resources: Vec<i32>,
    pub operation_id: CustomUUID,
}

pub fn build_photo_edit_content(drafts: Vec<PhotoEditContentDraft>) -> PhotoEditContentPayload {
    PhotoEditContentPayload {
        entries: drafts
            .into_iter()
            .map(|d| PhotoEditContentEntry {
                photo_id: d.photo_id,
                resources: d.resources,
                encrypted_metadata: d.encrypted_metadata,
                metadata_nonce: d.metadata_nonce,
                operation_id: d.operation_id,
                metadata_access: d.metadata_access,
                remove_resources: d.remove_resources,
            })
            .collect(),
    }
}

/// A metadata-only edit — no bytes moved, so nothing to upload.
pub struct PhotoEditMetadataDraft {
    pub photo_id: CustomUUID,
    pub encrypted_metadata: Vec<u8>,
    pub metadata_nonce: [u8; 12],
    pub metadata_access: Vec<MetadataAccessEntry>,
    pub operation_id: CustomUUID,
}

pub fn build_photo_edit_metadata(drafts: Vec<PhotoEditMetadataDraft>) -> PhotoEditMetadataPayload {
    PhotoEditMetadataPayload {
        entries: drafts
            .into_iter()
            .map(|d| PhotoEditMetadataEntry {
                photo_id: d.photo_id,
                encrypted_metadata: d.encrypted_metadata,
                metadata_nonce: d.metadata_nonce,
                operation_id: d.operation_id,
                metadata_access: d.metadata_access,
            })
            .collect(),
    }
}

pub fn build_photo_delete(photo_ids: Vec<CustomUUID>) -> PhotoDeletePayload {
    PhotoDeletePayload {
        entries: photo_ids
            .into_iter()
            .map(|photo_id| PhotoDeleteEntry {
                photo_id,
                operation_id: CustomUUID::new(None),
            })
            .collect(),
    }
}

pub fn build_photo_restore(photo_ids: Vec<CustomUUID>) -> PhotoRestorePayload {
    PhotoRestorePayload {
        entries: photo_ids
            .into_iter()
            .map(|photo_id| PhotoRestoreEntry {
                photo_id,
                operation_id: CustomUUID::new(None),
            })
            .collect(),
    }
}

pub fn build_photo_favorite(photo_ids: Vec<CustomUUID>) -> PhotoFavoritePayload {
    PhotoFavoritePayload {
        entries: photo_ids
            .into_iter()
            .map(|photo_id| PhotoFavoriteEntry {
                photo_id,
                operation_id: CustomUUID::new(None),
            })
            .collect(),
    }
}

pub fn encode_payload<T: Serialize>(t: &T) -> Result<Vec<u8>, PhotosCoreError> {
    bincode::serde::encode_to_vec(t, bincode::config::standard()).map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use hopnet_common::Blake3Hash;
    use hopnet_photos::envelopes::MetadataAccessEntry;

    fn make_draft() -> PhotoAddDraft {
        PhotoAddDraft {
            photo_id: CustomUUID::retention_cutoff(0),
            library_id: None,
            uploaded_by: 1,
            encrypted_metadata: b"enc_meta".to_vec(),
            metadata_nonce: [0u8; 12],
            resources: vec![PhotoResourceOp {
                resource_type: 0,
                op: hopnet_storage::store::BlobInsertOp {
                    blob_id: CustomUUID::retention_cutoff(1),
                    integrity_hash: Blake3Hash::from_bytes([0xCC; 32]),
                    added_bytes: 0,
                    file_size: 100,
                    fragments: vec![],
                    access: vec![],
                },
            }],
            metadata_access: vec![MetadataAccessEntry {
                user_id: 1,
                ephemeral_pubkey: [0x42; 32],
                encrypted_metadata_key: vec![0xFF; 48],
            }],
            operation_id: CustomUUID::retention_cutoff(2),
            cloud_fingerprint: Some([0x5A; 32]),
        }
    }

    // Should: carry the draft's cloud_fingerprint into the wire entry
    // unchanged, and preserve None for local-only assets.
    #[test]
    fn build_photo_add_threads_cloud_fingerprint() {
        let payload = build_photo_add(vec![make_draft()]);
        assert_eq!(payload.entries[0].cloud_fingerprint, Some([0x5A; 32]));

        let mut draft = make_draft();
        draft.cloud_fingerprint = None;
        let payload = build_photo_add(vec![draft]);
        assert_eq!(payload.entries[0].cloud_fingerprint, None);
    }

    #[test]
    fn photo_add_payload_bincode_round_trip() {
        let payload = build_photo_add(vec![make_draft()]);
        let encoded = encode_payload(&payload).unwrap();
        let (decoded, _): (PhotoAddPayload, _) =
            bincode::serde::decode_from_slice(&encoded, bincode::config::standard()).unwrap();
        let encoded2 = encode_payload(&decoded).unwrap();
        assert_eq!(
            encoded, encoded2,
            "bincode round-trip must be byte-identical"
        );
    }

    #[test]
    fn photo_delete_payload_round_trip() {
        let payload = build_photo_delete(vec![CustomUUID::retention_cutoff(3)]);
        let encoded = encode_payload(&payload).unwrap();
        let (decoded, _): (PhotoDeletePayload, _) =
            bincode::serde::decode_from_slice(&encoded, bincode::config::standard()).unwrap();
        assert_eq!(payload.entries.len(), decoded.entries.len());
    }

    #[test]
    fn photo_favorite_payload_round_trip() {
        let payload = build_photo_favorite(vec![CustomUUID::retention_cutoff(4)]);
        let encoded = encode_payload(&payload).unwrap();
        let (decoded, _): (PhotoFavoritePayload, _) =
            bincode::serde::decode_from_slice(&encoded, bincode::config::standard()).unwrap();
        assert_eq!(payload.entries.len(), decoded.entries.len());
    }

    #[test]
    fn build_photo_delete_mints_unique_operation_ids() {
        let payload = build_photo_delete(vec![
            CustomUUID::retention_cutoff(10),
            CustomUUID::retention_cutoff(11),
        ]);
        assert_eq!(payload.entries.len(), 2);
        assert_ne!(
            payload.entries[0].operation_id.to_string(),
            payload.entries[1].operation_id.to_string()
        );
    }

    // Should: round-trip a content edit carrying wraps and removals.
    #[test]
    fn photo_edit_content_payload_round_trip() {
        let payload = build_photo_edit_content(vec![PhotoEditContentDraft {
            photo_id: CustomUUID::retention_cutoff(5),
            resources: Vec::new(),
            encrypted_metadata: Some(b"meta".to_vec()),
            metadata_nonce: Some([0x11; 12]),
            metadata_access: vec![MetadataAccessEntry {
                user_id: 3,
                ephemeral_pubkey: [0x21; 32],
                encrypted_metadata_key: vec![0x31; 48],
            }],
            remove_resources: vec![1, 5],
            operation_id: CustomUUID::retention_cutoff(6),
        }]);
        let encoded = encode_payload(&payload).unwrap();
        let (decoded, _): (PhotoEditContentPayload, _) =
            bincode::serde::decode_from_slice(&encoded, bincode::config::standard()).unwrap();
        assert_eq!(decoded.entries[0].remove_resources, vec![1, 5]);
        assert_eq!(decoded.entries[0].metadata_access[0].user_id, 3);
    }

    // Should: round-trip a metadata-only edit's wraps.
    #[test]
    fn photo_edit_metadata_payload_round_trip() {
        let payload = build_photo_edit_metadata(vec![PhotoEditMetadataDraft {
            photo_id: CustomUUID::retention_cutoff(7),
            encrypted_metadata: b"refreshed".to_vec(),
            metadata_nonce: [0x41; 12],
            metadata_access: vec![MetadataAccessEntry {
                user_id: 4,
                ephemeral_pubkey: [0x51; 32],
                encrypted_metadata_key: vec![0x61; 48],
            }],
            operation_id: CustomUUID::retention_cutoff(8),
        }]);
        let encoded = encode_payload(&payload).unwrap();
        let (decoded, _): (PhotoEditMetadataPayload, _) =
            bincode::serde::decode_from_slice(&encoded, bincode::config::standard()).unwrap();
        assert_eq!(decoded.entries[0].metadata_access[0].user_id, 4);
        assert_eq!(decoded.entries[0].encrypted_metadata, b"refreshed".to_vec());
    }

    #[test]
    fn encode_payload_uses_standard_config() {
        // A single PhotoFavoriteEntry with fixed UUIDs: verify encoding
        // uses varint (not fixed-width integer).
        let payload = build_photo_favorite(vec![
            "00000000-0000-0000-0000-000000000001"
                .parse::<CustomUUID>()
                .unwrap(),
        ]);
        let encoded = encode_payload(&payload).unwrap();
        // Vec len varint(1) = 0x01, then photo_id UUID varint(16) + 16 bytes, then operation_id likewise.
        // The second byte is varint(16) = 0x10 (NOT 0x10 0x00 0x00 0x00 — that would be fixed-width).
        assert_eq!(encoded[0], 0x01, "vec len varint");
        assert_eq!(
            encoded[1], 0x10,
            "UUID uses varint encoding (standard config)"
        );
    }
}
