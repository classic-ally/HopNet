//! Photos transaction envelopes (RFC-011 Phase 1).
//!
//! Wire types for the photos projection's consensus transactions.
//! Serde field order is bincode-frozen; do not reorder. Bincode is
//! positional — field names are cosmetic; order IS the wire format.
//!
//! Evolution convention (matching drive): new function name, never
//! modify a frozen type. The envelope file lives here rather than in
//! photos-core so the projection crate owns its wire format as a single
//! freeze surface.

use hopnet_common::CustomUUID;
use serde::{Deserialize, Serialize};

// --- photo_add ---

/// The photos projection's insert envelope: substrate blob registrations
/// alongside the photo metadata that reference them. Both halves apply in
/// ONE handler transaction — blob + first reference are atomic, so
/// mark-and-sweep never observes a zero-ref blob (drive precedent:
/// hopnet-drive/src/envelopes.rs:13-16).
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PhotoAddPayload {
    pub entries: Vec<PhotoAddEntry>,
}

/// One photo in a batch add. Single upload is a batch of one.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PhotoAddEntry {
    /// Client-minted UUIDv7 (photos.md:56). Timestamp encodes upload date.
    pub photo_id: CustomUUID,
    /// NULL = personal library; set = a `shared_libraries` id.
    pub library_id: Option<CustomUUID>,
    /// Must equal the submitting user's id — enforced by the handler.
    pub uploaded_by: i32,
    /// ChaCha20-Poly1305 encrypted metadata blob (date, dimensions, EXIF,
    /// GPS, camera info, group_id/group_type/group_index/is_group_pick).
    pub encrypted_metadata: Vec<u8>,
    /// 12-byte ChaCha20 nonce for metadata decryption.
    pub metadata_nonce: [u8; 12],
    /// One entry per resource (original, edited, paired_video, etc.).
    /// The BlobInsertOp pairs the resource_type tag with its blob.
    pub resources: Vec<PhotoResourceOp>,
    /// Per-user metadata key wraps, including at minimum the uploader.
    /// Follows the `blob_access` pattern: ephemeral ECDH + wrapped
    /// per-photo metadata key. For personal photos, exactly one entry
    /// (uploader); for shared, one per library member.
    pub metadata_access: Vec<MetadataAccessEntry>,
    /// UUIDv7 for the `photo_operations` row.
    pub operation_id: CustomUUID,
}

/// A resource-type tag paired with its blob registration.
/// The struct pairing is load-bearing where drive's parallel `blob_ops`
/// and `inodes` vecs were — drive inodes carry path/owner/type beyond
/// blob_id, but a photo resource IS only its resource_type applied to
/// a blob, so pairing them structurally eliminates a length-mismatch
/// failure mode.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PhotoResourceOp {
    /// RFC-011 resource_type value (0=original, 1=edited, 2=paired_video,
    /// 3=adjustment_data, 4=raw_alternate, 7=edited_paired_video).
    /// i32 sidesteps bincode enum-variant order freeze.
    pub resource_type: i32,
    /// Substrate blob insert op — BlobInsertOp.access carries the
    /// per-blob key wraps pre-computed client-side (no RNG in handlers).
    pub op: hopnet_storage::store::BlobInsertOp,
}

/// Per-user metadata key wrap for the `photo_metadata_access` table.
/// Mirrors `BlobAccess` but is user_id-keyed rather than pubkey-keyed
/// (the photos projection knows users, the substrate doesn't).
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MetadataAccessEntry {
    pub user_id: i32,
    /// Fresh per-wrap ephemeral X25519 pubkey (32 bytes).
    pub ephemeral_pubkey: [u8; 32],
    /// ChaCha20-Poly1305(wrap_key, nonce, metadata_key) — 48 bytes.
    pub encrypted_metadata_key: Vec<u8>,
}

// --- photo_delete ---

/// Batch soft-delete. `deleted_at` is derived by the handler from each
/// entry's `operation_id.extract_timestamp()` — every timestamp in the
/// system is UUIDv7-derived (src/consensus/dispatch.rs:101, takeout
/// precedent); `datetime('now')` in consensus apply is nondeterministic.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PhotoDeletePayload {
    pub entries: Vec<PhotoDeleteEntry>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PhotoDeleteEntry {
    pub photo_id: CustomUUID,
    /// UUIDv7 — handler extracts deleted_at from the embedded timestamp.
    pub operation_id: CustomUUID,
}

// --- photo_restore ---

/// Batch restore (clear `deleted_at`/`deleted_by`, log operation).
/// Symmetric inverse of delete — same wire shape.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PhotoRestorePayload {
    pub entries: Vec<PhotoRestoreEntry>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PhotoRestoreEntry {
    pub photo_id: CustomUUID,
    pub operation_id: CustomUUID,
}

// --- photo_cleanup_expired ---

/// System-maintenance batch hard-delete of expired tombstones. Submitted
/// by the periodic cleanup cron (TxSigner::Node, no user auth). The
/// handler deterministically checks `datetime(deleted_at, '+30 days') <
/// datetime(scan_cutoff)` — the cutoff rides the payload, so all
/// validators apply the same predicate regardless of their local clock.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PhotoCleanupExpiredPayload {
    pub photo_ids: Vec<CustomUUID>,
    /// The scan's `datetime('now')` — wall-clock on the submitting node,
    /// deterministic across replicas because it's payload data.
    pub scan_cutoff: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use hopnet_common::Blake3Hash;

    /// Wire-freeze guard: encodes a minimal PhotoDeletePayload (the
    /// simplest payload type — two fixed UUIDs), asserts the exact byte
    /// sequence. CustomUUID serializes as 17 bytes (bincode varint len +
    /// 16 raw UUID bytes). i32 uses signed varint (bincode standard
    /// config → VarintEncoding). If this test fails, the bincode
    /// serialization of either CustomUUID, Vec, or one of the int
    /// encodings changed — a hard wire break.
    #[test]
    fn photo_delete_payload_golden_bytes() {
        let payload = PhotoDeletePayload {
            entries: vec![PhotoDeleteEntry {
                photo_id: "00000000-0000-0000-0000-000000000001"
                    .parse()
                    .unwrap(),
                operation_id: "00000000-0000-0000-0000-000000000002"
                    .parse()
                    .unwrap(),
            }],
        };
        let encoded = bincode::serde::encode_to_vec(&payload, bincode::config::standard())
            .expect("golden encode must succeed");
        // Vec<PhotoDeleteEntry>:
        //   varint(1) = 1                          — vec len
        //   CustomUUID(varint(16) + 16 bytes)      — photo_id
        //   CustomUUID(varint(16) + 16 bytes)      — operation_id
        let mut expected = vec![0x01u8]; // vec len 1
        expected.push(0x10); // varint(16) — UUID length prefix
        expected.extend_from_slice(&[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
        ]); // UUID bytes
        expected.push(0x10); // varint(16)
        expected.extend_from_slice(&[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
        ]); // UUID bytes
        assert_eq!(encoded, expected, "bincode wire format changed — field reorder or type change");
    }

    /// Golden bytes for MetadataAccessEntry — pins the fixed-size array
    /// encoding ([u8; 32] has no length prefix) and the i32 signed-varint
    /// encoding (user_id=1 → zigzag(1)=2 → varint 0x02).
    #[test]
    fn metadata_access_entry_golden_bytes() {
        let entry = MetadataAccessEntry {
            user_id: 1,
            ephemeral_pubkey: [0xAB; 32],
            encrypted_metadata_key: vec![0xCC; 48],
        };
        let encoded = bincode::serde::encode_to_vec(&entry, bincode::config::standard())
            .expect("golden encode must succeed");
        // user_id signed varint(0x02) + [u8; 32] raw + Vec len varint(48) + 48 bytes
        let mut expected = vec![0x02u8]; // user_id = 1 (signed varint zigzag)
        expected.extend_from_slice(&[0xAB; 32]); // ephemeral_pubkey — no length prefix
        expected.push(0x30); // vec len 48 (varint)
        expected.extend_from_slice(&[0xCC; 48]); // encrypted_metadata_key
        assert_eq!(encoded, expected, "MetadataAccessEntry wire format changed");
    }

    /// Golden round-trip: encode a minimal photo_add payload, decode it,
    /// assert equality. Pins field order — a mismatch here means the wire
    /// format changed, which is a hard break for all replicas.
    #[test]
    fn photo_add_payload_bincode_round_trip() {
        let payload = PhotoAddPayload {
            entries: vec![PhotoAddEntry {
                photo_id: CustomUUID::retention_cutoff(0), // roughly-now UUIDv7
                library_id: None,
                uploaded_by: 1,
                encrypted_metadata: b"fake_encrypted_meta".to_vec(),
                metadata_nonce: [0u8; 12],
                resources: vec![PhotoResourceOp {
                    resource_type: 0, // original
                    op: hopnet_storage::store::BlobInsertOp {
                        blob_id: CustomUUID::retention_cutoff(1),
                        integrity_hash: Blake3Hash::from_bytes([0xAB; 32]),
                        added_bytes: 0,
                        file_size: 1024,
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
            }],
        };

        let encoded = bincode::serde::encode_to_vec(&payload, bincode::config::standard())
            .expect("photo_add encode must succeed");
        let (decoded, _len): (PhotoAddPayload, _) =
            bincode::serde::decode_from_slice(&encoded, bincode::config::standard())
                .expect("photo_add decode must succeed");

        let encoded2 = bincode::serde::encode_to_vec(&decoded, bincode::config::standard())
            .expect("re-encode must succeed");
        assert_eq!(encoded, encoded2, "bincode round-trip must be byte-identical");
    }

    /// Golden round-trip for photo_delete — verifies field order is frozen.
    #[test]
    fn photo_delete_payload_bincode_round_trip() {
        let payload = PhotoDeletePayload {
            entries: vec![PhotoDeleteEntry {
                photo_id: CustomUUID::retention_cutoff(3),
                operation_id: CustomUUID::retention_cutoff(4),
            }],
        };
        let encoded = bincode::serde::encode_to_vec(&payload, bincode::config::standard())
            .expect("photo_delete encode must succeed");
        let (decoded, _len): (PhotoDeletePayload, _) =
            bincode::serde::decode_from_slice(&encoded, bincode::config::standard())
                .expect("photo_delete decode must succeed");
        let encoded2 = bincode::serde::encode_to_vec(&decoded, bincode::config::standard())
            .expect("re-encode must succeed");
        assert_eq!(encoded, encoded2, "bincode round-trip must be byte-identical");
    }

    /// Golden round-trip for photo_restore — symmetric shape as delete.
    #[test]
    fn photo_restore_payload_bincode_round_trip() {
        let payload = PhotoRestorePayload {
            entries: vec![PhotoRestoreEntry {
                photo_id: CustomUUID::retention_cutoff(5),
                operation_id: CustomUUID::retention_cutoff(6),
            }],
        };
        let encoded = bincode::serde::encode_to_vec(&payload, bincode::config::standard())
            .expect("photo_restore encode must succeed");
        let (decoded, _len): (PhotoRestorePayload, _) =
            bincode::serde::decode_from_slice(&encoded, bincode::config::standard())
                .expect("photo_restore decode must succeed");
        let encoded2 = bincode::serde::encode_to_vec(&decoded, bincode::config::standard())
            .expect("re-encode must succeed");
        assert_eq!(encoded, encoded2, "bincode round-trip must be byte-identical");
    }

    /// Golden bytes for PhotoCleanupExpiredPayload — pins the Vec of UUID
    /// encoding plus the scan_cutoff String.
    #[test]
    fn photo_cleanup_expired_payload_golden_bytes() {
        let payload = PhotoCleanupExpiredPayload {
            photo_ids: vec![
                "00000000-0000-0000-0000-00000000000a".parse().unwrap(),
                "00000000-0000-0000-0000-00000000000b".parse().unwrap(),
            ],
            scan_cutoff: "2025-01-01T00:00:00Z".into(),
        };
        let encoded = bincode::serde::encode_to_vec(&payload, bincode::config::standard())
            .expect("golden encode must succeed");
        // Vec<CustomUUID>: varint(2) + 2 × (varint(16) + 16 bytes)
        // String scan_cutoff: varint(20) + "2025-01-01T00:00:00Z"
        let mut expected = vec![0x02u8]; // vec len 2
        expected.push(0x10); // varint(16) — first UUID
        expected.extend_from_slice(&[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0a,
        ]);
        expected.push(0x10); // varint(16) — second UUID
        expected.extend_from_slice(&[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0b,
        ]);
        expected.push(0x14); // varint(20) — string len
        expected.extend_from_slice(b"2025-01-01T00:00:00Z");
        assert_eq!(encoded, expected, "PhotoCleanupExpiredPayload wire format changed");
    }
}
