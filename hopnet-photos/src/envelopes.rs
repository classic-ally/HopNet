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
    /// Cross-device asset identity: keyed HMAC (blake3 keyed_hash under a
    /// per-user derived key) of the source library's stable asset id
    /// (PHCloudIdentifier). None = local-only asset, no dedupe. Opaque to
    /// validators (no user key material node-side at validation) —
    /// enforcement is solely the partial UNIQUE indexes on
    /// photos.cloud_fingerprint. Appended pre-release (amend-in-place;
    /// dev meshes wiped) — the freeze convention applies from first release.
    pub cloud_fingerprint: Option<[u8; 32]>,
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

// --- photo_edit_content ---

/// Batch content edit: replace a resource's data block (original →
/// edited, or edited → new edited). The handler reads the current
/// `data_block_id` from `photo_resources` at execution time as the prior
/// value (LWW contract: photos.md:544-547). Thumbnail resource ops
/// (types 5/6) ride alongside the primary edited resource.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PhotoEditContentPayload {
    pub entries: Vec<PhotoEditContentEntry>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PhotoEditContentEntry {
    pub photo_id: CustomUUID,
    /// Resources to upsert — the first entry is the primary edited
    /// resource (logged in photo_operations with prior/new); additional
    /// entries are thumbnails (upserted without individual logging).
    /// May be empty iff `remove_resources` is not (a revert that only
    /// drops the edited render).
    pub resources: Vec<PhotoResourceOp>,
    /// Optional metadata update (e.g. dimensions changed by crop).
    pub encrypted_metadata: Option<Vec<u8>>,
    pub metadata_nonce: Option<[u8; 12]>,
    /// UUIDv7 for the photo_operations row.
    pub operation_id: CustomUUID,
    /// Re-wraps of the metadata key the new `encrypted_metadata` is under.
    /// Required whenever metadata is present and the writer minted a fresh
    /// key rather than reusing the photo's existing one — without them the
    /// stored wraps would unwrap to the OLD key and the new ciphertext
    /// would be undecryptable for every member, silently. Empty is legal
    /// only for a writer that re-encrypted under the existing key (a member
    /// client, which can unwrap its own `photo_metadata_access` row); the
    /// ingress daemon holds no private key and always sends fresh wraps.
    /// Appended pre-release — see the freeze convention on `cloud_fingerprint`.
    pub metadata_access: Vec<MetadataAccessEntry>,
    /// Resource types to drop entirely (wire values of `ResourceKind`). A
    /// revert in Apple Photos removes the edited render rather than
    /// replacing it, and no upsert can express an absence. The blob is not
    /// decremented here — `photo_operations` still references it for the
    /// undo window, and the orphan sweep collects it after.
    /// Appended pre-release.
    pub remove_resources: Vec<i32>,
}

// --- photo_edit_metadata ---

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PhotoEditMetadataPayload {
    pub entries: Vec<PhotoEditMetadataEntry>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PhotoEditMetadataEntry {
    pub photo_id: CustomUUID,
    pub encrypted_metadata: Vec<u8>,
    pub metadata_nonce: [u8; 12],
    pub operation_id: CustomUUID,
    /// Re-wraps of the metadata key — same contract as
    /// [`PhotoEditContentEntry::metadata_access`]. Appended pre-release.
    pub metadata_access: Vec<MetadataAccessEntry>,
}

// --- photo_undo ---

/// Content-only undo: swap a resource back to its prior blob. Metadata
/// undo is client-side (handler can't read encrypted operation_data);
/// album/favorite undo uses dedicated handlers.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PhotoUndoPayload {
    pub entries: Vec<PhotoUndoEntry>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PhotoUndoEntry {
    pub photo_id: CustomUUID,
    /// The operation to revert — validated as the latest revertible
    /// content edit.
    pub target_operation_id: CustomUUID,
    /// UUIDv7 for the new photo_operations row (logs the undo itself as
    /// type=1 with prior/new swapped).
    pub operation_id: CustomUUID,
}

// --- photo_favorite ---

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PhotoFavoritePayload {
    pub entries: Vec<PhotoFavoriteEntry>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PhotoFavoriteEntry {
    pub photo_id: CustomUUID,
    pub operation_id: CustomUUID,
}

// --- photo_unfavorite ---

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PhotoUnfavoritePayload {
    pub entries: Vec<PhotoUnfavoriteEntry>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PhotoUnfavoriteEntry {
    pub photo_id: CustomUUID,
    pub operation_id: CustomUUID,
}

// --- photo_ingress_claim ---

/// Claim (or transfer — same operation, upsert) ingress responsibility
/// for the submitting user within one scope: the personal partition
/// (`library_id: None`) or a shared library the user is a member of.
/// Responsibility is per (user, scope) — each member claims
/// independently for their own devices, and cross-member dedup within a
/// shared library is the fingerprint pair's job, not responsibility's.
/// The handler validates the device exists and belongs to the
/// submitting user against the consensus-replicated `device_tokens`
/// table (and membership for a shared scope), then upserts
/// `photo_ingress_responsibility`. JWT-route only: the thin-client
/// device route rejects this tx kind, so a daemon can never claim for
/// itself.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PhotoIngressClaimPayload {
    pub device_id: CustomUUID,
    /// UUIDv7 — audit/ordering stamp for the responsibility row.
    pub operation_id: CustomUUID,
    /// Scope of the claim. FINAL bincode field (appended after the
    /// personal-only v1 shipped — same precedent as
    /// `PhotoAddEntry::cloud_fingerprint`).
    pub library_id: Option<CustomUUID>,
}

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

// --- shared-library membership lifecycle ---

/// One user's X25519 ECDH wrap of a library key (LIBRARY_KEY_WRAP_DOMAIN,
/// wrap id = library id bytes). Rides create (creator) and invite
/// (invitee) payloads; lands in `shared_library_keys` /
/// `shared_library_invites`.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LibraryKeyWrap {
    pub user_id: i32,
    /// Fresh per-wrap ephemeral X25519 pubkey (32 bytes).
    pub ephemeral_pubkey: [u8; 32],
    /// ChaCha20-Poly1305(wrap_key, nonce, library_key) — 48 bytes.
    pub wrapped_key: Vec<u8>,
}

/// Mint a shared library: row + creator membership + creator's key wrap.
/// The library key is client-minted; the name is encrypted under it so
/// every current and future member can render it (via their own wrap).
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CreateSharedLibraryPayload {
    /// Client-minted UUIDv7.
    pub library_id: CustomUUID,
    /// ChaCha20-Poly1305(library_key, name_nonce, name).
    pub encrypted_name: Vec<u8>,
    pub name_nonce: [u8; 12],
    /// Creator's wrap — `user_id` must equal the submitting user.
    pub creator_key: LibraryKeyWrap,
    /// UUIDv7, audit/ordering.
    pub operation_id: CustomUUID,
}

/// Invite a mesh user into a library (consent pattern: membership only
/// materializes at the invitee's accept). Carries the invitee's library-
/// key wrap, minted AT invite time by the inviter, so accept needs no
/// inviter online and the library name renders in the invite listing.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LibraryInvitePayload {
    pub library_id: CustomUUID,
    /// The invitee's wrap — `user_id` is the invited user.
    pub invitee: LibraryKeyWrap,
    pub operation_id: CustomUUID,
}

/// Invitee-signed consent: insert membership, promote the invite-row key
/// wrap into `shared_library_keys`, delete the invite, and signal the
/// invitee's view change (the sidecar backfills the library client-side —
/// no photo_changes writes; the photos did not change).
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LibraryInviteAcceptPayload {
    pub library_id: CustomUUID,
    pub operation_id: CustomUUID,
}

/// Withdraw a pending invite. Submitter may be the invitee (refusal) or
/// any library member (retraction) — the drive decline pattern
/// generalized to equal-standing membership.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LibraryInviteDeclinePayload {
    pub library_id: CustomUUID,
    pub invitee_user_id: i32,
    pub operation_id: CustomUUID,
}

/// Remove a member or pending invitee. Self-removal is leave; removing
/// another is kick (all members have equal standing — RFC-011). Deletes
/// membership + key wrap + pending invite + the target's view-signal
/// row; the convergence worker lazily revokes the target's access rows
/// afterwards (row-deletion revocation; key rotation is a future lane).
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LibraryRemoveMemberPayload {
    pub library_id: CustomUUID,
    pub user_id: i32,
    pub operation_id: CustomUUID,
}

/// One photo's metadata-key wrap for a grant target.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LibraryMetadataGrant {
    pub photo_id: CustomUUID,
    pub ephemeral_pubkey: [u8; 32],
    /// ChaCha20-Poly1305 wrap of the per-photo metadata key — 48 bytes.
    pub encrypted_metadata_key: Vec<u8>,
}

/// One data block's blob-key wrap for a grant target. Deliberately NO
/// recipient pubkey on the wire: the handler resolves the target user's
/// `users.x25519_pubkey` itself, so a malicious grantor cannot plant
/// wraps for arbitrary keys.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LibraryBlobGrant {
    pub data_block_id: CustomUUID,
    pub ephemeral_pubkey: [u8; 32],
    /// ChaCha20-Poly1305 wrap of the per-blob file key — 48 bytes.
    pub wrapped_key: Vec<u8>,
}

/// Convergence-worker grant batch: access rows for ONE target user
/// (member or pending invitee) of ONE library. Inserts are OR IGNORE —
/// first committed wrap wins, so racing workers are harmless. Ends by
/// signalling the target's view change.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LibraryAccessGrantPayload {
    pub library_id: CustomUUID,
    pub user_id: i32,
    pub entries: Vec<LibraryMetadataGrant>,
    pub blob_wraps: Vec<LibraryBlobGrant>,
    pub operation_id: CustomUUID,
}

/// Convergence-worker revoke batch: delete access rows of a user who is
/// neither member nor invitee (the handler enforces that inversion — a
/// live member cannot be stealth-revoked; kicks go through
/// `library_remove_member`).
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LibraryAccessRevokePayload {
    pub library_id: CustomUUID,
    pub user_id: i32,
    pub photo_ids: Vec<CustomUUID>,
    pub data_block_ids: Vec<CustomUUID>,
    pub operation_id: CustomUUID,
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
                photo_id: "00000000-0000-0000-0000-000000000001".parse().unwrap(),
                operation_id: "00000000-0000-0000-0000-000000000002".parse().unwrap(),
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
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x01,
        ]); // UUID bytes
        expected.push(0x10); // varint(16)
        expected.extend_from_slice(&[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x02,
        ]); // UUID bytes
        assert_eq!(
            encoded, expected,
            "bincode wire format changed — field reorder or type change"
        );
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
                cloud_fingerprint: Some([0x5A; 32]),
            }],
        };

        let encoded = bincode::serde::encode_to_vec(&payload, bincode::config::standard())
            .expect("photo_add encode must succeed");
        let (decoded, _len): (PhotoAddPayload, _) =
            bincode::serde::decode_from_slice(&encoded, bincode::config::standard())
                .expect("photo_add decode must succeed");

        let encoded2 = bincode::serde::encode_to_vec(&decoded, bincode::config::standard())
            .expect("re-encode must succeed");
        assert_eq!(
            encoded, encoded2,
            "bincode round-trip must be byte-identical"
        );
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
        assert_eq!(
            encoded, encoded2,
            "bincode round-trip must be byte-identical"
        );
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
        assert_eq!(
            encoded, encoded2,
            "bincode round-trip must be byte-identical"
        );
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
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x0a,
        ]);
        expected.push(0x10); // varint(16) — second UUID
        expected.extend_from_slice(&[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x0b,
        ]);
        expected.push(0x14); // varint(20) — string len
        expected.extend_from_slice(b"2025-01-01T00:00:00Z");
        assert_eq!(
            encoded, expected,
            "PhotoCleanupExpiredPayload wire format changed"
        );
    }

    /// Golden bytes for PhotoFavoriteEntry — two UUIDs.
    #[test]
    fn photo_favorite_entry_golden_bytes() {
        let entry = PhotoFavoriteEntry {
            photo_id: "00000000-0000-0000-0000-0000000000ff".parse().unwrap(),
            operation_id: "00000000-0000-0000-0000-0000000000fe".parse().unwrap(),
        };
        let mut expected = vec![0x10u8]; // varint(16) — photo_id
        expected.extend_from_slice(&[0u8; 15]);
        expected.push(0xff);
        expected.push(0x10); // varint(16) — operation_id
        expected.extend_from_slice(&[0u8; 15]);
        expected.push(0xfe);
        let encoded = bincode::serde::encode_to_vec(&entry, bincode::config::standard()).unwrap();
        assert_eq!(encoded, expected, "PhotoFavoriteEntry wire format changed");
    }

    /// Should: encode cloud_fingerprint as the FINAL field of
    /// PhotoAddEntry — None as a single 0x00 tail byte, Some as 0x01
    /// followed by 32 raw bytes, with every preceding byte identical
    /// between the two encodings.
    /// Impact: pins the amended (pre-release) wire position of the dedupe
    /// fingerprint; a positional shift silently corrupts every field
    /// behind it on decode.
    #[test]
    fn photo_add_entry_fingerprint_tail_bytes() {
        let base = PhotoAddEntry {
            photo_id: "00000000-0000-0000-0000-000000000001".parse().unwrap(),
            library_id: None,
            uploaded_by: 1,
            encrypted_metadata: vec![0xEE; 4],
            metadata_nonce: [0u8; 12],
            resources: vec![],
            metadata_access: vec![],
            operation_id: "00000000-0000-0000-0000-000000000002".parse().unwrap(),
            cloud_fingerprint: None,
        };
        let mut with_fp = base.clone();
        with_fp.cloud_fingerprint = Some([0xD4; 32]);

        let none_bytes = bincode::serde::encode_to_vec(&base, bincode::config::standard()).unwrap();
        let some_bytes =
            bincode::serde::encode_to_vec(&with_fp, bincode::config::standard()).unwrap();

        assert_eq!(
            none_bytes.last(),
            Some(&0x00),
            "None must encode as a 0x00 tail"
        );
        assert_eq!(
            some_bytes.len(),
            none_bytes.len() + 32,
            "Some must add exactly the 32 fingerprint bytes"
        );
        let split = none_bytes.len() - 1;
        assert_eq!(
            some_bytes[..split],
            none_bytes[..split],
            "all fields before the fingerprint must be byte-identical"
        );
        assert_eq!(some_bytes[split], 0x01, "Some tag byte");
        assert_eq!(
            some_bytes[split + 1..],
            [0xD4; 32],
            "raw fingerprint bytes, no length prefix"
        );
    }

    /// Should: encode PhotoIngressClaimPayload as two length-prefixed
    /// UUIDs (device_id, operation_id) followed by library_id as the
    /// FINAL field — None a single 0x00 tail byte, Some 0x01 plus the
    /// length-prefixed UUID, with every preceding byte identical between
    /// the two encodings.
    /// Impact: pins the amended (pre-release) wire position of the claim
    /// scope; a positional shift silently corrupts decode.
    #[test]
    fn photo_ingress_claim_payload_golden_bytes() {
        let payload = PhotoIngressClaimPayload {
            device_id: "00000000-0000-0000-0000-0000000000aa".parse().unwrap(),
            operation_id: "00000000-0000-0000-0000-0000000000ab".parse().unwrap(),
            library_id: None,
        };
        let mut expected = vec![0x10u8]; // varint(16) — device_id
        expected.extend_from_slice(&[0u8; 15]);
        expected.push(0xaa);
        expected.push(0x10); // varint(16) — operation_id
        expected.extend_from_slice(&[0u8; 15]);
        expected.push(0xab);
        expected.push(0x00); // library_id: None tail
        let encoded = bincode::serde::encode_to_vec(&payload, bincode::config::standard()).unwrap();
        assert_eq!(
            encoded, expected,
            "PhotoIngressClaimPayload wire format changed"
        );

        let mut scoped = payload.clone();
        scoped.library_id = Some("00000000-0000-0000-0000-0000000000ac".parse().unwrap());
        let scoped_bytes =
            bincode::serde::encode_to_vec(&scoped, bincode::config::standard()).unwrap();
        let split = encoded.len() - 1;
        assert_eq!(
            scoped_bytes[..split],
            encoded[..split],
            "all fields before the scope must be byte-identical"
        );
        assert_eq!(scoped_bytes[split], 0x01, "Some tag byte");
        assert_eq!(scoped_bytes[split + 1], 0x10, "varint(16) — library_id");
        assert_eq!(scoped_bytes.last(), Some(&0xac));
        assert_eq!(scoped_bytes.len(), encoded.len() + 17);
    }

    // Should: encode LibraryKeyWrap as signed-varint user_id, raw 32-byte
    // ephemeral pubkey (no length prefix), then length-prefixed wrapped key.
    // Impact: this struct nests inside create and invite payloads — a
    // positional shift here corrupts both wire formats at once.
    #[test]
    fn library_key_wrap_golden_bytes() {
        let wrap = LibraryKeyWrap {
            user_id: 1,
            ephemeral_pubkey: [0xAB; 32],
            wrapped_key: vec![0xCC; 48],
        };
        let encoded = bincode::serde::encode_to_vec(&wrap, bincode::config::standard()).unwrap();
        let mut expected = vec![0x02u8]; // user_id = 1 (signed varint zigzag)
        expected.extend_from_slice(&[0xAB; 32]); // ephemeral_pubkey — no length prefix
        expected.push(0x30); // vec len 48 (varint)
        expected.extend_from_slice(&[0xCC; 48]);
        assert_eq!(encoded, expected, "LibraryKeyWrap wire format changed");
    }

    // Should: encode CreateSharedLibraryPayload as library_id UUID,
    // length-prefixed name ciphertext, raw 12-byte nonce, nested
    // LibraryKeyWrap, then operation_id UUID — in that order.
    #[test]
    fn create_shared_library_payload_golden_bytes() {
        let payload = CreateSharedLibraryPayload {
            library_id: "00000000-0000-0000-0000-0000000000c1".parse().unwrap(),
            encrypted_name: vec![0xEE; 4],
            name_nonce: [0xA1; 12],
            creator_key: LibraryKeyWrap {
                user_id: 1,
                ephemeral_pubkey: [0xAB; 32],
                wrapped_key: vec![0xCC; 48],
            },
            operation_id: "00000000-0000-0000-0000-0000000000c2".parse().unwrap(),
        };
        let encoded = bincode::serde::encode_to_vec(&payload, bincode::config::standard()).unwrap();
        let mut expected = vec![0x10u8]; // varint(16) — library_id
        expected.extend_from_slice(&[0u8; 15]);
        expected.push(0xc1);
        expected.push(0x04); // encrypted_name len 4
        expected.extend_from_slice(&[0xEE; 4]);
        expected.extend_from_slice(&[0xA1; 12]); // name_nonce — no length prefix
        expected.push(0x02); // creator_key.user_id = 1
        expected.extend_from_slice(&[0xAB; 32]);
        expected.push(0x30); // wrapped_key len 48
        expected.extend_from_slice(&[0xCC; 48]);
        expected.push(0x10); // varint(16) — operation_id
        expected.extend_from_slice(&[0u8; 15]);
        expected.push(0xc2);
        assert_eq!(
            encoded, expected,
            "CreateSharedLibraryPayload wire format changed"
        );
    }

    // Should: round-trip every membership-lifecycle payload byte-identically.
    #[test]
    fn library_lifecycle_payloads_bincode_round_trip() {
        fn round_trip<T>(payload: &T)
        where
            T: Serialize + for<'de> Deserialize<'de>,
        {
            let encoded =
                bincode::serde::encode_to_vec(payload, bincode::config::standard()).unwrap();
            let (decoded, _): (T, _) =
                bincode::serde::decode_from_slice(&encoded, bincode::config::standard()).unwrap();
            let encoded2 =
                bincode::serde::encode_to_vec(&decoded, bincode::config::standard()).unwrap();
            assert_eq!(
                encoded, encoded2,
                "bincode round-trip must be byte-identical"
            );
        }

        let lib: CustomUUID = "00000000-0000-0000-0000-0000000000d1".parse().unwrap();
        let op: CustomUUID = "00000000-0000-0000-0000-0000000000d2".parse().unwrap();
        round_trip(&LibraryInvitePayload {
            library_id: lib.clone(),
            invitee: LibraryKeyWrap {
                user_id: 2,
                ephemeral_pubkey: [0x11; 32],
                wrapped_key: vec![0x22; 48],
            },
            operation_id: op.clone(),
        });
        round_trip(&LibraryInviteAcceptPayload {
            library_id: lib.clone(),
            operation_id: op.clone(),
        });
        round_trip(&LibraryInviteDeclinePayload {
            library_id: lib.clone(),
            invitee_user_id: 2,
            operation_id: op.clone(),
        });
        round_trip(&LibraryRemoveMemberPayload {
            library_id: lib.clone(),
            user_id: 2,
            operation_id: op.clone(),
        });
        round_trip(&LibraryAccessGrantPayload {
            library_id: lib.clone(),
            user_id: 2,
            entries: vec![LibraryMetadataGrant {
                photo_id: "00000000-0000-0000-0000-0000000000d3".parse().unwrap(),
                ephemeral_pubkey: [0x33; 32],
                encrypted_metadata_key: vec![0x44; 48],
            }],
            blob_wraps: vec![LibraryBlobGrant {
                data_block_id: "00000000-0000-0000-0000-0000000000d4".parse().unwrap(),
                ephemeral_pubkey: [0x55; 32],
                wrapped_key: vec![0x66; 48],
            }],
            operation_id: op.clone(),
        });
        round_trip(&LibraryAccessRevokePayload {
            library_id: lib,
            user_id: 2,
            photo_ids: vec!["00000000-0000-0000-0000-0000000000d5".parse().unwrap()],
            data_block_ids: vec!["00000000-0000-0000-0000-0000000000d6".parse().unwrap()],
            operation_id: op,
        });
    }

    fn access_entry(user_id: i32) -> MetadataAccessEntry {
        MetadataAccessEntry {
            user_id,
            ephemeral_pubkey: [0x5C; 32],
            encrypted_metadata_key: vec![0x77; 48],
        }
    }

    /// Golden round-trip for photo_edit_metadata (batch of one).
    // Should: round-trip the appended metadata_access wraps byte-identically.
    #[test]
    fn photo_edit_metadata_bincode_round_trip() {
        let payload = PhotoEditMetadataPayload {
            entries: vec![PhotoEditMetadataEntry {
                photo_id: "00000000-0000-0000-0000-000000000001".parse().unwrap(),
                encrypted_metadata: b"updated_meta".to_vec(),
                metadata_nonce: [0xA1; 12],
                operation_id: "00000000-0000-0000-0000-000000000002".parse().unwrap(),
                metadata_access: vec![access_entry(1), access_entry(2)],
            }],
        };
        let encoded = bincode::serde::encode_to_vec(&payload, bincode::config::standard()).unwrap();
        let (decoded, _): (PhotoEditMetadataPayload, _) =
            bincode::serde::decode_from_slice(&encoded, bincode::config::standard()).unwrap();
        assert_eq!(decoded.entries[0].metadata_access.len(), 2);
        let encoded2 =
            bincode::serde::encode_to_vec(&decoded, bincode::config::standard()).unwrap();
        assert_eq!(
            encoded, encoded2,
            "bincode round-trip must be byte-identical"
        );
    }

    /// Golden round-trip for photo_edit_content — verifies field order is
    /// frozen across the appended wraps and removal list.
    // Should: round-trip a removal-only entry, whose resources vec is empty.
    #[test]
    fn photo_edit_content_bincode_round_trip() {
        let payload = PhotoEditContentPayload {
            entries: vec![PhotoEditContentEntry {
                photo_id: "00000000-0000-0000-0000-000000000003".parse().unwrap(),
                resources: Vec::new(),
                encrypted_metadata: Some(b"reverted_meta".to_vec()),
                metadata_nonce: Some([0xB2; 12]),
                operation_id: "00000000-0000-0000-0000-000000000004".parse().unwrap(),
                metadata_access: vec![access_entry(7)],
                remove_resources: vec![1, 5, 6],
            }],
        };
        let encoded = bincode::serde::encode_to_vec(&payload, bincode::config::standard())
            .expect("photo_edit_content encode must succeed");
        let (decoded, _len): (PhotoEditContentPayload, _) =
            bincode::serde::decode_from_slice(&encoded, bincode::config::standard())
                .expect("photo_edit_content decode must succeed");
        assert_eq!(decoded.entries[0].remove_resources, vec![1, 5, 6]);
        let encoded2 = bincode::serde::encode_to_vec(&decoded, bincode::config::standard())
            .expect("re-encode must succeed");
        assert_eq!(
            encoded, encoded2,
            "bincode round-trip must be byte-identical"
        );
    }
}
