//! Fragment cipher + key-wrap primitives.
//!
//! FORMAT-FROZEN (fragment cipher): `derive_chunk_key`, `derive_chunk_nonce`,
//! `encrypt_chunk`, `decrypt_chunk` moved verbatim from the main crate's
//! files/functions.rs — the on-disk/on-wire fragment ciphertext format must
//! survive the extraction byte-for-byte. The golden-vector tests at the bottom
//! of this file pin it; do not change derivation strings, segmenting, or the
//! stream construction without a deliberate format migration.
//!
//! The key-wrap here is the LEGACY (user_id-bound) format; Stage B replaces it
//! with the pubkey-bound "hopnet-storage … v1" wrap and the RecipientKey
//! capability seam.

use crate::error::StorageError;
use chacha20poly1305::aead::stream::{DecryptorBE32, EncryptorBE32};
use chacha20poly1305::aead::{Aead, KeyInit, OsRng};
use chacha20poly1305::ChaCha20Poly1305;
use hopnet_common::CustomUUID;
use x25519_dalek::{EphemeralSecret, PublicKey as X25519PublicKey};

/// Derive chunk encryption key from per-file key and fragment UUID
pub fn derive_chunk_key(
    per_file_key: &chacha20poly1305::Key,
    fragment_id: &CustomUUID,
) -> chacha20poly1305::Key {
    let mut key_bytes = [0u8; 32];
    let mut hasher = blake3::Hasher::new_derive_key("hopnet chunk_key");
    hasher.update(per_file_key);
    hasher.update(fragment_id.as_bytes());
    let mut xof = hasher.finalize_xof();
    xof.fill(&mut key_bytes);
    key_bytes.into()
}

/// Derive nonce from fragment UUID using Blake3 for collision resistance
pub fn derive_chunk_nonce(fragment_id: &CustomUUID) -> [u8; 7] {
    let mut nonce_bytes = [0u8; 7];
    let mut hasher = blake3::Hasher::new_derive_key("hopnet chunk_nonce");
    hasher.update(fragment_id.as_bytes());
    let mut xof = hasher.finalize_xof();
    xof.fill(&mut nonce_bytes);
    nonce_bytes
}

pub const BUFFER_SIZE: usize = 4096;

pub fn calculate_encrypted_chunk_length(chunk_length: usize) -> usize {
    chunk_length + (chunk_length.div_ceil(BUFFER_SIZE) * 16)
}

/// Encrypt a chunk using streaming ChaCha20-Poly1305 with true memory efficiency
pub fn encrypt_chunk(
    mut chunk: Vec<u8>, // Take ownership so we can consume it
    per_file_key: &chacha20poly1305::Key,
    fragment_id: &CustomUUID,
) -> Result<Vec<u8>, StorageError> {
    let chunk_key = derive_chunk_key(per_file_key, fragment_id);
    let nonce = derive_chunk_nonce(fragment_id);
    let cipher = ChaCha20Poly1305::new(&chunk_key);

    let mut stream_encryptor = EncryptorBE32::from_aead(cipher, nonce.as_ref().into());
    let mut encrypted_output = Vec::with_capacity(chunk.len() + 16);

    // Process all segments except the last one
    while chunk.len() > BUFFER_SIZE {
        let segment: Vec<u8> = chunk.drain(0..BUFFER_SIZE).collect();

        let ciphertext = stream_encryptor
            .encrypt_next(segment.as_slice())
            .map_err(|_| StorageError::Encryption)?;

        encrypted_output.extend_from_slice(&ciphertext);
        // segment is dropped here, freeing memory
    }

    // Process the last segment (or entire chunk if smaller than BUFFER_SIZE)
    if !chunk.is_empty() {
        let ciphertext = stream_encryptor
            .encrypt_last(chunk.as_slice())
            .map_err(|_| StorageError::Encryption)?;

        encrypted_output.extend_from_slice(&ciphertext);
    }

    Ok(encrypted_output)
}

/// Decrypt a chunk using streaming ChaCha20-Poly1305 with memory efficiency
pub fn decrypt_chunk(
    encrypted_chunk: &[u8],
    per_file_key: &chacha20poly1305::Key,
    fragment_id: &CustomUUID,
) -> Result<Vec<u8>, StorageError> {
    let chunk_key = derive_chunk_key(per_file_key, fragment_id);
    let nonce = derive_chunk_nonce(fragment_id);
    let cipher = ChaCha20Poly1305::new(&chunk_key);

    let mut stream_decryptor = DecryptorBE32::from_aead(cipher, nonce.as_ref().into());
    let mut decrypted_output = Vec::with_capacity(encrypted_chunk.len());

    const ENCRYPTED_SEGMENT_SIZE: usize = BUFFER_SIZE + 16; // Each segment has 16-byte auth tag
    let mut chunk_offset = 0;

    // Process all segments except the last one
    while chunk_offset + ENCRYPTED_SEGMENT_SIZE < encrypted_chunk.len() {
        let segment = &encrypted_chunk[chunk_offset..chunk_offset + ENCRYPTED_SEGMENT_SIZE];

        let plaintext = stream_decryptor
            .decrypt_next(segment)
            .map_err(|_| StorageError::Encryption)?;

        decrypted_output.extend_from_slice(&plaintext);
        chunk_offset += ENCRYPTED_SEGMENT_SIZE;
    }

    // Process the last segment (or entire chunk if smaller than ENCRYPTED_SEGMENT_SIZE)
    if chunk_offset < encrypted_chunk.len() {
        let segment = &encrypted_chunk[chunk_offset..];

        let plaintext = stream_decryptor
            .decrypt_last(segment)
            .map_err(|_| StorageError::Encryption)?;

        decrypted_output.extend_from_slice(&plaintext);
    }

    Ok(decrypted_output)
}

// --- v1 key custody (RFC-014) ----------------------------------------------

/// Domain-separation contexts for the v1 wrap/integrity family. New strings
/// (vs the legacy "hopnet key_wrap"/"hopnet wrap_nonce") give clean
/// separation from the pre-substrate format and from the app's other blake3
/// contexts. Fresh genesis makes the switch free.
pub const WRAP_KEY_CONTEXT_V1: &str = "hopnet-storage key_wrap v1";
pub const WRAP_NONCE_CONTEXT_V1: &str = "hopnet-storage wrap_nonce v1";
pub const INTEGRITY_CONTEXT_V1: &str = "hopnet-storage integrity v1";
pub const MESH_KEY_ID_CONTEXT_V1: &str = "hopnet-storage mesh_key id v1";

/// A v1 wrap domain: the (key, nonce) derive-key context pair. Frozen
/// wire-format constants — never edit an existing domain's strings; new
/// consumers mint their own `WrapDomain` const beside their envelope
/// freeze surface.
pub struct WrapDomain {
    pub key_context: &'static str,
    pub nonce_context: &'static str,
}

/// The substrate's own domain — bit-compatible with every existing
/// `blob_access` and `mesh_key_access` row.
pub const BLOB_WRAP_DOMAIN: WrapDomain = WrapDomain {
    key_context: WRAP_KEY_CONTEXT_V1,
    nonce_context: WRAP_NONCE_CONTEXT_V1,
};

/// Capability proving read access for ONE recipient pubkey. Implementations
/// hold the X25519 private key; it never crosses this trait — the substrate
/// receives only per-wrap shared secrets (single-use: every wrap has a fresh
/// ephemeral).
pub trait RecipientKey: Send + Sync {
    fn pubkey(&self) -> X25519PublicKey;
    /// x25519(privkey, ephemeral_pubkey) — the raw shared secret.
    fn dh(&self, ephemeral_pubkey: &X25519PublicKey) -> [u8; 32];
}

/// A RecipientKey backed by a held X25519 static secret. Hosts build these
/// from session-derived user keys (SessionRecipient) or the unwrapped mesh
/// privkey (MeshRecipient).
pub struct StaticRecipient(pub x25519_dalek::StaticSecret);

impl RecipientKey for StaticRecipient {
    fn pubkey(&self) -> X25519PublicKey {
        X25519PublicKey::from(&self.0)
    }
    fn dh(&self, ephemeral_pubkey: &X25519PublicKey) -> [u8; 32] {
        *self.0.diffie_hellman(ephemeral_pubkey).as_bytes()
    }
}

fn wrap_key_v1(domain: &WrapDomain, shared_secret: &[u8; 32]) -> chacha20poly1305::Key {
    let mut key_bytes = [0u8; 32];
    let mut hasher = blake3::Hasher::new_derive_key(domain.key_context);
    hasher.update(shared_secret);
    hasher.finalize_xof().fill(&mut key_bytes);
    key_bytes.into()
}

/// Deterministic wrap nonce, binding (wrap id, recipient, ephemeral). The
/// ephemeral is fresh per wrap so the wrap key is already single-use; the
/// bindings are defense-in-depth against cross-row ciphertext transplants.
fn wrap_nonce_v1(
    domain: &WrapDomain,
    id_bytes: &[u8; 16],
    recipient_pubkey: &[u8; 32],
    ephemeral_pubkey: &[u8; 32],
) -> [u8; 12] {
    let mut nonce_bytes = [0u8; 12];
    let mut hasher = blake3::Hasher::new_derive_key(domain.nonce_context);
    hasher.update(id_bytes);
    hasher.update(recipient_pubkey);
    hasher.update(ephemeral_pubkey);
    hasher.finalize_xof().fill(&mut nonce_bytes);
    nonce_bytes
}

fn wrap_to_recipient_v1(
    domain: &WrapDomain,
    id_bytes: &[u8; 16],
    recipient: &X25519PublicKey,
    key: &chacha20poly1305::Key,
) -> Result<([u8; 32], Vec<u8>), StorageError> {
    let ephemeral_secret = EphemeralSecret::random_from_rng(OsRng);
    let ephemeral_public = X25519PublicKey::from(&ephemeral_secret);
    let shared = ephemeral_secret.diffie_hellman(recipient);

    let wrap_key = wrap_key_v1(domain, shared.as_bytes());
    let nonce = wrap_nonce_v1(domain, id_bytes, recipient.as_bytes(), ephemeral_public.as_bytes());

    let wrapped = ChaCha20Poly1305::new(&wrap_key)
        .encrypt(&nonce.into(), key.as_slice())
        .map_err(|_| StorageError::Encryption)?;

    Ok((*ephemeral_public.as_bytes(), wrapped))
}

/// Wrap a per-blob key to a recipient pubkey (v1). Returns the replicable
/// `blob_access` row.
pub fn wrap_blob_key(
    blob_id: &crate::types::BlobId,
    recipient: &X25519PublicKey,
    per_blob_key: &chacha20poly1305::Key,
) -> Result<crate::types::BlobAccess, StorageError> {
    let (ephemeral_pubkey, wrapped_key) =
        wrap_to_recipient_v1(&BLOB_WRAP_DOMAIN, blob_id.as_bytes(), recipient, per_blob_key)?;
    Ok(crate::types::BlobAccess {
        blob_id: blob_id.clone(),
        recipient_pubkey: *recipient.as_bytes(),
        ephemeral_pubkey,
        wrapped_key,
    })
}

/// Unwrap a per-blob key via a reader capability (v1).
pub fn unwrap_blob_key(
    access: &crate::types::BlobAccess,
    reader: &dyn RecipientKey,
) -> Result<chacha20poly1305::Key, StorageError> {
    unwrap_v1(
        &BLOB_WRAP_DOMAIN,
        access.blob_id.as_bytes(),
        &access.recipient_pubkey,
        &access.ephemeral_pubkey,
        &access.wrapped_key,
        reader,
    )
}

/// Wrap a 32-byte key to a recipient pubkey under a wrap domain (v1).
/// Returns (ephemeral_pubkey, wrapped_key / 48 bytes). New consumers mint
/// their own [`WrapDomain`] const; the substrate's own `blob_access` rows
/// use [`BLOB_WRAP_DOMAIN`] for wire-compatibility with the public
/// `wrap_blob_key` entry point.
pub fn wrap_key_v1_in_domain(
    domain: &WrapDomain,
    id: &[u8; 16],
    recipient: &X25519PublicKey,
    plaintext_key: &chacha20poly1305::Key,
) -> Result<([u8; 32], Vec<u8>), StorageError> {
    wrap_to_recipient_v1(domain, id, recipient, plaintext_key)
}

/// Unwrap a key under a wrap domain (v1), given the reader capability.
pub fn unwrap_key_v1_in_domain(
    domain: &WrapDomain,
    id: &[u8; 16],
    recipient_pubkey: &[u8; 32],
    ephemeral_pubkey: &[u8; 32],
    wrapped_key: &[u8],
    reader: &dyn RecipientKey,
) -> Result<chacha20poly1305::Key, StorageError> {
    unwrap_v1(domain, id, recipient_pubkey, ephemeral_pubkey, wrapped_key, reader)
}

fn unwrap_v1(
    domain: &WrapDomain,
    id_bytes: &[u8; 16],
    recipient_pubkey: &[u8; 32],
    ephemeral_pubkey: &[u8; 32],
    wrapped_key: &[u8],
    reader: &dyn RecipientKey,
) -> Result<chacha20poly1305::Key, StorageError> {
    let ephemeral = X25519PublicKey::from(*ephemeral_pubkey);
    let shared = reader.dh(&ephemeral);
    let wrap_key = wrap_key_v1(domain, &shared);
    let nonce = wrap_nonce_v1(domain, id_bytes, recipient_pubkey, ephemeral_pubkey);

    let key_bytes = ChaCha20Poly1305::new(&wrap_key)
        .decrypt(&nonce.into(), wrapped_key)
        .map_err(|_| StorageError::Encryption)?;
    if key_bytes.len() != 32 {
        return Err(StorageError::Encryption);
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(&key_bytes);
    Ok(out.into())
}

/// Wrap-id for mesh-key grants: mesh_key_access rows reuse the v1 wrap code
/// path with a pubkey-derived 16-byte id in place of a blob id.
pub fn mesh_key_wrap_id(mesh_pubkey: &X25519PublicKey) -> [u8; 16] {
    let mut id = [0u8; 16];
    let mut hasher = blake3::Hasher::new_derive_key(MESH_KEY_ID_CONTEXT_V1);
    hasher.update(mesh_pubkey.as_bytes());
    hasher.finalize_xof().fill(&mut id);
    id
}

/// Wrap the mesh X25519 private key to a member's pubkey (v1). Returns
/// (ephemeral_pubkey, wrapped_privkey) for a `mesh_key_access` row.
pub fn wrap_mesh_privkey(
    mesh_pubkey: &X25519PublicKey,
    mesh_privkey: &x25519_dalek::StaticSecret,
    recipient: &X25519PublicKey,
) -> Result<([u8; 32], Vec<u8>), StorageError> {
    let id = mesh_key_wrap_id(mesh_pubkey);
    let key: chacha20poly1305::Key = mesh_privkey.to_bytes().into();
    wrap_to_recipient_v1(&BLOB_WRAP_DOMAIN, &id, recipient, &key)
}

/// Unwrap the mesh private key via a member capability (v1).
pub fn unwrap_mesh_privkey(
    mesh_pubkey: &X25519PublicKey,
    recipient_pubkey: &[u8; 32],
    ephemeral_pubkey: &[u8; 32],
    wrapped_privkey: &[u8],
    reader: &dyn RecipientKey,
) -> Result<x25519_dalek::StaticSecret, StorageError> {
    let id = mesh_key_wrap_id(mesh_pubkey);
    let key = unwrap_v1(&BLOB_WRAP_DOMAIN, &id, recipient_pubkey, ephemeral_pubkey, wrapped_privkey, reader)?;
    let mut bytes = [0u8; 32];
    bytes.copy_from_slice(key.as_slice());
    Ok(x25519_dalek::StaticSecret::from(bytes))
}

/// Subkey for the keyed whole-blob integrity hash — domain-separated from
/// the chunk-key tree so key usage never overlaps.
pub fn integrity_key(per_blob_key: &chacha20poly1305::Key) -> [u8; 32] {
    let mut key_bytes = [0u8; 32];
    let mut hasher = blake3::Hasher::new_derive_key(INTEGRITY_CONTEXT_V1);
    hasher.update(per_blob_key);
    hasher.finalize_xof().fill(&mut key_bytes);
    key_bytes
}

/// Streaming hasher for the keyed integrity hash. Verifiable only by key
/// holders — replicated state carries no unkeyed function of plaintext
/// (the confirmation-oracle fix; see RFC-014).
pub fn integrity_hasher(per_blob_key: &chacha20poly1305::Key) -> blake3::Hasher {
    blake3::Hasher::new_keyed(&integrity_key(per_blob_key))
}

/// Convenience one-shot keyed integrity hash.
pub fn integrity_hash(
    per_blob_key: &chacha20poly1305::Key,
    plaintext: &[u8],
) -> hopnet_common::Blake3Hash {
    let mut hasher = integrity_hasher(per_blob_key);
    hasher.update(plaintext);
    hopnet_common::Blake3Hash::new(hasher.finalize())
}

// --- legacy wrap (dies with the Stage-B call-site migration) ----------------

/// One wrap of a per-file key to a recipient X25519 pubkey (legacy format).
pub struct WrappedKey {
    pub ephemeral_pubkey: X25519PublicKey,
    pub wrapped_key: Vec<u8>, // 48 bytes (32 key + 16 auth tag)
}

/// Wrap a per-file key to a recipient's X25519 public key — LEGACY format
/// (nonce binds user_id; Stage B replaces this with the pubkey-bound v1 wrap).
/// Extracted verbatim from FileAccess::new_for_user_with_conn; the user lookup
/// stays with the caller.
pub fn wrap_file_key_legacy(
    recipient: &X25519PublicKey,
    data_block_id: &CustomUUID,
    user_id: i32,
    per_file_key: &chacha20poly1305::Key,
) -> Result<WrappedKey, StorageError> {
    // Generate ephemeral key pair for this file
    let ephemeral_secret = EphemeralSecret::random_from_rng(OsRng);
    let ephemeral_public = X25519PublicKey::from(&ephemeral_secret);

    // Perform ECDH with the recipient's X25519 public key
    let shared_secret = ephemeral_secret.diffie_hellman(recipient);

    // Derive ChaCha20Poly1305 key from shared secret using Blake3
    let mut wrap_key_bytes = [0u8; 32];
    let mut hasher = blake3::Hasher::new_derive_key("hopnet key_wrap");
    hasher.update(shared_secret.as_bytes());
    let mut xof = hasher.finalize_xof();
    xof.fill(&mut wrap_key_bytes);
    let wrap_key = chacha20poly1305::Key::from(wrap_key_bytes);

    // Derive deterministic nonce from data_block_id + user_id + ephemeral_pubkey
    let mut nonce_bytes = [0u8; 12];
    let mut nonce_hasher = blake3::Hasher::new_derive_key("hopnet wrap_nonce");
    nonce_hasher.update(data_block_id.as_bytes());
    nonce_hasher.update(&user_id.to_le_bytes());
    nonce_hasher.update(ephemeral_public.as_bytes());
    nonce_hasher.finalize_xof().fill(&mut nonce_bytes);
    let wrap_nonce = chacha20poly1305::Nonce::from(nonce_bytes);

    // Encrypt the per-file key
    let wrap_cipher = ChaCha20Poly1305::new(&wrap_key);
    let wrapped_key = wrap_cipher
        .encrypt(&wrap_nonce, per_file_key.as_slice())
        .map_err(|_| StorageError::Encryption)?;

    Ok(WrappedKey {
        ephemeral_pubkey: ephemeral_public,
        wrapped_key,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn hex_of(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    fn fixed_key() -> chacha20poly1305::Key {
        [0x42u8; 32].into()
    }

    fn fixed_fid() -> CustomUUID {
        CustomUUID::from_str("01890a5d-ac96-774b-b9aa-9f8b24f0c9a1").unwrap()
    }

    // FORMAT-SURVIVAL GOLDEN VECTORS — captured from the pre-extraction main
    // crate (src/files/functions.rs) on 2026-07-06, consensus-malachite branch.
    // If any of these fail, the fragment cipher format changed: every
    // already-stored fragment in every mesh becomes undecryptable. Do not
    // update the constants to make tests pass.

    #[test]
    fn golden_chunk_key_derivation() {
        let key = derive_chunk_key(&fixed_key(), &fixed_fid());
        assert_eq!(
            hex_of(key.as_slice()),
            "40538c786e75ba0c5912182cbade11c52a614a09c85d2d4f936de0d912a423fb"
        );
    }

    #[test]
    fn golden_chunk_nonce_derivation() {
        assert_eq!(hex_of(&derive_chunk_nonce(&fixed_fid())), "bc1f77d094b2f9");
    }

    #[test]
    fn golden_ciphertext_short_single_segment() {
        let pt = b"hopnet-storage format survival golden vector v1".to_vec();
        let ct = encrypt_chunk(pt, &fixed_key(), &fixed_fid()).unwrap();
        assert_eq!(
            hex_of(&ct),
            "7cac54cedb64fbf60831f4480f883e36a17880a808aacea2e8257be666df4ddb\
             12555c6d7458a6ee9224ca1aeef73fe96fa8cd1c741792f29c1b119a05d788"
        );
    }

    #[test]
    fn golden_ciphertext_exact_buffer_boundary() {
        let pt: Vec<u8> = (0..4096u32).map(|i| (i % 256) as u8).collect();
        let ct = encrypt_chunk(pt, &fixed_key(), &fixed_fid()).unwrap();
        assert_eq!(ct.len(), 4112);
        assert_eq!(
            blake3::hash(&ct).to_hex().to_string(),
            "c83bea197db8fb9d59adca6cd61b161e3cb9dd09195ac1c259a6f4fb3851a5fe"
        );
    }

    #[test]
    fn golden_ciphertext_multi_segment() {
        let pt: Vec<u8> = (0..5000u32)
            .map(|i| (i.wrapping_mul(31) % 256) as u8)
            .collect();
        let ct = encrypt_chunk(pt, &fixed_key(), &fixed_fid()).unwrap();
        assert_eq!(ct.len(), 5032);
        assert_eq!(
            blake3::hash(&ct).to_hex().to_string(),
            "e251300c5f1bb46e01a93eac165b7d6138590b57c05edfedbbec2d391101d010"
        );
    }

    #[test]
    fn encrypt_decrypt_round_trip() {
        // Should: decrypt(encrypt(x)) == x across segment boundaries.
        // Should not: succeed with a different fragment_id (wrong key + nonce).
        // Impact: a format or derivation regression would corrupt every read.
        for len in [0usize, 1, 100, 4096, 4097, 10000] {
            let pt: Vec<u8> = (0..len as u32).map(|i| (i % 251) as u8).collect();
            let ct = encrypt_chunk(pt.clone(), &fixed_key(), &fixed_fid()).unwrap();
            let rt = decrypt_chunk(&ct, &fixed_key(), &fixed_fid()).unwrap();
            assert_eq!(pt, rt, "round trip failed at len {len}");
            assert_eq!(ct.len(), calculate_encrypted_chunk_length(pt.len()));
        }
        let ct = encrypt_chunk(b"abc".to_vec(), &fixed_key(), &fixed_fid()).unwrap();
        let other = CustomUUID::from_str("01890a5d-ac96-774b-b9aa-9f8b24f0c9a2").unwrap();
        assert!(decrypt_chunk(&ct, &fixed_key(), &other).is_err());
    }

    #[test]
    fn v1_wrap_unwrap_round_trip() {
        // Should: reader capability unwraps its own wrap; wrong reader,
        // transplanted blob_id, or transplanted recipient row all fail.
        // Impact: blob_access is the entire access-control enforcement.
        let reader = StaticRecipient(x25519_dalek::StaticSecret::random_from_rng(OsRng));
        let other = StaticRecipient(x25519_dalek::StaticSecret::random_from_rng(OsRng));
        let per_blob_key = fixed_key();
        let blob_id = fixed_fid();

        let access = wrap_blob_key(&blob_id, &reader.pubkey(), &per_blob_key).unwrap();
        assert_eq!(access.wrapped_key.len(), 48);
        assert_eq!(access.recipient_pubkey, *reader.pubkey().as_bytes());

        let unwrapped = unwrap_blob_key(&access, &reader).unwrap();
        assert_eq!(unwrapped.as_slice(), per_blob_key.as_slice());

        // Wrong reader: DH mismatch → decrypt fails.
        assert!(unwrap_blob_key(&access, &other).is_err());

        // Transplanted blob id: nonce binding breaks.
        let mut moved = access.clone();
        moved.blob_id = CustomUUID::from_str("01890a5d-ac96-774b-b9aa-9f8b24f0c9a2").unwrap();
        assert!(unwrap_blob_key(&moved, &reader).is_err());
    }

    #[test]
    fn v1_mesh_key_wrap_round_trip() {
        // Should: member unwraps the mesh privkey from its grant, then acts
        // as MeshRecipient to unwrap mesh-wrapped blobs.
        let mesh_secret = x25519_dalek::StaticSecret::random_from_rng(OsRng);
        let mesh_pub = X25519PublicKey::from(&mesh_secret);
        let member = StaticRecipient(x25519_dalek::StaticSecret::random_from_rng(OsRng));

        let (eph, wrapped) =
            wrap_mesh_privkey(&mesh_pub, &mesh_secret, &member.pubkey()).unwrap();
        let recovered = unwrap_mesh_privkey(
            &mesh_pub,
            member.pubkey().as_bytes(),
            &eph,
            &wrapped,
            &member,
        )
        .unwrap();
        assert_eq!(recovered.to_bytes(), mesh_secret.to_bytes());

        // The recovered mesh key unwraps an all-users blob wrap.
        let per_blob_key = fixed_key();
        let blob_id = fixed_fid();
        let access = wrap_blob_key(&blob_id, &mesh_pub, &per_blob_key).unwrap();
        let mesh_reader = StaticRecipient(recovered);
        let unwrapped = unwrap_blob_key(&access, &mesh_reader).unwrap();
        assert_eq!(unwrapped.as_slice(), per_blob_key.as_slice());
    }

    #[test]
    fn integrity_hash_keyed_and_separated() {
        // Should: same plaintext under different blob keys → different
        // hashes (no cross-blob linkage); integrity subkey differs from
        // chunk keys (domain separation).
        // Impact: this is the confirmation-oracle fix — an unkeyed or
        // reused-key hash would leak plaintext equality to state holders.
        let pt = b"the same plaintext";
        let k1 = fixed_key();
        let k2: chacha20poly1305::Key = [0x43u8; 32].into();
        assert_ne!(integrity_hash(&k1, pt), integrity_hash(&k2, pt));
        assert_ne!(
            integrity_key(&k1).as_slice(),
            derive_chunk_key(&k1, &fixed_fid()).as_slice()
        );

        // Streaming == one-shot.
        let mut h = integrity_hasher(&k1);
        h.update(&pt[..5]);
        h.update(&pt[5..]);
        assert_eq!(
            hopnet_common::Blake3Hash::new(h.finalize()),
            integrity_hash(&k1, pt)
        );
    }

    #[test]
    fn placement_seed_from_blob_id() {
        // Should: deterministic per blob id; differs across ids.
        let a = crate::placement::placement_seed(&fixed_fid());
        let b = crate::placement::placement_seed(&fixed_fid());
        assert_eq!(a, b);
        let other = CustomUUID::from_str("01890a5d-ac96-774b-b9aa-9f8b24f0c9a2").unwrap();
        assert_ne!(a, crate::placement::placement_seed(&other));
    }

    #[test]
    fn wrap_round_trip_legacy() {
        // Should: recipient can unwrap via ECDH with the ephemeral pubkey.
        // Should not: unwrap with a different recipient key or user_id nonce.
        // Impact: wrap regressions lock every user out of their files.
        let recipient_secret = x25519_dalek::StaticSecret::random_from_rng(OsRng);
        let recipient_public = X25519PublicKey::from(&recipient_secret);
        let per_file_key = fixed_key();
        let id = fixed_fid();

        let wrapped = wrap_file_key_legacy(&recipient_public, &id, 7, &per_file_key).unwrap();
        assert_eq!(wrapped.wrapped_key.len(), 48);

        // Recipient-side unwrap (mirrors auth.rs decrypt_wrapped_file_key)
        let shared = recipient_secret.diffie_hellman(&wrapped.ephemeral_pubkey);
        let mut wrap_key_bytes = [0u8; 32];
        let mut hasher = blake3::Hasher::new_derive_key("hopnet key_wrap");
        hasher.update(shared.as_bytes());
        hasher.finalize_xof().fill(&mut wrap_key_bytes);
        let wrap_key = chacha20poly1305::Key::from(wrap_key_bytes);

        let mut nonce_bytes = [0u8; 12];
        let mut nonce_hasher = blake3::Hasher::new_derive_key("hopnet wrap_nonce");
        nonce_hasher.update(id.as_bytes());
        nonce_hasher.update(&7i32.to_le_bytes());
        nonce_hasher.update(wrapped.ephemeral_pubkey.as_bytes());
        nonce_hasher.finalize_xof().fill(&mut nonce_bytes);

        let cipher = ChaCha20Poly1305::new(&wrap_key);
        let unwrapped = cipher
            .decrypt(&nonce_bytes.into(), wrapped.wrapped_key.as_slice())
            .unwrap();
        assert_eq!(unwrapped.as_slice(), per_file_key.as_slice());

        // Wrong user_id in the nonce must fail
        let mut wrong_nonce = [0u8; 12];
        let mut wrong_hasher = blake3::Hasher::new_derive_key("hopnet wrap_nonce");
        wrong_hasher.update(id.as_bytes());
        wrong_hasher.update(&8i32.to_le_bytes());
        wrong_hasher.update(wrapped.ephemeral_pubkey.as_bytes());
        wrong_hasher.finalize_xof().fill(&mut wrong_nonce);
        assert!(cipher
            .decrypt(&wrong_nonce.into(), wrapped.wrapped_key.as_slice())
            .is_err());
    }

    /// Equivalence pin: generic domain-path functions are interoperable with
    /// the legacy `wrap_blob_key` / `unwrap_blob_key` wrappers (same domain =
    /// same derivation, so generic unwrap of legacy output succeeds and vice
    /// versa). Byte-identical output is not asserted — every wrap draws a
    /// fresh random ephemeral.
    #[test]
    fn generic_path_equivalent_to_legacy_wrappers() {
        let reader = StaticRecipient(x25519_dalek::StaticSecret::random_from_rng(OsRng));
        let per_blob_key = fixed_key();
        let blob_id = fixed_fid();

        let access = wrap_blob_key(&blob_id, &reader.pubkey(), &per_blob_key).unwrap();

        // Generic unwrap of legacy output succeeds.
        let generic_unwrapped = unwrap_key_v1_in_domain(
            &BLOB_WRAP_DOMAIN,
            access.blob_id.as_bytes(),
            &access.recipient_pubkey,
            &access.ephemeral_pubkey,
            &access.wrapped_key,
            &reader,
        )
        .unwrap();
        assert_eq!(generic_unwrapped.as_slice(), per_blob_key.as_slice());

        // Generic wrap → legacy unwrap also succeeds (same domain).
        let (eph, wrapped) = wrap_key_v1_in_domain(
            &BLOB_WRAP_DOMAIN,
            blob_id.as_bytes(),
            &reader.pubkey(),
            &per_blob_key,
        )
        .unwrap();
        let synthetic = crate::types::BlobAccess {
            blob_id: blob_id.clone(),
            recipient_pubkey: *reader.pubkey().as_bytes(),
            ephemeral_pubkey: eph,
            wrapped_key: wrapped,
        };
        let legacy_unwrapped = unwrap_blob_key(&synthetic, &reader).unwrap();
        assert_eq!(legacy_unwrapped.as_slice(), per_blob_key.as_slice());
    }

    /// Cross-domain: a key wrapped under BLOB_WRAP_DOMAIN must not unwrap
    /// via a different domain.
    #[test]
    fn cross_domain_unwrap_fails() {
        let reader = StaticRecipient(x25519_dalek::StaticSecret::random_from_rng(OsRng));
        let per_blob_key = fixed_key();
        let blob_id = fixed_fid();
        let access = wrap_blob_key(&blob_id, &reader.pubkey(), &per_blob_key).unwrap();

        let wrong_domain = WrapDomain {
            key_context: "hopnet-photos metadata_key v1",
            nonce_context: "hopnet-photos metadata_nonce v1",
        };
        assert!(unwrap_key_v1_in_domain(
            &wrong_domain,
            access.blob_id.as_bytes(),
            &access.recipient_pubkey,
            &access.ephemeral_pubkey,
            &access.wrapped_key,
            &reader,
        )
        .is_err());
    }
}
