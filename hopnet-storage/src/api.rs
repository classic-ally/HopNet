//! Substrate blob API surfaces (RFC-014).
//!
//! `put` is the encrypt-then-RS ingest pipeline, moved verbatim from the fs
//! projection's `process_uploaded_file`/`process_logical_chunk`: chunk the
//! plaintext stream into 40MB logical chunks, per-fragment ChaCha20-Poly1305
//! (format-frozen, see crypto.rs), Reed-Solomon 10+20 over the ciphertext,
//! content-addressed fragment files on disk, keyed whole-blob integrity hash.
//!
//! The host maps [`PutOutcome`] into its own record shapes and rides the
//! result through its consensus transaction; distribution kicks post-decide.

use crate::crypto;
use crate::error::StorageError;
use crate::fragstore;
use crate::rs::{
    CHUNK_SIZE, ORIGINAL_FRAGMENTS_PER_CHUNK, RECOVERY_FRAGMENTS_PER_CHUNK,
    calculate_chunk_padding, calculate_chunked_fragments, calculate_padding_and_chunks,
};
use crate::types::BlobId;
use hopnet_common::{Blake3Hash, CustomUUID};
use rand::Rng;
use reed_solomon_simd::ReedSolomonEncoder;
use tokio::io::{AsyncRead, AsyncReadExt};

/// One produced fragment (original or recovery).
#[derive(Debug, Clone)]
pub struct PutFragment {
    pub chunk_number: u32,
    pub local_index: u32,
    pub fragment_id: CustomUUID,
    pub fragment_hash: Blake3Hash,
    pub recovery: bool,
}

/// Result of ingesting one blob: everything the host needs for its
/// blob-insert transaction. Fragments are on local disk when this returns.
#[derive(Debug)]
pub struct PutOutcome {
    pub integrity_hash: Blake3Hash,
    pub fragments: Vec<PutFragment>,
    /// Padding added to the LAST chunk (stripped after reconstruction).
    pub added_bytes: u8,
}

/// Ingest a plaintext stream: encrypt per fragment, RS-encode, store
/// fragments locally, compute the keyed integrity hash.
///
/// Rejects empty input — empty content is a projection concern
/// (`data_id = NULL`), never a blob.
pub async fn put<R: AsyncRead + Unpin>(
    mut source: R,
    file_size: usize,
    blob_id: BlobId,
    per_blob_key: &chacha20poly1305::Key,
    fragments_dir: &str,
) -> Result<PutOutcome, StorageError> {
    if file_size == 0 {
        return Err(StorageError::Rs);
    }

    const READ_BUF_SIZE: usize = 64 * 1024;

    let mut fragments = Vec::new();
    // Keyed whole-blob integrity hash (RFC-014): verifiable only by key
    // holders — replicated state carries no unkeyed function of plaintext.
    let mut full_hasher = crypto::integrity_hasher(per_blob_key);

    let (num_chunks, total_original, total_recovery) = calculate_chunked_fragments(file_size);
    tracing::debug!(
        "put: file_size={}, num_chunks={}, total_original={}, total_recovery={}",
        file_size,
        num_chunks,
        total_original,
        total_recovery
    );

    let max_fragment_size = (CHUNK_SIZE / ORIGINAL_FRAGMENTS_PER_CHUNK) + 28;
    let mut encoder = ReedSolomonEncoder::new(
        ORIGINAL_FRAGMENTS_PER_CHUNK,
        RECOVERY_FRAGMENTS_PER_CHUNK,
        max_fragment_size,
    )
    .map_err(|e| {
        tracing::error!("Reed-Solomon encoder creation failed: {:?}", e);
        StorageError::Rs
    })?;

    let mut logical_chunk_buffer: Vec<u8> = Vec::new();
    let mut current_chunk_number = 0u32;
    let mut last_chunk_padding = 0usize;
    let mut read_buf = vec![0u8; READ_BUF_SIZE];

    loop {
        let n = source
            .read(&mut read_buf)
            .await
            .map_err(StorageError::Read)?;
        if n == 0 {
            break;
        }
        let bytes = &read_buf[..n];
        logical_chunk_buffer.extend_from_slice(bytes);
        full_hasher.update(bytes);

        while logical_chunk_buffer.len() >= CHUNK_SIZE {
            let chunk_data: Vec<u8> = logical_chunk_buffer.drain(..CHUNK_SIZE).collect();
            last_chunk_padding = process_logical_chunk(
                &mut encoder,
                &chunk_data,
                current_chunk_number,
                per_blob_key,
                fragments_dir,
                &mut fragments,
            )?;
            current_chunk_number += 1;
        }
    }

    // Process final partial chunk (if any remaining data < 40MB)
    if !logical_chunk_buffer.is_empty() {
        last_chunk_padding = process_logical_chunk(
            &mut encoder,
            &logical_chunk_buffer,
            current_chunk_number,
            per_blob_key,
            fragments_dir,
            &mut fragments,
        )?;
    }

    let integrity_hash = Blake3Hash::new(full_hasher.finalize());
    tracing::debug!(
        "put: blob {} complete — {} chunks, {} fragments, {} bytes last-chunk padding",
        blob_id,
        num_chunks,
        fragments.len(),
        last_chunk_padding
    );

    Ok(PutOutcome {
        integrity_hash,
        fragments,
        added_bytes: last_chunk_padding as u8,
    })
}

/// Process a single logical chunk with Reed-Solomon encoding (10 original +
/// 20 recovery). Returns the padding bytes added to this chunk.
fn process_logical_chunk(
    encoder: &mut ReedSolomonEncoder,
    chunk_data: &[u8],
    chunk_number: u32,
    per_blob_key: &chacha20poly1305::Key,
    fragments_dir: &str,
    fragments: &mut Vec<PutFragment>,
) -> Result<usize, StorageError> {
    let chunk_size = chunk_data.len();

    // Calculate padding needed to evenly divide into 10 fragments
    let padding = calculate_chunk_padding(chunk_size, ORIGINAL_FRAGMENTS_PER_CHUNK);
    let padded_size = chunk_size + padding;

    // Pad (random bytes — padding must not leak structure) and split into
    // 10 equal fragments
    let mut padded_chunk = chunk_data.to_vec();
    if padding > 0 {
        padded_chunk.resize(padded_size, 0);
        rand::rng().fill_bytes(&mut padded_chunk[chunk_size..]);
    }

    let (fragment_chunks, _) =
        calculate_padding_and_chunks(padded_chunk, ORIGINAL_FRAGMENTS_PER_CHUNK);

    // Encrypt each fragment (per-fragment key/nonce derive from the blob key
    // + fresh fragment id — the format-frozen cipher)
    let mut encrypted_fragments = Vec::new();
    for fragment_data in fragment_chunks.into_iter() {
        let fragment_id = CustomUUID::new(None);
        let encrypted_fragment = crypto::encrypt_chunk(fragment_data, per_blob_key, &fragment_id)?;
        encrypted_fragments.push((fragment_id, encrypted_fragment));
    }

    // All encrypted fragments have the same size (RS requirement)
    let encrypted_fragment_size = encrypted_fragments[0].1.len();
    encoder
        .reset(
            ORIGINAL_FRAGMENTS_PER_CHUNK,
            RECOVERY_FRAGMENTS_PER_CHUNK,
            encrypted_fragment_size,
        )
        .map_err(|e| {
            tracing::error!(
                "Reed-Solomon encoder reset failed for chunk {}: {:?}",
                chunk_number,
                e
            );
            StorageError::Rs
        })?;

    // Add encrypted fragments to the encoder and store them
    for (local_index, (fragment_id, encrypted_fragment)) in
        encrypted_fragments.into_iter().enumerate()
    {
        let fragment_hash = Blake3Hash::new(blake3::hash(&encrypted_fragment));
        encoder
            .add_original_shard(&encrypted_fragment)
            .map_err(|_| StorageError::Rs)?;

        fragstore::store_fragment(fragments_dir, &fragment_hash, encrypted_fragment)?;

        fragments.push(PutFragment {
            chunk_number,
            local_index: local_index as u32,
            fragment_id,
            fragment_hash,
            recovery: false,
        });
    }

    // Generate recovery fragments
    let recovery_generator = encoder.encode().map_err(|_| StorageError::Rs)?;
    let recovery_iter = recovery_generator.recovery_iter();

    let mut recovery_index = ORIGINAL_FRAGMENTS_PER_CHUNK;
    for recovery_fragment in recovery_iter {
        let fragment_id = CustomUUID::new(None);
        let fragment_hash = Blake3Hash::new(blake3::hash(recovery_fragment));

        fragstore::store_fragment(fragments_dir, &fragment_hash, recovery_fragment.to_vec())?;

        fragments.push(PutFragment {
            chunk_number,
            local_index: recovery_index as u32,
            fragment_id,
            fragment_hash,
            recovery: true,
        });
        recovery_index += 1;
    }

    Ok(padding)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[tokio::test]
    async fn put_round_trips_through_decrypt() {
        // Should: put stores 30 verified fragments for a small blob; originals
        // decrypt+concatenate back to the padded plaintext; integrity hash is
        // the keyed hash of the plaintext; empty input rejected.
        // Impact: this IS the write path for every projection.
        let dir = std::env::temp_dir().join(format!("hopnet-put-test-{}", std::process::id()));
        let dir = dir.to_str().unwrap().to_string();
        let key: chacha20poly1305::Key = [0x42u8; 32].into();
        let blob_id = CustomUUID::from_str("01890a5d-ac96-774b-b9aa-9f8b24f0c9a1").unwrap();

        let plaintext: Vec<u8> = (0..100_000u32).map(|i| (i % 251) as u8).collect();
        let outcome = put(
            plaintext.as_slice(),
            plaintext.len(),
            blob_id.clone(),
            &key,
            &dir,
        )
        .await
        .unwrap();

        assert_eq!(outcome.fragments.len(), 30);
        assert_eq!(
            outcome.integrity_hash,
            crypto::integrity_hash(&key, &plaintext)
        );

        // Reassemble from originals
        let mut reassembled = Vec::new();
        let mut originals: Vec<_> = outcome
            .fragments
            .iter()
            .filter(|f| !f.recovery)
            .collect();
        originals.sort_by_key(|f| f.local_index);
        for f in originals {
            let ct = fragstore::fetch_and_verify_fragment(&f.fragment_hash, &dir).unwrap();
            let pt = crypto::decrypt_chunk(&ct, &key, &f.fragment_id).unwrap();
            reassembled.extend_from_slice(&pt);
        }
        reassembled.truncate(reassembled.len() - outcome.added_bytes as usize);
        assert_eq!(reassembled, plaintext);

        // Empty input rejected
        assert!(put(&b""[..], 0, blob_id, &key, &dir).await.is_err());

        let _ = std::fs::remove_dir_all(&dir);
    }
}
