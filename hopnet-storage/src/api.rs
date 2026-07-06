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

// ---------------------------------------------------------------------------
// get: discovery + reconstruction + decrypt (moved from the fs projection)

use crate::store::BlobManifest;
use crate::traits::{LocalStateSink, PeerRef, StateReader, Transport, TransportError};
use std::collections::HashMap;
use std::sync::Arc;

/// The seams the get path needs for network discovery. Fields are Arcs so
/// the bundle clones cheaply into discovery workers.
pub struct GetNet<T, S, L> {
    pub transport: Arc<T>,
    pub state: Arc<S>,
    pub local_state: Arc<L>,
}

impl<T, S, L> Clone for GetNet<T, S, L> {
    fn clone(&self) -> Self {
        GetNet {
            transport: self.transport.clone(),
            state: self.state.clone(),
            local_state: self.local_state.clone(),
        }
    }
}

/// Reconstruct a blob as a byte stream: per-chunk network discovery
/// (rate-matched to the consumer), fast-path concat or Reed-Solomon
/// reconstruction, per-fragment decrypt, padding strip, and — for full
/// (non-range) reads with a key — keyed whole-blob integrity verification.
///
/// `net = None` disables discovery entirely (local fragments only; tests
/// and offline reconstruction). `range` is an inclusive byte range; range
/// reads slice boundary chunks and skip whole-blob verification (fragment
/// hashes still verify per hop).
pub fn get<T, S, L>(
    net: Option<GetNet<T, S, L>>,
    fragments_dir: String,
    mut manifest: BlobManifest,
    per_blob_key: Option<chacha20poly1305::Key>,
    range: Option<(u64, u64)>,
) -> impl tokio_stream::Stream<Item = Result<bytes::Bytes, StorageError>>
where
    T: Transport + 'static,
    S: StateReader + 'static,
    L: LocalStateSink + 'static,
{
    async_stream::try_stream! {
        // Empty manifests yield nothing (projections handle empty content
        // before ever minting a blob).
        if manifest.chunks.is_empty() {
            return;
        }

        let num_chunks = manifest.chunks.len() as u32;
        let chunk_size = CHUNK_SIZE as u64;

        // Determine which chunks to iterate
        let (start_chunk, end_chunk, range_start, range_end) = match range {
            Some((start, end)) => {
                let sc = (start / chunk_size) as u32;
                let ec = (end / chunk_size) as u32;
                (sc, ec.min(num_chunks - 1), start, end)
            }
            None => (0, num_chunks - 1, 0u64, u64::MAX),
        };

        let is_range = range.is_some();
        // Keyed integrity hasher (RFC-014) — requires the per-blob key, so
        // verification runs exactly when decryption does. Range reads skip
        // whole-blob verification (unchanged); fragment hashes verify per hop.
        let mut hasher = if is_range {
            None
        } else {
            per_blob_key.as_ref().map(crypto::integrity_hasher)
        };

        // Process chunks in order (streaming with per-chunk discovery for
        // rate-matching to the consumer's download speed).
        for chunk_number in start_chunk..=end_chunk {
            if let Some(ref net) = net {
                discover_chunk(net, &fragments_dir, &mut manifest, chunk_number).await?;
            }

            let (originals, recovery) = manifest.chunks.get(&chunk_number)
                .ok_or(StorageError::Rs)?;

            // Count local fragments for this chunk (after discovery)
            let local_count = originals.values().filter(|(_, _, local)| *local).count() +
                             recovery.values().filter(|(_, _, local)| *local).count();

            tracing::debug!("Processing chunk {}/{}: {} fragments available",
                           chunk_number + 1, num_chunks, local_count);

            // Verify we have enough fragments (discovery should have ensured this)
            if local_count < ORIGINAL_FRAGMENTS_PER_CHUNK {
                tracing::error!("Chunk {}: insufficient fragments after discovery ({}/{})",
                              chunk_number, local_count, ORIGINAL_FRAGMENTS_PER_CHUNK);
                Err(StorageError::Rs)?;
            }

            // Reconstruct this chunk
            let mut chunk_bytes = reconstruct_single_chunk(
                originals,
                recovery,
                &fragments_dir,
                &per_blob_key,
            )?;

            // Remove padding from last chunk before hashing/slicing
            if chunk_number == num_chunks - 1 && manifest.added_bytes > 0 {
                let final_length = chunk_bytes.len().saturating_sub(manifest.added_bytes as usize);
                chunk_bytes.truncate(final_length);
            }

            if is_range {
                // Slice boundary chunks for range requests
                let chunk_start_byte = chunk_number as u64 * chunk_size;
                let slice_start = if chunk_number == start_chunk {
                    (range_start - chunk_start_byte) as usize
                } else {
                    0
                };
                let slice_end = if chunk_number == end_chunk {
                    ((range_end - chunk_start_byte) as usize + 1).min(chunk_bytes.len())
                } else {
                    chunk_bytes.len()
                };

                if slice_start < chunk_bytes.len() && slice_start < slice_end {
                    yield bytes::Bytes::from(chunk_bytes[slice_start..slice_end].to_vec());
                }
            } else {
                // Full download: update incremental hash and yield
                if let Some(ref mut h) = hasher {
                    h.update(&chunk_bytes);
                }
                yield bytes::Bytes::from(chunk_bytes);
            }
        }

        // Verify final keyed hash after all chunks processed (full downloads
        // with a key; keyless reads have nothing to verify)
        if let Some(hasher) = hasher {
            let computed_hash = Blake3Hash::from(hasher.finalize());
            if computed_hash != manifest.integrity_hash {
                tracing::error!(
                    "Hash mismatch for blob {}: expected {}, got {}",
                    manifest.blob_id,
                    manifest.integrity_hash.to_hex(),
                    computed_hash.to_hex()
                );
                Err(StorageError::HashMismatch)?;
            }

            tracing::info!("Blob reconstruction complete and verified: {}", manifest.blob_id);
        }
    }
}

/// Discovery-disabled seam bundle for [`get_local`] — never invoked (get
/// skips discovery when `net` is None); exists only to pin the generics.
struct NullNet;

impl Transport for NullNet {
    async fn store_fragment(
        &self,
        _peer: &PeerRef,
        _fragment_hash: &Blake3Hash,
        _data: Vec<u8>,
    ) -> Result<crate::traits::StoreResult, TransportError> {
        Err(TransportError::Transport("null transport".into()))
    }
    async fn fetch_fragment(
        &self,
        _peer: &PeerRef,
        _fragment_hash: &Blake3Hash,
    ) -> Result<Vec<u8>, TransportError> {
        Err(TransportError::Transport("null transport".into()))
    }
    async fn fragment_health(
        &self,
        _peer: &PeerRef,
        _fragment_hash: &Blake3Hash,
    ) -> Result<bool, TransportError> {
        Ok(false)
    }
}

impl StateReader for NullNet {
    fn placement_inputs(&self) -> Result<crate::traits::PlacementInputs, StorageError> {
        Err(StorageError::Host("null state reader".into()))
    }
    fn placement_inputs_at(&self, _height: i32) -> Result<crate::traits::PlacementInputs, StorageError> {
        Err(StorageError::Host("null state reader".into()))
    }
    fn fragment_sources(
        &self,
        _fragment_hashes: &[Blake3Hash],
    ) -> Result<HashMap<Blake3Hash, Vec<PeerRef>>, StorageError> {
        Ok(HashMap::new())
    }
    fn all_peers(&self) -> Result<Vec<PeerRef>, StorageError> {
        Ok(Vec::new())
    }
    fn distributable_blob(
        &self,
        _blob_id: &BlobId,
    ) -> Result<Option<crate::store::DistributableBlob>, StorageError> {
        Ok(None)
    }
    fn local_node_id(&self) -> Option<i32> {
        None
    }
}

impl LocalStateSink for NullNet {
    fn mark_local(&self, _fragment_hash: Blake3Hash) {}
    fn mark_remote_batch(&self, _fragment_hashes: Vec<Blake3Hash>) {}
}

/// Local-only reconstruction: no network discovery, fragments must already
/// be on disk (tests, offline verification).
pub fn get_local(
    fragments_dir: String,
    manifest: BlobManifest,
    per_blob_key: Option<chacha20poly1305::Key>,
    range: Option<(u64, u64)>,
) -> impl tokio_stream::Stream<Item = Result<bytes::Bytes, StorageError>> {
    get(
        None::<GetNet<NullNet, NullNet, NullNet>>,
        fragments_dir,
        manifest,
        per_blob_key,
        range,
    )
}

/// Fetch missing fragments for one chunk until reconstruction is possible.
/// The fallback ladder per fragment: (0) inventory sources (top verified
/// holders), then (1) placement-directed candidates at the blob's placement
/// height, or the gossip peer set when unplaced. Workers race with early
/// exit once enough fragments landed.
async fn discover_chunk<T, S, L>(
    net: &GetNet<T, S, L>,
    fragments_dir: &str,
    manifest: &mut BlobManifest,
    chunk_number: u32,
) -> Result<(), StorageError>
where
    T: Transport + 'static,
    S: StateReader,
    L: LocalStateSink,
{
    let (originals, recovery) = manifest
        .chunks
        .get(&chunk_number)
        .ok_or(StorageError::Rs)?;

    // Count how many fragments we already have locally for this chunk
    let local_count = originals
        .values()
        .filter(|(_, _, exists_locally)| *exists_locally)
        .count()
        + recovery
            .values()
            .filter(|(_, _, exists_locally)| *exists_locally)
            .count();

    let fragments_needed = ORIGINAL_FRAGMENTS_PER_CHUNK.saturating_sub(local_count);

    // Early exit if we already have enough fragments locally
    if fragments_needed == 0 {
        tracing::debug!(
            "Chunk {}: already have {} fragments locally, no discovery needed",
            chunk_number,
            local_count
        );
        return Ok(());
    }

    tracing::debug!(
        "Chunk {}: have {} fragments locally, need {} more",
        chunk_number,
        local_count,
        fragments_needed
    );

    // Candidate set: the validator snapshot the placement was computed
    // against, or every known peer when the blob is unplaced.
    let candidates: Vec<PeerRef> = match manifest.placement_height {
        Some(height) => net.state.placement_inputs_at(height)?.validators,
        None => {
            tracing::warn!("No placement height available - using gossip-only fragment discovery");
            net.state.all_peers()?
        }
    };

    // Build list of missing fragments for the target chunk only
    let mut missing_fragments: Vec<(usize, Blake3Hash, bool)> = Vec::new();
    for (index, (hash, _, exists_locally)) in originals {
        if !exists_locally {
            missing_fragments.push((*index, *hash, false));
        }
    }
    for (index, (hash, _, exists_locally)) in recovery {
        if !exists_locally {
            missing_fragments.push((*index, *hash, true));
        }
    }

    // Batch query inventory sources for all missing fragments
    let missing_hashes: Vec<Blake3Hash> =
        missing_fragments.iter().map(|(_, hash, _)| *hash).collect();
    let mut source_map = net.state.fragment_sources(&missing_hashes)?;

    // Pre-distribute inventory hints into the work items
    let missing_fragments: Vec<(usize, Blake3Hash, bool, Option<Vec<PeerRef>>)> =
        missing_fragments
            .into_iter()
            .map(|(index, hash, recovery)| {
                let hint = source_map.remove(&hash);
                (index, hash, recovery, hint)
            })
            .collect();

    // Worker count: at least 2 for redundancy (even if only need 1), capped
    // at the missing count.
    let num_workers = if fragments_needed == 1 {
        2.min(missing_fragments.len())
    } else {
        fragments_needed.min(missing_fragments.len())
    };

    tracing::debug!(
        "Chunk {}: spawning {} workers to fetch {} fragments from {} candidates",
        chunk_number,
        num_workers,
        fragments_needed,
        missing_fragments.len()
    );

    let work_queue = Arc::new(tokio::sync::Mutex::new(missing_fragments));
    let (success_tx, mut success_rx) = tokio::sync::mpsc::unbounded_channel();
    let successful_downloads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let candidates = Arc::new(candidates);

    for worker_id in 0..num_workers {
        let tx = success_tx.clone();
        let queue = work_queue.clone();
        let transport = net.transport.clone();
        let candidates = candidates.clone();
        let fragments_dir = fragments_dir.to_string();
        let successes = successful_downloads.clone();

        tokio::spawn(async move {
            tracing::debug!("Worker {} starting fragment discovery", worker_id);
            loop {
                // Stop once enough successful downloads landed
                if successes.load(std::sync::atomic::Ordering::Relaxed) >= fragments_needed {
                    tracing::debug!("Worker {} stopping - enough fragments downloaded", worker_id);
                    break;
                }

                let next_work = {
                    let mut queue_lock = queue.lock().await;
                    queue_lock.pop()
                };
                let Some((index, fragment_hash, recovery, hint)) = next_work else {
                    tracing::debug!("Worker {} stopping - no more fragments to try", worker_id);
                    break;
                };

                match find_fragment_via(&transport, &fragment_hash, &candidates, hint).await {
                    Some(data) => {
                        if let Err(e) = fragstore::store_fragment(&fragments_dir, &fragment_hash, data) {
                            tracing::error!(
                                "Worker {} failed to store fragment {}: {}",
                                worker_id,
                                fragment_hash.to_hex(),
                                e
                            );
                            let _ = tx.send(Err((index, recovery)));
                            continue;
                        }
                        tracing::info!(
                            "Worker {} successfully cached fragment {} from network",
                            worker_id,
                            fragment_hash.to_hex()
                        );
                        successes.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let _ = tx.send(Ok((index, recovery, fragment_hash)));
                    }
                    None => {
                        tracing::warn!(
                            "Worker {} failed to discover fragment {}",
                            worker_id,
                            fragment_hash.to_hex()
                        );
                        let _ = tx.send(Err((index, recovery)));
                    }
                }
            }
        });
    }
    drop(success_tx);

    // Collect results and update the manifest's local flags
    let mut completed_downloads = 0;
    while let Some(result) = success_rx.recv().await {
        match result {
            Ok((index, recovery, fragment_hash)) => {
                // Queue the stored_locally settlement through the sink
                net.local_state.mark_local(fragment_hash);

                if let Some((originals, recovery_map)) = manifest.chunks.get_mut(&chunk_number) {
                    let bucket = if recovery { recovery_map } else { originals };
                    if let Some((_, _, exists_locally)) = bucket.get_mut(&index) {
                        *exists_locally = true;
                    }
                }

                completed_downloads += 1;
                tracing::debug!(
                    "Chunk {}: fragment discovery progress: {}/{} needed",
                    chunk_number,
                    completed_downloads,
                    fragments_needed
                );

                // Exit early once we have enough fragments
                if completed_downloads >= fragments_needed {
                    tracing::debug!(
                        "Chunk {}: collected {} fragments (needed {}), stopping collection early",
                        chunk_number,
                        completed_downloads,
                        fragments_needed
                    );
                    break;
                }
            }
            Err((index, recovery)) => {
                tracing::debug!(
                    "Failed to download fragment at index {} (recovery: {})",
                    index,
                    recovery
                );
            }
        }
    }

    // Workers finish in background — they watch the shared counter.

    if completed_downloads < fragments_needed {
        tracing::error!(
            "Chunk {}: discovery failed - collected {}/{} needed",
            chunk_number,
            completed_downloads,
            fragments_needed
        );
        return Err(StorageError::Rs);
    }

    tracing::debug!(
        "Chunk {}: discovery complete - fetched {} fragments",
        chunk_number,
        completed_downloads
    );
    Ok(())
}

/// Find one fragment: inventory sources first (primary rung), then reactive
/// discovery across the candidate set. Returns the verified fragment bytes.
async fn find_fragment_via<T: Transport + 'static>(
    transport: &Arc<T>,
    fragment_hash: &Blake3Hash,
    candidates: &[PeerRef],
    inventory_hint: Option<Vec<PeerRef>>,
) -> Option<Vec<u8>> {
    // Phase 0: inventory sources (top verified holders)
    if let Some(sources) = inventory_hint {
        if !sources.is_empty() {
            tracing::debug!(
                "Trying {} inventory nodes for fragment {}",
                sources.len(),
                fragment_hash.to_hex()
            );
            if let Some(data) = reactive_discover(transport, fragment_hash, &sources).await {
                tracing::debug!("Fragment {} found via inventory!", fragment_hash.to_hex());
                return Some(data);
            }
            tracing::debug!("Inventory nodes failed, falling back to network-wide search");
        }
    }

    // Phase 1: reactive discovery across all candidates
    if !candidates.is_empty() {
        tracing::debug!(
            "Trying reactive discovery across {} nodes",
            candidates.len()
        );
        if let Some(data) = reactive_discover(transport, fragment_hash, candidates).await {
            return Some(data);
        }
    }

    None
}

/// Reactive discovery and fetch: fan out health checks, start a download the
/// moment a peer reports having the fragment, return the first verified win.
async fn reactive_discover<T: Transport + 'static>(
    transport: &Arc<T>,
    fragment_hash: &Blake3Hash,
    peers: &[PeerRef],
) -> Option<Vec<u8>> {
    let (health_tx, mut health_rx) = tokio::sync::mpsc::unbounded_channel();
    let (download_tx, mut download_rx) = tokio::sync::mpsc::unbounded_channel();

    // Spawn health check tasks for all peers
    for peer in peers {
        let tx = health_tx.clone();
        let fragment_hash = *fragment_hash;
        let peer = *peer;
        let transport = transport.clone();

        tokio::spawn(async move {
            let has_fragment = transport
                .fragment_health(&peer, &fragment_hash)
                .await
                .unwrap_or(false);
            tracing::debug!(
                "Health check for fragment {} on node {}: {}",
                fragment_hash.to_hex(),
                peer.node_id,
                if has_fragment { "HAS" } else { "MISSING" }
            );
            if has_fragment {
                let _ = tx.send(peer);
            }
        });
    }
    drop(health_tx);

    // Process results as they flow in. Track in-flight downloads to detect
    // when all work is exhausted — the original download_tx keeps the
    // channel alive, so we can't rely on `else` in select!.
    let mut health_done = false;
    let mut in_flight = 0usize;

    loop {
        if health_done && in_flight == 0 {
            break;
        }

        tokio::select! {
            // New peer reports having the fragment — start download immediately
            result = health_rx.recv(), if !health_done => {
                match result {
                    Some(peer) => {
                        tracing::debug!("Node {} reports having fragment, starting download", peer.node_id);
                        let tx = download_tx.clone();
                        let fragment_hash = *fragment_hash;
                        let transport = transport.clone();
                        in_flight += 1;

                        tokio::spawn(async move {
                            let result = transport.fetch_fragment(&peer, &fragment_hash).await
                                .and_then(|data| {
                                    // Defense in depth: verify hash even though
                                    // the server already verified
                                    let actual = Blake3Hash::new(blake3::hash(&data));
                                    if actual == fragment_hash {
                                        Ok(data)
                                    } else {
                                        Err(TransportError::Peer("fragment hash mismatch".into()))
                                    }
                                });
                            let _ = tx.send(result);
                        });
                    }
                    None => {
                        health_done = true;
                    }
                }
            }

            // Download completed
            Some(download_result) = download_rx.recv(), if in_flight > 0 => {
                in_flight -= 1;
                match download_result {
                    Ok(data) => {
                        tracing::debug!("Successfully downloaded fragment {}", fragment_hash.to_hex());
                        return Some(data);
                    }
                    Err(e) => {
                        tracing::debug!("Download failed: {}, continuing with other candidates", e);
                    }
                }
            }
        }
    }

    None
}

/// Reconstruct a single logical chunk from its fragments: fast path
/// (concatenate originals) when all 10 originals are local, otherwise
/// Reed-Solomon over any available 10+ fragments. Decrypts per fragment
/// when a key is supplied.
fn reconstruct_single_chunk(
    originals: &HashMap<usize, crate::store::FragmentEntry>,
    recovery: &HashMap<usize, crate::store::FragmentEntry>,
    fragments_dir: &str,
    per_blob_key: &Option<chacha20poly1305::Key>,
) -> Result<Vec<u8>, StorageError> {
    // Check if all original fragments are available locally (fast path)
    let all_originals_local = originals
        .values()
        .all(|(_, _, exists_locally)| *exists_locally);

    if all_originals_local && originals.len() == ORIGINAL_FRAGMENTS_PER_CHUNK {
        tracing::debug!(
            "Using fast path: all {} originals available locally",
            ORIGINAL_FRAGMENTS_PER_CHUNK
        );

        let mut chunk_data = Vec::new();
        for i in 0..ORIGINAL_FRAGMENTS_PER_CHUNK {
            let Some((hash, fragment_id, _)) = originals.get(&i) else {
                return Err(StorageError::Rs);
            };
            let fragment_data = fragstore::fetch_and_verify_fragment(hash, fragments_dir)?;
            let decrypted = if let Some(key) = per_blob_key {
                crypto::decrypt_chunk(&fragment_data, key, fragment_id)?
            } else {
                fragment_data
            };
            chunk_data.extend_from_slice(&decrypted);
        }
        return Ok(chunk_data);
    }

    // Slow path: Reed-Solomon reconstruction
    tracing::debug!("Using slow path: RS reconstruction");

    let mut available_original = Vec::new();
    let mut available_recovery = Vec::new();

    for (index, (hash, _, exists_locally)) in originals.iter() {
        if *exists_locally {
            let fragment_data = fragstore::fetch_and_verify_fragment(hash, fragments_dir)?;
            available_original.push((*index, fragment_data));
        }
    }
    for (index, (hash, _, exists_locally)) in recovery.iter() {
        if *exists_locally {
            let fragment_data = fragstore::fetch_and_verify_fragment(hash, fragments_dir)?;
            available_recovery.push((*index, fragment_data));
        }
    }

    let total_available = available_original.len() + available_recovery.len();
    tracing::debug!(
        "RS reconstruction: collected {} originals + {} recovery = {} total (need {})",
        available_original.len(),
        available_recovery.len(),
        total_available,
        ORIGINAL_FRAGMENTS_PER_CHUNK
    );

    if total_available < ORIGINAL_FRAGMENTS_PER_CHUNK {
        tracing::error!(
            "Insufficient fragments for reconstruction: have {}, need {}",
            total_available,
            ORIGINAL_FRAGMENTS_PER_CHUNK
        );
        return Err(StorageError::Rs);
    }

    // All fragments in a chunk are the same size
    let fragment_size = available_original
        .first()
        .or(available_recovery.first())
        .map(|(_, data)| data.len())
        .ok_or(StorageError::Rs)?;

    let mut decoder = reed_solomon_simd::ReedSolomonDecoder::new(
        ORIGINAL_FRAGMENTS_PER_CHUNK,
        RECOVERY_FRAGMENTS_PER_CHUNK,
        fragment_size,
    )
    .map_err(|e| {
        tracing::error!("RS reconstruction: failed to create decoder: {:?}", e);
        StorageError::Rs
    })?;

    for (index, chunk_data) in &available_original {
        decoder.add_original_shard(*index, chunk_data).map_err(|e| {
            tracing::error!("RS reconstruction: failed to add original shard {}: {:?}", index, e);
            StorageError::Rs
        })?;
    }

    // Recovery fragments are stored with local_index 10-29; the RS decoder
    // expects recovery indices 0-19.
    for (index, chunk_data) in &available_recovery {
        let rs_recovery_index = index - ORIGINAL_FRAGMENTS_PER_CHUNK;
        decoder
            .add_recovery_shard(rs_recovery_index, chunk_data)
            .map_err(|e| {
                tracing::error!(
                    "RS reconstruction: failed to add recovery shard {} (RS index {}): {:?}",
                    index,
                    rs_recovery_index,
                    e
                );
                StorageError::Rs
            })?;
    }

    let decoder_result = decoder.decode().map_err(|e| {
        tracing::error!("RS decode failed: {:?}", e);
        StorageError::Rs
    })?;

    // Index reconstructed fragments
    let mut reconstructed_indices: HashMap<usize, Vec<u8>> = HashMap::new();
    for (index, chunk_data) in decoder_result.restored_original_iter() {
        reconstructed_indices.insert(index, chunk_data.to_vec());
    }

    // Decrypt and concatenate fragments in order
    let mut chunk_data = Vec::new();
    for i in 0..ORIGINAL_FRAGMENTS_PER_CHUNK {
        let encrypted_fragment = if let Some((_, encrypted_data)) =
            available_original.iter().find(|(idx, _)| *idx == i)
        {
            encrypted_data
        } else if let Some(encrypted_data) = reconstructed_indices.get(&i) {
            encrypted_data
        } else {
            return Err(StorageError::Rs);
        };

        if let Some(key) = per_blob_key {
            let Some((_, fragment_id, _)) = originals.get(&i) else {
                return Err(StorageError::Rs);
            };
            let decrypted = crypto::decrypt_chunk(encrypted_fragment, key, fragment_id)?;
            chunk_data.extend_from_slice(&decrypted);
        } else {
            chunk_data.extend_from_slice(encrypted_fragment);
        }
    }

    tracing::debug!(
        "RS reconstruction: complete, reconstructed {} bytes",
        chunk_data.len()
    );
    Ok(chunk_data)
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

    use std::sync::Mutex;
    use tokio_stream::StreamExt;

    /// In-memory peer for get-path tests: serves fragments from a map,
    /// optionally corrupting them; records stored_locally settlements.
    struct MemNet {
        served: Mutex<HashMap<Blake3Hash, Vec<u8>>>,
        corrupt: bool,
        marked_local: Mutex<Vec<Blake3Hash>>,
    }

    impl MemNet {
        fn new(corrupt: bool) -> Self {
            MemNet {
                served: Mutex::new(HashMap::new()),
                corrupt,
                marked_local: Mutex::new(Vec::new()),
            }
        }
    }

    impl Transport for MemNet {
        async fn store_fragment(
            &self,
            _peer: &PeerRef,
            _fragment_hash: &Blake3Hash,
            _data: Vec<u8>,
        ) -> Result<crate::traits::StoreResult, TransportError> {
            Err(TransportError::Transport("not used".into()))
        }
        async fn fetch_fragment(
            &self,
            _peer: &PeerRef,
            fragment_hash: &Blake3Hash,
        ) -> Result<Vec<u8>, TransportError> {
            let mut data = self
                .served
                .lock()
                .unwrap()
                .get(fragment_hash)
                .cloned()
                .ok_or_else(|| TransportError::Peer("fragment not found".into()))?;
            if self.corrupt {
                data[0] ^= 0xFF;
            }
            Ok(data)
        }
        async fn fragment_health(
            &self,
            _peer: &PeerRef,
            fragment_hash: &Blake3Hash,
        ) -> Result<bool, TransportError> {
            Ok(self.served.lock().unwrap().contains_key(fragment_hash))
        }
    }

    impl StateReader for MemNet {
        fn placement_inputs(&self) -> Result<crate::traits::PlacementInputs, StorageError> {
            self.placement_inputs_at(1)
        }
        fn placement_inputs_at(
            &self,
            height: i32,
        ) -> Result<crate::traits::PlacementInputs, StorageError> {
            Ok(crate::traits::PlacementInputs {
                height,
                validators: vec![PeerRef {
                    node_id: 2,
                    pubkey: [7u8; 32],
                }],
                metrics: vec![],
            })
        }
        fn fragment_sources(
            &self,
            _fragment_hashes: &[Blake3Hash],
        ) -> Result<HashMap<Blake3Hash, Vec<PeerRef>>, StorageError> {
            Ok(HashMap::new())
        }
        fn all_peers(&self) -> Result<Vec<PeerRef>, StorageError> {
            Ok(vec![PeerRef {
                node_id: 2,
                pubkey: [7u8; 32],
            }])
        }
        fn distributable_blob(
            &self,
            _blob_id: &BlobId,
        ) -> Result<Option<crate::store::DistributableBlob>, StorageError> {
            Ok(None)
        }
        fn local_node_id(&self) -> Option<i32> {
            Some(1)
        }
    }

    impl LocalStateSink for MemNet {
        fn mark_local(&self, fragment_hash: Blake3Hash) {
            self.marked_local.lock().unwrap().push(fragment_hash);
        }
        fn mark_remote_batch(&self, _fragment_hashes: Vec<Blake3Hash>) {}
    }

    fn manifest_from_outcome(
        blob_id: &BlobId,
        outcome: &PutOutcome,
        file_size: u64,
        stored: impl Fn(&PutFragment) -> bool,
    ) -> BlobManifest {
        let mut chunks: HashMap<u32, crate::store::ChunkFragmentMaps> = HashMap::new();
        for f in &outcome.fragments {
            let entry = chunks.entry(f.chunk_number).or_default();
            let bucket = if f.recovery { &mut entry.1 } else { &mut entry.0 };
            bucket.insert(
                f.local_index as usize,
                (f.fragment_hash, f.fragment_id.clone(), stored(f)),
            );
        }
        BlobManifest {
            blob_id: blob_id.clone(),
            integrity_hash: outcome.integrity_hash,
            added_bytes: outcome.added_bytes,
            file_size,
            placement_height: Some(1),
            chunks,
        }
    }

    async fn collect_stream(
        stream: impl tokio_stream::Stream<Item = Result<bytes::Bytes, StorageError>>,
    ) -> Result<Vec<u8>, StorageError> {
        tokio::pin!(stream);
        let mut out = Vec::new();
        while let Some(chunk) = stream.next().await {
            out.extend_from_slice(&chunk?);
        }
        Ok(out)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_reconstructs_k_of_n_over_faulty_transport() {
        // Should: (a) fast-path reconstruct + keyed-verify when all
        // originals are local; (b) RS-reconstruct from k-of-n when some
        // originals are gone but recovery fragments cover; (c) discover
        // every fragment over the Transport seam when nothing is local,
        // settling stored_locally through the sink; (d) FAIL (never emit
        // wrong bytes) when the only peer serves corrupted fragments.
        // Should not: accept a corrupt fragment (defense-in-depth hash
        // check) or a whole-blob mismatch (keyed integrity hash).
        // Impact: this is the read path for every projection — silent
        // corruption or a broken fallback rung is data loss at the worst
        // possible moment (origin offline).
        let base = std::env::temp_dir().join(format!("hopnet-get-test-{}", std::process::id()));
        let dir_a = base.join("a").to_str().unwrap().to_string();
        let key: chacha20poly1305::Key = [0x21u8; 32].into();
        let blob_id = CustomUUID::from_str("01890a5d-ac96-774b-b9aa-9f8b24f0c9a3").unwrap();
        let plaintext: Vec<u8> = (0..120_000u32).map(|i| (i % 241) as u8).collect();

        let outcome = put(
            plaintext.as_slice(),
            plaintext.len(),
            blob_id.clone(),
            &key,
            &dir_a,
        )
        .await
        .unwrap();

        // (a) fast path: everything local
        let manifest = manifest_from_outcome(&blob_id, &outcome, plaintext.len() as u64, |_| true);
        let got = collect_stream(get_local(dir_a.clone(), manifest, Some(key), None))
            .await
            .unwrap();
        assert_eq!(got, plaintext);

        // (b) k-of-n: drop 4 originals from disk + flags; recovery covers
        let dropped: Vec<Blake3Hash> = outcome
            .fragments
            .iter()
            .filter(|f| !f.recovery && f.local_index < 4)
            .map(|f| f.fragment_hash)
            .collect();
        for hash in &dropped {
            fragstore::delete_fragment(&dir_a, hash).unwrap();
        }
        let manifest = manifest_from_outcome(&blob_id, &outcome, plaintext.len() as u64, |f| {
            !dropped.contains(&f.fragment_hash)
        });
        let got = collect_stream(get_local(dir_a.clone(), manifest, Some(key), None))
            .await
            .unwrap();
        assert_eq!(got, plaintext, "RS path must reproduce the plaintext");

        // (c) full discovery: empty local dir, peer serves everything
        let net_peer = Arc::new(MemNet::new(false));
        for f in &outcome.fragments {
            if dropped.contains(&f.fragment_hash) {
                continue; // deleted above — peer holds the surviving set
            }
            let data = fragstore::read_fragment(&dir_a, &f.fragment_hash).unwrap();
            net_peer
                .served
                .lock()
                .unwrap()
                .insert(f.fragment_hash, data);
        }
        let dir_c = base.join("c").to_str().unwrap().to_string();
        std::fs::create_dir_all(&dir_c).unwrap();
        let manifest = manifest_from_outcome(&blob_id, &outcome, plaintext.len() as u64, |f| {
            // nothing local; dropped fragments aren't even on the peer
            let _ = f;
            false
        });
        let net = GetNet {
            transport: net_peer.clone(),
            state: net_peer.clone(),
            local_state: net_peer.clone(),
        };
        let got = collect_stream(get(
            Some(net),
            dir_c.clone(),
            manifest,
            Some(key),
            None,
        ))
        .await
        .unwrap();
        assert_eq!(got, plaintext, "discovery + RS must reproduce the plaintext");
        assert!(
            net_peer.marked_local.lock().unwrap().len() >= ORIGINAL_FRAGMENTS_PER_CHUNK,
            "downloaded fragments must settle through the sink"
        );

        // (d) corrupt peer: hash verification must reject every fetch
        let bad_peer = Arc::new(MemNet::new(true));
        for (hash, data) in net_peer.served.lock().unwrap().iter() {
            bad_peer.served.lock().unwrap().insert(*hash, data.clone());
        }
        let dir_d = base.join("d").to_str().unwrap().to_string();
        std::fs::create_dir_all(&dir_d).unwrap();
        let manifest =
            manifest_from_outcome(&blob_id, &outcome, plaintext.len() as u64, |_| false);
        let net = GetNet {
            transport: bad_peer.clone(),
            state: bad_peer.clone(),
            local_state: bad_peer,
        };
        let result = collect_stream(get(Some(net), dir_d, manifest, Some(key), None)).await;
        assert!(
            result.is_err(),
            "corrupted fragments must never reconstruct"
        );

        let _ = std::fs::remove_dir_all(&base);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_range_slices_without_whole_blob_verify() {
        // Should: an inclusive byte range returns exactly those bytes.
        // Impact: HTTP Range streaming (video scrub) reads through this.
        let base =
            std::env::temp_dir().join(format!("hopnet-get-range-test-{}", std::process::id()));
        let dir = base.to_str().unwrap().to_string();
        let key: chacha20poly1305::Key = [0x33u8; 32].into();
        let blob_id = CustomUUID::from_str("01890a5d-ac96-774b-b9aa-9f8b24f0c9a4").unwrap();
        let plaintext: Vec<u8> = (0..90_000u32).map(|i| (i % 239) as u8).collect();

        let outcome = put(
            plaintext.as_slice(),
            plaintext.len(),
            blob_id.clone(),
            &key,
            &dir,
        )
        .await
        .unwrap();

        let manifest = manifest_from_outcome(&blob_id, &outcome, plaintext.len() as u64, |_| true);
        let got = collect_stream(get_local(
            dir.clone(),
            manifest,
            Some(key),
            Some((1000, 4999)),
        ))
        .await
        .unwrap();
        assert_eq!(got, &plaintext[1000..5000]);

        let _ = std::fs::remove_dir_all(&base);
    }
}
