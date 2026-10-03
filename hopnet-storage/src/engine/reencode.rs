//! Ciphertext-domain re-encode repair (RFC-STORAGE-001 Repair).
//!
//! Fragments are encrypted BEFORE Reed-Solomon, so recovery shards are
//! parity of ciphertext: decode/re-encode runs entirely in the ciphertext
//! domain, regenerated shards are byte-identical to the originals, and
//! every result is verified against the manifest hashes before anything
//! touches disk. No key custody, no plaintext, no consensus transaction —
//! the inventory settles through the next self-check.

use crate::admission::{self, Admission, PauseReason, SpaceGuard, WriteClass};
use crate::fragstore;
use crate::rs::{ORIGINAL_FRAGMENTS_PER_CHUNK, RECOVERY_FRAGMENTS_PER_CHUNK};
use crate::traits::{LocalStateSink, StateReader, Transport};
use crate::types::BlobId;
use crate::StorageError;
use hopnet_common::Blake3Hash;
use std::collections::HashMap;

use super::EngineError;

#[derive(Debug)]
pub struct ReencodeOutcome {
    /// Classes regenerated and stored locally.
    pub regenerated: usize,
    /// Shards fetched from peers to reach K.
    pub fetched: usize,
}

/// Regenerate missing fragment classes from any ≥ K ciphertext shards.
///
/// Pure over in-memory shards (class index 0..N: 0..K originals, K..N
/// recovery). One decode restores every missing original; one re-encode
/// regenerates every recovery shard — flat cost per chunk regardless of
/// how many classes are missing (batch ALL missing classes per session).
pub(crate) fn regenerate_missing(
    shards: &HashMap<u32, Vec<u8>>,
    missing_classes: &[u32],
) -> Result<HashMap<u32, Vec<u8>>, StorageError> {
    if shards.len() < ORIGINAL_FRAGMENTS_PER_CHUNK {
        tracing::error!(
            "re-encode: {} shards available, need {}",
            shards.len(),
            ORIGINAL_FRAGMENTS_PER_CHUNK
        );
        return Err(StorageError::Rs);
    }
    let shard_size = shards
        .values()
        .next()
        .map(|d| d.len())
        .ok_or(StorageError::Rs)?;

    // Decode: restore every missing ORIGINAL from any K shards.
    let mut decoder = reed_solomon_simd::ReedSolomonDecoder::new(
        ORIGINAL_FRAGMENTS_PER_CHUNK,
        RECOVERY_FRAGMENTS_PER_CHUNK,
        shard_size,
    )
    .map_err(|e| {
        tracing::error!("re-encode: decoder init failed: {e:?}");
        StorageError::Rs
    })?;
    for (&class, data) in shards {
        let res = if (class as usize) < ORIGINAL_FRAGMENTS_PER_CHUNK {
            decoder.add_original_shard(class as usize, data)
        } else {
            decoder.add_recovery_shard(class as usize - ORIGINAL_FRAGMENTS_PER_CHUNK, data)
        };
        res.map_err(|e| {
            tracing::error!("re-encode: add shard {class} failed: {e:?}");
            StorageError::Rs
        })?;
    }
    let decoded = decoder.decode().map_err(|e| {
        tracing::error!("re-encode: decode failed: {e:?}");
        StorageError::Rs
    })?;
    let restored: HashMap<usize, Vec<u8>> = decoded
        .restored_original_iter()
        .map(|(i, d)| (i, d.to_vec()))
        .collect();

    // Full ordered ciphertext originals 0..K.
    let mut originals: Vec<&[u8]> = Vec::with_capacity(ORIGINAL_FRAGMENTS_PER_CHUNK);
    for i in 0..ORIGINAL_FRAGMENTS_PER_CHUNK {
        if let Some(d) = shards.get(&(i as u32)) {
            originals.push(d);
        } else if let Some(d) = restored.get(&i) {
            originals.push(d);
        } else {
            tracing::error!("re-encode: original {i} unreconstructable");
            return Err(StorageError::Rs);
        }
    }

    // Re-encode: regenerate ALL recovery shards (byte-identical parity).
    let mut encoder = reed_solomon_simd::ReedSolomonEncoder::new(
        ORIGINAL_FRAGMENTS_PER_CHUNK,
        RECOVERY_FRAGMENTS_PER_CHUNK,
        shard_size,
    )
    .map_err(|e| {
        tracing::error!("re-encode: encoder init failed: {e:?}");
        StorageError::Rs
    })?;
    for d in &originals {
        encoder.add_original_shard(d).map_err(|e| {
            tracing::error!("re-encode: encoder add failed: {e:?}");
            StorageError::Rs
        })?;
    }
    let encoded = encoder.encode().map_err(|e| {
        tracing::error!("re-encode: encode failed: {e:?}");
        StorageError::Rs
    })?;
    let recovery: Vec<Vec<u8>> = encoded.recovery_iter().map(|d| d.to_vec()).collect();

    let mut out = HashMap::with_capacity(missing_classes.len());
    for &class in missing_classes {
        let bytes = if (class as usize) < ORIGINAL_FRAGMENTS_PER_CHUNK {
            originals[class as usize].to_vec()
        } else {
            recovery
                .get(class as usize - ORIGINAL_FRAGMENTS_PER_CHUNK)
                .ok_or(StorageError::Rs)?
                .clone()
        };
        out.insert(class, bytes);
    }
    Ok(out)
}

/// Re-encode one chunk's missing classes on THIS node: gather any K live
/// shards (local disk first, then peers via discovery), regenerate, verify
/// EVERY result against the manifest hashes — a single mismatch stores
/// nothing (loud failure; a silent RS-library behavior change must never
/// propagate unverifiable bytes) — then store and mark local. Inventory
/// settles via the next self-check; no consensus transaction.
#[allow(clippy::too_many_arguments)]
pub async fn reencode_chunk<T, S, L>(
    transport: &std::sync::Arc<T>,
    state: &S,
    local_state: &L,
    fragments_dir: &str,
    blob_id: &BlobId,
    chunk_number: u32,
    missing_classes: &[u32],
    space: (&SpaceGuard, WriteClass),
) -> Result<ReencodeOutcome, EngineError>
where
    T: Transport + 'static,
    S: StateReader,
    L: LocalStateSink,
{
    reencode_chunk_via(
        state,
        local_state,
        fragments_dir,
        blob_id,
        chunk_number,
        missing_classes,
        space,
        |hash, hint, members| async move {
            crate::api::find_fragment_via(transport, &hash, &members, Some(hint)).await
        },
    )
    .await
}

/// `reencode_chunk` with the shard fetch supplied by the caller: the pull
/// path routes it through the fetch scheduler (caps, deadline, parked and
/// dark peers skipped); the repair path uses plain discovery. `fetch`
/// gets a shard's hash, its attested holders and the view's members.
#[allow(clippy::too_many_arguments)]
pub async fn reencode_chunk_via<S, L, F, Fut>(
    state: &S,
    local_state: &L,
    fragments_dir: &str,
    blob_id: &BlobId,
    chunk_number: u32,
    missing_classes: &[u32],
    (space, write_class): (&SpaceGuard, WriteClass),
    mut fetch: F,
) -> Result<ReencodeOutcome, EngineError>
where
    S: StateReader,
    L: LocalStateSink,
    F: FnMut(Blake3Hash, Vec<crate::traits::PeerRef>, Vec<crate::traits::PeerRef>) -> Fut,
    Fut: std::future::Future<Output = Option<Vec<u8>>>,
{
    let Some(manifest) = state.blob_manifest(blob_id)? else {
        return Ok(ReencodeOutcome {
            regenerated: 0,
            fetched: 0,
        }); // raced a delete
    };
    let Some((originals, recovery)) = manifest.chunks.get(&chunk_number) else {
        return Err(EngineError::Transfer(format!(
            "re-encode: blob {blob_id} has no chunk {chunk_number}"
        )));
    };

    // class → (manifest hash, stored locally)
    let mut classes: HashMap<u32, (Blake3Hash, bool)> = HashMap::new();
    for map in [originals, recovery] {
        for (idx, (hash, _, stored_locally)) in map {
            classes.insert(*idx as u32, (*hash, *stored_locally));
        }
    }
    let targets: Vec<u32> = missing_classes
        .iter()
        .copied()
        .filter(|c| classes.get(c).is_some_and(|(_, local)| !local))
        .collect();
    if targets.is_empty() {
        return Ok(ReencodeOutcome {
            regenerated: 0,
            fetched: 0,
        });
    }
    // Held for space: don't fetch K shards to write nothing. Asked against
    // this write's own floor (an urgent repair's is the ingest floor), at
    // the worst-case size of the classes it will store.
    let worst_case =
        targets.len() as u64 * admission::fragment_file_bytes(crate::rs::MAX_FRAGMENT_SIZE);
    let reason = match space.would_admit(fragments_dir, worst_case, write_class) {
        Admission::Admit => None,
        Admission::BelowFloor => Some(PauseReason::LowSpace),
        Admission::ProbeError => Some(PauseReason::ProbeError),
    };
    if let Some(reason) = reason {
        // A refused pull-class write (a pull-path rebuild, lazy repair)
        // holds the guard back, so the blob waits in the queue for the
        // pause to clear instead of coming back every re-probe interval to
        // be refused again. An urgent repair is counted and WARNed (its
        // chunk stays below the watermark) and the next policy tick
        // re-derives it; it never pauses the guard.
        match write_class {
            WriteClass::Pull => space.hold_back(reason),
            WriteClass::Repair => {
                space.note_urgent_refused(&format!("blob {blob_id} chunk {chunk_number}"), reason)
            }
        }
        return Err(EngineError::NoSpace);
    }

    // Gather ≥ K shards: local disk first (free), then peers.
    let mut shards: HashMap<u32, Vec<u8>> = HashMap::new();
    for (&class, (hash, stored_locally)) in &classes {
        if shards.len() >= ORIGINAL_FRAGMENTS_PER_CHUNK {
            break;
        }
        if *stored_locally {
            if let Ok(data) = fragstore::fetch_and_verify_fragment(hash, fragments_dir) {
                shards.insert(class, data);
            }
        }
    }
    let mut fetched = 0usize;
    if shards.len() < ORIGINAL_FRAGMENTS_PER_CHUNK {
        let view = state.storage_view()?;
        let want: Vec<(u32, Blake3Hash)> = classes
            .iter()
            .filter(|(c, _)| !shards.contains_key(c) && !targets.contains(c))
            .map(|(c, (h, _))| (*c, *h))
            .collect();
        let hashes: Vec<Blake3Hash> = want.iter().map(|(_, h)| *h).collect();
        let mut sources = state.fragment_sources(&hashes)?;
        for (class, hash) in want {
            if shards.len() >= ORIGINAL_FRAGMENTS_PER_CHUNK {
                break;
            }
            let hint = sources.remove(&hash).unwrap_or_default();
            if let Some(data) = fetch(hash, hint, view.members.clone()).await {
                shards.insert(class, data);
                fetched += 1;
            }
        }
    }
    if shards.len() < ORIGINAL_FRAGMENTS_PER_CHUNK {
        return Err(EngineError::Transfer(format!(
            "re-encode: blob {blob_id} chunk {chunk_number}: only {} of {} shards sourceable",
            shards.len(),
            ORIGINAL_FRAGMENTS_PER_CHUNK
        )));
    }

    let regenerated = regenerate_missing(&shards, &targets).map_err(EngineError::State)?;

    // Verify ALL before storing ANY.
    for (&class, bytes) in &regenerated {
        let expected = classes[&class].0;
        let actual = Blake3Hash::new(blake3::hash(bytes));
        if actual != expected {
            return Err(EngineError::Transfer(format!(
                "re-encode: blob {blob_id} chunk {chunk_number} class {class}: regenerated \
                 shard hash {} != manifest {} — storing nothing",
                actual.to_hex(),
                expected.to_hex()
            )));
        }
    }
    // The regenerated bytes are reserved against the floor for this
    // write's class before any of them land.
    let needed = regenerated
        .values()
        .map(|b| admission::fragment_file_bytes(b.len()))
        .sum();
    let _reserved = space
        .reserve(fragments_dir, needed, write_class)
        .map_err(|e| {
            if admission::is_disk_full(&e) {
                EngineError::NoSpace
            } else {
                EngineError::State(e)
            }
        })?;
    for (&class, bytes) in &regenerated {
        let hash = classes[&class].0;
        fragstore::store_fragment(fragments_dir, &hash, bytes.clone()).map_err(|e| {
            if admission::is_disk_full(&e) {
                space.note_disk_full();
                return EngineError::NoSpace;
            }
            EngineError::Transfer(format!("re-encode: store class {class}: {e}"))
        })?;
        local_state.mark_local(hash).await;
    }

    tracing::info!(
        "re-encode: blob {} chunk {} regenerated {} classes ({} shards fetched)",
        blob_id,
        chunk_number,
        regenerated.len(),
        fetched
    );
    Ok(ReencodeOutcome {
        regenerated: regenerated.len(),
        fetched,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Encode a synthetic ciphertext chunk exactly as the put path does:
    /// 10 original shards → 20 recovery shards, hashes over all 30.
    fn synthetic_chunk(shard_size: usize) -> (Vec<Vec<u8>>, Vec<Blake3Hash>) {
        let originals: Vec<Vec<u8>> = (0..ORIGINAL_FRAGMENTS_PER_CHUNK)
            .map(|i| {
                (0..shard_size)
                    .map(|b| ((i * 31 + b * 7) % 251) as u8)
                    .collect()
            })
            .collect();
        let mut encoder = reed_solomon_simd::ReedSolomonEncoder::new(
            ORIGINAL_FRAGMENTS_PER_CHUNK,
            RECOVERY_FRAGMENTS_PER_CHUNK,
            shard_size,
        )
        .unwrap();
        for d in &originals {
            encoder.add_original_shard(d).unwrap();
        }
        let encoded = encoder.encode().unwrap();
        let mut all: Vec<Vec<u8>> = originals;
        all.extend(encoded.recovery_iter().map(|d| d.to_vec()));
        let hashes = all
            .iter()
            .map(|d| Blake3Hash::new(blake3::hash(d)))
            .collect();
        (all, hashes)
    }

    // Should: regenerate BOTH a missing original and a missing recovery
    // class byte-identically (manifest hashes match) from any K surviving
    // shards, including recovery-only survivor sets.
    // Should not: require the original shards to survive.
    // Impact: byte identity is what makes repair keyless and verifiable —
    // without it every repair would need a consensus re-commit and key
    // custody on the repairer.
    #[test]
    fn regenerates_missing_classes_byte_identical() {
        let (all, hashes) = synthetic_chunk(64);

        // Missing one original (3) and one recovery (17); survivors = rest.
        let missing = [3u32, 17u32];
        let shards: HashMap<u32, Vec<u8>> = (0..30u32)
            .filter(|c| !missing.contains(c))
            .map(|c| (c, all[c as usize].clone()))
            .collect();
        let out = regenerate_missing(&shards, &missing).unwrap();
        for &c in &missing {
            assert_eq!(out[&c], all[c as usize], "class {c} bytes");
            assert_eq!(Blake3Hash::new(blake3::hash(&out[&c])), hashes[c as usize]);
        }

        // Recovery-only survivors: exactly K recovery shards regenerate
        // every original.
        let shards: HashMap<u32, Vec<u8>> =
            (10..20u32).map(|c| (c, all[c as usize].clone())).collect();
        let all_originals: Vec<u32> = (0..10).collect();
        let out = regenerate_missing(&shards, &all_originals).unwrap();
        for c in 0..10u32 {
            assert_eq!(
                out[&c], all[c as usize],
                "original {c} from recovery-only set"
            );
        }
    }

    // Should: refuse to regenerate below K shards.
    // Impact: below K the chunk is unreconstructable — a silent partial
    // result here would masquerade as repair while durability is already
    // lost.
    #[test]
    fn refuses_below_k() {
        let (all, _) = synthetic_chunk(64);
        let shards: HashMap<u32, Vec<u8>> =
            (0..9u32).map(|c| (c, all[c as usize].clone())).collect();
        assert!(regenerate_missing(&shards, &[15]).is_err());
    }
}
