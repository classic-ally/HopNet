//! Ingest admission: refuse a new blob when storing it would push the
//! fragments filesystem below a free-space floor.
//!
//! `api::put` writes every fragment class of every chunk locally (10 original
//! and 20 recovery per 40MB chunk, ~3x the payload) before distribution starts,
//! and the origin's copies stay protected until placement is confirmed. With
//! no admission check a fast client fills the disk and fragment writes start
//! failing mid-blob; this module lets the host refuse up front instead, so
//! clients back off while there is still headroom.
//!
//! The floor is process-wide and defaults to 0 (disabled): the host enables
//! it at startup ([`set_min_free_bytes`]). Library tests and tools that never
//! set it are unaffected.
//!
//! Concurrent ingests reserve their footprint in a process-wide counter for
//! as long as they run, so N uploads cannot all pass against the same
//! free-space reading. The counter is conservative: bytes an in-flight put
//! has already written are counted both as used space and as reserved.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::StorageError;
use crate::rs::{
    CHUNK_SIZE, MAX_FRAGMENT_SIZE, ORIGINAL_FRAGMENTS_PER_CHUNK, RECOVERY_FRAGMENTS_PER_CHUNK,
};

/// Free-space floor the host enables by default (matches the ingress
/// daemon's reserve and takeout's safety margin).
pub const DEFAULT_MIN_FREE_BYTES: u64 = 10 * 1024 * 1024 * 1024;

/// Per-fragment AEAD overhead (tag + nonce) on top of the fragment payload.
const FRAGMENT_OVERHEAD: u64 = 28;

/// Filesystem allocation rounding charged per fragment file.
const FILE_BLOCK: u64 = 4096;

static MIN_FREE_BYTES: AtomicU64 = AtomicU64::new(0);
static RESERVED: AtomicU64 = AtomicU64::new(0);

/// Set the process-wide free-space floor. 0 disables admission.
pub fn set_min_free_bytes(bytes: u64) {
    MIN_FREE_BYTES.store(bytes, Ordering::Relaxed);
}

pub fn min_free_bytes() -> u64 {
    MIN_FREE_BYTES.load(Ordering::Relaxed)
}

/// Bytes `put` will write locally for a blob of `file_size`: every original
/// and recovery fragment of every chunk, each padded to a full fragment plus
/// AEAD overhead, rounded up to a filesystem block.
pub fn ingest_footprint(file_size: usize) -> u64 {
    let chunks = (file_size as u64).div_ceil(CHUNK_SIZE as u64).max(1);
    let per_chunk_fragments = (ORIGINAL_FRAGMENTS_PER_CHUNK + RECOVERY_FRAGMENTS_PER_CHUNK) as u64;
    // The last chunk is padded to a multiple of the original count, so its
    // fragments are no larger than `file_size / originals` rounded up.
    let fragment_payload = (file_size as u64)
        .div_ceil(ORIGINAL_FRAGMENTS_PER_CHUNK as u64 * chunks)
        .min(MAX_FRAGMENT_SIZE as u64);
    let fragment_file = (fragment_payload + FRAGMENT_OVERHEAD).div_ceil(FILE_BLOCK) * FILE_BLOCK;
    chunks * per_chunk_fragments * fragment_file
}

/// A held reservation; releases its bytes when dropped.
#[derive(Debug)]
pub struct IngestReservation {
    bytes: u64,
    counter: &'static AtomicU64,
}

impl Drop for IngestReservation {
    fn drop(&mut self) {
        self.counter.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

/// Reserve room for a `file_size` ingest under `fragments_dir`, or refuse
/// with [`StorageError::InsufficientSpace`]. Admission is disabled (always
/// granted, nothing reserved) while the floor is 0.
pub fn reserve_ingest(
    fragments_dir: &str,
    file_size: usize,
) -> Result<IngestReservation, StorageError> {
    let floor = min_free_bytes();
    if floor == 0 {
        return Ok(IngestReservation {
            bytes: 0,
            counter: &RESERVED,
        });
    }
    let free = fs4::statvfs(Path::new(fragments_dir))?.available_space();
    try_reserve(&RESERVED, free, ingest_footprint(file_size), floor)
}

/// Would ingests of these sizes fit right now? A read-only admission probe
/// for clients that want an answer before streaming a body: a refusal sent
/// before the body is read reaches an HTTP client as a broken connection, not
/// a status. Reserves nothing; `put` still reserves (and can still refuse).
pub fn check_ingest(fragments_dir: &str, sizes: &[usize]) -> Result<(), StorageError> {
    let floor = min_free_bytes();
    if floor == 0 {
        return Ok(());
    }
    let free = fs4::statvfs(Path::new(fragments_dir))?.available_space();
    let needed = sizes.iter().map(|&s| ingest_footprint(s)).sum();
    fits(free, RESERVED.load(Ordering::Acquire), needed, floor)
}

/// Admission rule: what remains of `free` after outstanding reservations
/// and `needed` must stay above `floor`.
fn fits(free: u64, reserved: u64, needed: u64, floor: u64) -> Result<(), StorageError> {
    let remaining = free.saturating_sub(reserved).saturating_sub(needed);
    if remaining <= floor {
        return Err(StorageError::InsufficientSpace {
            free: free.saturating_sub(reserved),
            needed,
            floor,
        });
    }
    Ok(())
}

/// Admit `needed` bytes against `free` only if what remains after every
/// outstanding reservation stays above `floor`.
fn try_reserve(
    counter: &'static AtomicU64,
    free: u64,
    needed: u64,
    floor: u64,
) -> Result<IngestReservation, StorageError> {
    let mut reserved = counter.load(Ordering::Acquire);
    loop {
        fits(free, reserved, needed, floor)?;
        match counter.compare_exchange_weak(
            reserved,
            reserved + needed,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                return Ok(IngestReservation {
                    bytes: needed,
                    counter,
                })
            }
            Err(actual) => reserved = actual,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MB: u64 = 1024 * 1024;

    fn counter() -> &'static AtomicU64 {
        Box::leak(Box::new(AtomicU64::new(0)))
    }

    // Should: charge roughly three times the payload for a multi-chunk blob,
    // never less.
    // Should: charge a tiny blob at least one block per fragment file.
    #[test]
    fn footprint_covers_every_fragment_class() {
        let big = 100 * MB as usize;
        let fp = ingest_footprint(big);
        assert!(fp >= 3 * big as u64, "{fp} < 3x{big}");
        assert!(
            fp <= 3 * big as u64 + 3 * 30 * (MAX_FRAGMENT_SIZE as u64),
            "{fp}"
        );

        assert_eq!(ingest_footprint(1), 30 * FILE_BLOCK);
    }

    // Should: admit while free space minus the request stays above the floor.
    // Should not: admit a request that would leave the floor or less.
    #[test]
    fn admits_only_above_the_floor() {
        let c = counter();
        assert!(try_reserve(c, 100 * MB, 10 * MB, 50 * MB).is_ok());
        let err = try_reserve(c, 100 * MB, 60 * MB, 50 * MB).unwrap_err();
        assert!(matches!(
            err,
            StorageError::InsufficientSpace { needed, floor, .. }
                if needed == 60 * MB && floor == 50 * MB
        ));
    }

    // Impact: without the shared counter, every concurrent upload would pass
    // against the same free-space reading and together overshoot the floor.
    // Should: count outstanding reservations against later requests.
    // Should: release the bytes when a reservation is dropped.
    #[test]
    fn outstanding_reservations_count_until_dropped() {
        let c = counter();
        let first = try_reserve(c, 100 * MB, 30 * MB, 50 * MB).unwrap();
        assert!(try_reserve(c, 100 * MB, 30 * MB, 50 * MB).is_err());
        drop(first);
        assert_eq!(c.load(Ordering::Acquire), 0);
        assert!(try_reserve(c, 100 * MB, 30 * MB, 50 * MB).is_ok());
    }
}
