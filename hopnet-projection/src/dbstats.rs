//! Commit-latency instrumentation (moved down from the host's `db::shared`
//! at RFC-015 Stage D5b so service crates that commit their own local
//! transactions — hopnet-takeout — record into the SAME histogram the host
//! exposes at `GET /debug/db-stats`).

use hdrhistogram::Histogram;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use std::time::Instant;

/// Commit-phase latency in microseconds. Recorded by `commit_timed()` only.
/// Bounded 1us..60s, 3 significant figures (~10KB memory).
pub static COMMIT_LATENCY_US: Lazy<Mutex<Histogram<u64>>> = Lazy::new(|| {
    Mutex::new(
        Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).expect("hdrhistogram bounds are valid"),
    )
});

/// Project-wide replacement for `tx.commit()`: records commit latency into
/// `COMMIT_LATENCY_US` for benchmarking and prod observability. The registered
/// commit_hook also increments `DB_COUNTERS.txn_commits` for any commit path.
/// Same signature as `Transaction::commit`, drop-in.
pub fn commit_timed(tx: rusqlite::Transaction) -> rusqlite::Result<()> {
    let start = Instant::now();
    let result = tx.commit();
    let elapsed_us = start.elapsed().as_micros() as u64;
    let mut h = COMMIT_LATENCY_US.lock();
    let _ = h.record(elapsed_us.max(1));
    result
}
