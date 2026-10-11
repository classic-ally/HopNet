//! The seal artifact's hash, computed once per decided tip and outside the
//! consensus round (RFC-019 S5, Open Question 1).
//!
//! Proposing a seal, voting on it (vote-iff-match) and the decide's dissent
//! observation all need the hash of the canonical snapshot artifact at the
//! seal's parent. Computed in the round, on the synchronous shell, that cost
//! 75-150 s on the live mesh against ~3 s round timeouts: no seal could
//! decide (2026.10.14 crossing). The artifact covers exported committed state
//! only — `decided_blocks` and `regenesis_state` are DivergenceOnly — and
//! every exported write happens inside a decide, so the hash is a pure
//! function of the decided tip, and the tip's block hash (which chains every
//! earlier block, empty ones included) is a safe key.
//!
//! Each node warms its own cache in the background while a drained
//! moratorium holds (`warm`); the cache is per node on purpose — a replica
//! whose state diverged must compute its own hash, or vote-iff-match and the
//! dissent observation stop detecting it.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use hopnet_common::Blake3Hash;
use rusqlite::OptionalExtension;

use crate::db::DatabaseError;

/// A decided block: the state a snapshot artifact was computed at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tip {
    pub height: u64,
    pub block_hash: Vec<u8>,
}

/// One node's artifact hash at its latest tip, and whether a computation is
/// running (at most one at a time).
#[derive(Debug, Default)]
pub struct ArtifactCache {
    entry: Mutex<Option<(Tip, Blake3Hash)>>,
    computing: AtomicBool,
    /// Test seam: added to every computation, to make the artifact slower
    /// than a consensus round.
    compute_delay: Mutex<Duration>,
}

impl ArtifactCache {
    /// The hash computed at `tip`, if that is the tip cached.
    pub fn get(&self, tip: &Tip) -> Option<Blake3Hash> {
        let entry = self.entry.lock().unwrap_or_else(|p| p.into_inner());
        entry
            .as_ref()
            .filter(|(cached, _)| cached == tip)
            .map(|(_, hash)| *hash)
    }

    pub fn is_computing(&self) -> bool {
        self.computing.load(Ordering::Acquire)
    }

    /// Make every computation take at least `delay` longer (tests).
    pub fn set_compute_delay(&self, delay: Duration) {
        *self.compute_delay.lock().unwrap_or_else(|p| p.into_inner()) = delay;
    }

    /// Claim the single computation slot; `None` when one is running.
    fn begin(&self) -> Option<ComputeSlot<'_>> {
        self.computing
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .ok()
            .map(|_| ComputeSlot(self))
    }

    /// The artifact hash at `tx`'s decided tip: cached when the cache holds
    /// that tip, else computed now (and cached) — unless another computation
    /// is running, which returns `Ok(None)` rather than doubling the work.
    pub fn at_tip(
        &self,
        tx: &rusqlite::Transaction<'_>,
    ) -> Result<Option<(Option<Tip>, Blake3Hash)>, DatabaseError> {
        let tip = tip(tx)?;
        if let Some(hash) = tip.as_ref().and_then(|t| self.get(t)) {
            return Ok(Some((tip, hash)));
        }
        let Some(_slot) = self.begin() else {
            return Ok(None);
        };
        let hash = self.compute(tx)?;
        if let Some(tip) = &tip {
            *self.entry.lock().unwrap_or_else(|p| p.into_inner()) = Some((tip.clone(), hash));
        }
        Ok(Some((tip, hash)))
    }

    /// The artifact hash of `tx`'s state, cached for `tip` when it is the
    /// tip cached, else computed inline (not cached: `tx` may already hold
    /// writes past `tip`).
    pub fn at_or_compute(
        &self,
        tip: Option<&Tip>,
        tx: &rusqlite::Transaction<'_>,
    ) -> Result<Blake3Hash, DatabaseError> {
        match tip.and_then(|t| self.get(t)) {
            Some(hash) => Ok(hash),
            None => self.compute(tx),
        }
    }

    fn compute(&self, tx: &rusqlite::Transaction<'_>) -> Result<Blake3Hash, DatabaseError> {
        let delay = *self.compute_delay.lock().unwrap_or_else(|p| p.into_inner());
        if !delay.is_zero() {
            std::thread::sleep(delay);
        }
        crate::db::snapshot::compute_artifact_hash_tx(tx)
    }
}

/// The running computation; releases the slot when dropped.
struct ComputeSlot<'a>(&'a ArtifactCache);

impl Drop for ComputeSlot<'_> {
    fn drop(&mut self) {
        self.0.computing.store(false, Ordering::Release);
    }
}

/// The decided tip `conn` sees; `None` before the first decided block.
pub fn tip(conn: &rusqlite::Connection) -> Result<Option<Tip>, DatabaseError> {
    conn.query_row(
        "SELECT height, block_hash FROM decided_blocks ORDER BY height DESC LIMIT 1",
        [],
        |row| {
            Ok(Tip {
                height: row.get::<_, i64>(0)? as u64,
                block_hash: row.get(1)?,
            })
        },
    )
    .optional()
    .map_err(|e| DatabaseError::classified(&e, DatabaseError::RecallError))
}

/// The decided block at `height`, if any.
pub fn tip_at(conn: &rusqlite::Connection, height: u64) -> Result<Option<Tip>, DatabaseError> {
    conn.query_row(
        "SELECT block_hash FROM decided_blocks WHERE height = ?1",
        [height as i64],
        |row| {
            Ok(Tip {
                height,
                block_hash: row.get(0)?,
            })
        },
    )
    .optional()
    .map_err(|e| DatabaseError::classified(&e, DatabaseError::RecallError))
}

/// Warm `app_state`'s cache at its current tip on its own thread, unless it
/// already holds it or a computation is running. Never blocks the caller,
/// and needs no async runtime.
pub fn warm(app_state: &crate::AppState) {
    let cache = app_state.artifact_cache.clone();
    if cache.is_computing() {
        return;
    }
    let state = app_state.clone();
    let spawned = std::thread::Builder::new()
        .name("seal-artifact".into())
        .spawn(move || {
            let warmed = state
                .db_pool
                .get()
                .map_err(|e| format!("pool: {e}"))
                .and_then(|mut conn| {
                    let tx = conn.transaction().map_err(|e| format!("tx: {e}"))?;
                    let started = std::time::Instant::now();
                    let cached = tip(&tx)
                        .map_err(|e| format!("tip: {e:?}"))?
                        .is_some_and(|t| cache.get(&t).is_some());
                    if cached {
                        return Ok(());
                    }
                    if let Some((tip, hash)) = cache.at_tip(&tx).map_err(|e| format!("{e:?}"))? {
                        tracing::info!(
                            tip = tip.map(|t| t.height),
                            elapsed_ms = started.elapsed().as_millis() as u64,
                            artifact_hash = %hash.to_hex(),
                            "regenesis: seal artifact hash precomputed"
                        );
                    }
                    Ok(())
                });
            if let Err(e) = warmed {
                tracing::warn!("regenesis: seal artifact precompute failed: {e}");
            }
        });
    if let Err(e) = spawned {
        tracing::warn!("regenesis: seal artifact precompute thread: {e}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chain(heights: &[(u64, u8)]) -> rusqlite::Connection {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE decided_blocks (height INTEGER PRIMARY KEY, block_hash BLOB NOT NULL UNIQUE, round INTEGER NOT NULL, block BLOB NOT NULL);",
        )
        .unwrap();
        for (height, byte) in heights {
            conn.execute(
                "INSERT INTO decided_blocks VALUES (?1, ?2, 0, X'00')",
                rusqlite::params![*height as i64, vec![*byte; 32]],
            )
            .unwrap();
        }
        conn
    }

    // Should: read the highest decided block as the tip, and any block by
    // height.
    // Should: answer from the cache only for the exact tip it was computed
    // at — a later block at the same height count or a different hash is a
    // miss.
    #[test]
    fn the_cache_answers_only_for_its_own_tip() {
        let conn = chain(&[(1, 0xA1), (2, 0xA2)]);
        let at_two = Tip {
            height: 2,
            block_hash: vec![0xA2; 32],
        };
        assert_eq!(tip(&conn).unwrap(), Some(at_two.clone()));
        assert_eq!(
            tip_at(&conn, 1).unwrap().map(|t| t.block_hash),
            Some(vec![0xA1; 32])
        );
        assert_eq!(tip_at(&conn, 3).unwrap(), None);

        let cache = ArtifactCache::default();
        let hash = Blake3Hash::from_bytes([7; 32]);
        *cache.entry.lock().unwrap() = Some((at_two.clone(), hash));
        assert_eq!(cache.get(&at_two), Some(hash));
        assert_eq!(
            cache.get(&Tip {
                height: 2,
                block_hash: vec![0xB2; 32]
            }),
            None
        );
        assert_eq!(
            cache.get(&Tip {
                height: 3,
                block_hash: vec![0xA2; 32]
            }),
            None
        );
    }

    // Impact: a second computation started while the first runs doubles the
    // minutes the shell or the build thread are held for nothing.
    // Should: report a miss as "still computing" while another computation
    // holds the slot, and free the slot once it ends.
    #[test]
    fn a_miss_waits_for_the_running_computation() {
        let mut conn = chain(&[(1, 0xA1)]);
        let cache = ArtifactCache::default();
        let slot = cache.begin().expect("free");
        assert!(cache.is_computing());
        let tx = conn.transaction().unwrap();
        assert!(cache.at_tip(&tx).unwrap().is_none());
        drop(tx);
        drop(slot);
        assert!(!cache.is_computing());
        assert!(cache.begin().is_some(), "the slot is free again");
    }
}
