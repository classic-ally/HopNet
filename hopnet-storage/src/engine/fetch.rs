//! The fetch scheduler (RFC-STORAGE-003 S3): bounded parallel pulls.
//!
//! The worker used to pull one blob at a time and one class at a time,
//! moving 0.4–3 MB/s per node over idle links (production, 2026-10-03).
//! Concurrency is now bounded where the cost is — fragment fetches in
//! flight, globally and per source peer — while the blob window is wide
//! and cheap (an admitted blob holds descriptors, never bytes):
//!
//! - window: blobs admitted at once (`HOPNET_PULL_WINDOW`, default 64);
//! - global: fetches in flight across all blobs (`HOPNET_PULL_FETCH_GLOBAL`,
//!   default 24), less `HOPNET_PULL_URGENT_RESERVE` (default 4) held back
//!   for urgent re-encode, which runs beside the window;
//! - per peer: fetches in flight from one source (`HOPNET_PULL_FETCH_PER_PEER`,
//!   default 8), so one slow peer cannot take the global budget;
//! - per blob: at most a quarter of the global cap, so one many-class blob
//!   cannot either.
//!
//! Why wide: every blob starts with all its fragments on one origin. With
//! a few blobs in flight, one offline origin fills every slot. So a source
//! that fails at the transport level three times running is parked with
//! backoff, and a blob whose remaining sources are all parked or
//! unreachable (and that cannot be rebuilt) is parked itself and leaves
//! the window, which admits the next blob.

use crate::traits::{PeerRef, Transport, TransportError};
use crate::types::BlobId;
use hopnet_common::Blake3Hash;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

/// Pull concurrency knobs, read once at engine spawn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PullLimits {
    pub window: usize,
    pub fetch_global: usize,
    pub fetch_per_peer: usize,
    pub urgent_reserve: usize,
    pub fetch_timeout: Duration,
    /// Failures from one peer inside this window count as one strike: a
    /// burst of concurrent fetches failing together is one event.
    pub strike_window: Duration,
    /// Pull-path rebuilds running at once (`HOPNET_PULL_REBUILDS`, default 2):
    /// each holds a chunk of shards (tens of MB) in memory.
    pub rebuilds: usize,
}

impl Default for PullLimits {
    fn default() -> Self {
        PullLimits {
            window: 64,
            fetch_global: 24,
            fetch_per_peer: 8,
            urgent_reserve: 4,
            fetch_timeout: Duration::from_secs(PULL_FETCH_TIMEOUT_SECS),
            strike_window: Duration::from_secs(PEER_STRIKE_WINDOW_SECS),
            rebuilds: 2,
        }
    }
}

/// A fetch that has not answered in this long releases its slots.
pub const PULL_FETCH_TIMEOUT_SECS: u64 = 30;
/// Consecutive strikes (failure bursts, `strike_window` apart) before a
/// source peer is parked.
pub const PEER_FAIL_PARK: u32 = 3;
/// See `PullLimits::strike_window`.
pub const PEER_STRIKE_WINDOW_SECS: u64 = 5;
/// A peer that served a fetch this recently is slow, not dark, when it
/// times out: it is throttled to one fetch at a time, never parked.
pub const PEER_RECENT_SUCCESS: Duration = Duration::from_secs(120);
/// How long a slow peer stays throttled after its last timeout.
pub const PEER_SLOW_FOR: Duration = Duration::from_secs(60);
/// First peer park; doubles per repeat, capped.
pub const PEER_PARK_BASE: Duration = Duration::from_secs(30);
pub const PEER_PARK_CAP: Duration = Duration::from_secs(600);
/// First blob park; doubles per repeat, capped.
pub const BLOB_PARK_BASE: Duration = Duration::from_secs(60);
pub const BLOB_PARK_CAP: Duration = Duration::from_secs(1800);
/// How long a blob waits out busy rebuild slots (flat, no backoff).
pub const REBUILD_WAIT_PARK: Duration = Duration::from_secs(30);
/// A park entry whose park ended this long ago is forgotten.
pub const PARK_FORGET: Duration = Duration::from_secs(3600);
/// How long the cached storage-view membership is reused.
pub const MEMBERS_TTL: Duration = Duration::from_secs(60);

impl PullLimits {
    /// The defaults, overridden by `HOPNET_PULL_WINDOW`,
    /// `HOPNET_PULL_FETCH_GLOBAL`, `HOPNET_PULL_FETCH_PER_PEER` and
    /// `HOPNET_PULL_URGENT_RESERVE` where set to a positive integer.
    pub fn from_env() -> Self {
        Self::from_lookup(|k| std::env::var(k).ok())
    }

    pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Self {
        let d = Self::default();
        let read = |k: &str, default: usize| {
            get(k)
                .and_then(|v| v.trim().parse::<usize>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(default)
        };
        PullLimits {
            window: read("HOPNET_PULL_WINDOW", d.window),
            fetch_global: read("HOPNET_PULL_FETCH_GLOBAL", d.fetch_global),
            fetch_per_peer: read("HOPNET_PULL_FETCH_PER_PEER", d.fetch_per_peer),
            urgent_reserve: get("HOPNET_PULL_URGENT_RESERVE")
                .and_then(|v| v.trim().parse::<usize>().ok())
                .unwrap_or(d.urgent_reserve),
            fetch_timeout: d.fetch_timeout,
            strike_window: d.strike_window,
            rebuilds: read("HOPNET_PULL_REBUILDS", d.rebuilds),
        }
    }

    /// Fetch permits pulls may use: the global cap less the urgent reserve,
    /// never below one.
    pub fn pull_permits(&self) -> usize {
        self.fetch_global.saturating_sub(self.urgent_reserve).max(1)
    }

    /// Fetches one blob may hold at once: a quarter of the global cap.
    pub fn per_blob(&self) -> usize {
        self.fetch_global.div_ceil(4).max(1)
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct PeerPark {
    /// Consecutive strikes since the last success or park expiry.
    failures: u32,
    /// Parks since the last success (sets the backoff).
    parks: u32,
    until: Option<Instant>,
    last_strike: Option<Instant>,
    last_success: Option<Instant>,
    /// Throttled to one fetch at a time until then.
    slow_until: Option<Instant>,
}

/// How a fetch from a peer failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerFailure {
    /// No answer before the deadline.
    Timeout,
    /// The transport failed outright (refused, reset, unroutable).
    Transport,
}

#[derive(Debug, Clone, Copy)]
struct BlobPark {
    attempts: u32,
    until: Instant,
    since: Instant,
}

/// What the scheduler is doing right now — the planner's report field.
#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct SchedulerStats {
    pub window_used: usize,
    pub fetches_in_flight: usize,
    pub per_peer_in_flight: Vec<(i32, usize)>,
    pub parked_peers: Vec<ParkedPeer>,
    pub parked_blobs: usize,
    pub parked_longest_secs: u64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ParkedPeer {
    pub node_id: i32,
    pub failures: u32,
    pub parked_for_secs: u64,
}

/// Shared scheduler state: the semaphores and the park books.
pub struct FetchScheduler {
    pub limits: PullLimits,
    pub window: Arc<Semaphore>,
    global: Arc<Semaphore>,
    /// Pull-path rebuilds in flight (`PullLimits::rebuilds`).
    pub rebuild: Arc<Semaphore>,
    per_peer: Mutex<HashMap<i32, Arc<Semaphore>>>,
    /// One-permit gates for peers marked slow.
    slow_gates: Mutex<HashMap<i32, Arc<Semaphore>>>,
    /// Storage-view member ids, cached for `MEMBERS_TTL`.
    members: Mutex<Option<(Instant, Arc<std::collections::HashSet<i32>>)>>,
    /// One membership refresh at a time.
    members_refreshing: std::sync::atomic::AtomicBool,
    peers: Mutex<HashMap<i32, PeerPark>>,
    blobs: Mutex<HashMap<BlobId, BlobPark>>,
}

impl FetchScheduler {
    pub fn new(limits: PullLimits) -> Arc<Self> {
        Arc::new(FetchScheduler {
            limits,
            window: Arc::new(Semaphore::new(limits.window.max(1))),
            global: Arc::new(Semaphore::new(limits.pull_permits())),
            rebuild: Arc::new(Semaphore::new(limits.rebuilds.max(1))),
            per_peer: Mutex::new(HashMap::new()),
            slow_gates: Mutex::new(HashMap::new()),
            members: Mutex::new(None),
            members_refreshing: std::sync::atomic::AtomicBool::new(false),
            peers: Mutex::new(HashMap::new()),
            blobs: Mutex::new(HashMap::new()),
        })
    }

    fn slow_gate(&self, node_id: i32) -> Arc<Semaphore> {
        self.slow_gates
            .lock()
            .unwrap()
            .entry(node_id)
            .or_insert_with(|| Arc::new(Semaphore::new(1)))
            .clone()
    }

    fn peer_semaphore(&self, node_id: i32) -> Arc<Semaphore> {
        self.per_peer
            .lock()
            .unwrap()
            .entry(node_id)
            .or_insert_with(|| Arc::new(Semaphore::new(self.limits.fetch_per_peer.max(1))))
            .clone()
    }

    /// Free per-peer permits, for picking the least loaded source.
    fn peer_free(&self, node_id: i32) -> usize {
        self.peer_semaphore(node_id).available_permits()
    }

    pub fn peer_parked(&self, node_id: i32, now: Instant) -> bool {
        self.peers
            .lock()
            .unwrap()
            .get(&node_id)
            .and_then(|p| p.until)
            .is_some_and(|until| until > now)
    }

    /// Throttled to one fetch at a time (it has been timing out while it
    /// still serves).
    pub fn peer_slow(&self, node_id: i32, now: Instant) -> bool {
        self.peers
            .lock()
            .unwrap()
            .get(&node_id)
            .and_then(|p| p.slow_until)
            .is_some_and(|until| until > now)
    }

    pub fn record_peer_success(&self, node_id: i32, now: Instant) {
        let mut peers = self.peers.lock().unwrap();
        let p = peers.entry(node_id).or_default();
        let was_parked = p.until.is_some();
        p.failures = 0;
        p.parks = 0;
        p.until = None;
        p.last_strike = None;
        p.last_success = Some(now);
        if was_parked {
            tracing::info!(node = node_id, "pull: source peer unparked");
        }
    }

    /// Account one failed fetch. Concurrent failures inside the strike
    /// window are one strike; a park that has expired starts the count
    /// over; a timeout from a peer that served recently marks it slow
    /// (one fetch at a time) instead of striking it, so a slow disk is
    /// throttled, never parked by its peers.
    pub fn record_peer_failure(&self, node_id: i32, kind: PeerFailure, now: Instant) {
        let mut peers = self.peers.lock().unwrap();
        let p = peers.entry(node_id).or_default();
        if p.until.is_some_and(|u| u <= now) {
            p.until = None;
            p.failures = 0;
            p.last_strike = None;
        }
        if kind == PeerFailure::Timeout
            && p.last_success
                .is_some_and(|t| now.saturating_duration_since(t) < PEER_RECENT_SUCCESS)
        {
            if p.slow_until.is_none_or(|u| u <= now) {
                tracing::info!(node = node_id, "pull: source peer slow; throttled");
            }
            p.slow_until = Some(now + PEER_SLOW_FOR);
            return;
        }
        if p.until.is_some()
            || p.last_strike
                .is_some_and(|t| now.saturating_duration_since(t) < self.limits.strike_window)
        {
            return;
        }
        p.last_strike = Some(now);
        p.failures += 1;
        if p.failures >= PEER_FAIL_PARK {
            let backoff = backoff(PEER_PARK_BASE, PEER_PARK_CAP, p.parks);
            p.parks += 1;
            p.until = Some(now + backoff);
            tracing::info!(
                node = node_id,
                failures = p.failures,
                backoff_secs = backoff.as_secs(),
                "pull: source peer parked"
            );
        }
    }

    pub fn blob_parked(&self, blob_id: &BlobId, now: Instant) -> bool {
        self.blobs
            .lock()
            .unwrap()
            .get(blob_id)
            .is_some_and(|p| p.until > now)
    }

    /// Park a blob whose sources are all unreachable; backs off per repeat.
    pub fn park_blob(&self, blob_id: &BlobId, now: Instant) {
        let mut blobs = self.blobs.lock().unwrap();
        let entry = blobs.entry(blob_id.clone()).or_insert(BlobPark {
            attempts: 0,
            until: now,
            since: now,
        });
        entry.until = now + backoff(BLOB_PARK_BASE, BLOB_PARK_CAP, entry.attempts);
        entry.attempts += 1;
    }

    /// Hold a blob out briefly because every rebuild slot is busy: a flat
    /// `REBUILD_WAIT_PARK` that leaves the unreachable backoff alone, so a
    /// rebuild-needing (often at-risk) blob is back as soon as a slot may
    /// have freed. Never shortens a longer park already in force.
    pub fn park_blob_for_rebuild(&self, blob_id: &BlobId, now: Instant) {
        let mut blobs = self.blobs.lock().unwrap();
        let entry = blobs.entry(blob_id.clone()).or_insert(BlobPark {
            attempts: 0,
            until: now,
            since: now,
        });
        entry.until = entry.until.max(now + REBUILD_WAIT_PARK);
    }

    /// A blob that made progress (or turned out to owe nothing) leaves
    /// the park book.
    pub fn unpark_blob(&self, blob_id: &BlobId) {
        self.blobs.lock().unwrap().remove(blob_id);
    }

    /// Forget park entries whose park ended over `PARK_FORGET` ago: nothing
    /// re-parked them since, so the blob was confirmed, deleted, or went
    /// quiet. A blob still stuck is re-parked on every retry and keeps its
    /// entry. Returns how many were dropped.
    pub fn prune_parks(&self, now: Instant) -> usize {
        let mut blobs = self.blobs.lock().unwrap();
        let before = blobs.len();
        blobs.retain(|_, p| now.saturating_duration_since(p.until) < PARK_FORGET);
        before - blobs.len()
    }

    #[cfg(test)]
    /// How long the blob has been in the park book (since its first park,
    /// across repeats), if at all.
    pub fn blob_parked_for(&self, blob_id: &BlobId, now: Instant) -> Option<Duration> {
        self.blobs
            .lock()
            .unwrap()
            .get(blob_id)
            .map(|p| now.saturating_duration_since(p.since))
    }

    /// Current storage-view member ids, cached for `MEMBERS_TTL`. Deriving
    /// the view is a pool checkout plus 30 days of availability history,
    /// so it never runs under the cache lock or on an async worker: one
    /// caller at a time refreshes it on the blocking pool, in the
    /// background while a stale value exists (everyone keeps using that
    /// value meanwhile), awaited only by the first caller when there is
    /// none. `None` = no view yet or it cannot be read; nothing is
    /// filtered then.
    pub async fn members<S: crate::traits::StateReader + 'static>(
        self: &Arc<Self>,
        state: &Arc<S>,
        now: Instant,
    ) -> Option<Arc<std::collections::HashSet<i32>>> {
        let cached = self.members.lock().unwrap().clone();
        if let Some((at, ids)) = &cached {
            if now.saturating_duration_since(*at) < MEMBERS_TTL {
                return Some(ids.clone());
            }
        }
        if self
            .members_refreshing
            .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            return cached.map(|(_, ids)| ids);
        }
        let (this, state) = (self.clone(), state.clone());
        let refresh = async move {
            let view = tokio::task::spawn_blocking(move || state.storage_view()).await;
            let ids = match view {
                Ok(Ok(view)) => {
                    let ids: Arc<std::collections::HashSet<i32>> =
                        Arc::new(view.members.iter().map(|p| p.node_id).collect());
                    *this.members.lock().unwrap() = Some((Instant::now(), ids.clone()));
                    Some(ids)
                }
                _ => None,
            };
            this.members_refreshing
                .store(false, std::sync::atomic::Ordering::Release);
            ids
        };
        match cached {
            Some((_, stale)) => {
                tokio::spawn(refresh);
                Some(stale)
            }
            None => refresh.await,
        }
    }

    pub fn stats(&self, now: Instant) -> SchedulerStats {
        let per_peer: Vec<(i32, usize)> = {
            let map = self.per_peer.lock().unwrap();
            let mut v: Vec<(i32, usize)> = map
                .iter()
                .map(|(n, s)| {
                    (
                        *n,
                        self.limits.fetch_per_peer
                            - s.available_permits().min(self.limits.fetch_per_peer),
                    )
                })
                .filter(|(_, n)| *n > 0)
                .collect();
            v.sort_unstable();
            v
        };
        let parked_peers = {
            let peers = self.peers.lock().unwrap();
            let mut v: Vec<ParkedPeer> = peers
                .iter()
                .filter(|(_, p)| p.until.is_some_and(|u| u > now))
                .map(|(n, p)| ParkedPeer {
                    node_id: *n,
                    failures: p.failures,
                    parked_for_secs: p
                        .until
                        .map_or(0, |u| u.saturating_duration_since(now).as_secs()),
                })
                .collect();
            v.sort_unstable_by_key(|p| p.node_id);
            v
        };
        let (parked_blobs, parked_longest_secs) = {
            let blobs = self.blobs.lock().unwrap();
            let live: Vec<&BlobPark> = blobs.values().filter(|p| p.until > now).collect();
            let longest = live
                .iter()
                .map(|p| now.saturating_duration_since(p.since).as_secs())
                .max()
                .unwrap_or(0);
            (live.len(), longest)
        };
        SchedulerStats {
            window_used: self.limits.window.max(1) - self.window.available_permits(),
            fetches_in_flight: self.limits.pull_permits() - self.global.available_permits(),
            per_peer_in_flight: per_peer,
            parked_peers,
            parked_blobs,
            parked_longest_secs,
        }
    }
}

fn backoff(base: Duration, cap: Duration, repeats: u32) -> Duration {
    base.saturating_mul(1u32 << repeats.min(16)).min(cap)
}

/// Why a class was not fetched.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetchMiss {
    /// A holder replied that it does not have it, or served bytes that
    /// failed verification (or no holder is known): rebuild.
    NotServed,
    /// A known holder is parked, unreachable, or too busy to serve it now:
    /// retry later rather than rebuild.
    Unreachable,
}

/// Verified fragment bytes, still holding their global fetch slot: the
/// caller keeps this alive until the bytes are on disk, so the global cap
/// bounds bytes awaiting a write, not only bytes on the wire.
pub struct Fetched {
    pub data: Vec<u8>,
    pub slot: tokio::sync::OwnedSemaphorePermit,
}

/// Fetch one class: attested holders first, least-loaded first, each
/// under the global and per-peer caps and the fetch deadline; the bytes
/// are verified against the manifest hash. Then reactive discovery over
/// the other reachable peers.
pub async fn fetch_class<T: Transport + 'static>(
    transport: &Arc<T>,
    sched: &FetchScheduler,
    hash: &Blake3Hash,
    attested: &[PeerRef],
    others: &[PeerRef],
) -> Result<Fetched, FetchMiss> {
    let now = Instant::now();
    let mut holders: Vec<PeerRef> = attested
        .iter()
        .copied()
        .filter(|p| !sched.peer_parked(p.node_id, now))
        .collect();
    holders.sort_by_key(|p| std::cmp::Reverse(sched.peer_free(p.node_id)));
    let mut any_answer = false;
    // A holder we could not get a slot with (busy, or a slow peer's gate)
    // has not answered: the class is retried later, never rebuilt for it.
    let mut busy = false;
    for peer in &holders {
        // The peer's slot first, so a global slot never idles behind a
        // busy peer.
        let peer_sem = sched.peer_semaphore(peer.node_id);
        let Ok(Ok(_slot)) =
            tokio::time::timeout(sched.limits.fetch_timeout, peer_sem.acquire()).await
        else {
            busy = true;
            continue;
        };
        // A slow peer serves one fetch at a time.
        let _slow = if sched.peer_slow(peer.node_id, Instant::now()) {
            let gate = sched.slow_gate(peer.node_id);
            match tokio::time::timeout(sched.limits.fetch_timeout, gate.acquire_owned()).await {
                Ok(Ok(permit)) => Some(permit),
                _ => {
                    busy = true;
                    continue;
                }
            }
        } else {
            None
        };
        let Ok(global) = sched.global.clone().acquire_owned().await else {
            return Err(FetchMiss::NotServed);
        };
        match tokio::time::timeout(
            sched.limits.fetch_timeout,
            transport.fetch_fragment(peer, hash),
        )
        .await
        {
            Ok(Ok(data)) if Blake3Hash::new(blake3::hash(&data)) == *hash => {
                sched.record_peer_success(peer.node_id, Instant::now());
                return Ok(Fetched { data, slot: global });
            }
            Ok(Ok(_)) => {
                tracing::warn!(
                    node = peer.node_id,
                    "pull: fragment {} failed verification",
                    hash.to_hex()
                );
                any_answer = true;
            }
            Ok(Err(TransportError::Peer(_))) => {
                // The peer answered: reachable, just not holding it.
                sched.record_peer_success(peer.node_id, Instant::now());
                any_answer = true;
            }
            Ok(Err(TransportError::Transport(_))) => {
                sched.record_peer_failure(peer.node_id, PeerFailure::Transport, Instant::now());
            }
            Err(_) => {
                sched.record_peer_failure(peer.node_id, PeerFailure::Timeout, Instant::now());
            }
        }
    }

    // Discovery over the rest: health-checked fan-out, so a peer that does
    // not hold the class is asked, not fetched from.
    let now = Instant::now();
    let rest: Vec<PeerRef> = others
        .iter()
        .copied()
        .filter(|p| !attested.iter().any(|a| a.node_id == p.node_id))
        .filter(|p| !sched.peer_parked(p.node_id, now))
        .collect();
    if !rest.is_empty() {
        let Ok(global) = sched.global.clone().acquire_owned().await else {
            return Err(FetchMiss::NotServed);
        };
        if let Ok(Some(data)) = tokio::time::timeout(
            sched.limits.fetch_timeout,
            crate::api::find_fragment_via(transport, hash, &rest, None),
        )
        .await
        {
            return Ok(Fetched { data, slot: global });
        }
    }
    Err(miss_kind(!attested.is_empty(), any_answer, busy))
}

/// Why a class was not fetched, from what its holders did. NotServed
/// (rebuild) only on a real "not here" reply or a content mismatch, and
/// only when no holder was merely busy; a known holder that was busy,
/// parked or failing at the transport is Unreachable (retry later): the
/// bytes exist, the holder just has not answered.
pub fn miss_kind(has_holders: bool, any_answer: bool, busy: bool) -> FetchMiss {
    if busy || (has_holders && !any_answer) {
        FetchMiss::Unreachable
    } else {
        FetchMiss::NotServed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::StoreResult;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Peer {
        Serves,
        Down,
        Hangs,
    }

    /// Peers by node id; serving peers hold every fragment. Counts fetches
    /// in flight, globally and per peer, with their peaks.
    struct Net {
        peers: HashMap<i32, Peer>,
        delay: Duration,
        in_flight: AtomicUsize,
        peak: AtomicUsize,
        per_peer: Mutex<HashMap<i32, (usize, usize)>>,
    }

    impl Net {
        fn new(peers: &[(i32, Peer)], delay_ms: u64) -> Arc<Self> {
            Arc::new(Net {
                peers: peers.iter().copied().collect(),
                delay: Duration::from_millis(delay_ms),
                in_flight: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
                per_peer: Mutex::new(HashMap::new()),
            })
        }

        fn peer_peak(&self, node_id: i32) -> usize {
            self.per_peer
                .lock()
                .unwrap()
                .get(&node_id)
                .map_or(0, |(_, peak)| *peak)
        }
    }

    fn data_for(hash: &Blake3Hash) -> Vec<u8> {
        hash.as_bytes().to_vec()
    }

    fn hash_of(n: u32) -> (Blake3Hash, Vec<u8>) {
        let data = format!("fragment-{n}").into_bytes();
        (Blake3Hash::new(blake3::hash(&data)), data)
    }

    impl Transport for Net {
        async fn store_fragment(
            &self,
            _peer: &PeerRef,
            _fragment_hash: &Blake3Hash,
            _data: Vec<u8>,
        ) -> Result<StoreResult, TransportError> {
            Err(TransportError::Transport("not used".into()))
        }
        async fn fetch_fragment(
            &self,
            peer: &PeerRef,
            fragment_hash: &Blake3Hash,
        ) -> Result<Vec<u8>, TransportError> {
            let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(now, Ordering::SeqCst);
            {
                let mut map = self.per_peer.lock().unwrap();
                let e = map.entry(peer.node_id).or_insert((0, 0));
                e.0 += 1;
                e.1 = e.1.max(e.0);
            }
            let behaviour = self.peers.get(&peer.node_id).copied().unwrap_or(Peer::Down);
            if behaviour == Peer::Hangs {
                std::future::pending::<()>().await;
            }
            tokio::time::sleep(self.delay).await;
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            self.per_peer
                .lock()
                .unwrap()
                .get_mut(&peer.node_id)
                .unwrap()
                .0 -= 1;
            match behaviour {
                Peer::Serves => Ok(FRAGMENTS
                    .lock()
                    .unwrap()
                    .as_ref()
                    .and_then(|m| m.get(fragment_hash).cloned())
                    .unwrap_or_else(|| data_for(fragment_hash))),
                _ => Err(TransportError::Transport("connection refused".into())),
            }
        }
        async fn fragment_health(
            &self,
            peer: &PeerRef,
            _fragment_hash: &Blake3Hash,
        ) -> Result<bool, TransportError> {
            match self.peers.get(&peer.node_id) {
                Some(Peer::Serves) => Ok(true),
                _ => Err(TransportError::Transport("down".into())),
            }
        }
    }

    /// The bytes each test fragment hashes to (a served fetch returns them).
    static FRAGMENTS: Mutex<Option<HashMap<Blake3Hash, Vec<u8>>>> = Mutex::new(None);

    fn register(n: u32) -> Blake3Hash {
        let (hash, data) = hash_of(n);
        FRAGMENTS
            .lock()
            .unwrap()
            .get_or_insert_with(HashMap::new)
            .insert(hash, data);
        hash
    }

    fn peer(node_id: i32) -> PeerRef {
        PeerRef {
            node_id,
            pubkey: [node_id as u8; 32],
        }
    }

    fn limits(global: usize, per_peer: usize, timeout_ms: u64) -> PullLimits {
        PullLimits {
            window: 64,
            fetch_global: global,
            fetch_per_peer: per_peer,
            urgent_reserve: 0,
            fetch_timeout: Duration::from_millis(timeout_ms),
            // Every failure is its own strike in these tests.
            strike_window: Duration::ZERO,
            rebuilds: 1,
        }
    }

    // Should: read each knob from its env var, keeping the default for an
    // unset, empty or non-positive value; the reserve may be zero.
    // Should: give pulls the global cap less the urgent reserve, and each
    // blob a quarter of the global cap.
    #[test]
    fn pull_limits_read_overrides_and_derive_shares() {
        let env: HashMap<&str, &str> = [
            ("HOPNET_PULL_WINDOW", "16"),
            ("HOPNET_PULL_FETCH_GLOBAL", "6"),
            ("HOPNET_PULL_FETCH_PER_PEER", "0"),
            ("HOPNET_PULL_URGENT_RESERVE", "2"),
            ("HOPNET_PULL_REBUILDS", "1"),
        ]
        .into();
        let l = PullLimits::from_lookup(|k| env.get(k).map(|v| v.to_string()));
        assert_eq!(
            (l.window, l.fetch_global, l.fetch_per_peer, l.urgent_reserve),
            (16, 6, 8, 2)
        );
        assert_eq!(l.pull_permits(), 4);
        assert_eq!(l.per_blob(), 2);
        assert_eq!(l.rebuilds, 1);
        assert_eq!(PullLimits::default().rebuilds, 2);
        assert_eq!(PullLimits::from_lookup(|_| None), PullLimits::default());
        assert_eq!(PullLimits::default().per_blob(), 6);
    }

    // Impact: thor (HDD, RAM-pressed) crashed today under lock pressure;
    // the caps are what bound a node's fetch memory and source load.
    // Should not: exceed the global cap or any peer's cap, however many
    // classes are fetched at once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn fetches_never_exceed_global_or_per_peer_caps() {
        let net = Net::new(
            &[(2, Peer::Serves), (3, Peer::Serves), (4, Peer::Serves)],
            20,
        );
        let sched = FetchScheduler::new(limits(5, 2, 2_000));
        let holders = vec![peer(2), peer(3), peer(4)];
        let mut tasks = tokio::task::JoinSet::new();
        for n in 0..40 {
            let hash = register(1000 + n);
            let (net, sched, holders) = (net.clone(), sched.clone(), holders.clone());
            tasks.spawn(async move { fetch_class(&net, &sched, &hash, &holders, &[]).await });
        }
        while let Some(r) = tasks.join_next().await {
            assert!(r.unwrap().is_ok());
        }
        assert!(net.peak.load(Ordering::SeqCst) <= 5, "global cap");
        for node in [2, 3, 4] {
            assert!(net.peer_peak(node) <= 2, "peer {node} cap");
        }
        assert!(
            net.peak.load(Ordering::SeqCst) >= 3,
            "the work actually ran in parallel"
        );
    }

    // Impact: one dark origin stalled the whole pipeline when few blobs
    // were in flight.
    // Should: report a class whose only holders fail at the transport as
    // unreachable, and park that peer after three failures in a row.
    // Should: still fetch a class with a live holder meanwhile.
    #[tokio::test]
    async fn offline_origin_is_unreachable_and_parks() {
        let net = Net::new(&[(2, Peer::Down), (3, Peer::Serves)], 1);
        let sched = FetchScheduler::new(limits(8, 4, 1_000));
        for n in 0..3 {
            let hash = register(2000 + n);
            assert_eq!(
                fetch_class(&net, &sched, &hash, &[peer(2)], &[])
                    .await
                    .err(),
                Some(FetchMiss::Unreachable)
            );
        }
        assert!(sched.peer_parked(2, Instant::now()));
        let live = register(2100);
        assert!(fetch_class(&net, &sched, &live, &[peer(3)], &[])
            .await
            .is_ok());
        assert!(!sched.peer_parked(3, Instant::now()));
    }

    fn strikes(sched: &FetchScheduler, node: i32, from: Instant, n: u64) -> Instant {
        let mut t = from;
        for i in 0..n {
            t = from + Duration::from_secs(10 * i);
            sched.record_peer_failure(node, PeerFailure::Transport, t);
        }
        t
    }

    // Should: park a peer for 30 s on its third strike, double the next
    // park, and clear it on any success.
    #[test]
    fn parked_peer_backs_off_and_recovers_on_success() {
        let sched = FetchScheduler::new(PullLimits::default());
        let t0 = strikes(&sched, 7, Instant::now(), 3);
        assert!(sched.peer_parked(7, t0 + Duration::from_secs(29)));
        assert!(!sched.peer_parked(7, t0 + Duration::from_secs(31)));
        let t1 = strikes(&sched, 7, t0 + Duration::from_secs(31), 3);
        assert!(
            sched.peer_parked(7, t1 + Duration::from_secs(59)),
            "doubled"
        );
        sched.record_peer_success(7, t1);
        assert!(!sched.peer_parked(7, t1));
    }

    // Impact: review of #96 — eight concurrent timeouts from one peer
    // tripped the three-strike park at once, and the count survived the
    // park, so one failure after expiry re-parked the peer.
    // Should: count a burst of failures inside the strike window as one
    // strike; start the count over when a park expires.
    // Should not: park a peer for one burst, or for a single failure after
    // its park ran out.
    #[test]
    fn a_failure_burst_is_one_strike_and_park_expiry_resets_the_count() {
        let sched = FetchScheduler::new(PullLimits::default());
        let t0 = Instant::now();
        for _ in 0..8 {
            sched.record_peer_failure(4, PeerFailure::Transport, t0);
        }
        assert!(!sched.peer_parked(4, t0), "one burst, one strike");

        let t1 = strikes(&sched, 5, t0, 3);
        assert!(sched.peer_parked(5, t1));
        let after = t1 + Duration::from_secs(31);
        sched.record_peer_failure(5, PeerFailure::Transport, after);
        assert!(!sched.peer_parked(5, after), "count restarted after expiry");
    }

    // Impact: thor's HDD serves slowly; its peers must not park it for
    // timing out while it is still serving.
    // Should: throttle a peer that timed out soon after serving to one
    // fetch at a time.
    // Should not: park it, however many timeouts follow.
    #[test]
    fn a_slow_peer_that_still_serves_is_throttled_not_parked() {
        let sched = FetchScheduler::new(PullLimits::default());
        let t0 = Instant::now();
        sched.record_peer_success(1, t0);
        for i in 1..=6 {
            sched.record_peer_failure(1, PeerFailure::Timeout, t0 + Duration::from_secs(10 * i));
        }
        let t = t0 + Duration::from_secs(60);
        assert!(!sched.peer_parked(1, t));
        assert!(sched.peer_slow(1, t));
        assert!(!sched.peer_slow(1, t + PEER_SLOW_FOR));

        // A peer that never served is not "slow": its timeouts strike.
        let t1 = t0;
        for i in 0..3 {
            sched.record_peer_failure(2, PeerFailure::Timeout, t1 + Duration::from_secs(10 * i));
        }
        assert!(sched.peer_parked(2, t1 + Duration::from_secs(20)));
    }

    // Impact: third review of #96 — waiting for a rebuild slot used the
    // doubling unreachable backoff, so rebuild-needing (often at-risk)
    // blobs sat out minutes while slots idled.
    // Should: hold a blob out for a flat short wait each time no rebuild
    // slot is free.
    // Should not: grow that wait, or bump the unreachable backoff.
    #[test]
    fn waiting_for_a_rebuild_slot_is_a_short_flat_park() {
        use std::str::FromStr;
        let sched = FetchScheduler::new(PullLimits::default());
        let blob = BlobId::from_str("01890a5d-ac96-774b-b9aa-9f8b24f0c901").unwrap();
        let t0 = Instant::now();
        for i in 0..4 {
            let t = t0 + Duration::from_secs(100 * i);
            sched.park_blob_for_rebuild(&blob, t);
            assert!(sched.blob_parked(&blob, t + REBUILD_WAIT_PARK - Duration::from_secs(1)));
            assert!(!sched.blob_parked(&blob, t + REBUILD_WAIT_PARK));
        }
        // The unreachable backoff still starts from its base.
        let t = t0 + Duration::from_secs(1000);
        sched.park_blob(&blob, t);
        assert!(!sched.blob_parked(&blob, t + BLOB_PARK_BASE));
    }

    // Impact: re-review of #96 — park entries for blobs that became
    // quiescent or were deleted while parked were never removed.
    // Should: drop an entry whose park ended more than an hour ago.
    // Should not: drop one still parked, or recently ended (a stuck blob
    // re-parked on retry keeps its entry).
    #[test]
    fn stale_park_entries_are_pruned() {
        use std::str::FromStr;
        let sched = FetchScheduler::new(PullLimits::default());
        let (stale, fresh) = (
            BlobId::from_str("01890a5d-ac96-774b-b9aa-9f8b24f0c901").unwrap(),
            BlobId::from_str("01890a5d-ac96-774b-b9aa-9f8b24f0c902").unwrap(),
        );
        let t0 = Instant::now();
        sched.park_blob(&stale, t0);
        let later = t0 + BLOB_PARK_BASE + PARK_FORGET + Duration::from_secs(1);
        sched.park_blob(&fresh, later - Duration::from_secs(10));
        assert_eq!(sched.prune_parks(later), 1);
        assert!(sched.blob_parked_for(&stale, later).is_none());
        assert!(sched.blob_parked_for(&fresh, later).is_some());
    }

    // Should: hold a parked blob out until its backoff passes, double the
    // backoff on a repeat, and forget it once unparked.
    #[test]
    fn parked_blob_is_reoffered_after_its_backoff() {
        use std::str::FromStr;
        let sched = FetchScheduler::new(PullLimits::default());
        let blob = BlobId::from_str("01890a5d-ac96-774b-b9aa-9f8b24f0c901").unwrap();
        let t0 = Instant::now();
        sched.park_blob(&blob, t0);
        assert!(sched.blob_parked(&blob, t0 + Duration::from_secs(59)));
        assert!(!sched.blob_parked(&blob, t0 + Duration::from_secs(61)));
        let t1 = t0 + Duration::from_secs(61);
        sched.park_blob(&blob, t1);
        assert!(sched.blob_parked(&blob, t1 + Duration::from_secs(119)));
        assert_eq!(sched.stats(t1).parked_blobs, 1);
        sched.unpark_blob(&blob);
        assert!(!sched.blob_parked(&blob, t1));
    }

    // Impact: review of #96 — fragment buffers outlived their global slot
    // while the blocking store ran, so the cap no longer bounded the bytes
    // held in memory.
    // Should: keep a fetch's global slot taken until its bytes are dropped
    // (stored), and free it after.
    #[tokio::test]
    async fn fetched_bytes_hold_their_global_slot_until_dropped() {
        let net = Net::new(&[(3, Peer::Serves)], 1);
        let sched = FetchScheduler::new(limits(4, 2, 1_000));
        let hash = register(4000);
        let fetched = fetch_class(&net, &sched, &hash, &[peer(3)], &[])
            .await
            .ok()
            .unwrap();
        assert_eq!(sched.stats(Instant::now()).fetches_in_flight, 1);
        drop(fetched);
        assert_eq!(sched.stats(Instant::now()).fetches_in_flight, 0);
    }

    // Impact: review of #96 — a class held only by a slow peer (thor)
    // whose one-at-a-time gate was taken came back NotServed and was
    // rebuilt from shards instead of being fetched later.
    // Should: report a class as Unreachable (retry later) when its holder
    // could not be given a slot in time.
    // Should: report NotServed only on a real "not here" reply or bad
    // bytes, with no holder merely busy.
    #[tokio::test]
    async fn a_busy_or_gated_holder_is_retried_not_rebuilt() {
        let net = Net::new(&[(2, Peer::Serves)], 1);
        let sched = FetchScheduler::new(limits(4, 2, 50));
        let t0 = Instant::now();
        sched.record_peer_success(2, t0);
        sched.record_peer_failure(2, PeerFailure::Timeout, t0);
        assert!(sched.peer_slow(2, Instant::now()));
        let _held = sched.slow_gate(2).acquire_owned().await.unwrap();
        let hash = register(5000);
        let got = fetch_class(&net, &sched, &hash, &[peer(2)], &[]).await;
        assert_eq!(got.err(), Some(FetchMiss::Unreachable));

        assert_eq!(
            miss_kind(true, true, true),
            FetchMiss::Unreachable,
            "busy wins"
        );
        assert_eq!(
            miss_kind(true, false, false),
            FetchMiss::Unreachable,
            "no answer"
        );
        assert_eq!(
            miss_kind(true, true, false),
            FetchMiss::NotServed,
            "replied not here"
        );
        assert_eq!(
            miss_kind(false, false, false),
            FetchMiss::NotServed,
            "no holder known"
        );
    }

    // Should: give up on a fetch at its deadline and release its global
    // and per-peer slots.
    // Should not: let a hung peer hold slots past the deadline.
    #[tokio::test]
    async fn hung_fetch_releases_its_slots_at_the_deadline() {
        let net = Net::new(&[(2, Peer::Hangs)], 1);
        let sched = FetchScheduler::new(limits(4, 2, 50));
        let hash = register(3000);
        let started = Instant::now();
        let got = fetch_class(&net, &sched, &hash, &[peer(2)], &[]).await;
        assert_eq!(got.err(), Some(FetchMiss::Unreachable));
        assert!(started.elapsed() < Duration::from_secs(2));
        let stats = sched.stats(Instant::now());
        assert_eq!(stats.fetches_in_flight, 0);
        assert!(stats.per_peer_in_flight.is_empty());
    }
}
