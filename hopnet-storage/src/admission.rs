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
//!
//! Replica writes (pulls, pull-path rebuilds, re-encodes, the inbound store
//! arm) go through the [`SpaceGuard`], a second, higher floor on the same
//! counter: the ladder is ingest floor < pull floor < resume mark, so
//! pulls stop first and leave the headroom to the node's own new data. A
//! node below the pull floor keeps serving; it only stops taking on copies
//! until free space is back at the resume mark (2026-10-03: the macbook
//! pulled its volume from 2 GB free to 116 MB in a minute).

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

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

const GIB: u64 = 1024 * 1024 * 1024;

/// Pull floor default: max(20 GiB, 2% of the volume).
pub const DEFAULT_PULL_MIN_FREE_BYTES: u64 = 20 * GIB;
pub const DEFAULT_PULL_MIN_FREE_BASIS_POINTS: u64 = 200;
/// Resume gap default: max(10 GiB, 1% of the volume) above the pull floor.
pub const DEFAULT_PULL_RESUME_GAP_BYTES: u64 = 10 * GIB;
pub const DEFAULT_PULL_RESUME_GAP_BASIS_POINTS: u64 = 100;
/// While paused, a reminder WARN at most this often.
pub const PAUSED_REMINDER: Duration = Duration::from_secs(1800);

/// Disk space on file at the time of a write: the largest fragment file
/// one class can occupy (payload, AEAD overhead, block rounding).
pub fn fragment_file_bytes(payload: usize) -> u64 {
    (payload as u64 + FRAGMENT_OVERHEAD).div_ceil(FILE_BLOCK) * FILE_BLOCK
}

/// Whether a write failed because the filesystem is full.
pub fn is_disk_full(e: &StorageError) -> bool {
    match e {
        StorageError::InsufficientSpace { .. } => true,
        StorageError::Io(io) => {
            io.kind() == std::io::ErrorKind::StorageFull || io.raw_os_error() == Some(28)
        }
        _ => false,
    }
}

/// Which floor a replica write answers to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteClass {
    /// Pulls, pull-path rebuilds, lazy re-encode, the inbound store arm:
    /// stop at the pull floor and wait for the resume mark.
    Pull,
    /// Urgent re-encode (a chunk below the watermark): may use the reserve
    /// between the pull floor and the ingest floor, never below it, and
    /// never pauses anything.
    Repair,
}

/// The pull floor knobs (`HOPNET_PULL_MIN_FREE_BYTES`,
/// `HOPNET_PULL_MIN_FREE_PCT`, `HOPNET_PULL_RESUME_FREE_BYTES`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PullFloor {
    /// Absolute floor; 0 disables the guard.
    pub min_free_bytes: u64,
    /// Floor as a share of the volume, in basis points (200 = 2%).
    pub min_free_basis_points: u64,
    /// Resume gap above the floor; `None` = max(10 GiB, 1%).
    pub resume_gap_bytes: Option<u64>,
}

impl Default for PullFloor {
    fn default() -> Self {
        PullFloor {
            min_free_bytes: DEFAULT_PULL_MIN_FREE_BYTES,
            min_free_basis_points: DEFAULT_PULL_MIN_FREE_BASIS_POINTS,
            resume_gap_bytes: None,
        }
    }
}

impl PullFloor {
    /// No guard: library tests and tools that never configure one.
    pub const DISABLED: PullFloor = PullFloor {
        min_free_bytes: 0,
        min_free_basis_points: 0,
        resume_gap_bytes: Some(0),
    };

    pub fn from_env() -> Self {
        Self::from_lookup(|k| std::env::var(k).ok())
    }

    /// The defaults, overridden where a knob parses. `HOPNET_PULL_MIN_FREE_BYTES=0`
    /// disables the guard.
    pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Self {
        let d = Self::default();
        let bytes = |k: &str| get(k).and_then(|v| v.trim().parse::<u64>().ok());
        let min_free_bytes = bytes("HOPNET_PULL_MIN_FREE_BYTES").unwrap_or(d.min_free_bytes);
        if min_free_bytes == 0 {
            return Self::DISABLED;
        }
        let min_free_basis_points = get("HOPNET_PULL_MIN_FREE_PCT")
            .and_then(|v| v.trim().parse::<f64>().ok())
            .filter(|p| p.is_finite() && *p >= 0.0 && *p < 100.0)
            .map(|p| (p * 100.0).round() as u64)
            .unwrap_or(d.min_free_basis_points);
        PullFloor {
            min_free_bytes,
            min_free_basis_points,
            resume_gap_bytes: bytes("HOPNET_PULL_RESUME_FREE_BYTES"),
        }
    }

    pub fn enabled(&self) -> bool {
        self.min_free_bytes > 0
    }

    /// (pull floor, resume mark) for a volume of `total` bytes. The pull
    /// floor never sits below the ingest floor.
    pub fn marks(&self, total: u64, ingest_floor: u64) -> (u64, u64) {
        let (floor, resume, _) = self.marks_clamped(total, ingest_floor);
        (floor, resume)
    }

    /// [`PullFloor::marks`], and whether the volume clamped them. Unclamped,
    /// any volume of ~30 GiB or less got a resume mark at or above its own
    /// size, so a guard that paused there never resumed, even empty. The
    /// floor is held to a quarter of the volume and the resume mark to half.
    pub fn marks_clamped(&self, total: u64, ingest_floor: u64) -> (u64, u64, bool) {
        let share = |bp: u64| (total as u128 * bp as u128 / 10_000) as u64;
        let floor = self
            .min_free_bytes
            .max(share(self.min_free_basis_points))
            .max(ingest_floor);
        let gap = self.resume_gap_bytes.unwrap_or_else(|| {
            DEFAULT_PULL_RESUME_GAP_BYTES.max(share(DEFAULT_PULL_RESUME_GAP_BASIS_POINTS))
        });
        let resume = floor.saturating_add(gap);
        let (floor_cap, resume_cap) = (total / 4, total / 2);
        let clamped = floor > floor_cap || resume > resume_cap;
        (floor.min(floor_cap), resume.min(resume_cap), clamped)
    }
}

/// Log the pull floor's marks for the volume under `dir` at boot: INFO,
/// or a WARN when the volume is too small for the configured floor and the
/// marks were clamped to it.
pub fn log_pull_floor_marks(dir: &str) {
    let floor = SpaceGuard::global().pull_floor();
    if !floor.enabled() {
        return;
    }
    match statvfs_probe(Path::new(dir)) {
        Ok((free, total)) => {
            let (pull, resume, clamped) = floor.marks_clamped(total, min_free_bytes());
            if clamped {
                tracing::warn!(
                    total_bytes = total,
                    pull_floor_bytes = pull,
                    resume_bytes = resume,
                    "pull floor: volume too small for the configured floor; marks clamped to it"
                );
            } else {
                tracing::info!(
                    free_bytes = free,
                    pull_floor_bytes = pull,
                    resume_bytes = resume,
                    "pull floor marks"
                );
            }
        }
        Err(e) => tracing::warn!("pull floor: cannot read free space of {dir}: {e}"),
    }
}

/// Reads (free, total) bytes of the volume holding a path.
pub type SpaceProbe = dyn Fn(&Path) -> std::io::Result<(u64, u64)> + Send + Sync;

fn statvfs_probe(path: &Path) -> std::io::Result<(u64, u64)> {
    let stats = fs4::statvfs(path)?;
    Ok((stats.available_space(), stats.total_space()))
}

#[derive(Debug, Default)]
struct SpaceState {
    /// Paused since (monotonic, unix seconds); `None` = open.
    paused: Option<(Instant, i64)>,
    last_reminder: Option<Instant>,
    /// The latest probe: (free, total).
    observed: Option<(u64, u64)>,
    /// Last free-space probe failure WARN (rate-limited).
    last_probe_warn: Option<Instant>,
}

/// Probe-failure WARNs at most this often.
pub const PROBE_WARN_EVERY: Duration = Duration::from_secs(300);

/// The state of the guard, for the planner report.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct SpaceReport {
    pub enabled: bool,
    /// The latest probe; `None` before the first replica write.
    pub free_bytes: Option<u64>,
    pub total_bytes: Option<u64>,
    pub reserved_bytes: u64,
    pub ingest_floor_bytes: u64,
    pub pull_floor_bytes: Option<u64>,
    pub resume_bytes: Option<u64>,
    /// Unix seconds the guard paused pulls; `None` = taking copies.
    pub paused_since: Option<i64>,
}

/// The replica-write floor: one per process in production
/// ([`SpaceGuard::global`]), configured at boot; tests build their own
/// with a fake probe and counter.
pub struct SpaceGuard {
    floor: Mutex<PullFloor>,
    /// `None` = the process-wide ingest floor ([`min_free_bytes`]).
    ingest_floor: Option<u64>,
    probe: Box<SpaceProbe>,
    counter: &'static AtomicU64,
    state: Mutex<SpaceState>,
}

impl std::fmt::Debug for SpaceGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpaceGuard")
            .field("floor", &self.pull_floor())
            .field("paused", &self.paused())
            .finish_non_exhaustive()
    }
}

impl SpaceGuard {
    pub fn new(
        floor: PullFloor,
        ingest_floor: Option<u64>,
        probe: Box<SpaceProbe>,
        counter: &'static AtomicU64,
    ) -> Arc<Self> {
        Arc::new(SpaceGuard {
            floor: Mutex::new(floor),
            ingest_floor,
            probe,
            counter,
            state: Mutex::new(SpaceState::default()),
        })
    }

    /// The process guard: statvfs, the shared ingest counter, disabled
    /// until [`configure_pull_floor`].
    pub fn global() -> &'static Arc<SpaceGuard> {
        static GUARD: OnceLock<Arc<SpaceGuard>> = OnceLock::new();
        GUARD.get_or_init(|| {
            SpaceGuard::new(
                PullFloor::DISABLED,
                None,
                Box::new(statvfs_probe),
                &RESERVED,
            )
        })
    }

    fn pull_floor(&self) -> PullFloor {
        *self.floor.lock().unwrap()
    }

    fn ingest_floor(&self) -> u64 {
        self.ingest_floor.unwrap_or_else(min_free_bytes)
    }

    /// Whether replica writes are paused (below the pull floor, not yet
    /// back at the resume mark).
    pub fn paused(&self) -> bool {
        self.state.lock().unwrap().paused.is_some()
    }

    /// Reserve room for a replica write of `bytes` under `dir`, or refuse.
    /// Hold the reservation until the bytes are on disk. A pull-class
    /// refusal pauses the guard; while paused, pull-class writes are
    /// refused until [`SpaceGuard::reprobe`] (or this call) sees the
    /// resume mark.
    pub fn reserve(
        &self,
        dir: &str,
        bytes: u64,
        class: WriteClass,
    ) -> Result<IngestReservation, StorageError> {
        let floor = self.pull_floor();
        if !floor.enabled() {
            return Ok(IngestReservation {
                bytes: 0,
                counter: self.counter,
            });
        }
        let (free, total) = self.probe_or_hold(dir)?;
        let ingest = self.ingest_floor();
        let (pull, resume) = floor.marks(total, ingest);
        let reserved = self.counter.load(Ordering::Acquire);
        let mut state = self.state.lock().unwrap();
        state.observed = Some((free, total));
        match class {
            WriteClass::Repair => try_reserve(self.counter, free, bytes, ingest),
            WriteClass::Pull => {
                if state.paused.is_some() {
                    if free.saturating_sub(reserved) < resume {
                        Self::remind(&mut state, free, pull, resume);
                        return Err(StorageError::InsufficientSpace {
                            free: free.saturating_sub(reserved),
                            needed: bytes,
                            floor: resume,
                        });
                    }
                    Self::resume(&mut state, free);
                }
                let granted = try_reserve(self.counter, free, bytes, pull);
                if granted.is_err() {
                    Self::pause(&mut state, free, pull, resume);
                }
                granted
            }
        }
    }

    /// Probe free space. A probe failure fails safe: the guard pauses (an
    /// unreadable volume is not one to keep writing replicas to), a
    /// rate-limited WARN says why, and the error is returned so the write
    /// is refused.
    fn probe_or_hold(&self, dir: &str) -> Result<(u64, u64), StorageError> {
        match (self.probe)(Path::new(dir)) {
            Ok(probed) => Ok(probed),
            Err(e) => {
                let mut state = self.state.lock().unwrap();
                Self::warn_probe(&mut state, dir, &e);
                if state.paused.is_none() {
                    let (free, total) = state.observed.unwrap_or((0, 0));
                    let (pull, resume) = self.pull_floor().marks(total, self.ingest_floor());
                    Self::pause(&mut state, free, pull, resume);
                }
                Err(StorageError::Io(e))
            }
        }
    }

    fn warn_probe(state: &mut SpaceState, dir: &str, e: &std::io::Error) {
        let now = Instant::now();
        if state
            .last_probe_warn
            .is_some_and(|t| now.saturating_duration_since(t) < PROBE_WARN_EVERY)
        {
            return;
        }
        state.last_probe_warn = Some(now);
        tracing::warn!(
            "storage: cannot read free space of {dir}: {e}; holding back replica writes"
        );
    }

    /// Would a replica write of `bytes` be admitted right now? Read-only
    /// (reserves nothing, never pauses): a re-encode asks before gathering
    /// K shards, so a refusal costs no download. A probe failure answers no.
    pub fn would_admit(&self, dir: &str, bytes: u64, class: WriteClass) -> bool {
        let floor = self.pull_floor();
        if !floor.enabled() {
            return true;
        }
        if class == WriteClass::Pull && self.paused() {
            return false;
        }
        let Ok((free, total)) = self.probe_or_hold(dir) else {
            return false;
        };
        let ingest = self.ingest_floor();
        let limit = match class {
            WriteClass::Repair => ingest,
            WriteClass::Pull => floor.marks(total, ingest).0,
        };
        fits(free, self.counter.load(Ordering::Acquire), bytes, limit).is_ok()
    }

    /// A write failed with the disk full (another writer took the space
    /// under us): pause as if the floor had refused it.
    pub fn note_disk_full(&self) {
        let floor = self.pull_floor();
        if !floor.enabled() {
            return;
        }
        let mut state = self.state.lock().unwrap();
        let (free, total) = state.observed.unwrap_or((0, 0));
        let (pull, resume) = floor.marks(total, self.ingest_floor());
        Self::pause(&mut state, free, pull, resume);
    }

    /// While paused: probe again and resume at the resume mark. Returns
    /// whether replica writes are open.
    pub fn reprobe(&self, dir: &str) -> bool {
        let floor = self.pull_floor();
        if !floor.enabled() {
            self.state.lock().unwrap().paused = None;
            return true;
        }
        let probed = (self.probe)(Path::new(dir));
        let mut state = self.state.lock().unwrap();
        if state.paused.is_none() {
            return true;
        }
        let (free, total) = match probed {
            Ok(probed) => probed,
            // Stay paused (fail safe), and say why.
            Err(e) => {
                Self::warn_probe(&mut state, dir, &e);
                return false;
            }
        };
        state.observed = Some((free, total));
        let (pull, resume) = floor.marks(total, self.ingest_floor());
        let reserved = self.counter.load(Ordering::Acquire);
        if free.saturating_sub(reserved) >= resume {
            Self::resume(&mut state, free);
            true
        } else {
            Self::remind(&mut state, free, pull, resume);
            false
        }
    }

    pub fn report(&self) -> SpaceReport {
        let floor = self.pull_floor();
        let state = self.state.lock().unwrap();
        let ingest = self.ingest_floor();
        let marks = state
            .observed
            .filter(|_| floor.enabled())
            .map(|(_, total)| floor.marks(total, ingest));
        SpaceReport {
            enabled: floor.enabled(),
            free_bytes: state.observed.map(|o| o.0),
            total_bytes: state.observed.map(|o| o.1),
            reserved_bytes: self.counter.load(Ordering::Acquire),
            ingest_floor_bytes: ingest,
            pull_floor_bytes: marks.map(|m| m.0),
            resume_bytes: marks.map(|m| m.1),
            paused_since: state.paused.map(|p| p.1),
        }
    }

    fn pause(state: &mut SpaceState, free: u64, pull: u64, resume: u64) {
        if state.paused.is_some() {
            return;
        }
        let now = Instant::now();
        let unix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs() as i64);
        state.paused = Some((now, unix));
        state.last_reminder = Some(now);
        tracing::warn!(
            free_bytes = free,
            pull_floor_bytes = pull,
            resume_bytes = resume,
            "storage: below the pull floor; holding back pulls and re-encodes (still serving)"
        );
    }

    fn resume(state: &mut SpaceState, free: u64) {
        let held_secs = state.paused.map_or(0, |p| p.0.elapsed().as_secs());
        state.paused = None;
        state.last_reminder = None;
        tracing::info!(
            free_bytes = free,
            held_secs,
            "storage: back at the resume mark; pulls resume"
        );
    }

    fn remind(state: &mut SpaceState, free: u64, pull: u64, resume: u64) {
        let now = Instant::now();
        if state
            .last_reminder
            .is_some_and(|t| now.saturating_duration_since(t) < PAUSED_REMINDER)
        {
            return;
        }
        state.last_reminder = Some(now);
        tracing::warn!(
            free_bytes = free,
            pull_floor_bytes = pull,
            resume_bytes = resume,
            "storage: still holding back pulls for space"
        );
    }
}

/// Enable the process guard's pull floor (the host does this at boot).
pub fn configure_pull_floor(floor: PullFloor) {
    *SpaceGuard::global().floor.lock().unwrap() = floor;
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

    const TB: u64 = 1024 * GIB;

    /// A guard over a settable fake volume: (free, total) behind atomics,
    /// its own reservation counter, ingest floor 10 GiB.
    fn fake_guard(free: u64, total: u64) -> (Arc<SpaceGuard>, Arc<AtomicU64>, &'static AtomicU64) {
        let free_cell = Arc::new(AtomicU64::new(free));
        let read = free_cell.clone();
        let c = counter();
        let guard = SpaceGuard::new(
            PullFloor::default(),
            Some(DEFAULT_MIN_FREE_BYTES),
            Box::new(move |_| Ok((read.load(Ordering::Acquire), total))),
            c,
        );
        (guard, free_cell, c)
    }

    // Should: put the pull floor at max(20 GiB, 2%) and the resume mark
    // max(10 GiB, 1%) above it.
    // Should: never put the pull floor below the ingest floor.
    #[test]
    fn marks_scale_with_the_volume() {
        let d = PullFloor::default();
        assert_eq!(d.marks(927 * GIB, 10 * GIB), (20 * GIB, 30 * GIB));
        let (floor, resume) = d.marks(12 * TB, 10 * GIB);
        assert_eq!(floor, 12 * TB / 50);
        assert_eq!(resume, floor + 12 * TB / 100);
        assert_eq!(d.marks(TB, 40 * GIB).0, 40 * GIB);
    }

    // Should: read the floor, share and resume gap from their knobs.
    // Should: disable the guard when the byte floor is 0.
    #[test]
    fn knobs_parse_and_zero_disables() {
        let f = PullFloor::from_lookup(|k| match k {
            "HOPNET_PULL_MIN_FREE_BYTES" => Some("1000".into()),
            "HOPNET_PULL_MIN_FREE_PCT" => Some("5".into()),
            "HOPNET_PULL_RESUME_FREE_BYTES" => Some("77".into()),
            _ => None,
        });
        assert_eq!(f.min_free_bytes, 1000);
        assert_eq!(f.min_free_basis_points, 500);
        assert_eq!(f.resume_gap_bytes, Some(77));
        let off =
            PullFloor::from_lookup(|k| (k == "HOPNET_PULL_MIN_FREE_BYTES").then(|| "0".into()));
        assert!(!off.enabled());
        assert_eq!(PullFloor::from_lookup(|_| None), PullFloor::default());
    }

    // Should: refuse a pull reservation that would leave the pull floor or
    // less, and pause.
    // Should: still admit an ingest-sized reservation between the floors.
    #[test]
    fn pulls_stop_above_the_ingest_floor() {
        let (guard, _, c) = fake_guard(25 * GIB, 927 * GIB);
        assert!(guard.reserve("/x", 4 * GIB, WriteClass::Pull).is_ok());
        assert!(!guard.paused());
        assert!(guard.reserve("/x", 6 * GIB, WriteClass::Pull).is_err());
        assert!(guard.paused());
        assert!(try_reserve(c, 25 * GIB, 6 * GIB, DEFAULT_MIN_FREE_BYTES).is_ok());
    }

    // Impact: hysteresis — a node at the floor would otherwise flap
    // between pulling and holding back on every surplus release.
    // Should: stay paused while free space is between the floor and the
    // resume mark.
    // Should: resume once free space reaches the resume mark.
    #[test]
    fn paused_holds_until_the_resume_mark() {
        let (guard, free, _) = fake_guard(19 * GIB, 927 * GIB);
        assert!(guard.reserve("/x", MB, WriteClass::Pull).is_err());
        assert!(guard.paused());
        free.store(25 * GIB, Ordering::Release);
        assert!(!guard.reprobe("/x"));
        assert!(guard.reserve("/x", MB, WriteClass::Pull).is_err());
        free.store(31 * GIB, Ordering::Release);
        assert!(guard.reprobe("/x"));
        assert!(guard.reserve("/x", MB, WriteClass::Pull).is_ok());
        assert!(guard.report().paused_since.is_none());
    }

    // Impact: a pull burst and an upload must not both pass against one
    // free-space reading.
    // Should: count a held pull reservation against an ingest on the same
    // counter.
    #[test]
    fn pulls_and_ingest_share_one_counter() {
        let (guard, _, c) = fake_guard(40 * GIB, 927 * GIB);
        let held = guard.reserve("/x", 15 * GIB, WriteClass::Pull).unwrap();
        assert!(try_reserve(c, 40 * GIB, 16 * GIB, DEFAULT_MIN_FREE_BYTES).is_err());
        drop(held);
        assert!(try_reserve(c, 40 * GIB, 16 * GIB, DEFAULT_MIN_FREE_BYTES).is_ok());
    }

    // Should: let urgent repair write into the reserve between the pull
    // floor and the ingest floor, without pausing.
    // Should not: let urgent repair go below the ingest floor.
    #[test]
    fn urgent_repair_may_use_the_reserve_between_floors() {
        let (guard, _, _) = fake_guard(15 * GIB, 927 * GIB);
        assert!(guard.reserve("/x", GIB, WriteClass::Repair).is_ok());
        assert!(!guard.paused());
        assert!(guard.reserve("/x", 6 * GIB, WriteClass::Repair).is_err());
        assert!(guard.reserve("/x", GIB, WriteClass::Pull).is_err());
    }

    // Should not: refuse anything or reserve while the guard is disabled.
    #[test]
    fn a_disabled_guard_admits_everything() {
        let c = counter();
        let guard = SpaceGuard::new(PullFloor::DISABLED, Some(0), Box::new(|_| Ok((0, 1))), c);
        assert!(guard.reserve("/x", TB, WriteClass::Pull).is_ok());
        assert!(!guard.paused());
        assert_eq!(c.load(Ordering::Acquire), 0);
    }

    // Should: recognise ENOSPC and a refused reservation as a full disk.
    // Should not: read other I/O failures as a full disk.
    #[test]
    fn disk_full_is_recognised() {
        assert!(is_disk_full(&StorageError::Io(
            std::io::Error::from_raw_os_error(28)
        )));
        assert!(is_disk_full(&StorageError::InsufficientSpace {
            free: 0,
            needed: 1,
            floor: 1
        }));
        assert!(!is_disk_full(&StorageError::Io(std::io::Error::from(
            std::io::ErrorKind::PermissionDenied
        ))));
    }

    // Impact: review of #100 — unclamped, any volume of ~30 GiB or less got
    // a resume mark at or above its own size (20 GiB floor + 10 GiB gap), so
    // a guard that paused there never resumed, even with the disk empty.
    // Should: clamp the floor to a quarter and the resume mark to half of a
    // 32 GB volume, and say it clamped.
    // Should: resume a paused guard on that volume once it is empty.
    // Should not: clamp the marks of a volume big enough for them.
    #[test]
    fn marks_clamp_to_a_small_volume() {
        let total = 32_000_000_000u64;
        let (floor, resume, clamped) = PullFloor::default().marks_clamped(total, 10 * GIB);
        assert!(clamped);
        assert!(
            floor <= total / 4 && resume <= total / 2,
            "{floor} {resume}"
        );
        assert!(floor < resume);

        let (guard, free, _) = fake_guard(GIB, total);
        assert!(guard.reserve("/x", MB, WriteClass::Pull).is_err());
        assert!(guard.paused());
        free.store(total, Ordering::Release);
        assert!(guard.reprobe("/x"), "an empty disk resumes");

        assert!(!PullFloor::default().marks_clamped(927 * GIB, 10 * GIB).2);
    }

    /// A guard whose probe fails until `fail` is cleared.
    fn failing_guard() -> (Arc<SpaceGuard>, Arc<std::sync::atomic::AtomicBool>) {
        let fail = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let read = fail.clone();
        let guard = SpaceGuard::new(
            PullFloor::default(),
            Some(DEFAULT_MIN_FREE_BYTES),
            Box::new(move |_| {
                if read.load(Ordering::Acquire) {
                    Err(std::io::Error::other("statvfs: I/O error"))
                } else {
                    Ok((TB, TB))
                }
            }),
            counter(),
        );
        (guard, fail)
    }

    // Impact: review of #100 — a free-space probe failure on the pull path
    // was refused without pausing or logging, and a failing re-probe kept
    // the guard paused silently.
    // Should: refuse the write and pause (fail safe) when the probe fails.
    // Should: stay paused while re-probes fail, and resume once the probe
    // reads again.
    // Should not: let a re-encode's read-only check through on a failing
    // probe.
    #[test]
    fn a_failing_probe_holds_writes_back() {
        let (guard, fail) = failing_guard();
        assert!(guard.reserve("/x", MB, WriteClass::Pull).is_err());
        assert!(guard.paused());
        assert!(!guard.reprobe("/x"));
        assert!(!guard.would_admit("/x", MB, WriteClass::Repair));
        fail.store(false, Ordering::Release);
        assert!(guard.reprobe("/x"));
        assert!(guard.reserve("/x", MB, WriteClass::Pull).is_ok());
    }

    // Should: answer a re-encode's pre-check against its own floor: an
    // urgent repair down to the ingest floor, a lazy one only above the
    // pull floor and not while paused.
    #[test]
    fn would_admit_checks_the_class_floor() {
        let (guard, _, _) = fake_guard(15 * GIB, 927 * GIB);
        assert!(guard.would_admit("/x", GIB, WriteClass::Repair));
        assert!(!guard.would_admit("/x", 6 * GIB, WriteClass::Repair));
        assert!(!guard.would_admit("/x", GIB, WriteClass::Pull));
        assert!(!guard.paused(), "a read-only check never pauses");
    }
}
