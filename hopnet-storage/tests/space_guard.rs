//! The replica-write floor (`admission::SpaceGuard`) through its public
//! surface: regressions from the reviews of #100.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use hopnet_storage::admission::{PullFloor, SpaceGuard, WriteClass};

const GIB: u64 = 1024 * 1024 * 1024;
const MB: u64 = 1024 * 1024;

/// A guard over a fake 927 GiB volume with `free` bytes free and a probe
/// that fails while `fail` is set; ingest floor 10 GiB, default pull floor.
fn guard(free: Arc<AtomicU64>, fail: Arc<AtomicBool>) -> Arc<SpaceGuard> {
    SpaceGuard::new(
        PullFloor::default(),
        Some(10 * GIB),
        Box::new(move |_| {
            if fail.load(Ordering::Acquire) {
                Err(std::io::Error::from_raw_os_error(5)) // EIO
            } else {
                Ok((free.load(Ordering::Acquire), 927 * GIB))
            }
        }),
        Box::leak(Box::new(AtomicU64::new(0))),
    )
}

// Impact: second review of #100 — clamping the marks to a small volume
// pushed the pull floor below the ingest floor (8 GB on a 32 GB volume),
// so pulls kept filling the disk after uploads were already refused.
// Should: keep the pull floor at or above the ingest floor on a 32 GB
// volume, with the resume mark at or above the pull floor and inside the
// volume.
#[test]
fn the_pull_floor_never_drops_below_the_ingest_floor() {
    let total = 32_000_000_000u64;
    let ingest = 10 * GIB;
    let (pull, resume, clamped) = PullFloor::default().marks_clamped(total, ingest);
    assert!(clamped);
    assert!(pull >= ingest, "pull {pull} < ingest {ingest}");
    assert!(resume >= pull && resume < total, "{pull} {resume}");
}

// Impact: second review of #100 — a probe failure paused the guard, and
// only the resume mark cleared it, so one transient EIO held a node with
// 25 GiB free (between a 20 GiB floor and a 30 GiB resume mark) back
// indefinitely.
// Should: resume on the next good probe that reads above the pull floor.
#[test]
fn one_probe_error_does_not_hold_a_node_back() {
    let free = Arc::new(AtomicU64::new(25 * GIB));
    let fail = Arc::new(AtomicBool::new(true));
    let g = guard(free, fail.clone());
    assert!(g.reserve("/x", MB, WriteClass::Pull).is_err());
    assert!(g.paused());
    fail.store(false, Ordering::Release);
    assert!(g.reprobe("/x"), "a good probe above the floor clears it");
    assert!(g.reserve("/x", MB, WriteClass::Pull).is_ok());
}

// Should: keep a low-space pause until the resume mark, as before.
#[test]
fn a_low_space_pause_still_waits_for_the_resume_mark() {
    let free = Arc::new(AtomicU64::new(15 * GIB));
    let g = guard(free.clone(), Arc::new(AtomicBool::new(false)));
    assert!(g.reserve("/x", MB, WriteClass::Pull).is_err());
    free.store(25 * GIB, Ordering::Release);
    assert!(!g.reprobe("/x"));
    free.store(31 * GIB, Ordering::Release);
    assert!(g.reprobe("/x"));
}

// Impact: second review of #100 — `would_admit` is a read-only question,
// but its probe-error path paused the guard, so a re-encode merely asking
// held every pull on the node back.
// Should not: change the guard's state when its probe fails.
#[test]
fn would_admit_never_changes_the_guard() {
    let g = guard(
        Arc::new(AtomicU64::new(500 * GIB)),
        Arc::new(AtomicBool::new(true)),
    );
    let _ = g.would_admit("/x", MB, WriteClass::Pull);
    let _ = g.would_admit("/x", MB, WriteClass::Repair);
    assert!(!g.paused());
}
