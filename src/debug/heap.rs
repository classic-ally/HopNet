//! On-demand jemalloc heap introspection for the Linux node.
//!
//! The Linux `hopnet` binary runs on jemalloc (see the `#[global_allocator]`
//! in `src/main.rs`) with profiling compiled in but inactive. These owner-only
//! routes read allocator stats, switch sampling on and off, and dump a pprof
//! heap profile. Other targets keep the system allocator and answer 501.
//!
//! - `GET  /api/debug/heap/stats`     → allocator totals + profiler state
//! - `POST /api/debug/heap/profiling` → `{ "active": bool }`
//! - `GET  /api/debug/heap/profile`   → gzipped pprof (409 while inactive)

use axum::{
    Extension, Json,
    extract::State,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};

use crate::AppState;

/// jemalloc options baked into the Linux binary through its exported
/// `malloc_conf` symbol: profiling compiled in but inactive (no sampling cost
/// until switched on), a 512 KiB average sample interval once active, and
/// background threads that return dirty pages to the OS on decay. The
/// standard `MALLOC_CONF` env var is applied after this and overrides it.
pub const MALLOC_CONF: &std::ffi::CStr =
    c"prof:true,prof_active:false,lg_prof_sample:19,background_thread:true";

/// Allocator totals in bytes (read after an `epoch` advance) plus the
/// profiler's state.
#[derive(Debug, Serialize)]
pub struct HeapStats {
    pub allocated: u64,
    pub active: u64,
    pub resident: u64,
    pub mapped: u64,
    pub retained: u64,
    /// `opt.prof`: profiling compiled in and enabled at startup.
    pub prof_enabled: bool,
    /// `prof.active`: allocations are currently being sampled.
    pub prof_active: bool,
    /// `prof.lg_sample`: log2 of the average sample interval in bytes.
    pub lg_prof_sample: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct ProfilingToggle {
    pub active: bool,
}

type HeapError = (StatusCode, &'static str);

/// GET /debug/heap/stats
pub async fn get_heap_stats(
    State(app_state): State<AppState>,
    Extension(uid): Extension<i32>,
) -> Response {
    if let Err(status) = crate::auth::require_owner(&app_state, uid) {
        return status.into_response();
    }
    match imp::read_stats() {
        Ok(stats) => Json(stats).into_response(),
        Err(e) => e.into_response(),
    }
}

/// POST /debug/heap/profiling `{ "active": bool }`
pub async fn post_heap_profiling(
    State(app_state): State<AppState>,
    Extension(uid): Extension<i32>,
    Json(toggle): Json<ProfilingToggle>,
) -> Response {
    if let Err(status) = crate::auth::require_owner(&app_state, uid) {
        return status.into_response();
    }
    match imp::set_profiling(toggle.active).await {
        Ok(()) => {
            tracing::info!(
                active = toggle.active,
                uid,
                "jemalloc heap profiling toggled"
            );
            StatusCode::NO_CONTENT.into_response()
        }
        Err(e) => e.into_response(),
    }
}

/// GET /debug/heap/profile → gzipped pprof protobuf.
pub async fn get_heap_profile(
    State(app_state): State<AppState>,
    Extension(uid): Extension<i32>,
) -> Response {
    if let Err(status) = crate::auth::require_owner(&app_state, uid) {
        return status.into_response();
    }
    match imp::dump_profile().await {
        Ok(pprof) => (
            [
                (header::CONTENT_TYPE, "application/octet-stream"),
                (
                    header::CONTENT_DISPOSITION,
                    "attachment; filename=\"heap.pb.gz\"",
                ),
            ],
            pprof,
        )
            .into_response(),
        Err(e) => e.into_response(),
    }
}

#[cfg(target_os = "linux")]
mod imp {
    use super::{HeapError, HeapStats};
    use axum::http::StatusCode;
    use tikv_jemalloc_ctl::{epoch, profiling, raw, stats};

    const NOT_ENABLED: HeapError = (
        StatusCode::CONFLICT,
        "jemalloc profiling is not enabled (opt.prof is false; check MALLOC_CONF)",
    );

    fn ctl_error(what: &'static str, e: impl std::fmt::Display) -> HeapError {
        tracing::warn!("jemalloc {what} failed: {e}");
        (StatusCode::INTERNAL_SERVER_ERROR, "jemalloc mallctl failed")
    }

    pub(super) fn read_stats() -> Result<HeapStats, HeapError> {
        // Stats are cached per epoch; advance it so the totals are current.
        epoch::advance().map_err(|e| ctl_error("epoch advance", e))?;
        let read = |what: &'static str, r: tikv_jemalloc_ctl::Result<usize>| {
            r.map(|v| v as u64).map_err(|e| ctl_error(what, e))
        };
        let prof_enabled = profiling::prof::read().unwrap_or(false);
        // SAFETY: "prof.active" is documented as readable and returning bool:
        // http://jemalloc.net/jemalloc.3.html#prof.active
        let prof_active =
            prof_enabled && unsafe { raw::read::<bool>(b"prof.active\0") }.unwrap_or(false);
        // SAFETY: "prof.lg_sample" is documented as readable and returning size_t:
        // http://jemalloc.net/jemalloc.3.html#prof.lg_sample
        let lg_prof_sample = prof_enabled
            .then(|| unsafe { raw::read::<usize>(b"prof.lg_sample\0") }.ok())
            .flatten()
            .map(|v| v as u64);
        Ok(HeapStats {
            allocated: read("stats.allocated", stats::allocated::read())?,
            active: read("stats.active", stats::active::read())?,
            resident: read("stats.resident", stats::resident::read())?,
            mapped: read("stats.mapped", stats::mapped::read())?,
            retained: read("stats.retained", stats::retained::read())?,
            prof_enabled,
            prof_active,
            lg_prof_sample,
        })
    }

    pub(super) async fn set_profiling(active: bool) -> Result<(), HeapError> {
        let ctl = jemalloc_pprof::PROF_CTL.as_ref().ok_or(NOT_ENABLED)?;
        let mut ctl = ctl.lock().await;
        if ctl.activated() == active {
            return Ok(());
        }
        let result = if active {
            ctl.activate()
        } else {
            ctl.deactivate()
        };
        result.map_err(|e| ctl_error("prof.active write", e))
    }

    pub(super) async fn dump_profile() -> Result<Vec<u8>, HeapError> {
        let ctl = jemalloc_pprof::PROF_CTL.as_ref().ok_or(NOT_ENABLED)?;
        let mut ctl = ctl.clone().lock_owned().await;
        if !ctl.activated() {
            return Err((
                StatusCode::CONFLICT,
                "heap profiling is inactive; POST /api/debug/heap/profiling {\"active\":true} first",
            ));
        }
        // The dump writes a temp file and parses it back: blocking I/O.
        tokio::task::spawn_blocking(move || ctl.dump_pprof())
            .await
            .map_err(|e| ctl_error("profile dump task", e))?
            .map_err(|e| ctl_error("prof.dump", e))
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        // The lib test binary has no `#[global_allocator]`, but jemalloc is
        // linked unprefixed, so libc `malloc` (and with it Rust's System
        // allocator) resolves to jemalloc here too. Exporting the node's
        // `malloc_conf` makes the profiler state match production.
        #[unsafe(export_name = "malloc_conf")]
        static TEST_MALLOC_CONF: Option<&'static std::ffi::c_char> =
            // SAFETY: points at the first byte of a 'static NUL-terminated string.
            Some(unsafe { &*super::super::MALLOC_CONF.as_ptr() });

        // Impact: the stats route is how thor's resident memory gets compared
        // against glibc's after the switch; zeros would make it useless.
        // Should: report non-zero allocated and resident bytes once the epoch advances.
        // Should: report profiling as compiled in but inactive under the built-in config.
        #[test]
        fn heap_stats_report_resident_memory() {
            let held = vec![7u8; 4 << 20];
            let stats = read_stats().expect("jemalloc stats");
            assert!(stats.allocated >= held.len() as u64, "{stats:?}");
            assert!(stats.resident >= stats.active, "{stats:?}");
            assert!(stats.mapped > 0, "{stats:?}");
            assert!(stats.prof_enabled, "{stats:?}");
            assert!(!stats.prof_active, "{stats:?}");
            assert_eq!(stats.lg_prof_sample, Some(19), "{stats:?}");
            drop(held);
        }

        // Impact: a dump with sampling off would be an empty profile that
        // reads as "nothing is allocated" rather than "nothing was sampled".
        // Should: refuse a profile dump with 409 while profiling is inactive.
        #[tokio::test]
        async fn profile_dump_requires_profiling_active() {
            let err = dump_profile()
                .await
                .expect_err("inactive profiler must refuse");
            assert_eq!(err.0, StatusCode::CONFLICT);
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod imp {
    use super::{HeapError, HeapStats};
    use axum::http::StatusCode;

    const UNSUPPORTED: HeapError = (
        StatusCode::NOT_IMPLEMENTED,
        "heap profiling needs the Linux node's jemalloc allocator",
    );

    pub(super) fn read_stats() -> Result<HeapStats, HeapError> {
        Err(UNSUPPORTED)
    }

    pub(super) async fn set_profiling(_active: bool) -> Result<(), HeapError> {
        Err(UNSUPPORTED)
    }

    pub(super) async fn dump_profile() -> Result<Vec<u8>, HeapError> {
        Err(UNSUPPORTED)
    }
}
