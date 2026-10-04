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
//!
//! The profiling routes answer 503 when profiling was not enabled at startup
//! (`MALLOC_CONF` set `prof:false`): only a restart can change that.

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
    /// `stats.metadata`: jemalloc's own bookkeeping.
    pub metadata: u64,
    /// Dirty pages (freed, not yet purged) across all arenas, in bytes.
    pub dirty: Option<u64>,
    /// Muzzy pages (lazily purged, still counted resident) across all
    /// arenas, in bytes.
    pub muzzy: Option<u64>,
    /// `background_thread`: background purging is switched on.
    pub background_thread: bool,
    /// `stats.background_thread.num_threads`: purge threads running.
    pub background_threads: Option<u64>,
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

    pub(super) const NOT_ENABLED: HeapError = (
        StatusCode::SERVICE_UNAVAILABLE,
        "jemalloc profiling is not enabled in this process (opt.prof is false, \
         e.g. MALLOC_CONF set prof:false); restart without that override",
    );

    pub(super) const INACTIVE: HeapError = (
        StatusCode::CONFLICT,
        "heap profiling is inactive; POST /api/debug/heap/profiling {\"active\":true} first",
    );

    fn ctl_error(what: &'static str, e: impl std::fmt::Display) -> HeapError {
        tracing::warn!("jemalloc {what} failed: {e}");
        (StatusCode::INTERNAL_SERVER_ERROR, "jemalloc mallctl failed")
    }

    /// `jemalloc_pprof::PROF_CTL` is `None` when `opt.prof` was false at
    /// startup: profiling can't be switched on without a restart.
    pub(super) fn enabled_ctl<T>(ctl: Option<&T>) -> Result<&T, HeapError> {
        ctl.ok_or(NOT_ENABLED)
    }

    /// Reads a `size_t` mallctl by name; `None` if jemalloc doesn't know it.
    fn read_size(name: &'static [u8]) -> Option<u64> {
        // SAFETY: every name passed is documented as a readable size_t:
        // http://jemalloc.net/jemalloc.3.html#mallctl_namespace
        unsafe { raw::read::<usize>(name) }.ok().map(|v| v as u64)
    }

    /// Reads a `bool` mallctl by name; `false` if jemalloc doesn't know it.
    fn read_flag(name: &'static [u8]) -> bool {
        // SAFETY: every name passed is documented as a readable bool.
        unsafe { raw::read::<bool>(name) }.unwrap_or(false)
    }

    pub(super) fn read_stats() -> Result<HeapStats, HeapError> {
        // Stats are cached per epoch; advance it so the totals are current.
        epoch::advance().map_err(|e| ctl_error("epoch advance", e))?;
        let read = |what: &'static str, r: tikv_jemalloc_ctl::Result<usize>| {
            r.map(|v| v as u64).map_err(|e| ctl_error(what, e))
        };
        let prof_enabled = profiling::prof::read().unwrap_or(false);
        let prof_active = prof_enabled && read_flag(b"prof.active\0");
        let lg_prof_sample = prof_enabled
            .then(|| read_size(b"prof.lg_sample\0"))
            .flatten();
        // Page counts summed over every arena (MALLCTL_ARENAS_ALL = 4096).
        let page = read_size(b"arenas.page\0").unwrap_or(0);
        let pages = |name: &'static [u8]| read_size(name).map(|n| n * page);
        Ok(HeapStats {
            allocated: read("stats.allocated", stats::allocated::read())?,
            active: read("stats.active", stats::active::read())?,
            resident: read("stats.resident", stats::resident::read())?,
            mapped: read("stats.mapped", stats::mapped::read())?,
            retained: read("stats.retained", stats::retained::read())?,
            metadata: read("stats.metadata", stats::metadata::read())?,
            dirty: pages(b"stats.arenas.4096.pdirty\0"),
            muzzy: pages(b"stats.arenas.4096.pmuzzy\0"),
            background_thread: read_flag(b"background_thread\0"),
            background_threads: read_size(b"stats.background_thread.num_threads\0"),
            prof_enabled,
            prof_active,
            lg_prof_sample,
        })
    }

    pub(super) async fn set_profiling(active: bool) -> Result<(), HeapError> {
        let ctl = enabled_ctl(jemalloc_pprof::PROF_CTL.as_ref())?;
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
        let ctl = enabled_ctl(jemalloc_pprof::PROF_CTL.as_ref())?;
        let mut ctl = ctl.clone().lock_owned().await;
        if !ctl.activated() {
            return Err(INACTIVE);
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
        // Should: report allocated bytes covering a live allocation once the epoch advances.
        // Should: report resident at least active, and active at least allocated.
        // Should: report jemalloc's metadata size and the dirty and muzzy page totals.
        // Should: report profiling as compiled in but inactive under the built-in config.
        #[test]
        fn heap_stats_report_resident_memory() {
            let held = vec![7u8; 4 << 20];
            let stats = read_stats().expect("jemalloc stats");
            assert!(stats.allocated >= held.len() as u64, "{stats:?}");
            assert!(stats.active >= stats.allocated, "{stats:?}");
            assert!(stats.resident >= stats.active, "{stats:?}");
            assert!(stats.mapped > 0, "{stats:?}");
            assert!(stats.metadata > 0, "{stats:?}");
            assert!(stats.dirty.is_some(), "{stats:?}");
            assert!(stats.muzzy.is_some(), "{stats:?}");
            // MALLOC_CONF (the documented override) can change the profiler
            // state, so the built-in config is only checked without it.
            if std::env::var_os("MALLOC_CONF").is_none() {
                assert!(stats.prof_enabled, "{stats:?}");
                assert!(!stats.prof_active, "{stats:?}");
                assert_eq!(stats.lg_prof_sample, Some(19), "{stats:?}");
            }
            drop(held);
        }

        // Impact: a dump with sampling off would be an empty profile that
        // reads as "nothing is allocated" rather than "nothing was sampled".
        // Should: refuse a profile dump with 409 and the inactive message while profiling is inactive.
        #[tokio::test]
        async fn profile_dump_requires_profiling_active() {
            // The documented MALLOC_CONF override may enable or activate
            // profiling, which makes this path unreachable.
            if std::env::var_os("MALLOC_CONF").is_some() {
                return;
            }
            let err = dump_profile()
                .await
                .expect_err("inactive profiler must refuse");
            assert_eq!(err, INACTIVE);
        }

        // Impact: "restart without the override" and "switch sampling on" are
        // different operator actions; one status for both hid which applied.
        // Should: answer 503 when profiling was not enabled at startup.
        // Should: hand back the profiler handle when one exists.
        #[test]
        fn profiling_not_enabled_at_startup_is_unavailable() {
            let err = enabled_ctl(None::<&()>).expect_err("no profiler handle");
            assert_eq!(err, NOT_ENABLED);
            assert_eq!(err.0, StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(enabled_ctl(Some(&())), Ok(&()));
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
