use super::*;
use hdrhistogram::Histogram;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Always-on telemetry counters. Cost: one `fetch_add` per event. Negligible.
/// Exposed via `/debug/db-stats` for both bench harness and prod observability.
pub struct DbCounters {
    pub txn_commits: AtomicU64,
    pub txn_rollbacks: AtomicU64,
    pub conn_acquires: AtomicU64,
}

impl DbCounters {
    const fn new() -> Self {
        Self {
            txn_commits: AtomicU64::new(0),
            txn_rollbacks: AtomicU64::new(0),
            conn_acquires: AtomicU64::new(0),
        }
    }
}

pub static DB_COUNTERS: DbCounters = DbCounters::new();

/// Commit-latency instrumentation moved down to hopnet-projection (RFC-015
/// Stage D5b) so hopnet-takeout's local commits record into the SAME
/// histogram `/debug/db-stats` reads; re-exported here so every host call
/// site is unchanged.
pub use hopnet_projection::dbstats::{COMMIT_LATENCY_US, commit_timed};

/// The database file inside the node's data directory.
///
/// Resolution lives in [`crate::paths`]; this is the one derived name.
/// Nothing else should reach for `.parent()` of the result — that habit is
/// what let an "ephemeral" node scatter TLS material and photos sidecars
/// through a real user's data directory.
pub fn get_database_path() -> String {
    let db_path = crate::paths::data_dir()
        .join("database.db")
        .to_string_lossy()
        .into_owned();
    // Called from request handlers and a 30s retry loop, so DEBUG: the
    // resolved locations are logged once at startup instead.
    tracing::debug!("Using database path: {}", db_path);
    db_path
}

/// Check if the database file already exists
pub fn database_exists(db_path: &str) -> bool {
    Path::new(db_path).exists()
}

/// Check if the database schema is initialized by checking for critical tables
pub fn is_schema_initialized(db: &rusqlite::Connection) -> Result<bool, DuckdbError> {
    // Check if the critical 'this_node' table exists (the legacy 'blocks'
    // table died with the bespoke engine at Stage 5b)
    let result = db.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'this_node'",
        [],
        |row| row.get::<_, i64>(0),
    );

    match result {
        Ok(count) => Ok(count > 0),
        Err(_) => Ok(false),
    }
}

/// Ensure the database directory exists
pub fn ensure_database_dir(db_path: &str) -> Result<(), std::io::Error> {
    if let Some(parent) = Path::new(db_path).parent() {
        fs::create_dir_all(parent)?;
    }
    Ok(())
}

/// Optional pragma overrides read from environment.
/// Used for benchmarking; absent values fall back to HopNet's defaults where
/// one is named below, else SQLite's.
///
/// `HOPNET_DB_SYNCHRONOUS` — OFF | NORMAL | FULL | EXTRA (default: SQLite picks FULL under WAL)
/// `HOPNET_DB_CACHE_KIB`    — positive integer; applied as `PRAGMA cache_size = -<N>` (KiB form).
///                            Default 32768 (32 MiB). The cache is private to each pooled
///                            connection, so the old 256 MiB default reached ~8 GiB of
///                            anonymous memory across the 32-connection pool (12 GB RSS on
///                            a 30 GB laptop, an OOM kill on thor, 2026-10-03). Reads that
///                            miss it go through the shared mmap below instead of pread.
/// `HOPNET_DB_JOURNAL_SIZE_LIMIT_BYTES` — non-negative integer; `PRAGMA journal_size_limit`.
///                            Default 1073741824 (1 GiB): the WAL file is truncated back to
///                            this after a checkpoint instead of keeping its high-water mark
///                            (one node carried a 10.8 GB dead WAL).
/// `HOPNET_DB_MMAP_BYTES`   — non-negative integer; applied as `PRAGMA mmap_size = <N>`.
///                            Default 2 GiB (SQLite clamps it to its compile-time maximum).
///                            The mapping is the OS page cache, shared by every connection
///                            and reclaimable, so one copy of the hot pages serves the whole
///                            pool, and a cache miss is a page fault rather than a pread
///                            (the storm the old 256 MiB private cache was sized against:
///                            5.5 billion read syscalls in a day on one node). 0 turns it off.
/// `HOPNET_DB_TEMP_STORE`   — DEFAULT | FILE | MEMORY
/// `HOPNET_DB_PAGE_SIZE`    — power of 2 in [512, 65536]. Only takes effect on a fresh
///                            (empty) database. On a populated DB the PRAGMA is silently
///                            ignored by SQLite; migrate via `VACUUM INTO` to a new file.
/// Default page size for new HopNet databases. Bench results showed 16 KiB
/// reduces p99 commit latency by ~89% vs SQLite's 4 KiB default, with a +38%
/// read-burst gain, while keeping FULL synchronous durability. Cost is a ~70%
/// larger WAL per commit; absolute size is small for the metadata workload.
const DEFAULT_PAGE_SIZE: u64 = 16384;

/// Page size must be issued BEFORE journal_mode = WAL on a fresh DB; the WAL
/// init writes the file header at the current page size and locks it. Returned
/// separately from `env_pragma_overrides()` so on_acquire can run it first.
/// Falls back to `DEFAULT_PAGE_SIZE` when the env var is unset; the PRAGMA
/// silently no-ops on a populated DB regardless of value.
fn page_size_pragma() -> String {
    let chosen = match std::env::var("HOPNET_DB_PAGE_SIZE") {
        Ok(v) => match v.trim().parse::<u64>() {
            Ok(n) if (512..=65536).contains(&n) && n.is_power_of_two() => n,
            _ => {
                tracing::warn!(
                    "ignoring invalid HOPNET_DB_PAGE_SIZE={} (must be power of 2 in [512, 65536]); using default {}",
                    v,
                    DEFAULT_PAGE_SIZE
                );
                DEFAULT_PAGE_SIZE
            }
        },
        Err(_) => DEFAULT_PAGE_SIZE,
    };
    format!("PRAGMA page_size = {};\n", chosen)
}

/// Per-connection page cache, KiB. Private to each pooled connection, so
/// the pool's worst case is this times `DB_POOL_MAX_SIZE`: 32 MiB keeps
/// that near 1 GiB. The shared mmap (`DEFAULT_MMAP_BYTES`) holds the hot
/// inventory pages once for everyone.
const DEFAULT_CACHE_KIB: u64 = 32 * 1024;

/// Memory-mapped I/O window, bytes. Reads come from the shared,
/// reclaimable OS page cache instead of per-connection copies and pread
/// syscalls. SQLite clamps the request to its compile-time maximum.
const DEFAULT_MMAP_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// WAL truncation threshold after a checkpoint, bytes. 1 GiB leaves room
/// for a big transaction without carrying its high-water mark forever.
const DEFAULT_JOURNAL_SIZE_LIMIT_BYTES: u64 = 1024 * 1024 * 1024;

/// `PRAGMA cache_size` from an optional `HOPNET_DB_CACHE_KIB` value; an
/// absent or invalid value yields the HopNet default.
fn cache_pragma(env: Option<&str>) -> String {
    let kib = match env.map(|v| v.trim().parse::<u64>()) {
        Some(Ok(kib)) if kib > 0 => kib,
        Some(_) => {
            tracing::warn!(
                "ignoring invalid HOPNET_DB_CACHE_KIB={}; using default {}",
                env.unwrap_or(""),
                DEFAULT_CACHE_KIB
            );
            DEFAULT_CACHE_KIB
        }
        None => DEFAULT_CACHE_KIB,
    };
    format!("PRAGMA cache_size = -{kib};\n")
}

/// `PRAGMA journal_size_limit` from an optional
/// `HOPNET_DB_JOURNAL_SIZE_LIMIT_BYTES` value; an absent or invalid value
/// yields the HopNet default.
fn journal_size_limit_pragma(env: Option<&str>) -> String {
    let bytes = match env.map(|v| v.trim().parse::<u64>()) {
        Some(Ok(bytes)) => bytes,
        Some(Err(_)) => {
            tracing::warn!(
                "ignoring invalid HOPNET_DB_JOURNAL_SIZE_LIMIT_BYTES={}; using default {}",
                env.unwrap_or(""),
                DEFAULT_JOURNAL_SIZE_LIMIT_BYTES
            );
            DEFAULT_JOURNAL_SIZE_LIMIT_BYTES
        }
        None => DEFAULT_JOURNAL_SIZE_LIMIT_BYTES,
    };
    format!("PRAGMA journal_size_limit = {bytes};\n")
}

/// `PRAGMA mmap_size` from an optional `HOPNET_DB_MMAP_BYTES` value; an
/// absent or invalid value yields the HopNet default, and 0 turns mmap off.
fn mmap_pragma(env: Option<&str>) -> String {
    let bytes = match env.map(|v| v.trim().parse::<u64>()) {
        Some(Ok(bytes)) => bytes,
        Some(Err(_)) => {
            tracing::warn!(
                "ignoring invalid HOPNET_DB_MMAP_BYTES={}; using default {}",
                env.unwrap_or(""),
                DEFAULT_MMAP_BYTES
            );
            DEFAULT_MMAP_BYTES
        }
        None => DEFAULT_MMAP_BYTES,
    };
    format!("PRAGMA mmap_size = {bytes};\n")
}

fn env_pragma_overrides() -> String {
    let mut out = String::new();

    if let Ok(v) = std::env::var("HOPNET_DB_SYNCHRONOUS") {
        let upper = v.trim().to_ascii_uppercase();
        match upper.as_str() {
            "OFF" | "NORMAL" | "FULL" | "EXTRA" => {
                out.push_str(&format!("PRAGMA synchronous = {};\n", upper));
            }
            other => tracing::warn!("ignoring invalid HOPNET_DB_SYNCHRONOUS={}", other),
        }
    }

    out.push_str(&cache_pragma(
        std::env::var("HOPNET_DB_CACHE_KIB").ok().as_deref(),
    ));
    out.push_str(&journal_size_limit_pragma(
        std::env::var("HOPNET_DB_JOURNAL_SIZE_LIMIT_BYTES")
            .ok()
            .as_deref(),
    ));

    out.push_str(&mmap_pragma(
        std::env::var("HOPNET_DB_MMAP_BYTES").ok().as_deref(),
    ));

    if let Ok(v) = std::env::var("HOPNET_DB_TEMP_STORE") {
        let upper = v.trim().to_ascii_uppercase();
        match upper.as_str() {
            "DEFAULT" | "FILE" | "MEMORY" => {
                out.push_str(&format!("PRAGMA temp_store = {};\n", upper));
            }
            other => tracing::warn!("ignoring invalid HOPNET_DB_TEMP_STORE={}", other),
        }
    }

    out
}

/// Connection customizer that runs PRAGMAs and registers custom functions on each new connection
#[derive(Debug)]
pub struct SqliteInitializer;

impl r2d2::CustomizeConnection<rusqlite::Connection, rusqlite::Error> for SqliteInitializer {
    fn on_acquire(&self, conn: &mut rusqlite::Connection) -> Result<(), rusqlite::Error> {
        DB_COUNTERS.conn_acquires.fetch_add(1, Ordering::Relaxed);

        apply_connection_pragmas(conn)?;

        // Count successful commits / rollbacks across the whole codebase.
        // commit_hook returns false to allow the commit to proceed.
        conn.commit_hook(Some(|| {
            DB_COUNTERS.txn_commits.fetch_add(1, Ordering::Relaxed);
            false
        }))?;
        conn.rollback_hook(Some(|| {
            DB_COUNTERS.txn_rollbacks.fetch_add(1, Ordering::Relaxed);
        }))?;

        Ok(())
    }
}

/// The connection setup every HopNet database handle needs — pooled or
/// plain (the regenesis boot transition opens plain connections before
/// the pool exists). Ordering is load-bearing: page_size must run before
/// journal_mode = WAL because the WAL init writes the file header at the
/// current page size and locks it.
pub fn apply_connection_pragmas(conn: &rusqlite::Connection) -> Result<(), rusqlite::Error> {
    conn.execute_batch(&page_size_pragma())?;
    conn.execute_batch(
        "
        PRAGMA journal_mode = WAL;
        PRAGMA foreign_keys = ON;
        PRAGMA busy_timeout = 5000;
    ",
    )?;
    let overrides = env_pragma_overrides();
    if !overrides.is_empty() {
        conn.execute_batch(&overrides)?;
    }
    register_custom_functions(conn)?;
    Ok(())
}

/// Register custom SQL functions needed by queries across the codebase
pub fn register_custom_functions(conn: &rusqlite::Connection) -> rusqlite::Result<()> {
    // uuid_extract_timestamp(uuid_text) → INTEGER (NULL-safe: NULL in → NULL out)
    // Parse UUIDv7 hex, extract 48-bit timestamp, return epoch millis.
    // Implementation lives in hopnet-common so projections (and their tests)
    // can register the same function without depending on the host crate.
    hopnet_common::db_impl::register_uuid_extract_timestamp(conn)?;

    // reverse(text) → TEXT
    // String reversal for parent-path extraction patterns
    conn.create_scalar_function(
        "reverse",
        1,
        rusqlite::functions::FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let s: String = ctx.get(0)?;
            Ok(s.chars().rev().collect::<String>())
        },
    )?;

    // sqrt(x) → REAL
    conn.create_scalar_function(
        "sqrt",
        1,
        rusqlite::functions::FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let x: f64 = ctx.get(0)?;
            Ok(x.sqrt())
        },
    )?;

    // pow(x, y) → REAL
    conn.create_scalar_function(
        "pow",
        2,
        rusqlite::functions::FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let base: f64 = ctx.get(0)?;
            let exp: f64 = ctx.get(1)?;
            Ok(base.powf(exp))
        },
    )?;

    // log10(x) → REAL
    conn.create_scalar_function(
        "log10",
        1,
        rusqlite::functions::FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let x: f64 = ctx.get(0)?;
            Ok(x.log10())
        },
    )?;

    Ok(())
}

/// Node-local storage settings from the this_node singleton
/// (RFC-STORAGE-002 Configuration; surfaced in node settings UI later).
#[derive(Debug, Clone, Copy)]
pub struct StorageNodeSettings {
    pub gc_high_pct: u8,
    pub gc_low_pct: u8,
    pub reencode_enabled: bool,
    pub repair_budget_pct: u8,
}

pub fn read_storage_node_settings(
    conn: &rusqlite::Connection,
) -> Result<StorageNodeSettings, DatabaseError> {
    conn.query_row(
        "SELECT hopnet_storage_gc_high_pct, hopnet_storage_gc_low_pct,
                hopnet_storage_reencode_enabled, hopnet_storage_repair_budget_pct
         FROM this_node WHERE internal_id = 1",
        [],
        |row| {
            Ok(StorageNodeSettings {
                gc_high_pct: row.get::<_, i64>(0)? as u8,
                gc_low_pct: row.get::<_, i64>(1)? as u8,
                reencode_enabled: row.get::<_, i64>(2)? != 0,
                repair_budget_pct: row.get::<_, i64>(3)? as u8,
            })
        },
    )
    .map_err(|e| {
        tracing::error!("read storage node settings: {e:?}");
        DatabaseError::RecallError
    })
}

/// Node-local upgrade-provider settings from the this_node singleton
/// (RFC-019 S3).
#[derive(Debug, Clone)]
pub struct UpgradeNodeSettings {
    pub check_enabled: bool,
    /// None = derive the default from the crate's repository field.
    pub release_url: Option<String>,
}

pub fn read_upgrade_node_settings(
    conn: &rusqlite::Connection,
) -> Result<UpgradeNodeSettings, DatabaseError> {
    conn.query_row(
        "SELECT hopnet_upgrade_check_enabled, hopnet_upgrade_release_url
         FROM this_node WHERE internal_id = 1",
        [],
        |row| {
            Ok(UpgradeNodeSettings {
                check_enabled: row.get::<_, i64>(0)? != 0,
                release_url: row.get(1)?,
            })
        },
    )
    .map_err(|e| {
        tracing::error!("read upgrade node settings: {e:?}");
        DatabaseError::RecallError
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // Impact: the page cache is private to each of the pool's connections;
    // a 256 MiB default reached ~8 GiB of anonymous memory across the pool
    // and OOM-killed thor (2026-10-03). The default must be HopNet's, with
    // the env var still winning.
    // Should: emit the 32 MiB default when the variable is absent or
    // invalid, and the given KiB when it is valid.
    #[test]
    fn cache_pragma_defaults_and_overrides() {
        let default = "PRAGMA cache_size = -32768;\n";
        assert_eq!(cache_pragma(None), default);
        assert_eq!(cache_pragma(Some("garbage")), default);
        assert_eq!(cache_pragma(Some("0")), default);
        assert_eq!(cache_pragma(Some(" 4096 ")), "PRAGMA cache_size = -4096;\n");
    }

    // Should: keep the pool's worst-case private cache near 1 GiB.
    #[test]
    fn the_pools_private_cache_stays_near_a_gibibyte() {
        let pool_bytes = DEFAULT_CACHE_KIB * 1024 * u64::from(crate::db::DB_POOL_MAX_SIZE);
        assert!(pool_bytes <= 1024 * 1024 * 1024, "{pool_bytes}");
    }

    // Impact: with the private cache cut, the shared mmap is what keeps
    // inventory scans from turning back into a pread storm.
    // Should: emit the 2 GiB mmap default when the variable is absent or
    // invalid, and the given byte count when it is valid.
    // Should: allow 0, which turns memory-mapped I/O off.
    #[test]
    fn mmap_pragma_defaults_and_overrides() {
        let default = "PRAGMA mmap_size = 2147483648;\n";
        assert_eq!(mmap_pragma(None), default);
        assert_eq!(mmap_pragma(Some("lots")), default);
        assert_eq!(mmap_pragma(Some("0")), "PRAGMA mmap_size = 0;\n");
        assert_eq!(
            mmap_pragma(Some(" 1048576 ")),
            "PRAGMA mmap_size = 1048576;\n"
        );
    }

    // Should: emit the 1 GiB journal_size_limit default when the variable
    // is absent or invalid, and the given byte count when it is valid
    // (zero included: "truncate to nothing" is a legitimate choice).
    #[test]
    fn journal_size_limit_pragma_defaults_and_overrides() {
        let default = "PRAGMA journal_size_limit = 1073741824;\n";
        assert_eq!(journal_size_limit_pragma(None), default);
        assert_eq!(journal_size_limit_pragma(Some("x")), default);
        assert_eq!(
            journal_size_limit_pragma(Some("0")),
            "PRAGMA journal_size_limit = 0;\n"
        );
    }

    // Should: leave a freshly initialised connection with the HopNet cache
    // and journal-size defaults readable back through the pragmas.
    #[test]
    fn connection_pragmas_apply_the_defaults() {
        // Only meaningful when the env does not override; the two pure
        // tests above cover the override arithmetic.
        if std::env::var_os("HOPNET_DB_CACHE_KIB").is_some()
            || std::env::var_os("HOPNET_DB_JOURNAL_SIZE_LIMIT_BYTES").is_some()
        {
            return;
        }
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        apply_connection_pragmas(&conn).unwrap();
        let cache: i64 = conn
            .query_row("PRAGMA cache_size", [], |r| r.get(0))
            .unwrap();
        assert_eq!(cache, -(DEFAULT_CACHE_KIB as i64));
        let limit: i64 = conn
            .query_row("PRAGMA journal_size_limit", [], |r| r.get(0))
            .unwrap();
        assert_eq!(limit, DEFAULT_JOURNAL_SIZE_LIMIT_BYTES as i64);
    }

    // Should: leave a file-backed connection with memory-mapped I/O on
    // (SQLite clamps the 2 GiB request to its compile-time maximum).
    #[test]
    fn a_file_connection_maps_the_database() {
        if std::env::var_os("HOPNET_DB_MMAP_BYTES").is_some() {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let conn = rusqlite::Connection::open(dir.path().join("mmap.db")).unwrap();
        apply_connection_pragmas(&conn).unwrap();
        let mmap: i64 = conn
            .query_row("PRAGMA mmap_size", [], |r| r.get(0))
            .unwrap();
        assert!(mmap > 0, "mmap_size = {mmap}");
    }
}
