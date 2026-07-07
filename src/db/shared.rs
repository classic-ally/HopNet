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

/// Get the XDG data directory for storing the database
pub fn get_database_path() -> String {
    let data_dir = std::env::var("XDG_DATA_HOME").unwrap_or_else(|_| {
        format!(
            "{}/.local/share",
            std::env::var("HOME").unwrap_or_else(|_| ".".to_string())
        )
    });

    let db_dir = format!("{}/hopnet", data_dir);
    let db_path = format!("{}/database.db", db_dir);
    tracing::info!("Using database path: {}", db_path);
    db_path
}

/// Check if the database file already exists
pub fn database_exists(db_path: &str) -> bool {
    Path::new(db_path).exists()
}

/// Check if the database schema is initialized by checking for critical tables
pub fn is_schema_initialized(
    db: &PooledConnection<SqliteConnectionManager>,
) -> Result<bool, DuckdbError> {
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
/// Used for benchmarking; absent values fall back to SQLite defaults.
///
/// `HOPNET_DB_SYNCHRONOUS` — OFF | NORMAL | FULL | EXTRA (default: SQLite picks FULL under WAL)
/// `HOPNET_DB_CACHE_KIB`    — positive integer; applied as `PRAGMA cache_size = -<N>` (KiB form)
/// `HOPNET_DB_MMAP_BYTES`   — non-negative integer; applied as `PRAGMA mmap_size = <N>`
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

    if let Ok(v) = std::env::var("HOPNET_DB_CACHE_KIB") {
        match v.trim().parse::<u64>() {
            Ok(kib) if kib > 0 => {
                out.push_str(&format!("PRAGMA cache_size = -{};\n", kib));
            }
            _ => tracing::warn!("ignoring invalid HOPNET_DB_CACHE_KIB={}", v),
        }
    }

    if let Ok(v) = std::env::var("HOPNET_DB_MMAP_BYTES") {
        match v.trim().parse::<u64>() {
            Ok(bytes) => {
                out.push_str(&format!("PRAGMA mmap_size = {};\n", bytes));
            }
            Err(_) => tracing::warn!("ignoring invalid HOPNET_DB_MMAP_BYTES={}", v),
        }
    }

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

        // page_size must run before journal_mode = WAL: the WAL init writes
        // the DB header at the current page size and locks it.
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

        // Count successful commits / rollbacks across the whole codebase.
        // commit_hook returns false to allow the commit to proceed.
        conn.commit_hook(Some(|| {
            DB_COUNTERS.txn_commits.fetch_add(1, Ordering::Relaxed);
            false
        }))?;
        conn.rollback_hook(Some(|| {
            DB_COUNTERS.txn_rollbacks.fetch_add(1, Ordering::Relaxed);
        }))?;

        register_custom_functions(conn)?;
        Ok(())
    }
}

/// Register custom SQL functions needed by queries across the codebase
pub fn register_custom_functions(conn: &rusqlite::Connection) -> rusqlite::Result<()> {
    // uuid_extract_timestamp(uuid_text) → INTEGER (NULL-safe: NULL in → NULL out)
    // Parse UUIDv7 hex, extract 48-bit timestamp, return epoch millis
    conn.create_scalar_function(
        "uuid_extract_timestamp",
        1,
        rusqlite::functions::FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let uuid_str: Option<String> = ctx.get(0)?;
            match uuid_str {
                None => Ok(None),
                Some(s) => {
                    let hex_only: String = s.replace('-', "");
                    if hex_only.len() < 12 {
                        return Ok(Some(0i64));
                    }
                    match i64::from_str_radix(&hex_only[..12], 16) {
                        Ok(millis) => Ok(Some(millis)),
                        Err(_) => Ok(Some(0i64)),
                    }
                }
            }
        },
    )?;

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

pub fn initialize(db: PooledConnection<SqliteConnectionManager>) -> Result<(), DuckdbError> {
    db.execute_batch(
        "
            CREATE TABLE sequences (
                name            TEXT PRIMARY KEY,
                next_id         INTEGER NOT NULL
            );

            CREATE TABLE users (
                user_id         INTEGER PRIMARY KEY,
                username        TEXT NOT NULL,
                pubkey          BLOB NOT NULL,
                x25519_pubkey   BLOB NOT NULL,  -- 32 bytes X25519 public key for file access
                encrypted_privkey BLOB NOT NULL, -- nonce || ChaCha20-Poly1305 ciphertext
                key_salt        BLOB NOT NULL,   -- Argon2 salt
                first_name      TEXT,            -- optional first name (max 32 chars)
                last_name       TEXT,            -- optional last name (max 32 chars)
                avatar          BLOB,            -- optional avatar (JPEG, max 128KB)
                onboarding_flags INTEGER NOT NULL DEFAULT 0, -- u32 bitfield, see hopnet_common::users::onboarding_flags

                CONSTRAINT unique_username UNIQUE (username)
            );

            CREATE TABLE nodes (
                node_id         INTEGER PRIMARY KEY,
                name            TEXT NOT NULL,
                owner           INTEGER NOT NULL,
                pubkey          BLOB NOT NULL,

                FOREIGN KEY (owner) REFERENCES users(user_id)
            );

            -- Common query patterns:
            -- 1. user owns what nodes?
            CREATE INDEX idx_nodes_owner ON nodes(owner);

            -- Consensus: decided chain + engine WAL live in the crate-owned
            -- tables (decided_blocks, decided_certificates, consensus_wal,
            -- consensus_meta — installed below via hopnet_consensus).

            -- Track validators that are acceptable at any given time
            -- Not using views (nodes can be in different views due to network partitions)
            -- Not using timestamps (time sync requirement)
            -- Using height (deterministic, directly tied to the block being committed)
            CREATE TABLE validators (
                effective_height    INTEGER NOT NULL,   -- Height when this validator changes state
                node_id             INTEGER NOT NULL,
                is_active           INTEGER NOT NULL,

                PRIMARY KEY (effective_height, node_id),
                FOREIGN KEY (node_id) REFERENCES nodes(node_id)
            );

            -- Common query patterns:
            -- 1. Give me current validators (e.g. latest effective height for leave/rejoin)
            -- 2. For consensus rebuild, give me nodes active at a given height
            CREATE INDEX idx_validator_height ON validators(effective_height DESC); 
            CREATE INDEX idx_validator_active ON validators(effective_height, is_active);

            -- This node's identity. Consensus progress lives in the engine's
            -- WAL + consensus_meta, not here.
            CREATE TABLE this_node (
                internal_id             INTEGER PRIMARY KEY DEFAULT 1,
                node_id                 INTEGER NOT NULL UNIQUE,
                privkey                 BLOB NOT NULL
            );

            CREATE TABLE metrics (
                from_node       INTEGER NOT NULL,
                to_node         INTEGER NOT NULL,
                start_time      TEXT NOT NULL,
                rtt_latency     REAL,
                rtt_variance    REAL,
                rtt_jitter      REAL,
                throughput      INTEGER,
                height          INTEGER NOT NULL,  -- Consensus height for deterministic versioning
                available       INTEGER NOT NULL DEFAULT 1, -- Node availability (0 if unreachable)
                storage_total_gb INTEGER,  -- Total storage capacity in GB
                storage_used_gb INTEGER,   -- Used storage capacity in GB

                PRIMARY KEY     (from_node, to_node, start_time),
                FOREIGN KEY (from_node) REFERENCES nodes(node_id),
                FOREIGN KEY (to_node)   REFERENCES nodes(node_id)
            );

            -- Create indexes for common query patterns
            CREATE INDEX idx_metrics_time_range ON metrics(start_time, from_node, to_node);
            CREATE INDEX idx_metrics_from_node ON metrics(from_node, start_time);
            CREATE INDEX idx_metrics_to_node ON metrics(to_node, start_time);
            CREATE INDEX idx_metrics_height ON metrics(height DESC, to_node); -- For placement decisions at specific heights

            -- takeouts/imports moved to hopnet_takeout::db::install_schema
            -- (RFC-015 Stage D5b) — chained below with the other units.

            -- Local staging table for fragment request metrics (before consensus submission)
            CREATE TABLE pending_fragment_requests (
                from_node INTEGER NOT NULL,
                to_node INTEGER NOT NULL,
                success INTEGER NOT NULL,
                recorded_at_height INTEGER NOT NULL,      -- When request actually occurred
                batch_upload_height INTEGER,              -- When submitted to consensus (NULL = pending)

                FOREIGN KEY (from_node) REFERENCES nodes(node_id),
                FOREIGN KEY (to_node) REFERENCES nodes(node_id)
            );

            CREATE INDEX idx_pending_requests ON pending_fragment_requests (batch_upload_height, recorded_at_height);
            CREATE INDEX idx_timing_requests ON pending_fragment_requests (recorded_at_height, from_node, to_node);

            -- Consensus-tracked reputation metrics (aggregated from staging tables)
            CREATE TABLE fragment_request_metrics (
                reporting_node INTEGER NOT NULL,    -- Node that reported these metrics
                from_node INTEGER NOT NULL,         -- Node that requested fragments
                to_node INTEGER NOT NULL,           -- Node that served fragments
                consensus_height INTEGER NOT NULL,   -- When metrics were submitted
                requests_sent INTEGER NOT NULL,
                requests_succeeded INTEGER NOT NULL,

                PRIMARY KEY (reporting_node, from_node, to_node, consensus_height),
                FOREIGN KEY (reporting_node) REFERENCES nodes(node_id),
                FOREIGN KEY (from_node) REFERENCES nodes(node_id),
                FOREIGN KEY (to_node) REFERENCES nodes(node_id)
            );

            -- Indexes for reputation queries
            CREATE INDEX idx_reputation_to_node ON fragment_request_metrics (to_node, consensus_height);
            CREATE INDEX idx_reputation_from_node ON fragment_request_metrics (from_node, consensus_height);
            CREATE INDEX idx_reputation_consensus_height ON fragment_request_metrics (consensus_height);

            -- Device tokens for OS integration authentication (consensus-replicated)
            -- API key format: {token_id}.{secret} - only hash of secret is stored
            -- Device name is SIV-encrypted with user's key (privacy from other nodes)
            -- Device tokens for OS integration authentication (consensus-replicated)
            -- API key format: {token_id}.{secret} - only hash of secret is stored
            -- Device name is SIV-encrypted with user's key (privacy from other nodes)
            CREATE TABLE device_tokens (
                id                      TEXT PRIMARY KEY,   -- UUIDv7 encodes creation time
                user_id                 INTEGER NOT NULL,
                api_key_hash            BLOB NOT NULL,      -- Blake3 hash of secret portion
                encrypted_device_name   TEXT NOT NULL,      -- SIV-encrypted, hex-encoded
                wrapped_user_key        BLOB NOT NULL,      -- ChaCha20-Poly1305 wrapped user privkey
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            );
            CREATE INDEX idx_device_tokens_user_id ON device_tokens(user_id);

            -- Transaction nonce dedup (prevents stale resubmission after forward timeout)
            CREATE TABLE committed_tx_nonces (
                nonce TEXT PRIMARY KEY
            );

        "
    )?;

    // Malachite engine tables (consensus_wal, decided_blocks,
    // decided_certificates, consensus_meta) — owned by hopnet-consensus.
    hopnet_consensus::store::install_schema(&db).map_err(|e| match e {
        hopnet_consensus::store::StoreError::Db(db_err) => db_err,
        // install_schema only executes DDL — non-Db variants are unreachable
        other => rusqlite::Error::InvalidParameterName(other.to_string()),
    })?;

    // Schema seam (RFC-015): substrate tables, then each projection's unit.
    // Order matters — storage FKs the host's nodes table; drive FKs users
    // (host) and data_blocks (storage).
    hopnet_storage::store::install_schema(&db)?;
    hopnet_drive::db::install_schema(&db)?;
    hopnet_takeout::db::install_schema(&db)?;

    Ok(())
}
