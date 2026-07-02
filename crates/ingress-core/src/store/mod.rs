//! The `state.db` state store.
//!
//! Single-writer model (spec §blobs notes): one connection behind a
//! `max_connections(1)` pool; the CLI opens its own read-only pool. WAL
//! allows the concurrent readers.

use std::path::Path;
use std::str::FromStr;
use std::time::Duration;

use sqlx::SqlitePool;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};

use crate::error::{IngressError, Result};

pub(crate) mod blobs;
pub(crate) mod libraries;
pub(crate) mod log;
pub(crate) mod photos;
pub(crate) mod resources;
pub(crate) mod stats;

pub use log::LogEvent;
pub use resources::{RetrySummary, WriteCommit};
pub use stats::LibraryStats;

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

/// Handle to `state.db`. Cheap to clone (wraps a pool).
#[derive(Debug, Clone)]
pub struct StateStore {
    pool: SqlitePool,
}

impl StateStore {
    /// Open (creating if missing) and migrate a file-backed store.
    pub async fn open(path: &Path) -> Result<Self> {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .foreign_keys(true)
            .busy_timeout(Duration::from_millis(5000));
        let pool = SqlitePoolOptions::new()
            .max_connections(1) // single-writer invariant
            .connect_with(options)
            .await?;
        MIGRATOR.run(&pool).await?;
        Ok(Self { pool })
    }

    /// Open an existing store read-only (CLI inspection beside a live
    /// daemon — WAL allows concurrent readers). Never creates the file,
    /// never migrates; errors if the file is absent or its schema version
    /// does not match this build's migrations.
    ///
    /// Caveat: a read-only connection cannot run WAL recovery. After a
    /// daemon crash that left a hot `-wal`, this open fails
    /// (`SQLITE_READONLY_RECOVERY`); callers holding no-live-writer proof
    /// (absent/stale drain.lock) may fall back to [`StateStore::open`].
    pub async fn open_read_only(path: &Path) -> Result<Self> {
        if !path.is_file() {
            return Err(IngressError::Invariant(format!(
                "no state store at {} — run the daemon once to create it, or see `recover`",
                path.display()
            )));
        }
        let options = SqliteConnectOptions::new()
            .filename(path)
            .read_only(true)
            .foreign_keys(true)
            .busy_timeout(Duration::from_millis(5000));
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await?;
        let store = Self { pool };
        store.check_schema_version().await?;
        Ok(store)
    }

    /// Compare the store's applied migrations against this build's. A
    /// mismatch means a CLI/daemon version skew; failing here beats the
    /// opaque query errors a schema drift would produce downstream.
    async fn check_schema_version(&self) -> Result<()> {
        let expected = MIGRATOR
            .iter()
            .map(|m| m.version)
            .max()
            .expect("embedded migrations are never empty");
        let applied: Option<i64> = sqlx::query_scalar("SELECT MAX(version) FROM _sqlx_migrations")
            .fetch_one(&self.pool)
            .await?;
        match applied {
            Some(v) if v == expected => Ok(()),
            Some(v) if v < expected => Err(IngressError::Invariant(format!(
                "state store schema {v} is older than this build ({expected}) — run the daemon once to migrate"
            ))),
            Some(v) => Err(IngressError::Invariant(format!(
                "state store schema {v} is newer than this build ({expected}) — update this tool"
            ))),
            None => Err(IngressError::Invariant(
                "state store has no applied migrations — not a state.db?".into(),
            )),
        }
    }

    /// Open an in-memory store (tests).
    ///
    /// The pool must pin its single connection open: an in-memory SQLite
    /// database dies with its last connection, so idle reaping or lifetime
    /// rotation would silently drop the schema mid-test.
    pub async fn open_in_memory() -> Result<Self> {
        let options = SqliteConnectOptions::from_str("sqlite::memory:")
            .expect("static connection string")
            .foreign_keys(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .min_connections(1)
            .idle_timeout(None)
            .max_lifetime(None)
            .connect_with(options)
            .await?;
        MIGRATOR.run(&pool).await?;
        Ok(Self { pool })
    }

    /// The underlying pool — `pub(crate)`; external callers go through the
    /// typed methods on the per-table modules.
    pub(crate) fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// Raw pool access for test assertions (schema introspection). Not part
    /// of the supported API.
    #[cfg(any(test, feature = "fixtures"))]
    #[doc(hidden)]
    pub fn raw_pool(&self) -> &SqlitePool {
        &self.pool
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Impact: the CLI must never invent an empty store where the operator
    // expected an existing one — a silent create would mask a wrong
    // --data-dir and report a healthy-but-empty pipeline.
    // Should: error with a pointed message when the file is absent.
    // Should not: create the file as a side effect.
    #[tokio::test]
    async fn open_read_only_errors_on_missing_file() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("state.db");
        let err = StateStore::open_read_only(&path).await.unwrap_err();
        assert!(err.to_string().contains("no state store"), "{err}");
        assert!(!path.exists());
    }

    // Impact: status/fsck default runs promise zero mutation; a writable
    // connection would let a CLI bug corrupt live daemon state.
    // Should: read rows written by the writer pool.
    // Should not: accept writes on the read-only pool.
    #[tokio::test]
    async fn open_read_only_reads_beside_writer_and_rejects_writes() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("state.db");
        let writer = StateStore::open(&path).await.unwrap();

        let reader = StateStore::open_read_only(&path).await.unwrap();
        // Write through the still-open writer, then observe it read-only.
        writer.append_log("mount_lost", None, None).await.unwrap();
        let seen = reader.log_events("mount_lost").await.unwrap();
        assert_eq!(seen.len(), 1);

        let write_attempt = sqlx::query("DELETE FROM ingest_log")
            .execute(reader.pool())
            .await;
        assert!(write_attempt.is_err());
    }
}
