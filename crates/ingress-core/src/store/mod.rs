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

use crate::error::Result;

pub(crate) mod blobs;
pub(crate) mod libraries;
pub(crate) mod log;
pub(crate) mod photos;
pub(crate) mod resources;

pub use log::LogEvent;
pub use resources::{RetrySummary, WriteCommit};

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
