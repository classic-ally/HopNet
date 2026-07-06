pub use {
    crate::types::*,
    r2d2::PooledConnection,
    r2d2_sqlite::SqliteConnectionManager,
    rusqlite::{Error as DuckdbError, params},
    types::*,
};

// Re-export CustomUUID from common module for backward compatibility
pub use hopnet_common::CustomUUID;

pub mod consensus;
pub mod debug;
pub mod devices;
pub mod documentprovider;
pub mod fileprovider;
pub mod files;
pub mod fragments;
pub mod import_paths;
pub mod imports;
pub mod inventory;
pub mod mesh;
pub mod metrics;
pub mod nodes;
pub mod resilience;
pub mod setup;
pub mod shared;
pub mod shares;
pub mod takeout;
pub mod types;
pub mod users;
pub mod write_gate;

/// Maximum r2d2 connections checked out simultaneously across the process.
/// Bump cautiously — each connection costs SQLite memory and a file handle.
///
/// Three known long-held conns subtract from this budget:
///   - `consensus_queue::batch_processor` reserves 1 for its lifetime
///   - `execute_takeout_materialization` reserves 1 while a takeout is active
///   - the malachite engine shell holds 1 for its SQLite storage
///
/// The remainder must serve all HTTP routes, FileProvider/DocumentProvider
/// requests, and bounded background workers.
pub const DB_POOL_MAX_SIZE: u32 = 32;

/// Connections subtracted from the pool to account for the always-held
/// dedicated conns (batch processor, engine storage, engine app reads,
/// engine proposal builds). Used by `db_worker_concurrency_budget` to size
/// workers that compete with route handlers for pool capacity.
const DB_POOL_RESERVED: u32 = 5;

/// Conservative number of pool slots left available for HTTP route handling
/// while a worker pipeline is fully saturated. Keeps the system responsive
/// to user requests even during long-running background work.
const DB_POOL_ROUTE_HEADROOM: u32 = 8;

/// Concurrency cap for any worker pipeline that competes with route handlers
/// for pool connections. Workers should not exceed this number of in-flight
/// tasks that may simultaneously acquire a connection.
pub fn db_worker_concurrency_budget() -> usize {
    (DB_POOL_MAX_SIZE - DB_POOL_RESERVED - DB_POOL_ROUTE_HEADROOM) as usize
}
