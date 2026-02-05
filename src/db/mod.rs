pub use {
    duckdb::{params, DuckdbConnectionManager, Error as DuckdbError},
    r2d2::PooledConnection,
    types::*,
    crate::types::*
};

// Re-export CustomUUID from common module for backward compatibility
pub use hopnet_common::CustomUUID;

pub mod shared;
pub mod users;
pub mod consensus;
pub mod metrics;
pub mod types;
pub mod setup;
pub mod nodes;
pub mod files;
pub mod fragments;
pub mod fileprovider;
pub mod documentprovider;
pub mod takeout;
pub mod inventory;
pub mod resilience;
pub mod debug;