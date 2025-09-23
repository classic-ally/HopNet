// HopNet Common Types Library
pub mod db;
pub mod fileprovider;
pub mod setup;

// Database implementations (only when database feature is enabled)
#[cfg(feature = "database")]
pub mod db_impl;

// Re-export commonly used types at the top level
pub use db::{InodeType, TakeoutStatus, TakeoutRecord, CustomUUID, FileItem};
pub use fileprovider::{HealthStatus, HealthResponse, FileProviderItem, EnumerateResponse, DeleteItemRequest, ChangesResponse, ChangesQuery};