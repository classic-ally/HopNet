// HopNet Common Types Library
pub mod db;
pub mod debug;
pub mod devices;
pub mod documentprovider;
pub mod fileprovider;
pub mod hash;
pub mod height;
pub mod quorum;
pub mod setup;
pub mod shares;
pub mod users;
pub mod views;

// Database implementations (only when database feature is enabled)
#[cfg(feature = "database")]
pub mod db_impl;

// Re-export commonly used types at the top level
pub use db::{
    CustomUUID, FileItem, ImportPathCounts, ImportPathRow, ImportPathStatus, ImportRecord,
    ImportStatus, InodeType, TakeoutRecord, TakeoutStatus,
};
pub use debug::{StateSnapshot, TableHashInfo};
pub use devices::{DeviceInfo, RegisterDeviceRequest, RegisterDeviceResponse};
pub use fileprovider::{
    ChangesQuery, ChangesResponse, DeleteItemRequest, EnumerateResponse, FileProviderItem,
    HealthResponse, HealthStatus,
};
pub use hash::Blake3Hash;
pub use shares::{
    AcceptShareRequest, IncomingShareResponse, ShareCountResponse, ShareDetailResponse,
    ShareParticipant, ShareRequest,
};
pub use users::{OnboardingFlag, OnboardingFlags, PublicUserInfo, SelfUserInfo};
