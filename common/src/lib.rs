// HopNet Common Types Library
pub mod db;
pub mod devices;
pub mod fileprovider;
pub mod documentprovider;
pub mod setup;
pub mod debug;
pub mod shares;
pub mod users;

// Database implementations (only when database feature is enabled)
#[cfg(feature = "database")]
pub mod db_impl;

// Re-export commonly used types at the top level
pub use db::{InodeType, TakeoutStatus, TakeoutRecord, ImportStatus, ImportRecord, ImportPathStatus, ImportPathRow, ImportPathCounts, CustomUUID, FileItem};
pub use devices::{RegisterDeviceRequest, RegisterDeviceResponse, DeviceInfo};
pub use fileprovider::{HealthStatus, HealthResponse, FileProviderItem, EnumerateResponse, DeleteItemRequest, ChangesResponse, ChangesQuery};
pub use debug::{StateSnapshot, TableHashInfo};
pub use shares::{ShareRequest, AcceptShareRequest, IncomingShareResponse, ShareCountResponse, ShareDetailResponse, ShareParticipant};
pub use users::{PublicUserInfo, SelfUserInfo, OnboardingFlags, OnboardingFlag};