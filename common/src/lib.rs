// HopNet Common Types Library
pub mod chain;
pub mod compat;
pub mod db;
pub mod devices;
pub mod documentprovider;
pub mod fileprovider;
pub mod hash;
pub mod height;
pub mod mount;
pub mod photo_ingress;
pub mod quorum;
pub mod release_feed;
pub mod setup;
pub mod shares;
pub mod snapshot;
pub mod users;
pub mod version;
pub mod views;

// Database implementations (only when database feature is enabled)
#[cfg(feature = "database")]
pub mod db_impl;

// Re-export commonly used types at the top level
pub use chain::{Chain, Step, StepKind};
pub use db::{
    CustomUUID, FileItem, ImportPathCounts, ImportPathRow, ImportPathStatus, ImportRecord,
    ImportStatus, InodeType, TakeoutRecord, TakeoutStatus,
};
pub use devices::{DeviceInfo, PairingInfoResponse, RegisterDeviceRequest, RegisterDeviceResponse};
pub use fileprovider::{
    ChangesQuery, ChangesResponse, DeleteItemRequest, EnumerateResponse, FileProviderItem,
    HealthResponse, HealthStatus,
};
pub use hash::Blake3Hash;
pub use photo_ingress::{AgentRegistration, DisableRequest, DisableResponse, PhotoIngressStatus};
pub use shares::{
    AcceptShareRequest, IncomingShareResponse, ShareCountResponse, ShareDetailResponse,
    ShareParticipant, ShareRequest,
};
pub use snapshot::{
    NodeStateReport, SectionManifest, SectionSpec, SnapshotManifest, TableManifest, TableRole,
    TableSpec,
};
pub use users::{OnboardingFlag, OnboardingFlags, PublicUserInfo, SelfUserInfo};
