//! Platform-agnostic core for the Apple Photos ingress daemon.
//!
//! Everything downstream of PhotoKit asset enumeration lives here: the
//! `state.db` state store, identity resolution (match precedence), and
//! sidecar JSON serialization. See `docs/specs/apple-photos-ingress.md`.

pub mod descriptor;
pub mod error;
pub mod ext;
#[cfg(any(test, feature = "fixtures"))]
pub mod fixtures;
pub mod ids;
pub mod model;
pub mod paths;
pub mod resolve;
pub mod scheduler;
pub mod sidecar;
pub mod sidecar_io;
pub mod store;
pub mod writer;

pub use descriptor::{AssetDescriptor, LibraryScope, ResourceDescriptor};
pub use error::{IngressError, Result};
pub use ids::{ContentHash, LibraryId, PhotoId};
pub use model::{LibraryConfig, PhotoRecord, ResourceRecord, ResourceType};
pub use resolve::{
    late_binding_merge, resolve_descriptor, resolve_with_hash, seed_descriptor, HashResolution,
    Resolution, SeedOutcome,
};
pub use scheduler::{DrainReport, Scheduler, SchedulerConfig};
pub use sidecar::Sidecar;
pub use store::StateStore;
