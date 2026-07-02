//! Platform-agnostic core for the Apple Photos ingress daemon.
//!
//! Everything downstream of PhotoKit asset enumeration lives here: the
//! `state.db` state store, identity resolution (match precedence), and
//! sidecar JSON serialization. See `docs/specs/apple-photos-ingress.md`.

pub mod descriptor;
pub mod error;
#[cfg(any(test, feature = "fixtures"))]
pub mod fixtures;
pub mod ids;
pub mod model;
pub mod resolve;
pub mod sidecar;
pub mod store;

pub use descriptor::{AssetDescriptor, LibraryScope, ResourceDescriptor};
pub use error::{IngressError, Result};
pub use ids::{ContentHash, LibraryId, PhotoId};
pub use model::{LibraryConfig, PhotoRecord, ResourceRecord, ResourceType};
pub use resolve::{resolve_descriptor, resolve_with_hash, HashResolution, Resolution};
pub use sidecar::Sidecar;
pub use store::StateStore;
