//! UniFFI bridge for `ingress-core`: the Swift PhotoKit shim's view of the
//! Rust core. Thin by design — types mirror `ingress_core::descriptor`,
//! logic lives in the core.

uniffi::setup_scaffolding!();

pub mod convert;
pub mod error;
pub mod fetcher;
pub mod refreshing;
pub mod session;
pub mod types;

pub use error::FfiError;
pub use fetcher::{FfiFetchRequest, PhotoResourceFetcher};
pub use refreshing::{FfiPublishCredentials, PublishCredentialsProvider};
pub use session::{ChunkSink, IngressSession};
pub use types::*;
