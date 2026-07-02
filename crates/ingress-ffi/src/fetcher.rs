//! The resource-fetch foreign trait: Rust asks Swift for bytes.
//!
//! Blocking by design (Phase 2 contract): the scheduler invokes
//! implementations via `spawn_blocking` under the fetch-concurrency
//! semaphore. Swift implements this with `PHAssetResourceManager` streaming
//! into the provided sink.

use std::sync::Arc;

use crate::error::FfiError;
use crate::session::ChunkSink;
use crate::types::FfiAssetDescriptor;

/// Which resource of which photo to fetch.
#[derive(Debug, Clone, uniffi::Record)]
pub struct FfiFetchRequest {
    pub photo_id: String,
    /// `PHAsset.localIdentifier` to fetch by.
    pub local_id: String,
    /// Raw `PHAssetResourceType` selecting the resource on the asset.
    pub ph_resource_type: i32,
}

/// Implemented in Swift. Both methods are blocking; never called on the
/// main thread (scheduler worker threads only).
#[uniffi::export(with_foreign)]
pub trait PhotoResourceFetcher: Send + Sync {
    /// Re-extract a fresh descriptor for an asset (sidecar fields, ext
    /// derivation, expected sizes at admission).
    fn descriptor_for(&self, local_id: String) -> Result<FfiAssetDescriptor, FfiError>;

    /// Stream one resource's bytes into the sink. On success return Ok
    /// WITHOUT calling `finish` — commit control stays in Rust. On failure,
    /// classify the PhotoKit error into the matching `FfiError` variant.
    fn fetch_resource(
        &self,
        request: FfiFetchRequest,
        sink: Arc<ChunkSink>,
    ) -> Result<(), FfiError>;
}

/// Adapter: the foreign (Swift) fetcher as the scheduler's core trait.
pub(crate) struct ForeignFetcher {
    pub(crate) inner: Arc<dyn PhotoResourceFetcher>,
}

fn failure_from_ffi(e: FfiError) -> ingress_core::scheduler::FetchFailure {
    use ingress_core::scheduler::FetchFailure as F;
    match e {
        FfiError::LocalDiskPressure => F::LocalDiskPressure,
        FfiError::Cancelled => F::Cancelled,
        FfiError::AssetUnavailable { msg } => F::AssetUnavailable(msg),
        other => F::Transient(other.to_string()),
    }
}

impl ingress_core::scheduler::ResourceFetcher for ForeignFetcher {
    fn descriptor_for(
        &self,
        local_id: &str,
    ) -> Result<ingress_core::descriptor::AssetDescriptor, ingress_core::scheduler::FetchFailure>
    {
        let ffi = self
            .inner
            .descriptor_for(local_id.to_string())
            .map_err(failure_from_ffi)?;
        crate::convert::descriptor_from_ffi(ffi).map_err(failure_from_ffi)
    }

    fn fetch_resource(
        &self,
        request: ingress_core::scheduler::FetchRequest,
        sink: Arc<ingress_core::scheduler::StreamSink>,
    ) -> Result<(), ingress_core::scheduler::FetchFailure> {
        let ffi_request = FfiFetchRequest {
            photo_id: request.photo_id.to_string(),
            local_id: request.local_id,
            ph_resource_type: request.ph_resource_type,
        };
        let ffi_sink = Arc::new(ChunkSink::scheduled(sink));
        self.inner
            .fetch_resource(ffi_request, ffi_sink)
            .map_err(failure_from_ffi)
    }
}
