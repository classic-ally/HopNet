//! The core fetch abstraction: how the scheduler asks the platform layer
//! (Swift, or a test fake) for bytes. UniFFI-free — the FFI crate mirrors
//! this trait so scheduler tests stay pure Rust.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::descriptor::AssetDescriptor;
use crate::ids::PhotoId;
use crate::writer::{FinishedStream, ResourceWrite};

/// Classified fetch failure (the Swift side classifies PhotoKit NSErrors;
/// see spec §Failure Handling for dispositions).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FetchFailure {
    /// `CloudPhotoLibraryErrorDomain` 1005: daemon-wide pause, no retry consumed.
    LocalDiskPressure,
    /// Cancellation: rows untouched, no retry consumed.
    Cancelled,
    /// Asset gone between seed and drain: retry/backoff. Deliberately NOT
    /// reclassified as a deletion inline — local_id is not identity (library
    /// rebuilds change it), and PhotoKit absence only counts when the API is
    /// healthy. The observer's removed events and the reconciliation scan are
    /// the two sanctioned deletion detectors; retries here converge to
    /// gave-up, which the next scan resets.
    AssetUnavailable(String),
    /// Anything else from the platform layer: retry/backoff.
    Transient(String),
    /// Blob-root I/O failure (produced Rust-side in the sink): pause-class,
    /// mount-flavored; no retry consumed.
    Sink(String),
}

/// Which resource of which photo to fetch.
#[derive(Debug, Clone)]
pub struct FetchRequest {
    pub photo_id: PhotoId,
    pub local_id: String,
    pub ph_resource_type: i32,
}

/// Blocking platform callbacks, invoked via `spawn_blocking` under the
/// fetch-concurrency semaphore. Never called on the main thread.
pub trait ResourceFetcher: Send + Sync + 'static {
    /// Fresh descriptor for an asset: sidecar fields, ext derivation, and
    /// expected sizes all come from here at drain time (descriptors are
    /// deliberately not persisted).
    fn descriptor_for(&self, local_id: &str) -> Result<AssetDescriptor, FetchFailure>;

    /// Stream one resource's bytes into the sink via [`StreamSink::write`].
    /// Return Ok WITHOUT finishing — commit control stays with the scheduler.
    fn fetch_resource(
        &self,
        request: FetchRequest,
        sink: Arc<StreamSink>,
    ) -> Result<(), FetchFailure>;
}

/// Cooperative cancellation token shared across the drain.
#[derive(Debug, Clone, Default)]
pub struct CancelToken(Arc<CancelInner>);

#[derive(Debug, Default)]
struct CancelInner {
    flag: AtomicBool,
    /// Wakes the daemon's idle `select!` — without it, a SIGTERM landing
    /// while the loop sleeps on a distant retry deadline would wait the
    /// sleep out (observed live: cancel printed, daemon idled on).
    notify: tokio::sync::Notify,
}

impl CancelToken {
    pub fn cancel(&self) {
        self.0.flag.store(true, Ordering::SeqCst);
        self.0.notify.notify_waiters();
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.flag.load(Ordering::SeqCst)
    }

    /// Resolves when (or once) cancelled.
    pub async fn cancelled(&self) {
        if self.is_cancelled() {
            return;
        }
        // Re-check after registering to close the store→notified race.
        let notified = self.0.notify.notified();
        if self.is_cancelled() {
            return;
        }
        notified.await;
    }
}

/// Chunk receiver owned by the scheduler for one resource stream. `write`
/// checks the cancel token so SIGTERM propagates into inflight downloads.
/// After the fetcher returns Ok, the scheduler takes the stream back via
/// [`StreamSink::take_finished`].
pub struct StreamSink {
    state: Mutex<Option<ResourceWrite>>,
    cancel: CancelToken,
}

impl StreamSink {
    pub fn new(write: ResourceWrite, cancel: CancelToken) -> Self {
        Self {
            state: Mutex::new(Some(write)),
            cancel,
        }
    }

    pub fn write(&self, chunk: &[u8]) -> Result<(), FetchFailure> {
        if self.cancel.is_cancelled() {
            return Err(FetchFailure::Cancelled);
        }
        let mut guard = self.state.lock().expect("sink mutex");
        let write = guard
            .as_mut()
            .ok_or_else(|| FetchFailure::Sink("write after stream consumed".into()))?;
        write
            .append(chunk)
            .map_err(|e| FetchFailure::Sink(e.to_string()))
    }

    /// Consume the stream and produce its facts (hash/size/temp path).
    pub fn take_finished(&self) -> Result<FinishedStream, FetchFailure> {
        let write = self
            .state
            .lock()
            .expect("sink mutex")
            .take()
            .ok_or_else(|| FetchFailure::Sink("stream already consumed".into()))?;
        write
            .finish()
            .map_err(|e| FetchFailure::Sink(e.to_string()))
    }

    /// Best-effort temp cleanup on failure paths.
    pub fn abort(&self) {
        if let Some(write) = self.state.lock().expect("sink mutex").take() {
            write.abort();
        }
    }
}
