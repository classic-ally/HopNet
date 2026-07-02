//! The exported session and chunk-sink objects.
//!
//! Threading contract: every exported method is blocking (`block_on` on the
//! session's private tokio runtime). NEVER call from the main thread — Swift
//! callers run on a background queue (see the spike's main-queue deadlock
//! lesson). Phase 2 flows are strictly sequential; scheduler concurrency is
//! Phase 3.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use chrono::Utc;
use ingress_core::descriptor::AssetDescriptor;
use ingress_core::ext::{ext_for_uti, ExtDerivation};
use ingress_core::model::{LibraryConfig, ResourceType, ICLOUD_SHARED_LIBRARY_BINDING};
use ingress_core::paths::{BlobPaths, DataDir, TempKey};
use ingress_core::resolve::{resolve_descriptor, resolve_with_hash, HashResolution, Resolution};
use ingress_core::sidecar_io::write_photo_sidecar;
use ingress_core::writer::{finalize_resource, ResourceWrite};
use ingress_core::{LibraryId, PhotoId, StateStore};

use crate::convert::descriptor_from_ffi;
use crate::error::FfiError;
use crate::types::*;

struct Inner {
    store: StateStore,
    data_dir: DataDir,
    /// Descriptors retained until the photo's sidecar is written — sidecar
    /// fields (media type, subtypes, favorite, capture) are deliberately not
    /// persisted in state.db.
    inflight: Mutex<HashMap<String, AssetDescriptor>>,
}

/// One daemon-side ingest session over a data directory.
#[derive(uniffi::Object)]
pub struct IngressSession {
    runtime: tokio::runtime::Runtime,
    inner: Arc<Inner>,
}

#[uniffi::export]
impl IngressSession {
    /// Open (creating + migrating if needed) the state store under
    /// `data_dir` and start the runtime.
    #[uniffi::constructor]
    pub fn new(data_dir: String) -> Result<Arc<Self>, FfiError> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .map_err(|e| FfiError::Io { msg: e.to_string() })?;
        let data_dir = DataDir::new(data_dir);
        std::fs::create_dir_all(data_dir.root())
            .map_err(|e| FfiError::Io { msg: e.to_string() })?;
        let store = runtime.block_on(StateStore::open(&data_dir.state_db_path()))?;
        Ok(Arc::new(Self {
            runtime,
            inner: Arc::new(Inner { store, data_dir, inflight: Mutex::new(HashMap::new()) }),
        }))
    }

    /// Seed a library row (slice/CLI configuration path).
    pub fn add_library(
        &self,
        library_id: String,
        display_name: String,
        blob_root: String,
        scope: FfiLibraryScope,
    ) -> Result<(), FfiError> {
        let config = LibraryConfig {
            library_id: LibraryId::new(library_id),
            display_name,
            blob_root,
            sidecar_root_remote: None,
            scope_binding: match scope {
                FfiLibraryScope::Personal => None,
                FfiLibraryScope::Shared => Some(ICLOUD_SHARED_LIBRARY_BINDING.to_string()),
            },
            retention_days: 30,
            created_at: Utc::now(),
        };
        self.runtime.block_on(self.inner.store.insert_library(&config))?;
        Ok(())
    }

    /// Match-precedence rule 1. `NeedsOriginal` → stream the original via
    /// [`Self::begin_original`]; `AlreadyKnown` → done for the slice.
    pub fn ingest_descriptor(&self, desc: FfiAssetDescriptor) -> Result<FfiResolution, FfiError> {
        let desc = descriptor_from_ffi(desc)?;
        let resolution = self.runtime.block_on(resolve_descriptor(&self.inner.store, &desc))?;
        Ok(match resolution {
            Resolution::KnownByCloudId { photo_id, metadata_changed, scope_changed } => {
                FfiResolution::AlreadyKnown {
                    photo_id: photo_id.to_string(),
                    metadata_changed,
                    scope_changed,
                }
            }
            Resolution::NeedsContentHash => FfiResolution::NeedsOriginal,
            Resolution::UnmappedScope { photo_id } => {
                FfiResolution::UnmappedScope { photo_id: photo_id.to_string() }
            }
        })
    }

    /// Begin streaming the ORIGINAL resource of a not-yet-known asset
    /// (pre-mint probe temp; rules 2a–2c run at `finish`).
    pub fn begin_original(&self, desc: FfiAssetDescriptor) -> Result<Arc<ChunkSink>, FfiError> {
        let desc = descriptor_from_ffi(desc)?;
        let original = desc
            .resources
            .iter()
            .find(|r| ResourceType::from_ph_type(r.ph_resource_type) == Some(ResourceType::Original))
            .ok_or_else(|| FfiError::InvalidDescriptor {
                msg: "descriptor has no original resource".into(),
            })?;
        let ext = self.derive_ext(&original.uti, original.original_filename.as_deref(), None)?;

        let (library, paths) = self.library_for(&desc)?;
        let key = TempKey::Probe { token: uuid_token() };
        let write = ResourceWrite::begin(&paths, &key)?;
        Ok(Arc::new(ChunkSink {
            handle: self.runtime.handle().clone(),
            inner: self.inner.clone(),
            state: Mutex::new(Some(SinkState {
                write,
                paths,
                library,
                ext,
                kind: SinkKind::Original { desc: Box::new(desc) },
            })),
        }))
    }

    /// Begin streaming an additional resource of an already-minted photo.
    pub fn begin_resource(
        &self,
        photo_id: String,
        ph_resource_type: i32,
        uti: String,
        original_filename: Option<String>,
    ) -> Result<Arc<ChunkSink>, FfiError> {
        let resource_type = ResourceType::from_ph_type(ph_resource_type).ok_or_else(|| {
            FfiError::InvalidDescriptor {
                msg: format!("unmapped PHAssetResourceType {ph_resource_type} — filter before streaming"),
            }
        })?;

        let photo_id = PhotoId::from_string(photo_id);
        let (photo, library_id) = self.runtime.block_on(async {
            let photo = self.inner.store.photo(&photo_id).await?.ok_or_else(|| {
                ingress_core::IngressError::Invariant(format!("no photo row for {photo_id}"))
            })?;
            let lib = photo.library_id.clone().ok_or_else(|| {
                ingress_core::IngressError::Invariant(format!("photo {photo_id} has no library"))
            })?;
            Ok::<_, ingress_core::IngressError>((photo, lib))
        })?;
        let library = self
            .runtime
            .block_on(self.inner.store.library(&library_id))?
            .ok_or_else(|| FfiError::Invariant { msg: format!("no library row {library_id}") })?;

        let ext = self.derive_ext(&uti, original_filename.as_deref(), Some(&photo.photo_id))?;
        let paths = BlobPaths::new(&library.blob_root);
        let key = TempKey::Resource { photo_id: photo.photo_id.clone(), resource_type };
        let write = ResourceWrite::begin(&paths, &key)?;
        Ok(Arc::new(ChunkSink {
            handle: self.runtime.handle().clone(),
            inner: self.inner.clone(),
            state: Mutex::new(Some(SinkState {
                write,
                paths,
                library: library.library_id.clone(),
                ext,
                kind: SinkKind::Additional { photo_id: photo.photo_id, resource_type },
            })),
        }))
    }
}

impl IngressSession {
    fn library_for(&self, desc: &AssetDescriptor) -> Result<(LibraryId, BlobPaths), FfiError> {
        let config = self
            .runtime
            .block_on(self.inner.store.library_for_scope(desc.scope))?
            .ok_or(FfiError::UnmappedScope { msg: format!("{:?}", desc.scope) })?;
        let paths = BlobPaths::new(&config.blob_root);
        Ok((config.library_id, paths))
    }

    fn derive_ext(
        &self,
        uti: &str,
        filename: Option<&str>,
        photo_id: Option<&PhotoId>,
    ) -> Result<String, FfiError> {
        let derivation = ext_for_uti(uti, filename);
        if matches!(derivation, ExtDerivation::Fallback) {
            self.runtime.block_on(self.inner.store.append_log(
                "unknown_uti",
                photo_id,
                Some(serde_json::json!({ "uti": uti, "filename": filename })),
            ))?;
        }
        Ok(derivation.ext().to_string())
    }
}

fn uuid_token() -> String {
    // Reuse PhotoId's UUIDv7 mint for a unique probe token.
    PhotoId::mint().to_string()
}

enum SinkKind {
    Original { desc: Box<AssetDescriptor> },
    Additional { photo_id: PhotoId, resource_type: ResourceType },
}

struct SinkState {
    write: ResourceWrite,
    paths: BlobPaths,
    library: LibraryId,
    ext: String,
    kind: SinkKind,
}

/// One in-flight resource byte stream. `write` chunks (~1 MiB), then exactly
/// one of `finish` / `abort`.
#[derive(uniffi::Object)]
pub struct ChunkSink {
    handle: tokio::runtime::Handle,
    inner: Arc<Inner>,
    state: Mutex<Option<SinkState>>,
}

impl std::fmt::Debug for ChunkSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let open = self.state.lock().map(|g| g.is_some()).unwrap_or(false);
        f.debug_struct("ChunkSink").field("open", &open).finish()
    }
}

#[uniffi::export]
impl ChunkSink {
    pub fn write(&self, chunk: Vec<u8>) -> Result<(), FfiError> {
        let mut guard = self.state.lock().expect("sink mutex");
        let state = guard
            .as_mut()
            .ok_or_else(|| FfiError::SinkState { msg: "write after finish/abort".into() })?;
        state.write.append(&chunk)?;
        Ok(())
    }

    pub fn finish(&self) -> Result<FfiWriteOutcome, FfiError> {
        let state = self
            .state
            .lock()
            .expect("sink mutex")
            .take()
            .ok_or_else(|| FfiError::SinkState { msg: "finish after finish/abort".into() })?;
        let SinkState { write, paths, library, ext, kind } = state;
        let finished = write.finish()?;
        let inner = self.inner.clone();

        self.handle.block_on(async move {
            let (photo_id, resolution_kind, resource_type) = match kind {
                SinkKind::Original { desc } => {
                    let resolution =
                        resolve_with_hash(&inner.store, &desc, &finished.hash).await?;
                    let (photo_id, kind) = match resolution {
                        HashResolution::NewPhoto { photo_id } => {
                            (photo_id, FfiHashResolutionKind::NewPhoto)
                        }
                        HashResolution::LateBound { photo_id } => {
                            (photo_id, FfiHashResolutionKind::LateBound)
                        }
                        HashResolution::NewPhotoSharedBlob { photo_id, .. } => {
                            (photo_id, FfiHashResolutionKind::SharedBlob)
                        }
                    };
                    // Retain the descriptor for the eventual sidecar write.
                    inner
                        .inflight
                        .lock()
                        .expect("inflight mutex")
                        .insert(photo_id.to_string(), *desc);
                    (photo_id, kind, ResourceType::Original)
                }
                SinkKind::Additional { photo_id, resource_type } => {
                    (photo_id, FfiHashResolutionKind::ExistingPhoto, resource_type)
                }
            };

            let hash = finished.hash.clone();
            let size = finished.size_bytes;
            let outcome = finalize_resource(
                &inner.store, &paths, &library, &photo_id, resource_type, finished, &ext,
            )
            .await?;

            let sidecar_path = if outcome.photo_completed() {
                let desc = inner
                    .inflight
                    .lock()
                    .expect("inflight mutex")
                    .remove(&photo_id.to_string());
                match desc {
                    Some(desc) => Some(
                        write_photo_sidecar(&inner.store, &inner.data_dir, &desc, &photo_id)
                            .await?
                            .to_string_lossy()
                            .into_owned(),
                    ),
                    // Descriptor not retained (e.g. process restarted between
                    // resources): completion stands, sidecar comes with the
                    // next metadata pass. Phase 2 slice never hits this.
                    None => None,
                }
            } else {
                None
            };

            Ok(FfiWriteOutcome {
                photo_id: photo_id.to_string(),
                resolution_kind,
                content_hash: hash.to_string(),
                size_bytes: size,
                ext,
                deduped: outcome.deduped(),
                blob_path: outcome.blob_path().to_string_lossy().into_owned(),
                photo_completed: outcome.photo_completed(),
                sidecar_path,
            })
        })
    }

    pub fn abort(&self) {
        if let Some(state) = self.state.lock().expect("sink mutex").take() {
            state.write.abort();
        }
    }
}
