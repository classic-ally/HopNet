//! Fixture `AssetDescriptor` builders for tests (this crate's and downstream
//! consumers'). Shapes mirror the spike-verified resource sets in
//! `spikes/photokit/FINDINGS.md`.

use chrono::{DateTime, Utc};

use crate::descriptor::{
    AssetDescriptor, BurstInfo, CaptureMetadata, LibraryScope, MediaType, ResourceDescriptor,
};
use crate::ids::LibraryId;
use crate::model::{ICLOUD_SHARED_LIBRARY_BINDING, LibraryConfig};
use crate::store::StateStore;

/// An in-memory store seeded with a `personal` library.
pub async fn store_with_personal() -> (StateStore, LibraryId) {
    let store = StateStore::open_in_memory()
        .await
        .expect("open in-memory store");
    let id = LibraryId::new("personal");
    store
        .insert_library(&LibraryConfig {
            library_id: id.clone(),
            display_name: "Personal".into(),
            blob_root: "/tmp/blobs/personal".into(),
            sidecar_root_remote: None,
            scope_binding: None,
            retention_days: 30,
            created_at: Utc::now(),
        })
        .await
        .expect("seed personal library");
    (store, id)
}

/// Add a shared library bound to the iCloud Shared Photo Library marker.
pub async fn add_shared(store: &StateStore) -> LibraryId {
    let id = LibraryId::new("shared_household");
    store
        .insert_library(&LibraryConfig {
            library_id: id.clone(),
            display_name: "Shared".into(),
            blob_root: "/tmp/blobs/shared".into(),
            sidecar_root_remote: None,
            scope_binding: Some(ICLOUD_SHARED_LIBRARY_BINDING.into()),
            retention_days: 30,
            created_at: Utc::now(),
        })
        .await
        .expect("seed shared library");
    id
}

fn resource(ph_type: i32, uti: &str) -> ResourceDescriptor {
    ResourceDescriptor {
        ph_resource_type: ph_type,
        uti: uti.to_string(),
        original_filename: None,
        expected_size: Some(2_000_000),
        locally_available: Some(false),
    }
}

/// Fluent builder over a sensible default descriptor.
pub struct AssetDescriptorBuilder {
    desc: AssetDescriptor,
}

impl AssetDescriptorBuilder {
    fn base(resources: Vec<ResourceDescriptor>, media_type: MediaType) -> Self {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Self {
            desc: AssetDescriptor {
                local_id: format!("FIXTURE-LOCAL-{n}/L0/001"),
                cloud_id: Some(format!("FIXTURE-CLOUD-{n}:001")),
                scope: LibraryScope::Personal,
                media_type,
                media_subtypes: vec![],
                asset_modified_at: None,
                favorite: false,
                burst: None,
                capture: CaptureMetadata::default(),
                resources,
            },
        }
    }

    /// Plain photo: resource set `[photo]`.
    pub fn simple_image() -> Self {
        Self::base(vec![resource(1, "public.heic")], MediaType::Image)
    }

    /// Live Photo: `[photo, pairedVideo]`.
    pub fn live_photo() -> Self {
        Self::base(
            vec![
                resource(1, "public.heic"),
                resource(9, "com.apple.quicktime-movie"),
            ],
            MediaType::LivePhoto,
        )
    }

    /// Edited Live Photo, the five-resource shape observed live in the spike:
    /// `[photo, adjustmentData, pairedVideo, fullSizePhoto, fullSizePairedVideo]`.
    pub fn edited_live_photo() -> Self {
        Self::base(
            vec![
                resource(1, "public.heic"),
                resource(7, "com.apple.property-list"),
                resource(9, "com.apple.quicktime-movie"),
                resource(5, "public.heic"),
                resource(10, "com.apple.quicktime-movie"),
            ],
            MediaType::LivePhoto,
        )
    }

    /// RAW+JPEG pair: `[photo, alternatePhoto]`.
    pub fn raw_jpeg() -> Self {
        Self::base(
            vec![
                resource(1, "public.jpeg"),
                resource(4, "com.sony.arw-raw-image"),
            ],
            MediaType::Image,
        )
    }

    /// One frame of a burst.
    pub fn burst_frame(burst_identifier: &str, is_pick: bool) -> Self {
        let mut b = Self::simple_image();
        b.desc.burst = Some(BurstInfo {
            burst_identifier: burst_identifier.to_string(),
            is_pick,
        });
        b
    }

    /// Remove the cloud identifier (local-only asset).
    pub fn local_only(mut self) -> Self {
        self.desc.cloud_id = None;
        self
    }

    pub fn with_cloud_id(mut self, cloud_id: &str) -> Self {
        self.desc.cloud_id = Some(cloud_id.to_string());
        self
    }

    pub fn with_local_id(mut self, local_id: &str) -> Self {
        self.desc.local_id = local_id.to_string();
        self
    }

    pub fn scope(mut self, scope: LibraryScope) -> Self {
        self.desc.scope = scope;
        self
    }

    pub fn modified_at(mut self, at: DateTime<Utc>) -> Self {
        self.desc.asset_modified_at = Some(at);
        self
    }

    /// Append a raw resource by PH type (e.g. an unknown type for the
    /// archive-known-and-log tests).
    pub fn with_ph_resource(mut self, ph_type: i32, uti: &str) -> Self {
        self.desc.resources.push(resource(ph_type, uti));
        self
    }

    pub fn build(self) -> AssetDescriptor {
        self.desc
    }
}
