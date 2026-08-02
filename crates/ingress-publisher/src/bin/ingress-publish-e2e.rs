//! E2E driver for the publish flow: fabricate a real ingress state (state.db,
//! blobs and sidecars, via the actual seed → drain pipeline with an in-memory
//! fetcher — no PhotoKit) and publish it into a live node.
//!
//! Used by the orchestrator's `photos-ingress-publish` scenario and as a dev
//! tool against a local node. Output is JSON on stdout for machine
//! consumption; the per-resource `blake3` is the ingress content hash, which
//! equals the blake3 of the plaintext bytes the node must serve back.
//!
//! Commands:
//!   seed             --data-dir D [--count N] [--mesh-library-id U]
//!                    fabricate N photos (personal, or shared when a mesh
//!                    library id is given)
//!   publish          --data-dir D --node-url U --device-token T
//!                    drains BOTH queues: unpublished photos, and published
//!                    photos whose tombstone state the mesh has not been told
//!   reset-published  --data-dir D                 clear published_at (probe)
//!   tombstone        --data-dir D [--restore]     set/clear deleted_at on
//!                    every photo, standing in for a PhotoKit delete or a
//!                    restore out of Recently Deleted
//!   edit             --data-dir D [--revert] [--metadata-only]
//!                    re-deliver every asset's descriptor with an edited
//!                    render added (or removed, or with only its
//!                    modification date bumped) and drain — the real
//!                    classify → reopen → refetch chain, not a DB poke
//!
//! Publish exit codes: 0 = queue drained; 2 = node unreachable (pass
//! parked); 3 = SOME publish scope parked on responsibility — since the
//! pass is scope-partitioned, healthy scopes were still drained first
//! (check `published`/`parked_responsibility` in the JSON, not just the
//! code).

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use ingress_core::descriptor::AssetDescriptor;
use ingress_core::fixtures::AssetDescriptorBuilder;
use ingress_core::paths::DataDir;
use ingress_core::publish::{
    PassWork, PublishState, claim_editable, claim_publishable, claim_tombstone_propagatable,
    run_publish_pass,
};
use ingress_core::scheduler::{
    CancelToken, FetchFailure, FetchRequest, FreeSpaceProbe, ResourceFetcher, Scheduler,
    SchedulerConfig, StreamSink,
};
use ingress_core::{PhotoRecord, SeedOutcome, StateStore, seed_descriptor};
use ingress_publisher::NodePublisher;

#[derive(Parser)]
#[command(name = "ingress-publish-e2e")]
struct Args {
    /// Ingress data dir (state.db, sidecars). Created by `seed`.
    #[arg(long)]
    data_dir: PathBuf,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Fabricate a library with N fully-materialized photos — personal by
    /// default, or the SPL-bound shared library (mesh-bound, publishable)
    /// when `--mesh-library-id` is given.
    Seed {
        #[arg(long, default_value_t = 6)]
        count: u32,
        /// First asset index (cloud/local ids derive from the absolute
        /// index) — lets a scenario add NEW assets to an existing dir, or
        /// seed a second dir with the same identities as another.
        #[arg(long, default_value_t = 0)]
        start: u32,
        /// Seed into the shared library instead, bound to this consensus
        /// shared_libraries UUID as its publish target.
        #[arg(long)]
        mesh_library_id: Option<String>,
    },
    /// Publish everything claimable into the node; loops until the queue is
    /// drained or the pass parks.
    Publish {
        #[arg(long)]
        node_url: String,
        #[arg(long)]
        device_token: String,
    },
    /// Clear published_at on every photo (idempotency probe: a re-publish
    /// must confirm-first and report already_published, never re-submit).
    ResetPublished,
    /// Mark every photo deleted (or, with --restore, alive again) as though
    /// PhotoKit had observed it — drives the propagation queue without a
    /// Mac. Leaves `tombstone_published_at` alone; the next publish pass is
    /// what tells the mesh.
    Tombstone {
        /// Clear deleted_at instead of setting it (restore from Recently
        /// Deleted).
        #[arg(long)]
        restore: bool,
    },
    /// Re-deliver every seeded asset's descriptor as PhotoKit would after
    /// the user edited it, then drain. Goes through classify, so the reopen
    /// and refetch this exercises are the production ones.
    Edit {
        /// Drop the edited render again (Revert to Original).
        #[arg(long)]
        revert: bool,
        /// Bump only the modification date — no resource set change, no new
        /// bytes. Drives the metadata half of the edit queue.
        #[arg(long)]
        metadata_only: bool,
    },
}

struct MaxProbe;

impl FreeSpaceProbe for MaxProbe {
    fn free_bytes(&self, _: &std::path::Path) -> ingress_core::Result<u64> {
        Ok(u64::MAX)
    }
}

/// In-memory fetcher: deterministic bytes per (index, resource), so a
/// re-seed with the same count reproduces identical content hashes.
struct MemoryFetcher {
    descriptors: std::collections::HashMap<String, AssetDescriptor>,
    /// Bumped by `edit` so a refetched render yields DIFFERENT bytes.
    /// Mixed in only for edit-mutable types: an Original is never reopened,
    /// and changing its hash would look like corruption rather than an edit.
    generation: u32,
}

/// What the descriptor should claim about the asset's edited state. The
/// seed and the `edit` command build descriptors the same way so a
/// re-delivery differs from the original in exactly one respect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EditShape {
    /// As shot: original + thumbnails.
    Unedited,
    /// Adjusted: a `fullSizePhoto` render alongside the original.
    Edited,
}

/// The descriptor for one seeded asset. Deterministic in `index`, so any
/// command can reconstruct exactly what `seed` delivered.
///
/// `modified_bump` seconds are added to the modification date: PhotoKit
/// advances it on every change, and classify's fast path treats an
/// unchanged date as "nothing happened".
fn seeded_descriptor(
    index: u32,
    scope: ingress_core::descriptor::LibraryScope,
    shape: EditShape,
    modified_bump: i64,
) -> AssetDescriptor {
    let mut builder = AssetDescriptorBuilder::simple_image()
        .with_cloud_id(&format!("e2e-cloud-{index}"))
        .with_local_id(&format!("e2e-{index}"))
        .with_thumbnails()
        .scope(scope);
    if shape == EditShape::Edited {
        builder = builder.with_ph_resource(PH_FULL_SIZE_PHOTO, "public.heic");
    }
    let mut desc = builder.build();
    // A fixed epoch plus the bump: reproducible across processes, and
    // strictly increasing per bump so no delivery reads as stale.
    desc.asset_modified_at = Some(
        chrono::DateTime::from_timestamp(1_780_000_000 + modified_bump, 0)
            .expect("valid timestamp"),
    );
    desc
}

/// PhotoKit's `photo` resource — the Original, which an edit never replaces.
const PH_PHOTO: i32 = 1;
/// PhotoKit's `fullSizePhoto` — the edited render an adjustment produces.
const PH_FULL_SIZE_PHOTO: i32 = 5;

fn photo_bytes(index: u32) -> Vec<u8> {
    // ~96 KiB patterned payload, unique per index.
    (0..96 * 1024u32)
        .map(|i| ((i.wrapping_mul(31).wrapping_add(index.wrapping_mul(7919))) % 251) as u8)
        .collect()
}

impl ResourceFetcher for MemoryFetcher {
    fn descriptor_for(&self, local_id: &str) -> Result<AssetDescriptor, FetchFailure> {
        self.descriptors
            .get(local_id)
            .cloned()
            .ok_or_else(|| FetchFailure::AssetUnavailable(format!("no asset {local_id}")))
    }

    fn fetch_resource(
        &self,
        request: FetchRequest,
        sink: Arc<StreamSink>,
    ) -> Result<(), FetchFailure> {
        let index: u32 = request
            .local_id
            .strip_prefix("e2e-")
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| FetchFailure::AssetUnavailable("bad local id".into()))?;
        // Mix the resource type in so original/thumbnail_small/medium hash
        // distinctly (otherwise all three would dedup to one blob).
        let mut salt = (request.ph_resource_type as u32).wrapping_mul(0x9E37);
        if request.ph_resource_type != PH_PHOTO {
            salt ^= self.generation.wrapping_mul(0x85EB);
        }
        sink.write(&photo_bytes(index ^ salt))?;
        Ok(())
    }
}

#[derive(serde::Serialize)]
struct ResourceReport {
    r#type: String,
    size_bytes: i64,
    blake3: String,
}

#[derive(serde::Serialize)]
struct PhotoReport {
    photo_id: String,
    cloud_id: Option<String>,
    published_at: Option<String>,
    /// Set when the photo was ADOPTED (mesh already held it) — the remote
    /// consensus identity; NULL = self-published or unpublished.
    consensus_photo_id: Option<String>,
    resources: Vec<ResourceReport>,
}

async fn photo_reports(store: &StateStore) -> Vec<PhotoReport> {
    let photos: Vec<PhotoRecord> = sqlx::query_as("SELECT * FROM photos ORDER BY photo_id")
        .fetch_all(store.raw_pool())
        .await
        .expect("list photos");
    let mut reports = Vec::with_capacity(photos.len());
    for photo in photos {
        let resources = store
            .resources_for_photo(&photo.photo_id)
            .await
            .expect("resources")
            .into_iter()
            .filter(|r| r.written_at.is_some())
            .map(|r| ResourceReport {
                r#type: r.resource_type.as_str().to_string(),
                size_bytes: r.size_bytes.unwrap_or_default(),
                blake3: r.content_hash.map(|h| h.as_str().to_string()).unwrap_or_default(),
            })
            .collect();
        reports.push(PhotoReport {
            photo_id: photo.photo_id.as_str().to_string(),
            cloud_id: photo.cloud_id.clone(),
            published_at: photo.published_at.map(|t| t.to_rfc3339()),
            consensus_photo_id: photo.consensus_photo_id.clone(),
            resources,
        });
    }
    reports
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    std::fs::create_dir_all(&args.data_dir).expect("create data dir");
    let store = StateStore::open(&args.data_dir.join("state.db"))
        .await
        .expect("open state.db");
    let data_dir = DataDir::new(&args.data_dir);

    match args.cmd {
        Cmd::Seed {
            count,
            start,
            mesh_library_id,
        } => {
            // The personal library always exists (scope routing needs it);
            // shared-mode seeding additionally provisions the SPL-bound
            // library with its mesh publish target.
            let library_id = ingress_core::LibraryId::new("personal");
            if store.library(&library_id).await.expect("library").is_none() {
                store
                    .insert_library(&ingress_core::LibraryConfig {
                        library_id: library_id.clone(),
                        display_name: "Personal".into(),
                        scope_binding: None,
                        retention_days: 30,
                        created_at: chrono::Utc::now(),
                        mesh_library_id: None,
                    })
                    .await
                    .expect("insert library");
            }
            let scope = match &mesh_library_id {
                None => ingress_core::descriptor::LibraryScope::Personal,
                Some(mesh) => {
                    let shared_id = ingress_core::LibraryId::new("shared");
                    if store.library(&shared_id).await.expect("library").is_none() {
                        store
                            .insert_library(&ingress_core::LibraryConfig {
                                library_id: shared_id,
                                display_name: "Shared".into(),
                                scope_binding: Some(
                                    ingress_core::model::ICLOUD_SHARED_LIBRARY_BINDING.into(),
                                ),
                                retention_days: 30,
                                created_at: chrono::Utc::now(),
                                mesh_library_id: Some(mesh.clone()),
                            })
                            .await
                            .expect("insert shared library");
                    }
                    ingress_core::descriptor::LibraryScope::Shared
                }
            };

            let mut descriptors = std::collections::HashMap::new();
            for index in start..start + count {
                let desc = seeded_descriptor(index, scope, EditShape::Unedited, 0);
                match seed_descriptor(&store, &desc).await.expect("seed") {
                    SeedOutcome::MintedPending { .. } => {}
                    other => panic!("expected MintedPending, got {other:?}"),
                }
                descriptors.insert(desc.local_id.clone(), desc);
            }

            let scheduler = Scheduler::new(
                store.clone(),
                data_dir.clone(),
                Arc::new(MemoryFetcher {
                    descriptors,
                    generation: 0,
                }),
                Arc::new(MaxProbe),
                SchedulerConfig::default(),
                CancelToken::default(),
            );
            let report = scheduler.drain().await.expect("drain");
            assert_eq!(
                report.photos_completed, count as u64,
                "every seeded photo must materialize"
            );

            let out = serde_json::json!({
                "seeded": count,
                "photos": photo_reports(&store).await,
            });
            println!("{}", serde_json::to_string_pretty(&out).unwrap());
        }

        Cmd::Publish {
            node_url,
            device_token,
        } => {
            let publisher = NodePublisher::new(&node_url, &device_token).expect("publisher");
            let cfg = ingress_core::publish::PublishConfig::default();
            let spool = data_dir.spool();
            let mut state = PublishState::default();
            let mut totals = ingress_core::publish::PublishReport::default();
            loop {
                let claimed = claim_publishable(&store, &cfg, &HashSet::new())
                    .await
                    .expect("claim");
                let propagatable = claim_tombstone_propagatable(&store, &cfg, &HashSet::new())
                    .await
                    .expect("claim propagatable");
                let editable = claim_editable(&store, &cfg, &HashSet::new())
                    .await
                    .expect("claim editable");
                if claimed.is_empty() && propagatable.is_empty() && editable.is_empty() {
                    break;
                }
                let report = run_publish_pass(
                    &store,
                    &spool,
                    &publisher,
                    &cfg,
                    PassWork {
                        claimed,
                        propagatable,
                        editable,
                    },
                    &mut state,
                )
                .await
                .expect("publish pass");
                // Unreachable parks the whole pass — nothing more can move.
                // A responsibility park is PER SCOPE: a mixed claim batch may
                // park one scope while another still has queued photos, so
                // keep passing until a pass moves nothing (parked photos
                // burn no attempts and stay claimable — a zero-progress pass
                // means only parked scopes remain). Propagation counts as
                // progress: a pass that only told the mesh about deletes
                // still moved the queue forward.
                let progress = report.published
                    + report.already_published
                    + report.adopted
                    + report.tombstones_propagated
                    + report.restores_propagated
                    + report.edits_propagated
                    + report.metadata_propagated;
                let parked = report.parked;
                totals.absorb(&report);
                if parked || progress == 0 {
                    break;
                }
            }

            let out = serde_json::json!({
                "published": totals.published,
                "already_published": totals.already_published,
                "adopted": totals.adopted,
                "failed": totals.failed,
                "gave_up": totals.gave_up,
                "missing_descriptor": totals.missing_descriptor,
                "tombstones_propagated": totals.tombstones_propagated,
                "restores_propagated": totals.restores_propagated,
                "edits_propagated": totals.edits_propagated,
                "metadata_propagated": totals.metadata_propagated,
                "evicted_blobs": totals.evicted_blobs,
                "parked": totals.parked,
                "parked_responsibility": totals.parked_responsibility,
                "photos": photo_reports(&store).await,
            });
            println!("{}", serde_json::to_string_pretty(&out).unwrap());
            if totals.parked {
                std::process::exit(2);
            }
            if totals.parked_responsibility {
                // Distinct from unreachable-park so the orchestrator can
                // assert the explicit-claim contract cheaply.
                std::process::exit(3);
            }
        }

        Cmd::ResetPublished => {
            let cleared = sqlx::query("UPDATE photos SET published_at = NULL")
                .execute(store.raw_pool())
                .await
                .expect("reset")
                .rows_affected();
            println!("{}", serde_json::json!({ "reset": cleared }));
        }

        Cmd::Tombstone { restore } => {
            let affected = if restore {
                sqlx::query("UPDATE photos SET deleted_at = NULL WHERE deleted_at IS NOT NULL")
                    .execute(store.raw_pool())
                    .await
                    .expect("restore")
                    .rows_affected()
            } else {
                sqlx::query("UPDATE photos SET deleted_at = ? WHERE deleted_at IS NULL")
                    .bind(chrono::Utc::now())
                    .execute(store.raw_pool())
                    .await
                    .expect("tombstone")
                    .rows_affected()
            };
            let key = if restore { "restored" } else { "tombstoned" };
            println!("{}", serde_json::json!({ key: affected }));
        }

        Cmd::Edit {
            revert,
            metadata_only,
        } => {
            // Each mode is one step along the same asset's history, so the
            // modification date advances with it — classify's fast path
            // treats an unchanged date as "nothing happened". The byte
            // generation advances only when a render is actually re-made.
            //
            // A metadata refresh keeps whatever shape the asset already has:
            // re-delivering the edited render would make it a content edit,
            // which is precisely what this mode exists to exclude.
            let (bump, generation, key) = match (revert, metadata_only) {
                (_, true) => (3, 2, "metadata_refreshed"),
                (true, false) => (2, 2, "reverted"),
                (false, false) => (1, 1, "edited"),
            };

            let photos: Vec<PhotoRecord> =
                sqlx::query_as("SELECT * FROM photos WHERE local_id IS NOT NULL ORDER BY photo_id")
                    .fetch_all(store.raw_pool())
                    .await
                    .expect("list photos");
            let scope = |photo: &PhotoRecord| match photo.library_id.as_ref().map(|l| l.as_str()) {
                Some("shared") => ingress_core::descriptor::LibraryScope::Shared,
                _ => ingress_core::descriptor::LibraryScope::Personal,
            };

            let mut descriptors = std::collections::HashMap::new();
            let mut affected = 0u64;
            for photo in &photos {
                let local_id = photo.local_id.clone().expect("filtered on local_id");
                let index: u32 = local_id
                    .strip_prefix("e2e-")
                    .and_then(|s| s.parse().ok())
                    .expect("seeded local id");
                let shape = if metadata_only {
                    let live = store
                        .resources_for_photo(&photo.photo_id)
                        .await
                        .expect("resources");
                    if live
                        .iter()
                        .any(|r| r.resource_type == ingress_core::model::ResourceType::Edited)
                    {
                        EditShape::Edited
                    } else {
                        EditShape::Unedited
                    }
                } else if revert {
                    EditShape::Unedited
                } else {
                    EditShape::Edited
                };
                let desc = seeded_descriptor(index, scope(photo), shape, bump);
                descriptors.insert(local_id, desc.clone());
                ingress_core::classify::apply_change(&store, &data_dir.spool(), &desc)
                    .await
                    .expect("apply change");
                affected += 1;
            }

            // Drain refetches whatever classify reopened. A metadata-only
            // delivery reopens nothing, so this is a no-op there.
            let scheduler = Scheduler::new(
                store.clone(),
                data_dir.clone(),
                Arc::new(MemoryFetcher {
                    descriptors,
                    generation,
                }),
                Arc::new(MaxProbe),
                SchedulerConfig::default(),
                CancelToken::default(),
            );
            scheduler.drain().await.expect("drain");

            let out = serde_json::json!({
                key: affected,
                "photos": photo_reports(&store).await,
            });
            println!("{}", serde_json::to_string_pretty(&out).unwrap());
        }
    }
}
