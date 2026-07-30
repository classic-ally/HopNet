//! E2E driver for the publish flow: fabricate a real ingress state (state.db
//! + blobs + sidecars, via the actual seed → drain pipeline with an
//! in-memory fetcher — no PhotoKit) and publish it into a live node.
//!
//! Used by the orchestrator's `photos-ingress-publish` scenario and as a dev
//! tool against a local node. Output is JSON on stdout for machine
//! consumption; the per-resource `blake3` is the ingress content hash, which
//! equals the blake3 of the plaintext bytes the node must serve back.
//!
//! Commands:
//!   seed             --data-dir D [--count N]     fabricate N photos
//!   publish          --data-dir D --node-url U --device-token T
//!   reset-published  --data-dir D                 clear published_at (probe)

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use ingress_core::descriptor::AssetDescriptor;
use ingress_core::fixtures::AssetDescriptorBuilder;
use ingress_core::paths::DataDir;
use ingress_core::publish::{PublishState, claim_publishable, run_publish_pass};
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
    /// Fabricate a personal library with N fully-materialized photos.
    Seed {
        #[arg(long, default_value_t = 6)]
        count: u32,
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
}

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
        let salt = (request.ph_resource_type as u32).wrapping_mul(0x9E37);
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
    published_at: Option<String>,
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
            published_at: photo.published_at.map(|t| t.to_rfc3339()),
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
        Cmd::Seed { count } => {
            let library_id = ingress_core::LibraryId::new("personal");
            if store.library(&library_id).await.expect("library").is_none() {
                let blob_root = args.data_dir.join("blob-root");
                std::fs::create_dir_all(&blob_root).expect("create blob root");
                store
                    .insert_library(&ingress_core::LibraryConfig {
                        library_id: library_id.clone(),
                        display_name: "Personal".into(),
                        blob_root: blob_root.to_string_lossy().into_owned(),
                        sidecar_root_remote: None,
                        scope_binding: None,
                        retention_days: 30,
                        created_at: chrono::Utc::now(),
                    })
                    .await
                    .expect("insert library");
            }

            let mut descriptors = std::collections::HashMap::new();
            for index in 0..count {
                let desc = AssetDescriptorBuilder::simple_image()
                    .with_cloud_id(&format!("e2e-cloud-{index}"))
                    .with_local_id(&format!("e2e-{index}"))
                    .with_thumbnails()
                    .build();
                match seed_descriptor(&store, &desc).await.expect("seed") {
                    SeedOutcome::MintedPending { .. } => {}
                    other => panic!("expected MintedPending, got {other:?}"),
                }
                descriptors.insert(desc.local_id.clone(), desc);
            }

            let scheduler = Scheduler::new(
                store.clone(),
                data_dir.clone(),
                Arc::new(MemoryFetcher { descriptors }),
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
            let mut state = PublishState::default();
            let mut totals = ingress_core::publish::PublishReport::default();
            loop {
                let claimed = claim_publishable(&store, &cfg, &HashSet::new())
                    .await
                    .expect("claim");
                if claimed.is_empty() {
                    break;
                }
                let report = run_publish_pass(&store, &data_dir, &publisher, &cfg, claimed, &mut state)
                    .await
                    .expect("publish pass");
                let parked = report.parked || report.parked_responsibility;
                totals.absorb(&report);
                if parked {
                    break;
                }
            }

            let out = serde_json::json!({
                "published": totals.published,
                "already_published": totals.already_published,
                "adopted": totals.adopted,
                "failed": totals.failed,
                "gave_up": totals.gave_up,
                "missing_sidecar": totals.missing_sidecar,
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
    }
}
