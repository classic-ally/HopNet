//! Reconciliation-scan scenarios (spec §Discovery, §Failure Handling line on
//! terminal-resource re-enqueue, and the offline-deletion edge cases).

use chrono::{Duration, Utc};
use ingress_core::fixtures::{AssetDescriptorBuilder, add_shared, store_with_personal};
use ingress_core::model::ResourceType;
use ingress_core::resolve::{SeedOutcome, seed_descriptor};
use ingress_core::scan::{ScanProbe, ScanVerdict, begin, finish, probe};
use ingress_core::{AssetDescriptor, LibraryScope, PhotoId, StateStore};

fn probe_of(desc: &AssetDescriptor) -> ScanProbe {
    ScanProbe {
        local_id: desc.local_id.clone(),
        cloud_id: desc.cloud_id.clone(),
        scope: desc.scope,
        asset_modified_at: desc.asset_modified_at,
    }
}

async fn seed_one(store: &StateStore, desc: &AssetDescriptor) -> PhotoId {
    match seed_descriptor(store, desc).await.expect("seed") {
        SeedOutcome::MintedPending { photo_id, .. } => photo_id,
        other => panic!("expected MintedPending, got {other:?}"),
    }
}

// Impact: a 50k-asset library is probed on every scan — if unchanged assets
// don't verdict Done, every scan pays the full resource-enumeration cost the
// probe protocol exists to avoid.
// Should: Done for a known, active, unmodified asset — and mark it seen (no
// deletion synthesized at finish).
#[tokio::test]
async fn probe_done_for_unchanged_marks_seen() {
    let (store, _) = store_with_personal().await;
    let _tmp = tempfile::tempdir().unwrap();
    let desc = AssetDescriptorBuilder::simple_image()
        .modified_at(Utc::now())
        .build();
    let id = seed_one(&store, &desc).await;

    let scan = begin(&store).await.unwrap();
    assert_eq!(
        probe(&store, &scan, &probe_of(&desc)).await.unwrap(),
        ScanVerdict::Done
    );

    let summary = finish(&store, &scan, 1, 5).await.unwrap();
    assert_eq!(summary.deletions_synthesized, 0);
    assert!(
        store
            .photo(&id)
            .await
            .unwrap()
            .unwrap()
            .deleted_at
            .is_none()
    );
}

// Impact: every change class must survive the light filter — a probe that
// says Done for a modified/tombstoned/moved/unknown asset means scans
// silently miss changes.
// Should: NeedsFull for unknown, tombstoned (restore), scope-flipped,
// modified, and unmapped-now-bound assets.
#[tokio::test]
async fn probe_needs_full_for_every_change_class() {
    let (store, _) = store_with_personal().await;
    let shared = add_shared(&store).await;
    let _ = shared;
    let _tmp = tempfile::tempdir().unwrap();
    let t1 = Utc::now();

    let scan = begin(&store).await.unwrap();

    // Unknown asset.
    let unknown = AssetDescriptorBuilder::simple_image()
        .modified_at(t1)
        .build();
    assert_eq!(
        probe(&store, &scan, &probe_of(&unknown)).await.unwrap(),
        ScanVerdict::NeedsFull
    );

    // Tombstoned (restore pending).
    let tomb_desc = AssetDescriptorBuilder::simple_image()
        .modified_at(t1)
        .build();
    seed_one(&store, &tomb_desc).await;
    ingress_core::classify::apply_removal(&store, &tomb_desc.local_id)
        .await
        .unwrap();
    assert_eq!(
        probe(&store, &scan, &probe_of(&tomb_desc)).await.unwrap(),
        ScanVerdict::NeedsFull
    );

    // Scope flip.
    let flip_desc = AssetDescriptorBuilder::simple_image()
        .modified_at(t1)
        .build();
    seed_one(&store, &flip_desc).await;
    let mut flipped = probe_of(&flip_desc);
    flipped.scope = LibraryScope::Shared;
    assert_eq!(
        probe(&store, &scan, &flipped).await.unwrap(),
        ScanVerdict::NeedsFull
    );

    // Metadata modified.
    let mod_desc = AssetDescriptorBuilder::simple_image()
        .modified_at(t1)
        .build();
    seed_one(&store, &mod_desc).await;
    let mut newer = probe_of(&mod_desc);
    newer.asset_modified_at = Some(t1 + Duration::seconds(5));
    assert_eq!(
        probe(&store, &scan, &newer).await.unwrap(),
        ScanVerdict::NeedsFull
    );

    // Never-synced (stored NULL modification date).
    let null_desc = AssetDescriptorBuilder::simple_image().build();
    seed_one(&store, &null_desc).await;
    assert_eq!(
        probe(&store, &scan, &probe_of(&null_desc)).await.unwrap(),
        ScanVerdict::NeedsFull
    );
}

// Impact: offline deletion is detectable ONLY here — and over-synthesis
// (tombstoning seen or scan-window-minted photos) restarts retention clocks
// on live photos.
// Should: tombstone exactly the unseen pre-scan photos, with
// deletion_observed, timestamped at the scan moment.
#[tokio::test]
async fn finish_synthesizes_deletions_for_unseen_only() {
    let (store, _) = store_with_personal().await;
    let _tmp = tempfile::tempdir().unwrap();
    let t1 = Utc::now();

    let seen_desc = AssetDescriptorBuilder::simple_image()
        .modified_at(t1)
        .build();
    let seen_id = seed_one(&store, &seen_desc).await;
    let gone_desc = AssetDescriptorBuilder::simple_image()
        .modified_at(t1)
        .build();
    let gone_id = seed_one(&store, &gone_desc).await;

    let scan = begin(&store).await.unwrap();
    probe(&store, &scan, &probe_of(&seen_desc)).await.unwrap();
    // `gone` is never probed — absent from PhotoKit.

    // Minted mid-scan (observer insert racing the enumeration): protected by
    // the discovered_at >= started_at guard.
    let midscan_desc = AssetDescriptorBuilder::simple_image()
        .modified_at(t1)
        .build();
    let midscan_id = seed_one(&store, &midscan_desc).await;

    let summary = finish(&store, &scan, 2, 5).await.unwrap();
    assert_eq!(summary.deletions_synthesized, 1);
    assert!(
        store
            .photo(&gone_id)
            .await
            .unwrap()
            .unwrap()
            .deleted_at
            .is_some()
    );
    assert!(
        store
            .photo(&seen_id)
            .await
            .unwrap()
            .unwrap()
            .deleted_at
            .is_none()
    );
    assert!(
        store
            .photo(&midscan_id)
            .await
            .unwrap()
            .unwrap()
            .deleted_at
            .is_none()
    );
    assert_eq!(
        store.log_events("deletion_observed").await.unwrap().len(),
        1
    );
}

// Impact: spec — "absence of evidence from PhotoKit is only evidence of
// deletion when the API is healthy". Lost TCC authorization enumerates zero
// assets; synthesizing there would mass-tombstone the whole library.
// Should not: synthesize (or reset retries) when 0 enumerated vs non-empty db.
#[tokio::test]
async fn finish_with_zero_enumeration_skips_synthesis() {
    let (store, _) = store_with_personal().await;
    let _tmp = tempfile::tempdir().unwrap();
    let desc = AssetDescriptorBuilder::simple_image()
        .modified_at(Utc::now())
        .build();
    let id = seed_one(&store, &desc).await;

    let scan = begin(&store).await.unwrap();
    let summary = finish(&store, &scan, 0, 5).await.unwrap();
    assert!(summary.synthesis_skipped);
    assert_eq!(summary.deletions_synthesized, 0);
    assert!(
        store
            .photo(&id)
            .await
            .unwrap()
            .unwrap()
            .deleted_at
            .is_none()
    );
}

// Impact: transient iCloud outages must self-heal without operator action —
// the scan is the sanctioned re-enqueue point for gave-up resources.
// Should: reset terminal retry state and report the count in the summary +
// scan_completed event.
#[tokio::test]
async fn finish_resets_gave_up_and_logs_counts() {
    let (store, _) = store_with_personal().await;
    let _tmp = tempfile::tempdir().unwrap();
    let desc = AssetDescriptorBuilder::simple_image()
        .modified_at(Utc::now())
        .build();
    let id = seed_one(&store, &desc).await;

    let cap = 2i64;
    for _ in 0..cap {
        store
            .record_resource_failure(&id, ResourceType::Original, "icloud down", Utc::now(), cap)
            .await
            .unwrap();
    }

    let scan = begin(&store).await.unwrap();
    probe(&store, &scan, &probe_of(&desc)).await.unwrap();
    let summary = finish(&store, &scan, 1, cap).await.unwrap();
    assert_eq!(summary.gave_up_reset, 1);

    let row = store
        .resources_for_photo(&id)
        .await
        .unwrap()
        .into_iter()
        .find(|r| r.resource_type == ResourceType::Original)
        .unwrap();
    assert_eq!(row.retry_count, 0);

    let events = store.log_events("scan_completed").await.unwrap();
    assert_eq!(events.len(), 1);
    let detail: serde_json::Value =
        serde_json::from_str(events[0].detail.as_ref().unwrap()).unwrap();
    assert_eq!(detail["gave_up_reset"], 1);
    assert_eq!(detail["probed"], 1);
}
