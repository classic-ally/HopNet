//! Tier-3 disaster recovery (spec §Recovery Tier 3): explicit rebuild of
//! `state.db` from a storage root — snapshot-first, sidecar-tree fallback.
//! Blob-only recovery is deliberately NOT implemented (Phase 6 decision):
//! it would mint fresh photo_ids and lose all PhotoKit-derived metadata;
//! the inventory error explains what survived instead.
//!
//! `state.db` is dead in this scenario, so library configuration (which
//! roots to search/walk) comes from CLI arguments, not the store.

use std::fs;
use std::path::{Path, PathBuf};

use chrono::Utc;

use crate::descriptor::LibraryScope;
use crate::error::{IngressError, Result};
use crate::ids::LibraryId;
use crate::model::{GroupType, ICLOUD_SHARED_LIBRARY_BINDING, LibraryConfig, ResourceType};
use crate::paths::{BlobPaths, DataDir};
use crate::sidecar::Sidecar;
use crate::sidecar_io::walk_sidecars;
use crate::store::StateStore;

/// One library's recovery configuration, user-supplied
/// (`--library id=brave_otter,blob=/Volumes/p,sidecars=/Volumes/p/sidecars`).
#[derive(Debug, Clone)]
pub struct RecoverLibrarySpec {
    pub library_id: LibraryId,
    pub blob_root: PathBuf,
    /// The remote sidecar tree — both the rebuild source and the stored
    /// `sidecar_root_remote` of the recovered row.
    pub sidecar_root_remote: Option<PathBuf>,
    pub scope: LibraryScope,
    pub display_name: Option<String>,
    pub retention_days: i64,
}

impl RecoverLibrarySpec {
    /// Parse the `key=value,...` CLI form. Keys: `id` (required), `blob`
    /// (required, absolute), `sidecars`, `scope` (`personal`|`shared`,
    /// default personal), `retention` (days, default 30), `name`.
    pub fn parse(s: &str) -> Result<Self> {
        let mut id = None;
        let mut blob = None;
        let mut sidecars = None;
        let mut scope = LibraryScope::Personal;
        let mut retention = 30;
        let mut name = None;
        for pair in s.split(',') {
            let (key, value) = pair.split_once('=').ok_or_else(|| {
                IngressError::Invariant(format!("library spec: expected key=value, got {pair:?}"))
            })?;
            match key {
                "id" => id = Some(LibraryId::parse(value)?),
                "blob" => blob = Some(PathBuf::from(value)),
                "sidecars" => sidecars = Some(PathBuf::from(value)),
                "scope" => {
                    scope = match value {
                        "personal" => LibraryScope::Personal,
                        "shared" => LibraryScope::Shared,
                        other => {
                            return Err(IngressError::Invariant(format!(
                                "library spec: scope must be personal|shared, got {other:?}"
                            )));
                        }
                    }
                }
                "retention" => {
                    retention = value.parse().map_err(|_| {
                        IngressError::Invariant(format!(
                            "library spec: retention must be a day count, got {value:?}"
                        ))
                    })?
                }
                "name" => name = Some(value.to_string()),
                other => {
                    return Err(IngressError::Invariant(format!(
                        "library spec: unknown key {other:?}"
                    )));
                }
            }
        }
        let blob_root: PathBuf = blob.ok_or_else(|| {
            IngressError::Invariant("library spec: blob=<path> is required".into())
        })?;
        if !blob_root.is_absolute() {
            return Err(IngressError::Invariant(
                "library spec: blob root must be absolute".into(),
            ));
        }
        Ok(Self {
            library_id: id.ok_or_else(|| {
                IngressError::Invariant("library spec: id=<library_id> is required".into())
            })?,
            blob_root,
            sidecar_root_remote: sidecars,
            scope,
            display_name: name,
            retention_days: retention,
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct RecoverOptions {
    /// Storage roots whose `state-snapshots/` dirs are searched (the
    /// `--library` blob roots are always searched too).
    pub roots: Vec<PathBuf>,
    pub libraries: Vec<RecoverLibrarySpec>,
    /// Skip the snapshot search (the operator knows the snapshots are bad).
    pub from_sidecars: bool,
    /// Move an existing `state.db` aside instead of refusing.
    pub force: bool,
}

#[derive(Debug, serde::Serialize)]
#[serde(tag = "source", rename_all = "snake_case")]
pub enum RecoverSource {
    Snapshot { path: PathBuf, ts: i64 },
    Sidecars,
}

#[derive(Debug, serde::Serialize)]
pub struct RecoverReport {
    pub source: RecoverSource,
    pub photos: u64,
    pub resources: u64,
    /// `blobs` rows after the rebuild recount.
    pub blobs: u64,
    /// Remote sidecar documents copied into the local tree.
    pub sidecars_hydrated: u64,
    /// Referenced blob files that do not exist on disk (verification —
    /// usually means a wrong `blob=` path in a spec).
    pub missing_blob_files: u64,
    pub warnings: Vec<String>,
    /// The recovered library configuration, for operator review (paths
    /// from the dead Mac may need edits before the daemon starts).
    pub libraries: Vec<LibraryConfig>,
}

pub async fn recover(data_dir: &DataDir, opts: &RecoverOptions) -> Result<RecoverReport> {
    if opts.roots.is_empty() && opts.libraries.is_empty() {
        return Err(IngressError::Invariant(
            "recover needs at least one --root or --library".into(),
        ));
    }
    fs::create_dir_all(data_dir.root())
        .map_err(|e| IngressError::Invariant(format!("data dir: {e}")))?;
    // Exclusive lock: a live daemon must never race a db swap. The
    // unclean-reclaim signal is deliberately ignored — recover replaces
    // the database it would have repaired.
    let acquired = crate::runlock::DrainLock::acquire(data_dir)?;
    let _lock = acquired.lock;

    let db_path = data_dir.state_db_path();
    if db_path.exists() {
        if !opts.force {
            return Err(IngressError::Invariant(format!(
                "{} already exists — pass --force to move it aside",
                db_path.display()
            )));
        }
        move_db_aside(&db_path)?;
    }

    // Snapshot-first: search every candidate root's state-snapshots/.
    if !opts.from_sidecars
        && let Some((ts, path)) = newest_snapshot(opts)
    {
        return recover_from_snapshot(data_dir, &db_path, ts, &path).await;
    }

    // Sidecar-tree fallback.
    if !opts.libraries.is_empty()
        && opts
            .libraries
            .iter()
            .any(|spec| spec.sidecar_root_remote.as_deref().is_some_and(has_docs))
    {
        return recover_from_sidecars(data_dir, &db_path, opts).await;
    }

    Err(inventory_error(opts))
}

/// Both the db and its WAL companions move together — a stale `-wal`
/// beside a restored db is a corruption hazard, and keeping the trio's
/// names aligned lets the aside copy reopen later.
fn move_db_aside(db_path: &Path) -> Result<()> {
    let aside_base = format!(
        "{}.pre-recover.{}",
        db_path.file_name().unwrap_or_default().to_string_lossy(),
        Utc::now().timestamp()
    );
    for suffix in ["", "-wal", "-shm"] {
        let src = PathBuf::from(format!("{}{suffix}", db_path.display()));
        if src.exists() {
            let dst = db_path.with_file_name(format!("{aside_base}{suffix}"));
            fs::rename(&src, &dst)
                .map_err(|e| IngressError::Invariant(format!("move aside: {e}")))?;
        }
    }
    Ok(())
}

/// The globally-newest parseable snapshot across all candidate roots.
fn newest_snapshot(opts: &RecoverOptions) -> Option<(i64, PathBuf)> {
    let candidates = opts
        .roots
        .iter()
        .cloned()
        .chain(opts.libraries.iter().map(|s| s.blob_root.clone()));
    let mut best: Option<(i64, PathBuf)> = None;
    for root in candidates {
        let dir = BlobPaths::new(&root).snapshot_dir();
        if let Some(ts) = crate::cleanup::newest_snapshot_ts(&dir) {
            let path = dir.join(format!("state.db.{ts}.sqlite3"));
            if best.as_ref().is_none_or(|(b, _)| ts > *b) {
                best = Some((ts, path));
            }
        }
    }
    best
}

async fn recover_from_snapshot(
    data_dir: &DataDir,
    db_path: &Path,
    ts: i64,
    snapshot: &Path,
) -> Result<RecoverReport> {
    // Copy via temp + rename in the destination dir.
    let staging = db_path.with_extension("recovering");
    fs::copy(snapshot, &staging)
        .map_err(|e| IngressError::Invariant(format!("snapshot copy: {e}")))?;
    fs::rename(&staging, db_path)
        .map_err(|e| IngressError::Invariant(format!("snapshot rename: {e}")))?;

    // A normal open validates the file and migrates an older schema.
    let store = StateStore::open(db_path).await?;
    let libraries = store.libraries().await?;
    let mut warnings = Vec::new();

    // Local-sidecar hydration (spec errata): the restored db claims
    // materialized + replicated photos, but this Mac's local tree is
    // empty. The remote trees are the current copies by the "stamped ⇒
    // remote ≥ local" invariant; unhydrated photos remain fsck findings.
    let mut hydrated = 0u64;
    for library in &libraries {
        let Some(remote) = &library.sidecar_root_remote else {
            warnings.push(format!(
                "{}: no remote sidecar root — local sidecars not hydrated",
                library.library_id
            ));
            continue;
        };
        let remote_root = Path::new(remote);
        if !remote_root.is_dir() {
            warnings.push(format!(
                "{}: remote sidecar root unavailable ({remote}) — not hydrated",
                library.library_id
            ));
            continue;
        }
        let local_root = data_dir.sidecar_root(&library.library_id);
        for doc in walk_sidecars(remote_root)? {
            let rel = doc
                .strip_prefix(remote_root)
                .map_err(|_| IngressError::Invariant("sidecar outside its root".into()))?;
            let dst = local_root.join(rel);
            fs::create_dir_all(dst.parent().expect("YYYY/MM parents"))
                .and_then(|_| fs::copy(&doc, &dst))
                .map_err(|e| IngressError::Invariant(format!("hydrate {}: {e}", doc.display())))?;
            hydrated += 1;
        }
    }

    let (photos, resources, blobs) = table_counts(&store).await?;
    store
        .append_log(
            "recovered",
            None,
            Some(serde_json::json!({
                "source": "snapshot",
                "path": snapshot.to_string_lossy(),
                "ts": ts,
                "photos": photos,
                "sidecars_hydrated": hydrated,
            })),
        )
        .await?;

    Ok(RecoverReport {
        source: RecoverSource::Snapshot {
            path: snapshot.to_path_buf(),
            ts,
        },
        photos,
        resources,
        blobs,
        sidecars_hydrated: hydrated,
        missing_blob_files: 0,
        warnings,
        libraries,
    })
}

async fn recover_from_sidecars(
    data_dir: &DataDir,
    db_path: &Path,
    opts: &RecoverOptions,
) -> Result<RecoverReport> {
    validate_specs(&opts.libraries)?;
    let store = StateStore::open(db_path).await?;
    let mut warnings = Vec::new();
    let mut photos = 0u64;
    let mut resources = 0u64;
    let mut hydrated = 0u64;
    let now = Utc::now();

    for spec in &opts.libraries {
        store
            .insert_library(&LibraryConfig {
                library_id: spec.library_id.clone(),
                display_name: spec
                    .display_name
                    .clone()
                    .unwrap_or_else(|| spec.library_id.to_string()),
                blob_root: spec.blob_root.to_string_lossy().into_owned(),
                sidecar_root_remote: spec
                    .sidecar_root_remote
                    .as_ref()
                    .map(|p| p.to_string_lossy().into_owned()),
                scope_binding: match spec.scope {
                    LibraryScope::Personal => None,
                    LibraryScope::Shared => Some(ICLOUD_SHARED_LIBRARY_BINDING.to_string()),
                },
                retention_days: spec.retention_days,
                created_at: now,
            })
            .await?;
    }

    // One transaction for the whole rebuild: recovery is offline, and a
    // partial rebuild is worse than none.
    let mut tx = store.pool().begin().await?;
    for spec in &opts.libraries {
        let Some(remote_root) = spec.sidecar_root_remote.as_deref() else {
            warnings.push(format!(
                "{}: no sidecars= tree in the spec — library recovered empty",
                spec.library_id
            ));
            continue;
        };
        let local_root = data_dir.sidecar_root(&spec.library_id);
        for doc_path in walk_sidecars(remote_root)? {
            let doc = match fs::read_to_string(&doc_path)
                .map_err(|e| e.to_string())
                .and_then(|json| Sidecar::from_json(&json).map_err(|e| e.to_string()))
            {
                Ok(doc) => doc,
                Err(e) => {
                    warnings.push(format!("skipped {}: {e}", doc_path.display()));
                    continue;
                }
            };
            if doc.library_id != spec.library_id {
                // The CLI spec wins (covers pre-rename stragglers and
                // misfiled documents); the embedded id is advisory.
                warnings.push(format!(
                    "{}: document claims library {:?}, recovering into {}",
                    doc.photo_id, doc.library_id, spec.library_id
                ));
            }

            let group_type = doc
                .group
                .as_ref()
                .and_then(|g| GroupType::from_name(&g.group_type))
                .map(|g| g as i64);
            sqlx::query(
                "INSERT INTO photos \
                 (photo_id, library_id, cloud_id, local_id, group_id, group_type, group_index, \
                  is_group_pick, discovered_at, asset_modified_at, materialized_at, \
                  sidecar_replicated_at, deleted_at) \
                 VALUES (?, ?, ?, NULL, ?, ?, ?, ?, ?, NULL, ?, ?, ?)",
            )
            .bind(&doc.photo_id)
            .bind(&spec.library_id)
            .bind(&doc.cloud_id)
            .bind(doc.group.as_ref().map(|g| g.id.clone()))
            .bind(group_type)
            .bind(doc.group.as_ref().and_then(|g| g.index))
            .bind(doc.group.as_ref().is_some_and(|g| g.is_pick))
            .bind(doc.ingested_at)
            .bind(now) // materialized_at: the resources list reflects committed state
            .bind(now) // sidecar_replicated_at: the remote copy IS the source
            .bind(doc.deleted_at)
            .execute(&mut *tx)
            .await?;
            photos += 1;

            for res in &doc.resources {
                let Some(resource_type) = ResourceType::from_name(&res.resource_type) else {
                    warnings.push(format!(
                        "{}: unknown resource type {:?} skipped",
                        doc.photo_id, res.resource_type
                    ));
                    continue;
                };
                sqlx::query(
                    "INSERT INTO photo_resources \
                     (photo_id, resource_type, content_hash, ext, size_bytes, written_at) \
                     VALUES (?, ?, ?, ?, ?, ?)",
                )
                .bind(&doc.photo_id)
                .bind(resource_type)
                .bind(&res.content_hash)
                .bind(&res.ext)
                .bind(res.size_bytes)
                .bind(now)
                .execute(&mut *tx)
                .await?;
                resources += 1;
            }

            // Populate the local tree from the same document.
            let rel = doc_path
                .strip_prefix(remote_root)
                .map_err(|_| IngressError::Invariant("sidecar outside its root".into()))?;
            let dst = local_root.join(rel);
            fs::create_dir_all(dst.parent().expect("YYYY/MM parents"))
                .and_then(|_| fs::copy(&doc_path, &dst))
                .map_err(|e| IngressError::Invariant(format!("hydrate: {e}")))?;
            hydrated += 1;
        }
    }
    tx.commit().await?;

    // "Rebuild blobs by recounting" IS the repair's missing-row insertion
    // path — every referenced (library, hash) gains a row with the exact
    // reference count.
    crate::recovery::repair_refcounts(&store).await?;

    // Verification: every rebuilt row's file must exist. A flood here
    // usually means a wrong blob= path in a spec.
    let mut missing_blob_files = 0u64;
    for spec in &opts.libraries {
        let paths = BlobPaths::new(&spec.blob_root);
        for row in store.blobs_for_library(&spec.library_id).await? {
            if !paths.blob_path(&row.content_hash, &row.ext).is_file() {
                missing_blob_files += 1;
            }
        }
    }

    let (photos_count, resources_count, blobs) = table_counts(&store).await?;
    debug_assert_eq!(photos, photos_count);
    debug_assert_eq!(resources, resources_count);
    store
        .append_log(
            "recovered",
            None,
            Some(serde_json::json!({
                "source": "sidecars",
                "photos": photos,
                "resources": resources,
                "blobs": blobs,
                "missing_blob_files": missing_blob_files,
            })),
        )
        .await?;

    let libraries = store.libraries().await?;
    Ok(RecoverReport {
        source: RecoverSource::Sidecars,
        photos,
        resources,
        blobs,
        sidecars_hydrated: hydrated,
        missing_blob_files,
        warnings,
        libraries,
    })
}

fn validate_specs(specs: &[RecoverLibrarySpec]) -> Result<()> {
    let mut personal = 0;
    for (i, spec) in specs.iter().enumerate() {
        if specs[..i].iter().any(|s| s.library_id == spec.library_id) {
            return Err(IngressError::Invariant(format!(
                "duplicate library id {} in specs",
                spec.library_id
            )));
        }
        if matches!(spec.scope, LibraryScope::Personal) {
            personal += 1;
        }
    }
    if personal > 1 {
        return Err(IngressError::Invariant(
            "at most one personal-scope library spec".into(),
        ));
    }
    Ok(())
}

fn has_docs(root: &Path) -> bool {
    walk_sidecars(root).map(|d| !d.is_empty()).unwrap_or(false)
}

async fn table_counts(store: &StateStore) -> Result<(u64, u64, u64)> {
    let photos: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM photos")
        .fetch_one(store.pool())
        .await?;
    let resources: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM photo_resources")
        .fetch_one(store.pool())
        .await?;
    let blobs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM blobs")
        .fetch_one(store.pool())
        .await?;
    Ok((photos as u64, resources as u64, blobs as u64))
}

/// Neither source usable: tell the operator exactly what WAS found where,
/// and why blob-only recovery is not offered.
fn inventory_error(opts: &RecoverOptions) -> IngressError {
    let mut lines = vec!["no usable recovery source found".to_string()];
    let candidates: Vec<(String, PathBuf)> = opts
        .roots
        .iter()
        .map(|r| ("--root".to_string(), r.clone()))
        .chain(
            opts.libraries
                .iter()
                .map(|s| (format!("--library {}", s.library_id), s.blob_root.clone())),
        )
        .collect();
    for (label, root) in candidates {
        let snapshots = crate::cleanup::newest_snapshot_ts(&BlobPaths::new(&root).snapshot_dir())
            .map(|ts| format!("newest snapshot ts {ts}"))
            .unwrap_or_else(|| "no snapshots".to_string());
        let blobs = if BlobPaths::new(&root).blobs_dir().is_dir() {
            "blob tree present"
        } else {
            "no blob tree"
        };
        lines.push(format!(
            "  {label} ({}): {snapshots}; {blobs}",
            root.display()
        ));
    }
    for spec in &opts.libraries {
        if let Some(sidecars) = &spec.sidecar_root_remote {
            let n = walk_sidecars(sidecars).map(|d| d.len()).unwrap_or(0);
            lines.push(format!(
                "  --library {} sidecars ({}): {n} documents",
                spec.library_id,
                sidecars.display()
            ));
        }
    }
    lines.push(
        "blob-only recovery is deliberately not implemented: it would mint fresh photo_ids \
         and lose all PhotoKit-derived metadata (capture dates, grouping, favorites, edit \
         relationships). If blobs survived, keep them — a future EXIF pass can rebuild a \
         degraded archive."
            .to_string(),
    );
    IngressError::Invariant(lines.join("\n"))
}
