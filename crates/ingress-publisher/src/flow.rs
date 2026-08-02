//! `NodePublisher`: the confirm-then-retry publish flow.
//!
//! The publisher idempotency contract (photos-core `publisher` module docs)
//! requires the caller to persist `SourceIdentity → photo_id` BEFORE
//! publishing and to confirm committed state before retrying the same
//! photo_id — consensus hard-rejects duplicate photo ids at the proposer
//! preflight, so a blind retry after an ambiguous failure would loop on
//! rejections forever. The ingress side already satisfies the persistence
//! half: state.db's `photos.photo_id` IS the consensus id (spec commitment,
//! no remapping), minted at first discovery.

use std::str::FromStr;

use hopnet_common::CustomUUID;
use hopnet_photos_core::PhotosCoreError;
use hopnet_photos_core::dispatch::PhotoDispatch;
use hopnet_photos_core::payloads::{build_photo_delete, build_photo_restore, encode_payload};
use hopnet_photos_core::publisher::{
    ByteSource, EditRequest, EditResource as CoreEditResource, PublishRequest, publish_photo_add,
    publish_photo_edit,
};
use ingress_core::publish::{
    EditItem, PublishError, PublishItem, PublishOutcome, Publisher, ResolveEntry, ResolveOutcome,
    Responsibility, TombstoneOp,
};

use crate::dispatch::{CommitProbe, HttpDispatch, UNREACHABLE_PREFIX};

pub struct NodePublisher {
    dispatch: HttpDispatch,
}

impl NodePublisher {
    pub fn new(base_url: &str, device_token: &str) -> Result<Self, String> {
        Ok(Self {
            dispatch: HttpDispatch::new(base_url, device_token)?,
        })
    }
}

#[async_trait::async_trait]
impl Publisher for NodePublisher {
    async fn publish(&self, item: PublishItem) -> Result<PublishOutcome, PublishError> {
        // 1. Confirm-first, EVERY call: a previous ambiguous attempt (submit
        //    timeout, 500 after the consensus wait, daemon crash mid-pass)
        //    may have committed. 404 ⇒ the same photo_id is safe to submit.
        match self.dispatch.check_committed(item.photo.photo_id.as_str()).await {
            CommitProbe::Committed => return Ok(PublishOutcome::AlreadyPublished),
            CommitProbe::NotCommitted => {}
            CommitProbe::Unreachable(msg) => return Err(PublishError::NodeUnreachable(msg)),
            CommitProbe::Failed(msg) => return Err(PublishError::Transient(msg)),
        }

        // 2. Map to the RFC-011 asset (pure; failures are permanent).
        let asset = crate::map::to_photo_asset(&item).map_err(PublishError::Rejected)?;
        let photo_id = CustomUUID::from_str(item.photo.photo_id.as_str())
            .map_err(|e| PublishError::Rejected(format!("photo id not a uuid: {e}")))?;
        let cloud_fingerprint = item
            .cloud_fingerprint
            .as_deref()
            .map(parse_fingerprint)
            .transpose()
            .map_err(PublishError::Rejected)?;

        // 3. Open blob files as streaming byte sources. The unix fd survives
        //    a concurrent unlink; state races are excluded anyway — the
        //    daemon holds claimed photos inflight for the pass duration.
        let mut byte_sources = Vec::with_capacity(item.resources.len());
        for resource in &item.resources {
            let kind = hopnet_photos_core::asset::ResourceKind::from_name(
                resource.resource_type.as_str(),
            )
            .expect("mapping validated by to_photo_asset");
            let file = tokio::fs::File::open(&resource.blob_path).await.map_err(|e| {
                PublishError::Transient(format!(
                    "open {}: {e}",
                    resource.blob_path.display()
                ))
            })?;
            byte_sources.push((kind, ByteSource::Stream(Box::new(file))));
        }

        // 4. The publish target: the library's mesh binding (None = the
        //    personal partition). A malformed stored id is permanent —
        //    libconfig validates the UUID on write, so this only trips on
        //    direct DB edits, and a transient here would spin forever.
        let library_id = item
            .library
            .mesh_library_id
            .as_deref()
            .map(CustomUUID::from_str)
            .transpose()
            .map_err(|e| PublishError::Rejected(format!("mesh library id not a uuid: {e}")))?;

        // 5. The full client-side publish: validate → encrypt metadata →
        //    mint blob keys → wrap to members (the node's membership
        //    endpoint returns members ∪ invitees for a shared library) →
        //    upload per resource → photo_add. All over HttpDispatch.
        match publish_photo_add(
            &self.dispatch,
            PublishRequest {
                asset: &asset,
                photo_id,
                library_id,
                byte_sources,
                cloud_fingerprint,
            },
        )
        .await
        {
            Ok(_) => Ok(PublishOutcome::Published),
            Err(e) => Err(classify(e)),
        }
    }

    async fn resolve(
        &self,
        library_id: Option<&str>,
        cloud_ids: &[String],
    ) -> Result<ResolveOutcome, PublishError> {
        let wire = self
            .dispatch
            .resolve_cloud_ids(library_id, cloud_ids)
            .await
            .map_err(classify)?;
        let responsibility = match wire.responsibility.as_str() {
            "holder" => Responsibility::Holder,
            "other" => Responsibility::Other,
            "unclaimed" => Responsibility::Unclaimed,
            other => {
                return Err(PublishError::Transient(format!(
                    "resolve: unknown responsibility `{other}`"
                )));
            }
        };
        Ok(ResolveOutcome {
            responsibility,
            entries: wire
                .entries
                .into_iter()
                .map(|e| ResolveEntry {
                    cloud_id: e.cloud_id,
                    fingerprint: e.fingerprint,
                    committed_photo_id: e.photo_id,
                })
                .collect(),
        })
    }

    /// No confirm probe and no upload: `photo_delete` / `photo_restore`
    /// carry only ids, and both handlers are idempotent (a delete of a
    /// missing photo is skipped, not an error), so an ambiguous attempt is
    /// safe to repeat.
    async fn propagate_tombstone(
        &self,
        consensus_photo_id: &str,
        op: TombstoneOp,
    ) -> Result<(), PublishError> {
        let photo_id = CustomUUID::from_str(consensus_photo_id)
            .map_err(|e| PublishError::Rejected(format!("consensus photo id not a uuid: {e}")))?;

        let (tx_type, payload) = match op {
            TombstoneOp::Delete => (
                "photo_delete",
                encode_payload(&build_photo_delete(vec![photo_id])),
            ),
            TombstoneOp::Restore => (
                "photo_restore",
                encode_payload(&build_photo_restore(vec![photo_id])),
            ),
        };
        let payload = payload.map_err(classify)?;

        self.dispatch
            .submit_transaction(tx_type, payload)
            .await
            .map_err(classify)
    }

    /// No confirm probe either, for a different reason than tombstones: an
    /// edit is an idempotent REPLACEMENT against an id consensus already
    /// holds. There is no unique id to collide with, so re-sending an
    /// ambiguous edit lands the same bytes twice at worst.
    async fn publish_edit(&self, item: EditItem) -> Result<(), PublishError> {
        let photo_id = CustomUUID::from_str(&item.consensus_photo_id)
            .map_err(|e| PublishError::Rejected(format!("consensus photo id not a uuid: {e}")))?;

        let library_id = item
            .library
            .mesh_library_id
            .as_deref()
            .map(CustomUUID::from_str)
            .transpose()
            .map_err(|e| PublishError::Rejected(format!("mesh library id not a uuid: {e}")))?;

        // Only compose metadata when it actually diverged: an unchanged
        // photo would otherwise get a fresh key and fresh wraps on every
        // byte-only edit, for no gain.
        let metadata = if item.metadata_changed {
            Some(
                crate::map::to_photo_metadata(&item.sidecar, item.original_ext.as_deref())
                    .map_err(PublishError::Rejected)?,
            )
        } else {
            None
        };

        let mut resources = Vec::with_capacity(item.resources.len());
        for resource in &item.resources {
            let kind = kind_for(resource.resource_type)?;
            if resource.size_bytes <= 0 {
                return Err(PublishError::Rejected(format!(
                    "edited resource {kind} has non-positive size"
                )));
            }
            let file = tokio::fs::File::open(&resource.blob_path)
                .await
                .map_err(|e| {
                    PublishError::Transient(format!("open {}: {e}", resource.blob_path.display()))
                })?;
            resources.push(CoreEditResource {
                kind,
                byte_len: resource.size_bytes as u64,
                source: ByteSource::Stream(Box::new(file)),
            });
        }

        let mut remove_resources = Vec::with_capacity(item.removals.len());
        for resource_type in &item.removals {
            remove_resources.push(kind_for(*resource_type)?);
        }

        publish_photo_edit(
            &self.dispatch,
            EditRequest {
                photo_id,
                library_id,
                resources,
                remove_resources,
                metadata: metadata.as_ref(),
            },
        )
        .await
        .map(|_| ())
        .map_err(classify)
    }
}

/// Ingress resource type → RFC-011 kind. The names (and wire discriminants)
/// are identical by design; a miss is permanent, not worth retrying.
fn kind_for(
    resource_type: ingress_core::model::ResourceType,
) -> Result<hopnet_photos_core::asset::ResourceKind, PublishError> {
    hopnet_photos_core::asset::ResourceKind::from_name(resource_type.as_str()).ok_or_else(|| {
        PublishError::Rejected(format!(
            "resource type `{}` has no RFC-011 kind",
            resource_type.as_str()
        ))
    })
}

/// 64 hex chars → the 32-byte fingerprint the payload carries.
fn parse_fingerprint(hex_str: &str) -> Result<[u8; 32], String> {
    let bytes = hex::decode(hex_str).map_err(|e| format!("fingerprint not hex: {e}"))?;
    bytes
        .try_into()
        .map_err(|_| format!("fingerprint must be 32 bytes, got {} hex chars", hex_str.len()))
}

/// Map publish failures onto the queue's retry classes. Ambiguous outcomes
/// (anything transient-shaped, including a rejected duplicate submit) land
/// on Transient — the next pass's confirm-first probe disambiguates.
fn classify(error: PhotosCoreError) -> PublishError {
    match error {
        PhotosCoreError::InvalidAsset(_) | PhotosCoreError::InvalidPublishRequest(_) => {
            PublishError::Rejected(error.to_string())
        }
        PhotosCoreError::Dispatch(msg) if msg.starts_with(UNREACHABLE_PREFIX) => {
            PublishError::NodeUnreachable(msg)
        }
        PhotosCoreError::PartialPublish {
            photo_id,
            uploaded_blob_ids,
            source,
        } => {
            // Uploaded blob ids are reconciliation candidates owned by the
            // node's orphan sweep — recorded in the failure message (and thus
            // publish_last_error + the ingest log), never GC'd daemon-side.
            let context = format!(
                "partial publish of {photo_id} (uploaded blobs: {uploaded_blob_ids:?})"
            );
            match classify(*source) {
                PublishError::NodeUnreachable(msg) => {
                    PublishError::NodeUnreachable(format!("{context}: {msg}"))
                }
                PublishError::Rejected(msg) => {
                    PublishError::Rejected(format!("{context}: {msg}"))
                }
                PublishError::Transient(msg) => {
                    PublishError::Transient(format!("{context}: {msg}"))
                }
            }
        }
        other => PublishError::Transient(other.to_string()),
    }
}
