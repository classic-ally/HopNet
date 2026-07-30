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
use hopnet_photos_core::publisher::{ByteSource, PublishRequest, publish_photo_add};
use ingress_core::publish::{PublishError, PublishItem, PublishOutcome, Publisher};

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

        // 4. The full client-side publish: validate → encrypt metadata →
        //    mint blob keys → wrap to members → upload per resource →
        //    photo_add. All over HttpDispatch.
        match publish_photo_add(
            &self.dispatch,
            PublishRequest {
                asset: &asset,
                photo_id,
                library_id: None, // personal partition only this phase
                byte_sources,
            },
        )
        .await
        {
            Ok(_) => Ok(PublishOutcome::Published),
            Err(e) => Err(classify(e)),
        }
    }
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
