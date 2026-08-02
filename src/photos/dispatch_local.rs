use std::sync::Arc;

use hopnet_photos_core::PhotosCoreError;
use hopnet_photos_core::dispatch::{
    LibraryMembership, PhotoDispatch, SyncBatch, UploadedDataBlock, UploadedFragment,
};

use crate::consensus::dispatch::create_signed_user_transaction;

pub struct Submitter {
    app_state: Arc<crate::AppState>,
    user_id: i32,
}

impl Submitter {
    pub fn new(app_state: Arc<crate::AppState>, user_id: i32) -> Self {
        Self { app_state, user_id }
    }
}

#[async_trait::async_trait]
impl PhotoDispatch for Submitter {
    async fn submit_transaction(
        &self,
        tx_type: &str,
        payload_bytes: Vec<u8>,
    ) -> Result<(), PhotosCoreError> {
        let tx = create_signed_user_transaction(
            &self.app_state,
            tx_type.to_string(),
            payload_bytes,
            self.user_id,
        )
        .await
        .map_err(|e| PhotosCoreError::Dispatch(format!("sign: {e:?}")))?;

        let mut results = self.app_state.consensus_queue.submit_batch(vec![tx]).await;
        results
            .pop()
            .ok_or_else(|| PhotosCoreError::Dispatch("no result".into()))?
            .map_err(|e| PhotosCoreError::Dispatch(format!("submit: {e:?}")))?;
        Ok(())
    }

    async fn fetch_photos_since(&self, height: u64) -> Result<SyncBatch, PhotosCoreError> {
        super::query::read_photo_changes(&self.app_state.db_pool, self.user_id, height)
            .map_err(PhotosCoreError::Dispatch)
    }

    async fn upload_data_block(
        &self,
        blob_id: hopnet_storage::BlobId,
        source: Box<dyn tokio::io::AsyncRead + Unpin + Send>,
        file_size: usize,
        per_blob_key: chacha20poly1305::Key,
    ) -> Result<UploadedDataBlock, PhotosCoreError> {
        let outcome = hopnet_storage::api::put(
            source,
            file_size,
            blob_id,
            &per_blob_key,
            &self.app_state.fragments_dir,
        )
        .await?;

        Ok(UploadedDataBlock {
            integrity_hash: outcome.integrity_hash,
            fragments: outcome
                .fragments
                .into_iter()
                .map(|f| UploadedFragment {
                    chunk_number: f.chunk_number,
                    local_index: f.local_index,
                    fragment_id: f.fragment_id,
                    fragment_hash: f.fragment_hash,
                    recovery: f.recovery,
                })
                .collect(),
            added_bytes: outcome.added_bytes,
        })
    }

    async fn fetch_library_members(
        &self,
        library_id: Option<hopnet_common::CustomUUID>,
    ) -> Result<LibraryMembership, PhotosCoreError> {
        super::query::read_library_membership(&self.app_state.db_pool, self.user_id, library_id)
            .map_err(PhotosCoreError::Dispatch)
    }
}
