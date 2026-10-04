use std::sync::Arc;
use std::time::Instant;

use hopnet_photos_core::PhotosCoreError;
use hopnet_photos_core::dispatch::{
    LibraryMembership, PhotoDispatch, SyncBatch, UploadedDataBlock, UploadedFragment,
};
use hopnet_storage::api::PutTimings;

use crate::consensus::dispatch::create_signed_user_transaction;
use crate::debug::ingest::SubmitTimings;

pub struct Submitter {
    app_state: Arc<crate::AppState>,
    user_id: i32,
}

impl Submitter {
    pub fn new(app_state: Arc<crate::AppState>, user_id: i32) -> Self {
        Self { app_state, user_id }
    }

    /// [`PhotoDispatch::submit_transaction`], plus where the time went.
    /// Reported on failure too: a decision that timed out is exactly what
    /// the ingest timings are for.
    pub async fn submit_transaction_timed(
        &self,
        tx_type: &str,
        payload_bytes: Vec<u8>,
    ) -> (Result<(), PhotosCoreError>, SubmitTimings) {
        let mut timings = SubmitTimings::default();
        let signing = Instant::now();
        let signed = create_signed_user_transaction(
            &self.app_state,
            tx_type.to_string(),
            payload_bytes,
            self.user_id,
        )
        .await
        .map_err(|e| PhotosCoreError::Dispatch(format!("sign: {e:?}")));
        timings.sign = signing.elapsed();
        let tx = match signed {
            Ok(tx) => tx,
            Err(e) => return (Err(e), timings),
        };

        let deciding = Instant::now();
        let mut results = self.app_state.consensus_queue.submit_batch(vec![tx]).await;
        timings.decide = deciding.elapsed();
        let result = results
            .pop()
            .ok_or_else(|| PhotosCoreError::Dispatch("no result".into()))
            .and_then(|r| r.map_err(|e| PhotosCoreError::Dispatch(format!("submit: {e:?}"))))
            .map(|_| ());
        (result, timings)
    }

    /// [`PhotoDispatch::upload_data_block`], plus the put's phase timings.
    pub async fn upload_data_block_timed(
        &self,
        blob_id: hopnet_storage::BlobId,
        source: Box<dyn tokio::io::AsyncRead + Unpin + Send>,
        file_size: usize,
        per_blob_key: chacha20poly1305::Key,
    ) -> Result<(UploadedDataBlock, PutTimings), PhotosCoreError> {
        // The fragments are on disk long before the photo transaction is
        // signed; each batch is ledgered before its files are written, so
        // the sweep holds them (consensus-bugs 20), and a failed put is
        // abandoned rather than held. A ledger failure fails the upload, so
        // the client retries instead of proceeding with unprotected files.
        let outcome = hopnet_projection::host::put_own_upload(
            &self.app_state.db_pool,
            &self.app_state.fragments_dir,
            blob_id,
            source,
            file_size,
            &per_blob_key,
        )
        .await?;

        let uploaded = UploadedDataBlock {
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
        };
        Ok((uploaded, outcome.timings))
    }
}

#[async_trait::async_trait]
impl PhotoDispatch for Submitter {
    async fn submit_transaction(
        &self,
        tx_type: &str,
        payload_bytes: Vec<u8>,
    ) -> Result<(), PhotosCoreError> {
        self.submit_transaction_timed(tx_type, payload_bytes)
            .await
            .0
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
        self.upload_data_block_timed(blob_id, source, file_size, per_blob_key)
            .await
            .map(|(uploaded, _)| uploaded)
    }

    async fn fetch_library_members(
        &self,
        library_id: Option<hopnet_common::CustomUUID>,
    ) -> Result<LibraryMembership, PhotosCoreError> {
        super::query::read_library_membership(&self.app_state.db_pool, self.user_id, library_id)
            .map_err(PhotosCoreError::Dispatch)
    }
}
