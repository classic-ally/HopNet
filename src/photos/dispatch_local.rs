use std::sync::Arc;

use hopnet_photos_core::PhotosCoreError;
use hopnet_photos_core::dispatch::{PhotoDispatch, SyncBatch};

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
            .map_err(|e| PhotosCoreError::Dispatch(e))
    }
}
