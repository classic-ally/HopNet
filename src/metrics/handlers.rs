use crate::{db::{DatabaseError, metrics::insert_metrics_batch}, handlers::{HandlerResult, TransactionHandler}, metrics::types::Metric};
use crate::AppState;

pub struct SubmitMetricsHandler;

impl TransactionHandler for SubmitMetricsHandler {
    fn name(&self) -> &'static str { "submit_metrics" }

    fn process(&self, state: &AppState, payload: &[u8], execute: bool) -> HandlerResult {
        match bincode::serde::decode_from_slice::<Vec<Metric>, _>(payload, bincode::config::standard()) {
            Ok((metrics_data, _)) => {
                // Insert the metrics batch using the consensus-safe version with execute flag
                insert_metrics_batch(state.db_pool.get(), metrics_data, execute)?;
                Ok(())
            },
            Err(_) => Err(DatabaseError::InvalidPayload),
        }
    }
}

inventory::submit! {
    &SubmitMetricsHandler as &dyn TransactionHandler
}