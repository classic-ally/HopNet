use crate::{db::{DatabaseError, metrics::insert_metrics_batch}, handlers::{HandlerResult, TransactionHandler}, metrics::types::Metric, consensus::types::Transaction};
use crate::AppState;

pub struct SubmitMetricsHandler;

impl TransactionHandler for SubmitMetricsHandler {
    fn name(&self) -> &'static str { "submit_metrics" }

    fn process(&self, state: &AppState, tx: &Transaction, execute: bool, db_tx: &rusqlite::Transaction) -> HandlerResult {
        match bincode::serde::decode_from_slice::<Vec<Metric>, _>(&tx.rpc.payload, bincode::config::standard()) {
            Ok((metrics_data, _)) => {
                // Authorization: verify all metrics are from the submitting node
                for metric in &metrics_data {
                    if metric.from_node != tx.submitter.id {
                        tracing::warn!("Authorization failed: node {} attempted to submit metrics from node {}", tx.submitter.id, metric.from_node);
                        return Err(DatabaseError::AuthorizationError);
                    }
                }

                // Insert the metrics batch using shared transaction
                insert_metrics_batch(db_tx, metrics_data)?;
                Ok(())
            },
            Err(_) => Err(DatabaseError::InvalidPayload),
        }
    }
}

inventory::submit! {
    &SubmitMetricsHandler as &dyn TransactionHandler
}