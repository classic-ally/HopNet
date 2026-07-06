use crate::{
    db::{DatabaseError, metrics::insert_metrics_batch},
    handlers::{HandlerCtx, HandlerResult, TransactionHandler, TxMeta},
    metrics::types::Metric,
};

pub struct SubmitMetricsHandler;

impl TransactionHandler for SubmitMetricsHandler {
    fn name(&self) -> &'static str {
        "submit_metrics"
    }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        execute: bool,
        _ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        match bincode::serde::decode_from_slice::<Vec<Metric>, _>(
            tx.payload,
            bincode::config::standard(),
        ) {
            Ok((metrics_data, _)) => {
                // Authorization: verify all metrics are from the submitting node
                for metric in &metrics_data {
                    if metric.from_node != tx.submitter_node {
                        tracing::warn!(
                            "Authorization failed: node {} attempted to submit metrics from node {}",
                            tx.submitter_node,
                            metric.from_node
                        );
                        return Err(DatabaseError::AuthorizationError);
                    }
                }

                // Insert the metrics batch using shared transaction
                insert_metrics_batch(db_tx, metrics_data)?;
                Ok(())
            }
            Err(_) => Err(DatabaseError::InvalidPayload),
        }
    }
}

inventory::submit! {
    &SubmitMetricsHandler as &dyn TransactionHandler
}
