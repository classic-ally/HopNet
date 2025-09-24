use crate::db::DatabaseError;
use crate::AppState;
use crate::consensus::types::Transaction;

pub type HandlerResult = Result<(), DatabaseError>;
pub trait TransactionHandler: Send + Sync {
    // Stable lookup name for function that handles thing
    fn name(&self) -> &'static str;

    // Process with execution flag - receives full transaction for authorization checks
    // Handlers can access:
    // - tx.submitter.id (cryptographically verified node that submitted)
    // - tx.user (optional, cryptographically verified user if present)
    // - tx.rpc.payload (the actual operation payload to decode)
    fn process(&self, state: &AppState, tx: &Transaction, execute: bool) -> HandlerResult;

}

inventory::collect!(&'static dyn TransactionHandler);