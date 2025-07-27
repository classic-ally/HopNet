use crate::db::DatabaseError;
use crate::AppState;

pub type HandlerResult = Result<(), DatabaseError>;
pub trait TransactionHandler: Send + Sync {
    // Stable lookup name for function that handles thing
    fn name(&self) -> &'static str;

    // Process with execution flag - same logic, commit or rollback based on execute
    fn process(&self, state: &AppState, payload: &[u8], execute: bool) -> HandlerResult;

}

inventory::collect!(&'static dyn TransactionHandler);