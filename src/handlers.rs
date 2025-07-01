use crate::db::DatabaseError;
use crate::AppState;

pub type HandlerResult = Result<(), DatabaseError>;
pub trait TransactionHandler: Send + Sync {
    // Stable lookup name for function that handles thing
    fn name(&self) -> &'static str;

    // The method that executes the logic
    fn handle(&self, state: &AppState, payload: &[u8]) -> HandlerResult;

}

inventory::collect!(&'static dyn TransactionHandler);