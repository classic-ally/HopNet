use crate::{db::{DatabaseError, users::insert_user_tx}, handlers::{HandlerResult, TransactionHandler}, types::User, consensus::types::Transaction};
use crate::AppState;

pub struct InsertUserHandler;

impl TransactionHandler for InsertUserHandler {
    fn name(&self) -> &'static str { "insert_user" }

    fn process(&self, _state: &AppState, tx: &Transaction, _execute: bool, db_tx: &duckdb::Transaction) -> HandlerResult {
        match bincode::serde::decode_from_slice::<User, _>(&tx.rpc.payload, bincode::config::standard()) {
            Ok((user_data, _)) => {
                // Insert the user into the database using shared transaction
                insert_user_tx(db_tx, user_data)?;
                Ok(())
            },
            Err(_) => Err(DatabaseError::InvalidPayload),
        }
    }
}

inventory::submit! {
    &InsertUserHandler as &dyn TransactionHandler
}