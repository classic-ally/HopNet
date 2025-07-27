use crate::{db::{DatabaseError, users::insert_user}, handlers::{HandlerResult, TransactionHandler}, types::User};
use crate::AppState;

pub struct InsertUserHandler;

impl TransactionHandler for InsertUserHandler {
    fn name(&self) -> &'static str { "insert_user" }

    fn process(&self, state: &AppState, payload: &[u8], execute: bool) -> HandlerResult {
        match bincode::serde::decode_from_slice::<User, _>(payload, bincode::config::standard()) {
            Ok((user_data, _)) => {
                // Insert the user into the database with execute flag
                insert_user(state.db_pool.get(), user_data, execute)?;
                Ok(())
            },
            Err(_) => Err(DatabaseError::InvalidPayload),
        }
    }
}

inventory::submit! {
    &InsertUserHandler as &dyn TransactionHandler
}