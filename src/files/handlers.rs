use crate::{db::{DatabaseError, files::insert_files}, handlers::{HandlerResult, TransactionHandler}, db::Inode};
use crate::AppState;

pub struct InsertFilesHandler;

impl TransactionHandler for InsertFilesHandler {
    fn name(&self) -> &'static str { "insert_files" }

    fn handle(&self, state: &AppState, payload: &[u8]) -> HandlerResult {
        match bincode::serde::decode_from_slice::<Vec<Inode>, _>(payload, bincode::config::standard()) {
            Ok((inodes, _)) => {
                // Insert the files into the database using the existing insert_files function
                insert_files(&state.db, inodes)?;
                Ok(())
            },
            Err(_) => Err(DatabaseError::InvalidPayload),
        }
    }
}

inventory::submit! {
    &InsertFilesHandler as &dyn TransactionHandler
}