use crate::{db::{DatabaseError, files::insert_files}, handlers::{HandlerResult, TransactionHandler}, db::Inode};
use crate::AppState;
use crate::files::functions::fragment_exists_and_valid;
use either::Either;

pub struct InsertFilesHandler;

impl TransactionHandler for InsertFilesHandler {
    fn name(&self) -> &'static str { "insert_files" }

    fn handle(&self, state: &AppState, payload: &[u8]) -> HandlerResult {
        match bincode::serde::decode_from_slice::<Vec<Inode>, _>(payload, bincode::config::standard()) {
            Ok((mut inodes, _)) => {
                // Preprocess inodes to verify fragments exist locally and update stored_locally flags
                for inode in &mut inodes {
                    if let Some(Either::Right(data_record)) = &mut inode.data_id {
                        for fragment in &mut data_record.data.fragments {
                            // Check if fragment exists and is valid on this node
                            fragment.stored_locally = fragment_exists_and_valid(
                                &state.fragments_dir, 
                                &fragment.fragment_hash
                            );
                        }
                    }
                }
                
                // Insert the files into the database with corrected stored_locally flags
                insert_files(state.db_pool.get(), inodes)?;
                Ok(())
            },
            Err(_) => Err(DatabaseError::InvalidPayload),
        }
    }
}

inventory::submit! {
    &InsertFilesHandler as &dyn TransactionHandler
}