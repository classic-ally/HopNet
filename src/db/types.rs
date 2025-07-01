#[derive(Debug)]
pub enum DatabaseError {
    LockError,
    InsertError,
    RecordError,
    RecallError,
    ProcessingError,
    InvalidPayload
}

use crate::db::PrivKey;
pub struct MyNode {
    pub node_id: i32,
    pub privkey: PrivKey,
}