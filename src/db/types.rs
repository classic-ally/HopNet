pub enum DatabaseError {
    LockError,
    InsertError,
    RecordError,
    RecallError,
    ProcessingError
}

use ed25519_dalek::SigningKey;
pub struct MyNode {
    pub node_id: i32,
    pub privkey: SigningKey,
}