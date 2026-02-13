pub mod handler;
pub mod protocol;
pub mod routes;
pub mod transport;

pub use protocol::{IrohRequest, IrohResponse};
pub use transport::{IrohError, IrohTransport};

/// Cache for deduplicating retried iroh requests on the receiver side.
/// Maps request_id → OnceCell that holds the serialized response bytes.
/// Uses std::sync::Mutex (not tokio) because the critical section is just a HashMap lookup/insert.
pub type DedupCache = std::sync::Mutex<std::collections::HashMap<u64, std::sync::Arc<tokio::sync::OnceCell<Vec<u8>>>>>;
