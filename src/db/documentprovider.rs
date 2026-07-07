/// Drive-owned (RFC-015): the DocumentProvider DB surface lives in
/// hopnet-drive; re-exported at the old path so call sites don't churn.
pub use hopnet_drive::db::documentprovider::*;
