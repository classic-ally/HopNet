/// Drive-owned (RFC-015): share DB operations live in hopnet-drive;
/// re-exported at the old path so call sites don't churn.
pub use hopnet_drive::db::shares::*;
