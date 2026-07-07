/// Drive-owned (RFC-015): file/inode DB operations live in hopnet-drive;
/// re-exported at the old path so call sites (src/, snapshotter/) don't
/// churn. The two `stored_locally` functions are substrate-seam and moved
/// host-side to `db::fragments` — re-exported here for the same reason.
pub use hopnet_drive::db::files::*;

pub use super::fragments::{get_local_fragment_count, mark_fragments_local_state_batch};
