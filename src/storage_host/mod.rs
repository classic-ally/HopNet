//! The host's storage-substrate surface (RFC-014/016): consensus handlers
//! for placement/GC/self-check, maintenance jobs + routes, the fragment
//! RPC, and the engine seam adapters. Renamed from `src/files/` at
//! RFC-016 Stage 6 — the drive's fs logic left for hopnet-drive in
//! RFC-015; everything here serves the storage substrate. These stay
//! HOST-side deliberately: the layering is projection → storage (so
//! hopnet-storage can't see the handler seam), and the orphan GC
//! consults the takeout gate, which storage could never depend on.

use crate::AppState;
use crate::db::files as db;

pub mod db_apply;
pub mod functions;
pub mod handlers;
pub mod jobs;
pub mod placement;
pub mod routes;
pub mod substrate_host;
pub mod test_routes;

#[cfg(test)]
mod tests;
