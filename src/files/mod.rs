use crate::AppState;
use crate::db::files as db;

pub mod download;
pub mod functions;
pub mod handlers;
pub mod helpers;
pub mod jobs;
pub mod placement;
// Drive-owned (RFC-015, Stage D3): FilesystemReferenceProvider lives in
// hopnet_drive::reference_provider and registers cross-crate via inventory.
pub mod routes;
pub mod rpc;
pub mod substrate_host;
pub mod test_routes;
pub mod types;

#[cfg(test)]
mod tests;
