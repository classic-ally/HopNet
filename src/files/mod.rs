use crate::AppState;
use crate::db::files as db;

pub mod routes;
pub mod test_routes;
pub mod rpc;
pub mod functions;
pub mod handlers;
pub mod helpers;
pub mod placement;
pub mod distribution;
pub mod discovery;
pub mod jobs;
pub mod download;
pub mod types;
pub mod reference_provider;

#[cfg(test)]
mod tests;