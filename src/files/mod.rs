use crate::AppState;
use crate::db::files as db;

pub mod discovery;
pub mod distribution;
pub mod download;
pub mod functions;
pub mod handlers;
pub mod helpers;
pub mod jobs;
pub mod placement;
pub mod reference_provider;
pub mod routes;
pub mod rpc;
pub mod test_routes;
pub mod types;

#[cfg(test)]
mod tests;
