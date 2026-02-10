use crate::AppState;
use crate::db::files as db;

pub mod routes;
pub mod test_routes;
pub mod rpc;
pub mod functions;
pub mod handlers;
pub mod placement;
pub mod distribution;
pub mod discovery;
pub mod jobs;
pub mod download;
pub mod types;

#[cfg(test)]
mod tests;