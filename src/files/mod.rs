use crate::AppState;
use crate::db::files as db;

pub mod routes;
pub mod functions;
pub mod handlers;
pub mod placement;
pub mod distribution;
pub mod discovery;
pub mod jobs;
pub mod download;

#[cfg(test)]
mod tests;