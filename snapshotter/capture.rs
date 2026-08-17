use std::collections::BTreeMap;
use std::path::Path;

use chrono::Utc;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;

use crate::schema::{FunctionResult, Snapshot};

mod fixtures;
mod functions;

fn get_git_info() -> (String, bool) {
    let commit = std::process::Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_else(|_| "unknown".to_string());

    let dirty = std::process::Command::new("git")
        .args(["diff", "--quiet", "HEAD"])
        .status()
        .map(|s| !s.success())
        .unwrap_or(true);

    (commit, dirty)
}

fn create_ephemeral_pool() -> Pool<SqliteConnectionManager> {
    let manager = SqliteConnectionManager::memory();
    Pool::builder()
        .max_size(1)
        .connection_customizer(Box::new(hopnet::db::shared::SqliteInitializer))
        .build(manager)
        .expect("Failed to create ephemeral pool")
}

pub fn run_capture(output: &Path) {
    println!("Creating ephemeral database...");
    let pool = create_ephemeral_pool();

    println!("Initializing schema...");
    {
        let conn = pool.get().expect("Failed to get connection");
        hopnet::db::chains::install(&conn).expect("Failed to install schema");
    }

    println!("Seeding fixtures...");
    let ctx = fixtures::seed_all(&pool);

    println!("Capturing function outputs...");
    let results = functions::capture_all(&pool, &ctx);

    let (git_commit, git_dirty) = get_git_info();

    let snapshot = Snapshot {
        version: 1,
        git_commit,
        git_dirty,
        captured_at: Utc::now(),
        fixture_version: "1.0".to_string(),
        functions: results,
    };

    let json = serde_json::to_string_pretty(&snapshot).expect("Failed to serialize snapshot");
    std::fs::write(output, &json).expect("Failed to write snapshot file");

    let ok_count = snapshot
        .functions
        .values()
        .filter(|r| matches!(r, FunctionResult::Ok { .. }))
        .count();
    let err_count = snapshot.functions.len() - ok_count;

    println!(
        "Captured {} functions ({} ok, {} error) -> {}",
        snapshot.functions.len(),
        ok_count,
        err_count,
        output.display()
    );
}
