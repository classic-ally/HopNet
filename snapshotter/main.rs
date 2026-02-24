use std::path::PathBuf;

use clap::{Parser, Subcommand};

mod capture;
mod compare;
mod schema;

#[derive(Parser)]
#[command(name = "snapshotter", about = "DB function regression snapshot tool")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Capture a snapshot of all DB read functions against fixture data
    Capture {
        /// Output path for the snapshot JSON
        #[arg(short, long, default_value = "snapshot.json")]
        output: PathBuf,
    },
    /// Capture a snapshot at a specific git commit using worktrees
    CaptureAt {
        /// Git commit hash to build and capture at
        #[arg(short, long)]
        commit: String,
        /// Output path for the snapshot JSON
        #[arg(short, long)]
        output: Option<PathBuf>,
    },
    /// Compare two snapshots and report differences
    Compare {
        /// Path to the baseline snapshot
        #[arg(short, long)]
        baseline: PathBuf,
        /// Path to the current snapshot
        #[arg(short, long)]
        current: PathBuf,
        /// Epsilon for floating-point comparison
        #[arg(short, long, default_value = "1e-10")]
        epsilon: f64,
    },
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Capture { output } => {
            capture::run_capture(&output);
        }
        Commands::CaptureAt { commit, output } => {
            let output = output.unwrap_or_else(|| {
                PathBuf::from(format!("snapshot-{}.json", &commit[..7.min(commit.len())]))
            });
            run_capture_at(&commit, &output);
        }
        Commands::Compare {
            baseline,
            current,
            epsilon,
        } => {
            let has_diff = compare::run_compare(&baseline, &current, epsilon);
            std::process::exit(if has_diff { 1 } else { 0 });
        }
    }
}

fn run_capture_at(commit: &str, output: &std::path::Path) {
    let worktree_dir = format!("/tmp/hopnet-snap-{}", &commit[..7.min(commit.len())]);

    println!("Creating worktree at {}...", worktree_dir);
    let status = std::process::Command::new("git")
        .args(["worktree", "add", &worktree_dir, commit])
        .status()
        .expect("Failed to create git worktree");
    if !status.success() {
        eprintln!("Failed to create worktree for commit {}", commit);
        std::process::exit(1);
    }

    println!("Building snapshotter in worktree...");
    let build_status = std::process::Command::new("cargo")
        .args([
            "build",
            "--release",
            "--bin",
            "snapshotter",
            "--features",
            "skip-frontend",
        ])
        .current_dir(&worktree_dir)
        .status()
        .expect("Failed to build in worktree");

    if !build_status.success() {
        cleanup_worktree(&worktree_dir);
        eprintln!("Build failed in worktree");
        std::process::exit(1);
    }

    let output_abs = std::fs::canonicalize(".").unwrap().join(output);

    println!("Running capture...");
    let run_status = std::process::Command::new("./target/release/snapshotter")
        .args(["capture", "--output", &output_abs.to_string_lossy()])
        .current_dir(&worktree_dir)
        .status()
        .expect("Failed to run snapshotter in worktree");

    cleanup_worktree(&worktree_dir);

    if !run_status.success() {
        eprintln!("Capture failed in worktree");
        std::process::exit(1);
    }

    println!("Snapshot saved to {}", output.display());
}

fn cleanup_worktree(dir: &str) {
    let _ = std::process::Command::new("git")
        .args(["worktree", "remove", "--force", dir])
        .status();
}
