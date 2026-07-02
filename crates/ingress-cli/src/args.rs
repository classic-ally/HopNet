//! Argument tree. Thin by design: every subcommand parses here and executes
//! in `ingress-core` — the logic stays testable without this binary.

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};
use ingress_core::recover::RecoverLibrarySpec;

/// Operator CLI for the Apple Photos ingress daemon. Reads `state.db`
/// directly — no PhotoKit, runs without Photos authorization.
#[derive(Debug, Parser)]
#[command(name = "ingress-cli", version)]
pub struct Cli {
    /// Daemon data directory (state.db, sidecars, run lock).
    #[arg(long, global = true, default_value_os_t = default_data_dir())]
    pub data_dir: PathBuf,

    #[command(subcommand)]
    pub command: Command,
}

/// `~/.local/share/hopnet-photo-ingress` — the spec's canonical location.
fn default_data_dir() -> PathBuf {
    std::env::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".local/share/hopnet-photo-ingress")
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Library, pipeline, and per-photo views.
    Status(StatusArgs),
    /// Tier-2 invariant audit: refcounts, blob files, sidecars.
    Fsck(FsckArgs),
    /// Tier-3 disaster rebuild of state.db from a storage root.
    Recover(RecoverArgs),
    /// Library configuration.
    #[command(subcommand)]
    Library(LibraryCommand),
}

#[derive(Debug, Args)]
pub struct StatusArgs {
    /// Photo to inspect (photo_id or cloud_id); omit for the overview.
    pub photo: Option<String>,
    /// Retry cap used to split awaiting-retry from gave-up (match the
    /// daemon's --retry-cap).
    #[arg(long, default_value_t = 5)]
    pub retry_cap: i64,
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct FsckArgs {
    /// Apply repairs: refcount drift + orphan blob deletion (the one
    /// destructive repair). Takes the exclusive run lock.
    #[arg(long)]
    pub repair: bool,
    /// Byte-compare remote sidecars (default: existence check only).
    #[arg(long)]
    pub deep: bool,
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct RecoverArgs {
    /// Storage root(s) whose state-snapshots/ are searched. Repeatable.
    #[arg(long)]
    pub root: Vec<PathBuf>,
    /// Library spec(s) for the sidecar-tree rebuild:
    /// id=<id>,blob=<path>[,sidecars=<path>][,scope=personal|shared][,retention=<days>][,name=<label>].
    /// A wrong blob= path rebuilds a library pointing at the wrong tree —
    /// the post-rebuild blob verification is the backstop. Repeatable.
    #[arg(long, value_parser = parse_library_spec)]
    pub library: Vec<RecoverLibrarySpec>,
    /// Skip the snapshot search (the snapshots are known-bad).
    #[arg(long)]
    pub from_sidecars: bool,
    /// Move an existing state.db aside (state.db.pre-recover.<ts>) instead
    /// of refusing.
    #[arg(long)]
    pub force: bool,
}

fn parse_library_spec(s: &str) -> Result<RecoverLibrarySpec, String> {
    RecoverLibrarySpec::parse(s).map_err(|e| e.to_string())
}

#[derive(Debug, Subcommand)]
pub enum LibraryCommand {
    /// Add a library. The library_id is generated (e.g. brave_otter) and
    /// immutable; the display name is the label meant to change.
    Add(LibraryAddArgs),
    /// List configured libraries.
    List {
        #[arg(long)]
        json: bool,
    },
    /// Attach or detach the iCloud Shared Photo Library scope.
    Bind {
        id: String,
        /// `shared` binds the marker; `none` detaches it.
        #[arg(long)]
        scope: BindScope,
    },
    /// Change the display name (the library_id never changes).
    Rename { id: String, display_name: String },
    /// Change the tombstone retention window.
    SetRetention { id: String, days: i64 },
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum BindScope {
    Shared,
    None,
}

#[derive(Debug, Args)]
pub struct LibraryAddArgs {
    /// Absolute path to the per-library subtree on the storage root.
    #[arg(long)]
    pub blob_root: PathBuf,
    #[arg(long, value_enum)]
    pub scope: AddScope,
    #[arg(long)]
    pub display_name: Option<String>,
    /// Remote sidecar backup root. Omitting it degrades disaster recovery
    /// to blob-only — the command warns loudly.
    #[arg(long)]
    pub sidecar_remote: Option<PathBuf>,
    #[arg(long, default_value_t = 30)]
    pub retention_days: i64,
    /// Explicit id override (scripts/tests); lowercase [a-z0-9_].
    #[arg(long)]
    pub id: Option<String>,
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum AddScope {
    Personal,
    Shared,
}

#[cfg(test)]
mod tests {
    use super::*;

    // Impact: clap's debug_assert catches conflicting/misconfigured arg
    // definitions at test time instead of at first invocation in the field.
    // Should: the full command tree self-check passes.
    #[test]
    fn command_tree_is_valid() {
        use clap::CommandFactory;
        Cli::command().debug_assert();
    }
}
