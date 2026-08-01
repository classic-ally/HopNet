//! ingress-cli — operator surface for the Apple Photos ingress daemon
//! (spec §Phase 6). Parse, call ingress-core, render; exit codes follow
//! fsck(8): 0 clean/success, 1 findings remain, 2 usage/operational error.

mod args;
mod render;

use std::path::Path;
use std::process::ExitCode;

use clap::Parser;
use ingress_core::descriptor::LibraryScope;
use ingress_core::paths::DataDir;
use ingress_core::{IngressError, LibraryId, StateStore};

use args::{AddScope, BindScope, Cli, Command, LibraryCommand};

fn main() -> ExitCode {
    let cli = Cli::parse();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    match runtime.block_on(run(cli)) {
        Ok(code) => code,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::from(2)
        }
    }
}

async fn run(cli: Cli) -> Result<ExitCode, IngressError> {
    let data_dir = DataDir::new(&cli.data_dir);
    match cli.command {
        Command::Status(args) => {
            let store = open_read_only(&data_dir).await?;
            match args.photo {
                Some(key) => {
                    let Some(view) =
                        ingress_core::status::photo_status(&store, &data_dir.spool(), &key).await?
                    else {
                        eprintln!("no photo matches {key:?} (tried photo_id, then cloud_id)");
                        return Ok(ExitCode::from(2));
                    };
                    if args.json {
                        render::print_json(&view);
                    } else {
                        render::print_photo(&view);
                    }
                }
                None => {
                    let report = ingress_core::status::status(&store, args.retry_cap).await?;
                    if args.json {
                        render::print_json(&report);
                    } else {
                        render::print_status(&report);
                    }
                }
            }
            Ok(ExitCode::SUCCESS)
        }

        Command::Fsck(args) => {
            let store = if args.repair {
                open_existing_rw(&data_dir).await?
            } else {
                warn_if_daemon_live(&data_dir);
                open_read_only(&data_dir).await?
            };
            let opts = ingress_core::fsck::FsckOptions {
                repair: args.repair,
            };
            let report = ingress_core::fsck::run_fsck(&store, &data_dir, &opts).await?;
            if args.json {
                render::print_json(&report);
            } else {
                render::print_fsck(&report);
            }
            Ok(if report.is_clean() {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(1)
            })
        }

        Command::Library(cmd) => run_library(&data_dir, cmd).await,
    }
}

async fn run_library(data_dir: &DataDir, cmd: LibraryCommand) -> Result<ExitCode, IngressError> {
    match cmd {
        LibraryCommand::Add(args) => {
            // Creates state.db on a fresh data dir — `library add` is part
            // of first-time setup.
            std::fs::create_dir_all(data_dir.root())
                .map_err(|e| IngressError::Invariant(format!("data dir: {e}")))?;
            let store = StateStore::open(&data_dir.state_db_path()).await?;
            let id = args.id.as_deref().map(LibraryId::parse).transpose()?;
            let opts = ingress_core::libconfig::AddLibraryOptions {
                id,
                display_name: args.display_name,
                scope: match args.scope {
                    AddScope::Personal => LibraryScope::Personal,
                    AddScope::Shared => LibraryScope::Shared,
                },
                retention_days: args.retention_days,
            };
            let added = ingress_core::libconfig::add_library(&store, data_dir, &opts).await?;
            println!(
                "added library {} ({:?})",
                added.config.library_id, added.config.display_name
            );
            Ok(ExitCode::SUCCESS)
        }
        LibraryCommand::List { json } => {
            let store = open_read_only(data_dir).await?;
            let libraries = store.libraries().await?;
            if json {
                render::print_json(&libraries);
            } else {
                let rows: Vec<Vec<String>> = libraries
                    .iter()
                    .map(|l| {
                        vec![
                            l.library_id.to_string(),
                            l.display_name.clone(),
                            l.scope_binding.clone().unwrap_or_else(|| "personal".into()),
                            l.retention_days.to_string(),
                        ]
                    })
                    .collect();
                render::table(&["ID", "NAME", "SCOPE", "RETENTION"], &rows);
            }
            Ok(ExitCode::SUCCESS)
        }
        LibraryCommand::Bind { id, scope } => {
            let store = open_existing_rw(data_dir).await?;
            let id = LibraryId::parse(&id)?;
            let scope = match scope {
                BindScope::Shared => Some(LibraryScope::Shared),
                BindScope::None => None,
            };
            ingress_core::libconfig::bind_scope(&store, data_dir, &id, scope).await?;
            println!("bound {id}");
            Ok(ExitCode::SUCCESS)
        }
        LibraryCommand::Rename { id, display_name } => {
            let store = open_existing_rw(data_dir).await?;
            let id = LibraryId::parse(&id)?;
            ingress_core::libconfig::rename_library(&store, data_dir, &id, &display_name).await?;
            println!("renamed {id} to {display_name:?}");
            Ok(ExitCode::SUCCESS)
        }
        LibraryCommand::SetRetention { id, days } => {
            let store = open_existing_rw(data_dir).await?;
            let id = LibraryId::parse(&id)?;
            ingress_core::libconfig::set_retention(&store, data_dir, &id, days).await?;
            println!("retention for {id} set to {days} days");
            Ok(ExitCode::SUCCESS)
        }
    }
}

/// Read-only open with the WAL-recovery fallback: a read-only connection
/// cannot recover a hot `-wal` left by a crashed daemon. When that open
/// fails and no live daemon holds the run lock, a normal open is safe (no
/// live writer; migrations are a no-op) and heals the WAL.
async fn open_read_only(data_dir: &DataDir) -> Result<StateStore, IngressError> {
    let path = data_dir.state_db_path();
    match StateStore::open_read_only(&path).await {
        Ok(store) => Ok(store),
        Err(e) => {
            if path.is_file() && !lock_is_live(data_dir) {
                eprintln!(
                    "note: read-only open failed ({e}); no live daemon — opening read-write to recover the WAL"
                );
                StateStore::open(&path).await
            } else {
                Err(e)
            }
        }
    }
}

/// Read-write open that refuses to invent a store (write commands other
/// than `library add` and `recover` need an existing one).
async fn open_existing_rw(data_dir: &DataDir) -> Result<StateStore, IngressError> {
    let path = data_dir.state_db_path();
    if !path.is_file() {
        return Err(IngressError::Invariant(format!(
            "no state store at {} — run the daemon once to create it, or see `recover`",
            path.display()
        )));
    }
    StateStore::open(&path).await
}

fn warn_if_daemon_live(data_dir: &DataDir) {
    if lock_is_live(data_dir) {
        eprintln!(
            "warning: a daemon appears to be running — findings may include in-flight work \
             (a just-renamed blob shows as an orphan until its transaction commits)"
        );
    }
}

/// Lock-file presence as the liveness heuristic. A stale (dead-pid) lock
/// reads as live here — the only cost is a skipped WAL-recovery fallback
/// or an extra warning banner, never a wrong repair.
fn lock_is_live(data_dir: &DataDir) -> bool {
    Path::new(&data_dir.root().join("drain.lock")).exists()
}
