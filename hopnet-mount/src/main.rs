//! hopnet-mount daemon binary (RFC-018).
//!
//! S1 mounts the mock tree only (`--mock`); the real node transport
//! arrives in S3, credential/endpoint provisioning in S8.

#[cfg(target_os = "linux")]
fn main() {
    linux::run();
}

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("hopnet-mount only supports Linux (RFC-018)");
    std::process::exit(1);
}

#[cfg(target_os = "linux")]
mod linux {
    use std::path::PathBuf;
    use std::sync::Arc;

    use clap::Parser;

    use hopnet_mount::attrs::DEFAULT_TTL;
    use hopnet_mount::fuse::HopFs;
    use hopnet_mount::mock::MockTransport;
    use hopnet_mount::vfs::MountCore;

    #[derive(Parser)]
    #[command(name = "hopnet-mount", about = "Mount the HopNet drive (RFC-018)")]
    struct Args {
        /// Directory to mount the drive at (e.g. ~/HopDrive)
        mountpoint: PathBuf,

        /// Serve a built-in fake tree instead of a node (S1 demo)
        #[arg(long)]
        mock: bool,
    }

    pub fn run() {
        tracing_subscriber::fmt().init();
        let args = Args::parse();

        if !args.mock {
            eprintln!("the real node transport lands in S3; run with --mock for the demo tree");
            std::process::exit(2);
        }

        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");

        let core = Arc::new(MountCore::new(MockTransport::with_demo_tree(), DEFAULT_TTL));
        let fs = HopFs::new(core, rt.handle().clone());

        let mut config = fuser::Config::default();
        config.mount_options = vec![
            fuser::MountOption::RO,
            fuser::MountOption::FSName("hopnet".to_string()),
        ];
        let session = match fuser::spawn_mount(fs, &args.mountpoint, &config) {
            Ok(session) => session,
            Err(e) => {
                eprintln!("mount failed at {}: {e}", args.mountpoint.display());
                std::process::exit(1);
            }
        };
        tracing::info!(mountpoint = %args.mountpoint.display(), "mounted (mock tree)");

        rt.block_on(async {
            let _ = tokio::signal::ctrl_c().await;
        });
        drop(session);
        tracing::info!("unmounted");
    }
}
