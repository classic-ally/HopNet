//! hopnet-mount daemon binary (RFC-018).
//!
//! S3: mounts a real node's tree read-only over the HTTP transport, or the
//! built-in mock tree (`--mock`). Provisioning is deliberately minimal —
//! URL flag + token env/file; Secret Service, endpoint discovery, and the
//! systemd unit are S8.

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
    use hopnet_mount::http_transport::HttpTransport;
    use hopnet_mount::mock::MockTransport;
    use hopnet_mount::transport::{Health, NodeTransport};
    use hopnet_mount::vfs::MountCore;

    #[derive(Parser)]
    #[command(name = "hopnet-mount", about = "Mount the HopNet drive (RFC-018)")]
    struct Args {
        /// Directory to mount the drive at (e.g. ~/HopDrive)
        mountpoint: PathBuf,

        /// Serve a built-in fake tree instead of a node
        #[arg(long)]
        mock: bool,

        /// Node base URL
        #[arg(long, default_value = "http://127.0.0.1:34632")]
        url: String,

        /// File containing the device token (`device_id.secret`); the
        /// HOPNET_MOUNT_TOKEN env var takes precedence
        #[arg(long)]
        token_file: Option<PathBuf>,
    }

    fn resolve_token(args: &Args) -> String {
        if let Ok(token) = std::env::var("HOPNET_MOUNT_TOKEN") {
            return token.trim().to_string();
        }
        if let Some(path) = &args.token_file {
            match std::fs::read_to_string(path) {
                Ok(token) => return token.trim().to_string(),
                Err(e) => {
                    eprintln!("cannot read token file {}: {e}", path.display());
                    std::process::exit(2);
                }
            }
        }
        eprintln!("no device token: set HOPNET_MOUNT_TOKEN or pass --token-file");
        std::process::exit(2);
    }

    pub fn run() {
        tracing_subscriber::fmt().init();
        let args = Args::parse();

        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");

        let transport: Arc<dyn NodeTransport> = if args.mock {
            MockTransport::with_demo_tree()
        } else {
            let token = resolve_token(&args);
            let transport = match HttpTransport::new(&args.url, &token) {
                Ok(t) => Arc::new(t),
                Err(e) => {
                    eprintln!("transport setup failed: {e}");
                    std::process::exit(1);
                }
            };
            // Readiness preflight (RFC-018): distinguish "not running"
            // from "running, not set up" instead of mounting into EIO.
            match rt.block_on(transport.health()) {
                Ok(Health::Ready) => {}
                Ok(Health::NotReady) => {
                    eprintln!("node at {} is running but not set up", args.url);
                    std::process::exit(1);
                }
                Err(e) => {
                    eprintln!("node not reachable at {}: {e}", args.url);
                    std::process::exit(1);
                }
            }
            transport
        };

        let core = Arc::new(MountCore::new(transport.clone(), DEFAULT_TTL));
        let fs = HopFs::new(core.clone(), rt.handle().clone());

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
        tracing::info!(
            mountpoint = %args.mountpoint.display(),
            source = %if args.mock { "mock".to_string() } else { args.url.clone() },
            "mounted"
        );

        // Watch loop (RFC-018 S4): pokes → delta sync → kernel busting.
        let invalidator = Arc::new(hopnet_mount::fuse::FuserInvalidator(session.notifier()));
        rt.spawn(
            hopnet_mount::watch::Watcher::new(core.clone(), transport.clone(), invalidator).run(),
        );

        rt.block_on(async {
            let _ = tokio::signal::ctrl_c().await;
        });
        drop(session);
        tracing::info!("unmounted");
    }
}
