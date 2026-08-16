//! hopnet-mount daemon binary (RFC-018).
//!
//! S8: two subcommands. `login` validates and stores credentials (Secret
//! Service, file fallback); `mount` resolves credentials/URL through the
//! provisioning tiers, cleans up stale state from a crashed predecessor,
//! and serves until SIGINT/SIGTERM.

#[cfg(target_os = "linux")]
fn main() {
    linux::run();
}

#[cfg(not(target_os = "linux"))]
fn main() {
    // RFC-022 S1: every client binary answers --version, even the
    // unsupported-platform stub. RFC-023 S1: --min-node likewise.
    if std::env::args().any(|a| a == "--version" || a == "-V") {
        println!("hopnet-mount {}", env!("CARGO_PKG_VERSION"));
        return;
    }
    if std::env::args().any(|a| a == "--min-node") {
        println!("{}", hopnet_mount::min_node_display());
        return;
    }
    eprintln!("hopnet-mount only supports Linux (RFC-018)");
    std::process::exit(1);
}

#[cfg(target_os = "linux")]
mod linux {
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use clap::{Parser, Subcommand};

    use hopnet_mount::attrs::DEFAULT_TTL;
    use hopnet_mount::fuse::HopFs;
    use hopnet_mount::http_transport::HttpTransport;
    use hopnet_mount::mock::MockTransport;
    use hopnet_mount::provision::{self, Paths, StoredIn};
    use hopnet_mount::transport::{Health, ItemId, NodeTransport, TransportError};
    use hopnet_mount::vfs::MountCore;

    #[derive(Parser)]
    #[command(
        name = "hopnet-mount",
        version,
        about = "Mount the HopNet drive (RFC-018)"
    )]
    struct Cli {
        #[command(subcommand)]
        command: Command,
    }

    #[derive(Subcommand)]
    enum Command {
        /// Mount the drive at a directory (e.g. ~/HopDrive)
        Mount(MountArgs),
        /// Validate and store a device token for this user
        Login(LoginArgs),
    }

    #[derive(clap::Args)]
    struct MountArgs {
        /// Directory to mount the drive at (created if missing)
        mountpoint: PathBuf,

        /// Serve a built-in fake tree instead of a node
        #[arg(long)]
        mock: bool,

        /// Node base URL (default: login config > node endpoint file >
        /// http://127.0.0.1:34632)
        #[arg(long)]
        url: Option<String>,

        /// File containing the device token (`device_id.secret`);
        /// HOPNET_MOUNT_TOKEN env takes precedence, stored credentials
        /// (login) are the fallback
        #[arg(long)]
        token_file: Option<PathBuf>,

        /// Content cache directory (default: $XDG_CACHE_HOME/hopnet/content)
        #[arg(long)]
        cache_dir: Option<PathBuf>,

        /// Write staging directory — DURABLE, survives restarts
        /// (default: $XDG_DATA_HOME/hopnet/staging)
        #[arg(long)]
        staging_dir: Option<PathBuf>,

        /// Disable FUSE passthrough even where the kernel and
        /// privileges would allow it (measurement baseline / escape
        /// hatch); passthrough otherwise arms itself when possible
        #[arg(long)]
        no_passthrough: bool,
    }

    #[derive(clap::Args)]
    struct LoginArgs {
        /// Node base URL (default: node endpoint file >
        /// http://127.0.0.1:34632)
        #[arg(long)]
        url: Option<String>,

        /// Read the token from a file instead of prompting
        #[arg(long)]
        token_file: Option<PathBuf>,
    }

    /// `$XDG_DATA_HOME/hopnet` — durable per-user daemon state (staging
    /// lives under it; the crash-cleanup connection record beside it).
    fn default_data_dir() -> PathBuf {
        let base = std::env::var_os("XDG_DATA_HOME")
            .map(PathBuf::from)
            .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".local/share")))
            .unwrap_or_else(|| PathBuf::from("/tmp"));
        base.join("hopnet")
    }

    fn default_staging_dir() -> PathBuf {
        default_data_dir().join("staging")
    }

    fn default_cache_dir() -> PathBuf {
        let base = std::env::var_os("XDG_CACHE_HOME")
            .map(PathBuf::from)
            .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".cache")))
            .unwrap_or_else(|| PathBuf::from("/tmp"));
        base.join("hopnet").join("content")
    }

    pub fn run() {
        // --min-node, the sibling of --version (RFC-023 S1): answered
        // before clap because Cli requires a subcommand. Its only caller
        // is the upgrade wrapper interrogating a staged binary.
        if std::env::args().any(|a| a == "--min-node") {
            println!("{}", hopnet_mount::min_node_display());
            return;
        }
        tracing_subscriber::fmt().init();
        match Cli::parse().command {
            Command::Mount(args) => mount(args),
            Command::Login(args) => login(args),
        }
    }

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("tokio runtime")
    }

    /// Readiness preflight (RFC-018): distinguish "not running" from
    /// "running, not set up" instead of mounting into EIO. RFC-022 S4:
    /// the same probe settles both version policies before any user
    /// action — the node's gate answers 426 if THIS build is too old,
    /// and the report's node_version is checked against MIN_NODE.
    fn preflight(rt: &tokio::runtime::Runtime, transport: &HttpTransport, url: &str) {
        match rt.block_on(transport.health()) {
            Ok(report) => {
                if let Err(why) = hopnet_mount::check_node_version(&report) {
                    eprintln!("{why}");
                    std::process::exit(1);
                }
                match report.status {
                    Health::Ready => {}
                    Health::NotReady => {
                        eprintln!("node at {url} is running but not set up");
                        std::process::exit(1);
                    }
                }
            }
            Err(e @ TransportError::UpgradeRequired { .. }) => {
                eprintln!("{e} — upgrade hopnet-mount");
                std::process::exit(1);
            }
            Err(e) => {
                eprintln!("node not reachable at {url}: {e}");
                std::process::exit(1);
            }
        }
    }

    fn login(args: LoginArgs) {
        let rt = runtime();
        let paths = Paths::from_env();
        let url = provision::resolve_url(args.url.as_deref(), &paths);

        let token = match &args.token_file {
            Some(path) => match std::fs::read_to_string(path) {
                Ok(token) => token.trim().to_string(),
                Err(e) => {
                    eprintln!("cannot read token file {}: {e}", path.display());
                    std::process::exit(2);
                }
            },
            None => match provision::prompt_token() {
                Ok(token) => token,
                Err(e) => {
                    eprintln!("cannot read token: {e}");
                    std::process::exit(2);
                }
            },
        };
        if token.is_empty() {
            eprintln!("empty token; paste the `device_id.secret` string from the Devices pane");
            std::process::exit(2);
        }
        // Shape check before any network call: a mangled paste gets a
        // clear message here rather than the node's 400 on every request.
        let wellformed = token.split_once('.').is_some_and(|(id, secret)| {
            id.parse::<hopnet_common::CustomUUID>().is_ok() && !secret.is_empty()
        });
        if !wellformed {
            eprintln!("malformed token; expected `device_id.secret` from the Devices pane");
            std::process::exit(2);
        }

        let transport = match HttpTransport::new(&url, &token) {
            Ok(t) => t,
            Err(e) => {
                eprintln!("transport setup failed: {e}");
                std::process::exit(1);
            }
        };
        preflight(&rt, &transport, &url);

        // One authed read proves the token before anything is stored.
        match rt.block_on(transport.item(ItemId::Root)) {
            Ok(_) => {}
            Err(TransportError::Unauthorized) => {
                eprintln!("token rejected by {url} (invalid or revoked)");
                std::process::exit(2);
            }
            Err(e) => {
                eprintln!("token validation failed: {e}");
                std::process::exit(1);
            }
        }

        match rt.block_on(provision::store_token(&paths, &token)) {
            Ok(StoredIn::SecretService) => println!("token stored in the Secret Service keyring"),
            Ok(StoredIn::File(path)) => {
                println!("token stored in {} (0600)", path.display())
            }
            Err(e) => {
                eprintln!("could not store token: {e}");
                std::process::exit(1);
            }
        }
        if let Err(e) = provision::store_config_url(&paths, &url) {
            eprintln!("could not store node URL: {e}");
            std::process::exit(1);
        }
        println!("logged in against {url}; mount with: hopnet-mount mount ~/HopDrive");
    }

    fn resolve_token(rt: &tokio::runtime::Runtime, args: &MountArgs, paths: &Paths) -> String {
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
        if let Some(token) = rt.block_on(provision::load_token(paths)) {
            return token;
        }
        eprintln!(
            "no device token: run `hopnet-mount login`, or set HOPNET_MOUNT_TOKEN / --token-file"
        );
        std::process::exit(2);
    }

    /// Lazily unmount a fuse mount a crashed predecessor left on the
    /// mountpoint. A stale mountpoint stats as ENOTCONN, so this parses
    /// /proc/self/mounts rather than statting.
    fn cleanup_stale_mount(mountpoint: &Path) {
        let Ok(mounts) = std::fs::read_to_string("/proc/self/mounts") else {
            return;
        };
        let target = mountpoint.to_string_lossy();
        let stale = mounts.lines().any(|line| {
            let mut fields = line.split_whitespace();
            let (Some(_), Some(mp), Some(fstype)) = (fields.next(), fields.next(), fields.next())
            else {
                return false;
            };
            mp == target && fstype.starts_with("fuse")
        });
        if stale {
            tracing::info!("unmounting stale fuse mount at {}", mountpoint.display());
            let _ = std::process::Command::new("fusermount3")
                .arg("-uz")
                .arg(mountpoint)
                .status();
        }
    }

    fn mount(args: MountArgs) {
        let rt = runtime();
        let paths = Paths::from_env();

        let mut url = String::new();
        let transport: Arc<dyn NodeTransport> = if args.mock {
            MockTransport::with_demo_tree()
        } else {
            url = provision::resolve_url(args.url.as_deref(), &paths);
            let token = resolve_token(&rt, &args, &paths);
            let transport = match HttpTransport::new(&url, &token) {
                Ok(t) => t,
                Err(e) => {
                    eprintln!("transport setup failed: {e}");
                    std::process::exit(1);
                }
            };
            preflight(&rt, &transport, &url);
            Arc::new(transport)
        };

        let cache_config = hopnet_mount::cache::CacheConfig {
            root: args.cache_dir.clone().unwrap_or_else(default_cache_dir),
            segment_size: hopnet_mount::cache::DEFAULT_SEGMENT_SIZE,
            policy: hopnet_mount::cache::EvictionPolicy::default_min_free(
                hopnet_mount::cache::DEFAULT_SEGMENT_SIZE,
            ),
        };
        let cache = match hopnet_mount::cache::CacheManager::new(cache_config, transport.clone()) {
            Ok(cache) => Arc::new(cache),
            Err(e) => {
                eprintln!("cache setup failed: {e}");
                std::process::exit(1);
            }
        };

        let staging = match hopnet_mount::staging::Staging::new(
            args.staging_dir.clone().unwrap_or_else(default_staging_dir),
        ) {
            Ok(staging) => Arc::new(staging),
            Err(e) => {
                eprintln!("staging setup failed: {e}");
                std::process::exit(1);
            }
        };

        let core = Arc::new(
            MountCore::new(transport.clone(), DEFAULT_TTL)
                .with_cache(cache)
                .with_staging(staging),
        );

        // Recover dirty content a previous run left staged (S7).
        rt.block_on(core.recover());

        // Crash cleanup (S8): lazily unmount a stale predecessor mount,
        // then abort its orphaned kernel connection — a lazy unmount
        // alone leaves wedged /dev/fuse ops alive, and those block
        // system suspend.
        let data_dir = default_data_dir();
        if let Err(e) = std::fs::create_dir_all(&args.mountpoint) {
            eprintln!(
                "cannot create mountpoint {}: {e}",
                args.mountpoint.display()
            );
            std::process::exit(1);
        }
        cleanup_stale_mount(&args.mountpoint);
        provision::abort_recorded_conn(&data_dir);

        let fs = HopFs::new(core.clone(), rt.handle().clone(), !args.no_passthrough);

        let mut config = fuser::Config::default();
        config.mount_options = vec![fuser::MountOption::FSName("hopnet".to_string())];
        let session = match fuser::spawn_mount(fs, &args.mountpoint, &config) {
            Ok(session) => session,
            Err(e) => {
                eprintln!("mount failed at {}: {e}", args.mountpoint.display());
                std::process::exit(1);
            }
        };
        tracing::info!(
            mountpoint = %args.mountpoint.display(),
            source = %if args.mock { "mock".to_string() } else { url.clone() },
            "mounted"
        );

        // Record the fusectl connection id (= minor of the mount's
        // st_dev) so the next start can abort it if we die uncleanly.
        match std::fs::metadata(&args.mountpoint) {
            Ok(meta) => {
                use std::os::unix::fs::MetadataExt;
                let conn_id = rustix::fs::minor(meta.dev());
                if let Err(e) = provision::write_conn_record(&data_dir, conn_id as u64) {
                    tracing::warn!("could not write connection record: {e}");
                }
            }
            Err(e) => tracing::warn!("could not stat mountpoint for connection record: {e}"),
        }

        // Watch loop (RFC-018 S4): pokes → delta sync → kernel busting.
        let invalidator = Arc::new(hopnet_mount::fuse::FuserInvalidator(session.notifier()));
        rt.spawn(
            hopnet_mount::watch::Watcher::new(core.clone(), transport.clone(), invalidator).run(),
        );

        // systemd stops with SIGTERM; interactive use sends SIGINT.
        rt.block_on(async {
            let mut sigterm =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("sigterm handler");
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = sigterm.recv() => {}
            }
        });
        drop(session);
        provision::remove_conn_record(&data_dir);
        tracing::info!("unmounted");
    }
}
