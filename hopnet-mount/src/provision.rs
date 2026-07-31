//! Provisioning (RFC-018 S8): where the daemon's credentials and node
//! URL come from, and the storage half of the `login` flow.
//!
//! Token precedence at mount time: `HOPNET_MOUNT_TOKEN` env >
//! `--token-file` (both handled by the caller) > Secret Service >
//! 0600 fallback file. URL precedence: `--url` > login-written config >
//! node-written runtime endpoint file > the fixed headless default.
//!
//! The Secret Service tier talks D-Bus to the session keyring
//! (gnome-keyring, KWallet); headless sessions without one fall through
//! to the file tier transparently, in both directions. Tests exercise
//! the file/config/precedence layers only — the Secret Service on a dev
//! machine is the REAL user keyring and must not be touched by `cargo
//! test`.

use std::path::{Path, PathBuf};

/// Headless nodes bind this fixed port; everything else must be
/// discovered or configured.
pub const DEFAULT_URL: &str = "http://127.0.0.1:34632";

const SS_ATTR_SERVICE: &str = "service";
const SS_ATTR_VALUE: &str = "hopnet-mount";
const SS_LABEL: &str = "HopNet Mount device token";

/// Filesystem roots provisioning reads/writes; injectable for tests.
pub struct Paths {
    /// `$XDG_CONFIG_HOME/hopnet` — `mount.json` + fallback token file.
    pub config: PathBuf,
    /// `$XDG_RUNTIME_DIR/hopnet` — the node-written endpoint file.
    pub runtime: Option<PathBuf>,
}

impl Paths {
    pub fn from_env() -> Self {
        let config = std::env::var_os("XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".config")))
            .unwrap_or_else(|| PathBuf::from("/tmp"))
            .join("hopnet");
        let runtime = std::env::var_os("XDG_RUNTIME_DIR").map(|d| PathBuf::from(d).join("hopnet"));
        Paths { config, runtime }
    }

    fn token_file(&self) -> PathBuf {
        self.config.join("mount-token")
    }

    fn config_file(&self) -> PathBuf {
        self.config.join("mount.json")
    }
}

/// Where `store_token` ended up putting the credential.
#[derive(Debug, PartialEq, Eq)]
pub enum StoredIn {
    SecretService,
    File(PathBuf),
}

/// Store the device token: Secret Service when the session has one,
/// 0600 file otherwise.
pub async fn store_token(paths: &Paths, token: &str) -> std::io::Result<StoredIn> {
    match ss_store(token).await {
        Ok(()) => Ok(StoredIn::SecretService),
        Err(e) => {
            tracing::debug!("secret service unavailable, using file fallback: {e}");
            store_token_file(paths, token)?;
            Ok(StoredIn::File(paths.token_file()))
        }
    }
}

/// Load the stored device token: Secret Service first, then the
/// fallback file. None = never logged in on this tier.
pub async fn load_token(paths: &Paths) -> Option<String> {
    match ss_load().await {
        Ok(Some(token)) => return Some(token),
        Ok(None) => {}
        Err(e) => tracing::debug!("secret service unavailable, trying file fallback: {e}"),
    }
    load_token_file(paths)
}

/// Fallback-file write, 0600 — the token is a bearer credential.
pub fn store_token_file(paths: &Paths, token: &str) -> std::io::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;

    std::fs::create_dir_all(&paths.config)?;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(paths.token_file())?;
    file.write_all(token.as_bytes())?;
    Ok(())
}

pub fn load_token_file(paths: &Paths) -> Option<String> {
    let token = std::fs::read_to_string(paths.token_file()).ok()?;
    let token = token.trim();
    (!token.is_empty()).then(|| token.to_string())
}

/// Persist the URL login validated against, so `mount` needs no flags.
pub fn store_config_url(paths: &Paths, url: &str) -> std::io::Result<()> {
    std::fs::create_dir_all(&paths.config)?;
    let json = serde_json::json!({ "url": url });
    std::fs::write(paths.config_file(), format!("{json}\n"))
}

pub fn load_config_url(paths: &Paths) -> Option<String> {
    let raw = std::fs::read_to_string(paths.config_file()).ok()?;
    let parsed: serde_json::Value = serde_json::from_str(&raw).ok()?;
    Some(parsed.get("url")?.as_str()?.to_string())
}

/// The node-written endpoint file (same-user discovery; the Linux arm
/// of the macOS Keychain base_url refresh).
pub fn read_endpoint(paths: &Paths) -> Option<String> {
    let file = paths.runtime.as_ref()?.join("endpoint");
    let url = std::fs::read_to_string(file).ok()?;
    let url = url.trim();
    (!url.is_empty()).then(|| url.to_string())
}

/// URL precedence: explicit flag > login config > node endpoint file >
/// fixed headless default.
pub fn resolve_url(cli: Option<&str>, paths: &Paths) -> String {
    if let Some(url) = cli {
        return url.to_string();
    }
    if let Some(url) = load_config_url(paths) {
        return url;
    }
    if let Some(url) = read_endpoint(paths) {
        return url;
    }
    DEFAULT_URL.to_string()
}

/// Prompt for the token on stdin; echo suppressed on a tty (it's a
/// paste-once bearer credential).
pub fn prompt_token() -> std::io::Result<String> {
    use std::io::{BufRead, IsTerminal, Write};

    let stdin = std::io::stdin();
    let is_tty = stdin.is_terminal();
    if is_tty {
        eprint!("device token (device_id.secret): ");
        std::io::stderr().flush()?;
    }

    let restore = if is_tty { echo_off()? } else { None };
    let mut line = String::new();
    let read = stdin.lock().read_line(&mut line);
    if let Some(saved) = restore {
        let _ = rustix::termios::tcsetattr(
            std::io::stdin(),
            rustix::termios::OptionalActions::Flush,
            &saved,
        );
        eprintln!();
    }
    read?;
    Ok(line.trim().to_string())
}

fn echo_off() -> std::io::Result<Option<rustix::termios::Termios>> {
    let stdin = std::io::stdin();
    let saved = rustix::termios::tcgetattr(&stdin)?;
    let mut quiet = saved.clone();
    quiet.local_modes &= !rustix::termios::LocalModes::ECHO;
    rustix::termios::tcsetattr(&stdin, rustix::termios::OptionalActions::Flush, &quiet)?;
    Ok(Some(saved))
}

async fn ss_store(token: &str) -> Result<(), secret_service::Error> {
    use secret_service::{EncryptionType, SecretService};

    let ss = SecretService::connect(EncryptionType::Dh).await?;
    let collection = ss.get_default_collection().await?;
    collection.ensure_unlocked().await?;
    collection
        .create_item(
            SS_LABEL,
            std::collections::HashMap::from([(SS_ATTR_SERVICE, SS_ATTR_VALUE)]),
            token.as_bytes(),
            true, // replace: login twice = rotate in place
            "text/plain",
        )
        .await?;
    Ok(())
}

async fn ss_load() -> Result<Option<String>, secret_service::Error> {
    use secret_service::{EncryptionType, SecretService};

    let ss = SecretService::connect(EncryptionType::Dh).await?;
    let search = ss
        .search_items(std::collections::HashMap::from([(
            SS_ATTR_SERVICE,
            SS_ATTR_VALUE,
        )]))
        .await?;
    let Some(item) = search.unlocked.first() else {
        // Locked hits: try to unlock rather than silently falling back.
        let Some(item) = search.locked.first() else {
            return Ok(None);
        };
        item.unlock().await?;
        let secret = item.get_secret().await?;
        return Ok(Some(String::from_utf8_lossy(&secret).trim().to_string()));
    };
    let secret = item.get_secret().await?;
    Ok(Some(String::from_utf8_lossy(&secret).trim().to_string()))
}

/// The connection record for crash cleanup: `minor(st_dev)` of a live
/// mount IS its fusectl connection id. A lazy unmount detaches the
/// namespace but leaves the kernel connection (and any wedged ops —
/// they block system suspend); the record lets the next start abort
/// exactly our orphan and nothing else.
pub fn conn_record_path(data_dir: &Path) -> PathBuf {
    data_dir.join("mount-conn")
}

pub fn write_conn_record(data_dir: &Path, conn_id: u64) -> std::io::Result<()> {
    std::fs::create_dir_all(data_dir)?;
    std::fs::write(conn_record_path(data_dir), format!("{conn_id}\n"))
}

pub fn remove_conn_record(data_dir: &Path) {
    let _ = std::fs::remove_file(conn_record_path(data_dir));
}

/// Abort the previously-recorded fuse connection if it still exists.
/// Best-effort: every failure mode just means nothing to clean up.
pub fn abort_recorded_conn(data_dir: &Path) {
    let record = conn_record_path(data_dir);
    let Ok(raw) = std::fs::read_to_string(&record) else {
        return;
    };
    if let Ok(conn_id) = raw.trim().parse::<u64>() {
        let abort = PathBuf::from(format!("/sys/fs/fuse/connections/{conn_id}/abort"));
        if abort.exists() {
            match std::fs::write(&abort, "1") {
                Ok(()) => tracing::info!("aborted orphaned fuse connection {conn_id}"),
                Err(e) => tracing::warn!("could not abort fuse connection {conn_id}: {e}"),
            }
        }
    }
    let _ = std::fs::remove_file(&record);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn paths_in(dir: &Path) -> Paths {
        Paths {
            config: dir.join("config").join("hopnet"),
            runtime: Some(dir.join("runtime").join("hopnet")),
        }
    }

    // Impact: the token is a bearer credential on disk; group/world
    // readability would hand the drive to any local user.
    // Should: round-trip the token through the fallback file.
    // Should: create the fallback file with mode 0600.
    #[test]
    fn token_file_round_trips_with_owner_only_mode() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().expect("tempdir");
        let paths = paths_in(dir.path());

        store_token_file(&paths, "  abc.def  ").expect("store");
        assert_eq!(load_token_file(&paths).as_deref(), Some("abc.def"));

        let mode = std::fs::metadata(paths.config.join("mount-token"))
            .expect("metadata")
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
    }

    // Should: report no token when the fallback file is absent or blank.
    #[test]
    fn missing_or_blank_token_file_yields_none() {
        let dir = tempfile::tempdir().expect("tempdir");
        let paths = paths_in(dir.path());
        assert_eq!(load_token_file(&paths), None);

        store_token_file(&paths, "   ").expect("store");
        assert_eq!(load_token_file(&paths), None);
    }

    // Impact: wrong precedence silently mounts against the wrong node —
    // an explicit flag or a validated login must never lose to ambient
    // discovery state.
    // Should: prefer the explicit URL over config, endpoint, default.
    // Should: prefer login-written config over the endpoint file.
    // Should: prefer the endpoint file over the fixed default.
    #[test]
    fn url_resolution_precedence() {
        let dir = tempfile::tempdir().expect("tempdir");
        let paths = paths_in(dir.path());

        assert_eq!(resolve_url(None, &paths), DEFAULT_URL);

        let runtime = paths.runtime.clone().expect("runtime dir");
        std::fs::create_dir_all(&runtime).expect("mkdir");
        std::fs::write(runtime.join("endpoint"), "http://127.0.0.1:40001\n").expect("write");
        assert_eq!(resolve_url(None, &paths), "http://127.0.0.1:40001");

        store_config_url(&paths, "http://127.0.0.1:40002").expect("store");
        assert_eq!(resolve_url(None, &paths), "http://127.0.0.1:40002");

        assert_eq!(
            resolve_url(Some("http://10.0.0.7:34632"), &paths),
            "http://10.0.0.7:34632"
        );
    }

    // Should: round-trip the config URL through mount.json.
    #[test]
    fn config_url_round_trips() {
        let dir = tempfile::tempdir().expect("tempdir");
        let paths = paths_in(dir.path());
        assert_eq!(load_config_url(&paths), None);
        store_config_url(&paths, "http://192.168.1.20:34632").expect("store");
        assert_eq!(
            load_config_url(&paths).as_deref(),
            Some("http://192.168.1.20:34632")
        );
    }

    // Should: ignore a missing or malformed connection record.
    // Should: remove the record after an abort attempt.
    #[test]
    fn conn_record_lifecycle() {
        let dir = tempfile::tempdir().expect("tempdir");
        abort_recorded_conn(dir.path()); // absent: no panic

        write_conn_record(dir.path(), 999_999).expect("write");
        assert!(conn_record_path(dir.path()).exists());
        // Connection 999999 does not exist under fusectl; the record is
        // still consumed so a dead id cannot wedge every future start.
        abort_recorded_conn(dir.path());
        assert!(!conn_record_path(dir.path()).exists());
    }
}
