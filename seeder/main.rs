//! photo-seeder: seed a HopNet node with deterministic synthetic photos over
//! HTTP. Points at a locally running dev node by default; pass an
//! orchestrator mesh node's mapped port (see `orchestrator creds`) to seed a
//! mesh. Same (seed, count, months) always produces the same photos.

use clap::Parser;
use hopnet::dev_seed;

#[derive(Parser)]
#[command(
    name = "photo-seeder",
    about = "Seed a HopNet node with synthetic photos over HTTP"
)]
struct Args {
    /// Node base URL (local dev node or an orchestrator mesh node).
    /// Defaults to the local node's advertised loopback endpoint
    /// ($XDG_RUNTIME_DIR/hopnet/endpoint — the plaintext port is
    /// kernel-assigned now), then the fixed dev convention.
    #[arg(long)]
    base_url: Option<String>,

    #[arg(long, default_value = "allison")]
    username: String,

    /// Required unless --setup bootstraps a fresh node.
    #[arg(long, required_unless_present = "setup")]
    passphrase: Option<String>,

    /// Bootstrap a fresh node via POST /api/setup; prints the generated
    /// passphrase.
    #[arg(long)]
    setup: bool,

    #[arg(long, default_value = "seeder-node")]
    node_name: String,

    /// Number of photos to seed.
    #[arg(long, default_value_t = 24)]
    count: u32,

    /// Determinism seed; same seed reproduces the same photos.
    #[arg(long, default_value_t = 42)]
    seed: u64,

    /// Spread date_taken across this many distinct months (histogram rail).
    #[arg(long, default_value_t = 6)]
    months: u32,
}

/// Loopback discovery seam (mirrors hopnet-mount): the node writes its
/// plaintext loopback URL here because the port is kernel-assigned.
fn local_endpoint() -> Option<String> {
    let runtime_dir = std::env::var_os("XDG_RUNTIME_DIR")?;
    let path = std::path::Path::new(&runtime_dir).join("hopnet/endpoint");
    let url = std::fs::read_to_string(path).ok()?;
    let url = url.trim();
    (!url.is_empty()).then(|| url.to_string())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let base_url = args
        .base_url
        .clone()
        .or_else(local_endpoint)
        .unwrap_or_else(|| "http://localhost:34632".to_string());
    // Mesh nodes serve pinned-HTTPS with self-signed certs; a seeding tool
    // trusts its operator-supplied URL, not the cert chain.
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()?;

    let passphrase = if args.setup {
        let passphrase =
            dev_seed::setup_node(&client, &base_url, &args.username, &args.node_name).await?;
        println!("==========================================================");
        println!("  node bootstrapped — SAVE THIS PASSPHRASE:");
        println!("  {passphrase}");
        println!("==========================================================");
        passphrase
    } else {
        args.passphrase.clone().expect("clap enforces presence")
    };

    println!(
        "logging in as {} at {} (Argon2id — a few seconds)...",
        args.username, base_url
    );
    let jwt = dev_seed::login(&client, &base_url, &args.username, &passphrase).await?;

    dev_seed::enable_sidecar(&client, &base_url, &jwt).await?;
    println!("sidecar enabled");

    let mut posted = Vec::with_capacity(args.count as usize);
    for index in 0..args.count {
        let photo = dev_seed::generate_photo(args.seed, index, args.months);
        let result = dev_seed::post_photo(&client, &base_url, &jwt, &photo).await?;
        println!(
            "  [{}/{}] {} ({})",
            index + 1,
            args.count,
            result.photo_id,
            photo.asset.metadata.date_taken
        );
        posted.push(result);
    }

    println!(
        "seeded {} photos (seed {}, {} months) — open {}/photos and sign in to browse",
        posted.len(),
        args.seed,
        args.months,
        base_url
    );
    Ok(())
}
