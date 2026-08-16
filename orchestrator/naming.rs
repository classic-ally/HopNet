//! Per-checkout naming for every container resource the orchestrator owns.
//!
//! Multiple checkouts (git worktrees) of this repo share one container
//! daemon. Everything the orchestrator creates or scans is namespaced by a
//! short hash of the checkout path so concurrent orchestrators never see —
//! let alone delete — each other's images, containers, networks, or volumes.
//!
//! The orchestrator is a `[[bin]]` of the root `hopnet` package, so
//! `CARGO_MANIFEST_DIR` is the checkout root itself — and the same hash names
//! the disposable data directories in `hopnet::paths`, which is where it now
//! lives.

/// Label stamped on every relay/node container and volume. Deletion filters
/// require it to match [`checkout_hash`], so resources created by another
/// checkout (or by the pre-namespace scheme, which never set it) are
/// untouchable.
pub const CHECKOUT_LABEL: &str = "hopnet.checkout";

/// Stable per-checkout hash. `HOPNET_ORCH_HASH` overrides (test/escape hatch).
pub use hopnet::paths::checkout_hash;

/// Image ref used for container creation AND written into the loaded
/// archive's RepoTags by `load-image`. `HOPNET_ORCH_IMAGE` overrides.
pub fn image_ref() -> String {
    std::env::var("HOPNET_ORCH_IMAGE").unwrap_or_else(|_| format!("hopnet:{}", checkout_hash()))
}

/// `hopnet-<hash>-` — the prefix of every resource name this checkout owns.
pub fn prefix() -> String {
    format!("hopnet-{}-", checkout_hash())
}

pub fn network_name(mesh_id: u32) -> String {
    format!("{}{}-0", prefix(), mesh_id)
}

pub fn container_name(mesh_id: u32, node_id: u32) -> String {
    format!("{}{}-{}", prefix(), mesh_id, node_id)
}

pub fn relay_container_name(mesh_id: u32) -> String {
    format!("{}{}-relay", prefix(), mesh_id)
}

pub fn relay_url(mesh_id: u32) -> String {
    format!("http://{}:3340", relay_container_name(mesh_id))
}

pub fn volume_name(mesh_id: u32, node_id: u32) -> String {
    format!("{}{}-{}-data", prefix(), mesh_id, node_id)
}

/// Strip the docker API's optional leading '/' and this checkout's prefix.
/// `None` means the resource belongs to another checkout (or scheme).
fn strip(name: &str) -> Option<&str> {
    let name = name.strip_prefix('/').unwrap_or(name);
    name.strip_prefix(prefix().as_str())
}

pub fn is_ours(name: &str) -> bool {
    strip(name).is_some()
}

/// Mesh id of any of our resource names ("3-1", "3-relay", "3-1-data", …).
pub fn mesh_id_of(name: &str) -> Option<u32> {
    strip(name)?.split('-').next()?.parse().ok()
}

/// Node id for node container names; `None` for the relay and networks'
/// trailing "0" is indistinguishable from node 0 by design (callers only
/// apply this to container names, as before).
pub fn node_id_of(name: &str) -> Option<u32> {
    strip(name)?.split('-').nth(1)?.parse().ok()
}

/// Preferred-port slot rotation (see `sys::find_available_port`): spreads
/// different checkouts' mesh 0 across the 51 port slots in 40000..65500.
pub fn checkout_slot() -> u32 {
    let hex = &checkout_hash()[..8.min(checkout_hash().len())];
    u32::from_str_radix(hex, 16).unwrap_or(0) % 51
}

#[cfg(test)]
mod tests {
    use super::*;

    // Should: recover mesh and node ids from every constructor's output,
    //         including names the docker API returns with a leading slash.
    #[test]
    fn constructor_parser_round_trips() {
        assert_eq!(mesh_id_of(&container_name(3, 7)), Some(3));
        assert_eq!(node_id_of(&container_name(3, 7)), Some(7));
        assert_eq!(mesh_id_of(&network_name(4)), Some(4));
        assert_eq!(mesh_id_of(&relay_container_name(5)), Some(5));
        assert_eq!(mesh_id_of(&volume_name(6, 2)), Some(6));
        let slashed = format!("/{}", container_name(1, 2));
        assert_eq!(mesh_id_of(&slashed), Some(1));
        assert_eq!(node_id_of(&slashed), Some(2));
    }

    // Should: treat the relay's name segment as no node id.
    #[test]
    fn relay_has_no_node_id() {
        assert_eq!(node_id_of(&relay_container_name(5)), None);
    }

    // Should not: claim resources from another checkout or from the old
    //             machine-global naming scheme.
    #[test]
    fn foreign_names_are_not_ours() {
        assert!(!is_ours("hopnet-orchestrator-0-0"));
        assert!(!is_ours("hopnet-ffffffff-0-0"));
        assert_eq!(mesh_id_of("hopnet-orchestrator-0-0"), None);
    }

    // Should: shape the prefix as hopnet-<8 lowercase hex>-.
    #[test]
    fn prefix_shape() {
        let p = prefix();
        let hash = p
            .strip_prefix("hopnet-")
            .and_then(|s| s.strip_suffix('-'))
            .unwrap();
        assert_eq!(hash.len(), 8);
        assert!(
            hash.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        );
    }

    // Should: keep the rotation slot inside the 51-slot port range.
    #[test]
    fn checkout_slot_in_range() {
        assert!(checkout_slot() < 51);
    }
}
