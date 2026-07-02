//! Identifier newtypes and derivations.
//!
//! All identifiers are TEXT in `state.db` (readable in a `sqlite3` shell).

use uuid::Uuid;

/// Daemon-internal photo identity: a UUIDv7 minted at first discovery,
/// never reissued. Becomes the consensus `photos.id` at RFC-011 migration.
///
/// Minted by the daemon (48-bit timestamp + 74 random bits), NOT taken from
/// PhotoKit — `local_id` is device-scoped and unstable, `cloud_id` can be
/// absent (local-only assets) or replaced (delete + re-import). Both are
/// stored as lookup keys alongside this stable internal handle.
#[derive(Debug, Clone, PartialEq, Eq, Hash, sqlx::Type, serde::Serialize, serde::Deserialize)]
#[sqlx(transparent)]
pub struct PhotoId(String);

impl PhotoId {
    /// Mint a fresh UUIDv7 (creation timestamp encoded).
    pub fn mint() -> Self {
        Self(Uuid::now_v7().to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for PhotoId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// BLAKE3 of resource bytes, lowercase hex. Blob storage address and
/// secondary dedup key.
#[derive(Debug, Clone, PartialEq, Eq, Hash, sqlx::Type, serde::Serialize, serde::Deserialize)]
#[sqlx(transparent)]
pub struct ContentHash(String);

impl ContentHash {
    /// Hash a byte slice. The streaming pipeline (Phase 2+) feeds a hasher
    /// incrementally instead; this is for tests and small payloads.
    pub fn of_bytes(bytes: &[u8]) -> Self {
        Self(blake3::hash(bytes).to_hex().to_string())
    }

    /// Wrap an already-computed lowercase hex digest.
    pub fn from_hex(hex: impl Into<String>) -> Self {
        Self(hex.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The `<aa>/<bb>` fan-out prefix used in blob paths.
    pub fn fanout(&self) -> (&str, &str) {
        (&self.0[0..2], &self.0[2..4])
    }
}

impl std::fmt::Display for ContentHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Short, stable, human-readable library identifier ('personal', …). Doubles
/// as the on-disk path component; the CLI enforces `[a-z0-9_]`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, sqlx::Type, serde::Serialize, serde::Deserialize)]
#[sqlx(transparent)]
pub struct LibraryId(String);

impl LibraryId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for LibraryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Grouping domains for `derive_group_id`. Only `burst` is implemented in
/// Phase 1; the other PhotoKit grouping identifiers were not spiked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupDomain {
    Burst,
}

impl GroupDomain {
    fn as_str(self) -> &'static str {
        match self {
            GroupDomain::Burst => "burst",
        }
    }
}

/// Derive a stable, convergent `group_id` from an opaque PhotoKit grouping
/// identifier (spec §Group identifiers):
///
/// `BLAKE3("ingress-v1/<domain>/" || identifier).hex()[..32]`
///
/// One-way: the PhotoKit identifier never leaves the daemon.
pub fn derive_group_id(domain: GroupDomain, photokit_identifier: &str) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"ingress-v1/");
    hasher.update(domain.as_str().as_bytes());
    hasher.update(b"/");
    hasher.update(photokit_identifier.as_bytes());
    hasher.finalize().to_hex()[..32].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    // Impact: group_id convergence across devices depends on this derivation
    // never changing; a silent change would split existing groups.
    // Should: produce the pinned golden value for a known burst identifier.
    // Should: produce 32 lowercase hex characters.
    #[test]
    fn group_id_derivation_is_stable() {
        let id = derive_group_id(GroupDomain::Burst, "E5A6CEB6-B839-45AD-A028-D625CD72470D");
        assert_eq!(id.len(), 32);
        assert!(id.chars().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
        // Golden value pinned from the first run of this derivation.
        assert_eq!(id, "1b789102453c47eb496002d53501d570");
    }

    // Should: mint distinct photo ids on every call.
    #[test]
    fn photo_ids_are_unique() {
        assert_ne!(PhotoId::mint(), PhotoId::mint());
    }

    // Should: expose the two-level fan-out prefix of a content hash.
    #[test]
    fn content_hash_fanout() {
        let h = ContentHash::from_hex("ab34cdef00112233445566778899aabb");
        assert_eq!(h.fanout(), ("ab", "34"));
    }
}
