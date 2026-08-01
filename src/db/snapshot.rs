//! Host assembly of the canonical state snapshot (RFC-019 S1).
//!
//! Each schema-owning unit declares its own `SNAPSHOT_SECTION` /
//! `NODE_LOCAL_TABLES` next to its DDL; this module assembles them in the
//! `install_schema` walk order (= FK direction) and is the single source
//! of truth for "what is consensus state". The registry test below pins
//! the universe against sqlite_master, so a new table cannot silently
//! escape both the divergence check and the epoch export.

use hopnet_common::snapshot::{self, NodeStateReport, SectionSpec, TableSpec};

use crate::db::DatabaseError;

/// The host's own section: identity, membership inputs, telemetry, and
/// device auth — everything consensus-replicated that no extracted crate
/// owns. committed_tx_nonces is covered: rows are written and pruned only
/// by consensus transactions, so honest nodes agree on it.
pub const HOST_SECTION: SectionSpec = SectionSpec {
    name: "host",
    // v2: nodes gained the version-attestation columns (RFC-019 S3).
    // v3: regenesis_state joined (RFC-019 S5) — divergence-checked,
    //     never exported (epoch N+1 is born normal from the genesis
    //     installer, the decided_blocks precedent).
    format_version: 3,
    tables: &[
        TableSpec::exported("sequences"),
        TableSpec::exported("users"),
        TableSpec::exported("nodes"),
        TableSpec::exported("metrics"),
        TableSpec::exported("fragment_request_metrics"),
        TableSpec::exported("device_tokens"),
        TableSpec::exported("committed_tx_nonces"),
        TableSpec {
            name: "regenesis_state",
            role: snapshot::TableRole::DivergenceOnly,
            excluded_columns: &[],
        },
    ],
};

/// Host-owned node-local tables — outside the snapshot universe entirely.
pub const HOST_NODE_LOCAL_TABLES: &[&str] = &["this_node", "pending_fragment_requests"];

/// All sections, in the install_schema walk order (= FK direction; the
/// section order is therefore also a valid import insert order). The
/// named units sit below the projection seam — same split the schema
/// chain makes in `initialize` — then every registered projection
/// contributes via `Projection::snapshot_section` (RFC-016 amendment,
/// RFC-019 S2). Adding a projection touches zero host lines; adding a
/// named unit = one SNAPSHOT_SECTION const + one line here.
pub fn sections() -> Vec<&'static SectionSpec> {
    let mut sections: Vec<&'static SectionSpec> = vec![
        &HOST_SECTION,
        &hopnet_consensus::store::SNAPSHOT_SECTION,
        &hopnet_storage::store::SNAPSHOT_SECTION,
    ];
    sections.extend(
        crate::projections::manifests()
            .iter()
            .filter_map(|p| p.snapshot_section()),
    );
    sections
}

/// Union of every unit's node-local tables.
pub fn node_local_tables() -> Vec<&'static str> {
    let mut tables = Vec::new();
    tables.extend_from_slice(HOST_NODE_LOCAL_TABLES);
    tables.extend_from_slice(hopnet_consensus::store::NODE_LOCAL_TABLES);
    tables.extend_from_slice(hopnet_storage::store::NODE_LOCAL_TABLES);
    for projection in crate::projections::manifests() {
        tables.extend_from_slice(projection.node_local_tables());
    }
    tables
}

/// Manifest + height from one transaction snapshot, so the height and
/// every table hash describe the same decided state.
pub fn compute_node_state_tx(tx: &rusqlite::Transaction) -> Result<NodeStateReport, DatabaseError> {
    let consensus_height = crate::db::consensus::get_current_consensus_height(tx)?;
    let manifest = snapshot::compute_manifest(tx, &sections()).map_err(|e| {
        tracing::error!("state snapshot failed: {e}");
        DatabaseError::ProcessingError
    })?;
    Ok(NodeStateReport {
        consensus_height,
        manifest,
    })
}

/// Import an epoch snapshot artifact into this (fresh) database — the
/// S6 boot-gate entry point, fixed here beside the manifest reader.
/// Caller commits (via crate::db::shared::commit_timed on real paths).
pub fn import_snapshot_tx(
    tx: &rusqlite::Transaction,
    artifact: &[u8],
) -> Result<snapshot::ImportReport, snapshot::SnapshotError> {
    snapshot::import_snapshot(tx, &sections(), artifact)
}

/// Convenience wrapper that manages transaction creation.
pub fn compute_node_state(
    db_connection: Result<r2d2::PooledConnection<crate::db::SqliteConnectionManager>, r2d2::Error>,
) -> Result<NodeStateReport, DatabaseError> {
    let mut conn = db_connection.map_err(|_| DatabaseError::LockError)?;
    let tx = conn.transaction().map_err(|_| DatabaseError::LockError)?;
    compute_node_state_tx(&tx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn fresh_pool() -> r2d2::Pool<crate::db::SqliteConnectionManager> {
        let manager = crate::db::SqliteConnectionManager::memory();
        let pool = r2d2::Pool::builder()
            .max_size(1)
            .connection_customizer(Box::new(crate::db::shared::SqliteInitializer))
            .build(manager)
            .unwrap();
        crate::db::shared::initialize(pool.get().unwrap()).unwrap();
        pool
    }

    /// One or two deterministic rows into a representative table of every
    /// section (all five crates, both roles, the REAL columns, and every
    /// FK chain) — fixed keys, no wall clock, so the artifact bytes are
    /// reproducible forever.
    fn seed(conn: &rusqlite::Connection) {
        conn.execute_batch(
            "INSERT INTO sequences VALUES ('users', 2);
             INSERT INTO users (user_id, username, pubkey, x25519_pubkey, encrypted_privkey, key_salt)
             VALUES (1, 'alice', X'AA01', X'BB02', X'CC03', X'DD04');
             INSERT INTO nodes VALUES (1, 'n1', 1, X'0A', 20260800, NULL, 3);
             INSERT INTO nodes VALUES (2, 'n2', 1, X'0B', NULL, NULL, NULL);
             INSERT INTO metrics VALUES (1, 2, '2026-01-01T00:00:00Z', 10.5, 0.25, 0.125, 1000, 5, 1, 100, 50);
             INSERT INTO fragment_request_metrics VALUES (1, 1, 2, 5, 10, 9);
             INSERT INTO device_tokens VALUES ('dt1', 1, X'EE05', 'abcd', X'FF06');
             INSERT INTO committed_tx_nonces VALUES ('nonce-1');
             INSERT INTO regenesis_state VALUES (1, 2, 20260801, X'5EA1', 7);
             INSERT INTO validators VALUES (1, 1, 1, NULL);
             INSERT INTO validators VALUES (1, 2, 1, NULL);
             INSERT INTO hopnet_consensus_policy VALUES ('ck', 'cv');
             INSERT INTO decided_blocks VALUES (1, X'01', 0, X'B0');
             INSERT INTO decided_blocks VALUES (2, X'02', 0, X'B1');
             INSERT INTO data_blocks VALUES ('blob1', '2026-01-01T00:00:00Z', X'99', 30, 1, 5, 1000);
             INSERT INTO blob_access VALUES ('blob1', X'BB02', X'E1', X'77');
             INSERT INTO mesh_key VALUES (1, X'AB', 1);
             INSERT INTO mesh_key_access VALUES (X'BB02', X'E2', X'78');
             INSERT INTO fragment_hashes VALUES ('blob1', 0, 0, 'f1', X'F1', 0, 1);
             INSERT INTO fragment_inventory VALUES (X'F1', 1, 42);
             INSERT INTO hopnet_storage_policy VALUES ('sk', 'sv');
             INSERT INTO inodes VALUES ('i1', 1, 'deadbeef', 0, 'blob1');
             INSERT INTO incoming_shares VALUES ('s1', 'blob1', 1, 1, X'AC', X'AD', X'AE');
             INSERT INTO shares VALUES ('blob1', 1);
             INSERT INTO takeouts VALUES ('t1', 1, 1, 0, '2026-01-02T00:00:00Z', 5);
             INSERT INTO imports VALUES ('im1', 1, 1, 0);",
        )
        .unwrap();
    }

    // Should: cover exactly the full schema — covered tables plus
    // node-local tables equal sqlite_master's user tables, with no
    // overlap and no table in two sections.
    // Impact: a new table cannot silently escape both the divergence
    // universe and the epoch export set; it must be classified at birth.
    #[test]
    fn registry_covers_schema_exactly() {
        let pool = fresh_pool();
        let conn = pool.get().unwrap();

        let mut stmt = conn
            .prepare(
                "SELECT name FROM sqlite_master
                 WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
            )
            .unwrap();
        let schema_tables: HashSet<String> = stmt
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();

        let mut covered: HashSet<String> = HashSet::new();
        for section in sections() {
            for table in section.tables {
                assert!(
                    covered.insert(table.name.to_string()),
                    "table {} appears in two sections",
                    table.name
                );
            }
        }
        let node_local: HashSet<String> =
            node_local_tables().iter().map(|t| t.to_string()).collect();

        assert!(
            covered.is_disjoint(&node_local),
            "covered ∩ node-local: {:?}",
            covered.intersection(&node_local).collect::<Vec<_>>()
        );
        let universe: HashSet<String> = covered.union(&node_local).cloned().collect();
        assert_eq!(
            universe,
            schema_tables,
            "unclassified: {:?}; stale: {:?}",
            schema_tables.difference(&universe).collect::<Vec<_>>(),
            universe.difference(&schema_tables).collect::<Vec<_>>()
        );
    }

    // Should: produce the pinned top hash and per-section hashes on a
    // freshly installed empty schema.
    // Impact: golden anchor for the whole assembly — any change to the
    // covered set, section order, or encoding shows up here first.
    #[test]
    fn empty_db_golden() {
        let pool = fresh_pool();
        let report = compute_node_state(pool.get()).unwrap();
        assert_eq!(report.consensus_height, 0);

        let actual: Vec<(String, String)> = report
            .manifest
            .sections
            .iter()
            .map(|s| (s.name.clone(), s.section_hash.to_hex()))
            .collect();
        let expected: Vec<(String, String)> = EMPTY_SECTION_HASHES
            .iter()
            .map(|(n, h)| (n.to_string(), h.to_string()))
            .collect();
        assert_eq!(actual, expected);
        assert_eq!(report.manifest.top_hash.to_hex(), EMPTY_TOP_HASH);
    }

    const EMPTY_TOP_HASH: &str = "98a6b6eb298ae078036e9a91b1ce9bbc13600f48f7d6db33fbac17738a5c53dc";
    const EMPTY_SECTION_HASHES: &[(&str, &str)] = &[
        (
            "host",
            "bd6624fc32983b3ae8f4ce149f34e291c281eefde17dde82b40ef0bb6cd2cc0c",
        ),
        (
            "consensus",
            "3dd26220b0cf874b464ad2172c70a135204b75f22da4a02d31e482de660c2933",
        ),
        (
            "storage",
            "8a3c2e0b2d1a34c26fc6c2794543e42da783bd22ec9790d6fdc465ffc6f8ac35",
        ),
        (
            "drive",
            "e97564093bd2fc9180738c64b3a8a5a00b2c1c1c3d00b3f3dfd354e33cd96950",
        ),
        (
            "takeout",
            "e8512917a2d0f7620d5478afe5c02fef48aaec907d11415fccbff27dbec8966e",
        ),
    ];

    // Should: produce the pinned top hash, artifact hash, and artifact
    // length for the seeded deterministic fixture.
    #[test]
    fn seeded_db_golden() {
        let pool = fresh_pool();
        seed(&pool.get().unwrap());
        let mut conn = pool.get().unwrap();
        let tx = conn.transaction().unwrap();
        let (artifact, manifest) =
            hopnet_common::snapshot::serialize_snapshot(&tx, &sections()).unwrap();

        assert_eq!(manifest.top_hash.to_hex(), SEEDED_TOP_HASH);
        assert_eq!(
            blake3::hash(&artifact).to_hex().to_string(),
            SEEDED_ARTIFACT_HASH
        );
        assert_eq!(artifact.len(), SEEDED_ARTIFACT_LEN);
    }

    const SEEDED_TOP_HASH: &str =
        "731999ef6d44ae6797b7f2735db193ff2cb6ceae9064deb3164f65b8b330937e";
    const SEEDED_ARTIFACT_HASH: &str =
        "1ce11ba7a4a57dfbf9b0032eda5391715a606994a0e4a00cfa43ce96bfe3fae1";
    const SEEDED_ARTIFACT_LEN: usize = 3207;

    // Should: report identical manifests from the hash-only walk and the
    // full export, and byte-identical artifacts from two independently
    // built databases with the same logical content.
    // Impact: rebuild determinism across nodes is the whole point — this
    // includes the REAL metric columns, the one value class where SQLite
    // itself offers no cross-version text stability.
    #[test]
    fn serialize_matches_manifest_and_rebuilds_deterministically() {
        let pool_a = fresh_pool();
        seed(&pool_a.get().unwrap());
        let pool_b = fresh_pool();
        seed(&pool_b.get().unwrap());

        let report = compute_node_state(pool_a.get()).unwrap();
        let mut conn_a = pool_a.get().unwrap();
        let tx_a = conn_a.transaction().unwrap();
        let (artifact_a, manifest_a) =
            hopnet_common::snapshot::serialize_snapshot(&tx_a, &sections()).unwrap();
        drop(tx_a);
        let mut conn_b = pool_b.get().unwrap();
        let tx_b = conn_b.transaction().unwrap();
        let (artifact_b, manifest_b) =
            hopnet_common::snapshot::serialize_snapshot(&tx_b, &sections()).unwrap();

        assert_eq!(manifest_a, report.manifest);
        assert_eq!(manifest_a, manifest_b);
        assert_eq!(artifact_a, artifact_b);
    }

    /// Exported-role table manifests — the subset the post-import parity
    /// check compares (DivergenceOnly tables legitimately differ on a
    /// fresh database).
    fn exported_tables(
        manifest: &hopnet_common::SnapshotManifest,
    ) -> Vec<&hopnet_common::TableManifest> {
        manifest
            .sections
            .iter()
            .flat_map(|s| {
                s.tables
                    .iter()
                    .filter(|t| t.role == hopnet_common::TableRole::Exported)
            })
            .collect()
    }

    // Should: serialize the seeded five-section fixture, import it into
    // a second freshly initialized database, and get an equal top hash,
    // equal per-section hashes, equal exported table manifests, and a
    // byte-identical re-serialized artifact; the excluded node-local
    // columns land as fresh-node values.
    // Should not: compare DivergenceOnly manifests — decided_blocks
    // differs on a fresh node by design.
    // Impact: this is RFC-019 S2's acceptance gate over the real schema,
    // foreign-key chains included — the epoch boundary's state-transfer
    // contract.
    #[test]
    fn fresh_db_import_roundtrip_gate() {
        let source_pool = fresh_pool();
        seed(&source_pool.get().unwrap());
        let mut source = source_pool.get().unwrap();
        let tx = source.transaction().unwrap();
        let (artifact, manifest_src) =
            hopnet_common::snapshot::serialize_snapshot(&tx, &sections()).unwrap();
        drop(tx);

        let target_pool = fresh_pool();
        let mut target = target_pool.get().unwrap();
        {
            let tx = target.transaction().unwrap();
            let report = import_snapshot_tx(&tx, &artifact).unwrap();
            assert!(report.skipped.is_empty());
            assert_eq!(report.imported.len(), 5);
            tx.commit().unwrap();
        }

        let tx = target.transaction().unwrap();
        let (artifact_again, manifest_re) =
            hopnet_common::snapshot::serialize_snapshot(&tx, &sections()).unwrap();
        assert_eq!(artifact_again, artifact);
        assert_eq!(manifest_re.top_hash, manifest_src.top_hash);
        for (re, src) in manifest_re.sections.iter().zip(&manifest_src.sections) {
            assert_eq!(re.section_hash, src.section_hash, "section {}", re.name);
        }
        assert_eq!(
            exported_tables(&manifest_re),
            exported_tables(&manifest_src)
        );

        let (stored_locally, self_verified): (i64, Option<i64>) = tx
            .query_row(
                "SELECT stored_locally, self_verified_height
                 FROM fragment_hashes fh JOIN fragment_inventory fi
                 ON fh.fragment_hash = fi.fragment_hash",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(stored_locally, 0);
        assert_eq!(self_verified, None);
    }

    // Should not: import into a database that already has covered rows;
    // the error names the first non-empty table.
    #[test]
    fn import_into_nonempty_db_fails_loudly() {
        let source_pool = fresh_pool();
        seed(&source_pool.get().unwrap());
        let mut source = source_pool.get().unwrap();
        let tx = source.transaction().unwrap();
        let (artifact, _) = hopnet_common::snapshot::serialize_snapshot(&tx, &sections()).unwrap();
        drop(tx);

        let target_pool = fresh_pool();
        seed(&target_pool.get().unwrap());
        let mut target = target_pool.get().unwrap();
        let tx = target.transaction().unwrap();
        let err = import_snapshot_tx(&tx, &artifact).unwrap_err();
        assert!(matches!(
            err,
            hopnet_common::snapshot::SnapshotError::TargetNotEmpty { ref table }
                if table == "sequences"
        ));
    }

    // Should: skip a section missing from the import registry with an
    // UnknownSection report, import the rest, and recompute a top hash
    // that DIFFERS from the source's.
    // Impact: a skipped section surfaces as certificate mismatch — the
    // property the S6 boot gate turns into fatal-or-informed.
    #[test]
    fn import_with_missing_registry_section_skips_and_diverges() {
        let source_pool = fresh_pool();
        seed(&source_pool.get().unwrap());
        let mut source = source_pool.get().unwrap();
        let tx = source.transaction().unwrap();
        let (artifact, manifest_src) =
            hopnet_common::snapshot::serialize_snapshot(&tx, &sections()).unwrap();
        drop(tx);

        let all = sections();
        let without_takeout = &all[..4];
        let target_pool = fresh_pool();
        let mut target = target_pool.get().unwrap();
        let tx = target.transaction().unwrap();
        let report =
            hopnet_common::snapshot::import_snapshot(&tx, without_takeout, &artifact).unwrap();
        assert_eq!(report.imported.len(), 4);
        assert_eq!(report.skipped.len(), 1);
        assert_eq!(report.skipped[0].name, "takeout");
        assert_eq!(
            report.skipped[0].reason,
            hopnet_common::snapshot::SkipReason::UnknownSection
        );

        let manifest_partial =
            hopnet_common::snapshot::compute_manifest(&tx, without_takeout).unwrap();
        assert_ne!(manifest_partial.top_hash, manifest_src.top_hash);
    }
}
