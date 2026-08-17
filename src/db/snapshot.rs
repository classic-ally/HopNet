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

/// The identity module (RFC-020): who exists — users, nodes, device
/// auth, and the id sequences behind them. Host-crate code, its own
/// section: every other module FK-references these tables, which puts
/// identity at the bottom of the module graph and first in manifest
/// order.
pub const IDENTITY_SECTION: SectionSpec = SectionSpec {
    name: "identity",
    // Born at 0 with the RFC-020 section split; the pre-split lineage
    // ("host" v1-v3) is recorded by the cutover's one-time host@3
    // import mapping, not by this number.
    // v1: schema_ordinals joined (RFC-020 S3, step 0001) — node-local,
    //     so the wire shape is unchanged; the bump is the ordinal
    //     tracking chain position (ordinal IS format_version).
    format_version: 1,
    tables: &[
        TableSpec::exported("sequences"),
        TableSpec::exported("users"),
        TableSpec::exported("nodes"),
        TableSpec::exported("device_tokens"),
    ],
};

/// The telemetry module (RFC-020): reliability observations about nodes.
/// Host-crate code, its own section; leaf — nothing references it.
pub const TELEMETRY_SECTION: SectionSpec = SectionSpec {
    name: "telemetry",
    // Born at 0 with the RFC-020 section split (see identity).
    format_version: 0,
    tables: &[
        TableSpec::exported("metrics"),
        TableSpec::exported("fragment_request_metrics"),
    ],
};

/// Identity's node-local tables — outside the snapshot universe
/// entirely: this node's own key/settings singleton, and the ordinal
/// stamp describing THIS FILE's chain position (RFC-020 S3; never
/// consensus state, never carried across an epoch boundary).
pub const IDENTITY_NODE_LOCAL_TABLES: &[&str] = &["this_node", "schema_ordinals"];

/// Telemetry's node-local table — in-flight request tracking, per-node
/// by construction.
pub const TELEMETRY_NODE_LOCAL_TABLES: &[&str] = &["pending_fragment_requests"];

/// All sections, in the install_schema walk order (= FK direction; the
/// section order is therefore also a valid import insert order). The
/// named units sit below the projection seam — same split the schema
/// chain makes in `initialize` — then every registered projection
/// contributes via `Projection::snapshot_section` (RFC-016 amendment,
/// RFC-019 S2). Adding a projection touches zero host lines; adding a
/// named unit = one SNAPSHOT_SECTION const + one line here.
pub fn sections() -> Vec<&'static SectionSpec> {
    let mut sections: Vec<&'static SectionSpec> = vec![
        &IDENTITY_SECTION,
        &TELEMETRY_SECTION,
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
    tables.extend_from_slice(IDENTITY_NODE_LOCAL_TABLES);
    tables.extend_from_slice(TELEMETRY_NODE_LOCAL_TABLES);
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

/// Canonical artifact identity (RFC-019 S5): blake3 over the artifact
/// bytes — the EXPORTED subset only, so it is stable across the seal
/// transition (the boundary machinery mutates only DivergenceOnly
/// state: regenesis_state, decided_blocks). Identically computable by
/// the proposer, every validator at vote time, the artifact writer
/// after the seal, and a joiner verifying its download (S7). The
/// manifest's top hash is deliberately NOT used here: it covers
/// divergence-only tables and changes with every decided height.
pub fn compute_artifact_hash_tx(
    tx: &rusqlite::Transaction,
) -> Result<hopnet_common::Blake3Hash, DatabaseError> {
    let (artifact, _manifest) = snapshot::serialize_snapshot(tx, &sections()).map_err(|e| {
        tracing::error!("artifact serialization failed: {e}");
        DatabaseError::ProcessingError
    })?;
    Ok(hopnet_common::Blake3Hash::new(blake3::hash(&artifact)))
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
        crate::db::chains::install(&pool.get().unwrap()).unwrap();
        pool
    }

    /// One or two deterministic rows into EVERY exported table of every
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
             INSERT INTO imports VALUES ('im1', 1, 1, 0);
             -- Photos: every one of the section's 14 tables, in FK order.
             -- Seeded because the roundtrip gate re-IMPORTS this artifact,
             -- and an empty section executes zero INSERTs — so the
             -- `sections()` contract that section order is a valid insert
             -- order would never be exercised for the unit with the deepest
             -- FK chains. A mis-ordered table there aborts `build_next` on a
             -- FOREIGN KEY violation and parks every node at a real
             -- boundary, with every test still green.
             INSERT INTO shared_libraries VALUES ('lib1', X'11', X'12');
             INSERT INTO shared_library_members VALUES ('lib1', 1);
             INSERT INTO shared_library_keys VALUES ('lib1', 1, X'13', X'14');
             INSERT INTO shared_library_invites VALUES ('lib1', 1, 1, 'op1', X'15', X'16');
             INSERT INTO photos VALUES ('ph1', 'lib1', 1, X'17', X'18', NULL, NULL, 'fp1');
             INSERT INTO photo_metadata_access VALUES ('ph1', 1, X'19', X'1A');
             INSERT INTO photo_resources VALUES ('ph1', 0, 'blob1');
             INSERT INTO photo_operations
                 VALUES ('pop1', 'lib1', 'ph1', 0, 0, NULL, 'blob1', X'1B', 1);
             INSERT INTO photo_albums VALUES ('al1', 'lib1', X'1C', X'1D', 1);
             INSERT INTO photo_album_entries VALUES ('al1', 'ph1', 0);
             INSERT INTO photo_favorites VALUES ('ph1', 1);
             INSERT INTO photo_changes VALUES ('ph1', 6);
             INSERT INTO photo_view_changes VALUES (1, 'lib1', 6);
             INSERT INTO photo_ingress_responsibility VALUES (1, 'lib1', 'dt1', 'op2');",
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

    // Impact: index and trigger ownership is implicit — an object
    // belongs to its table's module (RFC-020) — so secondary DDL on an
    // unclaimed table would silently escape both the divergence
    // universe and the module chains.
    // Should: attach every index, trigger, and view to a table the
    // registry claims.
    #[test]
    fn secondary_ddl_attaches_to_claimed_tables() {
        let pool = fresh_pool();
        let conn = pool.get().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT name, tbl_name FROM sqlite_master \
                 WHERE type IN ('index', 'trigger', 'view') \
                 AND name NOT LIKE 'sqlite_%'",
            )
            .unwrap();
        let secondary: Vec<(String, String)> = stmt
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert!(!secondary.is_empty());

        let mut claimed: HashSet<&'static str> = HashSet::new();
        for section in sections() {
            claimed.extend(section.tables.iter().map(|t| t.name));
        }
        claimed.extend(node_local_tables());

        for (name, tbl_name) in secondary {
            assert!(
                claimed.contains(tbl_name.as_str()),
                "{name} attaches to unclaimed table {tbl_name}"
            );
        }
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

    const EMPTY_TOP_HASH: &str = "975747a4e1c8931bfda722c9c3a2ae85831afc7da75454f6238e0e290f417c1b";
    const EMPTY_SECTION_HASHES: &[(&str, &str)] = &[
        (
            "identity",
            "e2ba132a65c3c66a588ff5b917ba0248a853fe317a85cd5ec40fad771ce35356",
        ),
        (
            "telemetry",
            "b4994290b2f8b7b813a66f541982c1f5ebc69c6c7a2d648442337709ee699079",
        ),
        (
            "consensus",
            "62d94827859d6760294d88f1e23528e2f7118808336bcb26a6ca458e6a50541b",
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
            "photos",
            "9b442521498105dd6fbdc4952e9935599c9f9d9e7b61bbaa41e298c5223935a9",
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
        "74522360a2649b453f28d94091043f0e769336f7b044f88935dbae33d824d67e";
    const SEEDED_ARTIFACT_HASH: &str =
        "5e60c9ccb76178709b0b76dac430c963051931af336aa05e7632c7b5741d0fa8";
    // 5159 pre-split + 25: the "host" section header (16 bytes) became
    // identity (20) + telemetry (21) headers. Row bytes unchanged — the
    // delta being exactly the header arithmetic is the cheap proof the
    // RFC-020 S1 split moved no table content.
    const SEEDED_ARTIFACT_LEN: usize = 5184;

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
            assert_eq!(report.imported.len(), 7);
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
        // Drop the LAST section (takeout) rather than a fixed index, so
        // registering a new projection cannot silently turn this into a
        // two-section-missing test.
        let without_takeout = &all[..all.len() - 1];
        assert_eq!(all.last().unwrap().name, "takeout");
        let target_pool = fresh_pool();
        let mut target = target_pool.get().unwrap();
        let tx = target.transaction().unwrap();
        let report =
            hopnet_common::snapshot::import_snapshot(&tx, without_takeout, &artifact).unwrap();
        assert_eq!(report.imported.len(), 6);
        assert_eq!(report.skipped.len(), 1);
        assert_eq!(report.skipped[0].name, "takeout");
        assert_eq!(
            report.skipped[0].reason,
            hopnet_common::snapshot::SkipReason::UnknownSection
        );

        // Compare over the FULL registry, not `without_takeout`. Computing
        // the target manifest from the reduced list asserted nothing:
        // `compute_top_hash` folds `sections.len()` into its preimage, so a
        // 5-section and a 6-section manifest differ unconditionally — and
        // the skipped section's rows were never even walked, so a skip path
        // that silently INSERTED every row would have passed too.
        let manifest_target = hopnet_common::snapshot::compute_manifest(&tx, &all).unwrap();

        // The imported sections are byte-identical to the source's...
        for (target, src) in manifest_target
            .sections
            .iter()
            .zip(manifest_src.sections.iter())
        {
            assert_eq!(target.name, src.name, "section order must be stable");
            if target.name != "takeout" {
                assert_eq!(
                    target.section_hash, src.section_hash,
                    "section {} must import byte-identically",
                    target.name
                );
            }
        }

        // ...and the skipped one is NOT, because its rows never landed.
        let find = |m: &hopnet_common::snapshot::SnapshotManifest| {
            m.sections
                .iter()
                .find(|s| s.name == "takeout")
                .expect("takeout section present in both manifests")
                .section_hash
        };
        assert_ne!(
            find(&manifest_target),
            find(&manifest_src),
            "a skipped section must not match the source it was skipped from"
        );
        // The assertion a silently-inserting skip path cannot survive.
        assert_eq!(
            tx.query_row("SELECT COUNT(*) FROM takeouts", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            0,
            "the skipped section's rows must not have been inserted"
        );

        // And the property the test is named for: the divergence surfaces
        // as a top-hash mismatch, which is what the S6 boot gate turns into
        // a certificate refusal.
        assert_ne!(manifest_target.top_hash, manifest_src.top_hash);
    }
}
