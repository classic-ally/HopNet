// Canonical snapshot serializer (RFC-019 S1).
//
// A snapshot is a CANONICAL LOGICAL export of replicated state — never a
// database file copy (page layout, freelists, and vacuum state differ
// across nodes; the bytes must not). The same walk produces the
// divergence manifest served by /api/debug/state and the epoch snapshot
// artifact certified by regenesis_commit, so "what validators compare"
// and "what an epoch boundary carries" cannot drift apart.
//
// Determinism rules (each guarded by a test below):
// - column order: names sorted lexicographically by byte value, computed
//   AFTER removing excluded columns — never schema declaration order, so
//   a DDL refactor cannot silently change certified hashes
// - row order: ORDER BY the primary-key columns, names likewise sorted;
//   all covered tables use BINARY collation, so the order is total and
//   collation-stable
// - value encoding: tag byte + fixed-width or length-prefixed bytes (see
//   encode_value_into) — floats are IEEE-754 bits, never SQLite's
//   version-dependent float→text rendering
// - no wall-clock reads anywhere in the export path
//
// Format evolution: ARTIFACT_VERSION covers the container and value
// encoding; each SectionSpec carries its own format_version for the
// section's logical shape. Bumping either regenerates the golden hashes
// in the same commit — an unintentional preimage change fails the golden
// tests while the pinned-version test still passes, so the review diff
// for a legitimate bump is exactly "version bump + regenerated goldens".

use serde::{Deserialize, Serialize};

use crate::hash::Blake3Hash;

/// Container/value-encoding version of the snapshot artifact.
pub const ARTIFACT_VERSION: u32 = 1;

/// First bytes of a snapshot artifact file.
pub const MAGIC: &[u8; 8] = b"HOPSNAP\0";

// Domain tags are hash-preimage-only — never written to the artifact.
const DOMAIN_TABLE: &[u8] = b"hopnet/snapshot/v1/table\0";
const DOMAIN_SECTION: &[u8] = b"hopnet/snapshot/v1/section\0";
const DOMAIN_TOP: &[u8] = b"hopnet/snapshot/v1/top\0";

/// How a covered table participates in the snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TableRole {
    /// Replicated state: divergence-checked on the live mesh AND
    /// exported across an epoch boundary.
    Exported,
    /// Divergence-checked only. The live mesh must agree on it, but it
    /// never crosses a boundary — epoch history dies with the retained
    /// epoch-N database (RFC-019 Archival & Retention).
    DivergenceOnly,
}

/// A covered table within a module's section.
#[derive(Debug, Clone, Copy)]
pub struct TableSpec {
    pub name: &'static str,
    pub role: TableRole,
    /// Node-local columns of otherwise-replicated tables, removed from
    /// the canonical bytes (and therefore from hashes and the export).
    pub excluded_columns: &'static [&'static str],
}

impl TableSpec {
    pub const fn exported(name: &'static str) -> Self {
        Self {
            name,
            role: TableRole::Exported,
            excluded_columns: &[],
        }
    }
}

/// A module-owned section of the snapshot. Each schema-owning unit
/// declares one next to its DDL; the host assembles them in the
/// install_schema walk order (= FK direction), which is the manifest
/// order.
#[derive(Debug, Clone, Copy)]
pub struct SectionSpec {
    /// Manifest/section name ("host", "consensus", "storage", …).
    pub name: &'static str,
    /// Logical format version of this section's canonical shape; carried
    /// in the manifest so an importer can translate or skip a section it
    /// doesn't understand. Starts at 1, bumped on any covered-set or
    /// semantic change to the section.
    pub format_version: u32,
    /// Covered tables, in this section's canonical (artifact) order.
    pub tables: &'static [TableSpec],
}

/// Per-table entry of the snapshot manifest.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableManifest {
    pub name: String,
    pub role: TableRole,
    pub row_count: u64,
    pub hash: Blake3Hash,
    pub excluded_columns: Vec<String>,
}

/// Per-section entry of the snapshot manifest.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SectionManifest {
    pub name: String,
    pub format_version: u32,
    /// Hash over the section header and its EXPORTED tables' hashes.
    /// DivergenceOnly tables appear in `tables` for live-mesh comparison
    /// but contribute nothing here.
    pub section_hash: Blake3Hash,
    pub tables: Vec<TableManifest>,
}

/// The snapshot manifest: per-table and per-section hashes plus the top
/// hash that regenesis_commit certifies.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapshotManifest {
    pub artifact_version: u32,
    pub top_hash: Blake3Hash,
    pub sections: Vec<SectionManifest>,
}

impl SnapshotManifest {
    /// Recompute the top hash from the manifest's own fields. Equals
    /// `top_hash` for an honestly built manifest; a verifier recomputes
    /// this (and the section hashes from re-hashed tables) rather than
    /// trusting the sender.
    pub fn compute_top_hash(&self) -> Blake3Hash {
        let mut hasher = blake3::Hasher::new();
        hasher.update(DOMAIN_TOP);
        hasher.update(&ARTIFACT_VERSION.to_le_bytes());
        hasher.update(&(self.sections.len() as u32).to_le_bytes());
        for section in &self.sections {
            hasher.update(&(section.name.len() as u32).to_le_bytes());
            hasher.update(section.name.as_bytes());
            hasher.update(&section.format_version.to_le_bytes());
            hasher.update(section.section_hash.as_bytes());
        }
        hasher.finalize().into()
    }
}

/// What GET /api/debug/state returns: the manifest plus the height it
/// was computed at, read in the same transaction. Height is deliberately
/// NOT part of any hash — the regenesis_commit transaction binds
/// height↔hash at the protocol layer; the artifact is pure state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeStateReport {
    pub consensus_height: u64,
    pub manifest: SnapshotManifest,
}

#[cfg(feature = "database")]
pub use engine::{compute_manifest, serialize_snapshot, SnapshotError};

#[cfg(feature = "database")]
mod engine {
    use super::*;
    use rusqlite::types::ValueRef;
    use rusqlite::Transaction;

    #[derive(Debug)]
    pub enum SnapshotError {
        Db(rusqlite::Error),
        /// Every covered table must declare a primary key — it is the
        /// canonical row order.
        MissingPrimaryKey {
            table: String,
        },
        /// NaN has no canonical bit pattern across producers; nothing
        /// replicated legitimately stores one. Fail loud, don't launder.
        NonCanonicalFloat {
            table: String,
            column: String,
        },
        /// An excluded_columns entry names a column the table doesn't
        /// have — a stale registry, caught rather than ignored.
        UnknownExcludedColumn {
            table: String,
            column: String,
        },
    }

    impl From<rusqlite::Error> for SnapshotError {
        fn from(e: rusqlite::Error) -> Self {
            Self::Db(e)
        }
    }

    impl std::fmt::Display for SnapshotError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::Db(e) => write!(f, "snapshot: database error: {e}"),
                Self::MissingPrimaryKey { table } => {
                    write!(f, "snapshot: covered table {table} has no primary key")
                }
                Self::NonCanonicalFloat { table, column } => {
                    write!(
                        f,
                        "snapshot: NaN in {table}.{column} has no canonical encoding"
                    )
                }
                Self::UnknownExcludedColumn { table, column } => {
                    write!(
                        f,
                        "snapshot: excluded column {table}.{column} does not exist"
                    )
                }
            }
        }
    }

    impl std::error::Error for SnapshotError {}

    /// Hash-only walk over ALL covered tables. No artifact buffer is
    /// materialized — this is the divergence-route path, cheap enough to
    /// serve on demand.
    pub fn compute_manifest(
        tx: &Transaction,
        sections: &[&SectionSpec],
    ) -> Result<SnapshotManifest, SnapshotError> {
        walk(tx, sections, None)
    }

    /// Full export: canonical artifact bytes (Exported tables only) plus
    /// the manifest. The manifest is identical to compute_manifest's
    /// over the same state — both run the single walk below.
    pub fn serialize_snapshot(
        tx: &Transaction,
        sections: &[&SectionSpec],
    ) -> Result<(Vec<u8>, SnapshotManifest), SnapshotError> {
        let mut artifact = Vec::new();
        let manifest = walk(tx, sections, Some(&mut artifact))?;
        Ok((artifact, manifest))
    }

    fn walk(
        tx: &Transaction,
        sections: &[&SectionSpec],
        mut artifact: Option<&mut Vec<u8>>,
    ) -> Result<SnapshotManifest, SnapshotError> {
        if let Some(buf) = artifact.as_deref_mut() {
            buf.extend_from_slice(MAGIC);
            buf.extend_from_slice(&ARTIFACT_VERSION.to_le_bytes());
            buf.extend_from_slice(&(sections.len() as u32).to_le_bytes());
        }

        let mut section_manifests = Vec::with_capacity(sections.len());
        for spec in sections {
            section_manifests.push(walk_section(tx, spec, artifact.as_deref_mut())?);
        }

        let mut manifest = SnapshotManifest {
            artifact_version: ARTIFACT_VERSION,
            top_hash: Blake3Hash::from_bytes([0; 32]),
            sections: section_manifests,
        };
        manifest.top_hash = manifest.compute_top_hash();
        Ok(manifest)
    }

    fn walk_section(
        tx: &Transaction,
        spec: &SectionSpec,
        mut artifact: Option<&mut Vec<u8>>,
    ) -> Result<SectionManifest, SnapshotError> {
        let exported_count = spec
            .tables
            .iter()
            .filter(|t| t.role == TableRole::Exported)
            .count() as u32;

        if let Some(buf) = artifact.as_deref_mut() {
            buf.extend_from_slice(&(spec.name.len() as u32).to_le_bytes());
            buf.extend_from_slice(spec.name.as_bytes());
            buf.extend_from_slice(&spec.format_version.to_le_bytes());
            buf.extend_from_slice(&exported_count.to_le_bytes());
        }

        let mut section_hasher = blake3::Hasher::new();
        section_hasher.update(DOMAIN_SECTION);
        section_hasher.update(&(spec.name.len() as u32).to_le_bytes());
        section_hasher.update(spec.name.as_bytes());
        section_hasher.update(&spec.format_version.to_le_bytes());
        section_hasher.update(&exported_count.to_le_bytes());

        let mut tables = Vec::with_capacity(spec.tables.len());
        for table in spec.tables {
            let exported = table.role == TableRole::Exported;
            let table_manifest = walk_table(
                tx,
                table,
                if exported {
                    artifact.as_deref_mut()
                } else {
                    None
                },
            )?;
            if exported {
                section_hasher.update(table_manifest.hash.as_bytes());
            }
            tables.push(table_manifest);
        }

        Ok(SectionManifest {
            name: spec.name.to_string(),
            format_version: spec.format_version,
            section_hash: section_hasher.finalize().into(),
            tables,
        })
    }

    /// Emit one table unit — header, then rows in PK order — through the
    /// hasher and (for Exported tables) the artifact buffer. One encode
    /// path serves both, so route hashing and artifact hashing are
    /// identical by construction.
    fn walk_table(
        tx: &Transaction,
        table: &TableSpec,
        mut artifact: Option<&mut Vec<u8>>,
    ) -> Result<TableManifest, SnapshotError> {
        let mut hasher = blake3::Hasher::new();
        hasher.update(DOMAIN_TABLE);
        let write =
            |hasher: &mut blake3::Hasher, artifact: &mut Option<&mut Vec<u8>>, bytes: &[u8]| {
                hasher.update(bytes);
                if let Some(buf) = artifact.as_deref_mut() {
                    buf.extend_from_slice(bytes);
                }
            };

        let (columns, pk_columns) = canonical_columns(tx, table)?;

        write(
            &mut hasher,
            &mut artifact,
            &(table.name.len() as u32).to_le_bytes(),
        );
        write(&mut hasher, &mut artifact, table.name.as_bytes());
        write(
            &mut hasher,
            &mut artifact,
            &(columns.len() as u32).to_le_bytes(),
        );
        for column in &columns {
            write(
                &mut hasher,
                &mut artifact,
                &(column.len() as u32).to_le_bytes(),
            );
            write(&mut hasher, &mut artifact, column.as_bytes());
        }

        let row_count = tx.query_row(
            &format!("SELECT COUNT(*) FROM \"{}\"", table.name),
            [],
            |row| row.get::<_, i64>(0),
        )? as u64;
        write(&mut hasher, &mut artifact, &row_count.to_le_bytes());

        let column_list = columns
            .iter()
            .map(|c| format!("\"{c}\""))
            .collect::<Vec<_>>()
            .join(", ");
        let order_list = pk_columns
            .iter()
            .map(|c| format!("\"{c}\""))
            .collect::<Vec<_>>()
            .join(", ");
        let mut stmt = tx.prepare(&format!(
            "SELECT {column_list} FROM \"{}\" ORDER BY {order_list}",
            table.name
        ))?;

        let mut value = Vec::new();
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            for (i, column) in columns.iter().enumerate() {
                value.clear();
                encode_value_into(&mut value, row.get_ref(i)?, table.name, column)?;
                write(&mut hasher, &mut artifact, &value);
            }
        }

        Ok(TableManifest {
            name: table.name.to_string(),
            role: table.role,
            row_count,
            hash: hasher.finalize().into(),
            excluded_columns: table
                .excluded_columns
                .iter()
                .map(|c| c.to_string())
                .collect(),
        })
    }

    /// Canonical (sorted, exclusions removed) column list and sorted PK
    /// column list, both derived from PRAGMA table_info so they are
    /// recomputable from any schema layout with the same logical shape.
    fn canonical_columns(
        tx: &Transaction,
        table: &TableSpec,
    ) -> Result<(Vec<String>, Vec<String>), SnapshotError> {
        let mut stmt = tx.prepare(&format!("PRAGMA table_info(\"{}\")", table.name))?;
        let info: Vec<(String, bool)> = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(1)?, row.get::<_, i64>(5)? > 0))
            })?
            .collect::<Result<_, _>>()?;

        for excluded in table.excluded_columns {
            if !info.iter().any(|(name, _)| name == excluded) {
                return Err(SnapshotError::UnknownExcludedColumn {
                    table: table.name.to_string(),
                    column: excluded.to_string(),
                });
            }
        }

        let mut columns: Vec<String> = info
            .iter()
            .filter(|(name, _)| !table.excluded_columns.contains(&name.as_str()))
            .map(|(name, _)| name.clone())
            .collect();
        columns.sort_unstable();

        let mut pk_columns: Vec<String> = info
            .iter()
            .filter(|(_, pk)| *pk)
            .map(|(name, _)| name.clone())
            .collect();
        if pk_columns.is_empty() {
            return Err(SnapshotError::MissingPrimaryKey {
                table: table.name.to_string(),
            });
        }
        pk_columns.sort_unstable();

        Ok((columns, pk_columns))
    }

    /// One SQLite value → tag byte + self-delimiting bytes. Injective
    /// over the SQLite value model; all integers little-endian.
    ///
    /// INTEGER is the raw i64 bit pattern: heights ≥ 2^63 are stored as
    /// negative i64 by the height_to_db bit-cast, and the bit pattern IS
    /// the u64, so they encode losslessly with no special case. REAL is
    /// IEEE-754 bits with -0.0 canonicalized to +0.0 and NaN rejected —
    /// never SQLite's float→text rendering, which changed algorithms
    /// across SQLite releases and would make hashes version-dependent.
    pub(super) fn encode_value_into(
        out: &mut Vec<u8>,
        value: ValueRef<'_>,
        table: &str,
        column: &str,
    ) -> Result<(), SnapshotError> {
        match value {
            ValueRef::Null => out.push(0x00),
            ValueRef::Integer(i) => {
                out.push(0x01);
                out.extend_from_slice(&i.to_le_bytes());
            }
            ValueRef::Real(f) => {
                if f.is_nan() {
                    return Err(SnapshotError::NonCanonicalFloat {
                        table: table.to_string(),
                        column: column.to_string(),
                    });
                }
                let canonical = if f == 0.0 { 0.0f64 } else { f };
                out.push(0x02);
                out.extend_from_slice(&canonical.to_le_bytes());
            }
            ValueRef::Text(t) => {
                out.push(0x03);
                out.extend_from_slice(&(t.len() as u64).to_le_bytes());
                out.extend_from_slice(t);
            }
            ValueRef::Blob(b) => {
                out.push(0x04);
                out.extend_from_slice(&(b.len() as u64).to_le_bytes());
                out.extend_from_slice(b);
            }
        }
        Ok(())
    }
}

#[cfg(all(test, feature = "database"))]
mod tests {
    use super::*;
    use rusqlite::Connection;

    const ITEMS: TableSpec = TableSpec {
        name: "items",
        role: TableRole::Exported,
        excluded_columns: &["local_flag"],
    };
    const HISTORY: TableSpec = TableSpec {
        name: "history",
        role: TableRole::DivergenceOnly,
        excluded_columns: &[],
    };
    const EMPTY_A: TableSpec = TableSpec::exported("empty_a");
    const EMPTY_B: TableSpec = TableSpec::exported("empty_b");

    const MAIN_SECTION: SectionSpec = SectionSpec {
        name: "main",
        format_version: 1,
        tables: &[ITEMS, HISTORY],
    };
    const EMPTY_SECTION: SectionSpec = SectionSpec {
        name: "empty",
        format_version: 1,
        tables: &[EMPTY_A, EMPTY_B],
    };

    fn fixture_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE items (
                 id INTEGER PRIMARY KEY,
                 name TEXT,
                 payload BLOB,
                 score REAL,
                 height INTEGER,
                 local_flag INTEGER
             );
             CREATE TABLE history (height INTEGER PRIMARY KEY, block BLOB);
             CREATE TABLE empty_a (id INTEGER PRIMARY KEY);
             CREATE TABLE empty_b (id INTEGER PRIMARY KEY);",
        )
        .unwrap();
        conn
    }

    fn seed_items(conn: &Connection) {
        // Exercises every value type: NULL, negative INTEGER, a height
        // above 2^63 (stored negative via the bit-cast), REAL including
        // -0.0, unicode and empty TEXT, BLOB.
        conn.execute_batch(
            "INSERT INTO items VALUES (1, 'héllo', X'00FF10', 1.5, 42, 1);
             INSERT INTO items VALUES (2, '', NULL, -0.0, -7, 0);
             INSERT INTO items VALUES (3, NULL, X'', NULL, -9223372036854775808, 1);
             INSERT INTO history VALUES (1, X'AA');
             INSERT INTO history VALUES (2, X'BB');",
        )
        .unwrap();
    }

    fn snapshot(conn: &mut Connection, sections: &[&SectionSpec]) -> (Vec<u8>, SnapshotManifest) {
        let tx = conn.transaction().unwrap();
        serialize_snapshot(&tx, sections).unwrap()
    }

    // Golden values for the seeded two-section fixture. Regenerating
    // after an INTENTIONAL format change: bump ARTIFACT_VERSION (or the
    // section's format_version), run `cargo test -p hopnet-common
    // --features database snapshot -- --nocapture`, and copy the printed
    // actuals here in the same commit.
    const GOLDEN_ARTIFACT_HEX: &str = "\
        484f50534e4150000100000002000000040000006d61696e01000000010000000500\
        00006974656d730500000006000000686569676874020000006964040000006e616d\
        65070000007061796c6f61640500000073636f72650300000000000000012a000000\
        0000000001010000000000000003060000000000000068c3a96c6c6f040300000000\
        00000000ff1002000000000000f83f01f9ffffffffffffff01020000000000000003\
        00000000000000000002000000000000000001000000000000008001030000000000\
        0000000400000000000000000005000000656d707479010000000200000007000000\
        656d7074795f6101000000020000006964000000000000000007000000656d707479\
        5f62010000000200000069640000000000000000";

    // Should: pin ARTIFACT_VERSION so an accidental preimage change
    // fails the golden hashes while this still passes — an intentional
    // bump changes both in one reviewable diff.
    #[test]
    fn artifact_version_is_pinned() {
        assert_eq!(ARTIFACT_VERSION, 1);
    }

    // Should: encode each SQLite value type to the pinned tag+bytes,
    // including the negative-i64 bit pattern for heights >= 2^63.
    // Impact: the value encoding is the certificate preimage; silent
    // drift here breaks cross-node hash equality undetectably.
    #[test]
    fn artifact_golden_bytes() {
        let mut conn = fixture_conn();
        seed_items(&conn);
        let (artifact, manifest) = snapshot(&mut conn, &[&MAIN_SECTION, &EMPTY_SECTION]);
        assert_eq!(manifest.artifact_version, ARTIFACT_VERSION);
        assert_eq!(hex::encode(&artifact), GOLDEN_ARTIFACT_HEX);
    }

    // Should: produce identical manifests from compute_manifest and
    // serialize_snapshot over the same state, and byte-identical
    // artifacts across repeated serialization.
    #[test]
    fn serialize_and_manifest_agree() {
        let mut conn = fixture_conn();
        seed_items(&conn);
        let (artifact_a, manifest_a) = snapshot(&mut conn, &[&MAIN_SECTION, &EMPTY_SECTION]);
        let (artifact_b, manifest_b) = snapshot(&mut conn, &[&MAIN_SECTION, &EMPTY_SECTION]);
        let tx = conn.transaction().unwrap();
        let manifest_c = compute_manifest(&tx, &[&MAIN_SECTION, &EMPTY_SECTION]).unwrap();
        assert_eq!(artifact_a, artifact_b);
        assert_eq!(manifest_a, manifest_b);
        assert_eq!(manifest_a, manifest_c);
    }

    // Should: reject NaN with the offending table.column; encode -0.0 as
    // +0.0 bits. Should not: ever render floats as text.
    // Impact: floats are the one cross-version determinism hazard in the
    // covered set (SQLite changed float→text rendering across releases).
    #[test]
    fn real_rejects_nan_canonicalizes_negative_zero() {
        let mut conn = fixture_conn();
        conn.execute_batch(
            "INSERT INTO items VALUES (1, 'a', NULL, -0.0, 0, 0);
             INSERT INTO history VALUES (1, X'AA');",
        )
        .unwrap();
        let (artifact_neg, _) = snapshot(&mut conn, &[&MAIN_SECTION]);
        conn.execute("UPDATE items SET score = 0.0 WHERE id = 1", [])
            .unwrap();
        let (artifact_pos, _) = snapshot(&mut conn, &[&MAIN_SECTION]);
        assert_eq!(artifact_neg, artifact_pos);

        // NaN directly at the encoder seam: SQLite itself stores NaN as
        // NULL, so this guard is unreachable via storage — it exists to
        // make the invariant explicit rather than inherited.
        let mut out = Vec::new();
        let err = super::engine::encode_value_into(
            &mut out,
            rusqlite::types::ValueRef::Real(f64::NAN),
            "items",
            "score",
        )
        .unwrap_err();
        assert!(matches!(err, SnapshotError::NonCanonicalFloat { .. }));
    }

    // Should: produce byte-identical artifacts from two databases with
    // identical logical content but different column declaration order.
    // Impact: schema-file refactors must never change certified hashes.
    #[test]
    fn column_declaration_order_irrelevant() {
        let mut conn_a = fixture_conn();
        seed_items(&conn_a);

        let mut conn_b = Connection::open_in_memory().unwrap();
        conn_b
            .execute_batch(
                "CREATE TABLE items (
                     local_flag INTEGER,
                     height INTEGER,
                     score REAL,
                     payload BLOB,
                     name TEXT,
                     id INTEGER PRIMARY KEY
                 );
                 CREATE TABLE history (height INTEGER PRIMARY KEY, block BLOB);
                 CREATE TABLE empty_a (id INTEGER PRIMARY KEY);
                 CREATE TABLE empty_b (id INTEGER PRIMARY KEY);",
            )
            .unwrap();
        conn_b
            .execute_batch(
                "INSERT INTO items (id, name, payload, score, height, local_flag)
                 VALUES (1, 'héllo', X'00FF10', 1.5, 42, 1);
                 INSERT INTO items (id, name, payload, score, height, local_flag)
                 VALUES (2, '', NULL, -0.0, -7, 0);
                 INSERT INTO items (id, name, payload, score, height, local_flag)
                 VALUES (3, NULL, X'', NULL, -9223372036854775808, 1);
                 INSERT INTO history VALUES (1, X'AA');
                 INSERT INTO history VALUES (2, X'BB');",
            )
            .unwrap();

        assert_eq!(
            snapshot(&mut conn_a, &[&MAIN_SECTION, &EMPTY_SECTION]),
            snapshot(&mut conn_b, &[&MAIN_SECTION, &EMPTY_SECTION])
        );
    }

    // Should: hash identically regardless of row insert order (rows are
    // read back in primary-key order).
    #[test]
    fn row_insert_order_irrelevant() {
        let mut conn_a = fixture_conn();
        conn_a
            .execute_batch(
                "INSERT INTO items VALUES (2, 'b', NULL, NULL, 2, 0);
                 INSERT INTO items VALUES (1, 'a', NULL, NULL, 1, 0);
                 INSERT INTO history VALUES (1, X'AA');",
            )
            .unwrap();
        let mut conn_b = fixture_conn();
        conn_b
            .execute_batch(
                "INSERT INTO items VALUES (1, 'a', NULL, NULL, 1, 0);
                 INSERT INTO items VALUES (2, 'b', NULL, NULL, 2, 0);
                 INSERT INTO history VALUES (1, X'AA');",
            )
            .unwrap();
        assert_eq!(
            snapshot(&mut conn_a, &[&MAIN_SECTION]),
            snapshot(&mut conn_b, &[&MAIN_SECTION])
        );
    }

    // Should not: let two distinct empty tables share a table hash (the
    // table name and column list are part of the hash preimage).
    #[test]
    fn empty_tables_domain_separated() {
        let mut conn = fixture_conn();
        let (_, manifest) = snapshot(&mut conn, &[&EMPTY_SECTION]);
        let hashes: Vec<_> = manifest.sections[0].tables.iter().map(|t| t.hash).collect();
        assert_eq!(hashes.len(), 2);
        assert_ne!(hashes[0], hashes[1]);
    }

    // Should: keep excluded-column values out of the canonical bytes.
    // Should: error on an excluded column that doesn't exist.
    // Impact: exclusions carry node-local columns of replicated tables;
    // a typo'd exclusion silently re-including one would make honest
    // nodes diverge.
    #[test]
    fn excluded_columns_do_not_affect_bytes() {
        let mut conn_a = fixture_conn();
        conn_a
            .execute_batch(
                "INSERT INTO items VALUES (1, 'a', NULL, NULL, 1, 0);
                 INSERT INTO history VALUES (1, X'AA');",
            )
            .unwrap();
        let mut conn_b = fixture_conn();
        conn_b
            .execute_batch(
                "INSERT INTO items VALUES (1, 'a', NULL, NULL, 1, 999);
                 INSERT INTO history VALUES (1, X'AA');",
            )
            .unwrap();
        assert_eq!(
            snapshot(&mut conn_a, &[&MAIN_SECTION]),
            snapshot(&mut conn_b, &[&MAIN_SECTION])
        );

        const BAD: TableSpec = TableSpec {
            name: "items",
            role: TableRole::Exported,
            excluded_columns: &["no_such_column"],
        };
        const BAD_SECTION: SectionSpec = SectionSpec {
            name: "main",
            format_version: 1,
            tables: &[BAD],
        };
        let tx = conn_a.transaction().unwrap();
        let err = serialize_snapshot(&tx, &[&BAD_SECTION]).unwrap_err();
        assert!(matches!(err, SnapshotError::UnknownExcludedColumn { .. }));
    }

    // Should: list a DivergenceOnly table in the manifest with its hash
    // and role; mutating it changes its table hash but neither the
    // section hash, the top hash, nor the artifact bytes.
    // Impact: history is checked live but dies at the epoch boundary —
    // the role flag is the mechanism (RFC-019 covered-set carve-out).
    #[test]
    fn divergence_only_absent_from_artifact_and_top_hash() {
        let mut conn = fixture_conn();
        seed_items(&conn);
        let (artifact_a, manifest_a) = snapshot(&mut conn, &[&MAIN_SECTION]);
        conn.execute("INSERT INTO history VALUES (3, X'CC')", [])
            .unwrap();
        let (artifact_b, manifest_b) = snapshot(&mut conn, &[&MAIN_SECTION]);

        let history = |m: &SnapshotManifest| {
            m.sections[0]
                .tables
                .iter()
                .find(|t| t.name == "history")
                .unwrap()
                .clone()
        };
        assert_eq!(history(&manifest_a).role, TableRole::DivergenceOnly);
        assert_ne!(history(&manifest_a).hash, history(&manifest_b).hash);
        assert_eq!(history(&manifest_b).row_count, 3);

        assert_eq!(artifact_a, artifact_b);
        assert_eq!(manifest_a.top_hash, manifest_b.top_hash);
        assert_eq!(
            manifest_a.sections[0].section_hash,
            manifest_b.sections[0].section_hash
        );
    }

    // Should: recompute the top hash from manifest fields alone, equal
    // to the engine's.
    // Impact: a joiner verifies a fetched artifact against the decided
    // certificate without trusting the sender's manifest.
    #[test]
    fn top_hash_recomputable_from_manifest_alone() {
        let mut conn = fixture_conn();
        seed_items(&conn);
        let (_, manifest) = snapshot(&mut conn, &[&MAIN_SECTION, &EMPTY_SECTION]);
        assert_eq!(manifest.compute_top_hash(), manifest.top_hash);

        let mut tampered = manifest.clone();
        tampered.sections[0].format_version = 2;
        assert_ne!(tampered.compute_top_hash(), manifest.top_hash);
    }

    // Should: roundtrip the manifest through JSON with hashes as hex
    // strings (the orchestrator's wire contract).
    #[test]
    fn manifest_wire_shape() {
        let mut conn = fixture_conn();
        seed_items(&conn);
        let (_, manifest) = snapshot(&mut conn, &[&MAIN_SECTION]);
        let report = NodeStateReport {
            consensus_height: 7,
            manifest,
        };
        let json = serde_json::to_string(&report).unwrap();
        assert!(json.contains("\"top_hash\":\""));
        assert!(json.contains("\"divergence_only\""));
        let back: NodeStateReport = serde_json::from_str(&json).unwrap();
        assert_eq!(back, report);
    }
}
