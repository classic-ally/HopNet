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
pub use engine::{
    compute_manifest, import_snapshot, serialize_snapshot, ImportReport, ImportedSection,
    SkipReason, SkippedSection, SnapshotError,
};

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
        /// Artifact does not begin with MAGIC — not a snapshot at all.
        BadMagic,
        /// Container version this build cannot read (parse-far-enough:
        /// magic, then version, then nothing else is trusted).
        UnsupportedArtifactVersion {
            found: u32,
        },
        /// Structural decode failure: truncation, a length prefix past
        /// the buffer, an unknown value tag, trailing bytes.
        Malformed {
            offset: usize,
            what: &'static str,
        },
        /// Artifact section/table/column shape disagrees with the fresh
        /// schema's canonical shape.
        SchemaMismatch {
            section: String,
            table: String,
            detail: String,
        },
        /// Import target is not fresh: a covered table already has rows.
        TargetNotEmpty {
            table: String,
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
                Self::BadMagic => write!(f, "snapshot: artifact does not begin with HOPSNAP magic"),
                Self::UnsupportedArtifactVersion { found } => {
                    write!(
                        f,
                        "snapshot: artifact version {found} unreadable (this build reads {ARTIFACT_VERSION})"
                    )
                }
                Self::Malformed { offset, what } => {
                    write!(f, "snapshot: malformed artifact at byte {offset}: {what}")
                }
                Self::SchemaMismatch {
                    section,
                    table,
                    detail,
                } => {
                    write!(
                        f,
                        "snapshot: artifact disagrees with schema at {section}/{table}: {detail}"
                    )
                }
                Self::TargetNotEmpty { table } => {
                    write!(f, "snapshot: import target is not fresh — {table} has rows")
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

    /// What happened during an import: which sections landed (with row
    /// counts) and which were skipped. A skipped section means the
    /// recomputed top hash will NOT match the certified one — the caller
    /// decides fatal-or-informed; import only surfaces it.
    #[derive(Debug, Clone, PartialEq)]
    pub struct ImportReport {
        pub imported: Vec<ImportedSection>,
        pub skipped: Vec<SkippedSection>,
    }

    #[derive(Debug, Clone, PartialEq)]
    pub struct ImportedSection {
        pub name: String,
        /// (table, rows inserted) in artifact order.
        pub tables: Vec<(String, u64)>,
    }

    #[derive(Debug, Clone, PartialEq)]
    pub struct SkippedSection {
        pub name: String,
        pub reason: SkipReason,
    }

    /// Why a section was skipped rather than imported. Skip-with-report
    /// is what adjacent-version readability rides on: an importer without
    /// a matching registry entry parses past the section structurally
    /// (values are self-delimiting) instead of failing the whole import.
    #[derive(Debug, Clone, PartialEq)]
    pub enum SkipReason {
        UnknownSection,
        FormatVersionMismatch { artifact: u32, registry: u32 },
    }

    /// Import a snapshot artifact into a FRESH database — schema already
    /// installed, zero rows in every covered table. Plain INSERTs in
    /// artifact order: the section order is the FK direction (verified
    /// backward-only across the whole schema), so no constraint deferral
    /// is needed, and a hostile reordering fails loudly on FK inside the
    /// transaction. The CALLER commits (matches serialize_snapshot's
    /// shape).
    pub fn import_snapshot(
        tx: &Transaction,
        sections: &[&SectionSpec],
        artifact: &[u8],
    ) -> Result<ImportReport, SnapshotError> {
        let mut cur = Cursor {
            buf: artifact,
            pos: 0,
        };
        if artifact.len() < MAGIC.len() || &artifact[..MAGIC.len()] != MAGIC {
            return Err(SnapshotError::BadMagic);
        }
        cur.pos = MAGIC.len();
        let version = cur.read_u32()?;
        if version != ARTIFACT_VERSION {
            return Err(SnapshotError::UnsupportedArtifactVersion { found: version });
        }

        let section_count = cur.read_u32()?;
        let mut report = ImportReport {
            imported: Vec::new(),
            skipped: Vec::new(),
        };
        for _ in 0..section_count {
            let name = cur.read_name()?;
            let format_version = cur.read_u32()?;
            let table_count = cur.read_u32()?;

            let spec = sections.iter().copied().find(|s| s.name == name);
            let skip = match spec {
                None => Some(SkipReason::UnknownSection),
                Some(s) if s.format_version != format_version => {
                    Some(SkipReason::FormatVersionMismatch {
                        artifact: format_version,
                        registry: s.format_version,
                    })
                }
                Some(_) => None,
            };

            match skip {
                Some(reason) => {
                    // Parse-to-skip: the format has no section length
                    // prefix, so skipping IS a full structural decode.
                    for _ in 0..table_count {
                        let unit = read_table_header(&mut cur)?;
                        walk_table_rows(&mut cur, &unit, |_| Ok(()))?;
                    }
                    report.skipped.push(SkippedSection {
                        name: name.to_string(),
                        reason,
                    });
                }
                None => {
                    let spec = spec.expect("skip is None only when spec matched");
                    report
                        .imported
                        .push(import_section(tx, spec, &mut cur, table_count)?);
                }
            }
        }

        if !cur.done() {
            return Err(cur.malformed("trailing bytes after the last section"));
        }
        Ok(report)
    }

    fn import_section(
        tx: &Transaction,
        spec: &SectionSpec,
        cur: &mut Cursor<'_>,
        table_count: u32,
    ) -> Result<ImportedSection, SnapshotError> {
        let mismatch = |table: &str, detail: String| SnapshotError::SchemaMismatch {
            section: spec.name.to_string(),
            table: table.to_string(),
            detail,
        };

        let mut imported = ImportedSection {
            name: spec.name.to_string(),
            tables: Vec::new(),
        };
        for _ in 0..table_count {
            let unit = read_table_header(cur)?;
            let table_spec = spec
                .tables
                .iter()
                .find(|t| t.role == TableRole::Exported && t.name == unit.name)
                .ok_or_else(|| {
                    mismatch(unit.name, "not an exported table of this section".into())
                })?;
            // A duplicate table unit in a hostile artifact needs no
            // special case: the second pass fails the emptiness check
            // (or a PK violation) below.
            if imported.tables.iter().any(|(name, _)| name == unit.name) {
                return Err(mismatch(unit.name, "duplicate table unit".into()));
            }

            let (canonical, _pk) = canonical_columns(tx, table_spec)?;
            if !canonical
                .iter()
                .map(String::as_str)
                .eq(unit.columns.iter().copied())
            {
                return Err(mismatch(
                    unit.name,
                    format!(
                        "artifact columns {:?} != schema canonical columns {:?}",
                        unit.columns, canonical
                    ),
                ));
            }

            let existing = tx.query_row(
                &format!("SELECT COUNT(*) FROM \"{}\"", unit.name),
                [],
                |row| row.get::<_, i64>(0),
            )?;
            if existing != 0 {
                return Err(SnapshotError::TargetNotEmpty {
                    table: unit.name.to_string(),
                });
            }

            // Column-named INSERT: the excluded node-local columns take
            // their DDL defaults — right for a fresh node.
            let column_list = unit
                .columns
                .iter()
                .map(|c| format!("\"{c}\""))
                .collect::<Vec<_>>()
                .join(", ");
            let placeholders = vec!["?"; unit.columns.len()].join(", ");
            let mut stmt = tx.prepare(&format!(
                "INSERT INTO \"{}\" ({column_list}) VALUES ({placeholders})",
                unit.name
            ))?;
            let mut rows = 0u64;
            walk_table_rows(cur, &unit, |values| {
                stmt.execute(rusqlite::params_from_iter(values.iter()))?;
                rows += 1;
                Ok(())
            })?;
            imported.tables.push((unit.name.to_string(), rows));
        }

        // A short artifact must not import partial state and leave the
        // hash gate to notice later.
        for table in spec.tables {
            if table.role == TableRole::Exported
                && !imported.tables.iter().any(|(name, _)| name == table.name)
            {
                return Err(mismatch(table.name, "missing from artifact".into()));
            }
        }
        Ok(imported)
    }

    /// Bounds-checked reader over the artifact. Every length is checked
    /// against the remaining buffer BEFORE any slice or allocation, so a
    /// bogus 2^60 length prefix fails cleanly instead of allocating.
    struct Cursor<'a> {
        buf: &'a [u8],
        pos: usize,
    }

    impl<'a> Cursor<'a> {
        fn malformed(&self, what: &'static str) -> SnapshotError {
            SnapshotError::Malformed {
                offset: self.pos,
                what,
            }
        }

        fn read_bytes(&mut self, n: usize) -> Result<&'a [u8], SnapshotError> {
            let end = self
                .pos
                .checked_add(n)
                .filter(|&end| end <= self.buf.len())
                .ok_or_else(|| self.malformed("runs past the end of the artifact"))?;
            let out = &self.buf[self.pos..end];
            self.pos = end;
            Ok(out)
        }

        fn read_u32(&mut self) -> Result<u32, SnapshotError> {
            Ok(u32::from_le_bytes(self.read_bytes(4)?.try_into().unwrap()))
        }

        fn read_u64(&mut self) -> Result<u64, SnapshotError> {
            Ok(u64::from_le_bytes(self.read_bytes(8)?.try_into().unwrap()))
        }

        fn read_len_u64(&mut self) -> Result<usize, SnapshotError> {
            let len = self.read_u64()?;
            usize::try_from(len).map_err(|_| self.malformed("length prefix overflows usize"))
        }

        /// Section/table/column names are &'static str on the write side;
        /// non-UTF-8 here means a corrupt artifact.
        fn read_name(&mut self) -> Result<&'a str, SnapshotError> {
            let len = self.read_u32()? as usize;
            let start = self.pos;
            let bytes = self.read_bytes(len)?;
            std::str::from_utf8(bytes).map_err(|_| SnapshotError::Malformed {
                offset: start,
                what: "name is not UTF-8",
            })
        }

        fn done(&self) -> bool {
            self.pos == self.buf.len()
        }
    }

    struct TableUnit<'a> {
        name: &'a str,
        columns: Vec<&'a str>,
        row_count: u64,
    }

    fn read_table_header<'a>(cur: &mut Cursor<'a>) -> Result<TableUnit<'a>, SnapshotError> {
        let name = cur.read_name()?;
        let col_count = cur.read_u32()?;
        let mut columns = Vec::with_capacity(col_count.min(1024) as usize);
        for _ in 0..col_count {
            columns.push(cur.read_name()?);
        }
        let row_count = cur.read_u64()?;
        // A zero-column unit would make `walk_table_rows` advance the
        // cursor by NOTHING per row, so `read_bytes` never reaches the end
        // and never errors: a hostile `row_count` spins up to 2^64 times
        // consuming no input. `import_section` compares columns against the
        // canonical set before walking, but the parse-to-skip branch
        // deliberately does not — skipping IS a full structural decode of
        // bytes whose shape is not trusted. Rejected here, where every
        // other structural impossibility is. No real table has no columns.
        if columns.is_empty() && row_count != 0 {
            return Err(cur.malformed("table unit declares rows but no columns"));
        }
        Ok(TableUnit {
            name,
            columns,
            row_count,
        })
    }

    /// Decode a table unit's rows through `row_sink` — the one shared
    /// walk for both the import path and the parse-to-skip path, so the
    /// two cannot drift (mirror of the serializer's single-walk sink).
    fn walk_table_rows<'a>(
        cur: &mut Cursor<'a>,
        unit: &TableUnit<'a>,
        mut row_sink: impl FnMut(&[RawValue<'a>]) -> Result<(), SnapshotError>,
    ) -> Result<(), SnapshotError> {
        let mut values: Vec<RawValue<'a>> = Vec::with_capacity(unit.columns.len());
        for _ in 0..unit.row_count {
            values.clear();
            for column in &unit.columns {
                values.push(decode_value(cur, unit.name, column)?);
            }
            row_sink(&values)?;
        }
        Ok(())
    }

    /// A decoded SQLite value borrowing from the artifact. TEXT stays raw
    /// bytes end to end — SQLite permits non-UTF-8 text, and routing it
    /// through String would corrupt or reject bytes that are certificate
    /// preimage.
    enum RawValue<'a> {
        Null,
        Integer(i64),
        Real(f64),
        Text(&'a [u8]),
        Blob(&'a [u8]),
    }

    impl rusqlite::ToSql for RawValue<'_> {
        fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
            use rusqlite::types::ToSqlOutput;
            Ok(ToSqlOutput::Borrowed(match self {
                RawValue::Null => ValueRef::Null,
                RawValue::Integer(i) => ValueRef::Integer(*i),
                RawValue::Real(f) => ValueRef::Real(*f),
                RawValue::Text(bytes) => ValueRef::Text(bytes),
                RawValue::Blob(bytes) => ValueRef::Blob(bytes),
            }))
        }
    }

    /// Tag-for-tag mirror of `encode_value_into`. NaN and -0.0 bits are
    /// rejected: the serializer never emits either, so their presence
    /// means corruption — rejecting keeps re-serialize ≡ input an
    /// invariant rather than a hope.
    fn decode_value<'a>(
        cur: &mut Cursor<'a>,
        table: &str,
        column: &str,
    ) -> Result<RawValue<'a>, SnapshotError> {
        match cur.read_bytes(1)?[0] {
            0x00 => Ok(RawValue::Null),
            0x01 => Ok(RawValue::Integer(i64::from_le_bytes(
                cur.read_bytes(8)?.try_into().unwrap(),
            ))),
            0x02 => {
                let f = f64::from_le_bytes(cur.read_bytes(8)?.try_into().unwrap());
                if f.is_nan() || (f == 0.0 && f.is_sign_negative()) {
                    return Err(SnapshotError::NonCanonicalFloat {
                        table: table.to_string(),
                        column: column.to_string(),
                    });
                }
                Ok(RawValue::Real(f))
            }
            0x03 => {
                let len = cur.read_len_u64()?;
                Ok(RawValue::Text(cur.read_bytes(len)?))
            }
            0x04 => {
                let len = cur.read_len_u64()?;
                Ok(RawValue::Blob(cur.read_bytes(len)?))
            }
            _ => Err(SnapshotError::Malformed {
                offset: cur.pos - 1,
                what: "unknown value tag",
            }),
        }
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

    /// Exported-role table manifests across all sections — the subset a
    /// post-import parity check may compare (DivergenceOnly tables
    /// legitimately differ on a fresh database).
    fn exported_tables(manifest: &SnapshotManifest) -> Vec<&TableManifest> {
        manifest
            .sections
            .iter()
            .flat_map(|s| s.tables.iter().filter(|t| t.role == TableRole::Exported))
            .collect()
    }

    /// Patch helper for corruption tests: replace the single occurrence
    /// of `find` in the artifact (asserts exactly one, so a fixture
    /// change that makes the pattern ambiguous fails loudly).
    fn patch(artifact: &[u8], find: &[u8], replace: &[u8]) -> Vec<u8> {
        let positions: Vec<usize> = artifact
            .windows(find.len())
            .enumerate()
            .filter(|(_, w)| *w == find)
            .map(|(i, _)| i)
            .collect();
        assert_eq!(positions.len(), 1, "pattern must occur exactly once");
        let mut out = artifact.to_vec();
        out[positions[0]..positions[0] + replace.len()].copy_from_slice(replace);
        out
    }

    /// Import with the real caller contract: commit on Ok, drop (roll
    /// back) on Err — an errored import must leave the fresh target
    /// untouched.
    fn import_into_fresh(
        artifact: &[u8],
        sections: &[&SectionSpec],
    ) -> (Connection, Result<ImportReport, SnapshotError>) {
        let mut conn = fixture_conn();
        let result = {
            let tx = conn.transaction().unwrap();
            let result = import_snapshot(&tx, sections, artifact);
            if result.is_ok() {
                tx.commit().unwrap();
            }
            result
        };
        (conn, result)
    }

    // Should: import the seeded fixture's artifact into a fresh fixture
    // schema, re-serialize byte-identically, and report the imported
    // tables with row counts and no skips.
    // Should not: compare the DivergenceOnly history table's manifest —
    // a fresh database legitimately lacks its rows.
    // Impact: byte-identity across export→import→export is the epoch
    // boundary contract regenesis_commit certifies.
    #[test]
    fn import_roundtrip_synthetic_schema() {
        let mut source = fixture_conn();
        seed_items(&source);
        let (artifact, manifest_src) = snapshot(&mut source, &[&MAIN_SECTION, &EMPTY_SECTION]);

        let (mut imported, result) = import_into_fresh(&artifact, &[&MAIN_SECTION, &EMPTY_SECTION]);
        let report = result.unwrap();
        assert!(report.skipped.is_empty());
        assert_eq!(
            report.imported,
            vec![
                ImportedSection {
                    name: "main".to_string(),
                    tables: vec![("items".to_string(), 3)],
                },
                ImportedSection {
                    name: "empty".to_string(),
                    tables: vec![("empty_a".to_string(), 0), ("empty_b".to_string(), 0)],
                },
            ]
        );

        let (artifact_again, manifest_re) =
            snapshot(&mut imported, &[&MAIN_SECTION, &EMPTY_SECTION]);
        assert_eq!(artifact_again, artifact);
        assert_eq!(manifest_re.top_hash, manifest_src.top_hash);
        for (re, src) in manifest_re.sections.iter().zip(&manifest_src.sections) {
            assert_eq!(re.section_hash, src.section_hash);
        }
        assert_eq!(
            exported_tables(&manifest_re),
            exported_tables(&manifest_src)
        );
    }

    // Should: decode the pinned golden artifact directly into a fresh
    // fixture and match a freshly seeded source's exported manifests.
    // Impact: pins the parser against known bytes, not just against
    // whatever serialize currently produces.
    #[test]
    fn import_golden_artifact_parses() {
        let artifact = hex::decode(GOLDEN_ARTIFACT_HEX).unwrap();
        let (mut imported, result) = import_into_fresh(&artifact, &[&MAIN_SECTION, &EMPTY_SECTION]);
        result.unwrap();

        let mut source = fixture_conn();
        seed_items(&source);
        let (_, manifest_src) = snapshot(&mut source, &[&MAIN_SECTION, &EMPTY_SECTION]);
        let (_, manifest_imp) = snapshot(&mut imported, &[&MAIN_SECTION, &EMPTY_SECTION]);
        assert_eq!(
            exported_tables(&manifest_imp),
            exported_tables(&manifest_src)
        );
    }

    // Should not: read past the first bytes of a non-snapshot; BadMagic
    // is a hard error.
    #[test]
    fn import_rejects_bad_magic() {
        let (_, result) = import_into_fresh(b"NOTSNAP\0rest", &[&MAIN_SECTION]);
        assert!(matches!(result.unwrap_err(), SnapshotError::BadMagic));
    }

    // Should: hard-error on an unreadable container version — magic,
    // then version, then nothing else is trusted.
    #[test]
    fn import_rejects_wrong_artifact_version() {
        let mut source = fixture_conn();
        seed_items(&source);
        let (artifact, _) = snapshot(&mut source, &[&MAIN_SECTION]);
        let mut patched = artifact.clone();
        patched[8..12].copy_from_slice(&999u32.to_le_bytes());
        let (_, result) = import_into_fresh(&patched, &[&MAIN_SECTION]);
        assert!(matches!(
            result.unwrap_err(),
            SnapshotError::UnsupportedArtifactVersion { found: 999 }
        ));
    }

    // Should: return Err — never panic, never partially succeed — for
    // every strict prefix of a valid artifact.
    #[test]
    fn import_rejects_every_truncation() {
        let artifact = hex::decode(GOLDEN_ARTIFACT_HEX).unwrap();
        for n in 0..artifact.len() {
            let (conn, result) =
                import_into_fresh(&artifact[..n], &[&MAIN_SECTION, &EMPTY_SECTION]);
            assert!(result.is_err(), "prefix of {n} bytes must not import");
            let rows: i64 = conn
                .query_row("SELECT COUNT(*) FROM items", [], |row| row.get(0))
                .unwrap();
            assert_eq!(rows, 0, "prefix of {n} bytes must not leave rows behind");
        }
    }

    // Should not: accept bytes after the last declared section.
    #[test]
    fn import_rejects_trailing_bytes() {
        let mut artifact = hex::decode(GOLDEN_ARTIFACT_HEX).unwrap();
        artifact.push(0x00);
        let (_, result) = import_into_fresh(&artifact, &[&MAIN_SECTION, &EMPTY_SECTION]);
        assert!(matches!(
            result.unwrap_err(),
            SnapshotError::Malformed {
                what: "trailing bytes after the last section",
                ..
            }
        ));
    }

    // Should: bounds-check a patched huge length prefix rather than
    // allocate or slice past the buffer.
    #[test]
    fn import_rejects_bogus_length_prefix() {
        let artifact = hex::decode(GOLDEN_ARTIFACT_HEX).unwrap();
        // The TEXT value 'héllo': tag 0x03 + u64 len 6.
        let find: &[u8] = &[0x03, 6, 0, 0, 0, 0, 0, 0, 0];
        let replace: &[u8] = &[0x03, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff];
        let patched = patch(&artifact, find, replace);
        let (_, result) = import_into_fresh(&patched, &[&MAIN_SECTION, &EMPTY_SECTION]);
        assert!(matches!(
            result.unwrap_err(),
            SnapshotError::Malformed { .. }
        ));
    }

    // Impact: a zero-column unit is the one shape that makes the row walk
    // consume NO input per row, so `read_bytes` never reaches the end and
    // never errors — a patched `row_count` would spin up to 2^64 times on a
    // fixed buffer. `import_section` compares columns before walking, but
    // the PARSE-TO-SKIP branch deliberately does not: skipping an unknown
    // section IS a full structural decode of bytes whose shape is not
    // trusted, which is exactly where a hostile artifact would aim.
    // Not reachable in production (both importers verify the blake3 hash
    // first, so triggering it needs a preimage) — this is the routine's own
    // bounds check, not a defence that something else already provides.
    // Should: reject a unit that declares rows but no columns, rather than
    //   loop forever.
    #[test]
    fn import_rejects_a_zero_column_unit() {
        let artifact = hex::decode(GOLDEN_ARTIFACT_HEX).unwrap();
        // A table header is a length-prefixed name followed by u32
        // col_count. Anchor on `items`' name so the patch site is unique,
        // then zero its column count while leaving row_count intact — the
        // shape that makes the row walk consume nothing per iteration.
        let mut find = 5u32.to_le_bytes().to_vec();
        find.extend_from_slice(b"items");
        let cols = find.len();
        find.extend_from_slice(&5u32.to_le_bytes());
        let mut replace = find.clone();
        replace[cols..].copy_from_slice(&0u32.to_le_bytes());

        let patched = patch(&artifact, &find, &replace);
        let (_, result) = import_into_fresh(&patched, &[&MAIN_SECTION, &EMPTY_SECTION]);
        assert!(
            matches!(result.unwrap_err(), SnapshotError::Malformed { .. }),
            "a zero-column unit must be refused at the header"
        );
    }

    // Should: fail Malformed with the offset on an unknown value tag.
    #[test]
    fn import_rejects_unknown_value_tag() {
        let artifact = hex::decode(GOLDEN_ARTIFACT_HEX).unwrap();
        // First row's height value: tag 0x01, i64 42.
        let find: &[u8] = &[0x01, 42, 0, 0, 0, 0, 0, 0, 0];
        let replace: &[u8] = &[0x05, 42, 0, 0, 0, 0, 0, 0, 0];
        let patched = patch(&artifact, find, replace);
        let (_, result) = import_into_fresh(&patched, &[&MAIN_SECTION, &EMPTY_SECTION]);
        assert!(matches!(
            result.unwrap_err(),
            SnapshotError::Malformed {
                what: "unknown value tag",
                ..
            }
        ));
    }

    // Should not: accept NaN or -0.0 REAL bits — the serializer never
    // emits either, so their presence means corruption; rejecting keeps
    // re-serialize ≡ input an invariant rather than a hope.
    #[test]
    fn import_rejects_non_canonical_real() {
        let artifact = hex::decode(GOLDEN_ARTIFACT_HEX).unwrap();
        // The REAL value 1.5: tag 0x02 + IEEE-754 LE bits.
        let mut find = vec![0x02];
        find.extend_from_slice(&1.5f64.to_le_bytes());
        for bad in [f64::NAN.to_le_bytes(), (-0.0f64).to_le_bytes()] {
            let mut replace = vec![0x02];
            replace.extend_from_slice(&bad);
            let patched = patch(&artifact, &find, &replace);
            let (_, result) = import_into_fresh(&patched, &[&MAIN_SECTION, &EMPTY_SECTION]);
            assert!(matches!(
                result.unwrap_err(),
                SnapshotError::NonCanonicalFloat { .. }
            ));
        }
    }

    // Should: parse-to-skip a section absent from the registry and one
    // whose registry format_version differs, import the rest, and report
    // both reasons.
    // Impact: adjacent-version readability rides on skip-with-report; a
    // skipped section surfaces as a top-hash mismatch for the caller.
    #[test]
    fn import_skips_unknown_and_mismatched_sections_with_report() {
        let mut source = fixture_conn();
        seed_items(&source);
        let (artifact, _) = snapshot(&mut source, &[&MAIN_SECTION, &EMPTY_SECTION]);

        // Registry knows only "main": "empty" is unknown.
        let (_, result) = import_into_fresh(&artifact, &[&MAIN_SECTION]);
        let report = result.unwrap();
        assert_eq!(report.imported.len(), 1);
        assert_eq!(
            report.skipped,
            vec![SkippedSection {
                name: "empty".to_string(),
                reason: SkipReason::UnknownSection,
            }]
        );

        // Registry knows "empty" at a different format version.
        const EMPTY_V2: SectionSpec = SectionSpec {
            name: "empty",
            format_version: 2,
            tables: &[EMPTY_A, EMPTY_B],
        };
        let (_, result) = import_into_fresh(&artifact, &[&MAIN_SECTION, &EMPTY_V2]);
        let report = result.unwrap();
        assert_eq!(
            report.skipped,
            vec![SkippedSection {
                name: "empty".to_string(),
                reason: SkipReason::FormatVersionMismatch {
                    artifact: 1,
                    registry: 2,
                },
            }]
        );
    }

    // Should not: insert over existing rows; the error names the table.
    // Impact: standing guard against a future initialize() that
    // pre-seeds covered tables — import targets are FRESH by contract.
    #[test]
    fn import_rejects_nonempty_target() {
        let mut source = fixture_conn();
        seed_items(&source);
        let (artifact, _) = snapshot(&mut source, &[&MAIN_SECTION]);

        let mut target = fixture_conn();
        target
            .execute("INSERT INTO items (id) VALUES (99)", [])
            .unwrap();
        let tx = target.transaction().unwrap();
        let err = import_snapshot(&tx, &[&MAIN_SECTION], &artifact).unwrap_err();
        assert!(matches!(err, SnapshotError::TargetNotEmpty { ref table } if table == "items"));
    }

    // Should: hard-error when the artifact's column list differs from
    // the fresh schema's canonical columns, and when the artifact's
    // table set falls short of the spec's exported set.
    #[test]
    fn import_rejects_schema_mismatch() {
        let mut source = fixture_conn();
        seed_items(&source);
        let (artifact, _) = snapshot(&mut source, &[&MAIN_SECTION]);

        // Target whose items table lacks a column.
        let mut narrow = Connection::open_in_memory().unwrap();
        narrow
            .execute_batch(
                "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, local_flag INTEGER);
                 CREATE TABLE history (height INTEGER PRIMARY KEY, block BLOB);",
            )
            .unwrap();
        let tx = narrow.transaction().unwrap();
        let err = import_snapshot(&tx, &[&MAIN_SECTION], &artifact).unwrap_err();
        assert!(matches!(err, SnapshotError::SchemaMismatch { .. }));
        drop(tx);

        // Registry spec expecting an exported table the artifact lacks.
        const MAIN_PLUS: SectionSpec = SectionSpec {
            name: "main",
            format_version: 1,
            tables: &[ITEMS, HISTORY, EMPTY_A],
        };
        let (_, result) = import_into_fresh(&artifact, &[&MAIN_PLUS]);
        let err = result.unwrap_err();
        assert!(
            matches!(err, SnapshotError::SchemaMismatch { ref table, .. } if table == "empty_a")
        );
    }

    // Should: roundtrip non-UTF-8 TEXT byte-identically and preserve
    // every column's storage class across import.
    // Should not: route TEXT through String.
    // Impact: TEXT bytes are certificate preimage; a UTF-8 laundering
    // step would corrupt or reject them.
    #[test]
    fn import_preserves_non_utf8_text_and_storage_classes() {
        let mut source = fixture_conn();
        source
            .execute_batch(
                "INSERT INTO items (id, name, payload, score, height, local_flag)
                 VALUES (1, CAST(X'80FF' AS TEXT), X'01', 2.5, 7, 0);
                 INSERT INTO history VALUES (1, X'AA');",
            )
            .unwrap();
        let (artifact, _) = snapshot(&mut source, &[&MAIN_SECTION]);

        let (imported, result) = import_into_fresh(&artifact, &[&MAIN_SECTION]);
        result.unwrap();
        let (artifact_again, _) = {
            let mut conn = imported;
            snapshot(&mut conn, &[&MAIN_SECTION])
        };
        assert_eq!(artifact_again, artifact);

        let mut check = fixture_conn();
        {
            let tx = check.transaction().unwrap();
            import_snapshot(&tx, &[&MAIN_SECTION], &artifact).unwrap();
            tx.commit().unwrap();
        }
        let classes: (String, String, String) = check
            .query_row(
                "SELECT typeof(name), typeof(payload), typeof(score) FROM items WHERE id = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(classes, ("text".into(), "blob".into(), "real".into()));
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
