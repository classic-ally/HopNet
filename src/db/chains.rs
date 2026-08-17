//! Host assembly of the RFC-020 schema chains.
//!
//! Mirrors `db::snapshot::sections()` exactly: same modules, same
//! order (manifest order = FK direction, so chain order is a valid
//! creation order). Host-owned modules declare their chains here —
//! including consensus's, whose baseline covers two host-DDL tables
//! (`committed_tx_nonces`, `regenesis_state`): a module is a
//! schema-ownership unit, not a crate, and the embeddable
//! hopnet-consensus crate never learns the app's transaction envelope.

use hopnet_common::{Chain, Step};

/// The identity module's chain — see `snapshot::IDENTITY_SECTION`.
pub static IDENTITY_CHAIN: Chain = Chain {
    module: "identity",
    steps: &[
        Step::sql(
            0,
            "init",
            include_str!("../../migrations/identity/0000_init.sql"),
        ),
        Step::sql(
            1,
            "schema_ordinals",
            include_str!("../../migrations/identity/0001_schema_ordinals.sql"),
        ),
    ],
};

/// The telemetry module's chain — see `snapshot::TELEMETRY_SECTION`.
pub static TELEMETRY_CHAIN: Chain = Chain {
    module: "telemetry",
    steps: &[Step::sql(
        0,
        "init",
        include_str!("../../migrations/telemetry/0000_init.sql"),
    )],
};

/// The consensus module's chain. Baseline ordinal 2 adopts the
/// section's pre-chain `format_version` history (§Cutover).
pub static CONSENSUS_CHAIN: Chain = Chain {
    module: "consensus",
    steps: &[Step::sql(
        2,
        "init",
        include_str!("../../migrations/consensus/0002_init.sql"),
    )],
};

/// Every module's chain, in `sections()` order.
pub fn chains() -> Vec<&'static Chain> {
    let mut chains: Vec<&'static Chain> = vec![
        &IDENTITY_CHAIN,
        &TELEMETRY_CHAIN,
        &CONSENSUS_CHAIN,
        &hopnet_storage::store::CHAIN,
    ];
    chains.extend(crate::projections::manifests().iter().map(|p| p.chain()));
    chains
}

/// Schema lifecycle errors (RFC-020 S3): everything the boot dispatch
/// can refuse on. Loud by design — every variant names what disagreed.
#[derive(Debug)]
pub enum SchemaError {
    Chain(hopnet_common::chain::ChainError),
    Db(rusqlite::Error),
    /// The stamp table's contents disagree with the compiled chains:
    /// missing module, stray module, or an ordinal that is not a real
    /// chain position.
    InvalidStamp {
        detail: String,
    },
    /// The database's schema does not hash to what its recorded (or
    /// adopted) position requires. Failure Modes: refuse loudly and
    /// park; never guess.
    FingerprintMismatch {
        context: &'static str,
        expected: String,
        actual: String,
    },
}

impl std::fmt::Display for SchemaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Chain(e) => write!(f, "{e}"),
            Self::Db(e) => write!(f, "schema db error: {e}"),
            Self::InvalidStamp { detail } => write!(f, "invalid schema_ordinals stamp: {detail}"),
            Self::FingerprintMismatch {
                context,
                expected,
                actual,
            } => write!(
                f,
                "schema fingerprint mismatch ({context}): expected {expected}, found {actual} — \
                 refusing to guess (RFC-020 Failure Modes)"
            ),
        }
    }
}

impl std::error::Error for SchemaError {}

impl From<hopnet_common::chain::ChainError> for SchemaError {
    fn from(e: hopnet_common::chain::ChainError) -> Self {
        Self::Chain(e)
    }
}
impl From<rusqlite::Error> for SchemaError {
    fn from(e: rusqlite::Error) -> Self {
        Self::Db(e)
    }
}

/// THE installer (RFC-020 S2): replay every chain to head, then stamp
/// the file with its position (S3). The only way a fresh HopNet schema
/// comes to exist — `initialize` is gone, and the per-crate installer
/// batches with it.
pub fn install(conn: &rusqlite::Connection) -> Result<(), SchemaError> {
    hopnet_common::chain::replay_all(conn, &chains())?;
    // Shadowing gate (RFC-CONSENSUS-002 S1, relocated from
    // `initialize`): the consensus module owns validators; a stray
    // CREATE TABLE in another module's step would shadow it and lose
    // departure_kind. Fail loudly at install.
    conn.prepare("SELECT departure_kind FROM validators LIMIT 0")
        .map_err(|source| hopnet_common::chain::ChainError::Step {
            module: "consensus",
            ordinal: CONSENSUS_CHAIN.head(),
            slug: "validators shadowing gate",
            source,
        })?;
    write_stamps(conn)?;
    Ok(())
}

/// Canonical schema rows — the shared basis of every parity gate and
/// the fingerprint. Ordered, internal `sqlite_autoindex_*` excluded.
pub fn schema_rows(
    conn: &rusqlite::Connection,
) -> Result<Vec<(String, String, String, String)>, rusqlite::Error> {
    let mut stmt = conn.prepare(
        "SELECT type, name, tbl_name, sql FROM sqlite_master \
         WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%' \
         ORDER BY type, name",
    )?;
    stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))?
        .collect()
}

/// blake3 over the canonical schema rows — "what shape is this file",
/// independent of row content and page layout.
pub fn schema_fingerprint(conn: &rusqlite::Connection) -> Result<String, rusqlite::Error> {
    let mut hasher = blake3::Hasher::new();
    for (ty, name, tbl, sql) in schema_rows(conn)? {
        for part in [&ty, &name, &tbl, &sql] {
            hasher.update(&(part.len() as u32).to_le_bytes());
            hasher.update(part.as_bytes());
        }
    }
    Ok(hasher.finalize().to_hex().to_string())
}

/// Stamp the file at every chain's head. INSERT OR REPLACE: install
/// stamps from birth, fast-forward re-stamps after applying steps.
pub fn write_stamps(conn: &rusqlite::Connection) -> Result<(), rusqlite::Error> {
    for chain in chains() {
        conn.execute(
            "INSERT OR REPLACE INTO schema_ordinals (module, ordinal) VALUES (?1, ?2)",
            rusqlite::params![chain.module, chain.head()],
        )?;
    }
    Ok(())
}

/// Read this file's recorded positions.
pub fn read_stamps(
    conn: &rusqlite::Connection,
) -> Result<std::collections::BTreeMap<String, u32>, rusqlite::Error> {
    let mut stmt = conn.prepare("SELECT module, ordinal FROM schema_ordinals")?;
    stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect()
}

/// What kind of database file boot is looking at (RFC-020 S3).
/// Errors PROPAGATE — a corrupt database must never read as Fresh and
/// get reinstalled over.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaState {
    /// No user tables at all: install from nothing.
    Fresh,
    /// Has the ordinal stamp: validate + fast-forward.
    Stamped,
    /// Has user tables but predates the stamp (built ≤ S2, or arriving
    /// via the S6 cutover): adopt-at-baseline if the fingerprint
    /// agrees, then fast-forward.
    LegacyUnstamped,
}

pub fn assess(conn: &rusqlite::Connection) -> Result<SchemaState, rusqlite::Error> {
    let user_tables: i64 = conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
        [],
        |r| r.get(0),
    )?;
    if user_tables == 0 {
        return Ok(SchemaState::Fresh);
    }
    let stamped: i64 = conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'schema_ordinals'",
        [],
        |r| r.get(0),
    )?;
    Ok(if stamped > 0 {
        SchemaState::Stamped
    } else {
        SchemaState::LegacyUnstamped
    })
}

fn reference_fingerprint(at_baseline: bool) -> Result<String, SchemaError> {
    let conn = rusqlite::Connection::open_in_memory()?;
    for chain in chains() {
        let to = if at_baseline {
            chain.baseline()
        } else {
            chain.head()
        };
        hopnet_common::chain::replay(&conn, chain, to)?;
    }
    Ok(schema_fingerprint(&conn)?)
}

/// Adopt a pre-stamp database (§Cutover: "stamped, not migrated").
/// Sound because a stampless database is by construction at every
/// chain's BASELINE — stamps exist from S3 onward, so anything without
/// one was built when only baselines existed (S1/S2 dev files, or the
/// cutover's sealed pre-chain shape, generated byte-identical). The
/// fingerprint check enforces exactly that; anything else is
/// fatal-loud. Fast-forward afterwards applies every post-baseline
/// step and writes the real stamps.
pub fn adopt_legacy(conn: &rusqlite::Connection) -> Result<(), SchemaError> {
    let actual = schema_fingerprint(conn)?;
    let expected = reference_fingerprint(true)?;
    if actual != expected {
        return Err(SchemaError::FingerprintMismatch {
            context: "legacy adopt vs baseline shape",
            expected,
            actual,
        });
    }
    // The stamp table itself is post-baseline DDL; create it via the
    // identity steps above baseline, then record baseline positions
    // for every module — fast_forward owns the rest.
    hopnet_common::chain::advance(
        conn,
        &IDENTITY_CHAIN,
        IDENTITY_CHAIN.baseline(),
        IDENTITY_CHAIN.head(),
    )?;
    for chain in chains() {
        let recorded = if chain.module == "identity" {
            chain.head()
        } else {
            chain.baseline()
        };
        conn.execute(
            "INSERT OR REPLACE INTO schema_ordinals (module, ordinal) VALUES (?1, ?2)",
            rusqlite::params![chain.module, recorded],
        )?;
    }
    Ok(())
}

/// One fast-forward pass inside the CALLER's transaction (RFC-020 S4:
/// the epoch boot transition owns the transaction): validate stamps,
/// apply the gap for every chain, re-stamp, verify the final
/// fingerprint. Never commits.
pub fn fast_forward_tx(tx: &rusqlite::Connection) -> Result<u32, SchemaError> {
    let stamps = read_stamps(tx)?;

    let chains = chains();
    for stamped_module in stamps.keys() {
        if !chains.iter().any(|c| c.module == stamped_module) {
            return Err(SchemaError::InvalidStamp {
                detail: format!("stamp for unknown module {stamped_module}"),
            });
        }
    }
    let mut applied = 0;
    for chain in &chains {
        let from = *stamps
            .get(chain.module)
            .ok_or_else(|| SchemaError::InvalidStamp {
                detail: format!("no stamp for module {}", chain.module),
            })?;
        if !chain.contains(from) {
            return Err(SchemaError::InvalidStamp {
                detail: format!("module {} stamped at unknown ordinal {from}", chain.module),
            });
        }
        applied += hopnet_common::chain::advance(tx, chain, from, chain.head())?;
    }
    write_stamps(tx)?;

    let actual = schema_fingerprint(tx)?;
    let expected = reference_fingerprint(false)?;
    if actual != expected {
        return Err(SchemaError::FingerprintMismatch {
            context: "post fast-forward vs head shape",
            expected,
            actual,
        });
    }
    Ok(applied)
}

/// Standalone fast-forward: one transaction, committed with the
/// instrumented commit (project rule). The boot dispatch's entry.
pub fn fast_forward(conn: &mut rusqlite::Connection) -> Result<u32, SchemaError> {
    let tx = conn.transaction()?;
    let applied = fast_forward_tx(&tx)?;
    crate::db::shared::commit_timed(tx)?;
    Ok(applied)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema_rows(conn: &rusqlite::Connection) -> Vec<(String, String, String, String)> {
        super::schema_rows(conn).unwrap()
    }

    // Impact: migrations/schema.sql is the one READABLE rendering of the
    // current schema (the parity reference since `initialize` died at
    // S2); if it drifts from what replay actually builds, reviewers
    // read a schema no node runs.
    // Should: keep the checked-in generated snapshot byte-identical to
    // the replay-built schema.
    #[test]
    fn schema_snapshot_matches_replay() {
        let snapshot = rusqlite::Connection::open_in_memory().unwrap();
        snapshot
            .execute_batch(include_str!("../../migrations/schema.sql"))
            .unwrap();

        let replayed = rusqlite::Connection::open_in_memory().unwrap();
        install(&replayed).unwrap();

        let replayed_rows = schema_rows(&replayed);
        assert!(!replayed_rows.is_empty());
        assert_eq!(
            schema_rows(&snapshot),
            replayed_rows,
            "migrations/schema.sql is stale — regenerate it: \
             cargo test --lib regenerate_schema_snapshot -- --ignored"
        );
    }

    // Not a test: the regeneration ritual for migrations/schema.sql.
    // Run after landing a chain step:
    //   cargo test --lib regenerate_schema_snapshot -- --ignored
    #[test]
    #[ignore = "writes migrations/schema.sql; run explicitly to regenerate"]
    fn regenerate_schema_snapshot() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        install(&conn).unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT sql FROM sqlite_master \
                 WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%' ORDER BY rowid",
            )
            .unwrap();
        let statements: Vec<String> = stmt
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        let mut out = String::from(
            "-- GENERATED — do not edit. The chain (migrations/<module>/) is\n\
             -- the schema authority; this file is its readable rendering,\n\
             -- kept honest by the schema_snapshot_matches_replay gate.\n\
             -- Regenerate: cargo test --lib regenerate_schema_snapshot -- --ignored\n\n",
        );
        for statement in statements {
            out.push_str(&statement);
            out.push_str(";\n\n");
        }
        std::fs::write(
            concat!(env!("CARGO_MANIFEST_DIR"), "/migrations/schema.sql"),
            out,
        )
        .unwrap();
    }

    // Impact: hopnet-consensus keeps a standalone installer (its chain
    // lives host-side, unreachable from the embeddable crate), which is
    // a second definition of the crate-owned consensus DDL — this pin
    // is what keeps the two from drifting.
    // Should: build identical crate-owned consensus tables from the
    // crate installer and from chain replay.
    #[test]
    fn consensus_standalone_installer_agrees_with_chain() {
        let standalone = rusqlite::Connection::open_in_memory().unwrap();
        hopnet_consensus::store::install_schema(&standalone).unwrap();

        let replayed = rusqlite::Connection::open_in_memory().unwrap();
        install(&replayed).unwrap();

        let crate_tables: std::collections::HashSet<&str> = [
            "consensus_wal",
            "decided_blocks",
            "decided_certificates",
            "consensus_meta",
            "validators",
            "hopnet_consensus_policy",
        ]
        .into();
        let crate_rows = |conn: &rusqlite::Connection| {
            schema_rows(conn)
                .into_iter()
                .filter(|(_, _, tbl, _)| crate_tables.contains(tbl.as_str()))
                .collect::<Vec<_>>()
        };
        let standalone_rows = crate_rows(&standalone);
        assert!(!standalone_rows.is_empty());
        assert_eq!(standalone_rows, crate_rows(&replayed));
    }

    /// A database with every chain at its baseline — what any pre-stamp
    /// file is by construction (S1/S2 dev files, the cutover shape).
    fn baseline_db() -> rusqlite::Connection {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        for chain in chains() {
            hopnet_common::chain::replay(&conn, chain, chain.baseline()).unwrap();
        }
        conn
    }

    // Should: stamp a fresh install at every chain's head.
    #[test]
    fn install_stamps_the_file_at_head() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        install(&conn).unwrap();
        let stamps = read_stamps(&conn).unwrap();
        for chain in chains() {
            assert_eq!(stamps.get(chain.module), Some(&chain.head()));
        }
        assert_eq!(stamps.len(), chains().len());
    }

    // Impact: the boot dispatch trusts this classification to decide
    // between installing, fast-forwarding, and adopting — and a corrupt
    // database must surface as an error, never as Fresh.
    // Should: distinguish an empty file, a stamped file, and a
    // pre-stamp file with user tables.
    #[test]
    fn assess_distinguishes_fresh_stamped_and_legacy() {
        let fresh = rusqlite::Connection::open_in_memory().unwrap();
        assert_eq!(assess(&fresh).unwrap(), SchemaState::Fresh);

        let stamped = rusqlite::Connection::open_in_memory().unwrap();
        install(&stamped).unwrap();
        assert_eq!(assess(&stamped).unwrap(), SchemaState::Stamped);

        let legacy = baseline_db();
        assert_eq!(assess(&legacy).unwrap(), SchemaState::LegacyUnstamped);
    }

    // Impact: §Cutover's "stamped, not migrated" — the live mesh's
    // sealed pre-chain shape must adopt cleanly, and anything that is
    // NOT exactly the baseline shape must refuse rather than guess.
    // Should: adopt an exact-baseline database and land at head after
    // fast-forward, existing rows intact.
    #[test]
    fn adopt_legacy_accepts_exact_baseline_shape_then_fast_forwards() {
        let mut conn = baseline_db();
        conn.execute_batch("INSERT INTO sequences VALUES ('users', 7)")
            .unwrap();

        adopt_legacy(&conn).unwrap();
        let applied = fast_forward(&mut conn).unwrap();
        // identity's stamp step was applied during adopt; nothing else
        // has a gap yet.
        assert_eq!(applied, 0);

        let stamps = read_stamps(&conn).unwrap();
        for chain in chains() {
            assert_eq!(stamps.get(chain.module), Some(&chain.head()));
        }
        let kept: i64 = conn
            .query_row(
                "SELECT next_id FROM sequences WHERE name = 'users'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(kept, 7);
    }

    // Should: refuse to adopt a database whose shape is not exactly
    // the baseline shape, naming both fingerprints.
    #[test]
    fn adopt_legacy_refuses_a_diverged_shape() {
        let conn = baseline_db();
        conn.execute_batch("CREATE TABLE stray (id INTEGER PRIMARY KEY)")
            .unwrap();
        assert!(matches!(
            adopt_legacy(&conn),
            Err(SchemaError::FingerprintMismatch { .. })
        ));
    }

    // Should: refuse stamps naming unknown modules, missing modules,
    // or ordinals that are not real chain positions.
    #[test]
    fn fast_forward_refuses_bad_stamps() {
        let mut conn = rusqlite::Connection::open_in_memory().unwrap();
        install(&conn).unwrap();

        conn.execute_batch("INSERT INTO schema_ordinals VALUES ('ghost', 1)")
            .unwrap();
        assert!(matches!(
            fast_forward(&mut conn),
            Err(SchemaError::InvalidStamp { .. })
        ));
        conn.execute_batch("DELETE FROM schema_ordinals WHERE module = 'ghost'")
            .unwrap();

        conn.execute_batch("DELETE FROM schema_ordinals WHERE module = 'drive'")
            .unwrap();
        assert!(matches!(
            fast_forward(&mut conn),
            Err(SchemaError::InvalidStamp { .. })
        ));

        conn.execute_batch("INSERT INTO schema_ordinals VALUES ('drive', 9)")
            .unwrap();
        assert!(matches!(
            fast_forward(&mut conn),
            Err(SchemaError::InvalidStamp { .. })
        ));
    }

    // Impact: the RFC's +1-contiguity tripwire — Chain::validate only
    // enforces ascending, and a gap means a lost or mis-numbered file.
    // Should: find every chain contiguous from its baseline.
    #[test]
    fn chains_are_contiguous() {
        for chain in chains() {
            for pair in chain.steps.windows(2) {
                assert_eq!(
                    pair[1].ordinal,
                    pair[0].ordinal + 1,
                    "chain {} has a gap between {} and {}",
                    chain.module,
                    pair[0].ordinal,
                    pair[1].ordinal
                );
            }
        }
    }

    // Impact: ordinals live in both the NNNN_slug.sql filenames and
    // the Step consts; drift between them is the "step file without a
    // bump / bump without a step file" tripwire.
    // Should: find every migrations folder matching its chain const
    // exactly, in both directions.
    #[test]
    fn migrations_folders_match_chain_consts() {
        let root = env!("CARGO_MANIFEST_DIR");
        let dir_for = |module: &str| match module {
            "identity" | "telemetry" | "consensus" => format!("{root}/migrations/{module}"),
            "storage" => format!("{root}/hopnet-storage/migrations/storage"),
            "drive" => format!("{root}/hopnet-drive/migrations/drive"),
            "photos" => format!("{root}/hopnet-photos/migrations/photos"),
            "takeout" => format!("{root}/hopnet-takeout/migrations/takeout"),
            other => panic!("module {other} has no folder mapping"),
        };
        for chain in chains() {
            let mut on_disk: Vec<(u32, String)> = std::fs::read_dir(dir_for(chain.module))
                .unwrap()
                .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
                .filter(|n| n.ends_with(".sql"))
                .map(|n| {
                    let (num, rest) = n.split_once('_').expect("NNNN_slug.sql");
                    (
                        num.parse::<u32>().expect("numeric ordinal prefix"),
                        rest.trim_end_matches(".sql").to_string(),
                    )
                })
                .collect();
            on_disk.sort();
            let declared: Vec<(u32, String)> = chain
                .steps
                .iter()
                .map(|s| (s.ordinal, s.slug.to_string()))
                .collect();
            assert_eq!(
                on_disk, declared,
                "chain {} folder vs const mismatch",
                chain.module
            );
        }
    }

    /// Canonical full-database dump for step fixtures: schema rows plus
    /// every table's rows (quote()-encoded, rowid order).
    fn canonical_dump(conn: &rusqlite::Connection) -> String {
        let mut out = String::new();
        for (ty, name, tbl, sql) in schema_rows(conn) {
            out.push_str(&format!("{ty}|{name}|{tbl}|{sql}\n"));
        }
        let tables: Vec<String> = conn
            .prepare(
                "SELECT name FROM sqlite_master WHERE type='table' \
                 AND name NOT LIKE 'sqlite_%' ORDER BY name",
            )
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        for table in tables {
            let cols: Vec<String> = conn
                .prepare(&format!("SELECT name FROM pragma_table_info('{table}')"))
                .unwrap()
                .query_map([], |r| r.get(0))
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
            let expr = cols
                .iter()
                .map(|c| format!("quote(\"{c}\")"))
                .collect::<Vec<_>>()
                .join(" || ',' || ");
            let rows: Vec<String> = conn
                .prepare(&format!("SELECT {expr} FROM \"{table}\" ORDER BY rowid"))
                .unwrap()
                .query_map([], |r| r.get(0))
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
            for row in rows {
                out.push_str(&format!("{table}: {row}\n"));
            }
        }
        out
    }

    /// The per-step golden harness (RFC-020 §Validation, OQ2 resolved:
    /// hand-written raw-SQL fixtures): replay everything to the
    /// pre-step state, apply the fixture rows against that shape, apply
    /// the step, hash-pin the full canonical dump.
    fn run_step_fixture(module: &str, ordinal: u32, fixture_sql: &str) -> String {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        for chain in chains() {
            let to = if chain.module == module {
                ordinal - 1
            } else {
                chain.head()
            };
            hopnet_common::chain::replay(&conn, chain, to).unwrap();
        }
        conn.execute_batch(fixture_sql).unwrap();
        let target = chains().into_iter().find(|c| c.module == module).unwrap();
        hopnet_common::chain::advance(&conn, target, ordinal - 1, ordinal).unwrap();
        blake3::hash(canonical_dump(&conn).as_bytes())
            .to_hex()
            .to_string()
    }

    // Impact: contract rule 2's determinism net — every node replays
    // this step independently and must land byte-identical; the pinned
    // hash is the evidence, and a step edit after release moves it.
    // Should: produce the pinned dump hash for identity/0001 over the
    // fixture rows.
    #[test]
    fn step_fixture_identity_0001_schema_ordinals() {
        // Fixture rows written against the PRE-step shape (identity@0):
        // literal keys, no wall clock (contract rule 2 discipline).
        let hash = run_step_fixture(
            "identity",
            1,
            "INSERT INTO sequences VALUES ('users', 3);
             INSERT INTO users (user_id, username, pubkey, x25519_pubkey, encrypted_privkey, key_salt)
             VALUES (1, 'fixture', X'01', X'02', X'03', X'04');",
        );
        assert_eq!(
            hash, STEP_FIXTURE_IDENTITY_0001_HASH,
            "identity/0001 output moved — a released step may never change \
             (contract rules 1-2); if this is an intentional pre-release \
             redefinition, re-pin in the same commit"
        );
    }

    const STEP_FIXTURE_IDENTITY_0001_HASH: &str =
        "bf26241642c4ec3c060d50fd8552d3cf53fedd2d48a7db1dd01610b121cf693a";

    // Impact: "module names are section names" is load-bearing — the
    // ordinal stamp validates against the section registry, and the
    // artifact manifest's format_version IS the chain position.
    // Should: register exactly the section registry's modules, in its
    // order, with each chain's head at its section's format_version.
    #[test]
    fn chains_mirror_sections_exactly() {
        let chains = chains();
        let sections = crate::db::snapshot::sections();
        assert_eq!(
            chains.iter().map(|c| c.module).collect::<Vec<_>>(),
            sections.iter().map(|s| s.name).collect::<Vec<_>>(),
        );
        for (chain, section) in chains.iter().zip(&sections) {
            chain.validate().unwrap();
            assert_eq!(
                chain.head(),
                section.format_version,
                "chain {} head vs section format_version",
                chain.module
            );
        }
    }
}
