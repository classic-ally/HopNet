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
    steps: &[Step::sql(
        0,
        "init",
        include_str!("../../migrations/identity/0000_init.sql"),
    )],
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

/// THE installer (RFC-020 S2): replay every chain to head. The only way
/// a fresh HopNet schema comes to exist — `initialize` is gone, and the
/// per-crate installer batches with it.
pub fn install(conn: &rusqlite::Connection) -> Result<(), hopnet_common::chain::ChainError> {
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
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema_rows(conn: &rusqlite::Connection) -> Vec<(String, String, String, String)> {
        let mut stmt = conn
            .prepare(
                "SELECT type, name, tbl_name, sql FROM sqlite_master \
                 WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%' \
                 ORDER BY type, name",
            )
            .unwrap();
        stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
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
