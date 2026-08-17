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

    // Impact: S1's acceptance gate — the chain regime may replace
    // `initialize` (S2) only because replay provably builds the same
    // schema, to the byte, today.
    // Should: build a schema byte-identical to initialize's, comparing
    // every object's exact sqlite_master text.
    #[test]
    fn replay_matches_initialize_exactly() {
        let installed = rusqlite::Connection::open_in_memory().unwrap();
        crate::db::shared::initialize(&installed).unwrap();

        let replayed = rusqlite::Connection::open_in_memory().unwrap();
        hopnet_common::chain::replay_all(&replayed, &chains()).unwrap();

        let installed_rows = schema_rows(&installed);
        assert!(!installed_rows.is_empty());
        assert_eq!(installed_rows, schema_rows(&replayed));
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
