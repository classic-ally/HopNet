//! RFC-020 schema chains: per-module, append-only migration steps.
//!
//! A module's schema at ordinal k is DEFINED as an empty database with
//! its chain's steps baseline..=k applied in order — replay is the only
//! installer. Step files are embedded at compile time by each owning
//! crate (`include_str!` in an explicit `Chain` const), so chain/binary
//! skew is impossible by construction; `hopnet-common` owns only the
//! types and the replay engine, never the SQL.
//!
//! Contract (RFC-020 §The Chain): steps are frozen once released,
//! deterministic (no wall clock, no randomness, no environment), may
//! seed rows with literal values, and write only their own module's
//! tables while reading upstream modules already at head.

/// One migration step of a module's chain.
#[derive(Debug, Clone, Copy)]
pub struct Step {
    /// Chain position. Sequential, strictly ascending within a chain;
    /// doubles as the snapshot section `format_version` at this step.
    pub ordinal: u32,
    /// Human-readable name, mirroring the step's file name
    /// (`NNNN_<slug>.sql`).
    pub slug: &'static str,
    pub kind: StepKind,
}

/// What a step executes. SQL by default; a Rust variant is added only
/// when the first real Rust step exists (RFC-020: SQL by default, Rust
/// by exception).
#[derive(Debug, Clone, Copy)]
pub enum StepKind {
    /// Embedded SQL, applied as one batch.
    Sql(&'static str),
}

impl Step {
    pub const fn sql(ordinal: u32, slug: &'static str, text: &'static str) -> Self {
        Self {
            ordinal,
            slug,
            kind: StepKind::Sql(text),
        }
    }
}

/// A module's chain: the baseline step plus every later step, in
/// ascending ordinal order.
#[derive(Debug, Clone, Copy)]
pub struct Chain {
    /// Module name — equal to the module's snapshot section name
    /// (RFC-020 §Version Surfaces: one vocabulary everywhere).
    pub module: &'static str,
    /// Steps in strictly ascending ordinal order. The first entry is
    /// the baseline (`NNNN_init.sql`): ordinal 0 for a module born in
    /// the chain regime, the adopted pre-chain `format_version` for
    /// cutover baselines.
    pub steps: &'static [Step],
}

impl Chain {
    /// Newest ordinal — the shape this binary's live code runs against.
    pub fn head(&self) -> u32 {
        self.steps.last().map(|s| s.ordinal).unwrap_or(0)
    }

    /// Baseline ordinal — the lowest shape this chain can materialize.
    pub fn baseline(&self) -> u32 {
        self.steps.first().map(|s| s.ordinal).unwrap_or(0)
    }

    /// Structural validity: non-empty, strictly ascending ordinals.
    /// Checked by the engine before any step runs, and by registry
    /// tests at build time.
    pub fn validate(&self) -> Result<(), ChainShapeError> {
        if self.steps.is_empty() {
            return Err(ChainShapeError {
                module: self.module,
                detail: "chain has no steps (a baseline is mandatory)",
            });
        }
        for pair in self.steps.windows(2) {
            if pair[1].ordinal <= pair[0].ordinal {
                return Err(ChainShapeError {
                    module: self.module,
                    detail: "step ordinals must be strictly ascending",
                });
            }
        }
        Ok(())
    }

    /// Whether `ordinal` is an exact chain position (replay targets
    /// come from artifact manifests and must match a real step).
    pub fn contains(&self, ordinal: u32) -> bool {
        self.steps.iter().any(|s| s.ordinal == ordinal)
    }
}

/// A structurally invalid chain declaration — always a programming
/// error in a `Chain` const, never a runtime condition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChainShapeError {
    pub module: &'static str,
    pub detail: &'static str,
}

impl std::fmt::Display for ChainShapeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "chain {}: {}", self.module, self.detail)
    }
}

impl std::error::Error for ChainShapeError {}

#[cfg(feature = "database")]
pub use engine::{replay, replay_all, ChainError};

#[cfg(feature = "database")]
mod engine {
    use super::*;
    use rusqlite::Connection;

    #[derive(Debug)]
    pub enum ChainError {
        /// The chain const itself is malformed.
        Shape(ChainShapeError),
        /// The requested target is not a position this chain contains —
        /// e.g. an artifact manifest naming an ordinal this binary's
        /// chain has never heard of.
        UnknownOrdinal { module: &'static str, ordinal: u32 },
        /// A step's SQL failed. The step's own effects were rolled back
        /// to its savepoint; earlier steps' effects remain, and the
        /// caller's transaction is still open for it to roll back.
        Step {
            module: &'static str,
            ordinal: u32,
            slug: &'static str,
            source: rusqlite::Error,
        },
    }

    impl std::fmt::Display for ChainError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::Shape(e) => write!(f, "{e}"),
                Self::UnknownOrdinal { module, ordinal } => {
                    write!(f, "chain {module}: no step at ordinal {ordinal}")
                }
                Self::Step {
                    module,
                    ordinal,
                    slug,
                    source,
                } => write!(
                    f,
                    "chain {module}: step {ordinal} ({slug}) failed: {source}"
                ),
            }
        }
    }

    impl std::error::Error for ChainError {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            match self {
                Self::Step { source, .. } => Some(source),
                _ => None,
            }
        }
    }

    /// Materialize `chain` at ordinal `to` by applying steps
    /// baseline..=to in order.
    ///
    /// Runs inside the CALLER's transaction: this function never opens,
    /// commits, or closes one (RFC-020 §Execution — the boot transition
    /// owns the transaction; failure anywhere rolls the whole build
    /// back). Each step executes under its own SAVEPOINT so a failure
    /// reports its exact ordinal with the step's partial effects undone.
    pub fn replay(conn: &Connection, chain: &Chain, to: u32) -> Result<(), ChainError> {
        chain.validate().map_err(ChainError::Shape)?;
        if !chain.contains(to) {
            return Err(ChainError::UnknownOrdinal {
                module: chain.module,
                ordinal: to,
            });
        }
        for step in chain.steps.iter().filter(|s| s.ordinal <= to) {
            apply_step(conn, chain.module, step)?;
        }
        Ok(())
    }

    /// Replay every chain to its head, in the given order (manifest
    /// order = FK direction, so it is a valid creation order).
    pub fn replay_all(conn: &Connection, chains: &[&Chain]) -> Result<(), ChainError> {
        for chain in chains {
            replay(conn, chain, chain.head())?;
        }
        Ok(())
    }

    fn apply_step(conn: &Connection, module: &'static str, step: &Step) -> Result<(), ChainError> {
        let fail = |source| ChainError::Step {
            module,
            ordinal: step.ordinal,
            slug: step.slug,
            source,
        };
        // Savepoint names are derived from trusted compile-time chain
        // consts, never runtime input.
        let sp = format!("chain_step_{}", step.ordinal);
        conn.execute_batch(&format!("SAVEPOINT {sp}"))
            .map_err(fail)?;
        let result = match step.kind {
            StepKind::Sql(text) => conn.execute_batch(text),
        };
        match result {
            Ok(()) => conn
                .execute_batch(&format!("RELEASE SAVEPOINT {sp}"))
                .map_err(fail),
            Err(e) => {
                // Undo this step's partial effects, then surface the
                // step error; a rollback failure would mean a broken
                // connection the caller's own rollback will hit too.
                let _ = conn.execute_batch(&format!(
                    "ROLLBACK TO SAVEPOINT {sp}; RELEASE SAVEPOINT {sp}"
                ));
                Err(fail(e))
            }
        }
    }
}

#[cfg(all(test, feature = "database"))]
mod tests {
    use super::*;
    use rusqlite::Connection;

    const TWO_STEPS: Chain = Chain {
        module: "synthetic",
        steps: &[
            Step::sql(0, "init", "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);"),
            Step::sql(
                1,
                "add_w",
                "ALTER TABLE t ADD COLUMN w INTEGER; INSERT INTO t (id, v, w) VALUES (1, 'seed', 7);",
            ),
        ],
    };

    fn conn() -> Connection {
        Connection::open_in_memory().unwrap()
    }

    fn columns(conn: &Connection, table: &str) -> Vec<String> {
        let mut stmt = conn
            .prepare(&format!("SELECT name FROM pragma_table_info('{table}')"))
            .unwrap();
        stmt.query_map([], |r| r.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    }

    // Should: materialize the shape at the requested ordinal, applying
    // only the steps at or below it.
    #[test]
    fn replay_stops_at_the_requested_ordinal() {
        let c = conn();
        replay(&c, &TWO_STEPS, 0).unwrap();
        assert_eq!(columns(&c, "t"), vec!["id", "v"]);
    }

    // Should: apply steps in ascending order through the target,
    // including row transformations and seeds.
    #[test]
    fn replay_to_head_applies_every_step() {
        let c = conn();
        replay(&c, &TWO_STEPS, TWO_STEPS.head()).unwrap();
        assert_eq!(columns(&c, "t"), vec!["id", "v", "w"]);
        let w: i64 = c
            .query_row("SELECT w FROM t WHERE id = 1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(w, 7);
    }

    // Impact: replay targets come from artifact manifests; an ordinal
    // this binary has never heard of must refuse, not approximate.
    // Should: refuse a target that is not an exact chain position.
    #[test]
    fn replay_refuses_an_unknown_target_ordinal() {
        let c = conn();
        let err = replay(&c, &TWO_STEPS, 5).unwrap_err();
        assert!(matches!(
            err,
            ChainError::UnknownOrdinal {
                module: "synthetic",
                ordinal: 5
            }
        ));
    }

    // Should: report the failing step's module and ordinal.
    // Should: roll back only the failing step's partial effects,
    // leaving earlier steps' work intact in the caller's transaction.
    #[test]
    fn a_failing_step_reports_its_ordinal_and_undoes_only_itself() {
        const BAD_SECOND: Chain = Chain {
            module: "synthetic",
            steps: &[
                Step::sql(0, "init", "CREATE TABLE t (id INTEGER PRIMARY KEY);"),
                Step::sql(
                    1,
                    "bad",
                    "INSERT INTO t (id) VALUES (1); INSERT INTO missing (id) VALUES (2);",
                ),
            ],
        };
        let mut c = conn();
        let tx = c.transaction().unwrap();
        let err = replay(&tx, &BAD_SECOND, 1).unwrap_err();
        match err {
            ChainError::Step {
                module, ordinal, ..
            } => {
                assert_eq!(module, "synthetic");
                assert_eq!(ordinal, 1);
            }
            other => panic!("expected Step error, got {other:?}"),
        }
        // Step 0's table survives; step 1's partial insert does not.
        let rows: i64 = tx
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(rows, 0);
        // The caller's transaction is still usable and still its to
        // abort — the engine committed nothing.
        tx.rollback().unwrap();
        let table_count: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE name = 't'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(table_count, 0);
    }

    // Should: refuse a chain whose ordinals are not strictly ascending.
    // Should not: execute any step of a malformed chain.
    #[test]
    fn a_malformed_chain_is_refused_before_any_step_runs() {
        const DUPLICATE: Chain = Chain {
            module: "synthetic",
            steps: &[
                Step::sql(1, "a", "CREATE TABLE a (id INTEGER);"),
                Step::sql(1, "b", "CREATE TABLE b (id INTEGER);"),
            ],
        };
        let c = conn();
        assert!(matches!(
            replay(&c, &DUPLICATE, 1),
            Err(ChainError::Shape(_))
        ));
        let tables: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(tables, 0);
    }

    // Should: replay a set of chains to head in the given order.
    #[test]
    fn replay_all_walks_chains_in_order() {
        const UPSTREAM: Chain = Chain {
            module: "up",
            steps: &[Step::sql(
                0,
                "init",
                "CREATE TABLE up (id INTEGER PRIMARY KEY);",
            )],
        };
        const DOWNSTREAM: Chain = Chain {
            module: "down",
            steps: &[Step::sql(
                0,
                "init",
                "CREATE TABLE down (id INTEGER PRIMARY KEY, up_id INTEGER REFERENCES up(id));",
            )],
        };
        let c = conn();
        replay_all(&c, &[&UPSTREAM, &DOWNSTREAM]).unwrap();
        assert_eq!(columns(&c, "down"), vec!["id", "up_id"]);
    }
}
