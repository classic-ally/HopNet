//! SQLite-backed consensus persistence: the WAL plus decided blocks,
//! certificates, and metadata. This is the production `Storage` impl; the
//! deterministic simulator uses the in-memory fakes in `sim.rs`.
//!
//! Table ownership: everything here is consensus-owned and coexists with the
//! application schema in the same database file — that is the point:
//! `decide_atomically` runs the application's state mutation and the
//! consensus-side writes in ONE SQLite transaction, so app state and decided
//! history can never diverge across a crash.
//!
//! Durability: `wal_append` runs as a single autocommit INSERT, so it is as
//! durable as the connection's `synchronous` pragma. The engine publishes a
//! message only after its WAL entry is appended — that ordering is the
//! no-equivocation-across-crash guarantee, so the consensus connection must
//! not weaken `synchronous` below the deployment's crash model.
//!
//! The commit callback lets the embedding application observe commit latency
//! (HopNet passes `db::shared::commit_timed`) without this crate depending on
//! its telemetry.

use std::ops::DerefMut;

use rusqlite::{Connection, OptionalExtension, TransactionBehavior};

use crate::codec::{self, WireCommitCertificate, WireWalEntry};
use crate::context::Height;
use crate::traits::{ApplyError, Storage};
use crate::types::Block;

/// How `decide_atomically` commits its transaction. HopNet passes a wrapper
/// that records commit latency; tests pass `|tx| tx.commit()`.
pub type CommitFn = fn(rusqlite::Transaction<'_>) -> rusqlite::Result<()>;

/// Owned-connection wrapper so a plain `rusqlite::Connection` satisfies the
/// `DerefMut<Target = Connection>` bound (an r2d2 `PooledConnection` already
/// does — that is the handle production passes).
pub struct OwnedConn(pub Connection);

impl std::ops::Deref for OwnedConn {
    type Target = Connection;
    fn deref(&self) -> &Connection {
        &self.0
    }
}

impl DerefMut for OwnedConn {
    fn deref_mut(&mut self) -> &mut Connection {
        &mut self.0
    }
}

#[derive(Debug)]
pub enum StoreError {
    Db(rusqlite::Error),
    Codec(codec::CodecError),
    /// Application apply failure lifted into the decide transaction's error
    /// channel (rolls the whole decide back).
    Apply(String),
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreError::Db(e) => write!(f, "db: {e}"),
            StoreError::Codec(e) => write!(f, "codec: {e}"),
            StoreError::Apply(msg) => write!(f, "apply: {msg}"),
        }
    }
}

impl std::error::Error for StoreError {}

impl From<rusqlite::Error> for StoreError {
    fn from(e: rusqlite::Error) -> Self {
        StoreError::Db(e)
    }
}

const SCHEMA: &str = "
    CREATE TABLE IF NOT EXISTS consensus_wal (
        height      INTEGER NOT NULL,
        seq         INTEGER NOT NULL,
        entry_type  INTEGER NOT NULL,
        entry       BLOB NOT NULL,
        PRIMARY KEY (height, seq)
    );

    CREATE TABLE IF NOT EXISTS decided_blocks (
        height      INTEGER PRIMARY KEY,
        block_hash  BLOB NOT NULL UNIQUE,
        round       INTEGER NOT NULL,
        block       BLOB NOT NULL
    );

    CREATE TABLE IF NOT EXISTS decided_certificates (
        height      INTEGER PRIMARY KEY,
        block_hash  BLOB NOT NULL,
        round       INTEGER NOT NULL,
        certificate BLOB NOT NULL
    );

    CREATE TABLE IF NOT EXISTS consensus_meta (
        key         TEXT PRIMARY KEY,
        value       BLOB NOT NULL
    );

    -- Validator membership (RFC-CONSENSUS-002): height-versioned
    -- activation/deactivation rows, descended from the host schema. The
    -- host's nodes table is a documented interface (get_validators JOINs
    -- it) — deliberately NO foreign key: this crate installs standalone
    -- (crate tests, SqliteStorage self-install), and validator rows are
    -- written only by consensus handlers whose submitters are
    -- signature-verified against nodes before dispatch.
    CREATE TABLE IF NOT EXISTS validators (
        effective_height    INTEGER NOT NULL,   -- height at which the state change takes effect
        node_id             INTEGER NOT NULL,
        is_active           INTEGER NOT NULL,
        -- Departure class (RFC-CONSENSUS-001 'Departure classes'):
        -- NULL on activation rows; lastDeparture = latest is_active=0 row.
        -- NULL-proofed: SQLite CHECKs pass on NULL, so the deactivation
        -- disjunct must assert IS NOT NULL explicitly.
        departure_kind      TEXT
            CHECK ((is_active = 1 AND departure_kind IS NULL)
                OR (is_active = 0 AND departure_kind IS NOT NULL
                    AND departure_kind IN ('voluntary', 'voted_out'))),
        PRIMARY KEY (effective_height, node_id)
    );

    CREATE INDEX IF NOT EXISTS idx_validator_height ON validators(effective_height DESC);
    CREATE INDEX IF NOT EXISTS idx_validator_active ON validators(effective_height, is_active);
    CREATE INDEX IF NOT EXISTS idx_validator_node ON validators(node_id, effective_height DESC);

    -- Consensus membership policy (RFC-CONSENSUS-002 Configuration):
    -- consensus-replicated key/value, seeded at genesis through the host's
    -- GenesisPayload (HOPNET_GENESIS_CONSENSUS_POLICY at mesh creation);
    -- membership::ConsensusPolicy::from_rows resolves it with code defaults
    -- for absent keys. Values parameterize SUBJECTIVE votes only, so
    -- per-node disagreement degrades latency, never safety; replicated for
    -- band alignment. Covered by this crate's SNAPSHOT_SECTION.
    CREATE TABLE IF NOT EXISTS hopnet_consensus_policy (
        key   TEXT PRIMARY KEY,
        value TEXT NOT NULL
    );
";

const META_LAST_DECIDED: &str = "last_decided_height";

/// consensus_meta key: the chain id (32-byte genesis block hash). Binds all
/// signing payloads to this mesh; set once at genesis install.
pub const META_CHAIN_ID: &str = "chain_id";
/// consensus_meta key: the mesh's quorum profile (`QuorumProfile::as_str`).
/// Genesis-fixed; a post-genesis change requires its own consensus scheme.
pub const META_QUORUM_PROFILE: &str = "quorum_profile";

/// Install the consensus tables on any connection to the database (idempotent).
pub fn install_schema(conn: &Connection) -> Result<(), StoreError> {
    conn.execute_batch(SCHEMA)?;
    Ok(())
}

/// This crate's section of the canonical state snapshot (RFC-019 S1).
///
/// decided_blocks IS the agreement invariant, so the live-mesh divergence
/// check covers it — but epoch history dies with the retained epoch-N
/// database (RFC-019 Archival & Retention), and the next epoch's chain
/// tables are born from the genesis installer, so it is never exported
/// across a boundary (DivergenceOnly).
pub const SNAPSHOT_SECTION: hopnet_common::SectionSpec = hopnet_common::SectionSpec {
    name: "consensus",
    format_version: 1,
    tables: &[
        hopnet_common::TableSpec::exported("validators"),
        hopnet_common::TableSpec::exported("hopnet_consensus_policy"),
        hopnet_common::TableSpec {
            name: "decided_blocks",
            role: hopnet_common::TableRole::DivergenceOnly,
            excluded_columns: &[],
        },
    ],
};

/// Node-local tables — outside the snapshot universe entirely:
/// consensus_wal is per-node ephemeral (and empty at a seal by
/// construction), consensus_meta is a per-node cursor plus per-epoch
/// derived values, and decided_certificates is a node-local quorum proof —
/// different vote subsets are legitimate.
pub const NODE_LOCAL_TABLES: &[&str] = &["consensus_wal", "consensus_meta", "decided_certificates"];

/// Read `consensus_meta.last_decided_height` on any connection. `None` until
/// genesis has been installed. Free function so hosts can read it outside the
/// engine (startup height, progress endpoints).
pub fn last_decided_height(conn: &Connection) -> Result<Option<Height>, StoreError> {
    let v: Option<i64> = conn
        .query_row(
            "SELECT value FROM consensus_meta WHERE key = ?",
            [META_LAST_DECIDED],
            |row| row.get(0),
        )
        .optional()?;
    Ok(v.map(Height::from_db))
}

/// Read an arbitrary `consensus_meta` value (chain id, quorum profile, ...).
pub fn meta_get(conn: &Connection, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
    let v: Option<Vec<u8>> = conn
        .query_row(
            "SELECT value FROM consensus_meta WHERE key = ?",
            [key],
            |row| row.get(0),
        )
        .optional()?;
    Ok(v)
}

/// Write an arbitrary `consensus_meta` value.
pub fn meta_put(conn: &Connection, key: &str, value: &[u8]) -> Result<(), StoreError> {
    conn.execute(
        "INSERT OR REPLACE INTO consensus_meta (key, value) VALUES (?, ?)",
        rusqlite::params![key, value],
    )?;
    Ok(())
}

/// Seed/overwrite membership policy rows (genesis apply; later a settings
/// transaction — RFC-CONSENSUS-001 Deferred).
pub fn apply_policy_rows(conn: &Connection, rows: &[(String, String)]) -> Result<(), StoreError> {
    let mut stmt = conn
        .prepare("INSERT OR REPLACE INTO hopnet_consensus_policy (key, value) VALUES (?1, ?2)")?;
    for (key, value) in rows {
        stmt.execute(rusqlite::params![key, value])?;
    }
    Ok(())
}

/// Resolve the replicated membership policy (code defaults for absent keys).
pub fn read_policy(conn: &Connection) -> Result<crate::membership::ConsensusPolicy, StoreError> {
    let mut stmt = conn.prepare("SELECT key, value FROM hopnet_consensus_policy")?;
    let rows: Vec<(String, String)> = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<Result<_, _>>()?;
    Ok(crate::membership::ConsensusPolicy::from_rows(&rows))
}

/// Install a decided (block, certificate) pair outside the engine's decide
/// path. Genesis installation only, at the block's own height — 0 for a
/// fresh epoch-1 mesh, the boundary height H for an epoch-N+1 regenesis
/// genesis (RFC-019: heights are continuous across epochs) — with the
/// synthetic trusted certificate, plus `last_decided_height`. Everything
/// after the genesis height must go through the engine.
pub fn install_genesis(
    conn: &Connection,
    block: &Block,
    cert: &WireCommitCertificate,
) -> Result<(), StoreError> {
    let block_bytes = codec::encode(block).map_err(StoreError::Codec)?;
    let cert_bytes = codec::encode(cert).map_err(StoreError::Codec)?;
    let height = Height(block.data.height).as_db();
    conn.execute(
        "INSERT INTO decided_blocks (height, block_hash, round, block) VALUES (?, ?, 0, ?)",
        rusqlite::params![height, block.block_hash, block_bytes],
    )?;
    conn.execute(
        "INSERT INTO decided_certificates (height, block_hash, round, certificate) VALUES (?, ?, 0, ?)",
        rusqlite::params![height, block.block_hash, cert_bytes],
    )?;
    conn.execute(
        "INSERT OR REPLACE INTO consensus_meta (key, value) VALUES (?, ?)",
        rusqlite::params![META_LAST_DECIDED, height],
    )?;
    Ok(())
}

/// Decided (block, certificate) pairs for `[from, to]`, ascending, stopping
/// at the first gap. Serves the decided-value sync protocol; a free function
/// so RPC handlers can run it on any pooled connection.
pub fn decided_range(
    conn: &Connection,
    from: Height,
    to: Height,
) -> Result<Vec<(Block, WireCommitCertificate)>, StoreError> {
    let mut stmt = conn.prepare_cached(
        "SELECT b.height, b.block, c.certificate
         FROM decided_blocks b JOIN decided_certificates c ON b.height = c.height
         WHERE b.height >= ? AND b.height <= ?
         ORDER BY b.height ASC",
    )?;
    let rows = stmt.query_map([from.as_db(), to.as_db()], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, Vec<u8>>(1)?,
            row.get::<_, Vec<u8>>(2)?,
        ))
    })?;

    let mut out = Vec::new();
    for (i, row) in rows.enumerate() {
        let (height, block_bytes, cert_bytes) = row?;
        if height != from.as_db() + i as i64 {
            break; // gap — serve the contiguous prefix only
        }
        let block: Block = codec::decode(&block_bytes).map_err(StoreError::Codec)?;
        let cert: WireCommitCertificate = codec::decode(&cert_bytes).map_err(StoreError::Codec)?;
        out.push((block, cert));
    }
    Ok(out)
}

pub struct SqliteStorage<C: DerefMut<Target = Connection> + 'static = OwnedConn> {
    conn: C,
    commit: CommitFn,
}

impl SqliteStorage<OwnedConn> {
    /// Wrap an owned connection, installing the consensus tables if absent.
    /// The connection is held for the storage's lifetime (the host keeps it
    /// across heights).
    pub fn new(conn: Connection, commit: CommitFn) -> Result<Self, StoreError> {
        Self::from_handle(OwnedConn(conn), commit)
    }
}

impl<C: DerefMut<Target = Connection> + 'static> SqliteStorage<C> {
    /// Wrap any connection handle (e.g. an r2d2 `PooledConnection`, so the
    /// storage shares the application pool's database — required for shared
    /// in-memory databases and for the pool-reserved production conn).
    pub fn from_handle(conn: C, commit: CommitFn) -> Result<Self, StoreError> {
        install_schema(&conn)?;
        Ok(Self { conn, commit })
    }

    /// See [`decided_range`].
    pub fn decided_range(
        &mut self,
        from: Height,
        to: Height,
    ) -> Result<Vec<(Block, WireCommitCertificate)>, StoreError> {
        decided_range(&self.conn, from, to)
    }
}

impl<C: DerefMut<Target = Connection> + 'static> Storage for SqliteStorage<C> {
    type Tx<'a>
        = rusqlite::Transaction<'a>
    where
        Self: 'a;
    type Error = StoreError;

    fn wal_append(
        &mut self,
        height: Height,
        seq: u64,
        entry: &WireWalEntry,
    ) -> Result<(), StoreError> {
        let bytes = codec::encode(entry).map_err(StoreError::Codec)?;
        // Autocommit INSERT: durable (per the connection's synchronous level)
        // before this returns, which must precede the network publish.
        self.conn
            .prepare_cached(
                "INSERT INTO consensus_wal (height, seq, entry_type, entry) VALUES (?, ?, ?, ?)",
            )?
            .execute(rusqlite::params![
                height.as_db(),
                seq as i64,
                entry.entry_type(),
                bytes
            ])?;
        Ok(())
    }

    fn wal_fetch(&mut self, height: Height) -> Result<Vec<WireWalEntry>, StoreError> {
        let mut stmt = self
            .conn
            .prepare_cached("SELECT entry FROM consensus_wal WHERE height = ? ORDER BY seq ASC")?;
        let rows: Vec<Vec<u8>> = stmt
            .query_map([height.as_db()], |row| row.get(0))?
            .collect::<Result<_, _>>()?;

        let mut entries = Vec::with_capacity(rows.len());
        let last = rows.len().saturating_sub(1);
        for (i, bytes) in rows.iter().enumerate() {
            match codec::decode::<WireWalEntry>(bytes) {
                Ok(e) => entries.push(e),
                // A final entry that fails to decode is treated as torn and
                // dropped (recover with what we have); an earlier one is real
                // corruption and must not be silently skipped.
                Err(e) if i == last => {
                    tracing::warn!("dropping torn final WAL entry at height {height}: {e}");
                }
                Err(e) => return Err(StoreError::Codec(e)),
            }
        }
        Ok(entries)
    }

    fn wal_reset(&mut self) -> Result<(), StoreError> {
        self.conn.execute("DELETE FROM consensus_wal", [])?;
        Ok(())
    }

    fn decide_atomically<R>(
        &mut self,
        f: impl FnOnce(&mut Self::Tx<'_>) -> Result<R, StoreError>,
    ) -> Result<R, StoreError> {
        let mut tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let r = f(&mut tx)?;
        (self.commit)(tx)?;
        Ok(r)
    }

    fn with_rollback<R>(
        &mut self,
        f: impl FnOnce(&mut Self::Tx<'_>) -> R,
    ) -> Result<R, StoreError> {
        let mut tx = self.conn.transaction()?;
        let r = f(&mut tx);
        // Dropped without commit — rolls back.
        drop(tx);
        Ok(r)
    }

    fn with_rollback_immediate<R>(
        &mut self,
        busy_timeout_ms: u32,
        f: impl FnOnce(&mut Self::Tx<'_>) -> R,
    ) -> Result<R, StoreError> {
        // Bound the wait ourselves: this runs on the single-threaded
        // consensus shell, where the connection's default busy_timeout
        // (5s) would stall vote processing for far too long.
        let prior: i64 = self
            .conn
            .query_row("PRAGMA busy_timeout", [], |row| row.get(0))?;
        self.conn
            .execute_batch(&format!("PRAGMA busy_timeout = {busy_timeout_ms}"))?;
        let out = match self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
        {
            Ok(mut tx) => {
                let r = f(&mut tx);
                // Dropped without commit — rolls back.
                drop(tx);
                Ok(r)
            }
            Err(e) => Err(StoreError::from(e)),
        };
        let _ = self
            .conn
            .execute_batch(&format!("PRAGMA busy_timeout = {prior}"));
        out
    }

    fn store_decided_tx(
        tx: &mut Self::Tx<'_>,
        block: &Block,
        cert: &WireCommitCertificate,
    ) -> Result<(), StoreError> {
        let height = hopnet_common::height::height_to_db(block.data.height);
        let block_bytes = codec::encode(block).map_err(StoreError::Codec)?;
        let cert_bytes = codec::encode(cert).map_err(StoreError::Codec)?;
        tx.prepare_cached(
            "INSERT INTO decided_blocks (height, block_hash, round, block) VALUES (?, ?, ?, ?)",
        )?
        .execute(rusqlite::params![
            height,
            block.block_hash,
            block.data.round,
            block_bytes
        ])?;
        tx.prepare_cached(
            "INSERT INTO decided_certificates (height, block_hash, round, certificate)
             VALUES (?, ?, ?, ?)",
        )?
        .execute(rusqlite::params![
            height,
            cert.value_id,
            cert.round,
            cert_bytes
        ])?;
        Ok(())
    }

    fn truncate_wal_tx(tx: &mut Self::Tx<'_>, up_to: Height) -> Result<(), StoreError> {
        tx.prepare_cached("DELETE FROM consensus_wal WHERE height <= ?")?
            .execute([up_to.as_db()])?;
        Ok(())
    }

    fn set_last_decided_tx(tx: &mut Self::Tx<'_>, height: Height) -> Result<(), StoreError> {
        tx.prepare_cached("INSERT OR REPLACE INTO consensus_meta (key, value) VALUES (?, ?)")?
            .execute(rusqlite::params![META_LAST_DECIDED, height.as_db()])?;
        Ok(())
    }

    fn last_decided(&mut self) -> Result<Option<Height>, StoreError> {
        let v: Option<i64> = self
            .conn
            .query_row(
                "SELECT value FROM consensus_meta WHERE key = ?",
                [META_LAST_DECIDED],
                |row| row.get(0),
            )
            .optional()?;
        Ok(v.map(Height::from_db))
    }

    fn apply_error(e: ApplyError) -> StoreError {
        StoreError::Apply(e.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Blake3Hash, Block, BlockData, Transactions};

    fn fresh_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        install_schema(&conn).unwrap();
        conn
    }

    fn block_at(height: u64, parent: Option<Blake3Hash>) -> Block {
        Block::new(BlockData {
            height,
            round: 0,
            parent_hash: parent,
            transactions: Transactions(Vec::new()),
        })
        .unwrap()
    }

    // Impact: RFC-019 heights are continuous across epochs — the epoch-N+1
    // genesis sits at the boundary height H, not 0, and every boot reader
    // derives the start height from what this writes.
    // Should: install the genesis pair at the block's own height and report
    // that height as last decided.
    #[test]
    fn install_genesis_at_boundary_height() {
        let conn = fresh_conn();
        let h = 41_u64;
        let block = block_at(h, Some(Blake3Hash::new(blake3::hash(b"epoch-n-final"))));
        let cert = WireCommitCertificate {
            height: h,
            round: 0,
            value_id: block.block_hash,
            signatures: Vec::new(),
        };
        install_genesis(&conn, &block, &cert).unwrap();

        assert_eq!(last_decided_height(&conn).unwrap(), Some(Height(h)));
        let pairs = decided_range(&conn, Height(h), Height(h)).unwrap();
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].0.block_hash, block.block_hash);
        assert_eq!(pairs[0].1.value_id, block.block_hash);
    }

    /// File-backed DB so a second raw connection can contend for the write
    /// lock (`:memory:` databases are per-connection). WAL + a busy_timeout
    /// mirroring production (src/db/shared.rs in the host).
    fn contended_pair(tag: &str) -> (std::path::PathBuf, Connection) {
        let path = std::env::temp_dir().join(format!(
            "hopnet-consensus-busy-{tag}-{}.db",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch("PRAGMA journal_mode = WAL; PRAGMA busy_timeout = 5000;")
            .unwrap();
        install_schema(&conn).unwrap();
        conn.execute_batch("CREATE TABLE IF NOT EXISTS victim_probe (id INTEGER)")
            .unwrap();
        (path, conn)
    }

    fn hold_write_lock(
        path: &std::path::Path,
        hold: std::time::Duration,
    ) -> std::thread::JoinHandle<()> {
        let side = Connection::open(path).unwrap();
        side.execute_batch("BEGIN IMMEDIATE; INSERT INTO victim_probe (id) VALUES (4242);")
            .unwrap();
        std::thread::spawn(move || {
            std::thread::sleep(hold);
            side.execute_batch("COMMIT").unwrap();
        })
    }

    // Impact: validate_block dry-runs write inside the host's rollback
    // transaction; a DEFERRED transaction that read first cannot promote to
    // writer while another connection holds the lock, and SQLite refuses
    // WITHOUT consulting busy_timeout. This is the mechanism that turned
    // node-local lock contention into Invalid votes and false SyncInvalid
    // determinism alarms.
    // Should: fail the read-then-write under a DEFERRED rollback transaction
    // while a competing writer holds the lock.
    // Should: succeed via with_rollback_immediate under the same contention,
    // because IMMEDIATE keeps the busy handler in play and the snapshot
    // cannot go stale mid-transaction.
    #[test]
    fn immediate_rollback_transaction_survives_contention_deferred_does_not() {
        let (path, conn) = contended_pair("immediate");
        let mut storage = SqliteStorage::new(conn, |tx| tx.commit()).unwrap();

        let contender = hold_write_lock(&path, std::time::Duration::from_millis(150));
        let deferred = storage
            .with_rollback(|tx| {
                // Read first (pins the snapshot), then attempt the write.
                let _: i64 = tx
                    .query_row("SELECT COUNT(*) FROM victim_probe", [], |r| r.get(0))
                    .unwrap();
                tx.execute("INSERT INTO victim_probe (id) VALUES (1)", [])
            })
            .unwrap();
        assert!(
            deferred.is_err(),
            "deferred read-then-write should hit the busy path under contention"
        );

        let immediate = storage
            .with_rollback_immediate(300, |tx| {
                let _: i64 = tx
                    .query_row("SELECT COUNT(*) FROM victim_probe", [], |r| r.get(0))
                    .unwrap();
                tx.execute("INSERT INTO victim_probe (id) VALUES (1)", [])
            })
            .unwrap();
        assert!(
            immediate.is_ok(),
            "IMMEDIATE retry must produce a verdict once the lock clears: {immediate:?}"
        );
        contender.join().unwrap();

        // The probe row rolled back — with_rollback_immediate never commits.
        let check = Connection::open(&path).unwrap();
        let n: i64 = check
            .query_row("SELECT COUNT(*) FROM victim_probe WHERE id = 1", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(n, 0, "dry-run writes must roll back");
        let _ = std::fs::remove_file(&path);
    }

    // Should: restore the connection's prior busy_timeout after the bounded
    // IMMEDIATE attempt, so the shared consensus connection keeps its
    // production configuration.
    #[test]
    fn immediate_rollback_restores_busy_timeout() {
        let (path, conn) = contended_pair("timeout-restore");
        let mut storage = SqliteStorage::new(conn, |tx| tx.commit()).unwrap();
        storage
            .with_rollback_immediate(300, |tx| {
                tx.execute("INSERT INTO victim_probe (id) VALUES (2)", [])
                    .unwrap();
            })
            .unwrap();
        let restored: i64 = storage
            .conn
            .query_row("PRAGMA busy_timeout", [], |r| r.get(0))
            .unwrap();
        assert_eq!(restored, 5000);
        let _ = std::fs::remove_file(&path);
    }

    // Should: keep the fresh-mesh path byte-for-byte — a height-0 genesis
    // installs at 0 with last decided 0.
    #[test]
    fn install_genesis_at_height_zero_unchanged() {
        let conn = fresh_conn();
        let block = block_at(0, None);
        let cert = WireCommitCertificate {
            height: 0,
            round: 0,
            value_id: block.block_hash,
            signatures: Vec::new(),
        };
        install_genesis(&conn, &block, &cert).unwrap();
        assert_eq!(last_decided_height(&conn).unwrap(), Some(Height(0)));
        assert_eq!(decided_range(&conn, Height(0), Height(0)).unwrap().len(), 1);
    }
}
