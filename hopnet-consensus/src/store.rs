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
    -- band alignment. Host lists it in CONSENSUS_TABLES.
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
/// path. Genesis installation only: height 0 with the synthetic trusted
/// certificate, plus `last_decided_height`. Everything after height 0 must go
/// through the engine.
pub fn install_genesis(
    conn: &Connection,
    block: &Block,
    cert: &WireCommitCertificate,
) -> Result<(), StoreError> {
    let block_bytes = codec::encode(block).map_err(StoreError::Codec)?;
    let cert_bytes = codec::encode(cert).map_err(StoreError::Codec)?;
    conn.execute(
        "INSERT INTO decided_blocks (height, block_hash, round, block) VALUES (0, ?, 0, ?)",
        rusqlite::params![block.block_hash, block_bytes],
    )?;
    conn.execute(
        "INSERT INTO decided_certificates (height, block_hash, round, certificate) VALUES (0, ?, 0, ?)",
        rusqlite::params![block.block_hash, cert_bytes],
    )?;
    conn.execute(
        "INSERT OR REPLACE INTO consensus_meta (key, value) VALUES (?, 0)",
        [META_LAST_DECIDED],
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
    let mut expected = from.as_db();
    for row in rows {
        let (height, block_bytes, cert_bytes) = row?;
        if height != expected {
            break; // gap — serve the contiguous prefix only
        }
        expected += 1;
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

    fn with_rollback<R>(&mut self, f: impl FnOnce(&mut Self::Tx<'_>) -> R) -> Result<R, StoreError> {
        let mut tx = self.conn.transaction()?;
        let r = f(&mut tx);
        // Dropped without commit — rolls back.
        drop(tx);
        Ok(r)
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
