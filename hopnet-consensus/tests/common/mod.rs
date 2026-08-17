#![allow(dead_code)]
use std::path::PathBuf;

use ed25519_dalek::SigningKey;
use malachitebft_core_consensus::Params;
use malachitebft_core_types::{Round, ValuePayload};

use hopnet_consensus::codec::WireCommitCertificate;
use hopnet_consensus::config::{MalachiteThresholds, QuorumProfile};
use hopnet_consensus::context::{Address, Height, HopNetContext, Validator};
use hopnet_consensus::store::SqliteStorage;
use hopnet_consensus::traits::{Application, ApplyError, ValidationOrigin, ValidationVerdict};
use hopnet_consensus::types::{Blake3Hash, Block, BlockData, PrivKey, PubKey, Transactions};
use hopnet_consensus::HopNetValidatorSet;

/// Deterministic test key for a node id.
pub fn key(node_id: i32) -> PrivKey {
    let mut seed = [0u8; 32];
    seed[..4].copy_from_slice(&node_id.to_le_bytes());
    seed[31] = 0xA5;
    PrivKey(SigningKey::from_bytes(&seed))
}

pub fn pubkey(node_id: i32) -> PubKey {
    key(node_id).public()
}

/// Validator set over node ids 0..n with uniform power 1.
pub fn valset(n: i32) -> HopNetValidatorSet {
    HopNetValidatorSet::new((0..n).map(|i| Validator::new(i, pubkey(i))).collect())
}

pub fn chain_id() -> Blake3Hash {
    Blake3Hash::from_bytes([7u8; 32])
}

pub fn params(node_id: i32, profile: QuorumProfile) -> Params<HopNetContext> {
    Params {
        address: Address(node_id),
        threshold_params: profile.thresholds_for(1),
        value_payload: ValuePayload::PartsOnly,
        enabled: true,
    }
}

// ---------------------------------------------------------------------------
// SQLite-backed test fixtures (shared by store.rs and shell.rs tests)

/// Unique temp DB path per test (SQLite needs a real file to survive reopen).
pub fn temp_db(name: &str) -> PathBuf {
    let path =
        std::env::temp_dir().join(format!("hopnet-consensus-{name}-{}.db", std::process::id()));
    let _ = std::fs::remove_file(&path);
    path
}

/// Open storage over a file-backed DB, with the test `applied` table.
pub fn open_storage(path: &PathBuf) -> SqliteStorage {
    let conn = rusqlite::Connection::open(path).unwrap();
    storage_from_conn(conn)
}

/// Wrap an existing connection (also used with in-memory DBs).
pub fn storage_from_conn(conn: rusqlite::Connection) -> SqliteStorage {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS applied (height INTEGER PRIMARY KEY, hash BLOB NOT NULL)",
    )
    .unwrap();
    SqliteStorage::new(conn, |tx| tx.commit()).unwrap()
}

/// Minimal deterministic app over SQLite: applies blocks into the `applied`
/// table so tests can prove app writes commit atomically with consensus state.
pub struct SqlApp {
    pub valset: HopNetValidatorSet,
}

impl Application<SqliteStorage> for SqlApp {
    fn validate_block(
        &mut self,
        _height: Height,
        _block: &Block,
        _tx: &mut rusqlite::Transaction<'_>,
        _origin: ValidationOrigin,
    ) -> ValidationVerdict {
        ValidationVerdict::Valid
    }

    fn apply_block(
        &mut self,
        height: Height,
        block: &Block,
        tx: &mut rusqlite::Transaction<'_>,
    ) -> Result<(), ApplyError> {
        tx.execute(
            "INSERT INTO applied (height, hash) VALUES (?, ?)",
            rusqlite::params![height.as_db(), block.block_hash],
        )
        .map_err(|e| ApplyError(e.to_string()))?;
        Ok(())
    }

    fn validator_set(&mut self, _height: Height) -> HopNetValidatorSet {
        self.valset.clone()
    }

    fn on_decided(&mut self, _height: Height, _block: &Block, _cert: &WireCommitCertificate) {}
}

/// Open storage over a file-backed DB that a second raw connection can
/// contend for the write lock (`:memory:` databases are per-connection).
/// WAL + busy_timeout mirror production — the 5000 ms busy_timeout is
/// load-bearing: it keeps WAL appends and decides (genuinely fatal paths,
/// which post-fix abort the process) waiting out a test's write-lock hold
/// instead of failing. Only the validation dry-run paths bound their own
/// wait below it.
pub fn contended_db(name: &str) -> (PathBuf, SqliteStorage) {
    let path = temp_db(name);
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute_batch(
        "PRAGMA journal_mode = WAL; PRAGMA busy_timeout = 5000;
         CREATE TABLE IF NOT EXISTS victim_probe (id INTEGER);",
    )
    .unwrap();
    (path, storage_from_conn(conn))
}

/// Hold the database's write lock from a second connection for `hold`, then
/// commit. Mirrors the helper in store.rs's unit tests (module-private
/// there).
pub fn hold_write_lock(
    path: &std::path::Path,
    hold: std::time::Duration,
) -> std::thread::JoinHandle<()> {
    let side = rusqlite::Connection::open(path).unwrap();
    side.execute_batch("BEGIN IMMEDIATE; INSERT INTO victim_probe (id) VALUES (4242);")
        .unwrap();
    std::thread::spawn(move || {
        std::thread::sleep(hold);
        side.execute_batch("COMMIT").unwrap();
    })
}

/// SqlApp variant whose validate_block read-then-writes inside the dry-run
/// transaction and classifies lock contention as Undetermined — a miniature
/// of HopNetApplication's classified handler dry-run. Behaves exactly like
/// SqlApp (Valid) when uncontended, so it can back every node of a mesh.
pub struct ContendedApp {
    pub inner: SqlApp,
}

impl ContendedApp {
    pub fn new(valset: HopNetValidatorSet) -> Self {
        Self {
            inner: SqlApp { valset },
        }
    }
}

impl Application<SqliteStorage> for ContendedApp {
    fn validate_block(
        &mut self,
        _height: Height,
        _block: &Block,
        tx: &mut rusqlite::Transaction<'_>,
        _origin: ValidationOrigin,
    ) -> ValidationVerdict {
        let r = tx
            .query_row("SELECT COUNT(*) FROM victim_probe", [], |row| {
                row.get::<_, i64>(0)
            })
            .and_then(|_| tx.execute("INSERT INTO victim_probe (id) VALUES (1)", []));
        match r {
            Ok(_) => ValidationVerdict::Valid,
            Err(e)
                if matches!(
                    e.sqlite_error_code(),
                    Some(rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked)
                ) =>
            {
                ValidationVerdict::Undetermined(format!("test contention: {e}"))
            }
            Err(_) => ValidationVerdict::Invalid,
        }
    }

    fn apply_block(
        &mut self,
        height: Height,
        block: &Block,
        tx: &mut rusqlite::Transaction<'_>,
    ) -> Result<(), ApplyError> {
        self.inner.apply_block(height, block, tx)
    }

    fn validator_set(&mut self, height: Height) -> HopNetValidatorSet {
        self.inner.validator_set(height)
    }

    fn on_decided(&mut self, _height: Height, _block: &Block, _cert: &WireCommitCertificate) {}
}

/// Deterministic one-transaction block for a (height, round, proposer).
pub fn build_block(
    height: Height,
    round: Round,
    proposer: i32,
    parent: Option<Blake3Hash>,
) -> Block {
    Block::new(BlockData {
        height: height.0,
        round: round.as_u32().unwrap_or(0),
        parent_hash: parent,
        transactions: Transactions(vec![hopnet_consensus::types::Transaction::new(
            "noop".into(),
            height.0.to_le_bytes().to_vec(),
            proposer,
            &key(proposer),
        )
        .unwrap()]),
    })
    .unwrap()
}

/// Decided (height, hash) rows straight from a DB file.
pub fn decided_heights(path: &PathBuf) -> Vec<(i64, Vec<u8>)> {
    let conn = rusqlite::Connection::open(path).unwrap();
    let mut stmt = conn
        .prepare("SELECT height, block_hash FROM decided_blocks ORDER BY height")
        .unwrap();
    let rows = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap();
    rows.collect::<Result<_, _>>().unwrap()
}
