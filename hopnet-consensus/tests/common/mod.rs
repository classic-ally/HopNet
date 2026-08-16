#![allow(dead_code)]
use std::path::PathBuf;

use ed25519_dalek::SigningKey;
use malachitebft_core_consensus::Params;
use malachitebft_core_types::{Round, Validity, ValuePayload};

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
