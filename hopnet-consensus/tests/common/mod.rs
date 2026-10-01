#![allow(dead_code)]
use std::path::PathBuf;

use ed25519_dalek::SigningKey;
use malachitebft_core_consensus::Params;
use malachitebft_core_types::{Round, ValuePayload};

use hopnet_consensus::codec::{WireCommitCertificate, WireWalEntry};
use hopnet_consensus::config::{MalachiteThresholds, QuorumProfile};
use hopnet_consensus::context::{Address, Height, HopNetContext, Validator};
use hopnet_consensus::store::{SqliteStorage, StoreError};
use hopnet_consensus::traits::{
    Application, ApplyError, Storage, ValidationOrigin, ValidationVerdict,
};
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
        .map_err(|e| ApplyError::permanent(e.to_string()))?;
        Ok(())
    }

    fn validator_set(&mut self, _height: Height) -> HopNetValidatorSet {
        self.valset.clone()
    }

    fn on_decided(&mut self, _height: Height, _block: &Block, _cert: &WireCommitCertificate) {}
}

/// SqlApp whose `apply_block` fails the next `failures` calls — transient
/// (SQLITE_BUSY-shaped) or permanent — before behaving like SqlApp. Injects
/// the decide-time failure classes without a second storage implementation:
/// the host sees them through `Storage::apply_error`, exactly as a handler's
/// error reaches it in production.
pub struct FlakyApp {
    pub inner: SqlApp,
    pub failures: u32,
    pub transient: bool,
    pub attempts: u32,
}

impl FlakyApp {
    pub fn new(valset: HopNetValidatorSet, failures: u32, transient: bool) -> Self {
        Self {
            inner: SqlApp { valset },
            failures,
            transient,
            attempts: 0,
        }
    }
}

impl Application<SqliteStorage> for FlakyApp {
    fn validate_block(
        &mut self,
        height: Height,
        block: &Block,
        tx: &mut rusqlite::Transaction<'_>,
        origin: ValidationOrigin,
    ) -> ValidationVerdict {
        <SqlApp as Application<SqliteStorage>>::validate_block(
            &mut self.inner,
            height,
            block,
            tx,
            origin,
        )
    }

    fn apply_block(
        &mut self,
        height: Height,
        block: &Block,
        tx: &mut rusqlite::Transaction<'_>,
    ) -> Result<(), ApplyError> {
        self.attempts += 1;
        if self.failures > 0 {
            self.failures -= 1;
            return Err(if self.transient {
                ApplyError::transient("injected: database is locked")
            } else {
                ApplyError::permanent("injected: handler refused the block")
            });
        }
        <SqlApp as Application<SqliteStorage>>::apply_block(&mut self.inner, height, block, tx)
    }

    fn validator_set(&mut self, height: Height) -> HopNetValidatorSet {
        <SqlApp as Application<SqliteStorage>>::validator_set(&mut self.inner, height)
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
    contended_db_with_busy_timeout(name, 5000)
}

/// `contended_db` with an explicit busy_timeout. A short one lets a test
/// make a WAL append or decide actually see SQLITE_BUSY from a write lock
/// held longer than the connection waits — the case the host's retry budget
/// exists for — without holding the lock for seconds of wall time.
pub fn contended_db_with_busy_timeout(name: &str, busy_ms: u32) -> (PathBuf, SqliteStorage) {
    let path = temp_db(name);
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute_batch(&format!(
        "PRAGMA journal_mode = WAL; PRAGMA busy_timeout = {busy_ms};
         CREATE TABLE IF NOT EXISTS victim_probe (id INTEGER);"
    ))
    .unwrap();
    (path, storage_from_conn(conn))
}

/// Shared tally of a `FlakyStorage`'s `wal_append` calls, readable after the
/// core has taken ownership of the storage.
pub type CallCount = std::rc::Rc<std::cell::Cell<u32>>;

/// SqliteStorage whose `wal_append` fails the next `failures` calls —
/// transient (SQLITE_BUSY-shaped) or permanent — before delegating. Injects
/// the WAL-append failure classes the host must retry or make fatal, without
/// a real lock holder; every other operation is SqliteStorage's own.
pub struct FlakyStorage {
    pub inner: SqliteStorage,
    pub failures: u32,
    pub transient: bool,
    pub wal_append_calls: CallCount,
}

impl FlakyStorage {
    pub fn new(inner: SqliteStorage, failures: u32, transient: bool) -> (Self, CallCount) {
        let calls = CallCount::default();
        (
            Self {
                inner,
                failures,
                transient,
                wal_append_calls: calls.clone(),
            },
            calls,
        )
    }
}

impl Storage for FlakyStorage {
    type Tx<'a> = rusqlite::Transaction<'a>;
    type Error = StoreError;

    fn wal_append(
        &mut self,
        height: Height,
        seq: u64,
        entry: &WireWalEntry,
    ) -> Result<(), StoreError> {
        self.wal_append_calls.set(self.wal_append_calls.get() + 1);
        if self.failures > 0 {
            self.failures -= 1;
            return Err(if self.transient {
                StoreError::ApplyTransient("injected: database is locked".into())
            } else {
                StoreError::Apply("injected: append refused".into())
            });
        }
        self.inner.wal_append(height, seq, entry)
    }

    fn wal_fetch(&mut self, height: Height) -> Result<Vec<WireWalEntry>, StoreError> {
        self.inner.wal_fetch(height)
    }

    fn wal_reset(&mut self) -> Result<(), StoreError> {
        self.inner.wal_reset()
    }

    fn decide_atomically<R>(
        &mut self,
        f: impl FnOnce(&mut Self::Tx<'_>) -> Result<R, StoreError>,
    ) -> Result<R, StoreError> {
        self.inner.decide_atomically(f)
    }

    fn with_rollback<R>(
        &mut self,
        f: impl FnOnce(&mut Self::Tx<'_>) -> R,
    ) -> Result<R, StoreError> {
        self.inner.with_rollback(f)
    }

    fn with_rollback_immediate<R>(
        &mut self,
        busy_timeout_ms: u32,
        f: impl FnOnce(&mut Self::Tx<'_>) -> R,
    ) -> Result<R, StoreError> {
        self.inner.with_rollback_immediate(busy_timeout_ms, f)
    }

    fn error_is_transient(e: &StoreError) -> bool {
        <SqliteStorage>::error_is_transient(e)
    }

    fn store_decided_tx(
        tx: &mut Self::Tx<'_>,
        block: &Block,
        cert: &WireCommitCertificate,
    ) -> Result<(), StoreError> {
        <SqliteStorage>::store_decided_tx(tx, block, cert)
    }

    fn truncate_wal_tx(tx: &mut Self::Tx<'_>, up_to: Height) -> Result<(), StoreError> {
        <SqliteStorage>::truncate_wal_tx(tx, up_to)
    }

    fn set_last_decided_tx(tx: &mut Self::Tx<'_>, height: Height) -> Result<(), StoreError> {
        <SqliteStorage>::set_last_decided_tx(tx, height)
    }

    fn last_decided(&mut self) -> Result<Option<Height>, StoreError> {
        self.inner.last_decided()
    }

    fn apply_error(e: ApplyError) -> StoreError {
        <SqliteStorage>::apply_error(e)
    }
}

/// SqlApp drives a `FlakyStorage` core unchanged: the transaction type is
/// SqliteStorage's own, so every method delegates to the SqliteStorage impl.
impl Application<FlakyStorage> for SqlApp {
    fn validate_block(
        &mut self,
        height: Height,
        block: &Block,
        tx: &mut rusqlite::Transaction<'_>,
        origin: ValidationOrigin,
    ) -> ValidationVerdict {
        <SqlApp as Application<SqliteStorage>>::validate_block(self, height, block, tx, origin)
    }

    fn apply_block(
        &mut self,
        height: Height,
        block: &Block,
        tx: &mut rusqlite::Transaction<'_>,
    ) -> Result<(), ApplyError> {
        <SqlApp as Application<SqliteStorage>>::apply_block(self, height, block, tx)
    }

    fn validator_set(&mut self, height: Height) -> HopNetValidatorSet {
        <SqlApp as Application<SqliteStorage>>::validator_set(self, height)
    }

    fn on_decided(&mut self, _height: Height, _block: &Block, _cert: &WireCommitCertificate) {}
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
        <SqlApp as Application<SqliteStorage>>::apply_block(&mut self.inner, height, block, tx)
    }

    fn validator_set(&mut self, height: Height) -> HopNetValidatorSet {
        <SqlApp as Application<SqliteStorage>>::validator_set(&mut self.inner, height)
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
