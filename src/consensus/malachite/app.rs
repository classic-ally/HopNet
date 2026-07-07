//! `HopNetApplication`: the application seam between the Malachite host and
//! HopNet's transaction dispatch, plus the proposer's value builder.
//!
//! Type bridging: `hopnet_consensus::types::Transaction` is the future
//! canonical of `crate::consensus::types::Transaction` — identical serde
//! shape (rpc / submitter / user / nonce), so conversion is a bincode
//! round-trip. The duplication dies at the Stage-5 swap when the main crate
//! re-points at the crate's types.

use std::collections::HashMap;
use std::ops::DerefMut;

use rusqlite::{Connection, OptionalExtension, TransactionBehavior};

use hopnet_consensus::codec::WireCommitCertificate;
use hopnet_consensus::context::{Height, HopNetValidatorSet, Validator};
use hopnet_consensus::store::SqliteStorage;
use hopnet_consensus::traits::{Application, ApplyError, ValidationOrigin};
use hopnet_consensus::types as engine;
use hopnet_consensus::{Round, Validity};

use crate::consensus::dispatch::{
    MAX_TRANSACTION_AGE, process_transaction, process_transactions,
};
use crate::consensus::types::Transactions as OldTransactions;
use crate::db::consensus as db;
use crate::AppState;
use crate::DISPATCH_TABLE;

// ---------------------------------------------------------------------------
// Type bridging

/// Engine → main-crate transactions (identical serde shape, bincode bridge).
pub fn to_old_transactions(txs: &engine::Transactions) -> Result<OldTransactions, String> {
    let bytes = bincode::serde::encode_to_vec(txs, bincode::config::standard())
        .map_err(|e| format!("encode: {e}"))?;
    bincode::serde::decode_from_slice(&bytes, bincode::config::standard())
        .map(|(v, _)| v)
        .map_err(|e| format!("decode: {e}"))
}

/// Main-crate → engine transactions.
pub fn to_engine_transactions(txs: &OldTransactions) -> Result<engine::Transactions, String> {
    let bytes = bincode::serde::encode_to_vec(txs, bincode::config::standard())
        .map_err(|e| format!("encode: {e}"))?;
    bincode::serde::decode_from_slice(&bytes, bincode::config::standard())
        .map(|(v, _)| v)
        .map_err(|e| format!("decode: {e}"))
}

// ---------------------------------------------------------------------------
// Application impl

/// The application seam. Deterministic by construction: every verdict is a
/// function of the block and committed DB state (the same on every node).
pub struct HopNetApplication {
    app_state: AppState,
    /// Dedicated connection for shell-thread reads (validator sets). The
    /// engine must NEVER compete for a pool checkout: validator_set runs on
    /// the shell thread, where a missed checkout is fatal (the shell aborts
    /// the process on panic — that was the image-11 crash under burst load).
    conn: r2d2::PooledConnection<r2d2_sqlite::SqliteConnectionManager>,
}

impl HopNetApplication {
    pub fn new(
        app_state: AppState,
        conn: r2d2::PooledConnection<r2d2_sqlite::SqliteConnectionManager>,
    ) -> Self {
        Self { app_state, conn }
    }

    /// Rule-8 validation: structural checks, parent linkage, per-transaction
    /// signatures (node + user), then the execute=false handler dry-run — the
    /// same checks the bespoke engine's Ballot verification performed.
    ///
    /// Time-dependent checks (nonce staleness, committed-nonce dedup) run
    /// only for LIVE proposals: a syncing node replaying old history would
    /// otherwise reject every block past the staleness window.
    fn validate_inner(
        &self,
        height: Height,
        block: &engine::Block,
        db_tx: &rusqlite::Transaction<'_>,
        origin: ValidationOrigin,
    ) -> Result<(), String> {
        block.verify().map_err(|e| format!("block hash: {e:?}"))?;
        if block.data.height != height.0 {
            return Err(format!(
                "height mismatch: block {} vs consensus {}",
                block.data.height, height.0
            ));
        }

        // Parent linkage: must extend the last decided block exactly.
        let last: Option<Vec<u8>> = db_tx
            .query_row(
                "SELECT block_hash FROM decided_blocks ORDER BY height DESC LIMIT 1",
                [],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| format!("parent lookup: {e}"))?;
        let parent = block.data.parent_hash.map(|h| h.as_bytes().to_vec());
        if parent != last {
            return Err("parent hash does not extend the decided chain".into());
        }

        // Per-transaction signature verification against the registered keys.
        let old_txs = to_old_transactions(&block.data.transactions)?;
        let node_keys = pubkeys(db_tx, "SELECT node_id, pubkey FROM nodes")?;
        let user_keys = pubkeys(db_tx, "SELECT user_id, pubkey FROM users")?;
        for tx in old_txs.0.iter() {
            let key = node_keys
                .get(&tx.submitter.id)
                .ok_or_else(|| format!("unknown submitter node {}", tx.submitter.id))?;
            tx.verify_signature(key)
                .map_err(|_| format!("bad node signature (node {})", tx.submitter.id))?;
            if let Some(ref user) = tx.user {
                let key = user_keys
                    .get(&user.id)
                    .ok_or_else(|| format!("unknown user {}", user.id))?;
                tx.verify_user_signature(key)
                    .map_err(|_| format!("bad user signature (user {})", user.id))?;
            }
        }

        // Time-dependent checks — live proposals only.
        if origin == ValidationOrigin::Live {
            let now = chrono::Utc::now();
            let mut dedup = db_tx
                .prepare_cached("SELECT 1 FROM committed_tx_nonces WHERE nonce = ?")
                .map_err(|e| e.to_string())?;
            for tx in old_txs.0.iter() {
                if let Some(created_at) = tx.nonce.extract_timestamp()
                    && now - created_at > MAX_TRANSACTION_AGE
                {
                    return Err(format!("stale transaction {}", tx.rpc.function));
                }
                let committed = dedup
                    .exists([tx.nonce.to_string()])
                    .map_err(|e| e.to_string())?;
                if committed {
                    return Err("already-committed nonce (leader replay)".into());
                }
            }
        }

        // Handler dry-run (execute=false) — deterministic, both origins.
        for tx in old_txs.0.iter() {
            process_transaction(tx, &self.app_state, false, db_tx)
                .map_err(|e| format!("handler validation ({}): {e:?}", tx.rpc.function))?;
        }
        Ok(())
    }
}

fn pubkeys(
    db_tx: &rusqlite::Transaction<'_>,
    sql: &str,
) -> Result<HashMap<i32, crate::db::PubKey>, String> {
    let mut stmt = db_tx.prepare_cached(sql).map_err(|e| e.to_string())?;
    let rows = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .map_err(|e| e.to_string())?;
    rows.collect::<Result<_, _>>().map_err(|e| e.to_string())
}

impl<C: DerefMut<Target = Connection> + 'static> Application<SqliteStorage<C>>
    for HopNetApplication
{
    fn validate_block(
        &mut self,
        height: Height,
        block: &engine::Block,
        tx: &mut rusqlite::Transaction<'_>,
        origin: ValidationOrigin,
    ) -> Validity {
        match self.validate_inner(height, block, tx, origin) {
            Ok(()) => Validity::Valid,
            Err(reason) => {
                tracing::warn!(height = height.0, "block rejected: {reason}");
                Validity::Invalid
            }
        }
    }

    fn apply_block(
        &mut self,
        height: Height,
        block: &engine::Block,
        tx: &mut rusqlite::Transaction<'_>,
    ) -> Result<(), ApplyError> {
        let old_txs = to_old_transactions(&block.data.transactions).map_err(ApplyError)?;
        // execute=true: dispatch-table application + nonce insertion, in the
        // host's decide transaction. Staleness/dedup checks are validation-
        // time only (execute skips them so sync can replay old blocks).
        process_transactions(&Some(old_txs), &self.app_state, true, tx)
            .map_err(|e| ApplyError(format!("apply at height {}: {e:?}", height.0)))
    }

    fn validator_set(&mut self, height: Height) -> HopNetValidatorSet {
        let h = i32::try_from(height.as_db()).unwrap_or(i32::MAX);
        let nodes = db::get_validators_with_conn(&self.conn, h).expect("validator query");
        HopNetValidatorSet::new(
            nodes
                .into_iter()
                .map(|n| Validator::new(n.node_id, engine::PubKey(n.pubkey.0)))
                .collect(),
        )
    }

    fn on_decided(&mut self, height: Height, block: &engine::Block, _cert: &WireCommitCertificate) {
        tracing::debug!(height = height.0, "block decided (malachite engine)");

        // Distribution kick (RFC-014): collect blob ids registered in this
        // block and push them to the global distribution queue. Runs on the
        // shell thread — NON-BLOCKING ONLY (unbounded send; no DB, no
        // awaits). Covers both drive envelopes, so modify content updates
        // distribute too.
        let Some(storage) = self.app_state.storage.get() else {
            return;
        };
        for tx in block.data.transactions.iter() {
            let blob_ids: Vec<hopnet_common::CustomUUID> = match tx.rpc.function.as_str() {
                "insert_files" => bincode::serde::decode_from_slice::<
                    crate::storage_host::handlers::DriveInsertPayload,
                    _,
                >(&tx.rpc.payload, bincode::config::standard())
                .map(|(p, _)| p.blob_ops.into_iter().map(|op| op.blob_id).collect())
                .unwrap_or_default(),
                "modify_item" => bincode::serde::decode_from_slice::<
                    crate::storage_host::handlers::ModifyItemPayload,
                    _,
                >(&tx.rpc.payload, bincode::config::standard())
                .map(|(p, _)| {
                    p.content_update
                        .and_then(|u| u.blob_op)
                        .map(|op| op.blob_id)
                        .into_iter()
                        .collect()
                })
                .unwrap_or_default(),
                _ => Vec::new(),
            };
            for blob_id in blob_ids {
                storage.notify_blob_committed(blob_id);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Value builder (proposer path)

/// Result of building a proposal value.
pub struct BuiltValue {
    pub block: engine::Block,
    /// (index into `candidates`, reason) for transactions dropped by preflight.
    pub rejected: Vec<(usize, String)>,
}

/// Build the proposer's block for (height, round) from candidate transactions:
/// committed-nonce dedup, then a SAVEPOINT preflight (execute=false, each tx
/// validating against cumulative prior state) under the write gate — the
/// bespoke leader path's logic — then parent linkage to the decided tip.
pub fn build_value(
    app_state: &AppState,
    conn: &mut r2d2::PooledConnection<r2d2_sqlite::SqliteConnectionManager>,
    height: Height,
    round: Round,
    candidates: Vec<crate::consensus::types::Transaction>,
) -> Result<BuiltValue, String> {
    let mut rejected = Vec::new();

    // Committed-nonce dedup: already-committed transactions are DONE, not
    // rejected — the caller resolves their notifiers as committed.
    let nonces: Vec<_> = candidates.iter().map(|t| t.nonce.clone()).collect();
    let committed = db::check_committed_nonces(conn, &nonces).unwrap_or_default();

    let mut survivors: Vec<(usize, &crate::consensus::types::Transaction)> = Vec::new();
    for (i, tx) in candidates.iter().enumerate() {
        if committed.contains(&tx.nonce.to_string()) {
            rejected.push((i, "already committed".into()));
        } else {
            survivors.push((i, tx));
        }
    }

    // SAVEPOINT preflight (dry run, rolled back wholesale).
    {
        let _wg = app_state.write_gate.guard();
        let db_tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|e| format!("preflight tx: {e}"))?;
        let mut failed = Vec::new();
        for (slot, (i, tx)) in survivors.iter().enumerate() {
            let sp = format!("preflight_{slot}");
            if db_tx.execute_batch(&format!("SAVEPOINT {sp}")).is_err() {
                failed.push((*i, "savepoint error".to_string()));
                continue;
            }
            let verdict = match DISPATCH_TABLE.get(tx.rpc.function.as_str()) {
                Some(handler) => {
                    // Seam boundary (RFC-015): same narrowing as
                    // dispatch::process_transaction.
                    let meta = crate::handlers::TxMeta {
                        function: &tx.rpc.function,
                        payload: &tx.rpc.payload,
                        submitter_node: tx.submitter.id,
                        user_id: tx.user.as_ref().map(|u| u.id),
                    };
                    let notifier = crate::handlers::HostNotifier {
                        test_mode: app_state.test_mode,
                    };
                    // execute=false here — the scheduler is never invoked
                    // during preflight; constructed for ctx uniformity.
                    let scheduler = crate::handlers::HostWorkScheduler {
                        app_state: app_state.clone(),
                    };
                    let ctx = crate::handlers::HandlerCtx {
                        fragments_dir: &app_state.fragments_dir,
                        node_id: app_state.node_id.get().copied(),
                        notifier: &notifier,
                        work: &scheduler,
                    };
                    handler
                        .process(&meta, false, &ctx, &db_tx)
                        .map_err(|e| format!("{e:?}"))
                }
                None => Err(format!("no handler: {}", tx.rpc.function)),
            };
            match verdict {
                Ok(()) => {
                    let _ = db_tx.execute_batch(&format!("RELEASE {sp}"));
                }
                Err(reason) => {
                    let _ = db_tx.execute_batch(&format!("ROLLBACK TO {sp}"));
                    let _ = db_tx.execute_batch(&format!("RELEASE {sp}"));
                    failed.push((*i, reason));
                }
            }
        }
        // db_tx drops here — the whole preflight rolls back.
        let failed_set: Vec<usize> = failed.iter().map(|(i, _)| *i).collect();
        rejected.extend(failed);
        survivors.retain(|(i, _)| !failed_set.contains(i));
    }

    // Parent = decided tip (None before the first decide / genesis).
    let parent: Option<Vec<u8>> = conn
        .query_row(
            "SELECT block_hash FROM decided_blocks ORDER BY height DESC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .optional()
        .map_err(|e| format!("tip lookup: {e}"))?;
    let parent_hash = match parent {
        Some(bytes) => Some(engine::Blake3Hash::from_bytes(
            bytes
                .as_slice()
                .try_into()
                .map_err(|_| "malformed tip hash".to_string())?,
        )),
        None => None,
    };

    let old = OldTransactions(survivors.into_iter().map(|(_, t)| t.clone()).collect());
    let transactions = to_engine_transactions(&old)?;
    let block = engine::Block::new(engine::BlockData {
        height: height.0,
        round: round.as_u32().unwrap_or(0),
        parent_hash,
        transactions,
    })
    .map_err(|e| format!("block build: {e:?}"))?;

    Ok(BuiltValue { block, rejected })
}
