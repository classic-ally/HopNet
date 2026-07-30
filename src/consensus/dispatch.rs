//! Transaction dispatch and signing — the app-facing seam that survives the
//! Malachite migration. Everything here is engine-agnostic: it maps signed
//! transactions onto the DISPATCH_TABLE handlers and builds signed
//! transactions for local submission. The bespoke-engine machinery in
//! `functions.rs` re-exports these for its remaining (dormant) call sites and
//! dies at Stage 5b.

use super::*;

use crate::DISPATCH_TABLE;
use crate::handlers::HandlerResult;
use ed25519_dalek::{SigningKey, VerifyingKey};
use rand::rand_core::UnwrapErr;
use rand::rngs::SysRng;

pub fn generate_ed25519_key() -> (SigningKey, VerifyingKey) {
    let mut csprng = UnwrapErr(SysRng);
    let private_key = SigningKey::generate(&mut csprng);
    let public_key = private_key.verifying_key();

    (private_key, public_key)
}

#[derive(Debug)]
pub enum ConsensusError {
    InsufficientVotes,
    BlockError,
    DatabaseError,
    SigningError,
    TimeoutError,
    MalformedReply,
    ThreadError,
    ForwardingError,
    NetworkError,
    NetworkTimeout,              // Network is timing out, leader should abandon
    TransactionRejected(String), // Business logic rejection (permanent)
}

// Create a node-only transaction (for automated operations)
pub fn create_signed_transaction(
    app_state: &AppState,
    function: String,
    payload: Vec<u8>,
) -> Result<Transaction, ConsensusError> {
    let node_id = app_state
        .get_node_id()
        .map_err(|_| ConsensusError::DatabaseError)?;
    Transaction::new(function, payload, node_id, &app_state.private_key)
        .map_err(|_| ConsensusError::SigningError)
}

// Create a user-initiated transaction (for user operations)
pub async fn create_signed_user_transaction(
    app_state: &AppState,
    function: String,
    payload: Vec<u8>,
    user_id: i32,
) -> Result<Transaction, ConsensusError> {
    let node_id = app_state
        .get_node_id()
        .map_err(|_| ConsensusError::DatabaseError)?;
    let session = app_state
        .get_session(user_id)
        .await
        .map_err(|_| ConsensusError::DatabaseError)?;

    Transaction::new_with_user(
        function,
        payload,
        node_id,
        &app_state.private_key,
        user_id,
        &session.user_keys.private_key,
    )
    .map_err(|_| ConsensusError::SigningError)
}

/// Maximum age for transaction nonces (50 minutes).
/// Must be strictly less than the nonce cleanup cutoff (1 hour) to ensure that
/// any transaction whose nonce was cleaned up is also caught by the staleness check.
/// The 10-minute gap provides clock skew tolerance across nodes.
pub const MAX_TRANSACTION_AGE: chrono::TimeDelta = chrono::TimeDelta::minutes(50);

pub fn process_transactions(
    transactions: &Option<Transactions>,
    app_state: &AppState,
    execute: bool,
    block_height: i32,
    db_tx: &rusqlite::Transaction,
) -> HandlerResult {
    if let Some(transactions) = transactions {
        // Validation path (ballot verification): check for replayed or stale transactions.
        // This runs on every follower before voting, preventing Byzantine leaders from
        // replaying already-committed transactions or transactions older than the cleanup window.
        // Skipped during execution (execute=true) because catch-up replays old blocks.
        if !execute {
            let now = chrono::Utc::now();

            // Staleness check: reject transactions with nonces older than MAX_TRANSACTION_AGE.
            // Catches replays of transactions whose nonces were already cleaned up.
            for tx in transactions.iter() {
                if let Some(created_at) = tx.nonce.extract_timestamp()
                    && now - created_at > MAX_TRANSACTION_AGE
                {
                    tracing::warn!(
                        "Rejecting stale transaction {} (nonce age: {:?}, max: {:?})",
                        tx.rpc.function,
                        now - created_at,
                        MAX_TRANSACTION_AGE
                    );
                    return Err(crate::db::DatabaseError::ProcessingError);
                }
            }

            // Nonce dedup check: reject blocks containing already-committed nonces.
            // Prevents Byzantine leader from including the same signed transaction twice.
            let nonces: Vec<_> = transactions.iter().map(|tx| tx.nonce.clone()).collect();
            if let Ok(conn) = app_state.db_pool.get()
                && let Ok(committed) = crate::db::consensus::check_committed_nonces(&conn, &nonces)
                && !committed.is_empty()
            {
                tracing::warn!(
                    "Rejecting block with {} already-committed nonce(s) — possible leader replay attack",
                    committed.len()
                );
                return Err(crate::db::DatabaseError::ProcessingError);
            }
        }

        let mut nonces = Vec::new();
        for tx in transactions.iter() {
            match process_transaction(tx, app_state, execute, block_height, db_tx) {
                Ok(_) => {
                    tracing::debug!(
                        "Transaction {} successfully: {}",
                        if execute { "processed" } else { "validated" },
                        &tx.rpc.function
                    );
                    if execute {
                        nonces.push(tx.nonce.clone());
                    }
                }
                Err(e) => {
                    // Both validation and execution phases return error immediately
                    // Transaction auto-rolls back when db_tx is dropped
                    tracing::error!(
                        "Failed to {} transaction {}: {:?}",
                        if execute { "process" } else { "validate" },
                        &tx.rpc.function,
                        e
                    );
                    return Err(e);
                }
            }
        }
        // Insert nonces atomically with block commit (all nodes do this)
        if execute && !nonces.is_empty() {
            crate::db::consensus::insert_tx_nonces_tx(db_tx, &nonces).map_err(|e| {
                tracing::error!("Failed to insert transaction nonces: {:?}", e);
                crate::db::DatabaseError::InsertError
            })?;
        }
    }
    Ok(())
}

pub fn process_transaction(
    tx: &Transaction,
    app_state: &AppState,
    execute: bool,
    block_height: i32,
    db_tx: &rusqlite::Transaction,
) -> HandlerResult {
    if let Some(handler) = DISPATCH_TABLE.get(tx.rpc.function.as_str()) {
        // Seam boundary (RFC-015): handlers get a NARROWED view — the
        // signature-verified identities and payload, plus the minimal host
        // slice. AppState and the full Transaction never cross.
        let meta = crate::handlers::TxMeta {
            function: &tx.rpc.function,
            payload: &tx.rpc.payload,
            submitter_node: tx.submitter.id,
            user_id: tx.user.as_ref().map(|u| u.id),
        };
        let notifier = crate::handlers::HostNotifier {
            test_mode: app_state.test_mode,
            change_tx: app_state.change_tx.clone(),
        };
        let scheduler = crate::handlers::HostWorkScheduler {
            app_state: app_state.clone(),
        };
        let ctx = crate::handlers::HandlerCtx {
            fragments_dir: &app_state.fragments_dir,
            node_id: app_state.node_id.get().copied(),
            height: block_height,
            notifier: &notifier,
            work: &scheduler,
        };
        handler.process(&meta, execute, &ctx, db_tx)
    } else {
        tracing::warn!("No handler found for function: {}", &tx.rpc.function);
        Err(crate::db::DatabaseError::InvalidPayload)
    }
}
