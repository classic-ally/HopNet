//! Consensus handlers for the regenesis boundary (RFC-019 S5).
//!
//! Authorization is the membership-ops class (OQ2, v1 resolution): a
//! SEATED validator's node signature — no admin role exists in the
//! system and none is invented here; the human gate is the
//! JWT/RPC-authenticated route that triggers submission. Phase rules
//! run in BOTH the validate and apply passes so that two boundary txs
//! packed into one block die at apply against the first one's write.
//! Behavioral tests live in src/consensus/tests/regenesis.rs.

use hopnet_projection::{HandlerCtx, HandlerResult, TransactionHandler, TxMeta};

use crate::db::DatabaseError;
use crate::db::regenesis::{
    RegenesisPhase, clear_to_normal_tx, read_regenesis_state, set_moratorium_tx, set_sealed_tx,
};
use crate::regenesis::{RegenesisAbort, RegenesisCommit, RegenesisStart};

/// Seated-validator check shared by the boundary handlers.
fn require_seated(
    db_tx: &rusqlite::Transaction<'_>,
    submitter_node: i32,
) -> Result<(), DatabaseError> {
    let committed_height = crate::db::consensus::get_current_consensus_height(db_tx)?;
    if !crate::db::consensus::is_node_active(db_tx, submitter_node, committed_height)? {
        tracing::warn!(
            "Authorization failed: node {} is not seated for a regenesis boundary op",
            submitter_node
        );
        return Err(DatabaseError::AuthorizationError);
    }
    Ok(())
}

pub struct RegenesisStartHandler;

impl TransactionHandler for RegenesisStartHandler {
    fn name(&self) -> &'static str {
        "regenesis_start"
    }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        execute: bool,
        _ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        let Ok((req, _)) = bincode::serde::decode_from_slice::<RegenesisStart, _>(
            tx.payload,
            bincode::config::standard(),
        ) else {
            return Err(DatabaseError::InvalidPayload);
        };

        require_seated(db_tx, tx.submitter_node)?;

        if !crate::version::code_is_valid(req.target_version_code) {
            return Err(DatabaseError::InvalidPayload);
        }

        if read_regenesis_state(db_tx)?.phase != RegenesisPhase::Normal {
            return Err(DatabaseError::ProcessingError);
        }

        // The precondition (RFC-019): every SEATED validator has target
        // staged in committed state — running counts as trivially staged.
        // A mesh cannot decide a regenesis it visibly cannot complete.
        // (The v1 provider never stages, so only a same-version
        // housekeeping regenesis is startable today — recorded in the
        // S3/S5 landing notes.)
        let committed_height = crate::db::consensus::get_current_consensus_height(db_tx)?;
        let seated = hopnet_consensus::validators::get_validators(db_tx, committed_height)
            .map_err(|_| DatabaseError::RecallError)?;
        let versions: std::collections::HashMap<i32, (Option<u32>, Option<u32>)> =
            crate::db::versions::read_mesh_versions(db_tx)?
                .into_iter()
                .map(|v| (v.node_id, (v.running_code, v.staged_code)))
                .collect();
        for validator in &seated {
            let (running, staged) = versions
                .get(&validator.node_id)
                .copied()
                .unwrap_or((None, None));
            if running != Some(req.target_version_code) && staged != Some(req.target_version_code) {
                tracing::warn!(
                    "regenesis_start refused: seated validator {} has not staged target {}",
                    validator.node_id,
                    req.target_version_code
                );
                return Err(DatabaseError::ProcessingError);
            }
        }

        if !execute {
            return Ok(());
        }
        set_moratorium_tx(db_tx, req.target_version_code)
    }
}

inventory::submit! { &RegenesisStartHandler as &dyn TransactionHandler }

pub struct RegenesisAbortHandler;

impl TransactionHandler for RegenesisAbortHandler {
    fn name(&self) -> &'static str {
        "regenesis_abort"
    }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        execute: bool,
        _ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        let Ok((_req, _)) = bincode::serde::decode_from_slice::<RegenesisAbort, _>(
            tx.payload,
            bincode::config::standard(),
        ) else {
            return Err(DatabaseError::InvalidPayload);
        };

        require_seated(db_tx, tx.submitter_node)?;

        // The abort window is exactly (start decided, commit decided):
        // sealed is forward-only, normal has nothing to abort.
        if read_regenesis_state(db_tx)?.phase != RegenesisPhase::Moratorium {
            return Err(DatabaseError::ProcessingError);
        }

        if !execute {
            return Ok(());
        }
        clear_to_normal_tx(db_tx)
    }
}

inventory::submit! { &RegenesisAbortHandler as &dyn TransactionHandler }

pub struct RegenesisCommitHandler;

impl TransactionHandler for RegenesisCommitHandler {
    fn name(&self) -> &'static str {
        "regenesis_commit"
    }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        execute: bool,
        _ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        let Ok((req, _)) = bincode::serde::decode_from_slice::<RegenesisCommit, _>(
            tx.payload,
            bincode::config::standard(),
        ) else {
            return Err(DatabaseError::InvalidPayload);
        };

        require_seated(db_tx, tx.submitter_node)?;

        // Deterministic phase rule only. Everything subjective about the
        // commit — the vote-iff-match hash recompute, the own-pool drain
        // check, the seal_height ↔ block height binding, the solo-block
        // shape — is the vote-time layer's job (validate_inner): apply
        // must NEVER fail on it, because decided is decided.
        if read_regenesis_state(db_tx)?.phase != RegenesisPhase::Moratorium {
            return Err(DatabaseError::ProcessingError);
        }

        if !execute {
            return Ok(());
        }
        set_sealed_tx(db_tx, &req.snapshot_hash, req.seal_height)?;

        // Write the node-local marker in the SAME transaction as the
        // phase, so the two can never disagree. `consensus_meta` sits
        // outside the snapshot universe, so this is node-local state and
        // does not enter any exported or divergence hash — the same reason
        // the seal work was allowed to write it from a separate connection.
        //
        // Belt, not braces: `sealed_marker` derives H from the committed
        // row when this key is missing, which is what recovers nodes
        // stranded by the old ordering. This closes the window for FUTURE
        // seals rather than fixing past ones, and it is deliberately not
        // the only line of defence — a failure here still aborts the whole
        // decide (the handler's error rolls the block back), which is
        // preferable to a half-sealed epoch.
        hopnet_consensus::store::meta_put(
            db_tx,
            crate::regenesis::seal::META_SEALED_AT,
            &req.seal_height.to_be_bytes(),
        )
        .map_err(|e| {
            tracing::error!("sealing: marker write failed inside the decide: {e}");
            DatabaseError::InsertError
        })
    }
}

inventory::submit! { &RegenesisCommitHandler as &dyn TransactionHandler }
