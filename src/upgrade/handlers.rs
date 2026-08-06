//! Consensus handler for `node_staged_version` (RFC-019 S3) — a
//! self-reported node attribute, like name/pubkey: node-signed, no user
//! signature, and NOT the evidence layer (that exists for subjective
//! observations; a staged version is the node's objective claim about
//! itself). Behavioral tests live in src/consensus/tests/upgrade.rs
//! beside the shared Mock helpers.

use hopnet_projection::{HandlerCtx, HandlerResult, TransactionHandler, TxMeta};

use crate::db::DatabaseError;
use crate::upgrade::NodeStagedVersion;

pub struct NodeStagedVersionHandler;

impl TransactionHandler for NodeStagedVersionHandler {
    fn name(&self) -> &'static str {
        "node_staged_version"
    }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        _execute: bool,
        _ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        let Ok((report, _)) = bincode::serde::decode_from_slice::<NodeStagedVersion, _>(
            tx.payload,
            bincode::config::standard(),
        ) else {
            return Err(DatabaseError::InvalidPayload);
        };

        // A node may only attest for itself — impersonation would
        // fabricate the S5 regenesis_start precondition.
        if report.node_id != tx.submitter_node {
            tracing::warn!(
                "Authorization failed: node {} attempted a version attestation for node {}",
                tx.submitter_node,
                report.node_id
            );
            return Err(DatabaseError::AuthorizationError);
        }

        // Validity before any write: only well-formed CalVer codes enter
        // committed state, and running counts as trivially staged so a
        // staged repeat is a malformed claim.
        if !crate::version::code_is_valid(report.running_code) {
            return Err(DatabaseError::InvalidPayload);
        }
        if let Some(staged) = report.staged_code
            && (!crate::version::code_is_valid(staged) || staged == report.running_code)
        {
            return Err(DatabaseError::InvalidPayload);
        }

        crate::db::versions::set_node_version_tx(db_tx, &report)
    }
}

inventory::submit! { &NodeStagedVersionHandler as &dyn TransactionHandler }
