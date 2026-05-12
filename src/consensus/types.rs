use super::*;
use crate::consensus::functions::process_transactions;
use crate::db::consensus as db;
use crate::db::types::MyNode;
use bincode::config;
use bincode::serde::encode_to_vec;
use blake3::Hasher;
use ed25519_dalek::Signature;
use hopnet_common::CustomUUID;
use rayon::prelude::*;
use rusqlite::{
    ToSql, TransactionBehavior, types::FromSql, types::FromSqlResult, types::ToSqlOutput,
    types::ValueRef,
};
use serde::{Deserialize, Serialize};
use std::ops::Deref;

/// BFT threshold for transitioning between relaxed and full BFT modes
pub const BFT_THRESHOLD: usize = 6;

/// Calculate the required quorum threshold based on validator count
///
/// For small networks (≤6 validators): simple majority (n/2 + 1)
/// For larger networks (7+ validators): BFT threshold (2n/3 + 1)
///
/// This function is critical for consensus safety and must produce correct
/// thresholds at all validator counts, especially at the boundary (6→7).
pub fn calculate_quorum_threshold(validator_count: usize) -> usize {
    if validator_count <= BFT_THRESHOLD {
        // Relaxed mode: simple majority (tolerates f crash faults where n = 2f+1)
        (validator_count / 2) + 1
    } else {
        // Full BFT mode: 2/3 majority (tolerates f Byzantine faults where n = 3f+1)
        ((validator_count * 2) / 3) + 1
    }
}

#[derive(Debug)]
pub enum ProgressionErrorKind {
    LockPhaseQcMismatch,
    ViewTooOld,
    ViewMismatch,
    AlreadyIssuedTimeout,
    DoubleVote,
    InvalidParent,
    PreparedBlockConflict,
    InvalidHeight,
}

#[derive(Debug)]
pub enum VoteError {
    DatabaseError,
    InitiatorError,
    ProcessingError,
    ProgressionError(ProgressionErrorKind),
    BlockError,
    TransactionValidationError(String),
}

#[derive(Debug)]
pub enum CertificateError {
    DatabaseError,
    SigningError,
    ValidationError,
    SignerNotFound,
    InsufficientVotes,
    NetworkTimeout,
}

#[derive(Debug)]
pub enum BlockError {
    EncodingError,
    DatabaseError,
    ValidationError,
}

#[derive(Clone, Copy, Serialize, Deserialize, Debug, PartialEq)]
pub enum ConsensusPhase {
    Propose,
    Lock,
}

impl ToSql for ConsensusPhase {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        let v: i32 = match self {
            ConsensusPhase::Propose => 0,
            ConsensusPhase::Lock => 1,
        };
        Ok(ToSqlOutput::from(v))
    }
}

impl FromSql for ConsensusPhase {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Integer(i) => match i as i32 {
                0 => Ok(ConsensusPhase::Propose),
                1 => Ok(ConsensusPhase::Lock),
                _ => Err(rusqlite::types::FromSqlError::InvalidType),
            },
            _ => Err(rusqlite::types::FromSqlError::InvalidType),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Ballot {
    // initiator of the vote, must be leader
    pub initiator: VoteSignMessage,

    // vote contents received for us to decide on
    pub data: VoteSignData,

    // associated block
    pub block: Block,
}

impl Ballot {
    pub fn propose<'a>(
        block: Block,
        phase: ConsensusPhase,
        me: &MyNode,
        tx: rusqlite::Transaction<'a>,
    ) -> Result<Ballot, VoteError> {
        // Create vote data from block and phase
        let data = VoteSignData::from_block(block.clone(), phase);

        // Create signature (same as follower does in sign())
        let signature = data
            .sign(&me.privkey)
            .map_err(|_| VoteError::ProcessingError)?;
        let initiator = VoteSignMessage {
            replica_id: me.node_id,
            signature,
        };

        // Bug #5 fix: Leader double-vote check (only for Propose phase)
        if data.phase == ConsensusPhase::Propose {
            // Use the transaction to read last vote (avoids race conditions)
            let last_vote_hash =
                db::get_last_propose_vote_tx(&tx).map_err(|_| VoteError::DatabaseError)?;

            if let Some(last_vote_hash) = last_vote_hash {
                if last_vote_hash != block.block_hash {
                    tracing::warn!(
                        "Leader double-vote attempt rejected: already proposed {:?}, rejecting proposal for {:?} in view {}",
                        last_vote_hash,
                        block.block_hash,
                        data.view
                    );
                    return Err(VoteError::ProgressionError(
                        ProgressionErrorKind::DoubleVote,
                    ));
                }
                // Same block = retry allowed
                tracing::debug!(
                    "Leader retry detected: already proposed {:?} in view {}, allowing re-proposal",
                    block.block_hash,
                    data.view
                );
            }

            // Record leader's vote in the provided transaction
            db::update_last_propose_vote_tx(&tx, block.block_hash)
                .map_err(|_| VoteError::DatabaseError)?;

            // Commit the vote immediately for double-vote protection
            crate::db::shared::commit_timed(tx).map_err(|_| VoteError::DatabaseError)?;
        }

        Ok(Ballot {
            initiator,
            data,
            block,
        })
    }

    pub fn verify_proposal(&self, state: &AppState) -> Result<(), VoteError> {
        // Get single DB connection for all queries (snapshot consistency + performance)
        let db_conn = state.db_pool.get().map_err(|_| VoteError::DatabaseError)?;

        // Check leader signature is valid and leader is authorized for view
        let consensus_state =
            db::get_consensus_with_conn(&db_conn).map_err(|_| VoteError::DatabaseError)?;

        let leader_verifyingkey = consensus_state.leader.pubkey;
        let message = self.data.encode().map_err(|_| VoteError::ProcessingError)?;
        match leader_verifyingkey.verify_strict(message.as_slice(), &self.initiator.signature) {
            Ok(_) => {
                // just to make sure it's the leader
                if self.initiator.replica_id != consensus_state.leader.node_id {
                    return Err(VoteError::InitiatorError);
                }
            }
            Err(_) => return Err(VoteError::InitiatorError),
        }

        // Check block hash is valid and actually matches the proposal's hash
        match self.block.verify() {
            Ok(_) => {
                if self.block.block_hash != self.data.block_hash {
                    return Err(VoteError::BlockError);
                }
            }
            Err(_) => return Err(VoteError::BlockError),
        }

        // Validate all transaction signatures in parallel
        if let Some(ref transactions) = self.block.data.transactions {
            // Batch fetch all pubkeys for signature validation (using shared connection)
            let node_pubkeys =
                db::get_all_node_pubkeys(&db_conn).map_err(|_| VoteError::DatabaseError)?;
            let user_pubkeys =
                db::get_all_user_pubkeys(&db_conn).map_err(|_| VoteError::DatabaseError)?;

            // Parallel signature verification using Rayon
            transactions
                .0
                .par_iter()
                .try_for_each(|tx| -> Result<(), VoteError> {
                    // Verify node signature
                    let node_pubkey = node_pubkeys.get(&tx.submitter.id).ok_or_else(|| {
                        VoteError::TransactionValidationError(format!(
                            "Unknown node_id: {}",
                            tx.submitter.id
                        ))
                    })?;
                    tx.verify_signature(node_pubkey).map_err(|_| {
                        VoteError::TransactionValidationError(format!(
                            "Invalid node signature for node_id: {}",
                            tx.submitter.id
                        ))
                    })?;

                    // Verify user signature if present
                    if let Some(ref user) = tx.user {
                        let user_pubkey = user_pubkeys.get(&user.id).ok_or_else(|| {
                            VoteError::TransactionValidationError(format!(
                                "Unknown user_id: {}",
                                user.id
                            ))
                        })?;
                        tx.verify_user_signature(user_pubkey).map_err(|_| {
                            VoteError::TransactionValidationError(format!(
                                "Invalid user signature for user_id: {}",
                                user.id
                            ))
                        })?;
                    }

                    Ok(())
                })?;
        }

        // 0. Double certificate check
        // Only vote on LOCK phase if we agree that PREPARE phase was quorum'd right before
        if self.data.phase == ConsensusPhase::Lock {
            if self.data.block_hash != consensus_state.highest_qc_block.block_hash {
                // we're locking a phase we didn't just get phase 1 QC for
                tracing::warn!(
                    "Lock phase ballot rejected: QC mismatch (ballot={:?}, highest_qc={:?})",
                    self.data.block_hash,
                    consensus_state.highest_qc_block.block_hash
                );
                return Err(VoteError::ProgressionError(
                    ProgressionErrorKind::LockPhaseQcMismatch,
                ));
            }
            // not checking view number matches original
            // logic: if crash after phase1 issuance, later view leader can request phase2 votes
            // if phase2 quorum received, later view can issue the QC2 for earlier block proposal
        } else if self.data.view <= consensus_state.highest_qc_block.data.view_number {
            // 1. View number progression
            // Reject proposals with views less than or equal to highest QC view seen
            // View number must go up with each successful proposal
            // One leader cannot make two proposals
            return Err(VoteError::ProgressionError(
                ProgressionErrorKind::ViewTooOld,
            ));
        }

        // View must equal our current view
        if self.data.view != consensus_state.view {
            tracing::warn!(
                "Ballot rejected: view mismatch (ballot view={}, our view={})",
                self.data.view,
                consensus_state.view
            );
            return Err(VoteError::ProgressionError(
                ProgressionErrorKind::ViewMismatch,
            ));
        }

        // Check if we've already issued a timeout vote for this view
        if consensus_state.last_timeout_vote_view == self.data.view {
            tracing::warn!(
                "Rejecting ballot for view {} - already issued timeout vote",
                self.data.view
            );
            return Err(VoteError::ProgressionError(
                ProgressionErrorKind::AlreadyIssuedTimeout,
            ));
        }

        // Bug #5 fix: Check for double-voting in Propose phase
        if self.data.phase == ConsensusPhase::Propose
            && let Some(last_vote_hash) = consensus_state.last_propose_vote_block_hash {
                if last_vote_hash != self.block.block_hash {
                    // Different block in same view = double-vote attempt
                    tracing::warn!(
                        "Double-vote attempt rejected: already voted for {:?}, rejecting vote for {:?} in view {}",
                        last_vote_hash,
                        self.block.block_hash,
                        self.data.view
                    );
                    return Err(VoteError::ProgressionError(
                        ProgressionErrorKind::DoubleVote,
                    ));
                }
                // Same block = retry allowed (idempotent, will regenerate same signature)
                tracing::debug!(
                    "Retry detected: already voted for {:?} in view {}, allowing re-vote",
                    self.block.block_hash,
                    self.data.view
                );
            }

        // 2. Chain validity check
        // Reject proposals that aren't listing tip of chain as parent
        match &self.block.data.parent_hash {
            Some(parent_hash) => {
                if parent_hash != &consensus_state.committed_block.block_hash {
                    tracing::warn!(
                        "Ballot rejected: invalid parent (expected={:?}, got={:?})",
                        consensus_state.committed_block.block_hash,
                        parent_hash
                    );
                    return Err(VoteError::ProgressionError(
                        ProgressionErrorKind::InvalidParent,
                    ));
                }
            }
            None => {
                return Err(VoteError::ProgressionError(
                    ProgressionErrorKind::InvalidParent,
                ));
            }
        }

        // 3. Preparation safety
        if self.data.phase == ConsensusPhase::Propose {
            // Propose phase: shouldn't have a prepared block at this height yet
            if consensus_state.prepared_block.is_some()
                && self.data.block_height == consensus_state.prepared_block.unwrap().data.height {
                    tracing::warn!(
                        "Ballot rejected: prepared block conflict (height={})",
                        self.data.block_height
                    );
                    return Err(VoteError::ProgressionError(
                        ProgressionErrorKind::PreparedBlockConflict,
                    ));
                }
        } else if self.data.phase == ConsensusPhase::Lock {
            // Lock phase: ballot must match the block we prepared
            if let Some(ref prepared) = consensus_state.prepared_block
                && self.data.block_hash != prepared.block_hash {
                    tracing::warn!(
                        "Lock phase ballot rejected: block mismatch (ballot={:?}, prepared={:?})",
                        self.data.block_hash,
                        prepared.block_hash
                    );
                    return Err(VoteError::ProgressionError(
                        ProgressionErrorKind::PreparedBlockConflict,
                    ));
                }
        }

        // 4. Height validation
        // need to increase by 1 height each time
        if self.data.block_height != consensus_state.committed_block.data.height + 1 {
            tracing::warn!(
                "Ballot rejected: invalid height (expected={}, got={})",
                consensus_state.committed_block.data.height + 1,
                self.data.block_height
            );
            return Err(VoteError::ProgressionError(
                ProgressionErrorKind::InvalidHeight,
            ));
        }

        // 5. Transaction validation (only for Propose phase)
        // Validate that all transactions can actually succeed before voting
        if self.data.phase == ConsensusPhase::Propose {
            // Create validation transaction
            let mut conn = state.db_pool.get().map_err(|_| VoteError::DatabaseError)?;
            let _wg = state.write_gate.guard();
            let db_tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(|_| VoteError::DatabaseError)?;

            if let Err(e) =
                process_transactions(&self.block.data.transactions, state, false, &db_tx)
            {
                let error_msg = format!("Transaction validation failed: {:?}", e);
                tracing::warn!("Refusing to vote for ballot - {}", error_msg);
                return Err(VoteError::TransactionValidationError(error_msg));
            }

            // Explicitly rollback validation transaction
            db_tx.rollback().map_err(|_| VoteError::DatabaseError)?;
            tracing::debug!("Transaction validation passed, changes rolled back");
        }

        Ok(())
    }

    pub fn sign(&self, app_state: &AppState) -> Result<VoteSignMessage, VoteError> {
        let node_id = app_state
            .get_node_id()
            .map_err(|_| VoteError::DatabaseError)?;
        let signature = self
            .data
            .sign(&app_state.private_key)
            .map_err(|_| VoteError::ProcessingError)?;

        // Bug #5 fix: Record Propose vote after signing to prevent double-voting
        if self.data.phase == ConsensusPhase::Propose {
            let _wg = app_state.write_gate.guard();
            db::update_last_propose_vote(app_state.db_pool.get(), self.block.block_hash)
                .map_err(|_| VoteError::DatabaseError)?;
        }

        Ok(VoteSignMessage {
            replica_id: node_id,
            signature,
        })
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VoteSignData {
    pub block_hash: Blake3Hash,
    pub block_height: i32,
    pub view: i32,
    pub phase: ConsensusPhase,
}

impl VoteSignData {
    pub fn encode(&self) -> Result<Vec<u8>, VoteError> {
        encode_to_vec(self, config::standard()).map_err(|_| VoteError::ProcessingError)
    }
    pub fn from_block(block: Block, phase: ConsensusPhase) -> VoteSignData {
        VoteSignData {
            block_hash: block.block_hash,
            block_height: block.data.height,
            view: block.data.view_number,
            phase,
        }
    }
    pub fn sign(&self, private_key: &PrivKey) -> Result<Signature, VoteError> {
        let data = &self.encode()?;
        let signature = private_key
            .try_sign(data)
            .map_err(|_| VoteError::ProcessingError)?;
        Ok(signature)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct VoteSignMessage {
    pub replica_id: i32,
    pub signature: Signature,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VoteSignMessages(pub Vec<VoteSignMessage>);

impl ToSql for VoteSignMessage {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>, rusqlite::Error> {
        // let's turn votesignmessage into Vec<u8>
        match bincode::serde::encode_to_vec(self, bincode::config::standard()) {
            Ok(data) => Ok(ToSqlOutput::Owned(rusqlite::types::Value::Blob(data))),
            Err(e) => Err(rusqlite::Error::ToSqlConversionFailure(Box::new(e))),
        }
    }
}

impl FromSql for VoteSignMessage {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Blob(b) => {
                match bincode::serde::decode_from_slice(b, bincode::config::standard()) {
                    Ok((data, _)) => Ok(data),
                    Err(_) => Err(rusqlite::types::FromSqlError::InvalidType),
                }
            }
            _ => Err(rusqlite::types::FromSqlError::InvalidType),
        }
    }
}

impl Deref for VoteSignMessages {
    type Target = Vec<VoteSignMessage>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ToSql for VoteSignMessages {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>, rusqlite::Error> {
        match bincode::serde::encode_to_vec(self, bincode::config::standard()) {
            Ok(data) => Ok(ToSqlOutput::Owned(rusqlite::types::Value::Blob(data))),
            Err(e) => Err(rusqlite::Error::ToSqlConversionFailure(Box::new(e))),
        }
    }
}

impl FromSql for VoteSignMessages {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Blob(b) => {
                match bincode::serde::decode_from_slice(b, bincode::config::standard()) {
                    Ok((data, _)) => Ok(data),
                    Err(_) => Err(rusqlite::types::FromSqlError::InvalidType),
                }
            }
            _ => Err(rusqlite::types::FromSqlError::InvalidType),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct QuorumCertificate {
    pub view_number: i32,
    pub phase: ConsensusPhase,
    pub block_hash: Blake3Hash,

    // Vote tracking
    pub proposer_signature: VoteSignMessage,
    pub voter_signatures: VoteSignMessages,
}

impl QuorumCertificate {
    /// Create a verified QC (production use - safe by default)
    pub async fn create(
        block: &Block,
        phase: ConsensusPhase,
        proposer_id: i32,
        proposer_key: &PrivKey,
        voter_signatures: Vec<VoteSignMessage>,
        validators: &[Node],
        app_state: &AppState,
    ) -> Result<QuorumCertificate, CertificateError> {
        // Layer 1: Leader abandonment - check if network is timing out
        let timeout_count = app_state
            .timeout_vote_collector
            .get_vote_count(block.data.view_number)
            .await;
        let quorum_threshold = calculate_quorum_threshold(validators.len());

        // If timeout votes >= quorum threshold, network is timing out - refuse to create QC
        if timeout_count >= quorum_threshold {
            tracing::warn!(
                "Layer 1: Leader abandonment - refusing to create {:?} QC for view {} ({} timeout votes >= {} threshold)",
                phase,
                block.data.view_number,
                timeout_count,
                quorum_threshold
            );
            return Err(CertificateError::NetworkTimeout);
        }

        let qc =
            Self::create_unverified(block, phase, proposer_id, proposer_key, voter_signatures)?;
        qc.verify(app_state, block)?;
        Ok(qc)
    }

    /// Create an unverified QC (genesis/tests only - explicit opt-out)
    pub fn create_unverified(
        block: &Block,
        phase: ConsensusPhase,
        proposer_id: i32,
        proposer_key: &PrivKey,
        voter_signatures: Vec<VoteSignMessage>,
    ) -> Result<QuorumCertificate, CertificateError> {
        // sign off ourselves
        let proposer_signature = VoteSignData::from_block(block.clone(), phase)
            .sign(proposer_key)
            .map_err(|_| CertificateError::SigningError)?;
        let proposer_signature_message = VoteSignMessage {
            replica_id: proposer_id,
            signature: proposer_signature,
        };
        // cast to VoteSignMessages
        let vsm = VoteSignMessages(voter_signatures);

        Ok(QuorumCertificate {
            view_number: block.data.view_number,
            phase,
            block_hash: block.block_hash,
            proposer_signature: proposer_signature_message,
            voter_signatures: vsm,
        })
    }
    pub fn verify(&self, state: &AppState, block: &Block) -> Result<(), CertificateError> {
        // Get current consensus state for view validation
        let consensus_state =
            db::get_consensus(state.db_pool.get()).map_err(|_| CertificateError::DatabaseError)?;

        // Validate view number - only accept current view
        if self.view_number != consensus_state.view {
            tracing::warn!(
                "QC verification failed: invalid view {} (current view: {})",
                self.view_number,
                consensus_state.view
            );
            return Err(CertificateError::ValidationError);
        }

        // Bug #8 fix: Lock QC requires preceding Propose QC
        // Ensures we don't accept Lock QC for a block we never saw prepared
        if self.phase == ConsensusPhase::Lock {
            match db::get_quorum_certificate_by_hash(
                state.db_pool.get(),
                &self.view_number,
                &self.block_hash,
                &ConsensusPhase::Propose,
            ) {
                Ok(_) => {
                    // Propose QC exists, safe to proceed with Lock QC
                    tracing::debug!(
                        "Lock QC validation passed: found Propose QC for view {} block {:?}",
                        self.view_number,
                        self.block_hash
                    );
                }
                Err(_) => {
                    tracing::warn!(
                        "Lock QC rejected: missing Propose QC for view {} block {:?}",
                        self.view_number,
                        self.block_hash
                    );
                    return Err(CertificateError::ValidationError);
                }
            }
        }

        // Get validators for committed height (parent block), not proposed block height
        // This ensures consistency with middleware activation checks and leader's validator selection
        let committed_height = consensus_state.committed_block.data.height;
        let validators = db::get_validators(state.db_pool.get(), committed_height)
            .map_err(|_| CertificateError::DatabaseError)?;
        let num_validators = validators.len();

        // Deduplicate voters: collect unique voter replica_ids (excluding proposer)
        let mut unique_voters: std::collections::HashMap<i32, &VoteSignMessage> =
            std::collections::HashMap::new();
        for voter_sig in &*self.voter_signatures {
            // Skip if this is the proposer trying to vote again (proposer already counted separately)
            if voter_sig.replica_id == self.proposer_signature.replica_id {
                continue;
            }
            // Insert only first occurrence of each replica_id (deduplicates)
            unique_voters
                .entry(voter_sig.replica_id)
                .or_insert(voter_sig);
        }

        // Filter to only votes from validators (prevents DoS from invalid votes)
        // Collect valid votes with their corresponding validator nodes
        let mut valid_voter_pairs: Vec<(&VoteSignMessage, &crate::types::Node)> = Vec::new();
        let mut invalid_vote_count = 0;

        for voter_sig in unique_voters.values() {
            if let Some(voter_node) = validators
                .iter()
                .find(|v| v.node_id == voter_sig.replica_id)
            {
                valid_voter_pairs.push((voter_sig, voter_node));
            } else {
                invalid_vote_count += 1;
                tracing::warn!(
                    "Ignoring vote from non-validator node {} in QC for view {} phase {:?}",
                    voter_sig.replica_id,
                    self.view_number,
                    self.phase
                );
            }
        }

        // Check we have enough valid signatures for quorum (dynamic threshold)
        let required_signatures = calculate_quorum_threshold(num_validators);
        let total_valid_signatures = 1 + valid_voter_pairs.len(); // proposer + valid voters

        if total_valid_signatures < required_signatures {
            tracing::warn!(
                "QC verification failed: insufficient valid signatures (got: {}, required: {}, validators: {}, invalid votes filtered: {})",
                total_valid_signatures,
                required_signatures,
                num_validators,
                invalid_vote_count
            );
            return Err(CertificateError::InsufficientVotes);
        }

        tracing::debug!(
            "Verifying QC for view {} phase {:?} with {} valid signatures from {} validators (filtered {} invalid votes)",
            self.view_number,
            self.phase,
            total_valid_signatures,
            num_validators,
            invalid_vote_count
        );

        // Prepare data for batch verification
        let vote_data = VoteSignData::from_block(block.clone(), self.phase);
        let message = vote_data
            .encode()
            .map_err(|_| CertificateError::ValidationError)?;

        // Collect all signatures and public keys for batch verification (using only valid voters)
        let mut signatures = Vec::new();
        let mut public_keys = Vec::new();
        let mut messages = Vec::new();

        // Add proposer signature
        signatures.push(self.proposer_signature.signature);

        // Find proposer's public key (proposer must be a validator)
        let proposer_node = validators
            .iter()
            .find(|v| v.node_id == self.proposer_signature.replica_id)
            .ok_or(CertificateError::SignerNotFound)?;
        let proposer_pubkey = proposer_node.pubkey;
        public_keys.push(*proposer_pubkey);
        messages.push(message.as_slice());

        // Add valid voter signatures only (already filtered to validators)
        for (voter_sig, voter_node) in valid_voter_pairs {
            signatures.push(voter_sig.signature);
            public_keys.push(*voter_node.pubkey);
            messages.push(message.as_slice());
        }

        // Perform batch verification
        match ed25519_dalek::verify_batch(&messages, &signatures, &public_keys) {
            Ok(_) => {
                // Additional validation: ensure block hash matches
                if self.block_hash != block.block_hash {
                    tracing::warn!(
                        "QC verification failed: block hash mismatch (qc: {:?}, block: {:?})",
                        self.block_hash,
                        block.block_hash
                    );
                    return Err(CertificateError::ValidationError);
                }

                // Ensure view number matches
                if self.view_number != block.data.view_number {
                    tracing::warn!(
                        "QC verification failed: view number mismatch (qc: {}, block: {})",
                        self.view_number,
                        block.data.view_number
                    );
                    return Err(CertificateError::ValidationError);
                }

                tracing::debug!(
                    "QC verified successfully for view {} phase {:?} block {:?}",
                    self.view_number,
                    self.phase,
                    self.block_hash
                );
                Ok(())
            }
            Err(_) => {
                tracing::warn!(
                    "QC verification failed: signature verification failed for view {} phase {:?}",
                    self.view_number,
                    self.phase
                );
                Err(CertificateError::ValidationError)
            }
        }
    }
}

// Timeout data structures following VoteSignMessage/Ballot pattern
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TimeoutSignData {
    pub view_number: i32,
    pub highest_qc_view: i32,
    pub highest_qc_phase: ConsensusPhase,
    pub highest_qc_hash: Blake3Hash,
}

impl TimeoutSignData {
    pub fn encode(&self) -> Result<Vec<u8>, VoteError> {
        encode_to_vec(self, config::standard()).map_err(|_| VoteError::ProcessingError)
    }

    pub fn sign(&self, private_key: &PrivKey) -> Result<Signature, VoteError> {
        let data = &self.encode()?;
        let signature = private_key
            .try_sign(data)
            .map_err(|_| VoteError::ProcessingError)?;
        Ok(signature)
    }

    pub fn from_consensus_state(
        view_number: i32,
        consensus_state: &ConsensusState,
    ) -> TimeoutSignData {
        TimeoutSignData {
            view_number,
            highest_qc_view: consensus_state.highest_qc_block.data.view_number,
            highest_qc_phase: consensus_state.highest_qc_phase,
            highest_qc_hash: consensus_state.highest_qc_block.block_hash,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TimeoutVote {
    // sender of the timeout vote (like Ballot.initiator)
    pub sender: VoteSignMessage,

    // timeout vote contents (like Ballot.data)
    pub data: TimeoutSignData,

    // Lock vote evidence: if this voter voted Lock for the same view,
    // carries the leader's and voter's Lock-phase signatures for QC reconstruction
    pub lock_vote_evidence: Option<LockVoteEvidence>,
}

/// Evidence that a voter participated in a Lock ballot for this view.
/// Carries independently-verifiable signatures (not covered by timeout signature).
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LockVoteEvidence {
    pub vote_data: VoteSignData, // Lock phase (block_hash, height, view, phase=Lock)
    pub proposer_signature: VoteSignMessage, // Leader's signature from Lock ballot
    pub voter_signature: VoteSignMessage, // This voter's Lock ballot response
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TimeoutCertificate {
    pub view_number: i32, // View that timed out
    pub highest_qc: QuorumCertificate,
    pub signatures: VoteSignMessages,
}

/// Result of timeout vote collection: either a TC (normal path) or a
/// reconstructed Lock QC (safety fix when Lock evidence reaches quorum).
#[derive(Debug, Clone)]
pub enum TimeoutResolution {
    TC(TimeoutCertificate),
    LockQC(QuorumCertificate),
}

impl TimeoutCertificate {
    pub fn create(
        timeout_votes: Vec<TimeoutVote>,
        app_state: &AppState,
    ) -> Result<TimeoutCertificate, CertificateError> {
        if timeout_votes.is_empty() {
            return Err(CertificateError::InsufficientVotes);
        }

        // Find majority timeout data with quorum validation for each view
        let (majority_timeout_data, valid_votes) =
            Self::find_majority_timeout_data_and_filter(timeout_votes, app_state)?;

        // Get the QC from the majority timeout data
        let highest_qc = db::get_quorum_certificate_by_hash(
            app_state.db_pool.get(),
            &majority_timeout_data.highest_qc_view,
            &majority_timeout_data.highest_qc_hash,
            &majority_timeout_data.highest_qc_phase,
        )
        .map_err(|_| CertificateError::DatabaseError)?;

        // Extract signatures from valid votes
        let signatures: Vec<VoteSignMessage> =
            valid_votes.into_iter().map(|vote| vote.sender).collect();

        Ok(TimeoutCertificate {
            view_number: majority_timeout_data.view_number,
            highest_qc,
            signatures: VoteSignMessages(signatures),
        })
    }

    pub fn verify(&self, app_state: &AppState) -> Result<(), CertificateError> {
        // Get current consensus state for view validation
        let consensus_state = db::get_consensus(app_state.db_pool.get())
            .map_err(|_| CertificateError::DatabaseError)?;

        // Validate view number - only accept current view (prevents out-of-order integration)
        if self.view_number != consensus_state.view {
            tracing::warn!(
                "TC verification failed: invalid view {} (current view: {})",
                self.view_number,
                consensus_state.view
            );
            return Err(CertificateError::ValidationError);
        }

        // Get validators for the height at this view (not raw view number)
        // This ensures correct threshold calculation when view diverges from height
        let mut conn = app_state
            .db_pool
            .get()
            .map_err(|_| CertificateError::DatabaseError)?;
        let tx = conn
            .transaction()
            .map_err(|_| CertificateError::DatabaseError)?;
        let height = db::get_height_at_view_tx(&tx, self.view_number)
            .map_err(|_| CertificateError::DatabaseError)?;
        drop(tx); // Release transaction before getting validators

        let validators = db::get_validators(app_state.db_pool.get(), height)
            .map_err(|_| CertificateError::DatabaseError)?;
        let num_validators = validators.len();

        // Filter to only votes from validators (prevents DoS from invalid votes)
        let mut valid_voter_pairs: Vec<(&VoteSignMessage, &crate::types::Node)> = Vec::new();
        let mut invalid_vote_count = 0;

        for vote_sig in &*self.signatures {
            if let Some(voter_node) = validators.iter().find(|v| v.node_id == vote_sig.replica_id) {
                valid_voter_pairs.push((vote_sig, voter_node));
            } else {
                invalid_vote_count += 1;
                tracing::warn!(
                    "Ignoring timeout vote from non-validator node {} in TC for view {}",
                    vote_sig.replica_id,
                    self.view_number
                );
            }
        }

        // Check we have enough valid signatures for quorum (dynamic threshold)
        let required_signatures = calculate_quorum_threshold(num_validators);
        let total_valid_signatures = valid_voter_pairs.len();

        if total_valid_signatures < required_signatures {
            tracing::warn!(
                "TC verification failed: insufficient valid signatures (got: {}, required: {}, validators: {}, invalid votes filtered: {})",
                total_valid_signatures,
                required_signatures,
                num_validators,
                invalid_vote_count
            );
            return Err(CertificateError::InsufficientVotes);
        }

        tracing::debug!(
            "Verifying TC for view {} with {} valid signatures from {} validators (filtered {} invalid votes)",
            self.view_number,
            total_valid_signatures,
            num_validators,
            invalid_vote_count
        );

        // Verify signatures on timeout data
        let timeout_data = TimeoutSignData {
            view_number: self.view_number,
            highest_qc_view: self.highest_qc.view_number,
            highest_qc_phase: self.highest_qc.phase,
            highest_qc_hash: self.highest_qc.block_hash,
        };
        let message = timeout_data
            .encode()
            .map_err(|_| CertificateError::ValidationError)?;

        // Collect signatures and public keys for batch verification (using only valid voters)
        let mut signatures = Vec::new();
        let mut public_keys = Vec::new();
        let mut messages = Vec::new();

        for (vote_sig, voter_node) in valid_voter_pairs {
            signatures.push(vote_sig.signature);
            public_keys.push(*voter_node.pubkey);
            messages.push(message.as_slice());
        }

        // Perform batch verification
        match ed25519_dalek::verify_batch(&messages, &signatures, &public_keys) {
            Ok(_) => Ok(()),
            Err(_) => Err(CertificateError::ValidationError),
        }
    }

    fn find_majority_timeout_data_and_filter(
        timeout_votes: Vec<TimeoutVote>,
        app_state: &AppState,
    ) -> Result<(TimeoutSignData, Vec<TimeoutVote>), CertificateError> {
        use std::collections::HashMap;

        let mut data_counts = HashMap::new();

        // Group votes by their timeout data hash
        for vote in &timeout_votes {
            let data_hash = vote
                .data
                .encode()
                .map_err(|_| CertificateError::ValidationError)?;
            data_counts
                .entry(data_hash)
                .or_insert(Vec::new())
                .push(vote);
        }

        // Check each group to see if it has sufficient quorum for its view
        let mut valid_groups = Vec::new();

        for (data_hash, votes) in data_counts {
            let view_number = votes[0].data.view_number;

            // Get validator count for the height at this view (not raw view number)
            // This ensures correct threshold calculation when view diverges from height
            let mut conn = app_state
                .db_pool
                .get()
                .map_err(|_| CertificateError::DatabaseError)?;
            let tx = conn
                .transaction()
                .map_err(|_| CertificateError::DatabaseError)?;
            let height = db::get_height_at_view_tx(&tx, view_number)
                .map_err(|_| CertificateError::DatabaseError)?;
            drop(tx); // Release transaction before getting validators

            let validators = db::get_validators(app_state.db_pool.get(), height)
                .map_err(|_| CertificateError::DatabaseError)?;
            let required_quorum = calculate_quorum_threshold(validators.len());

            // Check if this group has sufficient quorum
            if votes.len() >= required_quorum {
                valid_groups.push((data_hash, votes));
            }
        }

        // Find the group with the most votes among valid groups
        let (_, majority_votes) = valid_groups
            .into_iter()
            .max_by_key(|(_, votes)| votes.len())
            .ok_or(CertificateError::InsufficientVotes)?; // Fail if no group has quorum

        let majority_timeout_data = majority_votes[0].data.clone();
        let valid_votes = majority_votes.into_iter().cloned().collect();

        Ok((majority_timeout_data, valid_votes))
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Block {
    // hash of this block: db key
    pub block_hash: Blake3Hash,

    // computed based on these: db value
    pub data: BlockData,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BlockData {
    pub height: i32,
    pub view_number: i32,
    pub parent_hash: Option<Blake3Hash>,
    pub transactions: Option<Transactions>,
}

impl BlockData {
    pub fn encode(&self) -> Result<Vec<u8>, BlockError> {
        encode_to_vec(self, config::standard()).map_err(|_| BlockError::EncodingError)
    }

    pub fn compute_hash(&self) -> Result<Blake3Hash, BlockError> {
        let mut hasher = Hasher::new();
        let encoded_data = &self.encode()?;
        hasher.update(encoded_data.as_slice());
        let digest = Blake3Hash::new(hasher.finalize());
        Ok(digest)
    }
}

impl Block {
    pub fn new(data: BlockData) -> Result<Block, BlockError> {
        // compute hash over blockdata
        let digest = data.compute_hash()?;

        Ok(Block {
            block_hash: digest,
            data,
        })
    }

    pub fn new_tip(
        app_state: &AppState,
        transactions: Vec<Transaction>,
    ) -> Result<Block, BlockError> {
        // Get single DB connection for all queries (snapshot consistency + performance)
        let mut db_conn = app_state
            .db_pool
            .get()
            .map_err(|_| BlockError::DatabaseError)?;

        // Validate all transaction signatures before creating block (leader validation)
        if !transactions.is_empty() {
            // Batch fetch all pubkeys for signature validation
            let node_pubkeys =
                db::get_all_node_pubkeys(&db_conn).map_err(|_| BlockError::DatabaseError)?;
            let user_pubkeys =
                db::get_all_user_pubkeys(&db_conn).map_err(|_| BlockError::DatabaseError)?;

            // Parallel signature verification
            transactions
                .par_iter()
                .try_for_each(|tx| -> Result<(), BlockError> {
                    // Verify node signature
                    let node_pubkey = node_pubkeys
                        .get(&tx.submitter.id)
                        .ok_or(BlockError::ValidationError)?;
                    tx.verify_signature(node_pubkey)
                        .map_err(|_| BlockError::ValidationError)?;

                    // Verify user signature if present
                    if let Some(ref user) = tx.user {
                        let user_pubkey = user_pubkeys
                            .get(&user.id)
                            .ok_or(BlockError::ValidationError)?;
                        tx.verify_user_signature(user_pubkey)
                            .map_err(|_| BlockError::ValidationError)?;
                    }

                    Ok(())
                })?;
        }

        // Get the current tip (committed_block)
        let consensus_state =
            db::get_consensus_with_conn(&db_conn).map_err(|_| BlockError::DatabaseError)?;

        let height = consensus_state.committed_block.data.height + 1;
        let view = consensus_state.view;
        tracing::info!(
            "Creating new block at height {} for view {} with {} transactions",
            height,
            view,
            transactions.len()
        );

        let tip_data = BlockData {
            height,
            view_number: view,
            parent_hash: Some(consensus_state.committed_block.block_hash),
            transactions: Some(Transactions(transactions)),
        };
        let new_block = Block::new(tip_data)?;

        // Add block to DB (prepared_block_hash set later when Propose QC arrives)
        let _wg = app_state.write_gate.guard();
        db::insert_block_with_conn(&mut db_conn, &new_block)
            .map_err(|_| BlockError::DatabaseError)?;

        Ok(new_block)
    }

    pub fn verify(&self) -> Result<(), BlockError> {
        // compute hash and compare to self
        let digest = self.data.compute_hash()?;
        if digest != self.block_hash {
            return Err(BlockError::EncodingError);
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RpcCall {
    pub function: String,
    pub payload: Vec<u8>,
}

impl RpcCall {
    pub fn encode(&self) -> Result<Vec<u8>, TransactionError> {
        encode_to_vec(self, config::standard()).map_err(|_| TransactionError::EncodingError)
    }

    pub fn sign(&self, private_key: &PrivKey) -> Result<Signature, TransactionError> {
        let data = &self.encode()?;
        let signature = private_key
            .try_sign(data)
            .map_err(|_| TransactionError::SigningError)?;
        Ok(signature)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SignedIdentity {
    pub id: i32,
    pub signature: Signature,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Transaction {
    pub rpc: RpcCall,
    pub submitter: SignedIdentity, // Node that submitted this transaction
    pub user: Option<SignedIdentity>, // User who initiated this (if user operation)
    pub nonce: CustomUUID,         // UUIDv7 nonce for dedup (prevents stale resubmission)
}

impl Transaction {
    // Create a node-only transaction (automated operations)
    pub fn new(
        function: String,
        payload: Vec<u8>,
        submitter_id: i32,
        submitter_key: &PrivKey,
    ) -> Result<Self, TransactionError> {
        let rpc = RpcCall { function, payload };
        let signature = rpc.sign(submitter_key)?;

        Ok(Transaction {
            rpc,
            submitter: SignedIdentity {
                id: submitter_id,
                signature,
            },
            user: None,
            nonce: CustomUUID::new(None),
        })
    }

    // Create a user-initiated transaction
    pub fn new_with_user(
        function: String,
        payload: Vec<u8>,
        submitter_id: i32,
        submitter_key: &PrivKey,
        user_id: i32,
        user_key: &PrivKey,
    ) -> Result<Self, TransactionError> {
        let rpc = RpcCall { function, payload };
        let submitter_signature = rpc.sign(submitter_key)?;
        let user_signature = rpc.sign(user_key)?;

        Ok(Transaction {
            rpc,
            submitter: SignedIdentity {
                id: submitter_id,
                signature: submitter_signature,
            },
            user: Some(SignedIdentity {
                id: user_id,
                signature: user_signature,
            }),
            nonce: CustomUUID::new(None),
        })
    }

    pub fn verify_signature(&self, submitter_pubkey: &PubKey) -> Result<(), TransactionError> {
        let message = self.rpc.encode()?;
        submitter_pubkey
            .verify_strict(&message, &self.submitter.signature)
            .map_err(|_| TransactionError::InvalidSignature)
    }

    pub fn verify_user_signature(&self, user_pubkey: &PubKey) -> Result<(), TransactionError> {
        if let Some(user) = &self.user {
            let message = self.rpc.encode()?;
            user_pubkey
                .verify_strict(&message, &user.signature)
                .map_err(|_| TransactionError::InvalidSignature)
        } else {
            Err(TransactionError::InvalidSignature)
        }
    }
}

#[derive(Debug)]
pub enum TransactionError {
    SigningError,
    EncodingError,
    InvalidSignature,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Transactions(pub Vec<Transaction>);

impl Deref for Transactions {
    type Target = Vec<Transaction>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ToSql for Transactions {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>, rusqlite::Error> {
        // let's turn transactions into Vec<u8>
        match bincode::serde::encode_to_vec(self, bincode::config::standard()) {
            Ok(data) => Ok(ToSqlOutput::Owned(rusqlite::types::Value::Blob(data))),
            Err(e) => Err(rusqlite::Error::ToSqlConversionFailure(Box::new(e))),
        }
    }
}

impl FromSql for Transactions {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Blob(b) => {
                match bincode::serde::decode_from_slice(b, bincode::config::standard()) {
                    Ok((data, _)) => Ok(Transactions(data)),
                    Err(_) => Err(rusqlite::types::FromSqlError::InvalidType),
                }
            }
            _ => Err(rusqlite::types::FromSqlError::InvalidType),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ConsensusState {
    pub leader: Node,
    pub view: i32,
    pub phase: ConsensusPhase,
    pub prepared_block: Option<Block>,
    pub committed_block: Block,
    pub highest_qc_block: Block,
    pub highest_qc_phase: ConsensusPhase,
    pub last_timeout_vote_view: i32,
    pub last_propose_vote_block_hash: Option<Blake3Hash>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ViewConsensusData {
    pub view: i32,
    pub timeout_certificate: Option<TimeoutCertificate>,
    pub propose_qc: Option<QuorumCertificate>,
    pub lock_qc: Option<QuorumCertificate>,
    pub blocks: Vec<Block>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ViewCompletenessState {
    Complete,   // Has lock QC or TC (view finished, network moved on)
    InProgress, // Current view, may be incomplete (propose phase in progress)
}

/// Validate that view data is complete enough to integrate
/// Historical views must have lock QC or TC, current view can be incomplete
pub fn validate_view_completeness(
    view_data: &ViewConsensusData,
    target_view: i32,
) -> Result<ViewCompletenessState, super::functions::CatchUpError> {
    use super::functions::CatchUpError;

    // Genesis bypass - always trust view 0
    if view_data.view == 0 {
        return Ok(ViewCompletenessState::Complete);
    }

    // Check for lock QC or timeout certificate (view is complete)
    if view_data.lock_qc.is_some() || view_data.timeout_certificate.is_some() {
        return Ok(ViewCompletenessState::Complete);
    }

    // Historical view without QC or TC is invalid
    if view_data.view < target_view {
        tracing::warn!(
            "View {} is incomplete (no lock QC or TC) but is historical (target: {})",
            view_data.view,
            target_view
        );
        return Err(CatchUpError::ValidationFailed(view_data.view));
    }

    // Current view can be incomplete (propose phase in progress)
    if view_data.view == target_view {
        return Ok(ViewCompletenessState::InProgress);
    }

    // Future view - shouldn't happen
    tracing::error!(
        "View {} is in the future (target: {})",
        view_data.view,
        target_view
    );
    Err(CatchUpError::ValidationFailed(view_data.view))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quorum_threshold_single_node() {
        // 1 node network: need 1 vote (the node itself)
        assert_eq!(calculate_quorum_threshold(1), 1);
    }

    #[test]
    fn test_quorum_threshold_two_nodes() {
        // 2 nodes: need 2 votes (simple majority)
        assert_eq!(calculate_quorum_threshold(2), 2);
    }

    #[test]
    fn test_quorum_threshold_three_nodes() {
        // 3 nodes: need 2 votes (simple majority)
        assert_eq!(calculate_quorum_threshold(3), 2);
    }

    #[test]
    fn test_quorum_threshold_four_nodes() {
        // 4 nodes: need 3 votes (simple majority)
        assert_eq!(calculate_quorum_threshold(4), 3);
    }

    #[test]
    fn test_quorum_threshold_five_nodes() {
        // 5 nodes: need 3 votes (simple majority)
        assert_eq!(calculate_quorum_threshold(5), 3);
    }

    #[test]
    fn test_quorum_threshold_six_nodes_boundary() {
        // 6 nodes: last node in relaxed mode, need 4 votes (simple majority)
        assert_eq!(calculate_quorum_threshold(6), 4);
    }

    #[test]
    fn test_quorum_threshold_seven_nodes_bft_transition() {
        // 7 nodes: first node in BFT mode, need 5 votes (2/3 + 1)
        // (7 * 2) / 3 + 1 = 14 / 3 + 1 = 4 + 1 = 5
        assert_eq!(calculate_quorum_threshold(7), 5);
    }

    #[test]
    fn test_quorum_threshold_eight_nodes() {
        // 8 nodes: BFT mode, need 6 votes
        // (8 * 2) / 3 + 1 = 16 / 3 + 1 = 5 + 1 = 6
        assert_eq!(calculate_quorum_threshold(8), 6);
    }

    #[test]
    fn test_quorum_threshold_nine_nodes() {
        // 9 nodes: BFT mode, need 7 votes
        // (9 * 2) / 3 + 1 = 18 / 3 + 1 = 6 + 1 = 7
        assert_eq!(calculate_quorum_threshold(9), 7);
    }

    #[test]
    fn test_quorum_threshold_ten_nodes() {
        // 10 nodes: BFT mode, need 7 votes
        // (10 * 2) / 3 + 1 = 20 / 3 + 1 = 6 + 1 = 7
        assert_eq!(calculate_quorum_threshold(10), 7);
    }

    #[test]
    fn test_quorum_threshold_large_network() {
        // 100 nodes: BFT mode, need 67 votes
        // (100 * 2) / 3 + 1 = 200 / 3 + 1 = 66 + 1 = 67
        assert_eq!(calculate_quorum_threshold(100), 67);
    }

    #[test]
    fn test_fault_tolerance_progression_no_regression() {
        // Verify that adding validators never decreases fault tolerance
        let mut prev_tolerable = 0;

        for n in 1..=20 {
            let required = calculate_quorum_threshold(n);
            let tolerable = n - required;

            // Fault tolerance should never decrease
            assert!(
                tolerable >= prev_tolerable,
                "Fault tolerance regressed at n={}: can tolerate {} faults (previous: {})",
                n,
                tolerable,
                prev_tolerable
            );

            prev_tolerable = tolerable;
        }
    }

    #[test]
    fn test_boundary_transition_maintains_safety() {
        // At boundary (6→7), verify no regression in fault tolerance
        let threshold_6 = calculate_quorum_threshold(6);
        let threshold_7 = calculate_quorum_threshold(7);

        let tolerable_6 = 6 - threshold_6; // 6 - 4 = 2
        let tolerable_7 = 7 - threshold_7; // 7 - 5 = 2

        assert_eq!(tolerable_6, 2, "6 validators should tolerate 2 faults");
        assert_eq!(tolerable_7, 2, "7 validators should tolerate 2 faults");

        // No regression at boundary (lateral move)
        assert_eq!(
            tolerable_6, tolerable_7,
            "Boundary transition should maintain fault tolerance"
        );
    }

    #[test]
    fn test_relaxed_mode_simple_majority() {
        // Verify all relaxed mode nodes use simple majority
        for n in 1..=6 {
            let threshold = calculate_quorum_threshold(n);
            let expected = (n / 2) + 1;
            assert_eq!(
                threshold, expected,
                "Node count {} in relaxed mode should use simple majority",
                n
            );
        }
    }

    #[test]
    fn test_bft_mode_two_thirds() {
        // Verify all BFT mode nodes use 2/3 + 1
        for n in 7..=20 {
            let threshold = calculate_quorum_threshold(n);
            let expected = ((n * 2) / 3) + 1;
            assert_eq!(
                threshold, expected,
                "Node count {} in BFT mode should use 2/3 + 1",
                n
            );
        }
    }

    #[test]
    fn test_quorum_always_majority() {
        // Verify that quorum is always more than half
        for n in 1..=100 {
            let threshold = calculate_quorum_threshold(n);
            assert!(
                threshold > n / 2,
                "Quorum {} must be more than half of {} validators",
                threshold,
                n
            );
        }
    }

    #[test]
    fn test_bft_buffer_always_sufficient() {
        // Verify that BFT mode (7+) always has buffer ≥ 2
        for n in 7..=100 {
            let threshold = calculate_quorum_threshold(n);
            let buffer = n - threshold;
            assert!(
                buffer >= 2,
                "BFT mode with {} validators must have buffer ≥ 2, got {}",
                n,
                buffer
            );
        }
    }
}
