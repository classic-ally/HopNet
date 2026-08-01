use crate::{
    db::{
        DatabaseError,
        consensus::{activate_validator, get_current_consensus_height},
        nodes::insert_node_tx,
        setup::initialize_sequences_tx,
        users::insert_user_tx,
    },
    handlers::{HandlerCtx, HandlerResult, TransactionHandler, TxMeta},
    types::{Node, User},
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ActivationRequest {
    /// Strictly increasing candidate node ids — a PROPOSER-signed batch
    /// (RFC-CONSENSUS-002 S5, mesh-initiated seating: a seated validator
    /// proposes; nodes never request seats). The catch-up proof moved
    /// evidence-side: each approver checks its own last_known_height for
    /// every member (membership_guards::check_activation, Live only).
    pub members: Vec<i32>,
}

pub struct ValidatorActivationHandler;

impl TransactionHandler for ValidatorActivationHandler {
    fn name(&self) -> &'static str {
        "validator_activation"
    }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        execute: bool,
        _ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        let (req, _) = bincode::serde::decode_from_slice::<ActivationRequest, _>(
            tx.payload,
            bincode::config::standard(),
        )
        .map_err(|_| DatabaseError::InvalidPayload)?;

        // Objective shape checks, both phases: non-empty, within the batch
        // licence, strictly increasing (sorted + deduped — determinism).
        if req.members.is_empty()
            || req.members.len() > hopnet_consensus::membership::B_MAX
            || !req.members.windows(2).all(|w| w[0] < w[1])
        {
            return Err(DatabaseError::InvalidPayload);
        }

        let committed_height = get_current_consensus_height(db_tx)?;

        if !execute {
            // Mesh-initiated: the SUBMITTER must be seated (this also
            // structurally excludes submitter ∈ members — members must be
            // unseated below).
            if !crate::db::consensus::is_node_active(db_tx, tx.submitter_node, committed_height)? {
                return Err(DatabaseError::AuthorizationError);
            }
            for m in &req.members {
                // Registered…
                let registered: bool = db_tx
                    .query_row(
                        "SELECT 1 FROM nodes WHERE node_id = ?",
                        rusqlite::params![m],
                        |_| Ok(true),
                    )
                    .unwrap_or(false);
                if !registered {
                    return Err(DatabaseError::ProcessingError);
                }
                // …and not currently seated (overlapping/duplicate batches
                // die here, whole-batch).
                if crate::db::consensus::is_node_active(db_tx, *m, committed_height)? {
                    return Err(DatabaseError::ProcessingError);
                }
            }
            // The subjective batch checks — joint posture, proven-quorum
            // ceiling, per-member S_min/liveness/catch-up — run HOST-side
            // (membership_guards), Live origin only.
            return Ok(());
        }

        for m in &req.members {
            activate_validator(db_tx, *m, committed_height)?;
        }
        tracing::info!(
            "Batch-activated {:?} at effective height {} (proposed by {})",
            req.members,
            committed_height,
            tx.submitter_node
        );
        Ok(())
    }
}

inventory::submit! {
    &ValidatorActivationHandler as &dyn TransactionHandler
}

// ============================================================================
// Membership-transition registry (solo-block rule keys off this)
// ============================================================================

/// Function names that mutate the validator set. `build_value` and
/// `validate_inner` enforce at most one per block, riding alone
/// (RFC-CONSENSUS-002: joint constraints — one-removal-per-height, joint
/// leave safety — are invisible to per-tx validation, so the block shape
/// carries them).
pub const MEMBERSHIP_TX_FUNCTIONS: &[&str] = &[
    "validator_activation",
    "validator_leave",
    "validator_vote_out",
];

pub fn is_membership_tx(function: &str) -> bool {
    MEMBERSHIP_TX_FUNCTIONS.contains(&function)
}

// ============================================================================
// Voluntary leave (RFC-CONSENSUS-002 S1) — the departure twin of activation
// ============================================================================

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LeaveRequest {
    pub node_id: i32,
}

pub struct ValidatorLeaveHandler;

impl TransactionHandler for ValidatorLeaveHandler {
    fn name(&self) -> &'static str {
        "validator_leave"
    }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        execute: bool,
        _ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        let (req, _) = bincode::serde::decode_from_slice::<LeaveRequest, _>(
            tx.payload,
            bincode::config::standard(),
        )
        .map_err(|_| DatabaseError::InvalidPayload)?;

        // Authorization (objective, both phases): only self-leave — the
        // departing node's signature is the consent (spec: Departure
        // classes, LEAVE).
        if req.node_id != tx.submitter_node {
            tracing::warn!(
                "Authorization failed: node {} attempted to remove node {}",
                tx.submitter_node,
                req.node_id
            );
            return Err(DatabaseError::AuthorizationError);
        }

        let committed_height = get_current_consensus_height(db_tx)?;

        if !execute {
            // Target must currently be seated.
            if !crate::db::consensus::is_node_active(db_tx, req.node_id, committed_height)? {
                tracing::warn!("leave refused: node {} is not active", req.node_id);
                return Err(DatabaseError::ProcessingError);
            }

            // Objective floor (both origins): INV-FLOOR — the set never
            // empties. (S1's interim quorum clause was vacuous:
            // v−1 < quorum(v−1) is false at every v.) The real leave-safety
            // guard — survivors-I-see-live ≥ quorum(v−1) — is subjective
            // and lives in membership_guards::check_leave, Live-origin only.
            let v = hopnet_consensus::validators::count_active_validators(db_tx, committed_height)
                .map_err(|_| DatabaseError::RecallError)?;
            if v < 2 {
                tracing::warn!("leave refused for node {}: set floor (v={v})", req.node_id);
                return Err(DatabaseError::ProcessingError);
            }
            return Ok(());
        }

        // Execute: deterministic deactivation at the committed height —
        // the same effective-height computation as activation.
        crate::db::consensus::deactivate_validator(
            db_tx,
            req.node_id,
            committed_height,
            crate::db::consensus::DepartureKind::Voluntary,
        )?;
        tracing::info!(
            "Node {} voluntarily left the validator set at height {}",
            req.node_id,
            committed_height
        );
        Ok(())
    }
}

inventory::submit! {
    &ValidatorLeaveHandler as &dyn TransactionHandler
}

// ============================================================================
// Unreachability vote-out (RFC-CONSENSUS-002 S4). OBJECTIVE checks only —
// the subjective dark(target) attestation lives host-side in
// membership_guards (Live origin only), so this handler is replayable at
// ValidationOrigin::Sync forever.
// ============================================================================

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VoteOutRequest {
    /// The dark target (never the submitter — self-removal is leave's job).
    pub node_id: i32,
}

pub struct ValidatorVoteOutHandler;

impl TransactionHandler for ValidatorVoteOutHandler {
    fn name(&self) -> &'static str {
        "validator_vote_out"
    }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        execute: bool,
        _ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        let (req, _) = bincode::serde::decode_from_slice::<VoteOutRequest, _>(
            tx.payload,
            bincode::config::standard(),
        )
        .map_err(|_| DatabaseError::InvalidPayload)?;

        // Objective, both phases: self-removal is leave's job.
        if req.node_id == tx.submitter_node {
            return Err(DatabaseError::AuthorizationError);
        }

        let committed_height = get_current_consensus_height(db_tx)?;

        if !execute {
            // Objective (deterministic over committed state — replay-safe):
            // target seated (duplicate proposals die here), submitter
            // seated (only validators propose removals; TxMeta's submitter
            // is the signature-verified node identity).
            if !crate::db::consensus::is_node_active(db_tx, req.node_id, committed_height)? {
                return Err(DatabaseError::ProcessingError);
            }
            if !crate::db::consensus::is_node_active(db_tx, tx.submitter_node, committed_height)? {
                return Err(DatabaseError::AuthorizationError);
            }
            return Ok(());
        }

        crate::db::consensus::deactivate_validator(
            db_tx,
            req.node_id,
            committed_height,
            crate::db::consensus::DepartureKind::VotedOut,
        )?;
        tracing::info!(
            "Node {} voted out of the validator set at height {} (proposed by {})",
            req.node_id,
            committed_height,
            tx.submitter_node
        );
        Ok(())
    }
}

inventory::submit! {
    &ValidatorVoteOutHandler as &dyn TransactionHandler
}

// ============================================================================
// Genesis Handler - Creates initial network state from genesis transaction
// ============================================================================
//
// The genesis handler initializes a new HopNet network by processing the
// genesis transaction embedded in the genesis block (height 0). This handler
// is called in two contexts:
//
// 1. Initial network creation (post_initial_setup):
//    - Creates genesis transaction with initial user and node
//    - Processes it to initialize sequences and create first validator
//
// 2. New node bootstrap (catch-up):
//    - New nodes replay the genesis transaction from the genesis block
//    - Builds identical initial state without separate checkpoint sync
//
// Security: Genesis handler validates it's only called once (checks sequences
// table is empty). Attempting to process genesis on initialized database fails.

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GenesisPayload {
    pub user: User,
    pub node: Node,
    /// Mesh-wide X25519 pubkey (RFC-014 all-users access primitive).
    pub mesh_pubkey: [u8; 32],
    /// Mesh privkey wrapped to user 0's pubkey — in the payload (not
    /// recomputed) so joining nodes reproduce it via deterministic replay.
    pub mesh_grant: hopnet_storage::MeshKeyGrant,
    /// Mesh storage policy seed rows (RFC-STORAGE-002 Configuration):
    /// key/value pairs for `hopnet_storage_policy`. Empty = code defaults.
    pub storage_policy: Vec<(String, String)>,
    /// Consensus membership policy seed rows (RFC-CONSENSUS-002
    /// Configuration): key/value pairs for `hopnet_consensus_policy`.
    /// Empty = code defaults.
    pub consensus_policy: Vec<(String, String)>,
}

pub struct InsertGenesisHandler;

impl TransactionHandler for InsertGenesisHandler {
    fn name(&self) -> &'static str {
        "insert_genesis"
    }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        execute: bool,
        _ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        tracing::debug!("InsertGenesisHandler: Starting (execute={})", execute);

        // Decode genesis payload
        let (genesis_data, _) = bincode::serde::decode_from_slice::<GenesisPayload, _>(
            tx.payload,
            bincode::config::standard(),
        )
        .map_err(|e| {
            tracing::error!(
                "InsertGenesisHandler: Failed to decode genesis payload: {:?}",
                e
            );
            DatabaseError::InvalidPayload
        })?;
        tracing::debug!("InsertGenesisHandler: Decoded genesis payload");

        // Safety check: Only allow genesis handler at height 0
        // Check if sequences already exist - if so, genesis already processed
        // Fail if query fails (don't proceed with unknown database state)
        tracing::debug!("InsertGenesisHandler: Checking if sequences exist");
        let existing_sequences: i32 = db_tx
            .query_row("SELECT COUNT(*) FROM sequences", [], |row| row.get(0))
            .map_err(|e| {
                tracing::error!("InsertGenesisHandler: Failed to query sequences: {:?}", e);
                DatabaseError::RecallError
            })?;
        tracing::debug!(
            "InsertGenesisHandler: Found {} existing sequences",
            existing_sequences
        );

        if existing_sequences > 0 {
            tracing::error!(
                "insert_genesis called on already-initialized database (sequences exist: {})",
                existing_sequences
            );
            return Err(DatabaseError::ProcessingError);
        }

        // === ALL LOGIC OUTSIDE EXECUTE FLAG ===
        // Genesis has no validation - it's the fiat root of trust

        // 1. Initialize sequences
        tracing::debug!("InsertGenesisHandler: Initializing sequences");
        initialize_sequences_tx(db_tx).map_err(|e| {
            tracing::error!(
                "InsertGenesisHandler: Failed to initialize sequences: {:?}",
                e
            );
            e
        })?;
        tracing::debug!("InsertGenesisHandler: Initialized sequences");

        // 2. Insert user (returns user_id=0)
        tracing::debug!("InsertGenesisHandler: Inserting user");
        let user_id = insert_user_tx(db_tx, genesis_data.user).map_err(|e| {
            tracing::error!("InsertGenesisHandler: Failed to insert user: {:?}", e);
            e
        })?;
        tracing::debug!("InsertGenesisHandler: Inserted user with id={}", user_id);

        // 3. Insert node (returns node_id=0)
        tracing::debug!("InsertGenesisHandler: Inserting node");
        let node_id = insert_node_tx(db_tx, genesis_data.node).map_err(|e| {
            tracing::error!("InsertGenesisHandler: Failed to insert node: {:?}", e);
            e
        })?;
        tracing::debug!("InsertGenesisHandler: Inserted node with id={}", node_id);

        // 4. Activate genesis validator (special case - only for genesis)
        // Uses same activate_validator function as normal activation for consistency
        tracing::debug!("InsertGenesisHandler: Activating validator");
        activate_validator(db_tx, node_id, 0).map_err(|e| {
            tracing::error!(
                "InsertGenesisHandler: Failed to activate validator: {:?}",
                e
            );
            e
        })?;
        tracing::debug!("InsertGenesisHandler: Activated validator");

        // 5. Install the mesh keypair: public key + user-0 grant
        crate::db::mesh::insert_mesh_key_tx(db_tx, &genesis_data.mesh_pubkey)?;
        crate::db::mesh::insert_mesh_grant_tx(db_tx, &genesis_data.mesh_grant)?;
        tracing::debug!("InsertGenesisHandler: Installed mesh keypair + genesis grant");

        // 6. Seed the mesh storage policy (absent keys resolve to code
        // defaults; see hopnet_storage::membership::StoragePolicy).
        hopnet_storage::store::apply_policy_rows(db_tx, &genesis_data.storage_policy).map_err(
            |e| {
                tracing::error!("InsertGenesisHandler: Failed to seed storage policy: {e}");
                DatabaseError::ProcessingError
            },
        )?;

        // 7. Seed the consensus membership policy (absent keys resolve to
        // code defaults; see hopnet_consensus::membership::ConsensusPolicy).
        hopnet_consensus::store::apply_policy_rows(db_tx, &genesis_data.consensus_policy).map_err(
            |e| {
                tracing::error!("InsertGenesisHandler: Failed to seed consensus policy: {e}");
                DatabaseError::ProcessingError
            },
        )?;

        // === EXECUTION PHASE ===
        if execute {
            tracing::info!(
                "Genesis initialized: user_id={}, node_id={} (validator active at height 0)",
                user_id,
                node_id
            );
        } else {
            // Validation phase - genesis always valid
            tracing::debug!("Genesis handler validated successfully");
        }

        tracing::debug!("InsertGenesisHandler: Completed successfully");
        Ok(())
    }
}

inventory::submit! {
    &InsertGenesisHandler as &dyn TransactionHandler
}

// ============================================================================
// Nonce Cleanup Handler - Consensus-tracked cleanup of committed_tx_nonces
// ============================================================================

pub struct CleanupNoncesHandler;

impl TransactionHandler for CleanupNoncesHandler {
    fn name(&self) -> &'static str {
        "system.cleanup_nonces"
    }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        execute: bool,
        _ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        let (cutoff, _) = bincode::serde::decode_from_slice::<hopnet_common::CustomUUID, _>(
            tx.payload,
            bincode::config::standard(),
        )
        .map_err(|_| DatabaseError::InvalidPayload)?;

        if !execute {
            return Ok(());
        }

        let deleted = crate::db::consensus::cleanup_old_nonces(db_tx, &cutoff)?;
        tracing::debug!(
            "Cleaned up {} old transaction nonces (cutoff: {})",
            deleted,
            cutoff
        );
        Ok(())
    }
}

inventory::submit! {
    &CleanupNoncesHandler as &dyn TransactionHandler
}

#[cfg(test)]
mod leave_tests {
    use super::*;
    use crate::handlers::{NullNotifier, NullScheduler};
    use ed25519_dalek::SigningKey;

    fn setup_pool(
        n_validators: i32,
        profile: &str,
    ) -> r2d2::Pool<crate::db::SqliteConnectionManager> {
        let manager = crate::db::SqliteConnectionManager::memory();
        let pool = r2d2::Pool::builder()
            .max_size(1)
            .connection_customizer(Box::new(crate::db::shared::SqliteInitializer))
            .build(manager)
            .unwrap();
        crate::db::shared::initialize(pool.get().unwrap()).unwrap();

        let conn = pool.get().unwrap();
        conn.execute(
            "INSERT INTO users (user_id, username, pubkey, x25519_pubkey, encrypted_privkey, key_salt)
             VALUES (1, 'allison', X'00', X'00', X'00', X'00')",
            [],
        )
        .unwrap();
        for id in 1..=n_validators {
            let key = SigningKey::from_bytes(&[id as u8; 32]);
            let pubkey = crate::types::PubKey(key.verifying_key());
            conn.execute(
                "INSERT INTO nodes (node_id, name, owner, pubkey) VALUES (?, ?, 1, ?)",
                rusqlite::params![id, format!("node-{id}"), &pubkey],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO validators (effective_height, node_id, is_active) VALUES (10, ?, true)",
                rusqlite::params![id],
            )
            .unwrap();
        }
        conn.execute(
            "INSERT OR REPLACE INTO consensus_meta (key, value) VALUES ('last_decided_height', ?)",
            rusqlite::params![100i64],
        )
        .unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO consensus_meta (key, value) VALUES ('quorum_profile', ?)",
            rusqlite::params![profile.as_bytes()],
        )
        .unwrap();
        pool
    }

    fn run_leave(
        pool: &r2d2::Pool<crate::db::SqliteConnectionManager>,
        target: i32,
        submitter: i32,
        execute: bool,
    ) -> HandlerResult {
        let payload = bincode::serde::encode_to_vec(
            &LeaveRequest { node_id: target },
            bincode::config::standard(),
        )
        .unwrap();
        let meta = TxMeta {
            function: "validator_leave",
            payload: &payload,
            submitter_node: submitter,
            user_id: None,
        };
        let notifier = NullNotifier;
        let scheduler = NullScheduler;
        let ctx = HandlerCtx {
            fragments_dir: "",
            node_id: Some(submitter),
            notifier: &notifier,
            work: &scheduler,
        };
        let mut conn = pool.get().unwrap();
        let db_tx = conn.transaction().unwrap();
        let result = ValidatorLeaveHandler.process(&meta, execute, &ctx, &db_tx);
        if result.is_ok() && execute {
            db_tx.commit().unwrap();
        }
        result
    }

    // Should: refuse the last active validator's leave (v > 1 fails).
    // Impact: INV-FLOOR — the set never empties.
    #[test]
    fn leave_v1_refused() {
        let pool = setup_pool(1, "bft");
        assert!(run_leave(&pool, 1, 1, false).is_err());
    }

    // Should: refuse a leave from a node that is not seated.
    #[test]
    fn leave_not_active_refused() {
        let pool = setup_pool(2, "bft");
        // node 3 is registered nowhere — not seated.
        let payload = bincode::serde::encode_to_vec(
            &LeaveRequest { node_id: 3 },
            bincode::config::standard(),
        )
        .unwrap();
        let meta = TxMeta {
            function: "validator_leave",
            payload: &payload,
            submitter_node: 3,
            user_id: None,
        };
        let notifier = NullNotifier;
        let scheduler = NullScheduler;
        let ctx = HandlerCtx {
            fragments_dir: "",
            node_id: Some(3),
            notifier: &notifier,
            work: &scheduler,
        };
        let mut conn = pool.get().unwrap();
        let db_tx = conn.transaction().unwrap();
        assert!(
            ValidatorLeaveHandler
                .process(&meta, false, &ctx, &db_tx)
                .is_err()
        );
    }

    // Should not: let one node remove another via leave — the signature is
    // the consent (both phases).
    #[test]
    fn leave_not_self_refused() {
        let pool = setup_pool(3, "bft");
        assert!(matches!(
            run_leave(&pool, 2, 1, false),
            Err(DatabaseError::AuthorizationError)
        ));
        assert!(matches!(
            run_leave(&pool, 2, 1, true),
            Err(DatabaseError::AuthorizationError)
        ));
    }

    // Should: a 3-validator BFT mesh allow a leave (v−1 = 2 ≥ quorum(2) = 2)
    // and record the voluntary departure at the committed height.
    // Impact: the leave -> deactivation execute path end to end.
    #[test]
    fn leave_v3_bft_allowed_then_executes() {
        let pool = setup_pool(3, "bft");
        assert!(run_leave(&pool, 2, 2, false).is_ok());
        assert!(run_leave(&pool, 2, 2, true).is_ok());

        let conn = pool.get().unwrap();
        let validators = hopnet_consensus::validators::get_validators(&conn, 101).unwrap();
        let ids: Vec<i32> = validators.iter().map(|v| v.node_id).collect();
        assert_eq!(ids, vec![1, 3]);
        assert_eq!(
            hopnet_consensus::validators::last_departure(&conn, 2, 101).unwrap(),
            Some(hopnet_consensus::validators::DepartureKind::Voluntary)
        );
    }

    // Should: a 2-validator mesh allow a leave — quorum(1) = 1 under both
    // profiles; a solo mesh is degenerate but legitimate.
    #[test]
    fn leave_v2_allowed() {
        let pool = setup_pool(2, "majority");
        assert!(run_leave(&pool, 2, 2, false).is_ok());
    }

    // Should: leave then reactivate round-trip through both handlers at
    // increasing committed heights.
    // Impact: the S1 rejoin path (legacy self-request activation).
    #[test]
    fn leave_then_reactivate_roundtrip() {
        let pool = setup_pool(3, "bft");
        assert!(run_leave(&pool, 3, 3, false).is_ok());
        assert!(run_leave(&pool, 3, 3, true).is_ok());

        // Mesh advances, node 3 requests activation again.
        {
            let conn = pool.get().unwrap();
            conn.execute(
                "INSERT OR REPLACE INTO consensus_meta (key, value) VALUES ('last_decided_height', ?)",
                rusqlite::params![110i64],
            )
            .unwrap();
        }
        let payload = bincode::serde::encode_to_vec(
            &ActivationRequest { members: vec![3] },
            bincode::config::standard(),
        )
        .unwrap();
        // Mesh-initiated: a seated validator (node 1) proposes the batch.
        let meta = TxMeta {
            function: "validator_activation",
            payload: &payload,
            submitter_node: 1,
            user_id: None,
        };
        let notifier = NullNotifier;
        let scheduler = NullScheduler;
        let ctx = HandlerCtx {
            fragments_dir: "",
            node_id: Some(3),
            notifier: &notifier,
            work: &scheduler,
        };
        let mut conn = pool.get().unwrap();
        let db_tx = conn.transaction().unwrap();
        assert!(
            ValidatorActivationHandler
                .process(&meta, false, &ctx, &db_tx)
                .is_ok()
        );
        assert!(
            ValidatorActivationHandler
                .process(&meta, true, &ctx, &db_tx)
                .is_ok()
        );
        db_tx.commit().unwrap();
        drop(conn); // max_size-1 pool: release before the assert checkout

        let conn = pool.get().unwrap();
        let ids: Vec<i32> = hopnet_consensus::validators::get_validators(&conn, 120)
            .unwrap()
            .iter()
            .map(|v| v.node_id)
            .collect();
        assert_eq!(ids, vec![1, 2, 3]);
    }
}

#[cfg(test)]
mod genesis_tests {
    use super::*;
    use crate::handlers::{NullNotifier, NullScheduler};
    use ed25519_dalek::SigningKey;

    // Should: the genesis handler seed hopnet_consensus_policy rows from
    // the payload, resolving absent keys to defaults, without disturbing
    // the storage-policy seeding (field-addition regression).
    // Impact: the genesis path IS the orchestrator test path — tiny
    // seeded windows are how membership tests run at seconds-scale.
    #[test]
    fn genesis_seeds_consensus_policy() {
        let manager = crate::db::SqliteConnectionManager::memory();
        let pool = r2d2::Pool::builder()
            .max_size(1)
            .connection_customizer(Box::new(crate::db::shared::SqliteInitializer))
            .build(manager)
            .unwrap();
        crate::db::shared::initialize(pool.get().unwrap()).unwrap();

        // Real user key material (the handler inserts the user + node).
        let signing_key = crate::types::PrivKey(SigningKey::from_bytes(&[7u8; 32]));
        let x25519_pubkey = crate::auth::derive_x25519_pubkey_from_user(&signing_key);
        let (encrypted_privkey, key_salt) =
            crate::auth::wrap_user_privkey(&signing_key, "password").unwrap();
        let user = crate::types::User::new(
            0,
            "genesis_user".to_string(),
            crate::types::PubKey(signing_key.0.verifying_key()),
            x25519_pubkey,
            encrypted_privkey,
            key_salt,
        );
        let node_key = SigningKey::from_bytes(&[8u8; 32]);
        let node = crate::types::Node {
            node_id: 0,
            name: "node_0".to_string(),
            owner: 0,
            pubkey: crate::types::PubKey(node_key.verifying_key()),
        };

        // Mesh keypair + user-0 grant, exactly as post_initial_setup.
        let mesh_secret =
            x25519_dalek::StaticSecret::random_from_rng(chacha20poly1305::aead::OsRng);
        let mesh_pubkey = x25519_dalek::PublicKey::from(&mesh_secret);
        let (mesh_eph, mesh_wrapped) = hopnet_storage::crypto::wrap_mesh_privkey(
            &mesh_pubkey,
            &mesh_secret,
            user.x25519_pubkey.as_x25519(),
        )
        .unwrap();
        let payload = GenesisPayload {
            user,
            node,
            mesh_pubkey: *mesh_pubkey.as_bytes(),
            mesh_grant: hopnet_storage::MeshKeyGrant {
                recipient_pubkey: [0u8; 32],
                ephemeral_pubkey: mesh_eph,
                wrapped_privkey: mesh_wrapped,
            },
            storage_policy: vec![("burst_cap".to_string(), "3".to_string())],
            consensus_policy: vec![
                ("probe_base".to_string(), "2".to_string()),
                ("s_full".to_string(), "6".to_string()),
            ],
        };

        let encoded = bincode::serde::encode_to_vec(&payload, bincode::config::standard()).unwrap();
        let meta = TxMeta {
            function: "insert_genesis",
            payload: &encoded,
            submitter_node: 0,
            user_id: None,
        };
        let notifier = NullNotifier;
        let scheduler = NullScheduler;
        let ctx = crate::handlers::HandlerCtx {
            fragments_dir: "",
            node_id: Some(0),
            notifier: &notifier,
            work: &scheduler,
        };

        let mut conn = pool.get().unwrap();
        let db_tx = conn.transaction().unwrap();
        InsertGenesisHandler
            .process(&meta, true, &ctx, &db_tx)
            .unwrap();
        db_tx.commit().unwrap();

        let policy = hopnet_consensus::store::read_policy(&conn).unwrap();
        assert_eq!(policy.probe_base, std::time::Duration::from_secs(2));
        assert_eq!(policy.s_full, std::time::Duration::from_secs(6));
        assert_eq!(policy.grace, std::time::Duration::from_secs(5)); // default
        assert_eq!(policy.p_prove, std::time::Duration::from_secs(1800)); // default

        // Storage-policy seeding still resolves (field-addition regression).
        let storage = hopnet_storage::store::read_policy(&conn).unwrap();
        assert_eq!(storage.b_max, 3);
    }
}

#[cfg(test)]
mod vote_out_tests {
    use super::*;
    use crate::handlers::{NullNotifier, NullScheduler};
    use ed25519_dalek::SigningKey;

    fn setup_pool(n_validators: i32) -> r2d2::Pool<crate::db::SqliteConnectionManager> {
        let manager = crate::db::SqliteConnectionManager::memory();
        let pool = r2d2::Pool::builder()
            .max_size(1)
            .connection_customizer(Box::new(crate::db::shared::SqliteInitializer))
            .build(manager)
            .unwrap();
        crate::db::shared::initialize(pool.get().unwrap()).unwrap();
        let conn = pool.get().unwrap();
        conn.execute(
            "INSERT INTO users (user_id, username, pubkey, x25519_pubkey, encrypted_privkey, key_salt)
             VALUES (1, 'allison', X'00', X'00', X'00', X'00')",
            [],
        )
        .unwrap();
        for id in 1..=n_validators {
            let key = SigningKey::from_bytes(&[id as u8; 32]);
            let pubkey = crate::types::PubKey(key.verifying_key());
            conn.execute(
                "INSERT INTO nodes (node_id, name, owner, pubkey) VALUES (?, ?, 1, ?)",
                rusqlite::params![id, format!("node-{id}"), &pubkey],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO validators (effective_height, node_id, is_active) VALUES (10, ?, true)",
                rusqlite::params![id],
            )
            .unwrap();
        }
        conn.execute(
            "INSERT OR REPLACE INTO consensus_meta (key, value) VALUES ('last_decided_height', ?)",
            rusqlite::params![100i64],
        )
        .unwrap();
        pool
    }

    fn run_vote_out(
        pool: &r2d2::Pool<crate::db::SqliteConnectionManager>,
        target: i32,
        submitter: i32,
        execute: bool,
    ) -> HandlerResult {
        let payload = bincode::serde::encode_to_vec(
            &VoteOutRequest { node_id: target },
            bincode::config::standard(),
        )
        .unwrap();
        let meta = TxMeta {
            function: "validator_vote_out",
            payload: &payload,
            submitter_node: submitter,
            user_id: None,
        };
        let notifier = NullNotifier;
        let scheduler = NullScheduler;
        let ctx = HandlerCtx {
            fragments_dir: "",
            node_id: Some(submitter),
            notifier: &notifier,
            work: &scheduler,
        };
        let mut conn = pool.get().unwrap();
        let db_tx = conn.transaction().unwrap();
        let result = ValidatorVoteOutHandler.process(&meta, execute, &ctx, &db_tx);
        if result.is_ok() && execute {
            db_tx.commit().unwrap();
        }
        result
    }

    // Should: refuse self-removal in both phases — that is leave's job.
    #[test]
    fn vote_out_self_refused() {
        let pool = setup_pool(3);
        assert!(matches!(
            run_vote_out(&pool, 1, 1, false),
            Err(DatabaseError::AuthorizationError)
        ));
        assert!(matches!(
            run_vote_out(&pool, 1, 1, true),
            Err(DatabaseError::AuthorizationError)
        ));
    }

    // Should: refuse an unseated target (duplicate proposals die here)
    // and an unseated submitter (only validators propose removals).
    #[test]
    fn vote_out_objective_refusals() {
        let pool = setup_pool(2);
        assert!(run_vote_out(&pool, 9, 1, false).is_err()); // target unseated
        assert!(matches!(
            run_vote_out(&pool, 2, 9, false),
            Err(DatabaseError::AuthorizationError) // submitter unseated
        ));
    }

    // Should: execute record the voted_out departure at the committed
    // height; a replayed proposal then fails validation (target gone).
    // Impact: the shared deactivation path + the duplicate-harmlessness
    // property the spec relies on.
    #[test]
    fn vote_out_executes_and_replay_refused() {
        let pool = setup_pool(3);
        assert!(run_vote_out(&pool, 3, 1, false).is_ok());
        assert!(run_vote_out(&pool, 3, 1, true).is_ok());

        {
            let conn = pool.get().unwrap();
            let ids: Vec<i32> = hopnet_consensus::validators::get_validators(&conn, 101)
                .unwrap()
                .iter()
                .map(|v| v.node_id)
                .collect();
            assert_eq!(ids, vec![1, 2]);
            assert_eq!(
                hopnet_consensus::validators::last_departure(&conn, 3, 101).unwrap(),
                Some(hopnet_consensus::validators::DepartureKind::VotedOut)
            );
        }
        // Replay: target no longer seated -> validation refuses.
        assert!(run_vote_out(&pool, 3, 1, false).is_err());
    }
}

#[cfg(test)]
mod activation_batch_tests {
    use super::*;
    use crate::handlers::{NullNotifier, NullScheduler};
    use ed25519_dalek::SigningKey;

    // Seat validators 1..=seated; register (but don't seat) extra..
    fn setup_pool(
        seated: i32,
        registered_extra: &[i32],
    ) -> r2d2::Pool<crate::db::SqliteConnectionManager> {
        let manager = crate::db::SqliteConnectionManager::memory();
        let pool = r2d2::Pool::builder()
            .max_size(1)
            .connection_customizer(Box::new(crate::db::shared::SqliteInitializer))
            .build(manager)
            .unwrap();
        crate::db::shared::initialize(pool.get().unwrap()).unwrap();
        let conn = pool.get().unwrap();
        conn.execute(
            "INSERT INTO users (user_id, username, pubkey, x25519_pubkey, encrypted_privkey, key_salt)
             VALUES (1, 'allison', X'00', X'00', X'00', X'00')",
            [],
        )
        .unwrap();
        let add_node = |id: i32| {
            let key = SigningKey::from_bytes(&[id as u8; 32]);
            let pk = crate::types::PubKey(key.verifying_key());
            conn.execute(
                "INSERT INTO nodes (node_id, name, owner, pubkey) VALUES (?, ?, 1, ?)",
                rusqlite::params![id, format!("node-{id}"), &pk],
            )
            .unwrap();
        };
        for id in 1..=seated {
            add_node(id);
            conn.execute(
                "INSERT INTO validators (effective_height, node_id, is_active) VALUES (10, ?, true)",
                rusqlite::params![id],
            )
            .unwrap();
        }
        for id in registered_extra {
            add_node(*id);
        }
        conn.execute(
            "INSERT OR REPLACE INTO consensus_meta (key, value) VALUES ('last_decided_height', ?)",
            rusqlite::params![100i64],
        )
        .unwrap();
        pool
    }

    fn run_batch(
        pool: &r2d2::Pool<crate::db::SqliteConnectionManager>,
        members: Vec<i32>,
        submitter: i32,
        execute: bool,
    ) -> HandlerResult {
        let payload = bincode::serde::encode_to_vec(
            &ActivationRequest { members },
            bincode::config::standard(),
        )
        .unwrap();
        let meta = TxMeta {
            function: "validator_activation",
            payload: &payload,
            submitter_node: submitter,
            user_id: None,
        };
        let notifier = NullNotifier;
        let scheduler = NullScheduler;
        let ctx = HandlerCtx {
            fragments_dir: "",
            node_id: Some(submitter),
            notifier: &notifier,
            work: &scheduler,
        };
        let mut conn = pool.get().unwrap();
        let db_tx = conn.transaction().unwrap();
        let result = ValidatorActivationHandler.process(&meta, execute, &ctx, &db_tx);
        if result.is_ok() && execute {
            db_tx.commit().unwrap();
        }
        result
    }

    // Should: refuse malformed batches (empty, oversize, unsorted).
    #[test]
    fn batch_shape_refusals() {
        let pool = setup_pool(3, &[4, 5, 6, 7, 8, 9]);
        assert!(run_batch(&pool, vec![], 1, false).is_err());
        assert!(run_batch(&pool, vec![4, 5, 6, 7, 8, 9], 1, false).is_err()); // > B_MAX
        assert!(run_batch(&pool, vec![5, 4], 1, false).is_err()); // unsorted
        assert!(run_batch(&pool, vec![4, 4], 1, false).is_err()); // dup (not strictly increasing)
    }

    // Should: refuse an unseated submitter, an unregistered member, and an
    // already-seated member (whole batch).
    #[test]
    fn batch_objective_refusals() {
        let pool = setup_pool(3, &[4, 5]);
        assert!(matches!(
            run_batch(&pool, vec![4], 9, false), // submitter 9 not seated
            Err(DatabaseError::AuthorizationError)
        ));
        assert!(run_batch(&pool, vec![99], 1, false).is_err()); // unregistered
        assert!(run_batch(&pool, vec![2, 4], 1, false).is_err()); // 2 already seated
    }

    // Should: activate every member of a valid batch at the committed
    // height; the set grows by the whole batch.
    #[test]
    fn batch_executes() {
        let pool = setup_pool(3, &[4, 5]);
        assert!(run_batch(&pool, vec![4, 5], 1, false).is_ok());
        assert!(run_batch(&pool, vec![4, 5], 1, true).is_ok());
        let conn = pool.get().unwrap();
        let ids: Vec<i32> = hopnet_consensus::validators::get_validators(&conn, 101)
            .unwrap()
            .iter()
            .map(|v| v.node_id)
            .collect();
        assert_eq!(ids, vec![1, 2, 3, 4, 5]);
    }
}
