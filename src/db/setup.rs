use super::*;
use axum::http::StatusCode;

/// Parse a "k=v;k=v" genesis policy spec (mesh-creation input, not runtime
/// config). Pairs without '=' are skipped; keys/values are trimmed.
fn parse_policy_spec(spec: &str) -> Vec<(String, String)> {
    spec.split(';')
        .filter_map(|pair| {
            let (k, v) = pair.split_once('=')?;
            Some((k.trim().to_string(), v.trim().to_string()))
        })
        .collect()
}

pub fn get_initial_setup(
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
) -> Result<StatusCode, DatabaseError> {
    match db_connection {
        Ok(db_lock) => {
            // if there is entry in the this_node table, we're set up
            let count = db_lock
                .query_row("SELECT COUNT(*) FROM this_node", [], |row| {
                    row.get::<_, i32>(0)
                })
                .map_err(|_| DatabaseError::RecallError)?;

            if count > 0 {
                Ok(StatusCode::OK)
            } else {
                Ok(StatusCode::NOT_FOUND)
            }
        }
        Err(_) => Err(DatabaseError::LockError),
    }
}

/// Initialize sequences to 0 - used by genesis block and network creation
/// Operates within provided transaction for atomicity
pub fn initialize_sequences_tx(tx: &rusqlite::Transaction) -> Result<(), DatabaseError> {
    tx.execute_batch(
        "
        INSERT INTO sequences (name, next_id) VALUES ('users', 0);
        INSERT INTO sequences (name, next_id) VALUES ('nodes', 0);
    ",
    )
    .map_err(|_| DatabaseError::InsertError)?;
    Ok(())
}

/// Initialize a new HopNet network: create the genesis transaction, apply it
/// through the dispatch table (sequences, user 0, node 0, first validator),
/// record this node's identity, and install the malachite genesis — the
/// engine-shape block at height 0 with a synthetic TRUSTED certificate (empty
/// signatures; no validator set exists before the genesis transaction creates
/// one) plus consensus_meta{last_decided_height, chain_id, quorum_profile}.
///
/// The genesis block CONTAINS the genesis transaction, so joining nodes
/// bootstrap by fetching height 0 and replaying it — no separate checkpoint
/// mechanism.
pub fn post_initial_setup(
    state: &crate::AppState,
    user: User,
    node: Node,
) -> Result<(i32, i32), DatabaseError> {
    use crate::consensus::handlers::GenesisPayload;
    use crate::consensus::types::{Transaction, Transactions};

    tracing::debug!("post_initial_setup: Starting genesis setup");

    // Mesh-wide keypair (RFC-014): generated once at genesis; privkey wrapped
    // to user 0's X25519 pubkey. Wrap bytes go INTO the payload so replay is
    // deterministic.
    let mesh_secret =
        x25519_dalek::StaticSecret::random_from_rng(chacha20poly1305::aead::OsRng);
    let mesh_pubkey = x25519_dalek::PublicKey::from(&mesh_secret);
    let (mesh_eph, mesh_wrapped) = hopnet_storage::crypto::wrap_mesh_privkey(
        &mesh_pubkey,
        &mesh_secret,
        user.x25519_pubkey.as_x25519(),
    )
    .map_err(|e| {
        tracing::error!("post_initial_setup: mesh key wrap failed: {e}");
        DatabaseError::ProcessingError
    })?;
    let mesh_grant = hopnet_storage::MeshKeyGrant {
        recipient_pubkey: *user.x25519_pubkey.as_x25519().as_bytes(),
        ephemeral_pubkey: mesh_eph,
        wrapped_privkey: mesh_wrapped,
    };

    // Mesh storage policy seed (RFC-STORAGE-002): mesh-creation-time input,
    // NOT runtime config — the rows land in replicated state, so every node
    // resolves the same policy regardless of its own environment. Format:
    // "key=value;key=value" (e.g. "decay_tiers=60,120,180,240" for
    // orchestrator tests). Empty/absent = code defaults.
    let storage_policy: Vec<(String, String)> =
        std::env::var("HOPNET_GENESIS_STORAGE_POLICY")
            .map(|s| parse_policy_spec(&s))
            .unwrap_or_default();
    let consensus_policy: Vec<(String, String)> =
        std::env::var("HOPNET_GENESIS_CONSENSUS_POLICY")
            .map(|s| parse_policy_spec(&s))
            .unwrap_or_default();

    // Create genesis payload with user, node, and mesh key material
    let genesis_payload = GenesisPayload {
        user: user.clone(),
        node: node.clone(),
        mesh_pubkey: *mesh_pubkey.as_bytes(),
        mesh_grant,
        storage_policy,
        consensus_policy,
    };

    // Encode the payload
    let payload_bytes =
        bincode::serde::encode_to_vec(&genesis_payload, bincode::config::standard()).map_err(
            |e| {
                tracing::error!(
                    "post_initial_setup: Failed to encode genesis payload: {:?}",
                    e
                );
                DatabaseError::ProcessingError
            },
        )?;

    // Create genesis transaction (signed by node)
    let genesis_tx = Transaction::new(
        "insert_genesis".to_string(),
        payload_bytes,
        0, // Genesis node_id
        &state.private_key,
    )
    .map_err(|e| {
        tracing::error!(
            "post_initial_setup: Failed to create genesis transaction: {:?}",
            e
        );
        DatabaseError::ProcessingError
    })?;

    // === TRANSACTION 1: Apply the genesis transaction via its handler ===
    // Initializes sequences, inserts user 0 + node 0, activates the first
    // validator.
    {
        let mut conn = state.db_pool.get().map_err(|e| {
            tracing::error!(
                "post_initial_setup: Failed to get DB connection for genesis transaction: {:?}",
                e
            );
            DatabaseError::LockError
        })?;
        let genesis_tx_db = conn.transaction().map_err(|e| {
            tracing::error!(
                "post_initial_setup: Failed to begin transaction for genesis transaction: {:?}",
                e
            );
            DatabaseError::InsertError
        })?;

        crate::consensus::dispatch::process_transaction(&genesis_tx, state, true, 1, &genesis_tx_db)
            .map_err(|e| {
                tracing::error!(
                    "post_initial_setup: Handler failed to process genesis transaction: {:?}",
                    e
                );
                e
            })?;

        crate::db::shared::commit_timed(genesis_tx_db).map_err(|e| {
            tracing::error!(
                "post_initial_setup: Failed to commit genesis transaction: {:?}",
                e
            );
            DatabaseError::InsertError
        })?;
        tracing::debug!("post_initial_setup: Handler completed successfully");
    }

    // === TRANSACTION 2: this_node identity (node_id 0 now exists) ===
    {
        let conn = state.db_pool.get().map_err(|e| {
            tracing::error!(
                "post_initial_setup: Failed to get DB connection for this_node: {:?}",
                e
            );
            DatabaseError::LockError
        })?;
        conn.execute(
            "INSERT INTO this_node (internal_id, node_id, privkey) VALUES (?, ?, ?)",
            params![1, 0, state.private_key],
        )
        .map_err(|e| {
            tracing::error!("post_initial_setup: Failed to insert this_node: {:?}", e);
            DatabaseError::InsertError
        })?;
        tracing::debug!("post_initial_setup: Inserted this_node");
    }

    // === TRANSACTION 3: malachite genesis (decided_blocks[0] + meta, atomic) ===
    // The engine's chain starts here: the crate-shape genesis block at height 0
    // with a synthetic TRUSTED certificate (empty signatures — there is no
    // validator set before the genesis tx creates one). The chain id is the
    // genesis block hash; it binds every consensus signature to this mesh.
    // Sync special-cases height 0 as trusted for joining nodes.
    {
        let quorum_profile = match std::env::var("HOPNET_QUORUM_PROFILE") {
            Ok(s) => hopnet_consensus::config::QuorumProfile::parse(&s).ok_or_else(|| {
                tracing::error!(
                    "post_initial_setup: invalid HOPNET_QUORUM_PROFILE {:?} (expected 'bft', 'majority', or 'auto')",
                    s
                );
                DatabaseError::ProcessingError
            })?,
            // Majority is the pre-S6 default (RFC-CONSENSUS-002): a
            // default-BFT mesh below 4 nodes cannot FORM once self-request
            // seating is gone (BFT v=1 grows only by a batch of 3, and a
            // 3-node mesh has 2 candidates). AUTO — the S6 default — is
            // majority at v < 7 anyway, so this is where AUTO already lands.
            Err(_) => hopnet_consensus::config::QuorumProfile::Auto,
        };

        let engine_txs = crate::consensus::malachite::app::to_engine_transactions(
            &Transactions(vec![genesis_tx.clone()]),
        )
        .map_err(|e| {
            tracing::error!(
                "post_initial_setup: Failed to bridge genesis transactions: {}",
                e
            );
            DatabaseError::ProcessingError
        })?;
        let engine_block = hopnet_consensus::types::Block::new(hopnet_consensus::types::BlockData {
            height: 0,
            round: 0,
            parent_hash: None,
            transactions: engine_txs,
        })
        .map_err(|e| {
            tracing::error!(
                "post_initial_setup: Failed to build engine genesis block: {:?}",
                e
            );
            DatabaseError::ProcessingError
        })?;
        let synthetic_cert = hopnet_consensus::codec::WireCommitCertificate {
            height: 0,
            round: 0,
            value_id: engine_block.block_hash,
            signatures: Vec::new(),
        };

        let mut conn = state.db_pool.get().map_err(|e| {
            tracing::error!(
                "post_initial_setup: Failed to get DB connection for malachite genesis: {:?}",
                e
            );
            DatabaseError::LockError
        })?;
        let tx_db = conn.transaction().map_err(|e| {
            tracing::error!(
                "post_initial_setup: Failed to start transaction for malachite genesis: {:?}",
                e
            );
            DatabaseError::LockError
        })?;
        hopnet_consensus::store::install_genesis(&tx_db, &engine_block, &synthetic_cert).map_err(
            |e| {
                tracing::error!("post_initial_setup: Failed to install engine genesis: {}", e);
                DatabaseError::InsertError
            },
        )?;
        hopnet_consensus::store::meta_put(
            &tx_db,
            hopnet_consensus::store::META_CHAIN_ID,
            engine_block.block_hash.as_bytes(),
        )
        .map_err(|e| {
            tracing::error!("post_initial_setup: Failed to store chain id: {}", e);
            DatabaseError::InsertError
        })?;
        hopnet_consensus::store::meta_put(
            &tx_db,
            hopnet_consensus::store::META_QUORUM_PROFILE,
            quorum_profile.as_str().as_bytes(),
        )
        .map_err(|e| {
            tracing::error!("post_initial_setup: Failed to store quorum profile: {}", e);
            DatabaseError::InsertError
        })?;
        crate::db::shared::commit_timed(tx_db).map_err(|e| {
            tracing::error!(
                "post_initial_setup: Failed to commit malachite genesis: {:?}",
                e
            );
            DatabaseError::InsertError
        })?;
        tracing::debug!(
            "post_initial_setup: Installed malachite genesis (chain_id {:?}, profile {})",
            engine_block.block_hash,
            quorum_profile.as_str()
        );
    }

    tracing::info!("Successfully completed initial database setup for node 0");

    Ok((0, 0)) // Genesis always creates user_id=0, node_id=0
}

/// Initialize a joining node's database for the malachite join bootstrap.
///
/// This creates ONLY the this_node identity row. All other state (sequences,
/// users, nodes, validators, decided history) comes from the height-0 trusted
/// genesis install plus decided-value sync.
pub fn initialize_joining_node(
    db_connection: Result<r2d2::PooledConnection<SqliteConnectionManager>, r2d2::Error>,
    join_info: crate::types::JoinInfo,
    node_privkey: PrivKey,
) -> Result<(), DatabaseError> {
    match db_connection {
        Ok(db_lock) => {
            db_lock
                .execute(
                    "INSERT INTO this_node (internal_id, node_id, privkey) VALUES (?, ?, ?)",
                    params![1, join_info.node_id, node_privkey],
                )
                .map_err(|e| {
                    tracing::error!(
                        "Failed to initialize this_node for joining node {}: {:?}",
                        join_info.node_id,
                        e
                    );
                    DatabaseError::InsertError
                })?;

            tracing::info!(
                "Initialized joining node {} (user_id={}) for join bootstrap",
                join_info.node_id,
                join_info.user_id
            );

            Ok(())
        }
        Err(e) => {
            tracing::error!(
                "Failed to get database connection for initialize_joining_node: {:?}",
                e
            );
            Err(DatabaseError::LockError)
        }
    }
}

#[cfg(test)]
mod policy_spec_tests {
    use super::parse_policy_spec;

    // Should: parse "k=v;k=v" with trimming; skip pairs without '=';
    // tolerate trailing separators and empty input.
    // Impact: HOPNET_GENESIS_*_POLICY is the mesh-creation input for both
    // the storage and consensus policy tables.
    #[test]
    fn parse_policy_spec_shapes() {
        assert_eq!(
            parse_policy_spec("a=1; b = 2 ;c=3;"),
            vec![
                ("a".to_string(), "1".to_string()),
                ("b".to_string(), "2".to_string()),
                ("c".to_string(), "3".to_string()),
            ]
        );
        assert_eq!(parse_policy_spec("noequals;x=9"), vec![(
            "x".to_string(),
            "9".to_string()
        )]);
        assert!(parse_policy_spec("").is_empty());
    }
}
