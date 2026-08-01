//! File/folder creation primitives shared between `post_files` and per-file
//! callers (import pipeline). Built out incrementally in Phase 3.2.
//!
//! Moved from the host's `files::helpers` (RFC-015 Stage D4): DB access via
//! the drive's pool, sessions via `SessionAccess`, and consensus submission
//! via `TxGateway` (signing stays host-side).

use axum::http::StatusCode;
use chacha20poly1305::{aead::OsRng, ChaCha20Poly1305, KeyInit};
use rusqlite::Transaction as RusqliteTransaction;
use tokio::io::AsyncRead;

use crate::host::{DriveState, SessionError, TxSigner, TxSpec, UserSession};
use crate::http::files::process_uploaded_file;
use crate::model::{Inode, InodeOwner};
use crate::paths::{encrypt_part, encrypt_path};
use hopnet_common::{Blake3Hash, CustomUUID};
use hopnet_projection::DatabaseError;
use hopnet_storage::SelfCheckFragments;

/// Resolve the caller's session through the host seam, mapping to the same
/// StatusCodes the host's `AppState::get_session` returned (401 expired,
/// 428 missing).
pub(crate) async fn session_or_status(
    state: &DriveState,
    user_id: i32,
) -> Result<UserSession, StatusCode> {
    state
        .sessions
        .user_session(user_id)
        .await
        .map_err(|e| match e {
            SessionError::Unauthorized => StatusCode::UNAUTHORIZED,
            SessionError::PreconditionRequired => StatusCode::PRECONDITION_REQUIRED,
        })
}

#[derive(Debug, thiserror::Error)]
pub enum AttestationError {
    #[error("DB query failed: {0}")]
    DbQuery(#[from] rusqlite::Error),
    #[error("Consensus height query failed: {0:?}")]
    ConsensusHeight(DatabaseError),
    #[error("Serialization failed: {0}")]
    Serialize(#[from] bincode::error::EncodeError),
}

/// Find missing ancestors of `inodes` and prepend synthetic folder inodes
/// (depth-ascending) so consensus inserts parents before children.
///
/// Caller owns the `Transaction` so DB lifespan stays visible at the call
/// site. Read-only — `find_missing_parents` creates/drops its own temp table.
pub fn prepend_missing_parents(
    tx: &RusqliteTransaction,
    inodes: &mut Vec<Inode>,
    user_id: i32,
) -> Result<(), StatusCode> {
    let paths: Vec<&str> = inodes.iter().map(|i| i.path.as_str()).collect();
    let missing = crate::db::files::find_missing_parents(tx, &paths)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    if missing.is_empty() {
        return Ok(());
    }

    let mut prefix: Vec<Inode> = missing
        .into_iter()
        .map(|path| Inode {
            id: CustomUUID::new(None),
            owner: InodeOwner::Id(user_id),
            path,
            inode_type: hopnet_common::InodeType::Folder,
            data_id: None,
        })
        .collect();
    prefix.append(inodes);
    *inodes = prefix;
    Ok(())
}

/// Assemble a file `Inode` from `source` bytes:
/// - encrypts `filename` (per-segment AES-SIV) and appends to the
///   already-encrypted parent path
/// - mints a fresh data block id and per-file ChaCha20Poly1305 key
/// - creates the user's `FileAccess` entry
/// - streams `source` through `process_uploaded_file` for Reed-Solomon
///   encoding + fragment writes (skipped when `file_size == 0`)
///
/// Returns the prepared `Inode` and the freshly minted `data_block_id`. The
/// caller uses the id for downstream distribution scheduling.
pub async fn assemble_file_inode<R: AsyncRead + Unpin>(
    state: &DriveState,
    session: &UserSession,
    user_id: i32,
    encrypted_parent_path: &str,
    filename: &str,
    source: R,
    file_size: usize,
) -> Result<
    (
        Inode,
        CustomUUID,
        Option<hopnet_storage::store::BlobInsertOp>,
    ),
    StatusCode,
> {
    let encrypted_filename = encrypt_part(filename, &session.siv_key, &session.siv_nonce)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let encrypted_path = encrypted_parent_path.to_string() + &encrypted_filename;

    let dataid = CustomUUID::new(None);

    // Empty content: no blob at all (data_id = NULL) — the substrate never
    // sees zero-byte puts and no key exists to wrap.
    if file_size == 0 {
        let inode = Inode {
            id: CustomUUID::new(None),
            owner: InodeOwner::Id(user_id),
            path: encrypted_path,
            inode_type: hopnet_common::InodeType::File,
            data_id: None,
        };
        return Ok((inode, dataid, None));
    }

    let per_file_key = ChaCha20Poly1305::generate_key(&mut OsRng);

    let file_access = crate::db::users::blob_access_for_user(
        state.db_pool.get(),
        dataid.clone(),
        user_id,
        &per_file_key,
    )
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut blob_op = process_uploaded_file(
        source,
        file_size,
        dataid.clone(),
        &per_file_key,
        &state.fragments_dir,
    )
    .await?;
    blob_op.access = vec![file_access];

    let inode = Inode {
        id: CustomUUID::new(None),
        owner: InodeOwner::Id(user_id),
        path: encrypted_path,
        inode_type: hopnet_common::InodeType::File,
        data_id: Some(dataid.clone()),
    };

    Ok((inode, dataid, Some(blob_op)))
}

/// Submit `inodes` (with parents already prepended by the caller) through
/// consensus and trigger fragment distribution. Caller owns parent backfill
/// and attestation construction so DB tx scopes stay visible at the call
/// site; pass `attestation = None` for folder-only batches or when the
/// caller already short-circuited attestation building.
pub async fn submit_inodes(
    state: &DriveState,
    user_id: i32,
    blob_ops: Vec<hopnet_storage::store::BlobInsertOp>,
    inodes: Vec<Inode>,
    attestation: Option<Vec<u8>>,
) -> Result<(), StatusCode> {
    let payload = crate::envelopes::DriveInsertPayload { blob_ops, inodes };
    let encoded_inodes = bincode::serde::encode_to_vec(&payload, bincode::config::standard())
        .map_err(|e| {
            tracing::error!("Bincode encoding error: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let mut transactions = vec![TxSpec {
        function: "insert_files",
        payload: encoded_inodes,
        signer: TxSigner::User(user_id),
    }];
    if let Some(attestation_payload) = attestation {
        tracing::debug!("Including fragment attestation in upload consensus batch");
        transactions.push(TxSpec {
            function: "self_check_fragments",
            payload: attestation_payload,
            signer: TxSigner::Node,
        });
    }

    let results = state.txs.submit_batch(transactions).await;
    if results.iter().any(|r| r.is_err()) {
        tracing::error!("Failed to submit inodes to consensus");
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }

    // Distribution is kicked by our own apply (on_decided → global worker
    // queue) — no per-file spawns, no polling, and modify updates are
    // covered by the same path.
    Ok(())
}

/// Collect the fragment hashes across `blob_ops`. Pure — no DB.
fn extract_uploaded_fragment_hashes(
    blob_ops: &[hopnet_storage::store::BlobInsertOp],
) -> Vec<Blake3Hash> {
    blob_ops
        .iter()
        .flat_map(|op| op.fragments.iter().map(|f| f.fragment_hash))
        .collect()
}

/// Build a `self_check_fragments` attestation PAYLOAD for fragments freshly
/// written during an upload. Filters against the node's current
/// `fragment_inventory` so retries don't trigger PRIMARY KEY violations
/// downstream. Returns `None` when there's nothing new to attest
/// (folder-only batch or full duplicate). The caller wraps the payload in a
/// node-signed `TxSpec` — signing itself stays host-side (TxGateway).
///
/// Caller threads `tx` so DB lifespan is visible at the call site. The tx is
/// read-only; caller is responsible for rolling it back (or letting it drop).
pub(crate) fn build_upload_attestation(
    tx: &RusqliteTransaction,
    node_id: i32,
    blob_ops: &[hopnet_storage::store::BlobInsertOp],
) -> Result<Option<Vec<u8>>, AttestationError> {
    let uploaded_fragments = extract_uploaded_fragment_hashes(blob_ops);
    if uploaded_fragments.is_empty() {
        tracing::debug!("No fragments to attest (empty upload or folder-only)");
        return Ok(None);
    }

    let (previous_count, existing_fragments) =
        hopnet_storage::store::query_inventory_state(tx, node_id, &uploaded_fragments)?;

    let new_fragments: Vec<Blake3Hash> = uploaded_fragments
        .into_iter()
        .filter(|h| !existing_fragments.contains(h))
        .collect();

    if new_fragments.is_empty() {
        tracing::debug!("All uploaded fragments already in inventory, skipping attestation");
        return Ok(None);
    }

    let self_verified_height =
        crate::db::current_height(tx).map_err(AttestationError::ConsensusHeight)?;

    let attestation = SelfCheckFragments {
        node_id,
        self_verified_height,
        previous_count,
        fragments_added: new_fragments.clone(),
        fragments_removed: Vec::new(),
    };

    let payload = bincode::serde::encode_to_vec(&attestation, bincode::config::standard())?;

    tracing::info!(
        "Built upload attestation: {} new fragments (filtered {} existing), previous_count={}, height={}",
        new_fragments.len(),
        existing_fragments.len(),
        attestation.previous_count,
        attestation.self_verified_height
    );

    Ok(Some(payload))
}

/// Top-level per-file submitter. Encrypts parent path, assembles the file
/// inode, runs parent backfill, builds attestation, and submits via
/// consensus.
///
/// Used by per-file callers (e.g. import pipeline § 3.5). Each call is one
/// `insert_files` consensus transaction; consensus pressure is linear in
/// caller invocations. Phase 5 parallelism overlaps these calls but does
/// not change txn count.
///
/// Returns the freshly minted `data_block_id`.
pub async fn create_file_with_fragments<R: AsyncRead + Unpin>(
    state: &DriveState,
    user_id: i32,
    plaintext_parent_path: &str,
    filename: &str,
    source: R,
    file_size: usize,
) -> Result<CustomUUID, StatusCode> {
    let session = session_or_status(state, user_id).await?;
    let encrypted_parent = encrypt_path(
        plaintext_parent_path.to_string(),
        &session.siv_key,
        &session.siv_nonce,
    )
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let (inode, dataid, blob_op) = assemble_file_inode(
        state,
        &session,
        user_id,
        &encrypted_parent,
        filename,
        source,
        file_size,
    )
    .await?;

    let blob_ops: Vec<hopnet_storage::store::BlobInsertOp> = blob_op.into_iter().collect();

    let mut inodes = vec![inode];

    {
        let conn = state
            .db_pool
            .get()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let tx = conn
            .unchecked_transaction()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        prepend_missing_parents(&tx, &mut inodes, user_id)?;
    }

    let attestation = if let Some(node_id) = state.node_id() {
        let conn = state
            .db_pool
            .get()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let tx = conn
            .unchecked_transaction()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        match build_upload_attestation(&tx, node_id, &blob_ops) {
            Ok(opt) => opt,
            Err(e) => {
                tracing::warn!(
                    "Failed to build upload attestation: {}. Continuing without — periodic self-check will reconcile",
                    e
                );
                None
            }
        }
    } else {
        None
    };

    submit_inodes(state, user_id, blob_ops, inodes, attestation).await?;
    Ok(dataid)
}

/// Top-level folder submitter. Encrypts the full plaintext path, runs
/// parent backfill, and submits via consensus. No fragments → no
/// attestation, no distribution.
pub async fn create_folder(
    state: &DriveState,
    user_id: i32,
    plaintext_path: &str,
) -> Result<(), StatusCode> {
    let session = session_or_status(state, user_id).await?;
    let encrypted_path = encrypt_path(
        plaintext_path.to_string(),
        &session.siv_key,
        &session.siv_nonce,
    )
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let folder_inode = Inode {
        id: CustomUUID::new(None),
        owner: InodeOwner::Id(user_id),
        path: encrypted_path,
        inode_type: hopnet_common::InodeType::Folder,
        data_id: None,
    };

    let mut inodes = vec![folder_inode];

    {
        let conn = state
            .db_pool
            .get()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let tx = conn
            .unchecked_transaction()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        prepend_missing_parents(&tx, &mut inodes, user_id)?;
    }

    submit_inodes(state, user_id, Vec::new(), inodes, None).await
}
