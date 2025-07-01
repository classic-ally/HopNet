use super::*;

use axum::{
    extract::State,
    response::IntoResponse,
    http::StatusCode,
    Json
};

use crate::db::consensus as db;
/// CONSENSUS ARCHITECTURE
/// Key notes:
/// - Using ed25519 over threshold w/ distributed key generation (e.g. BLS)
///   - Verifier: O(nodes) signature verification vs O(1), but ed is ~10x faster per op
///   - Signer: both O(1), ed is ~5x faster per op
///   - ed25519_dalek library gives us batch verify, ~6-8x faster 128+ batch
///   - Conclusion: probably faster until nodes is multiple hundreds (O(nodes) dominates)
/// 
///   - Gives us audit trail (which node did what?) over BLS
///   - Simpler implementation (no secret sharing polynomial pain)
///   - But, more vulnerable to mistakes
///     - No cryptographic protection against committing undersigned changes
///   - O(nodes) sigs broadcast for changes introduces network overhead
///     - BLS: 96 bytes per sig
///     - ed: 64 bytes per sig -> 6.4kb per 100 nodes
/// 
///   - We may want to address this later based on % overhead stats


use crate::AppState;

// route to get the consensus status
pub async fn get_consensus(
    State(app_state): State<AppState>,
) -> impl IntoResponse {
    match db::get_consensus(&app_state.db) {
        Ok(info) => (StatusCode::OK, Json(info)).into_response(),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "Failed to get leader info").into_response(),
    }
}

// route to get acceptable validators for a given view
pub async fn get_validators(
    State(app_state): State<AppState>,
    Json(height): Json<i32>,
) -> impl IntoResponse {
    match db::get_validators(&app_state.db, height) {
        Ok(nodes) => (StatusCode::OK, Json(nodes)).into_response(),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "Failed to get validators").into_response(),
    }
}

// route to accept ballots and operate on them
pub async fn post_ballot(
    State(app_state): State<AppState>,
    Json(ballot): Json<Ballot>
) -> impl IntoResponse {
    // validate the ballot proposal
    match ballot.verify_proposal(&app_state) {
        Ok(()) => {
            // sign and return the response
            dbg!("Signing off on block hash {}", ballot.block.block_hash);
            match ballot.sign(&app_state) {
                Ok(signoff) => {
                    // Only insert block during Propose phase, not Lock phase
                    if ballot.data.phase == ConsensusPhase::Propose {
                        dbg!("Adding to database block hash {}", ballot.block.block_hash);
                        match db::insert_block(&app_state.db, &ballot.block) {
                            Ok(()) => {
                                dbg!("Block saved!");
                                return (StatusCode::OK, Json(signoff)).into_response()
                            },
                            Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, "Error adding block to database").into_response(),
                        }
                    } else {
                        // Lock phase - block should already exist, just return the signature
                        dbg!("Lock phase - block already exists, returning signature");
                        return (StatusCode::OK, Json(signoff)).into_response()
                    }
                }
                Err(e) => {
                    dbg!(e);
                    return (StatusCode::INTERNAL_SERVER_ERROR, "Error signing ballot").into_response()
                },
            }
        }
        Err(e) => {
            dbg!(e);
            return (StatusCode::UNAUTHORIZED, "Ballot rejected").into_response()
        },
    }
}

// route to accept qcs and operate on them
pub async fn post_qc(
    State(app_state): State<AppState>,
    Json(qc): Json<QuorumCertificate>
) -> impl IntoResponse {
    // validate the QC against internal block
    dbg!("Received QC");
    match db::get_block(&app_state.db, qc.block_hash) {
        Ok(block) => {
            dbg!("We have the block, verifying...");
            match qc.verify(&app_state, &block) {
                Ok(()) => {
                    // save it to db
                    dbg!("QC looks good, committing");
                    match db::insert_qc(&app_state.db, qc.clone()) {
                        Ok(()) => {
                            // Process transactions if this is a Lock phase QC
                            if qc.phase == ConsensusPhase::Lock {
                                dbg!("Lock phase QC committed, processing transactions");
                                crate::consensus::functions::process_transactions(&block.data.transactions, &app_state);
                            }
                            StatusCode::OK
                        },
                        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
                    }
                }
                Err(e) => {
                    dbg!("Don't like the QC, printing error");
                    dbg!(e);
                    StatusCode::UNAUTHORIZED
                }
            }
        }
        Err(_) => StatusCode::NOT_FOUND
    }
    
}