use super::*;
use crate::consensus::{types::{Block, BlockData, VoteSignMessage}, ConsensusPhase, QuorumCertificate};
use axum::http::StatusCode;

pub fn get_initial_setup(
    db: &Arc<Mutex<Connection>>
) -> Result<StatusCode, DatabaseError> {
    match db.lock() {
        Ok(db_lock) => {
            // if there is entry in the this_node table, we're set up
            let count = db_lock.query_row(
                "SELECT COUNT(*) FROM this_node",
                [],
                |row| row.get::<_, i32>(0)
            ).map_err(|_| DatabaseError::RecallError)?;

            if count > 0 {
                return Ok(StatusCode::OK);
            } else {
                return Ok(StatusCode::NOT_FOUND);
            }
        },
        Err(_) => Err(DatabaseError::LockError)
    }
}

pub fn post_initial_setup(
    db: &Arc<Mutex<Connection>>,
    mut user: User,
    node: Node,
    pubkey: PubKey,
    privkey: PrivKey,
    user_privkey: PrivKey
) -> Result<(), DatabaseError> {
    match db.lock() {
        Ok(mut db_lock) => {
            let tx = db_lock.transaction().map_err(|_| DatabaseError::LockError)?;

            // initialize counters
            tx.execute_batch("
                INSERT INTO sequences (name, next_id) VALUES ('users', 0);
                INSERT INTO sequences (name, next_id) VALUES ('nodes', 0);
            ").map_err(|_| DatabaseError::InsertError)?;

            // compute user password
            let password_hash = user.password_hash().map_err(|_| DatabaseError::ProcessingError)?;

            // insert the user first
            let next_user_id = tx.query_row(
                "SELECT next_id FROM sequences WHERE name = 'users'",
                [],
                |row| row.get::<_, i32>(0)
            ).map_err(|_| DatabaseError::RecallError)?;
            tx.execute(
                "INSERT INTO users (user_id, username, password_hash, pubkey) VALUES (?, ?, ?, ?)",
                params![next_user_id, user.username, password_hash, user.pubkey]
            ).map_err(|_| DatabaseError::InsertError)?;
            // Update the sequence for next user
            tx.execute(
                "UPDATE sequences SET next_id = next_id + 1 WHERE name = 'users'",
                []
            ).map_err(|_| DatabaseError::InsertError)?;

            // insert the node
            let next_node_id = tx.query_row(
                "SELECT next_id FROM sequences WHERE name = 'nodes'",
                [],
                |row| row.get::<_, i32>(0)
            ).map_err(|_| DatabaseError::RecallError)?;
            tx.execute(
                "INSERT INTO nodes (node_id, name, ip_address, port, owner, pubkey) VALUES (?, ?, ?, ?, ?, ?)",
                params![next_node_id, node.name, node.ip_address, node.port, next_user_id, pubkey]
            ).map_err(|_| DatabaseError::InsertError)?;

            // Update sequence
            tx.execute(
                "UPDATE sequences SET next_id = next_id + 1 WHERE name = 'nodes'", 
                []
            ).map_err(|_| DatabaseError::InsertError)?;

            // create genesis block for database
            let genesis_block = Block::new(
                BlockData {
                    height: 0,
                    view_number: 0,
                    parent_hash: None,
                    transactions: None,
                }
            ).map_err(|_| DatabaseError::ProcessingError)?;

            tx.execute(
                "INSERT INTO blocks (block_hash, height, view_number) VALUES (?, ?, ?)",
                params![genesis_block.block_hash, genesis_block.data.height, genesis_block.data.view_number]
            ).map_err(|_| DatabaseError::InsertError)?;

            // mark myself as a validator from view 0
            tx.execute(
                "INSERT INTO validators (effective_height, node_id, is_active) VALUES (?, ?, ?)",
                params![0, next_node_id, true]
            ).map_err(|_| DatabaseError::InsertError)?;

            // create a quorum certificate for this block such that we always have a chain of QCs
            // it will validate because at block height zero we are only validator
            let signatures: Vec<VoteSignMessage> = Vec::new();

            let genesis_qc_1 = QuorumCertificate::create(
                &genesis_block, 
                ConsensusPhase::Propose, 
                next_node_id, 
                &privkey, 
                signatures.clone()
            ).map_err(|_| DatabaseError::ProcessingError)?;

            let genesis_qc_2 = QuorumCertificate::create(
                &genesis_block, 
                ConsensusPhase::Lock, 
                next_node_id, 
                &privkey, 
                signatures
            ).map_err(|_| DatabaseError::ProcessingError)?;

            tx.execute(
                "INSERT INTO quorum_certificates (view_number, phase, block_hash, proposer_signature, voter_signatures) VALUES (?, ?, ?, ?, ?)",
                params![genesis_qc_1.view_number, genesis_qc_1.phase, genesis_qc_1.block_hash, genesis_qc_1.proposer_signature, genesis_qc_1.voter_signatures]
            ).map_err(|_| DatabaseError::InsertError)?;

            tx.execute(
                "INSERT INTO quorum_certificates (view_number, phase, block_hash, proposer_signature, voter_signatures) VALUES (?, ?, ?, ?, ?)",
                params![genesis_qc_2.view_number, genesis_qc_2.phase, genesis_qc_2.block_hash, genesis_qc_2.proposer_signature, genesis_qc_2.voter_signatures]
            ).map_err(|_| DatabaseError::InsertError)?;

            // also write this node so we know setup is completed
            tx.execute(
                "INSERT INTO this_node (internal_id, node_id, privkey, current_view, committed_block_hash, highest_qc_block_hash, user_privkey) VALUES (?, ?, ?, ?, ?, ?, ?)",
                params![1, next_node_id, privkey, genesis_block.data.view_number + 1, genesis_block.block_hash, genesis_block.block_hash, user_privkey]
            ).map_err(|_| DatabaseError::InsertError)?;

            // Commit the transaction
            tx.commit().map_err(|_| DatabaseError::InsertError)?;
            
            dbg!("Successfully inserted setup info");

            Ok(())

        }
        Err(_) => Err(DatabaseError::LockError)
    }
}

pub fn put_join_setup(
    db: &Arc<Mutex<Connection>>,
    setupobj: crate::setup::SyncSetupObject,
    privkey: PrivKey
) -> Result<(), DatabaseError> {
    match db.lock() {
        Ok(mut db_lock) => {
            // in this case we need to write the list of nodes and users to the DB
            let tx = db_lock.transaction().map_err(|_| DatabaseError::LockError)?;

            dbg!("Inserting users");
            for user in setupobj.users {
                tx.execute(
                    "INSERT INTO users (user_id, username, password_hash, pubkey) VALUES (?, ?, ?, ?)",
                    params![user.user_id, user.username, user.password, user.pubkey]
                ).map_err(|_| DatabaseError::InsertError)?;
            }

            dbg!("Inserting nodes");
            for node in setupobj.nodes {
                tx.execute(
                    "INSERT INTO nodes (node_id, name, ip_address, port, owner, pubkey) VALUES (?, ?, ?, ?, ?, ?)",
                    params![node.node_id, node.name, node.ip_address, node.port, node.owner, node.pubkey]
                ).map_err(|_| DatabaseError::InsertError)?;
            }

            dbg!("Inserting sequences");
            for sequence in setupobj.sequences {
                tx.execute(
                    "INSERT INTO sequences (name, next_id) VALUES (?, ?)",
                    params![sequence.name, sequence.next_id]
                ).map_err(|_| DatabaseError::InsertError)?;
            }

            dbg!("Inserting blocks");
            for block in setupobj.blocks {
                tx.execute(
                    "INSERT INTO blocks (block_hash, height, view_number, parent_hash, transactions) VALUES (?, ?, ?, ?, ?)",
                    params![
                        block.block_hash,
                        block.data.height,
                        block.data.view_number,
                        block.data.parent_hash,
                        block.data.transactions
                    ]
                ).map_err(|_| DatabaseError::InsertError)?;
            }

            dbg!("Inserting validators");
            for validator in setupobj.validators {
                tx.execute(
                    "INSERT INTO validators (effective_height, node_id, is_active) VALUES (?, ?, ?)",
                    params![validator.effective_height, validator.node_id, validator.is_active]
                ).map_err(|_| DatabaseError::InsertError)?;
            }

            dbg!("Inserting quorum certificates");
            for quorum_certificate in setupobj.quorum_certificates {
                tx.execute(
                    "INSERT INTO quorum_certificates (view_number, phase, block_hash, proposer_signature, voter_signatures) VALUES (?, ?, ?, ?, ?)", 
                    params![quorum_certificate.view_number, quorum_certificate.phase, quorum_certificate.block_hash, quorum_certificate.proposer_signature, quorum_certificate.voter_signatures]
                ).map_err(|_| DatabaseError::InsertError)?;
            }

            dbg!("Inserting data_blocks");
            for data_block in setupobj.data_blocks {
                tx.execute(
                    "INSERT INTO data_blocks (id, access_list, modified_at, file_hash, fragment_count, added_bytes) VALUES (?, ?, ?, ?, ?, ?)",
                    params![data_block.id, data_block.access_list, data_block.modified_at, data_block.data.hash, data_block.data.fragments.len() as i32, data_block.data.added_bytes]
                ).map_err(|_| DatabaseError::InsertError)?;
            }

            dbg!("Inserting fragment_hashes");
            for fragment_hash in setupobj.fragment_hashes {
                tx.execute(
                    "INSERT INTO fragment_hashes (data_block_id, fragment_index, fragment_hash, chunk_type, stored_locally) VALUES (?, ?, ?, ?, FALSE)",
                    params![fragment_hash.data_block_id, fragment_hash.fragment_index, fragment_hash.fragment_hash, fragment_hash.chunk_type]
                ).map_err(|_| DatabaseError::InsertError)?;
            }

            dbg!("Inserting inodes");
            for inode in setupobj.inodes {
                let owner_id = match inode.owner {
                    either::Either::Left(id) => id,
                    either::Either::Right(user) => user.user_id,
                };
                let data_id = match inode.data_id {
                    Some(either::Either::Left(uuid)) => Some(uuid),
                    Some(either::Either::Right(data_record)) => Some(data_record.id),
                    None => None,
                };
                tx.execute(
                    "INSERT INTO inodes (owner_id, path, type, data_id) VALUES (?, ?, ?, ?)",
                    params![owner_id, inode.path, inode.inode_type, data_id]
                ).map_err(|_| DatabaseError::InsertError)?;
            }

            dbg!("Inserting this_node");
            tx.execute(
                "INSERT INTO this_node (internal_id, node_id, privkey, current_phase, current_view, prepared_block_hash, committed_block_hash, highest_qc_block_hash, user_privkey) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                params![
                    1,
                    setupobj.yournode.node_id,
                    privkey,
                    setupobj.yournode.current_phase,
                    setupobj.yournode.current_view,
                    setupobj.yournode.prepared_block_hash,
                    setupobj.yournode.committed_block_hash,
                    setupobj.yournode.highest_qc_block_hash,
                    setupobj.user_privkey
                ]
            ).map_err(|_| DatabaseError::InsertError)?;

            dbg!("TX Commit");
            tx.commit().map_err(|_| DatabaseError::InsertError)?;
            
            Ok(())
        }
        Err(_) => Err(DatabaseError::LockError)
    }
}
