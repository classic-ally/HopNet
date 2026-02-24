use super::*;
use crate::consensus::types::{Block, QuorumCertificate, VoteSignData, VoteSignMessage, ConsensusPhase, CertificateError};

#[cfg(test)]
mod byzantine_tests {
    use super::*;

    fn get_leader_for_view(network: &MockNetwork, view: i32) -> &MockNode {
        let num_validators = network.nodes.len();
        let leader_index = (view as usize) % num_validators;
        &network.nodes[leader_index]
    }

    #[test]
    fn test_duplicate_voter_signatures() {
        // Byzantine attack: Include the same voter's signature twice in QC
        // This artificially inflates vote count to reach quorum
        // Expected: Should either be caught or at least not count duplicate votes
        let network = MockNetwork::setup_with_validators(4);

        let leader = get_leader_for_view(&network, 1);
        let block = Block::new_tip(&leader.app_state, vec![])
            .expect("Leader should be able to create block");
        let phase = ConsensusPhase::Propose;

        // For 4 validators, need (4*2)/3 + 1 = 3 signatures
        // Get only 1 legitimate voter
        let legitimate_voter = network.nodes.iter()
            .find(|n| n.node_id != leader.node_id)
            .unwrap();

        // Create signature from legitimate voter
        let vote_data = VoteSignData::from_block(block.clone(), phase.clone());
        let signature = vote_data.sign(&legitimate_voter.signing_key)
            .expect("Failed to sign vote");
        let voter_sig = VoteSignMessage {
            replica_id: legitimate_voter.node_id,
            signature,
        };

        // Byzantine attack: Include the same signature twice to fake quorum
        let voter_signatures = vec![voter_sig.clone(), voter_sig.clone()];

        let qc = QuorumCertificate::create_unverified(
            &block,
            phase.clone(),
            leader.node_id,
            &leader.signing_key,
            voter_signatures,
        ).expect("Failed to create QC");

        // This QC has proposer + 2 duplicate voter sigs = appears to be 3 signatures
        // But after deduplication: only 2 unique nodes voted (proposer + 1 voter)
        // For 4 validators, need 3 unique signatures - so this should fail
        let result = qc.verify(&leader.app_state, &block);

        // After deduplication, we have: 1 proposer + 1 unique voter = 2 signatures
        // But we need (4*2)/3 + 1 = 3 signatures
        // So this should fail with InsufficientVotes due to insufficient unique signatures
        assert!(result.is_err(), "QC with duplicate signatures should fail after deduplication");
        if let Err(e) = &result {
            eprintln!("Actual error: {:?}", e);
        }
        assert!(matches!(result.unwrap_err(), CertificateError::InsufficientVotes),
            "Should fail with InsufficientVotes due to insufficient unique signatures");
    }

    #[test]
    fn test_forged_voter_signature() {
        // Byzantine attack: QC claims node X voted, but signature is from node Y's key
        // This tests that signature verification catches mismatched keys
        // Expected: Batch signature verification should fail
        let network = MockNetwork::setup_with_validators(4);

        let leader = get_leader_for_view(&network, 1);
        let block = Block::new_tip(&leader.app_state, vec![])
            .expect("Leader should be able to create block");
        let phase = ConsensusPhase::Propose;

        // Get two non-leader nodes
        let voters: Vec<MockNode> = network.nodes.iter()
            .filter(|n| n.node_id != leader.node_id)
            .cloned()
            .collect();

        let victim_node = &voters[0];
        let attacker_node = &voters[1];

        // Create a forged signature: claim it's from victim, but sign with attacker's key
        let vote_data = VoteSignData::from_block(block.clone(), phase.clone());
        let forged_signature = vote_data.sign(&attacker_node.signing_key)
            .expect("Failed to sign vote");

        let forged_voter_sig = VoteSignMessage {
            replica_id: victim_node.node_id,  // Claims to be victim
            signature: forged_signature,       // But signed with attacker's key
        };

        // Create one legitimate voter signature (so we have enough count)
        let legitimate_signature = vote_data.sign(&attacker_node.signing_key)
            .expect("Failed to sign vote");
        let legitimate_voter_sig = VoteSignMessage {
            replica_id: attacker_node.node_id,
            signature: legitimate_signature,
        };

        let voter_signatures = vec![forged_voter_sig, legitimate_voter_sig];

        let qc = QuorumCertificate::create_unverified(
            &block,
            phase.clone(),
            leader.node_id,
            &leader.signing_key,
            voter_signatures,
        ).expect("Failed to create QC");

        // Verification should fail - the forged signature won't verify against victim's pubkey
        let result = qc.verify(&leader.app_state, &block);

        assert!(result.is_err(), "QC with forged signature should fail verification");
        assert!(matches!(result.unwrap_err(), CertificateError::ValidationError),
            "Should fail with ValidationError due to signature mismatch");
    }

    #[test]
    fn test_non_validator_vote() {
        // Byzantine attack: Include a vote from a node that exists in the network
        // but is NOT in the validator set for this height
        // Expected: Should fail with SignerNotFound because get_validators() won't include it
        let network = MockNetwork::setup_with_validators(4);

        let leader = get_leader_for_view(&network, 1);
        let block = Block::new_tip(&leader.app_state, vec![])
            .expect("Leader should be able to create block");
        let phase = ConsensusPhase::Propose;

        // Create a rogue node and add it to nodes table (but NOT validators table)
        let rogue_node = MockNode::new(999);

        // Add rogue node to all network nodes' databases (but NOT as a validator)
        for node in &network.nodes {
            let db = node.app_state.db_pool.get().expect("Failed to get DB");
            db.execute(
                "INSERT INTO nodes (node_id, name, owner, pubkey) VALUES (?, ?, ?, ?)",
                rusqlite::params![
                    rogue_node.node_id,
                    format!("rogue_node_{}", rogue_node.node_id),
                    0,
                    rogue_node.verifying_key
                ]
            ).expect("Failed to insert rogue node");
        }

        // Get one legitimate voter
        let legitimate_voter = network.nodes.iter()
            .find(|n| n.node_id != leader.node_id)
            .unwrap();

        // Create signatures
        let vote_data = VoteSignData::from_block(block.clone(), phase.clone());

        let rogue_signature = vote_data.sign(&rogue_node.signing_key)
            .expect("Failed to sign vote");
        let rogue_voter_sig = VoteSignMessage {
            replica_id: rogue_node.node_id,
            signature: rogue_signature,
        };

        let legitimate_signature = vote_data.sign(&legitimate_voter.signing_key)
            .expect("Failed to sign vote");
        let legitimate_voter_sig = VoteSignMessage {
            replica_id: legitimate_voter.node_id,
            signature: legitimate_signature,
        };

        let voter_signatures = vec![rogue_voter_sig, legitimate_voter_sig];

        let qc = QuorumCertificate::create_unverified(
            &block,
            phase.clone(),
            leader.node_id,
            &leader.signing_key,
            voter_signatures,
        ).expect("Failed to create QC");

        // Verification should fail - rogue node exists in nodes table but NOT in validators
        // get_validators() won't return it, so the rogue vote is filtered out
        // After filtering: only 2 valid signatures (proposer + 1 legitimate voter)
        // For 4 validators, need 3 signatures, so this fails with InsufficientVotes
        let result = qc.verify(&leader.app_state, &block);

        assert!(result.is_err(), "QC with non-validator vote should fail");
        assert!(matches!(result.unwrap_err(), CertificateError::InsufficientVotes),
            "Should fail with InsufficientVotes after filtering out non-validator vote");
    }

    #[test]
    fn test_forged_node_signature() {
        // Transaction claims node_id=0 but signed with node_id=1's key
        // Expected: Signature verification fails, block rejected
        let network = MockNetwork::setup_with_validators(3);

        let node0 = &network.nodes[0];
        let node1 = &network.nodes[1];

        // Create transaction claiming to be from node0, but signed with node1's key
        let rpc = crate::consensus::types::RpcCall {
            function: "test_function".to_string(),
            payload: vec![1, 2, 3],
        };

        let forged_signature = rpc.sign(&node1.signing_key)
            .expect("Failed to sign with node1's key");

        let forged_tx = crate::consensus::types::Transaction {
            rpc,
            submitter: crate::consensus::types::SignedIdentity {
                id: node0.node_id,  // Claims to be node0
                signature: forged_signature,  // But signed with node1's key
            },
            user: None,
            nonce: hopnet_common::CustomUUID::new(None),
        };

        // Try to create block with forged transaction
        let result = Block::new_tip(&node0.app_state, vec![forged_tx]);

        // Block creation should fail due to signature verification
        assert!(result.is_err(), "Block with forged node signature should be rejected");
        assert!(matches!(result.unwrap_err(), crate::consensus::types::BlockError::ValidationError),
            "Should fail with ValidationError due to forged signature");
    }

    #[test]
    fn test_forged_user_signature() {
        // Transaction claims user_id=0 but signed with user_id=1's key
        // Expected: Signature verification fails, block rejected
        let network = MockNetwork::setup_with_validators(3);

        let node = &network.nodes[0];
        let user0 = &network.users[0];

        // Create a second user for the attack
        let user1 = MockUser::new(1);

        // Add user1 to the database so pubkey lookup works
        for net_node in &network.nodes {
            let db = net_node.app_state.db_pool.get().expect("Failed to get DB");
            let x25519_pubkey = crate::auth::derive_x25519_pubkey_from_user(&user1.signing_key);
            db.execute(
                "INSERT INTO users (user_id, username, pubkey, x25519_pubkey, encrypted_privkey, key_salt) VALUES (?, ?, ?, ?, ?, ?)",
                rusqlite::params![
                    user1.user_id,
                    format!("user_{}", user1.user_id),
                    user1.verifying_key,
                    x25519_pubkey,
                    vec![0u8; 44],  // dummy encrypted_privkey for test
                    vec![0u8; 16]   // dummy key_salt for test
                ]
            ).expect("Failed to insert user1");
        }

        // Create transaction claiming to be from user0, but with user1's signature
        let rpc = crate::consensus::types::RpcCall {
            function: "test_function".to_string(),
            payload: vec![1, 2, 3],
        };

        let node_signature = rpc.sign(&node.signing_key)
            .expect("Failed to sign with node key");
        let forged_user_signature = rpc.sign(&user1.signing_key)
            .expect("Failed to sign with user1's key");

        let forged_tx = crate::consensus::types::Transaction {
            rpc,
            submitter: crate::consensus::types::SignedIdentity {
                id: node.node_id,
                signature: node_signature,
            },
            user: Some(crate::consensus::types::SignedIdentity {
                id: user0.user_id,  // Claims to be user0
                signature: forged_user_signature,  // But signed with user1's key
            }),
            nonce: hopnet_common::CustomUUID::new(None),
        };

        // Try to create block with forged user signature
        let result = Block::new_tip(&node.app_state, vec![forged_tx]);

        // Block creation should fail due to signature verification
        assert!(result.is_err(), "Block with forged user signature should be rejected");
        assert!(matches!(result.unwrap_err(), crate::consensus::types::BlockError::ValidationError),
            "Should fail with ValidationError due to forged user signature");
    }

    #[test]
    fn test_one_invalid_tx_rejects_block() {
        // Block with 5 valid transactions and 1 with invalid signature
        // Expected: Entire block rejected (all-or-nothing)
        let network = MockNetwork::setup_with_validators(3);

        let leader = get_leader_for_view(&network, 1);
        let phase = ConsensusPhase::Propose;

        // Create 5 valid transactions
        let mut transactions = Vec::new();
        for i in 0..5 {
            let tx = crate::consensus::types::Transaction::new(
                format!("valid_function_{}", i),
                vec![i as u8; 10],
                leader.node_id,
                &leader.signing_key,
            ).expect("Failed to create valid transaction");
            transactions.push(tx);
        }

        // Create 1 invalid transaction (forged signature)
        let other_node = network.nodes.iter().find(|n| n.node_id != leader.node_id).unwrap();
        let mut invalid_tx = crate::consensus::types::Transaction::new(
            "invalid_function".to_string(),
            vec![99; 10],
            leader.node_id,
            &leader.signing_key,
        ).expect("Failed to create transaction");

        // Corrupt the signature by signing with wrong key
        let wrong_signature = invalid_tx.rpc.sign(&other_node.signing_key)
            .expect("Failed to sign with wrong key");
        invalid_tx.submitter.signature = wrong_signature;

        transactions.push(invalid_tx);

        // Try to create block with mixed valid/invalid transactions
        let result = Block::new_tip(&leader.app_state, transactions);

        // Block creation should fail because of the invalid transaction
        assert!(result.is_err(), "Block with invalid transaction should be rejected");
        assert!(matches!(result.unwrap_err(), crate::consensus::types::BlockError::ValidationError),
            "Should fail with ValidationError due to invalid transaction signature");
    }

}