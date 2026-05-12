use super::*;
use crate::consensus::types::{
    Block, BlockData, ConsensusPhase, QuorumCertificate, VoteSignData, VoteSignMessage,
};

#[cfg(test)]
mod quorum_tests {
    use super::*;

    fn get_leader_for_view(network: &MockNetwork, view: i32) -> &MockNode {
        let num_validators = network.nodes.len();
        let leader_index = (view as usize) % num_validators;
        &network.nodes[leader_index]
    }

    fn create_vote_signatures(
        validators: &[MockNode],
        block: &Block,
        phase: ConsensusPhase,
        count: usize,
    ) -> Vec<VoteSignMessage> {
        validators
            .iter()
            .take(count)
            .map(|validator| {
                let vote_data = VoteSignData::from_block(block.clone(), phase);
                let signature = vote_data
                    .sign(&validator.signing_key)
                    .expect("Failed to sign vote");
                VoteSignMessage {
                    replica_id: validator.node_id,
                    signature,
                }
            })
            .collect()
    }

    #[test]
    fn test_quorum_insufficient_two_thirds() {
        // 7 nodes (BFT mode), only 5 vote (proposer + 4 voters) = exactly 5/7 (just under 2/3)
        // BFT quorum requires ((7*2)/3) + 1 = 5 signatures (more than 2/3)
        // With 5 signatures, we're exactly at threshold, should succeed
        // To test insufficient, use 4 signatures (proposer + 3 voters)
        // Expected: QC fails with ValidationError
        let network = MockNetwork::setup_with_validators(7);

        let leader = get_leader_for_view(&network, 1);
        let block = Block::new_tip(&leader.app_state, vec![])
            .expect("Leader should be able to create block");
        let phase = ConsensusPhase::Propose;

        // Get only 3 voters (insufficient for BFT quorum with 7 validators)
        let all_nodes_except_leader: Vec<MockNode> = network
            .nodes
            .iter()
            .filter(|n| n.node_id != leader.node_id)
            .cloned()
            .collect();
        let voter_signatures =
            create_vote_signatures(&all_nodes_except_leader, &block, phase, 3);

        let qc = QuorumCertificate::create_unverified(
            &block,
            phase,
            leader.node_id,
            &leader.signing_key,
            voter_signatures,
        )
        .expect("Failed to create QC");

        let result = qc.verify(&leader.app_state, &block);
        assert!(
            result.is_err(),
            "QC with 4/7 votes should fail in BFT mode (need 5)"
        );
        assert!(matches!(
            result.unwrap_err(),
            CertificateError::InsufficientVotes
        ));
    }

    #[test]
    fn test_quorum_relaxed_mode_accepts_simple_majority() {
        // 5 nodes (relaxed mode ≤6), exactly 3 signatures (proposer + 2 voters)
        // Relaxed mode quorum: (5/2) + 1 = 3 signatures (simple majority)
        // BFT mode would require: ((5*2)/3) + 1 = 4 signatures
        // This test proves relaxed mode is active: passes with 3, would fail with BFT
        let network = MockNetwork::setup_with_validators(5);

        let leader = get_leader_for_view(&network, 1);
        let block = Block::new_tip(&leader.app_state, vec![])
            .expect("Leader should be able to create block");
        let phase = ConsensusPhase::Propose;

        // Get exactly 2 voters (3 total with proposer = simple majority threshold)
        let all_nodes_except_leader: Vec<MockNode> = network
            .nodes
            .iter()
            .filter(|n| n.node_id != leader.node_id)
            .cloned()
            .collect();
        let voter_signatures =
            create_vote_signatures(&all_nodes_except_leader, &block, phase, 2);

        let qc = QuorumCertificate::create_unverified(
            &block,
            phase,
            leader.node_id,
            &leader.signing_key,
            voter_signatures,
        )
        .expect("Failed to create QC");

        let result = qc.verify(&leader.app_state, &block);
        assert!(
            result.is_ok(),
            "QC with simple majority (3/5) should succeed in relaxed mode: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_quorum_relaxed_mode_three_validators() {
        // 3 nodes (relaxed mode ≤6), exactly 2 signatures (proposer + 1 voter)
        // Relaxed mode quorum: (3/2) + 1 = 2 signatures (simple majority)
        // BFT mode would require: ((3*2)/3) + 1 = 3 signatures
        // This proves 3-validator networks can operate with 2-of-3 quorum
        let network = MockNetwork::setup_with_validators(3);

        let leader = get_leader_for_view(&network, 1);
        let block = Block::new_tip(&leader.app_state, vec![])
            .expect("Leader should be able to create block");
        let phase = ConsensusPhase::Propose;

        // Get exactly 1 voter (2 total with proposer = simple majority threshold)
        let all_nodes_except_leader: Vec<MockNode> = network
            .nodes
            .iter()
            .filter(|n| n.node_id != leader.node_id)
            .cloned()
            .collect();
        let voter_signatures =
            create_vote_signatures(&all_nodes_except_leader, &block, phase, 1);

        let qc = QuorumCertificate::create_unverified(
            &block,
            phase,
            leader.node_id,
            &leader.signing_key,
            voter_signatures,
        )
        .expect("Failed to create QC");

        let result = qc.verify(&leader.app_state, &block);
        assert!(
            result.is_ok(),
            "QC with simple majority (2/3) should succeed in relaxed mode: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_quorum_two_thirds_plus_one() {
        // 3 nodes, all 3 vote (proposer + 2 voters) = 3/3 votes
        // Quorum requires (3/2) + 1 = 2 signatures (relaxed mode)
        // Expected: QC succeeds (exceeds requirement)
        let network = MockNetwork::setup_with_validators(3);

        let leader = get_leader_for_view(&network, 1);
        let block = Block::new_tip(&leader.app_state, vec![])
            .expect("Leader should be able to create block");
        let phase = ConsensusPhase::Propose;

        // Get all non-leader nodes as voters (2 voters, exceeds 2-signature requirement)
        let all_nodes_except_leader: Vec<MockNode> = network
            .nodes
            .iter()
            .filter(|n| n.node_id != leader.node_id)
            .cloned()
            .collect();
        let voter_signatures =
            create_vote_signatures(&all_nodes_except_leader, &block, phase, 2);

        let qc = QuorumCertificate::create_unverified(
            &block,
            phase,
            leader.node_id,
            &leader.signing_key,
            voter_signatures,
        )
        .expect("Failed to create QC");

        let result = qc.verify(&leader.app_state, &block);
        assert!(
            result.is_ok(),
            "QC with all validators should succeed: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_quorum_insufficient_only_proposer() {
        // 3 nodes, only proposer votes (1/3)
        // Expected: QC fails with ValidationError
        let network = MockNetwork::setup_with_validators(3);

        let leader = get_leader_for_view(&network, 1);
        let block = Block::new_tip(&leader.app_state, vec![])
            .expect("Leader should be able to create block");
        let phase = ConsensusPhase::Propose;

        let voter_signatures = vec![];

        let qc = QuorumCertificate::create_unverified(
            &block,
            phase,
            leader.node_id,
            &leader.signing_key,
            voter_signatures,
        )
        .expect("Failed to create QC");

        let result = qc.verify(&leader.app_state, &block);
        assert!(result.is_err(), "QC with only proposer should fail");
        assert!(matches!(
            result.unwrap_err(),
            CertificateError::InsufficientVotes
        ));
    }

    #[test]
    fn test_quorum_rounding_insufficient() {
        // 10 nodes (BFT mode), only 6 signatures (proposer + 5 voters)
        // BFT quorum requires ((10*2)/3) + 1 = 7 signatures
        // Tests that rounding down still requires 7, not 6
        // Expected: QC fails with ValidationError
        let network = MockNetwork::setup_with_validators(10);

        let leader = get_leader_for_view(&network, 1);
        let block = Block::new_tip(&leader.app_state, vec![])
            .expect("Leader should be able to create block");
        let phase = ConsensusPhase::Propose;

        // Get only 5 voters (insufficient for BFT quorum with 10 validators)
        let all_nodes_except_leader: Vec<MockNode> = network
            .nodes
            .iter()
            .filter(|n| n.node_id != leader.node_id)
            .cloned()
            .collect();
        let voter_signatures =
            create_vote_signatures(&all_nodes_except_leader, &block, phase, 5);

        let qc = QuorumCertificate::create_unverified(
            &block,
            phase,
            leader.node_id,
            &leader.signing_key,
            voter_signatures,
        )
        .expect("Failed to create QC");

        let result = qc.verify(&leader.app_state, &block);
        assert!(
            result.is_err(),
            "QC with 6 signatures should fail when 7 required in BFT mode"
        );
        assert!(matches!(
            result.unwrap_err(),
            CertificateError::InsufficientVotes
        ));
    }

    #[test]
    fn test_quorum_rounding_sufficient() {
        // 10 nodes (BFT mode), quorum requires ((10*2)/3) + 1 = 7 signatures (proposer + 6 voters)
        // Tests rounding down from 6.66... voters to 6 voters needed
        // Expected: QC succeeds with 7 signatures
        let network = MockNetwork::setup_with_validators(10);

        let leader = get_leader_for_view(&network, 1);
        let block = Block::new_tip(&leader.app_state, vec![])
            .expect("Leader should be able to create block");
        let phase = ConsensusPhase::Propose;

        // Get 6 voters (enough for BFT quorum with 10 validators)
        let all_nodes_except_leader: Vec<MockNode> = network
            .nodes
            .iter()
            .filter(|n| n.node_id != leader.node_id)
            .cloned()
            .collect();
        let voter_signatures =
            create_vote_signatures(&all_nodes_except_leader, &block, phase, 6);

        let qc = QuorumCertificate::create_unverified(
            &block,
            phase,
            leader.node_id,
            &leader.signing_key,
            voter_signatures,
        )
        .expect("Failed to create QC");

        let result = qc.verify(&leader.app_state, &block);
        assert!(
            result.is_ok(),
            "QC with BFT quorum should succeed: {:?}",
            result.err()
        );
    }
}
