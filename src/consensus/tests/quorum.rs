use super::*;
use crate::consensus::types::{Block, BlockData, QuorumCertificate, VoteSignData, VoteSignMessage, ConsensusPhase};

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
        validators.iter()
            .take(count)
            .map(|validator| {
                let vote_data = VoteSignData::from_block(block.clone(), phase.clone());
                let signature = vote_data.sign(&validator.signing_key)
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
        // 3 nodes, only 2 vote (proposer + 1 voter) = exactly 2/3
        // Quorum requires (3*2)/3 + 1 = 3 signatures (more than 2/3)
        // Expected: QC fails with InsufficientVotes
        let network = MockNetwork::setup_with_validators(3);

        let leader = get_leader_for_view(&network, 1);
        let block = Block::new_tip(&leader.app_state, vec![])
            .expect("Leader should be able to create block");
        let phase = ConsensusPhase::Propose;

        // Get only 1 voter (insufficient for quorum)
        let all_nodes_except_leader: Vec<MockNode> = network.nodes.iter()
            .filter(|n| n.node_id != leader.node_id)
            .cloned()
            .collect();
        let voter_signatures = create_vote_signatures(&all_nodes_except_leader, &block, phase.clone(), 1);

        let qc = QuorumCertificate::create(
            &block,
            phase.clone(),
            leader.node_id,
            &leader.signing_key,
            voter_signatures,
        ).expect("Failed to create QC");

        let result = qc.verify(&leader.app_state, &block);
        assert!(result.is_err(), "QC with exactly 2/3 votes should fail");
        assert!(matches!(result.unwrap_err(), CertificateError::ValidationError));
    }

    #[test]
    fn test_quorum_two_thirds_plus_one() {
        // 3 nodes, all 3 vote (proposer + 2 voters) = 3/3 votes
        // Quorum requires (3*2)/3 + 1 = 3 signatures
        // Expected: QC succeeds
        let network = MockNetwork::setup_with_validators(3);

        let leader = get_leader_for_view(&network, 1);
        let block = Block::new_tip(&leader.app_state, vec![])
            .expect("Leader should be able to create block");
        let phase = ConsensusPhase::Propose;

        // Get all non-leader nodes as voters (2 voters for quorum)
        let all_nodes_except_leader: Vec<MockNode> = network.nodes.iter()
            .filter(|n| n.node_id != leader.node_id)
            .cloned()
            .collect();
        let voter_signatures = create_vote_signatures(&all_nodes_except_leader, &block, phase.clone(), 2);

        let qc = QuorumCertificate::create(
            &block,
            phase.clone(),
            leader.node_id,
            &leader.signing_key,
            voter_signatures,
        ).expect("Failed to create QC");

        let result = qc.verify(&leader.app_state, &block);
        assert!(result.is_ok(), "QC with 2/3+1 votes should succeed: {:?}", result.err());
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

        let qc = QuorumCertificate::create(
            &block,
            phase,
            leader.node_id,
            &leader.signing_key,
            voter_signatures,
        ).expect("Failed to create QC");

        let result = qc.verify(&leader.app_state, &block);
        assert!(result.is_err(), "QC with only proposer should fail");
        assert!(matches!(result.unwrap_err(), CertificateError::ValidationError));
    }

    #[test]
    fn test_quorum_rounding_insufficient() {
        // 5 nodes, only 3 signatures (proposer + 2 voters)
        // Quorum requires (5*2)/3 + 1 = 4 signatures
        // Tests that rounding down still requires 4, not 3
        // Expected: QC fails with ValidationError
        let network = MockNetwork::setup_with_validators(5);

        let leader = get_leader_for_view(&network, 1);
        let block = Block::new_tip(&leader.app_state, vec![])
            .expect("Leader should be able to create block");
        let phase = ConsensusPhase::Propose;

        // Get only 2 voters (insufficient for quorum with 5 validators)
        let all_nodes_except_leader: Vec<MockNode> = network.nodes.iter()
            .filter(|n| n.node_id != leader.node_id)
            .cloned()
            .collect();
        let voter_signatures = create_vote_signatures(&all_nodes_except_leader, &block, phase.clone(), 2);

        let qc = QuorumCertificate::create(
            &block,
            phase,
            leader.node_id,
            &leader.signing_key,
            voter_signatures,
        ).expect("Failed to create QC");

        let result = qc.verify(&leader.app_state, &block);
        assert!(result.is_err(), "QC with 3 signatures should fail when 4 required");
        assert!(matches!(result.unwrap_err(), CertificateError::ValidationError));
    }

    #[test]
    fn test_quorum_rounding_sufficient() {
        // 5 nodes, quorum requires (5*2)/3 + 1 = 4 signatures (proposer + 3 voters)
        // Tests rounding down from 3.33... voters to 3 voters needed
        // Expected: QC succeeds with 4 signatures
        let network = MockNetwork::setup_with_validators(5);

        let leader = get_leader_for_view(&network, 1);
        let block = Block::new_tip(&leader.app_state, vec![])
            .expect("Leader should be able to create block");
        let phase = ConsensusPhase::Propose;

        // Get 3 voters (enough for quorum with 5 validators)
        let all_nodes_except_leader: Vec<MockNode> = network.nodes.iter()
            .filter(|n| n.node_id != leader.node_id)
            .cloned()
            .collect();
        let voter_signatures = create_vote_signatures(&all_nodes_except_leader, &block, phase.clone(), 3);

        let qc = QuorumCertificate::create(
            &block,
            phase,
            leader.node_id,
            &leader.signing_key,
            voter_signatures,
        ).expect("Failed to create QC");

        let result = qc.verify(&leader.app_state, &block);
        assert!(result.is_ok(), "QC with rounded quorum should succeed: {:?}", result.err());
    }
}