use super::*;

#[cfg(test)]
mod signature_tests {
    use super::*;

    #[test]
    fn test_valid_node_signature() {
        // Create a mock node with keypair
        let node = MockNode::new(0);

        // Create a transaction signed by this node
        let function = "test_function".to_string();
        let payload = b"test payload".to_vec();

        let tx = Transaction::new(function, payload, node.node_id, &node.signing_key)
            .expect("Failed to create transaction");

        // Verify the signature using the node's public key
        let result = tx.verify_signature(&node.verifying_key);

        // Assert that signature verification succeeds
        assert!(result.is_ok(), "Valid node signature should be accepted");
    }

    #[test]
    fn test_invalid_node_signature() {
        // Create two mock nodes with different keypairs
        let node1 = MockNode::new(0);
        let node2 = MockNode::new(1);

        // Create a transaction signed by node1
        let function = "test_function".to_string();
        let payload = b"test payload".to_vec();

        let tx = Transaction::new(function, payload, node1.node_id, &node1.signing_key)
            .expect("Failed to create transaction");

        // Try to verify the signature using node2's public key (should fail)
        let result = tx.verify_signature(&node2.verifying_key);

        // Assert that signature verification fails
        assert!(result.is_err(), "Invalid node signature should be rejected");
    }

    #[test]
    fn test_valid_user_signature() {
        // Create a mock node and user with keypairs
        let node = MockNode::new(0);
        let user = MockUser::new(100);

        // Create a transaction with both node and user signatures
        let function = "test_function".to_string();
        let payload = b"test payload".to_vec();

        let tx = Transaction::new_with_user(
            function,
            payload,
            node.node_id,
            &node.signing_key,
            user.user_id,
            &user.signing_key,
        )
        .expect("Failed to create transaction");

        // Verify both signatures
        let node_result = tx.verify_signature(&node.verifying_key);
        let user_result = tx.verify_user_signature(&user.verifying_key);

        // Assert that both signature verifications succeed
        assert!(
            node_result.is_ok(),
            "Valid node signature should be accepted"
        );
        assert!(
            user_result.is_ok(),
            "Valid user signature should be accepted"
        );
    }

    #[test]
    fn test_missing_user_signature() {
        // Create a node-only transaction (no user signature)
        let node = MockNode::new(0);

        let function = "test_function".to_string();
        let payload = b"test payload".to_vec();

        let tx = Transaction::new(function, payload, node.node_id, &node.signing_key)
            .expect("Failed to create transaction");

        // Verify that tx.user is None
        assert!(
            tx.user.is_none(),
            "Node-only transaction should have no user signature"
        );

        // Create a mock user pubkey to attempt verification
        let user = MockUser::new(100);

        // Attempting to verify user signature should fail (no user signature present)
        let result = tx.verify_user_signature(&user.verifying_key);

        assert!(result.is_err(), "Missing user signature should be rejected");
    }

    #[test]
    fn test_forged_signature() {
        // Attacker has user1's key but tries to impersonate user2
        let node = MockNode::new(0);
        let user1 = MockUser::new(100);
        let user2 = MockUser::new(101);

        let function = "test_function".to_string();
        let payload = b"test payload".to_vec();

        // Attacker creates transaction signed with user1's key but claims to be user2
        let tx = Transaction::new_with_user(
            function,
            payload,
            node.node_id,
            &node.signing_key,
            user2.user_id,      // Claims to be user2
            &user1.signing_key, // But signed with user1's key (forgery)
        )
        .expect("Failed to create transaction");

        // When we verify with user2's public key (the claimed identity), it should fail
        let result = tx.verify_user_signature(&user2.verifying_key);

        // Assert that forged signature is rejected
        assert!(result.is_err(), "Forged user signature should be rejected");
    }

    #[test]
    fn test_payload_tampering() {
        // Node creates valid transaction, then attacker modifies payload
        let node = MockNode::new(0);

        let function = "test_function".to_string();
        let payload = b"original payload".to_vec();

        let mut tx = Transaction::new(function, payload, node.node_id, &node.signing_key)
            .expect("Failed to create transaction");

        // Attacker modifies the payload after signing
        tx.rpc.payload = b"tampered payload".to_vec();

        // Signature verification should fail (signature doesn't match modified payload)
        let result = tx.verify_signature(&node.verifying_key);

        assert!(
            result.is_err(),
            "Tampered payload should invalidate signature"
        );
    }

    #[test]
    fn test_function_tampering() {
        // Node creates valid transaction, then attacker modifies function name
        let node = MockNode::new(0);

        let function = "original_function".to_string();
        let payload = b"test payload".to_vec();

        let mut tx = Transaction::new(function, payload, node.node_id, &node.signing_key)
            .expect("Failed to create transaction");

        // Attacker modifies the function name after signing
        tx.rpc.function = "tampered_function".to_string();

        // Signature verification should fail (signature doesn't match modified function)
        let result = tx.verify_signature(&node.verifying_key);

        assert!(
            result.is_err(),
            "Tampered function should invalidate signature"
        );
    }
}
