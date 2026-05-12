use super::*;
use crate::consensus::functions::process_transaction;
use crate::db::CustomDateTime;
use crate::db::DatabaseError;
use crate::db::imports::ImportPayload;
use crate::db::takeout::TakeoutPayload;
use crate::files::handlers::{DeleteFilesPayload, ModifyItemPayload};
use crate::metrics::types::Metric;
use either::Either;
use hopnet_common::{ImportStatus, TakeoutStatus};

#[cfg(test)]
mod authorization_tests {
    use super::*;

    #[test]
    fn test_user_authorized_insert_files() {
        // User creates transaction with matching user_id in payload
        // Node provides valid signature (but node_id doesn't need to match payload)
        // Tests: insert_files
        // Expected: Authorization succeeds
        let app_state = create_test_app_state();
        let node = MockNode::new(0);
        let user = MockUser::new(100);

        let function = "insert_files".to_string();

        let inode = crate::db::Inode {
            id: crate::db::CustomUUID::new(None),
            owner: Either::Left(user.user_id),
            path: "test/path".to_string(),
            inode_type: hopnet_common::InodeType::File,
            data_id: None,
        };

        let payload = bincode::serde::encode_to_vec(&vec![inode], bincode::config::standard())
            .expect("Failed to encode payload");

        let tx = Transaction::new_with_user(
            function,
            payload,
            node.node_id,
            &node.signing_key,
            user.user_id,
            &user.signing_key,
        )
        .expect("Failed to create transaction");

        let mut conn = app_state
            .db_pool
            .get()
            .expect("Failed to get DB connection");
        let db_tx = conn.transaction().expect("Failed to start transaction");
        let result = process_transaction(&tx, &app_state, false, &db_tx);
        if let Err(e) = &result {
            assert!(
                !matches!(e, DatabaseError::AuthorizationError),
                "insert_files should not fail authorization but got: {:?}",
                e
            );
        }
        let _ = db_tx.rollback();
    }

    #[test]
    fn test_user_authorized_modify_item() {
        // User creates transaction with matching user_id in payload
        // Tests: modify_item
        // Expected: Authorization succeeds
        let app_state = create_test_app_state();
        let node = MockNode::new(0);
        let user = MockUser::new(100);

        let function = "modify_item".to_string();

        let payload_data = ModifyItemPayload {
            user_id: user.user_id,
            inode_id: crate::db::CustomUUID::new(None),
            new_encrypted_path: Some("new/path".to_string()),
            new_data_block_id: None,
            new_data_record: None,
            incoming_share_updates: None,
        };

        let payload = bincode::serde::encode_to_vec(&payload_data, bincode::config::standard())
            .expect("Failed to encode payload");

        let tx = Transaction::new_with_user(
            function,
            payload,
            node.node_id,
            &node.signing_key,
            user.user_id,
            &user.signing_key,
        )
        .expect("Failed to create transaction");

        let mut conn = app_state
            .db_pool
            .get()
            .expect("Failed to get DB connection");
        let db_tx = conn.transaction().expect("Failed to start transaction");
        let result = process_transaction(&tx, &app_state, false, &db_tx);
        if let Err(e) = &result {
            assert!(
                !matches!(e, DatabaseError::AuthorizationError),
                "modify_item should not fail authorization but got: {:?}",
                e
            );
        }
        let _ = db_tx.rollback();
    }

    #[test]
    fn test_user_authorized_delete_files() {
        // User creates transaction with matching user_id in payload
        // Tests: delete_files
        // Expected: Authorization succeeds
        let app_state = create_test_app_state();
        let node = MockNode::new(0);
        let user = MockUser::new(100);

        let function = "delete_files".to_string();

        let payload_data = DeleteFilesPayload {
            encrypted_path: "test/path".to_string(),
            user_id: user.user_id,
        };

        let payload = bincode::serde::encode_to_vec(&payload_data, bincode::config::standard())
            .expect("Failed to encode payload");

        let tx = Transaction::new_with_user(
            function,
            payload,
            node.node_id,
            &node.signing_key,
            user.user_id,
            &user.signing_key,
        )
        .expect("Failed to create transaction");

        let mut conn = app_state
            .db_pool
            .get()
            .expect("Failed to get DB connection");
        let db_tx = conn.transaction().expect("Failed to start transaction");
        let result = process_transaction(&tx, &app_state, false, &db_tx);
        if let Err(e) = &result {
            assert!(
                !matches!(e, DatabaseError::AuthorizationError),
                "delete_files should not fail authorization but got: {:?}",
                e
            );
        }
        let _ = db_tx.rollback();
    }

    #[test]
    fn test_user_unauthorized_insert_files() {
        // User creates transaction with DIFFERENT user_id in payload
        // Node provides valid signature
        // Tests: insert_files
        // Expected: AuthorizationError
        let app_state = create_test_app_state();
        let node = MockNode::new(0);
        let user1 = MockUser::new(100);
        let user2_id = 101;

        let function = "insert_files".to_string();

        let inode = crate::db::Inode {
            id: crate::db::CustomUUID::new(None),
            owner: Either::Left(user2_id),
            path: "test/path".to_string(),
            inode_type: hopnet_common::InodeType::File,
            data_id: None,
        };

        let payload = bincode::serde::encode_to_vec(&vec![inode], bincode::config::standard())
            .expect("Failed to encode payload");

        let tx = Transaction::new_with_user(
            function,
            payload,
            node.node_id,
            &node.signing_key,
            user1.user_id,
            &user1.signing_key,
        )
        .expect("Failed to create transaction");

        let mut conn = app_state
            .db_pool
            .get()
            .expect("Failed to get DB connection");
        let db_tx = conn.transaction().expect("Failed to start transaction");
        let result = process_transaction(&tx, &app_state, false, &db_tx);
        assert!(result.is_err(), "Unauthorized user transaction should fail");
        assert!(matches!(
            result.unwrap_err(),
            DatabaseError::AuthorizationError
        ));
        let _ = db_tx.rollback();
    }

    #[test]
    fn test_user_unauthorized_modify_item() {
        // User creates transaction with DIFFERENT user_id in payload
        // Tests: modify_item
        // Expected: AuthorizationError
        let app_state = create_test_app_state();
        let node = MockNode::new(0);
        let user1 = MockUser::new(100);
        let user2_id = 101;

        let function = "modify_item".to_string();

        let payload_data = ModifyItemPayload {
            user_id: user2_id,
            inode_id: crate::db::CustomUUID::new(None),
            new_encrypted_path: Some("new/path".to_string()),
            new_data_block_id: None,
            new_data_record: None,
            incoming_share_updates: None,
        };

        let payload = bincode::serde::encode_to_vec(&payload_data, bincode::config::standard())
            .expect("Failed to encode payload");

        let tx = Transaction::new_with_user(
            function,
            payload,
            node.node_id,
            &node.signing_key,
            user1.user_id,
            &user1.signing_key,
        )
        .expect("Failed to create transaction");

        let mut conn = app_state
            .db_pool
            .get()
            .expect("Failed to get DB connection");
        let db_tx = conn.transaction().expect("Failed to start transaction");
        let result = process_transaction(&tx, &app_state, false, &db_tx);
        assert!(
            result.is_err(),
            "Mismatched user ID should fail authorization"
        );
        assert!(matches!(
            result.unwrap_err(),
            DatabaseError::AuthorizationError
        ));
        let _ = db_tx.rollback();
    }

    #[test]
    fn test_user_unauthorized_delete_files() {
        // User creates transaction with DIFFERENT user_id in payload
        // Tests: delete_files
        // Expected: AuthorizationError
        let app_state = create_test_app_state();
        let node = MockNode::new(0);
        let user1 = MockUser::new(100);
        let user2_id = 101;

        let function = "delete_files".to_string();

        let payload_data = DeleteFilesPayload {
            encrypted_path: "test/path".to_string(),
            user_id: user2_id,
        };

        let payload = bincode::serde::encode_to_vec(&payload_data, bincode::config::standard())
            .expect("Failed to encode payload");

        let tx = Transaction::new_with_user(
            function,
            payload,
            node.node_id,
            &node.signing_key,
            user1.user_id,
            &user1.signing_key,
        )
        .expect("Failed to create transaction");

        let mut conn = app_state
            .db_pool
            .get()
            .expect("Failed to get DB connection");
        let db_tx = conn.transaction().expect("Failed to start transaction");
        let result = process_transaction(&tx, &app_state, false, &db_tx);
        assert!(result.is_err(), "Unauthorized delete should fail");
        assert!(matches!(
            result.unwrap_err(),
            DatabaseError::AuthorizationError
        ));
        let _ = db_tx.rollback();
    }

    #[test]
    fn test_node_authorized() {
        // Node creates transaction with matching node_id in payload
        // No user signature (automated operation)
        // Tests: submit_metrics
        // Expected: Operation succeeds
        let app_state = create_test_app_state();
        let node = MockNode::new(0);

        let function = "submit_metrics".to_string();

        let metric = Metric {
            from_node: node.node_id,
            to_node: 1,
            start_time: chrono::Utc::now(),
            rtt_latency: Some(10.5),
            rtt_variance: None,
            rtt_jitter: None,
            throughput: None,
            height: 0,
            available: true,
            storage_total_gb: None,
            storage_used_gb: None,
        };

        let payload = bincode::serde::encode_to_vec(&vec![metric], bincode::config::standard())
            .expect("Failed to encode payload");

        let tx = Transaction::new(function, payload, node.node_id, &node.signing_key)
            .expect("Failed to create transaction");

        let mut conn = app_state
            .db_pool
            .get()
            .expect("Failed to get DB connection");
        let db_tx = conn.transaction().expect("Failed to start transaction");
        let result = process_transaction(&tx, &app_state, false, &db_tx);
        if let Err(e) = &result {
            assert!(
                !matches!(e, DatabaseError::AuthorizationError),
                "Should not fail authorization but got: {:?}",
                e
            );
        }
        let _ = db_tx.rollback();
    }

    #[test]
    fn test_node_unauthorized() {
        // Node creates transaction with DIFFERENT node_id in payload
        // No user signature (automated operation)
        // Tests: submit_metrics
        // Expected: Operation fails with AuthorizationError
        let app_state = create_test_app_state();
        let node1 = MockNode::new(0);
        let node2_id = 1;

        let function = "submit_metrics".to_string();

        let metric = Metric {
            from_node: node2_id,
            to_node: 2,
            start_time: chrono::Utc::now(),
            rtt_latency: Some(10.5),
            rtt_variance: None,
            rtt_jitter: None,
            throughput: None,
            height: 0,
            available: true,
            storage_total_gb: None,
            storage_used_gb: None,
        };

        let payload = bincode::serde::encode_to_vec(&vec![metric], bincode::config::standard())
            .expect("Failed to encode payload");

        let tx = Transaction::new(function, payload, node1.node_id, &node1.signing_key)
            .expect("Failed to create transaction");

        let mut conn = app_state
            .db_pool
            .get()
            .expect("Failed to get DB connection");
        let db_tx = conn.transaction().expect("Failed to start transaction");
        let result = process_transaction(&tx, &app_state, false, &db_tx);
        assert!(result.is_err(), "Unauthorized node transaction should fail");
        assert!(matches!(
            result.unwrap_err(),
            DatabaseError::AuthorizationError
        ));
        let _ = db_tx.rollback();
    }

    #[test]
    fn test_dual_authorized_create_takeout() {
        // User AND node both match their respective payload fields
        // tx.user.id == payload.user_id AND tx.submitter.id == payload.owner_node_id
        // Tests: create_takeout
        // Expected: Authorization succeeds
        let app_state = create_test_app_state();
        let node = MockNode::new(0);
        let user = MockUser::new(100);

        let function = "create_takeout".to_string();

        let payload_data = TakeoutPayload {
            takeout_id: crate::db::CustomUUID::new(None),
            user_id: user.user_id,
            owner_node_id: node.node_id,
            status: TakeoutStatus::Pending,
            expires_at: CustomDateTime::new(chrono::Utc::now() + chrono::Duration::hours(24)),
            consensus_height: 0,
        };

        let payload = bincode::serde::encode_to_vec(&payload_data, bincode::config::standard())
            .expect("Failed to encode payload");

        let tx = Transaction::new_with_user(
            function,
            payload,
            node.node_id,
            &node.signing_key,
            user.user_id,
            &user.signing_key,
        )
        .expect("Failed to create transaction");

        let mut conn = app_state
            .db_pool
            .get()
            .expect("Failed to get DB connection");
        let db_tx = conn.transaction().expect("Failed to start transaction");
        let result = process_transaction(&tx, &app_state, false, &db_tx);
        if let Err(e) = &result {
            assert!(
                !matches!(e, DatabaseError::AuthorizationError),
                "create_takeout should not fail authorization but got: {:?}",
                e
            );
        }
        let _ = db_tx.rollback();
    }

    #[test]
    fn test_dual_user_unauthorized_create_takeout() {
        // User ID mismatch but node ID matches
        // tx.user.id != payload.user_id BUT tx.submitter.id == payload.owner_node_id
        // Tests: create_takeout
        // Expected: AuthorizationError
        let app_state = create_test_app_state();
        let node = MockNode::new(0);
        let user1 = MockUser::new(100);
        let user2_id = 101;

        let function = "create_takeout".to_string();

        let payload_data = TakeoutPayload {
            takeout_id: crate::db::CustomUUID::new(None),
            user_id: user2_id,
            owner_node_id: node.node_id,
            status: TakeoutStatus::Pending,
            expires_at: CustomDateTime::new(chrono::Utc::now() + chrono::Duration::hours(24)),
            consensus_height: 0,
        };

        let payload = bincode::serde::encode_to_vec(&payload_data, bincode::config::standard())
            .expect("Failed to encode payload");

        let tx = Transaction::new_with_user(
            function,
            payload,
            node.node_id,
            &node.signing_key,
            user1.user_id,
            &user1.signing_key,
        )
        .expect("Failed to create transaction");

        let mut conn = app_state
            .db_pool
            .get()
            .expect("Failed to get DB connection");
        let db_tx = conn.transaction().expect("Failed to start transaction");
        let result = process_transaction(&tx, &app_state, false, &db_tx);
        assert!(
            result.is_err(),
            "Mismatched user ID should fail authorization"
        );
        assert!(matches!(
            result.unwrap_err(),
            DatabaseError::AuthorizationError
        ));
        let _ = db_tx.rollback();
    }

    #[test]
    fn test_dual_node_unauthorized_create_takeout() {
        // Node ID mismatch but user ID matches
        // tx.user.id == payload.user_id BUT tx.submitter.id != payload.owner_node_id
        // Tests: create_takeout
        // Expected: AuthorizationError
        let app_state = create_test_app_state();
        let node1 = MockNode::new(0);
        let node2_id = 1;
        let user = MockUser::new(100);

        let function = "create_takeout".to_string();

        let payload_data = TakeoutPayload {
            takeout_id: crate::db::CustomUUID::new(None),
            user_id: user.user_id,
            owner_node_id: node2_id,
            status: TakeoutStatus::Pending,
            expires_at: CustomDateTime::new(chrono::Utc::now() + chrono::Duration::hours(24)),
            consensus_height: 0,
        };

        let payload = bincode::serde::encode_to_vec(&payload_data, bincode::config::standard())
            .expect("Failed to encode payload");

        let tx = Transaction::new_with_user(
            function,
            payload,
            node1.node_id,
            &node1.signing_key,
            user.user_id,
            &user.signing_key,
        )
        .expect("Failed to create transaction");

        let mut conn = app_state
            .db_pool
            .get()
            .expect("Failed to get DB connection");
        let db_tx = conn.transaction().expect("Failed to start transaction");
        let result = process_transaction(&tx, &app_state, false, &db_tx);
        assert!(
            result.is_err(),
            "Mismatched node ID should fail authorization"
        );
        assert!(matches!(
            result.unwrap_err(),
            DatabaseError::AuthorizationError
        ));
        let _ = db_tx.rollback();
    }

    #[test]
    fn test_dual_authorized_create_import() {
        // tx.user.id == payload.user_id AND tx.submitter.id == payload.owner_node_id
        // Tests: create_import
        // Expected: Authorization passes (downstream eligibility check may fail for other reasons,
        // but we explicitly assert the failure is NOT an AuthorizationError)
        let app_state = create_test_app_state();
        let node = MockNode::new(0);
        let user = MockUser::new(100);

        let function = "create_import".to_string();

        let payload_data = ImportPayload {
            import_id: crate::db::CustomUUID::new(None),
            user_id: user.user_id,
            owner_node_id: node.node_id,
            status: ImportStatus::Pending,
        };

        let payload = bincode::serde::encode_to_vec(&payload_data, bincode::config::standard())
            .expect("Failed to encode payload");

        let tx = Transaction::new_with_user(
            function,
            payload,
            node.node_id,
            &node.signing_key,
            user.user_id,
            &user.signing_key,
        )
        .expect("Failed to create transaction");

        let mut conn = app_state
            .db_pool
            .get()
            .expect("Failed to get DB connection");
        let db_tx = conn.transaction().expect("Failed to start transaction");
        let result = process_transaction(&tx, &app_state, false, &db_tx);
        if let Err(e) = &result {
            assert!(
                !matches!(e, DatabaseError::AuthorizationError),
                "create_import should not fail authorization but got: {:?}",
                e
            );
        }
        let _ = db_tx.rollback();
    }

    #[test]
    fn test_dual_user_unauthorized_create_import() {
        // User ID mismatch but node ID matches
        // tx.user.id != payload.user_id BUT tx.submitter.id == payload.owner_node_id
        // Tests: create_import
        // Expected: AuthorizationError
        let app_state = create_test_app_state();
        let node = MockNode::new(0);
        let user1 = MockUser::new(100);
        let user2_id = 101;

        let function = "create_import".to_string();

        let payload_data = ImportPayload {
            import_id: crate::db::CustomUUID::new(None),
            user_id: user2_id,
            owner_node_id: node.node_id,
            status: ImportStatus::Pending,
        };

        let payload = bincode::serde::encode_to_vec(&payload_data, bincode::config::standard())
            .expect("Failed to encode payload");

        let tx = Transaction::new_with_user(
            function,
            payload,
            node.node_id,
            &node.signing_key,
            user1.user_id,
            &user1.signing_key,
        )
        .expect("Failed to create transaction");

        let mut conn = app_state
            .db_pool
            .get()
            .expect("Failed to get DB connection");
        let db_tx = conn.transaction().expect("Failed to start transaction");
        let result = process_transaction(&tx, &app_state, false, &db_tx);
        assert!(
            result.is_err(),
            "Mismatched user ID should fail authorization"
        );
        assert!(matches!(
            result.unwrap_err(),
            DatabaseError::AuthorizationError
        ));
        let _ = db_tx.rollback();
    }

    #[test]
    fn test_dual_node_unauthorized_create_import() {
        // Node ID mismatch but user ID matches
        // tx.user.id == payload.user_id BUT tx.submitter.id != payload.owner_node_id
        // Tests: create_import
        // Expected: AuthorizationError
        let app_state = create_test_app_state();
        let node1 = MockNode::new(0);
        let node2_id = 1;
        let user = MockUser::new(100);

        let function = "create_import".to_string();

        let payload_data = ImportPayload {
            import_id: crate::db::CustomUUID::new(None),
            user_id: user.user_id,
            owner_node_id: node2_id,
            status: ImportStatus::Pending,
        };

        let payload = bincode::serde::encode_to_vec(&payload_data, bincode::config::standard())
            .expect("Failed to encode payload");

        let tx = Transaction::new_with_user(
            function,
            payload,
            node1.node_id,
            &node1.signing_key,
            user.user_id,
            &user.signing_key,
        )
        .expect("Failed to create transaction");

        let mut conn = app_state
            .db_pool
            .get()
            .expect("Failed to get DB connection");
        let db_tx = conn.transaction().expect("Failed to start transaction");
        let result = process_transaction(&tx, &app_state, false, &db_tx);
        assert!(
            result.is_err(),
            "Mismatched node ID should fail authorization"
        );
        assert!(matches!(
            result.unwrap_err(),
            DatabaseError::AuthorizationError
        ));
        let _ = db_tx.rollback();
    }
}
