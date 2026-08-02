use crate::{
    db::{DatabaseError, nodes::{insert_node_tx, pubkey_exists}},
    handlers::{HandlerCtx, HandlerResult, TransactionHandler, TxMeta},
    types::Node,
};

pub struct InsertNodeHandler;

impl TransactionHandler for InsertNodeHandler {
    fn name(&self) -> &'static str {
        "insert_node"
    }

    fn process(
        &self,
        tx: &TxMeta<'_>,
        _execute: bool,
        _ctx: &HandlerCtx<'_>,
        db_tx: &rusqlite::Transaction<'_>,
    ) -> HandlerResult {
        match bincode::serde::decode_from_slice::<Node, _>(tx.payload, bincode::config::standard())
        {
            Ok((node_data, _)) => {
                // Authorization: verify user owns the node being inserted
                if let Some(user_id) = tx.user_id {
                    if node_data.owner != user_id {
                        tracing::warn!(
                            "Authorization failed: user {} attempted to insert node owned by user {}",
                            user_id,
                            node_data.owner
                        );
                        return Err(DatabaseError::AuthorizationError);
                    }
                } else {
                    tracing::warn!(
                        "Authorization failed: insert_node requires user authentication"
                    );
                    return Err(DatabaseError::AuthorizationError);
                }

                // Reject duplicate pubkey — two nodes sharing the same public
                // key would be the same physical host, breaking quorum safety.
                if pubkey_exists(db_tx, &node_data.pubkey) {
                    tracing::warn!(
                        "Rejecting insert_node: pubkey {:?} already registered",
                        node_data.pubkey
                    );
                    return Err(DatabaseError::ProcessingError);
                }

                // Insert the node using shared transaction
                insert_node_tx(db_tx, node_data)?;
                Ok(())
            }
            Err(_) => Err(DatabaseError::InvalidPayload),
        }
    }
}

inventory::submit! {
    &InsertNodeHandler as &dyn TransactionHandler
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handlers::{NullNotifier, NullScheduler};
    use ed25519_dalek::SigningKey;
    use rand::rand_core::UnwrapErr;
    use rand::rngs::SysRng;

    fn setup_pool() -> r2d2::Pool<crate::db::SqliteConnectionManager> {
        let manager = crate::db::SqliteConnectionManager::memory();
        let pool = r2d2::Pool::builder()
            .max_size(1)
            .connection_customizer(Box::new(crate::db::shared::SqliteInitializer))
            .build(manager)
            .unwrap();
        crate::db::shared::initialize(&pool.get().unwrap()).unwrap();

        let conn = pool.get().unwrap();
        conn.execute(
            "INSERT INTO users (user_id, username, pubkey, x25519_pubkey, encrypted_privkey, key_salt)
             VALUES (1, 'allison', X'00', X'00', X'00', X'00')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO sequences (name, next_id) VALUES ('nodes', 1)",
            [],
        )
        .unwrap();
        pool
    }

    fn new_pubkey() -> crate::types::PubKey {
        let mut rng = UnwrapErr(SysRng);
        let signing_key = SigningKey::generate(&mut rng);
        crate::types::PubKey(signing_key.verifying_key())
    }

    fn run_insert(
        pool: &r2d2::Pool<crate::db::SqliteConnectionManager>,
        node: &Node,
        user_id: i32,
        execute: bool,
    ) -> HandlerResult {
        let payload =
            bincode::serde::encode_to_vec(node, bincode::config::standard()).unwrap();
        let meta = TxMeta {
            function: "insert_node",
            payload: &payload,
            submitter_node: 0,
            user_id: Some(user_id),
        };
        let notifier = NullNotifier;
        let scheduler = NullScheduler;
        let ctx = HandlerCtx {
            fragments_dir: "",
            node_id: Some(0),
            notifier: &notifier,
            work: &scheduler,
        };
        let mut conn = pool.get().unwrap();
        let db_tx = conn.transaction().unwrap();
        let result = InsertNodeHandler.process(&meta, execute, &ctx, &db_tx);
        if result.is_ok() && execute {
            db_tx.commit().unwrap();
        }
        result
    }

    fn make_node(node_id: i32, name: &str, owner: i32, pubkey: crate::types::PubKey) -> Node {
        Node {
            node_id,
            name: name.to_string(),
            owner,
            pubkey,
        }
    }

    // Should: first insert succeeds (validate + execute).
    #[test]
    fn insert_succeeds() {
        let pool = setup_pool();
        let pk = new_pubkey();
        let node = make_node(99, "test-node", 1, pk);
        assert!(run_insert(&pool, &node, 1, false).is_ok());
        assert!(run_insert(&pool, &node, 1, true).is_ok());
    }

    // Should: reject an insert where the pubkey is already registered.
    #[test]
    fn insert_duplicate_pubkey_rejected() {
        let pool = setup_pool();
        let pk = new_pubkey();

        let first = make_node(99, "first", 1, pk);
        assert!(run_insert(&pool, &first, 1, false).is_ok());
        assert!(run_insert(&pool, &first, 1, true).is_ok());

        let second = make_node(100, "second", 1, pk);
        let err = run_insert(&pool, &second, 1, false).unwrap_err();
        assert_eq!(err, DatabaseError::ProcessingError);
    }

    // Should: allow two nodes from the same user with different pubkeys.
    #[test]
    fn insert_same_owner_different_pubkey_succeeds() {
        let pool = setup_pool();

        let first = make_node(99, "first", 1, new_pubkey());
        assert!(run_insert(&pool, &first, 1, false).is_ok());
        assert!(run_insert(&pool, &first, 1, true).is_ok());

        let second = make_node(100, "second", 1, new_pubkey());
        assert!(run_insert(&pool, &second, 1, false).is_ok());
        assert!(run_insert(&pool, &second, 1, true).is_ok());
    }

    // Should: reject an insert where the user_id doesn't match the node owner.
    #[test]
    fn insert_wrong_owner_rejected() {
        let pool = setup_pool();
        let node = make_node(99, "stolen", 2, new_pubkey());
        let err = run_insert(&pool, &node, 1, false).unwrap_err();
        assert_eq!(err, DatabaseError::AuthorizationError);
    }

    // Should: reject an insert without user authentication.
    #[test]
    fn insert_no_user_rejected() {
        let pool = setup_pool();
        let node = make_node(99, "anon", 1, new_pubkey());
        let payload =
            bincode::serde::encode_to_vec(&node, bincode::config::standard()).unwrap();
        let meta = TxMeta {
            function: "insert_node",
            payload: &payload,
            submitter_node: 0,
            user_id: None,
        };
        let notifier = NullNotifier;
        let scheduler = NullScheduler;
        let ctx = HandlerCtx {
            fragments_dir: "",
            node_id: Some(0),
            notifier: &notifier,
            work: &scheduler,
        };
        let mut conn = pool.get().unwrap();
        let db_tx = conn.transaction().unwrap();
        let err = InsertNodeHandler
            .process(&meta, false, &ctx, &db_tx)
            .unwrap_err();
        assert_eq!(err, DatabaseError::AuthorizationError);
    }
}
