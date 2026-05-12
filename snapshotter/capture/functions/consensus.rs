use std::collections::BTreeMap;

use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use serde::Serialize;

use super::super::fixtures::FixtureContext;
use super::helpers::wrap;
use crate::schema::FunctionResult;

pub fn capture(
    pool: &Pool<SqliteConnectionManager>,
    ctx: &FixtureContext,
    results: &mut BTreeMap<String, FunctionResult>,
) {
    use hopnet::consensus::ConsensusPhase;
    use hopnet::db::consensus;

    results.insert(
        "db::consensus::get_consensus".into(),
        wrap(|| consensus::get_consensus(pool.get())),
    );

    results.insert(
        "db::consensus::get_consensus_history".into(),
        wrap(|| consensus::get_consensus_history(pool.get())),
    );

    results.insert(
        "db::consensus::get_validators(height=5)".into(),
        wrap(|| consensus::get_validators(pool.get(), 5)),
    );

    results.insert(
        "db::consensus::get_validators(height=0)".into(),
        wrap(|| consensus::get_validators(pool.get(), 0)),
    );

    results.insert(
        "db::consensus::get_validators_elect(height=5)".into(),
        wrap(|| consensus::get_validators_elect(pool.get(), 5)),
    );

    results.insert(
        "db::consensus::get_node_pubkey(node=0)".into(),
        wrap(|| consensus::get_node_pubkey(pool.get(), 0)),
    );

    results.insert(
        "db::consensus::get_node_pubkey(node=1)".into(),
        wrap(|| consensus::get_node_pubkey(pool.get(), 1)),
    );

    {
        let conn = pool.get().unwrap();
        results.insert(
            "db::consensus::get_all_node_pubkeys".into(),
            wrap(|| consensus::get_all_node_pubkeys(&conn)),
        );

        results.insert(
            "db::consensus::get_all_user_pubkeys".into(),
            wrap(|| consensus::get_all_user_pubkeys(&conn)),
        );

        results.insert(
            "db::consensus::get_consensus_progress".into(),
            wrap(|| consensus::get_consensus_progress(&conn)),
        );
    }

    results.insert("db::consensus::get_me".into(), {
        match consensus::get_me(pool.get()) {
            Ok(me) => {
                #[derive(Serialize)]
                struct MyNodeProxy {
                    node_id: i32,
                }
                FunctionResult::Ok {
                    value: serde_json::to_value(MyNodeProxy {
                        node_id: me.node_id,
                    })
                    .unwrap(),
                }
            }
            Err(e) => FunctionResult::Error {
                error_variant: format!("{:?}", e),
            },
        }
    });

    results.insert("db::consensus::get_startup_state".into(), {
        match consensus::get_startup_state(pool.get()) {
            Ok(state) => {
                #[derive(Serialize)]
                struct StartupProxy {
                    node_id: i32,
                    user_id: i32,
                }
                FunctionResult::Ok {
                    value: serde_json::to_value(StartupProxy {
                        node_id: state.node_id,
                        user_id: state.user_id,
                    })
                    .unwrap(),
                }
            }
            Err(e) => FunctionResult::Error {
                error_variant: format!("{:?}", e),
            },
        }
    });

    results.insert(
        "db::consensus::get_view_consensus_data(view=0)".into(),
        wrap(|| consensus::get_view_consensus_data(pool.get(), 0)),
    );

    results.insert(
        "db::consensus::get_view_consensus_data(view=3)".into(),
        wrap(|| consensus::get_view_consensus_data(pool.get(), 3)),
    );

    results.insert(
        "db::consensus::get_view_consensus_data(view=5)".into(),
        wrap(|| consensus::get_view_consensus_data(pool.get(), 5)),
    );

    results.insert(
        "db::consensus::get_block(genesis)".into(),
        wrap(|| consensus::get_block(pool.get(), ctx.genesis_hash)),
    );

    if let Some(h) = ctx.block_hashes.first() {
        results.insert(
            "db::consensus::get_block(height=1)".into(),
            wrap(|| consensus::get_block(pool.get(), *h)),
        );
    }

    // get QC by hash for genesis block
    results.insert(
        "db::consensus::get_quorum_certificate_by_hash(genesis,Propose)".into(),
        wrap(|| {
            consensus::get_quorum_certificate_by_hash(
                pool.get(),
                &0,
                &ctx.genesis_hash,
                &ConsensusPhase::Propose,
            )
        }),
    );

    results.insert(
        "db::consensus::get_quorum_certificate_by_hash(genesis,Lock)".into(),
        wrap(|| {
            consensus::get_quorum_certificate_by_hash(
                pool.get(),
                &0,
                &ctx.genesis_hash,
                &ConsensusPhase::Lock,
            )
        }),
    );

    // check_committed_nonces — returns HashSet, sort for determinism
    {
        let conn = pool.get().unwrap();
        let nonces_ref: Vec<_> = ctx.committed_nonces.clone();
        results.insert("db::consensus::check_committed_nonces".into(), {
            match consensus::check_committed_nonces(&conn, &nonces_ref) {
                Ok(set) => {
                    let mut sorted: Vec<String> = set.into_iter().collect();
                    sorted.sort();
                    FunctionResult::Ok {
                        value: serde_json::to_value(sorted).unwrap(),
                    }
                }
                Err(e) => FunctionResult::Error {
                    error_variant: format!("{:?}", e),
                },
            }
        });
    }

    // is_node_active
    {
        let mut conn = pool.get().unwrap();
        let tx = conn.transaction().unwrap();
        results.insert(
            "db::consensus::is_node_active(node=0,height=5)".into(),
            wrap(|| consensus::is_node_active(&tx, 0, 5)),
        );
        results.insert(
            "db::consensus::is_node_active(node=1,height=5)".into(),
            wrap(|| consensus::is_node_active(&tx, 1, 5)),
        );
    }
}
