use std::collections::BTreeMap;
use std::str::FromStr;

use aes_siv::aead::OsRng;
use chrono::{Duration, Utc};
use either::Either;
use hopnet::db;
use hopnet::db::types::{
    BlobAccess, CustomUUID, Inode, XPubKey,
};
use hopnet_storage::SelfCheckFragments;
use hopnet::metrics::types::Metric;
use hopnet::types::{Blake3Hash, Node, PrivKey, PubKey, User};
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::params;

use super::FixtureContext;
use super::keys;

/// Deterministic Blake3Hash from an index
fn hash_from_index(index: u8) -> Blake3Hash {
    let mut input = [0u8; 32];
    input[0] = index;
    Blake3Hash::new(blake3::hash(&input))
}

/// Deterministic CustomUUID from an index.
/// Constructs a UUIDv7-compatible UUID with fully deterministic bytes.
fn uuid_from_index(index: u16) -> CustomUUID {
    // UUIDv7 format: 48-bit timestamp | 4-bit version (7) | 12-bit rand_a | 2-bit variant (10) | 62-bit rand_b
    // We use index as timestamp_ms and fill rand fields deterministically from index
    let ts_ms: u64 = index as u64 * 1000; // milliseconds
    let mut bytes = [0u8; 16];
    // 48-bit timestamp (big-endian)
    bytes[0] = ((ts_ms >> 40) & 0xFF) as u8;
    bytes[1] = ((ts_ms >> 32) & 0xFF) as u8;
    bytes[2] = ((ts_ms >> 24) & 0xFF) as u8;
    bytes[3] = ((ts_ms >> 16) & 0xFF) as u8;
    bytes[4] = ((ts_ms >> 8) & 0xFF) as u8;
    bytes[5] = (ts_ms & 0xFF) as u8;
    // version 7 + deterministic rand_a from index
    bytes[6] = 0x70 | ((index >> 8) as u8 & 0x0F);
    bytes[7] = index as u8;
    // variant 10 + deterministic rand_b from index hash
    let hash = blake3::hash(&index.to_le_bytes());
    let hash_bytes = hash.as_bytes();
    bytes[8] = 0x80 | (hash_bytes[0] & 0x3F);
    bytes[9..16].copy_from_slice(&hash_bytes[1..8]);

    CustomUUID::from_str(&uuid::Uuid::from_bytes(bytes).to_string()).unwrap()
}

pub fn populate(pool: &Pool<SqliteConnectionManager>, ctx: &mut FixtureContext) {
    let (_node0_priv, node0_pub) = keys::ed25519_from_seed(keys::NODE_0_SEED);
    let (node1_priv, node1_pub) = keys::ed25519_from_seed(keys::NODE_1_SEED);
    let (node2_priv, node2_pub) = keys::ed25519_from_seed(keys::NODE_2_SEED);
    let (user1_priv, user1_pub) = keys::ed25519_from_seed(keys::USER_1_SEED);
    let user1_x25519 = keys::x25519_pubkey_from_seed(keys::USER_1_X25519_SEED);

    // Use scoped connections to avoid deadlocking the max_size=1 pool
    // when insert_block() also calls pool.get()

    // === Insert user 1, nodes 1-2, validators 1-2 ===
    {
        let mut conn = pool.get().expect("Failed to get connection");
        let tx = conn.transaction().expect("Failed to begin transaction");

        let user1 = User::new(
            1,
            "bob".to_string(),
            user1_pub,
            user1_x25519,
            vec![0u8; 48],
            vec![0u8; 16],
        );
        db::users::insert_user_tx(&tx, user1).expect("Failed to insert user 1");

        let node1 = Node {
            node_id: 1,
            name: "node-1".to_string(),
            owner: 0,
            pubkey: node1_pub,
        };
        db::nodes::insert_node_tx(&tx, node1).expect("Failed to insert node 1");

        let node2 = Node {
            node_id: 2,
            name: "node-2".to_string(),
            owner: 1,
            pubkey: node2_pub,
        };
        db::nodes::insert_node_tx(&tx, node2).expect("Failed to insert node 2");

        db::consensus::activate_validator(&tx, 1, 0).expect("Failed to activate validator 1");
        db::consensus::activate_validator(&tx, 2, 0).expect("Failed to activate validator 2");

        tx.commit()
            .expect("Failed to commit extra nodes/validators");
    } // conn dropped here

    // === Insert decided heights 1-5 (malachite engine tables) ===
    // Empty engine blocks chained from the fixture genesis, each with a
    // signature-less certificate — populates the decided history the
    // surviving read functions (heights, history shims) consume.
    {
        let mut conn = pool.get().expect("Failed to get connection");
        let tx = conn.transaction().expect("Failed to begin transaction");

        let mut prev_hash: hopnet_consensus::types::Blake3Hash = tx
            .query_row(
                "SELECT block_hash FROM decided_blocks WHERE height = 0",
                [],
                |row| {
                    let bytes: Vec<u8> = row.get(0)?;
                    Ok(hopnet_consensus::types::Blake3Hash::from_bytes(
                        bytes.as_slice().try_into().expect("32-byte hash"),
                    ))
                },
            )
            .expect("fixture genesis missing");

        for i in 1..=5u64 {
            let block =
                hopnet_consensus::types::Block::new(hopnet_consensus::types::BlockData {
                    height: i,
                    round: 0,
                    parent_hash: Some(prev_hash),
                    transactions: hopnet_consensus::types::Transactions(Vec::new()),
                })
                .expect("Failed to create block");
            prev_hash = block.block_hash;

            let block_bytes = hopnet_consensus::codec::encode(&block).expect("encode block");
            let cert = hopnet_consensus::codec::WireCommitCertificate {
                height: i,
                round: 0,
                value_id: block.block_hash,
                signatures: Vec::new(),
            };
            let cert_bytes = hopnet_consensus::codec::encode(&cert).expect("encode cert");
            tx.execute(
                "INSERT INTO decided_blocks (height, block_hash, round, block) VALUES (?, ?, 0, ?)",
                params![i as i64, block.block_hash.as_bytes().as_slice(), block_bytes],
            )
            .expect("Failed to insert decided block");
            tx.execute(
                "INSERT INTO decided_certificates (height, block_hash, round, certificate) VALUES (?, ?, 0, ?)",
                params![i as i64, block.block_hash.as_bytes().as_slice(), cert_bytes],
            )
            .expect("Failed to insert decided certificate");
        }
        tx.execute(
            "INSERT OR REPLACE INTO consensus_meta (key, value) VALUES ('last_decided_height', 5)",
            [],
        )
        .expect("Failed to update last decided height");
        tx.commit().expect("Failed to commit decided heights");
    }

    // === Insert committed nonces ===
    {
        let mut conn = pool.get().expect("Failed to get connection");
        let tx = conn.transaction().expect("Failed to begin transaction");
        let nonces = vec![
            uuid_from_index(500),
            uuid_from_index(501),
            uuid_from_index(502),
        ];
        ctx.committed_nonces = nonces.clone();
        db::consensus::insert_tx_nonces_tx(&tx, &nonces).expect("Failed to insert nonces");
        tx.commit().expect("Failed to commit nonces");
    }

    // === Insert file data ===
    let (siv_key, siv_nonce) = keys::siv_from_seed(keys::SIV_SEED);

    // Encrypt paths with AES-SIV to match what the read functions expect
    let encrypt_path = |path: &str| -> String {
        use aes_siv::{
            Aes256SivAead,
            aead::{Aead, KeyInit},
        };
        let cipher = Aes256SivAead::new(&siv_key);
        // Encrypt each segment individually, prefixed with / (matching HopNet encrypt_part)
        let mut output = String::new();
        for segment in path.split('/').filter(|s| !s.is_empty()) {
            let encrypted = cipher.encrypt(&siv_nonce, segment.as_bytes()).unwrap();
            output.push('/');
            output.push_str(&hex::encode(encrypted));
        }
        output
    };

    // Create folder inodes
    let root_folder_id = uuid_from_index(100);
    let subfolder_id = uuid_from_index(101);

    // Create data blocks with fragment hashes
    let data_block_ids: Vec<CustomUUID> = (0..3).map(|i| uuid_from_index(200 + i)).collect();
    let file_ids: Vec<CustomUUID> = (0..4).map(|i| uuid_from_index(300 + i)).collect();

    // Create fragment metadata (3 per blob: 2 original + 1 recovery)
    let mut all_fragment_hashes: Vec<Blake3Hash> = Vec::new();
    let mut blob_ops = Vec::new();

    for (db_idx, data_block_id) in data_block_ids.iter().enumerate() {
        let mut fragments = Vec::new();
        for chunk_num in 0..3u32 {
            let frag_hash = hash_from_index((db_idx * 3 + chunk_num as usize) as u8 + 50);
            let frag_id = uuid_from_index(400 + (db_idx as u16 * 3) + chunk_num as u16);
            all_fragment_hashes.push(frag_hash);

            fragments.push(hopnet_storage::store::FragmentMeta {
                blob_id: data_block_id.clone(),
                chunk_number: chunk_num,
                local_index: chunk_num,
                fragment_id: frag_id,
                fragment_hash: frag_hash,
                recovery: chunk_num >= 2,
            });
        }

        // Blob access wrap for user 0 — recipient pubkey MUST be user 0's
        // actual x25519 pubkey (reads JOIN users on it).
        let user0_x25519 = keys::x25519_pubkey_from_seed(keys::USER_0_X25519_SEED);
        let file_access = vec![BlobAccess {
            blob_id: data_block_id.clone(),
            recipient_pubkey: *user0_x25519.as_x25519().as_bytes(),
            ephemeral_pubkey: [41u8 + db_idx as u8; 32],
            wrapped_key: vec![0u8; 48],
        }];

        blob_ops.push(hopnet_storage::store::BlobInsertOp {
            blob_id: data_block_id.clone(),
            integrity_hash: hash_from_index(db_idx as u8 + 10),
            added_bytes: 0,
            file_size: (1024 * (db_idx + 1)) as u64,
            fragments,
            access: file_access,
        });
    }

    ctx.siv_key = Some(siv_key);
    ctx.siv_nonce = Some(siv_nonce);
    ctx.data_block_ids = data_block_ids.clone();
    ctx.fragment_hashes = all_fragment_hashes.clone();
    ctx.root_folder_id = Some(root_folder_id.clone());
    ctx.file_ids = file_ids.clone();

    // Build inodes: 2 folders + 3 files (one file per data block) + 1 extra file in subfolder
    let encrypted_root = encrypt_path("/root");
    let encrypted_subfolder = encrypt_path("/root/docs");
    let encrypted_file0 = encrypt_path("/root/file0.txt");
    let encrypted_file1 = encrypt_path("/root/file1.txt");
    let encrypted_file2 = encrypt_path("/root/docs/file2.txt");
    let encrypted_file3 = encrypt_path("/root/docs/file3.txt");

    {
        let mut conn = pool.get().expect("Failed to get connection");
        let tx = conn.transaction().expect("Failed to begin transaction");

        // Insert folders first (they have no data_id)
        let mut folder_inodes = vec![
            Inode {
                id: root_folder_id.clone(),
                owner: hopnet_drive::InodeOwner::Id(0),
                path: encrypted_root.clone(),
                inode_type: hopnet_common::InodeType::Folder,
                data_id: None,
            },
            Inode {
                id: subfolder_id,
                owner: hopnet_drive::InodeOwner::Id(0),
                path: encrypted_subfolder.clone(),
                inode_type: hopnet_common::InodeType::Folder,
                data_id: None,
            },
        ];
        // Insert root folder first (before subfolder, to avoid conflict with auto-created parents)
        db::files::insert_files(&tx, &[], vec![folder_inodes.remove(0)], "/tmp/hopnet-snapshotter-fragments")
            .expect("Failed to insert root folder");
        // Then insert subfolder (its parent /root now exists)
        db::files::insert_files(&tx, &[], folder_inodes, "/tmp/hopnet-snapshotter-fragments").expect("Failed to insert subfolder");

        // Insert files referencing the blobs by id (blob ops apply first)
        let file_inodes = vec![
            Inode {
                id: file_ids[0].clone(),
                owner: hopnet_drive::InodeOwner::Id(0),
                path: encrypted_file0.clone(),
                inode_type: hopnet_common::InodeType::File,
                data_id: Some(data_block_ids[0].clone()),
            },
            Inode {
                id: file_ids[1].clone(),
                owner: hopnet_drive::InodeOwner::Id(0),
                path: encrypted_file1.clone(),
                inode_type: hopnet_common::InodeType::File,
                data_id: Some(data_block_ids[1].clone()),
            },
            Inode {
                id: file_ids[2].clone(),
                owner: hopnet_drive::InodeOwner::Id(0),
                path: encrypted_file2.clone(),
                inode_type: hopnet_common::InodeType::File,
                data_id: Some(data_block_ids[2].clone()),
            },
        ];
        db::files::insert_files(&tx, &blob_ops, file_inodes, "/tmp/hopnet-snapshotter-fragments").expect("Failed to insert files");

        // Insert a file for user 1 (file3 in subfolder, no data block - just inode)
        let user1_folder_id = uuid_from_index(102);
        let user1_file_inodes = vec![Inode {
            id: user1_folder_id,
            owner: hopnet_drive::InodeOwner::Id(1),
            path: encrypted_root.clone(),
            inode_type: hopnet_common::InodeType::Folder,
            data_id: None,
        }];
        db::files::insert_files(&tx, &[], user1_file_inodes, "/tmp/hopnet-snapshotter-fragments").expect("Failed to insert user 1 folder");

        tx.commit().expect("Failed to commit files");
    }

    ctx.encrypted_root = Some(encrypted_root.clone());

    // Fixed base time for all metric timestamps (determinism).
    // capture_all() will time-shift metrics into the live window before running aggregate queries.
    let base_time = chrono::DateTime::parse_from_rfc3339("2026-01-01T12:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    ctx.metrics_base_time = Some(base_time);

    // === Insert metrics ===
    {
        let mut conn = pool.get().expect("Failed to get connection");
        let tx = conn.transaction().expect("Failed to begin transaction");
        let mut metrics = Vec::new();

        // 6 node pairs x 2 timestamps = 12 metric entries
        let node_pairs = [(0, 1), (0, 2), (1, 0), (1, 2), (2, 0), (2, 1)];
        for (from, to) in &node_pairs {
            for offset_hours in [2i64, 1] {
                metrics.push(Metric {
                    from_node: *from,
                    to_node: *to,
                    start_time: base_time - Duration::hours(offset_hours),
                    rtt_latency: Some(10.0 + *from as f64 + *to as f64),
                    rtt_variance: Some(1.0),
                    rtt_jitter: Some(0.5),
                    throughput: Some(100_000),
                    height: 5,
                    available: true,
                    storage_total_gb: Some(100),
                    storage_used_gb: Some(25),
                });
            }
        }
        db::metrics::insert_metrics_batch(&tx, metrics).expect("Failed to insert metrics");
        tx.commit().expect("Failed to commit metrics");
    }

    // === Insert fragment inventory ===
    // Map all 9 fragments across all 3 nodes
    {
        let mut conn = pool.get().expect("Failed to get connection");
        let tx = conn.transaction().expect("Failed to begin transaction");
        for node_id in 0..3i32 {
            let report = SelfCheckFragments {
                node_id,
                self_verified_height: 5,
                previous_count: 0,
                fragments_added: all_fragment_hashes.clone(),
                fragments_removed: Vec::new(),
            };
            hopnet::storage_host::db_apply::apply_self_check_updates(&tx, &report)
                .expect("Failed to apply inventory updates");
        }
        tx.commit().expect("Failed to commit inventory");
    }

    // === Insert device tokens ===
    {
        let mut conn = pool.get().expect("Failed to get connection");
        let tx = conn.transaction().expect("Failed to begin transaction");
        let device0_id = uuid_from_index(600);
        let device1_id = uuid_from_index(601);
        ctx.device_ids = vec![device0_id.clone(), device1_id.clone()];

        db::devices::insert_device_token_tx(
            &tx,
            &device0_id,
            0,
            &hash_from_index(200),
            "encrypted_device_0",
            &[0u8; 60],
        )
        .expect("Failed to insert device token 0");

        db::devices::insert_device_token_tx(
            &tx,
            &device1_id,
            1,
            &hash_from_index(201),
            "encrypted_device_1",
            &[0u8; 60],
        )
        .expect("Failed to insert device token 1");

        tx.commit().expect("Failed to commit device tokens");
    }

    // === Insert shares ===
    {
        let mut conn = pool.get().expect("Failed to get connection");
        let tx = conn.transaction().expect("Failed to begin transaction");
        let share_id = uuid_from_index(700);
        ctx.share_ids = vec![share_id.clone()];

        db::shares::insert_incoming_share(
            &tx,
            share_id,
            data_block_ids[0].clone(), // shares the first data block
            0,                         // sender: user 0
            1,                         // recipient: user 1
            &[0u8; 48],                // dummy file_access
            &[0u8; 32],                // dummy display_ephemeral_pubkey
            &[0u8; 32],                // dummy encrypted_display_name
        )
        .expect("Failed to insert share");

        tx.commit().expect("Failed to commit shares");
    }

    // === Insert modification log entries ===
    {
        let mut conn = pool.get().expect("Failed to get connection");
        let tx = conn.transaction().expect("Failed to begin transaction");
        for (i, file_id) in file_ids.iter().take(3).enumerate() {
            db::files::log_modification(
                &tx,
                file_id.clone(),
                0,                                     // owner_id
                None,                                  // old_parent_id (new file)
                None,                                  // old_path
                Some(&format!("/root/file{}.txt", i)), // new_path
                i as i32 + 1,                          // modification_height
            )
            .expect("Failed to log modification");
        }
        // One more for a move operation
        db::files::log_modification(
            &tx,
            file_ids[0].clone(),
            0,
            Some(root_folder_id.clone()),
            Some("/root/file0.txt"),
            Some("/root/docs/file0.txt"),
            4,
        )
        .expect("Failed to log modification (move)");

        tx.commit().expect("Failed to commit modification log");
    }

    // ====================================================================
    // ENRICHMENT: Additional data for comprehensive query path coverage
    // ====================================================================

    // --- 1. Diverse metrics (exercises get_all_node_metrics branches) ---
    // All timestamps are fixed offsets from base_time for determinism.
    // capture_all() time-shifts the metrics table into the live datetime('now') window before
    // running time-sensitive captures, so the aggregate queries see "recent" data.
    {
        let mut conn = pool.get().expect("Failed to get connection");
        let tx = conn.transaction().expect("Failed to begin transaction");
        let mut metrics = Vec::new();

        // Node 0 as target: 100 samples → trust_factor caps at 1.0
        // High throughput, full availability, normal storage (25%)
        // 30 within "24h" of base, 70 within "7d" of base
        for i in 0..50u32 {
            let hours_before_base = if i < 15 {
                3 + i as i64 // 3-17h before base: within 24h window after shift
            } else {
                25 + (i - 15) as i64 * 3 // 25-130h before base: within 7d, outside 24h
            };
            for &from_node in &[1i32, 2] {
                metrics.push(Metric {
                    from_node,
                    to_node: 0,
                    start_time: base_time
                        - Duration::hours(hours_before_base)
                        - Duration::minutes(30 + from_node as i64 * 10),
                    rtt_latency: Some(8.0 + i as f64 * 0.1 + from_node as f64),
                    rtt_variance: Some(0.5 + i as f64 * 0.02),
                    rtt_jitter: Some(0.3),
                    throughput: Some(500_000 + i as i64 * 2000 + from_node as i64 * 100),
                    height: 5,
                    available: true,
                    storage_total_gb: Some(100),
                    storage_used_gb: Some(25),
                });
            }
        }

        // Node 1 as target: 20 samples, low throughput, 20% unavailable, high latency variance
        for i in 0..10u32 {
            let hours_before_base = 3 + i as i64 * 8; // spread across 7 days
            for &from_node in &[0i32, 2] {
                metrics.push(Metric {
                    from_node,
                    to_node: 1,
                    start_time: base_time
                        - Duration::hours(hours_before_base)
                        - Duration::minutes(30 + from_node as i64 * 10),
                    rtt_latency: Some(80.0 + i as f64 * 30.0 + from_node as f64 * 5.0),
                    rtt_variance: Some(15.0 + i as f64 * 5.0),
                    rtt_jitter: Some(8.0),
                    throughput: Some(10_000 + i as i64 * 500 + from_node as i64 * 100),
                    height: 5,
                    available: i < 8, // last 2 iterations → unavailable
                    storage_total_gb: Some(50),
                    storage_used_gb: Some(20),
                });
            }
        }

        // Node 2 as target: 20 samples, >90% storage → quartic decay, medium throughput
        for i in 0..10u32 {
            let hours_before_base = 3 + i as i64 * 8;
            for &from_node in &[0i32, 1] {
                metrics.push(Metric {
                    from_node,
                    to_node: 2,
                    start_time: base_time
                        - Duration::hours(hours_before_base)
                        - Duration::minutes(30 + from_node as i64 * 10),
                    rtt_latency: Some(15.0 + i as f64 * 0.5 + from_node as f64),
                    rtt_variance: Some(2.0),
                    rtt_jitter: Some(1.0),
                    throughput: Some(200_000 + i as i64 * 3000 + from_node as i64 * 100),
                    height: 5,
                    available: true,
                    storage_total_gb: Some(100),
                    storage_used_gb: Some(95), // 95% → triggers quartic decay
                });
            }
        }

        // 3 metrics with NULL throughput/latency → exercises COALESCE fallback paths
        for i in 0..3u32 {
            metrics.push(Metric {
                from_node: 0,
                to_node: 1,
                start_time: base_time - Duration::hours(4 + i as i64) - Duration::minutes(45),
                rtt_latency: None,
                rtt_variance: None,
                rtt_jitter: None,
                throughput: None,
                height: 5,
                available: true,
                storage_total_gb: None,
                storage_used_gb: None,
            });
        }

        db::metrics::insert_metrics_batch(&tx, metrics)
            .expect("Failed to insert enrichment metrics");
        tx.commit().expect("Failed to commit enrichment metrics");
    }

    // --- 2. Diverse resilience distribution patterns ---
    // Creates data blocks that exercise all 6 fault tolerance classification levels.
    {
        let mut conn = pool.get().expect("Failed to get connection");
        let tx = conn.transaction().expect("Failed to begin transaction");

        // 4 data blocks with targeted distribution patterns
        struct ResilienceBlock {
            db_id: CustomUUID,
            hash_base: u8,   // base index for hash_from_index
            frag_base: u16,  // base index for uuid_from_index (fragment IDs)
            originals: u32,  // number of original chunks (chunk_type=0)
            recoveries: u32, // number of recovery chunks (chunk_type=1)
        }
        let enrichment_blocks = vec![
            ResilienceBlock {
                db_id: uuid_from_index(210),
                hash_base: 100,
                frag_base: 800,
                originals: 2,
                recoveries: 1,
            }, // → level 1 (good)
            ResilienceBlock {
                db_id: uuid_from_index(211),
                hash_base: 110,
                frag_base: 810,
                originals: 2,
                recoveries: 1,
            }, // → level 0 (critical)
            ResilienceBlock {
                db_id: uuid_from_index(212),
                hash_base: 120,
                frag_base: 820,
                originals: 3,
                recoveries: 0,
            }, // → level -1 (unrecoverable)
            ResilienceBlock {
                db_id: uuid_from_index(213),
                hash_base: 130,
                frag_base: 830,
                originals: 2,
                recoveries: 1,
            }, // → level -2 (unknown)
        ];

        for block in &enrichment_blocks {
            let total = block.originals + block.recoveries;
            let file_hash = hash_from_index(block.hash_base);
            tx.execute(
                "INSERT INTO data_blocks (id, modified_at, file_hash, fragment_count, added_bytes, file_size) VALUES (?, NULL, ?, ?, 0, 2048)",
                params![block.db_id.to_string(), file_hash, total],
            ).expect("Failed to insert enrichment data_block");

            for chunk in 0..total {
                let frag_hash = hash_from_index(block.hash_base + 1 + chunk as u8);
                let frag_id = uuid_from_index(block.frag_base + chunk as u16);
                let chunk_type: i32 = if chunk < block.originals { 0 } else { 1 };
                tx.execute(
                    "INSERT INTO fragment_hashes (data_block_id, chunk_number, local_index, fragment_id, fragment_hash, chunk_type, stored_locally) VALUES (?, ?, ?, ?, ?, ?, 1)",
                    params![block.db_id.to_string(), chunk, chunk, frag_id.to_string(), frag_hash, chunk_type],
                ).expect("Failed to insert enrichment fragment_hash");
            }
        }

        // "good" block (index 0): all fragments on nodes 0 and 1 → can survive 1 failure
        for chunk in 0..3u32 {
            let frag_hash = hash_from_index(101 + chunk as u8);
            for &node_id in &[0i32, 1] {
                tx.execute(
                    "INSERT INTO fragment_inventory (fragment_hash, node_id, self_verified_height) VALUES (?, ?, 5)",
                    params![frag_hash, node_id],
                ).expect("Failed to insert good-block inventory");
            }
        }

        // "critical" block (index 1): all fragments on node 0 only → single point of failure
        for chunk in 0..3u32 {
            let frag_hash = hash_from_index(111 + chunk as u8);
            tx.execute(
                "INSERT INTO fragment_inventory (fragment_hash, node_id, self_verified_height) VALUES (?, ?, 5)",
                params![frag_hash, 0i32],
            ).expect("Failed to insert critical-block inventory");
        }

        // "unrecoverable" block (index 2): only 2 of 3 original fragments attested → total < original_chunks
        for chunk in 0..2u32 {
            let frag_hash = hash_from_index(121 + chunk as u8);
            tx.execute(
                "INSERT INTO fragment_inventory (fragment_hash, node_id, self_verified_height) VALUES (?, ?, 5)",
                params![frag_hash, 0i32],
            ).expect("Failed to insert unrecoverable-block inventory");
        }
        // 3rd fragment (hash_from_index(123)) intentionally has NO inventory → also contributes to "unknown"

        // "unknown" block (index 3): NO inventory entries at all → pure unknown

        // --- 3. Inventory differential mismatch ---
        // Add a phantom inventory entry for node 0 (hash not in fragment_hashes as stored_locally=true)
        // → compute_inventory_differential(node=0) will report it as "fragments_removed"
        let phantom_hash = hash_from_index(250);
        tx.execute(
            "INSERT INTO fragment_inventory (fragment_hash, node_id, self_verified_height) VALUES (?, ?, 5)",
            params![phantom_hash, 0i32],
        ).expect("Failed to insert phantom inventory entry");

        tx.commit()
            .expect("Failed to commit resilience/inventory enrichment");
    }

    // (this_node carries only identity now; consensus progress lives in
    // consensus_meta, set above with the decided heights.)
}
