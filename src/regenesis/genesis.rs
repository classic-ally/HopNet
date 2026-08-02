//! Epoch genesis construction (RFC-019 S6): every node independently
//! derives the epoch-N+1 genesis from its own sealed database — no peer
//! involved. The canonical genesis is a deterministic engine Block at
//! the boundary height H whose single synthetic transaction carries the
//! `EpochGenesisRecord`; the new epoch's chain id is that block's hash,
//! exactly the rule epoch 1 already uses. Every record field is a
//! deterministic read of replicated-or-certified state, so all honest
//! nodes build byte-identical geneses.
//!
//! The one legitimately node-divergent input — the epoch-N final decide
//! CERTIFICATE (`decided_certificates` is node-local: different vote
//! subsets are valid proofs of the same decision) — is deliberately
//! EXCLUDED from the canonical bytes. Only the final block's HASH is
//! bound into the record; the certificate travels beside the genesis in
//! the lineage record as this node's evidence, verified against the
//! seated set (boot gate 2, and S7's joiner path) rather than compared
//! byte-for-byte.

use serde::{Deserialize, Serialize};

use hopnet_common::CustomUUID;
use hopnet_consensus::codec::WireCommitCertificate;
use hopnet_consensus::config::QuorumProfile;
use hopnet_consensus::context::{HopNetContext, HopNetValidatorSet, Validator};
use hopnet_consensus::store;
use hopnet_consensus::types::{
    Blake3Hash, Block, BlockData, RpcCall, SignedIdentity, Transaction, Transactions,
};

use crate::db::regenesis::{RegenesisPhase, read_regenesis_state};

/// Function name of the synthetic genesis transaction. Never dispatched:
/// the genesis block sits below the engine's start height, so no handler
/// exists or is needed — the name is a self-describing label for anyone
/// inspecting decided_blocks.
pub const EPOCH_GENESIS_FN: &str = "epoch_genesis";

/// consensus_meta key: this database's epoch number, big-endian u64.
/// ABSENT means epoch 1 (every pre-regenesis mesh); the boot transition
/// writes N+1 into the fresh database.
pub const META_EPOCH: &str = "epoch";

/// consensus_meta key: the epoch's genesis height H, big-endian u64.
/// Read by the rollback-window cleanup (delete the retained epoch-N
/// database once a height past H decides).
pub const META_EPOCH_GENESIS_HEIGHT: &str = "epoch_genesis_height";

/// Lineage records live in `<db_dir>/lineage/epoch-<E>.bin`, kept
/// FOREVER (spec: Archival & Retention) — bytes per epoch, not blocks.
pub const LINEAGE_DIR: &str = "lineage";

/// The canonical epoch genesis content. Canonical bytes = bincode
/// standard encoding (the payload precedent throughout the tree); the
/// determinism golden test pins them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpochGenesisRecord {
    pub format_version: u16,
    /// N+1.
    pub epoch: u64,
    /// The version every node must run to boot this epoch — the target
    /// from `regenesis_start`, exact match (boot gate 1).
    pub required_version_code: u32,
    /// Epoch-N chain id: the lineage back-pointer.
    pub prev_chain_id: [u8; 32],
    /// Hash of epoch-N's final decided block (height H).
    pub final_block_hash: [u8; 32],
    /// The boundary height H. Heights are continuous: this genesis sits
    /// at H, the first decided block of the new epoch is H+1.
    pub seal_height: u64,
    /// Certified snapshot identity: blake3 over the canonical artifact
    /// bytes, as committed by `regenesis_commit`.
    pub snapshot_hash: [u8; 32],
    /// Carried VERBATIM from epoch N (consensus_meta is node-local, so
    /// the profile must ride the record to reach the fresh database).
    pub quorum_profile: String,
    /// The carried validator set — seated as of the seal, node_id
    /// ascending. The boundary never changes the seated set (the final
    /// block is a solo regenesis_commit).
    pub seated: Vec<(i32, [u8; 32])>,
}

impl EpochGenesisRecord {
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, String> {
        bincode::serde::encode_to_vec(self, bincode::config::standard())
            .map_err(|e| format!("genesis record encode: {e}"))
    }
}

/// Everything the boot transition needs: the canonical record and block,
/// plus this node's lineage evidence (the epoch-N final pair).
pub struct EpochGenesis {
    pub record: EpochGenesisRecord,
    /// The canonical genesis block at H; its hash is the new chain id.
    pub block: Block,
    /// Epoch-N's final decided block (lineage evidence).
    pub final_block: Block,
    /// This node's decide certificate for the final block — a valid
    /// quorum proof, but NOT byte-canonical across nodes.
    pub final_cert: WireCommitCertificate,
}

fn meta_u64(conn: &rusqlite::Connection, key: &str) -> Option<u64> {
    let bytes = store::meta_get(conn, key).ok()??;
    Some(u64::from_be_bytes(bytes.try_into().ok()?))
}

/// This database's epoch. Absent key = 1: every mesh born before
/// regenesis existed — and every fresh mesh — is epoch 1.
pub fn current_epoch(conn: &rusqlite::Connection) -> u64 {
    meta_u64(conn, META_EPOCH).unwrap_or(1)
}

/// Build the epoch-N+1 genesis from a SEALED database. Pure function of
/// committed-or-certified state: two honest replicas of the same sealed
/// epoch produce byte-identical records and blocks.
pub fn build_epoch_genesis(conn: &rusqlite::Connection) -> Result<EpochGenesis, String> {
    let state = read_regenesis_state(conn).map_err(|e| format!("regenesis state: {e:?}"))?;
    if state.phase != RegenesisPhase::Sealed {
        return Err(format!("not sealed (phase {:?})", state.phase));
    }
    let seal_height = state.seal_height.ok_or("sealed row missing seal_height")?;
    let required_version_code = state
        .target_version_code
        .ok_or("sealed row missing target_version_code")?;
    let snapshot_hash: [u8; 32] = state
        .snapshot_hash
        .ok_or("sealed row missing snapshot_hash")?
        .try_into()
        .map_err(|_| "committed snapshot_hash is not 32 bytes".to_string())?;

    let mut pairs = store::decided_range(
        conn,
        hopnet_consensus::context::Height(seal_height),
        hopnet_consensus::context::Height(seal_height),
    )
    .map_err(|e| format!("decided_range({seal_height}): {e}"))?;
    let (final_block, final_cert) = pairs
        .pop()
        .ok_or_else(|| format!("no decided block at seal height {seal_height}"))?;

    let prev_chain_id: [u8; 32] = store::meta_get(conn, store::META_CHAIN_ID)
        .map_err(|e| format!("chain id: {e}"))?
        .ok_or("no chain id in consensus_meta")?
        .try_into()
        .map_err(|_| "malformed chain id".to_string())?;

    // Verbatim carry: the stored profile bytes are identical mesh-wide
    // (every installer writes QuorumProfile::as_str at genesis).
    let quorum_profile = match store::meta_get(conn, store::META_QUORUM_PROFILE)
        .map_err(|e| format!("quorum profile: {e}"))?
    {
        Some(bytes) => String::from_utf8(bytes).map_err(|_| "malformed quorum profile")?,
        None => return Err("no quorum profile in consensus_meta".into()),
    };

    let mut seated: Vec<(i32, [u8; 32])> =
        hopnet_consensus::validators::get_validators(conn, seal_height + 1)
            .map_err(|e| format!("seated set: {e}"))?
            .into_iter()
            .map(|v| (v.node_id, v.pubkey.to_bytes()))
            .collect();
    seated.sort_by_key(|(id, _)| *id);
    if seated.is_empty() {
        return Err("empty seated set at the seal".into());
    }

    let record = EpochGenesisRecord {
        format_version: 1,
        epoch: current_epoch(conn) + 1,
        required_version_code,
        prev_chain_id,
        final_block_hash: *final_block.block_hash.0.as_bytes(),
        seal_height,
        snapshot_hash,
        quorum_profile,
        seated,
    };
    let block = genesis_block_for(&record)?;
    Ok(EpochGenesis {
        record,
        block,
        final_block,
        final_cert,
    })
}

/// The canonical genesis block for a record: height H, round 0, parent =
/// the epoch-N final block hash, one synthetic unsigned transaction
/// carrying the record. Deterministic end to end — the nonce derives
/// from the record bytes (a v7 nonce would be random and break
/// byte-identity), and nothing ever verifies the zero signature because
/// the block sits below start height and is never dispatched.
pub fn genesis_block_for(record: &EpochGenesisRecord) -> Result<Block, String> {
    let payload = record.canonical_bytes()?;
    let mut nonce = [0u8; 16];
    nonce.copy_from_slice(&blake3::hash(&payload).as_bytes()[..16]);
    let tx = Transaction {
        rpc: RpcCall {
            function: EPOCH_GENESIS_FN.to_string(),
            payload,
        },
        submitter: SignedIdentity {
            id: -1,
            signature: ed25519_dalek::Signature::from_bytes(&[0u8; 64]),
        },
        user: None,
        nonce: CustomUUID::from_fixed_bytes(nonce),
    };
    Block::new(BlockData {
        height: record.seal_height,
        round: 0,
        parent_hash: Some(Blake3Hash::from_bytes(record.final_block_hash)),
        transactions: Transactions(vec![tx]),
    })
    .map_err(|e| format!("genesis block: {e:?}"))
}

/// The synthetic trusted certificate installed beside the genesis block
/// — empty signatures, the epoch-1 precedent: decided_certificates is
/// node-local, and the genesis is trusted by construction (each node
/// built and verified it itself).
pub fn synthetic_genesis_cert(block: &Block) -> WireCommitCertificate {
    WireCommitCertificate {
        height: block.data.height,
        round: 0,
        value_id: block.block_hash,
        signatures: Vec::new(),
    }
}

/// Boot gate 2 (and S7's joiner verification): the epoch-N final block
/// really is what the record points at, and the certificate is a valid
/// quorum proof over it by the seated set this node already trusted.
pub fn verify_lineage(
    record: &EpochGenesisRecord,
    final_block: &Block,
    cert: &WireCommitCertificate,
    valset: &HopNetValidatorSet,
    profile: &QuorumProfile,
) -> Result<(), String> {
    final_block
        .verify()
        .map_err(|e| format!("final block malformed: {e:?}"))?;
    if *final_block.block_hash.0.as_bytes() != record.final_block_hash {
        return Err("final block does not match the record's hash".into());
    }
    if final_block.data.height != record.seal_height {
        return Err(format!(
            "final block height {} != seal height {}",
            final_block.data.height, record.seal_height
        ));
    }
    if cert.value_id != final_block.block_hash || cert.height != final_block.data.height {
        return Err("certificate does not bind the final block".into());
    }
    let prev_chain_id = Blake3Hash::from_bytes(record.prev_chain_id);
    hopnet_consensus::verify::verify_wire_certificate(&prev_chain_id, cert, valset, profile)
}

/// The seated set from a record, as an engine validator set — what a
/// node verifies lineage against when the record's set IS the set it
/// trusted (boot gate 2; S7 layers the overlap rule on top).
pub fn record_valset(record: &EpochGenesisRecord) -> Result<HopNetValidatorSet, String> {
    let validators = record
        .seated
        .iter()
        .map(|(id, pk)| {
            ed25519_dalek::VerifyingKey::from_bytes(pk)
                .map(|key| Validator::new(*id, hopnet_consensus::types::PubKey(key)))
                .map_err(|e| format!("seated pubkey for node {id}: {e}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(HopNetValidatorSet::new(validators))
}

/// A node's own lineage record: the canonical genesis content plus this
/// node's evidence for it. `final_cert` is a valid proof, not a
/// canonical byte string — two nodes' lineage files may differ there
/// and both verify.
#[derive(Serialize, Deserialize)]
pub struct LineageRecord {
    pub record: EpochGenesisRecord,
    pub final_block: Vec<u8>,
    pub final_cert: Vec<u8>,
}

pub fn lineage_path(dir: &std::path::Path, epoch: u64) -> std::path::PathBuf {
    dir.join(LINEAGE_DIR).join(format!("epoch-{epoch}.bin"))
}

/// Write the lineage record atomically (tmp + rename), creating the
/// lineage directory. Idempotent: recomputation writes equal content
/// (modulo the node-local certificate, which is stable per node).
pub fn write_lineage(dir: &std::path::Path, genesis: &EpochGenesis) -> Result<std::path::PathBuf, String> {
    let record = LineageRecord {
        record: genesis.record.clone(),
        final_block: hopnet_consensus::codec::encode(&genesis.final_block)
            .map_err(|e| format!("lineage block encode: {e:?}"))?,
        final_cert: hopnet_consensus::codec::encode(&genesis.final_cert)
            .map_err(|e| format!("lineage cert encode: {e:?}"))?,
    };
    let bytes = bincode::serde::encode_to_vec(&record, bincode::config::standard())
        .map_err(|e| format!("lineage encode: {e}"))?;
    let path = lineage_path(dir, genesis.record.epoch);
    let parent = path.parent().expect("lineage path has a parent");
    std::fs::create_dir_all(parent).map_err(|e| format!("lineage dir: {e}"))?;
    let tmp = path.with_extension(format!("tmp.{}", std::process::id()));
    std::fs::write(&tmp, &bytes).map_err(|e| format!("lineage write: {e}"))?;
    std::fs::rename(&tmp, &path).map_err(|e| format!("lineage rename: {e}"))?;
    Ok(path)
}

pub fn read_lineage(path: &std::path::Path) -> Result<LineageRecord, String> {
    let bytes = std::fs::read(path).map_err(|e| format!("lineage read: {e}"))?;
    bincode::serde::decode_from_slice(&bytes, bincode::config::standard())
        .map(|(r, _)| r)
        .map_err(|e| format!("lineage decode: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;
    use hopnet_consensus::context::Height;
    use hopnet_consensus::types::PrivKey;
    use hopnet_consensus::verify::wire_commit_signature;
    use rusqlite::params;

    const H: u64 = 7;
    const TARGET: u32 = 20260800;
    const SNAP: [u8; 32] = [0xAB; 32];
    const PREV_CHAIN: [u8; 32] = [3; 32];

    fn key(seed: u8) -> SigningKey {
        SigningKey::from_bytes(&[seed; 32])
    }

    fn pubkey_blob(k: &SigningKey) -> Vec<u8> {
        bincode::serde::encode_to_vec(&k.verifying_key(), bincode::config::standard()).unwrap()
    }

    /// A sealed epoch-N database with two seated validators and a real
    /// (signed) final certificate. `flip` reverses node insert order to
    /// prove the construction is a function of state, not write order.
    fn sealed_pool(flip: bool) -> r2d2::Pool<crate::db::SqliteConnectionManager> {
        let manager = crate::db::SqliteConnectionManager::memory();
        let pool = r2d2::Pool::builder().max_size(1).build(manager).unwrap();
        crate::db::shared::initialize(pool.get().unwrap()).unwrap();
        let conn = pool.get().unwrap();

        conn.execute(
            "INSERT INTO users (user_id, username, pubkey, x25519_pubkey, encrypted_privkey, key_salt)
             VALUES (1, 'test', ?, ?, ?, ?)",
            params![pubkey_blob(&key(9)), vec![0u8; 32], vec![0u8; 44], vec![0u8; 16]],
        )
        .unwrap();
        let mut ids = [1i32, 2i32];
        if flip {
            ids.reverse();
        }
        for id in ids {
            conn.execute(
                "INSERT INTO nodes (node_id, name, owner, pubkey) VALUES (?, ?, 1, ?)",
                params![id, format!("node{id}"), pubkey_blob(&key(id as u8))],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO validators (effective_height, node_id, is_active) VALUES (1, ?, 1)",
                params![id],
            )
            .unwrap();
        }

        hopnet_consensus::store::meta_put(&conn, hopnet_consensus::store::META_CHAIN_ID, &PREV_CHAIN)
            .unwrap();
        hopnet_consensus::store::meta_put(
            &conn,
            hopnet_consensus::store::META_QUORUM_PROFILE,
            b"majority",
        )
        .unwrap();

        let final_block = Block::new(BlockData {
            height: H,
            round: 0,
            parent_hash: Some(Blake3Hash::from_bytes([2; 32])),
            transactions: Transactions(Vec::new()),
        })
        .unwrap();
        let chain = Blake3Hash::from_bytes(PREV_CHAIN);
        let cert = WireCommitCertificate {
            height: H,
            round: 0,
            value_id: final_block.block_hash,
            signatures: vec![
                wire_commit_signature(&chain, &PrivKey(key(1)), Height(H), final_block.block_hash, 1),
                wire_commit_signature(&chain, &PrivKey(key(2)), Height(H), final_block.block_hash, 2),
            ],
        };
        hopnet_consensus::store::install_genesis(&conn, &final_block, &cert).unwrap();

        conn.execute(
            "INSERT INTO regenesis_state (internal_id, phase, target_version_code, snapshot_hash, seal_height)
             VALUES (1, 2, ?, ?, ?)",
            params![TARGET, &SNAP[..], H as i64],
        )
        .unwrap();
        drop(conn);
        pool
    }

    // Impact: independent construction is the whole trust model — no node
    // ever fetches a genesis in S6, so byte-identity across replicas IS
    // the agreement mechanism for the new chain id.
    // Should: build byte-identical canonical records and genesis blocks
    // from two separately-written replicas of the same sealed state.
    // Should not: let row insert order leak into the canonical bytes.
    #[test]
    fn construction_is_deterministic_across_replicas() {
        let (a, b) = (sealed_pool(false), sealed_pool(true));
        let ga = build_epoch_genesis(&a.get().unwrap()).unwrap();
        let gb = build_epoch_genesis(&b.get().unwrap()).unwrap();
        assert_eq!(ga.record, gb.record);
        assert_eq!(
            ga.record.canonical_bytes().unwrap(),
            gb.record.canonical_bytes().unwrap()
        );
        assert_eq!(ga.block.block_hash, gb.block.block_hash);
        assert_eq!(ga.record.epoch, 2);
        assert_eq!(ga.record.seal_height, H);
        assert_eq!(ga.record.required_version_code, TARGET);
        assert_eq!(ga.record.snapshot_hash, SNAP);
        assert_eq!(ga.record.quorum_profile, "majority");
        assert_eq!(ga.record.seated.len(), 2);
        // The new chain id is the genesis block hash — a new signing
        // domain, never the old chain id.
        assert_ne!(*ga.block.block_hash.0.as_bytes(), PREV_CHAIN);
        assert_eq!(ga.block.data.height, H);
        assert_eq!(
            ga.block.data.parent_hash.unwrap().0.as_bytes(),
            ga.final_block.block_hash.0.as_bytes()
        );
    }

    // Impact: the golden pins the canonical encoding — any accidental
    // nondeterminism (wall clock, randomness, map ordering) or silent
    // format change breaks here first, before it splits a real mesh.
    // Should: produce exactly the pinned canonical bytes for the fixture.
    #[test]
    fn canonical_bytes_golden() {
        let pool = sealed_pool(false);
        let g = build_epoch_genesis(&pool.get().unwrap()).unwrap();
        let digest = blake3::hash(&g.record.canonical_bytes().unwrap());
        assert_eq!(
            digest.to_hex().as_str(),
            "f56b90cd1454fea53030fa232865cf3876bcf7da311256bf4b8f06c2da8d2f54",
            "canonical genesis encoding changed — if intentional, bump \
             format_version and re-pin"
        );
    }

    // Should: round-trip the lineage record through disk and verify the
    // certificate against the record's own seated set.
    // Should not: verify a tampered signature or a sub-quorum certificate.
    #[test]
    fn lineage_roundtrip_and_verification() {
        let pool = sealed_pool(false);
        let g = build_epoch_genesis(&pool.get().unwrap()).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = write_lineage(dir.path(), &g).unwrap();
        assert_eq!(path, lineage_path(dir.path(), 2));

        let lineage = read_lineage(&path).unwrap();
        assert_eq!(lineage.record, g.record);
        let final_block: Block =
            hopnet_consensus::codec::decode(&lineage.final_block).unwrap();
        let cert: WireCommitCertificate =
            hopnet_consensus::codec::decode(&lineage.final_cert).unwrap();
        let valset = record_valset(&lineage.record).unwrap();
        let profile = QuorumProfile::parse(&lineage.record.quorum_profile).unwrap();
        verify_lineage(&lineage.record, &final_block, &cert, &valset, &profile).unwrap();

        // Tampered signature: flip one byte.
        let mut bad = cert.clone();
        bad.signatures[0].1.0[0] ^= 1;
        assert!(verify_lineage(&lineage.record, &final_block, &bad, &valset, &profile).is_err());

        // Sub-quorum: majority of 2 needs both signatures.
        let mut thin = cert.clone();
        thin.signatures.truncate(1);
        assert!(verify_lineage(&lineage.record, &final_block, &thin, &valset, &profile).is_err());

        // Wrong block: the record's hash must bind.
        let other = Block::new(BlockData {
            height: H,
            round: 0,
            parent_hash: None,
            transactions: Transactions(Vec::new()),
        })
        .unwrap();
        assert!(verify_lineage(&lineage.record, &other, &cert, &valset, &profile).is_err());
    }

    // Should: refuse to construct from anything but a sealed database —
    // the boundary is crossed once, from exactly one committed phase.
    #[test]
    fn refuses_unsealed_state() {
        let pool = sealed_pool(false);
        let conn = pool.get().unwrap();
        conn.execute("UPDATE regenesis_state SET phase = 1", []).unwrap();
        assert!(build_epoch_genesis(&conn).is_err());
        conn.execute("DELETE FROM regenesis_state", []).unwrap();
        assert!(build_epoch_genesis(&conn).is_err());
    }

    // Should: derive the synthetic genesis certificate from the block —
    // empty signatures, trusted by construction (the epoch-1 precedent).
    #[test]
    fn synthetic_cert_binds_the_block() {
        let pool = sealed_pool(false);
        let g = build_epoch_genesis(&pool.get().unwrap()).unwrap();
        let cert = synthetic_genesis_cert(&g.block);
        assert_eq!(cert.height, H);
        assert_eq!(cert.value_id, g.block.block_hash);
        assert!(cert.signatures.is_empty());
    }
}
