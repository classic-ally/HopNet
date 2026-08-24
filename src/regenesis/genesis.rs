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

/// This epoch's genesis height H, if the database was born from a
/// regenesis boundary (absent on epoch-1 databases, whose genesis is 0).
pub fn epoch_genesis_height(conn: &rusqlite::Connection) -> Option<u64> {
    meta_u64(conn, META_EPOCH_GENESIS_HEIGHT)
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
/// really is what the record points at, the certificate is a valid quorum
/// proof over it by the seated set this node already trusted, AND every
/// field of the record that the quorum actually decided matches what the
/// block says.
///
/// That last clause is load-bearing and was missing. A record is a
/// PEER-SUPPLIED structure; only the certificate is evidence. The
/// certificate binds the block hash, so anything inside the block is
/// transitively quorum-bound and anything outside it is just a claim.
/// `snapshot_hash` in particular decides which bytes the joiner imports
/// as its entire initial state — with it unbound, a peer could pair a
/// GENUINE block and a GENUINE certificate with a substituted hash and
/// have the joiner accept an artifact of the peer's choosing, because
/// the peer then controls both sides of the artifact comparison.
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
    verify_commit_binds_record(record, final_block)?;
    let prev_chain_id = Blake3Hash::from_bytes(record.prev_chain_id);
    hopnet_consensus::verify::verify_wire_certificate(&prev_chain_id, cert, valset, profile)
}

/// The record's certified fields, checked against the `regenesis_commit`
/// the block actually carries. Split out so the failure modes read as
/// separate refusals rather than one opaque mismatch.
///
/// The boundary block is a SOLO commit by construction (the drain empties
/// the pool and the proposer strips siblings), so "exactly one" is
/// asserted rather than assumed — a block padded with extra commits would
/// otherwise let a peer choose which one the joiner reads.
fn verify_commit_binds_record(
    record: &EpochGenesisRecord,
    final_block: &Block,
) -> Result<(), String> {
    let mut commits = final_block
        .data
        .transactions
        .iter()
        .filter(|t| t.rpc.function == "regenesis_commit");
    let commit = commits
        .next()
        .ok_or("final block carries no regenesis_commit")?;
    if commits.next().is_some() {
        return Err("final block carries more than one regenesis_commit".into());
    }

    let (payload, _) = bincode::serde::decode_from_slice::<super::RegenesisCommit, _>(
        &commit.rpc.payload,
        bincode::config::standard(),
    )
    .map_err(|e| format!("regenesis_commit payload decode: {e}"))?;

    if payload.snapshot_hash != record.snapshot_hash {
        return Err("record snapshot_hash is not the one regenesis_commit decided".into());
    }
    if payload.seal_height != record.seal_height {
        return Err(format!(
            "record seal height {} != committed seal height {}",
            record.seal_height, payload.seal_height
        ));
    }
    // The version gate the joiner will run at boot. Unbound, this is a
    // field the serving peer picks: name a version the joiner is not
    // running and it parks awaiting an upgrade that never arrives (a remote
    // denial of service), or name one it is running and it boots a binary
    // the mesh is not on. Every validator checked this against its own
    // committed target before signing, so the block's copy is the quorum's.
    if payload.target_version_code != record.required_version_code {
        return Err(format!(
            "record requires version {} but regenesis_commit decided {}",
            crate::version::format_code(record.required_version_code),
            crate::version::format_code(payload.target_version_code)
        ));
    }
    Ok(())
}

/// The seated set from a record, as an engine validator set — what a
/// node verifies lineage against when the record's set IS the set it
/// trusted (boot gate 2; the chain verification layers the overlap rule
/// on top).
pub fn record_valset(record: &EpochGenesisRecord) -> Result<HopNetValidatorSet, String> {
    valset_of(&record.seated)
}

/// An engine validator set from raw (node_id, pubkey) pairs — records
/// and chain anchors carry the same shape.
pub fn valset_of(seated: &[(i32, [u8; 32])]) -> Result<HopNetValidatorSet, String> {
    let validators = seated
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
#[derive(Debug, Serialize, Deserialize)]
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
pub fn write_lineage(
    dir: &std::path::Path,
    genesis: &EpochGenesis,
) -> Result<std::path::PathBuf, String> {
    let record = LineageRecord {
        record: genesis.record.clone(),
        final_block: hopnet_consensus::codec::encode(&genesis.final_block)
            .map_err(|e| format!("lineage block encode: {e:?}"))?,
        final_cert: hopnet_consensus::codec::encode(&genesis.final_cert)
            .map_err(|e| format!("lineage cert encode: {e:?}"))?,
    };
    let bytes = bincode::serde::encode_to_vec(&record, bincode::config::standard())
        .map_err(|e| format!("lineage encode: {e}"))?;
    write_lineage_bytes(dir, genesis.record.epoch, &bytes)
}

/// Persist an ALREADY-ENCODED lineage record. A node that arrived by
/// epoch join holds its chain in exactly the form the serving scope
/// hands out; keeping every record it verified is what lets it answer
/// the next straggler (records are kept forever).
pub fn write_lineage_bytes(
    dir: &std::path::Path,
    epoch: u64,
    bytes: &[u8],
) -> Result<std::path::PathBuf, String> {
    let path = lineage_path(dir, epoch);
    let parent = path.parent().expect("lineage path has a parent");
    std::fs::create_dir_all(parent).map_err(|e| format!("lineage dir: {e}"))?;
    let tmp = path.with_extension(format!("tmp.{}", std::process::id()));
    std::fs::write(&tmp, bytes).map_err(|e| format!("lineage write: {e}"))?;
    std::fs::rename(&tmp, &path).map_err(|e| format!("lineage rename: {e}"))?;
    Ok(path)
}

pub fn read_lineage(path: &std::path::Path) -> Result<LineageRecord, String> {
    let bytes = std::fs::read(path).map_err(|e| format!("lineage read: {e}"))?;
    decode_lineage(&bytes)
}

/// Decode a lineage record from its on-disk/wire encoding — fetched
/// records (the "regenesis" scope serves raw file bytes) and local files
/// share one codec.
pub fn decode_lineage(bytes: &[u8]) -> Result<LineageRecord, String> {
    bincode::serde::decode_from_slice(bytes, bincode::config::standard())
        .map(|(r, _)| r)
        .map_err(|e| format!("lineage decode: {e}"))
}

/// The lowest lineage epoch on disk, if any records exist.
pub fn lowest_lineage_epoch(data_dir: &std::path::Path) -> Option<u64> {
    let dir = std::fs::read_dir(data_dir.join(LINEAGE_DIR)).ok()?;
    dir.filter_map(|entry| {
        let name = entry.ok()?.file_name().into_string().ok()?;
        name.strip_prefix("epoch-")?
            .strip_suffix(".bin")?
            .parse::<u64>()
            .ok()
    })
    .min()
}

/// The mesh magic (RFC-025 §The ALPN Scheme): the first 4 bytes of the
/// ANCHOR chain id — the mesh's permanent epoch-1 identity, deliberately
/// NOT the per-epoch chain id, which would lock stragglers out of the
/// compat class at exactly the boundary it exists for. At epoch 1 the
/// anchor IS consensus_meta's chain id; past a boundary it is the
/// lowest lineage record's back-pointer. Fail-stop policy belongs to
/// the caller: the host panics at boot, because a wrong or absent magic
/// on a live member is a silent TLS partition.
pub fn mesh_magic(
    conn: &rusqlite::Connection,
    data_dir: &std::path::Path,
) -> Result<[u8; 4], String> {
    let anchor_id: [u8; 32] = if current_epoch(conn) == 1 {
        store::meta_get(conn, store::META_CHAIN_ID)
            .map_err(|e| format!("chain id: {e}"))?
            .ok_or("no chain id in consensus_meta (epoch 1)")?
            .try_into()
            .map_err(|_| "malformed chain id".to_string())?
    } else {
        // The lowest lineage record is ALWAYS epoch 2: every join
        // fetches the chain from epoch 1 (epoch_join_bootstrap_with)
        // and records are kept forever, so the first record's
        // back-pointer IS the epoch-1 chain id. Anything else on disk
        // would make prev_chain_id a mid-chain identity — refuse
        // rather than derive a wrong mesh magic.
        let lowest = lowest_lineage_epoch(data_dir)
            .ok_or("no lineage records on a post-boundary database")?;
        if lowest != 2 {
            return Err(format!(
                "lowest lineage record is epoch {lowest}, expected 2 — \
                 the epoch-1 chain id is unreachable"
            ));
        }
        read_lineage(&lineage_path(data_dir, lowest))?.record.prev_chain_id
    };
    Ok(anchor_id[..4].try_into().expect("4-byte truncation of 32"))
}

/// The trust root a lineage chain is verified from.
///
/// A straggler anchors at its own database (`from_db`): the chain id it
/// last trusted and the seated set it last saw. A fresh joiner has no
/// prior state and anchors at first contact (`tofu`) — trust rooted in
/// the authenticated join ceremony, with every other check enforced. An
/// operator re-trust is `from_db` with the seated set swapped for a
/// FINGERPRINT the operator supplies out of band; linkage, per-hop
/// quorum and the snapshot hash still hold, and the chain must land on
/// exactly the named epoch identity.
/// What roots the first hop's trust. There is deliberately no "trust
/// anything" variant: a record is peer-supplied, so with nothing to check
/// the first hop against, the whole chain is self-certified — the peer
/// declares the seated set that verifies its own certificate.
#[derive(Debug, Clone)]
pub enum ChainTrust {
    /// The ordinary straggler: the seated set this node last saw, checked
    /// by the weak-subjectivity overlap rule.
    Seated(Vec<(i32, [u8; 32])>),
    /// Operator re-trust (RFC-019 S7). Validator churn moved past the
    /// overlap window, so the operator supplies the TARGET epoch's chain
    /// id out of band and the verified chain must terminate in exactly
    /// that. This REPLACES the overlap check; it does not remove it. A
    /// fabricated chain cannot match without a blake3 preimage, since the
    /// chain id is the hash of a genesis block embedding the record.
    Fingerprint([u8; 32]),
    /// FIRST CONTACT ONLY — a node with no history, no chain id and no
    /// database to lose (the height-0 join ceremony). Trust is rooted in
    /// the authenticated join instead. Never valid for a node that
    /// already holds state: there, "trust anything" means "replace
    /// everything on one peer's word".
    FirstContact,
}

pub struct ChainAnchor {
    /// Epoch the anchor speaks for — the first record must be epoch + 1.
    pub epoch: u64,
    /// The chain id the anchor trusts (its own META_CHAIN_ID).
    pub chain_id: [u8; 32],
    /// What roots the first hop.
    pub trust: ChainTrust,
    /// Quorum profile the trusted set operated under (Byzantine-bound
    /// source for the overlap threshold).
    pub profile: String,
}

impl ChainAnchor {
    /// Anchor at this database's own trusted state: its epoch, chain id,
    /// and the seated set at its decided tip.
    pub fn from_db(conn: &rusqlite::Connection) -> Result<Self, String> {
        let tip = store::last_decided_height(conn)
            .map_err(|e| format!("decided tip: {e}"))?
            .ok_or("no decided history to anchor at")?;
        let chain_id: [u8; 32] = store::meta_get(conn, store::META_CHAIN_ID)
            .map_err(|e| format!("chain id: {e}"))?
            .ok_or("no chain id in consensus_meta")?
            .try_into()
            .map_err(|_| "malformed chain id".to_string())?;
        let profile = match store::meta_get(conn, store::META_QUORUM_PROFILE)
            .map_err(|e| format!("quorum profile: {e}"))?
        {
            Some(bytes) => String::from_utf8(bytes).map_err(|_| "malformed quorum profile")?,
            None => return Err("no quorum profile in consensus_meta".into()),
        };
        let mut trusted: Vec<(i32, [u8; 32])> =
            hopnet_consensus::validators::get_validators(conn, tip.0 + 1)
                .map_err(|e| format!("trusted seated set: {e}"))?
                .into_iter()
                .map(|v| (v.node_id, v.pubkey.to_bytes()))
                .collect();
        trusted.sort_by_key(|(id, _)| *id);
        if trusted.is_empty() {
            return Err("empty trusted seated set".into());
        }
        Ok(ChainAnchor {
            epoch: current_epoch(conn),
            chain_id,
            trust: ChainTrust::Seated(trusted),
            profile,
        })
    }

    /// The fresh-joiner TOFU anchor: trust the chain the first record
    /// claims to extend. No overlap is possible (there is no prior
    /// trusted set); the join ceremony itself is the root, exactly as
    /// in the original trusted height-0 bootstrap.
    pub fn tofu(first: &LineageRecord) -> Self {
        ChainAnchor {
            epoch: first.record.epoch.saturating_sub(1),
            chain_id: first.record.prev_chain_id,
            trust: ChainTrust::FirstContact,
            profile: first.record.quorum_profile.clone(),
        }
    }
}

/// Verify a lineage chain hop by hop from an anchor (RFC-019 S7). Per
/// record E: epoch continuity, chain-id linkage (each epoch's chain id
/// is DERIVED from the previous verified record via `genesis_block_for`
/// — never taken from the peer), structural + internal-quorum checks
/// (`verify_lineage` against the record's own seated set), and the
/// weak-subjectivity OVERLAP rule: the boundary certificate's signers
/// must intersect the anchor's trusted set in MORE than that set's
/// Byzantine bound (`f_eq` from the active quorum profile — under
/// Majority `f_eq == 0`, so the rule degenerates to at least one known
/// signer, per spec). Each verified record's seated set then becomes
/// the trusted set for the next hop.
///
/// Returns the last (target-epoch) record on success — its
/// `snapshot_hash` is what the joiner's downloaded artifact must match.
pub fn verify_lineage_chain(
    records: &[LineageRecord],
    anchor: ChainAnchor,
) -> Result<&LineageRecord, String> {
    if records.is_empty() {
        return Err("empty lineage chain".into());
    }
    let mut epoch = anchor.epoch;
    let mut chain_id = anchor.chain_id;
    let mut profile = anchor.profile;
    // Only the FIRST hop can be rooted by the anchor; every later hop is
    // checked against the set the hop before it carried.
    let (mut trusted, expected_target) = match anchor.trust {
        ChainTrust::Seated(set) => (Some(set), None),
        ChainTrust::Fingerprint(fp) => (None, Some(fp)),
        ChainTrust::FirstContact => (None, None),
    };

    for lr in records {
        let record = &lr.record;
        if record.epoch != epoch + 1 {
            return Err(format!(
                "lineage gap: expected epoch {}, record is for {}",
                epoch + 1,
                record.epoch
            ));
        }
        if record.prev_chain_id != chain_id {
            return Err(format!(
                "lineage linkage break at epoch {}: record extends a different chain",
                record.epoch
            ));
        }
        let final_block: Block = hopnet_consensus::codec::decode(&lr.final_block)
            .map_err(|e| format!("epoch {} final block decode: {e:?}", record.epoch))?;
        let cert: WireCommitCertificate = hopnet_consensus::codec::decode(&lr.final_cert)
            .map_err(|e| format!("epoch {} final cert decode: {e:?}", record.epoch))?;

        let record_profile = QuorumProfile::parse(&record.quorum_profile)
            .ok_or_else(|| format!("epoch {}: unknown quorum profile", record.epoch))?;
        verify_lineage(
            record,
            &final_block,
            &cert,
            &record_valset(record)?,
            &record_profile,
        )
        .map_err(|e| format!("epoch {} lineage: {e}", record.epoch))?;

        if let Some(ref t) = trusted {
            let anchor_profile = QuorumProfile::parse(&profile)
                .ok_or_else(|| format!("epoch {}: unknown anchor profile", record.epoch))?;
            let f = anchor_profile.f_eq(t.len() as u64);
            let overlap = hopnet_consensus::verify::count_trusted_signers(
                &Blake3Hash::from_bytes(record.prev_chain_id),
                &cert,
                &valset_of(t)?,
            ) as u64;
            if overlap <= f {
                return Err(format!(
                    "epoch {} overlap: only {overlap} of the {} trusted validators signed \
                     the boundary (need more than the Byzantine bound {f}) — churn beyond \
                     the overlap window requires manual re-trust",
                    record.epoch,
                    t.len()
                ));
            }
        }

        chain_id = *genesis_block_for(record)?.block_hash.0.as_bytes();
        trusted = Some(record.seated.clone());
        profile = record.quorum_profile.clone();
        epoch = record.epoch;
    }

    // Operator re-trust: the overlap rule could not run, so the operator's
    // out-of-band fingerprint is what roots the chain instead. Checked at
    // the END, against the derived identity of the epoch actually reached
    // — a chain that verifies internally but lands somewhere else is
    // exactly the fabrication this is here to stop. `chain_id` is the hash
    // of a genesis block embedding the record, so matching a named
    // fingerprint without the real history needs a blake3 preimage.
    if let Some(expected) = expected_target
        && chain_id != expected
    {
        return Err(format!(
            "re-trust fingerprint mismatch: the chain verifies to epoch {epoch} but its \
             identity is not the one named in the request — refusing to import"
        ));
    }
    Ok(records.last().expect("chain verified non-empty"))
}

#[cfg(test)]
pub(crate) mod tests {
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

    /// A block at `height` carrying `txs`, plus a full-quorum certificate
    /// over it by the fixture's two seated nodes. Every negative case below
    /// needs a cert that genuinely binds its own block, or the earlier
    /// cert-binding check fires and the assertion under test is never
    /// reached.
    fn certified(height: u64, txs: Vec<Transaction>) -> (Block, WireCommitCertificate) {
        let block = Block::new(BlockData {
            height,
            round: 0,
            parent_hash: Some(Blake3Hash::from_bytes([2; 32])),
            transactions: Transactions(txs),
        })
        .unwrap();
        let chain = Blake3Hash::from_bytes(PREV_CHAIN);
        let cert = WireCommitCertificate {
            height,
            round: 0,
            value_id: block.block_hash,
            signatures: vec![
                wire_commit_signature(
                    &chain,
                    &PrivKey(key(1)),
                    Height(height),
                    block.block_hash,
                    1,
                ),
                wire_commit_signature(
                    &chain,
                    &PrivKey(key(2)),
                    Height(height),
                    block.block_hash,
                    2,
                ),
            ],
        };
        (block, cert)
    }

    /// The boundary block as production builds it: a SOLO `regenesis_commit`
    /// carrying the same snapshot identity the sealed row holds. The record
    /// is verified AGAINST this block, so a fixture with an empty block
    /// could never exercise that binding.
    ///
    /// Deterministic on purpose — Ed25519 signing is, and the nonce is
    /// fixed rather than a fresh UUIDv7, so the block hash (and therefore
    /// `canonical_bytes_golden`) is reproducible.
    pub(crate) fn commit_tx(snapshot_hash: [u8; 32], seal_height: u64) -> Transaction {
        let payload = bincode::serde::encode_to_vec(
            &crate::regenesis::RegenesisCommit {
                snapshot_hash,
                seal_height,
                target_version_code: TARGET,
            },
            bincode::config::standard(),
        )
        .unwrap();
        let rpc = RpcCall {
            function: "regenesis_commit".to_string(),
            payload,
        };
        let signature = rpc.sign(&PrivKey(key(1))).unwrap();
        Transaction {
            rpc,
            submitter: SignedIdentity { id: 1, signature },
            user: None,
            nonce: "00000000-0000-7000-8000-000000000001"
                .parse::<CustomUUID>()
                .unwrap(),
        }
    }

    fn pubkey_blob(k: &SigningKey) -> Vec<u8> {
        bincode::serde::encode_to_vec(k.verifying_key(), bincode::config::standard()).unwrap()
    }

    /// A sealed epoch-N database with two seated validators and a real
    /// (signed) final certificate. `flip` reverses node insert order to
    /// prove the construction is a function of state, not write order.
    fn sealed_pool(flip: bool) -> r2d2::Pool<crate::db::SqliteConnectionManager> {
        let manager = crate::db::SqliteConnectionManager::memory();
        let pool = r2d2::Pool::builder().max_size(1).build(manager).unwrap();
        crate::db::chains::install(&pool.get().unwrap()).unwrap();
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

        hopnet_consensus::store::meta_put(
            &conn,
            hopnet_consensus::store::META_CHAIN_ID,
            &PREV_CHAIN,
        )
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
            transactions: Transactions(vec![commit_tx(SNAP, H)]),
        })
        .unwrap();
        let chain = Blake3Hash::from_bytes(PREV_CHAIN);
        let cert = WireCommitCertificate {
            height: H,
            round: 0,
            value_id: final_block.block_hash,
            signatures: vec![
                wire_commit_signature(
                    &chain,
                    &PrivKey(key(1)),
                    Height(H),
                    final_block.block_hash,
                    1,
                ),
                wire_commit_signature(
                    &chain,
                    &PrivKey(key(2)),
                    Height(H),
                    final_block.block_hash,
                    2,
                ),
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
        // Re-pinned twice, both times because the FIXTURE's boundary block
        // changed rather than the encoding: first when it gained the real
        // `regenesis_commit` it always should have carried, then when that
        // commit gained `target_version_code`. The record embeds
        // `final_block_hash`, so any change to the block moves this digest.
        // `format_version` is deliberately NOT bumped in either case —
        // `EpochGenesisRecord`'s own layout is untouched.
        assert_eq!(
            digest.to_hex().as_str(),
            "f79f2fa809b073b7967f72df93aa01991e6f5fd7b09863d7e5f6d3ff94d0800c",
            "canonical genesis encoding changed — if intentional, bump \
             format_version and re-pin"
        );
    }

    // Impact: this is the identity every production node binds its ALPNs
    // with (RFC-025) — a wrong truncation partitions the mesh at TLS at
    // the enforcement release.
    // Should: derive the magic from consensus_meta's chain id at epoch 1,
    // and from the lowest lineage record's back-pointer past a boundary —
    // the SAME magic from both paths (the identity is epoch-stable).
    #[test]
    fn mesh_magic_is_the_epoch_one_identity_on_both_paths() {
        let pool = sealed_pool(false);
        let conn = pool.get().unwrap();
        let dir = tempfile::tempdir().unwrap();

        // Epoch 1: META_CHAIN_ID is the anchor directly.
        assert_eq!(mesh_magic(&conn, dir.path()).unwrap(), PREV_CHAIN[..4]);

        // Past the boundary: epoch 2, anchor recovered from the lineage
        // back-pointer. write_lineage produces the real epoch-2 record.
        let g = build_epoch_genesis(&conn).unwrap();
        write_lineage(dir.path(), &g).unwrap();
        hopnet_consensus::store::meta_put(
            &conn,
            META_EPOCH,
            &2u64.to_be_bytes(),
        )
        .unwrap();
        assert_eq!(mesh_magic(&conn, dir.path()).unwrap(), PREV_CHAIN[..4]);
    }

    // Should: refuse to derive — naming what is missing — rather than
    // ever produce a wrong or default identity: absent chain id at epoch
    // 1, absent lineage past a boundary, a lowest record above epoch 2
    // (whose back-pointer is a mid-chain id, not the anchor), and a
    // corrupt record file.
    #[test]
    fn mesh_magic_refuses_underivable_states() {
        let dir = tempfile::tempdir().unwrap();

        // Epoch 1, schema installed, no chain id (the half-set-up state).
        let manager = crate::db::SqliteConnectionManager::memory();
        let fresh = r2d2::Pool::builder().max_size(1).build(manager).unwrap();
        crate::db::chains::install(&fresh.get().unwrap()).unwrap();
        let fresh_conn = fresh.get().unwrap();
        assert!(mesh_magic(&fresh_conn, dir.path())
            .unwrap_err()
            .contains("no chain id"));

        // Post-boundary states, on a sealed pool flipped to epoch 2.
        let pool = sealed_pool(false);
        let conn = pool.get().unwrap();
        let g = build_epoch_genesis(&conn).unwrap();
        hopnet_consensus::store::meta_put(&conn, META_EPOCH, &2u64.to_be_bytes()).unwrap();

        // No lineage records at all.
        assert!(mesh_magic(&conn, dir.path())
            .unwrap_err()
            .contains("no lineage"));

        // Lowest record above 2: refuse, never trust its back-pointer.
        let gap_dir = tempfile::tempdir().unwrap();
        let valid = {
            let tmp = tempfile::tempdir().unwrap();
            std::fs::read(write_lineage(tmp.path(), &g).unwrap()).unwrap()
        };
        write_lineage_bytes(gap_dir.path(), 3, &valid).unwrap();
        assert!(mesh_magic(&conn, gap_dir.path())
            .unwrap_err()
            .contains("expected 2"));

        // Corrupt epoch-2 file: the decode error propagates.
        let corrupt_dir = tempfile::tempdir().unwrap();
        write_lineage_bytes(corrupt_dir.path(), 2, &[0u8; 3]).unwrap();
        assert!(mesh_magic(&conn, corrupt_dir.path())
            .unwrap_err()
            .contains("lineage decode"));
    }

    // Should: report the lowest epoch present, and nothing for an absent
    // or empty lineage directory. (Previously untested; now backs the
    // boot-critical magic derivation.)
    #[test]
    fn lowest_lineage_epoch_scans_the_directory() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(lowest_lineage_epoch(dir.path()), None);
        write_lineage_bytes(dir.path(), 4, b"x").unwrap();
        write_lineage_bytes(dir.path(), 2, b"x").unwrap();
        write_lineage_bytes(dir.path(), 3, b"x").unwrap();
        assert_eq!(lowest_lineage_epoch(dir.path()), Some(2));
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
        let final_block: Block = hopnet_consensus::codec::decode(&lineage.final_block).unwrap();
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

    // Impact: this is the S7 straggler/join attack stated as a test. The
    // record is PEER-SUPPLIED and only the certificate is evidence, so a
    // field the quorum decided but nothing cross-checks is a field the
    // serving peer chooses. `snapshot_hash` selects the bytes a joiner
    // imports as its ENTIRE initial state, and the joiner compares its
    // download against that same field — so leaving it unbound hands both
    // sides of the comparison to the attacker.
    // Should: refuse a record whose snapshot_hash was swapped, even though
    //   the block and the quorum certificate are completely genuine.
    // Should: refuse a swapped seal height on the same basis.
    // Should not: accept a boundary block with no regenesis_commit to bind
    //   against, nor one padded with a second commit to choose from.
    #[test]
    fn record_fields_must_match_the_committed_regenesis_commit() {
        let pool = sealed_pool(false);
        let g = build_epoch_genesis(&pool.get().unwrap()).unwrap();
        let valset = record_valset(&g.record).unwrap();
        let profile = QuorumProfile::parse(&g.record.quorum_profile).unwrap();

        // Baseline: the honest record verifies.
        verify_lineage(&g.record, &g.final_block, &g.final_cert, &valset, &profile).unwrap();

        // THE ATTACK: genuine block, genuine cert, substituted snapshot
        // identity. Everything the old implementation checked still holds.
        let mut forged = g.record.clone();
        forged.snapshot_hash = [0xFE; 32];
        let err = verify_lineage(&forged, &g.final_block, &g.final_cert, &valset, &profile)
            .expect_err("a substituted snapshot_hash must be refused");
        assert!(err.contains("snapshot_hash"), "unexpected refusal: {err}");

        // Same shape for the height the commit decided. Bumped in BOTH the
        // record and the block so the pre-existing height check passes and
        // only the commit binding can catch it.
        let (lied_block, lied_cert) = certified(H + 1, vec![commit_tx(SNAP, H)]);
        let mut lied = g.record.clone();
        lied.seal_height = H + 1;
        lied.final_block_hash = *lied_block.block_hash.0.as_bytes();
        let err = verify_lineage(&lied, &lied_block, &lied_cert, &valset, &profile)
            .expect_err("a seal height the commit did not decide must be refused");
        assert!(err.contains("seal height"), "unexpected refusal: {err}");

        // Nothing to bind against. The certificate has to cover THIS block,
        // otherwise the earlier cert-binding check fires and the commit
        // check is never reached.
        let (empty, empty_cert) = certified(H, Vec::new());
        let mut points_at_empty = g.record.clone();
        points_at_empty.final_block_hash = *empty.block_hash.0.as_bytes();
        let err = verify_lineage(&points_at_empty, &empty, &empty_cert, &valset, &profile)
            .expect_err("a block with no regenesis_commit must be refused");
        assert!(
            err.contains("no regenesis_commit"),
            "unexpected refusal: {err}"
        );

        // Two commits would let the peer pick which one the joiner reads.
        let (padded, padded_cert) =
            certified(H, vec![commit_tx(SNAP, H), commit_tx([0x11; 32], H)]);
        let mut points_at_padded = g.record.clone();
        points_at_padded.final_block_hash = *padded.block_hash.0.as_bytes();
        let err = verify_lineage(&points_at_padded, &padded, &padded_cert, &valset, &profile)
            .expect_err("more than one regenesis_commit must be refused");
        assert!(err.contains("more than one"), "unexpected refusal: {err}");

        // The version gate the joiner runs at boot. Unbound, a serving peer
        // could name a version the joiner is not running — parking it
        // awaiting an upgrade that never arrives — or one it IS running, so
        // it boots a binary the mesh is not on.
        let mut wrong_version = g.record.clone();
        wrong_version.required_version_code = TARGET + 1;
        let err = verify_lineage(
            &wrong_version,
            &g.final_block,
            &g.final_cert,
            &valset,
            &profile,
        )
        .expect_err("a version the commit did not decide must be refused");
        assert!(
            err.contains("requires version"),
            "unexpected refusal: {err}"
        );
    }

    // Should: refuse to construct from anything but a sealed database —
    // the boundary is crossed once, from exactly one committed phase.
    #[test]
    fn refuses_unsealed_state() {
        let pool = sealed_pool(false);
        let conn = pool.get().unwrap();
        conn.execute("UPDATE regenesis_state SET phase = 1", [])
            .unwrap();
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

    // ------------------------------------------------------------------
    // Lineage CHAIN verification (RFC-019 S7): hop-by-hop linkage,
    // per-hop quorum, and the weak-subjectivity overlap rule.
    // ------------------------------------------------------------------

    fn seated_of(ids: &[u8]) -> Vec<(i32, [u8; 32])> {
        let mut s: Vec<(i32, [u8; 32])> = ids
            .iter()
            .map(|&id| (id as i32, key(id).verifying_key().to_bytes()))
            .collect();
        s.sort_by_key(|(id, _)| *id);
        s
    }

    /// Build a verifiable chain: one record per hop, each certified by
    /// the hop's own seated set over the PREVIOUS epoch's chain id, and
    /// each epoch's chain id derived from the record before it — the same
    /// derivation the verifier performs.
    fn build_chain(
        start_chain: [u8; 32],
        start_epoch: u64,
        hops: &[(&[u8], &str)],
    ) -> Vec<LineageRecord> {
        let mut chain = start_chain;
        let mut out = Vec::new();
        for (i, (ids, profile)) in hops.iter().enumerate() {
            let epoch = start_epoch + 1 + i as u64;
            let seal_height = H + i as u64 * 10;
            let snapshot_hash = [epoch as u8; 32];
            // Each hop's block must carry the commit its record claims —
            // `verify_lineage` binds the two, so an empty block here would
            // make every hop unverifiable.
            let final_block = Block::new(BlockData {
                height: seal_height,
                round: 0,
                parent_hash: Some(Blake3Hash::from_bytes([2; 32])),
                transactions: Transactions(vec![commit_tx(snapshot_hash, seal_height)]),
            })
            .unwrap();
            let domain = Blake3Hash::from_bytes(chain);
            let cert = WireCommitCertificate {
                height: seal_height,
                round: 0,
                value_id: final_block.block_hash,
                signatures: ids
                    .iter()
                    .map(|&id| {
                        wire_commit_signature(
                            &domain,
                            &PrivKey(key(id)),
                            Height(seal_height),
                            final_block.block_hash,
                            id as i32,
                        )
                    })
                    .collect(),
            };
            let record = EpochGenesisRecord {
                format_version: 1,
                epoch,
                required_version_code: TARGET,
                prev_chain_id: chain,
                final_block_hash: *final_block.block_hash.0.as_bytes(),
                seal_height,
                snapshot_hash,
                quorum_profile: (*profile).to_string(),
                seated: seated_of(ids),
            };
            chain = *genesis_block_for(&record).unwrap().block_hash.0.as_bytes();
            out.push(LineageRecord {
                record,
                final_block: hopnet_consensus::codec::encode(&final_block).unwrap(),
                final_cert: hopnet_consensus::codec::encode(&cert).unwrap(),
            });
        }
        out
    }

    fn anchor_at(trusted: Option<&[u8]>, profile: &str) -> ChainAnchor {
        ChainAnchor {
            epoch: 1,
            chain_id: PREV_CHAIN,
            trust: match trusted {
                Some(ids) => ChainTrust::Seated(seated_of(ids)),
                None => ChainTrust::FirstContact,
            },
            profile: profile.to_string(),
        }
    }

    // Impact: this is weak subjectivity in one assertion — a straggler
    // accepts a boundary only if enough validators it ALREADY trusted
    // signed it, so a mesh that re-keyed entirely cannot walk it onto a
    // forged history.
    // Should: accept a chain whose boundary certificate overlaps the
    // trusted set beyond its Byzantine bound, and return the target
    // record.
    // Should not: accept one that overlaps only up to the bound.
    #[test]
    fn overlap_rule_holds_the_byzantine_bound_under_bft() {
        // Trusted set of 4 under BFT: f_eq = 1, so 2 overlapping signers
        // pass and 1 does not.
        let two_overlap = build_chain(PREV_CHAIN, 1, &[(&[3, 4, 5], "bft")]);
        let target =
            verify_lineage_chain(&two_overlap, anchor_at(Some(&[1, 2, 3, 4]), "bft")).unwrap();
        assert_eq!(target.record.epoch, 2);
        assert_eq!(target.record.snapshot_hash, [2u8; 32]);

        let one_overlap = build_chain(PREV_CHAIN, 1, &[(&[4, 5, 6], "bft")]);
        let err = verify_lineage_chain(&one_overlap, anchor_at(Some(&[1, 2, 3, 4]), "bft"))
            .expect_err("one overlapping signer is exactly the bound, not beyond it");
        assert!(err.contains("overlap"), "{err}");
        assert!(err.contains("manual re-trust"), "{err}");
    }

    // Impact: on a home mesh the profile's Byzantine bound is zero, so
    // the spec's rule degenerates to "at least one validator I knew" —
    // recorded here so the weaker guarantee is deliberate, not a bug.
    // Should: accept a single overlapping signer under Majority.
    // Should not: accept a boundary with no overlapping signer at all.
    #[test]
    fn overlap_rule_degenerates_under_majority() {
        let one = build_chain(PREV_CHAIN, 1, &[(&[4, 5, 6], "majority")]);
        assert!(verify_lineage_chain(&one, anchor_at(Some(&[1, 2, 3, 4]), "majority")).is_ok());

        let none = build_chain(PREV_CHAIN, 1, &[(&[5, 6, 7], "majority")]);
        let err = verify_lineage_chain(&none, anchor_at(Some(&[1, 2, 3, 4]), "majority"))
            .expect_err("no overlap must refuse even under majority");
        assert!(err.contains("overlap"), "{err}");
    }

    // Impact: each hop must be anchored in the hop BEFORE it, not in the
    // node's original set — otherwise a multi-epoch straggler could
    // never catch up through legitimate validator churn.
    // Should: verify a multi-epoch chain by rotating the trusted set to
    // each verified record's seated set.
    #[test]
    fn trusted_set_rotates_per_hop() {
        // Anchor trusts {1,2}. Hop 1 is signed by {1,2,3,4} (overlap 2),
        // hop 2 by {3,4,5} — which overlaps hop 1's seated set in {3,4}
        // but the ORIGINAL anchor set in nothing. Verifying at all is
        // what proves the rotation happened.
        let chain = build_chain(
            PREV_CHAIN,
            1,
            &[(&[1, 2, 3, 4], "bft"), (&[3, 4, 5], "bft")],
        );
        let target = verify_lineage_chain(&chain, anchor_at(Some(&[1, 2]), "bft")).unwrap();
        assert_eq!(target.record.epoch, 3, "the LATEST record is the target");
        assert_eq!(target.record.snapshot_hash, [3u8; 32]);
    }

    // Should: refuse a chain that does not start at the anchor's next
    // epoch, one whose records skip an epoch mid-chain, and an empty one.
    #[test]
    fn epoch_continuity_is_required() {
        let ahead = build_chain(PREV_CHAIN, 2, &[(&[1, 2], "majority")]);
        let err = verify_lineage_chain(&ahead, anchor_at(Some(&[1, 2]), "majority"))
            .expect_err("a chain starting at epoch 3 cannot extend an epoch-1 anchor");
        assert!(err.contains("gap"), "{err}");

        let mut gapped = build_chain(
            PREV_CHAIN,
            1,
            &[(&[1, 2], "majority"), (&[1, 2], "majority")],
        );
        gapped.remove(0);
        assert!(verify_lineage_chain(&gapped, anchor_at(Some(&[1, 2]), "majority")).is_err());

        assert!(verify_lineage_chain(&[], anchor_at(Some(&[1, 2]), "majority")).is_err());
    }

    // Impact: chain ids are DERIVED from each verified record, never
    // taken from the server — a peer that swaps in a record from another
    // lineage cannot make it link.
    // Should not: accept a record whose prev_chain_id names a chain the
    // verifier never derived.
    #[test]
    fn linkage_break_is_refused() {
        let chain = build_chain([0x77; 32], 1, &[(&[1, 2], "majority")]);
        let err = verify_lineage_chain(&chain, anchor_at(Some(&[1, 2]), "majority"))
            .expect_err("a record extending a different chain must not link");
        assert!(err.contains("linkage"), "{err}");
    }

    // Impact: the per-hop quorum check is what makes a record's claim
    // about its own seated set meaningful; the overlap rule sits on top
    // of it, never instead of it.
    // Should not: accept a chain whose boundary certificate is
    // sub-quorum for the set the record itself claims.
    #[test]
    fn sub_quorum_boundary_is_refused() {
        let mut chain = build_chain(PREV_CHAIN, 1, &[(&[1, 2, 3, 4], "bft")]);
        // Drop signatures below the record's own quorum (bft, 4 seated
        // → 3 needed), keeping two signers that both overlap the anchor.
        let mut cert: WireCommitCertificate =
            hopnet_consensus::codec::decode(&chain[0].final_cert).unwrap();
        cert.signatures.truncate(2);
        chain[0].final_cert = hopnet_consensus::codec::encode(&cert).unwrap();

        let err = verify_lineage_chain(&chain, anchor_at(Some(&[1, 2]), "bft"))
            .expect_err("sub-quorum boundary certificate");
        assert!(err.contains("lineage"), "{err}");
    }

    // Impact: a fresh joiner has no prior trusted set — its root is the
    // authenticated join ceremony, exactly as in the trusted height-0
    // bootstrap. Only the FIRST hop is unanchored; from there the chain
    // binds as strictly as it does for a straggler, so a joiner can
    // never adopt a lineage no existing node could have followed.
    // Should: verify a full chain from a TOFU anchor built off the first
    // record.
    // Should not: waive overlap past the first hop, nor stop binding
    // linkage because overlap was waived.
    #[test]
    fn tofu_anchor_waives_only_the_first_hop() {
        let chain = build_chain(
            PREV_CHAIN,
            1,
            &[(&[1, 2], "majority"), (&[2, 3], "majority")],
        );
        let target = verify_lineage_chain(&chain, ChainAnchor::tofu(&chain[0])).unwrap();
        assert_eq!(target.record.epoch, 3);

        let churned = build_chain(
            PREV_CHAIN,
            1,
            &[(&[1, 2], "majority"), (&[8, 9], "majority")],
        );
        let err = verify_lineage_chain(&churned, ChainAnchor::tofu(&churned[0]))
            .expect_err("churn beyond the overlap window is refused from any anchor");
        assert!(err.contains("epoch 3 overlap"), "{err}");

        let mut tampered = chain;
        tampered[1].record.prev_chain_id = [0x5A; 32];
        let tofu = ChainAnchor::tofu(&tampered[0]);
        assert!(verify_lineage_chain(&tampered, tofu).is_err());
    }
}

