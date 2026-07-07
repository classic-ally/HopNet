use crate::consensus::rpc as consensus_rpc;
use crate::storage_host::rpc as files_rpc;
use crate::metrics::rpc as metrics_rpc;
use crate::setup;
use serde::{Deserialize, Serialize};

/// Request envelope for all iroh communication.
///
/// Stage 5b removed the bespoke engine's variants (ViewDataFetch, ViewPoll,
/// TimeoutVoteBroadcast, TcBroadcast, QcBroadcast, BallotSubmission) —
/// consensus traffic is `ConsensusMsg`/`DecidedFetch`. No wire compatibility
/// to preserve (fresh meshes only).
#[derive(Serialize, Deserialize, Debug)]
pub enum IrohRequest {
    /// Ping for connection health/warmup
    Ping { nonce: u64 },
    /// Fragment health check
    FragmentHealthCheck(files_rpc::FragmentHealthRequest),
    /// Forward transactions to the current proposer for consensus
    TransactionForward(consensus_rpc::TransactionForwardRequest),
    /// Fetch a fragment from a remote node
    FragmentFetch(files_rpc::FragmentFetchRequest),
    /// Store a fragment on a remote node
    FragmentStore(files_rpc::FragmentStoreRequest),
    /// Latency ping (echo timestamp back for RTT measurement)
    LatencyPing(metrics_rpc::LatencyPingRequest),
    /// Upload throughput data chunk (server acks, client measures speed)
    ThroughputUpload(metrics_rpc::ThroughputUploadRequest),
    /// Query remote node's storage usage
    StorageQuery(metrics_rpc::StorageQueryRequest),
    /// Deliver JoinInfo to a joining node (coordinator → new node)
    JoinDeliver(setup::JoinDeliverRequest),
    /// Malachite consensus gossip: a bincode-encoded
    /// `hopnet_consensus::codec::WireConsensusMsg`. Fire-and-forget (ack only).
    ConsensusMsg(Vec<u8>),
    /// Fetch decided (block, certificate) pairs for `[from_height, to_height]`
    /// — the decided-value sync protocol.
    DecidedFetch { from_height: i64, to_height: i64 },
}

/// Response envelope for all iroh communication
#[derive(Serialize, Deserialize, Debug)]
pub enum IrohResponse {
    /// Pong response to Ping
    Pong { nonce: u64 },
    /// Fragment health check result
    FragmentHealthCheckResponse(files_rpc::FragmentHealthResponse),
    /// Ack for transaction forward (immediate ACK before processing)
    TransactionForwardAck,
    /// Rejection: this node is not the proposer for its current (height,
    /// round). Includes the handler's position so the forwarder can retarget.
    TransactionForwardNotProposer { height: i64, round: u32 },
    /// Rejection: node is busy (kept for wire-shape stability; the malachite
    /// path never sends it)
    TransactionForwardBusy,
    /// Ack for transaction forward (final result after processing)
    TransactionForwardResponse(consensus_rpc::TransactionForwardResponse),
    /// Fragment fetch result
    FragmentFetchResponse(files_rpc::FragmentFetchResponse),
    /// Fragment store result
    FragmentStoreResponse(files_rpc::FragmentStoreResponse),
    /// Latency pong (echoed timestamp)
    LatencyPong(metrics_rpc::LatencyPongResponse),
    /// Ack for throughput upload chunk
    ThroughputAck(metrics_rpc::ThroughputAckResponse),
    /// Storage query result
    StorageResult(metrics_rpc::StorageResultResponse),
    /// Ack for JoinDeliver
    JoinAck(setup::JoinAckResponse),
    /// Error response
    Error { message: String },
    /// Ack for ConsensusMsg (the publish is fire-and-forget)
    ConsensusMsgAck,
    /// Decided (block bytes, certificate bytes) pairs, ascending and
    /// contiguous from `from_height` (bincode-encoded engine types)
    DecidedFetchResponse { items: Vec<(Vec<u8>, Vec<u8>)> },
}
