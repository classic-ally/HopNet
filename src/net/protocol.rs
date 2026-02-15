use serde::{Deserialize, Serialize};
use crate::files::rpc as files_rpc;
use crate::consensus::rpc as consensus_rpc;

/// Request envelope for all iroh communication
#[derive(Serialize, Deserialize, Debug)]
pub enum IrohRequest {
    /// Ping for connection health/warmup
    Ping { nonce: u64 },
    /// Fragment health check
    FragmentHealthCheck(files_rpc::FragmentHealthRequest),
    /// Fetch consensus data for a specific view (catch-up)
    ViewDataFetch(consensus_rpc::ViewDataRequest),
    /// Poll for current view number (sync detection)
    ViewPoll(consensus_rpc::ViewPollRequest),
    /// Broadcast timeout vote
    TimeoutVoteBroadcast(consensus_rpc::TimeoutVoteBroadcastRequest),
    /// Broadcast timeout certificate
    TcBroadcast(consensus_rpc::TcBroadcastRequest),
    /// Broadcast quorum certificate
    QcBroadcast(consensus_rpc::QcBroadcastRequest),
    /// Submit ballot for voting (returns signed vote)
    BallotSubmission(consensus_rpc::BallotRequest),
    /// Forward transactions to leader for consensus
    TransactionForward(consensus_rpc::TransactionForwardRequest),
    /// Fetch a fragment from a remote node
    FragmentFetch(files_rpc::FragmentFetchRequest),
    /// Store a fragment on a remote node
    FragmentStore(files_rpc::FragmentStoreRequest),
}

/// Response envelope for all iroh communication
#[derive(Serialize, Deserialize, Debug)]
pub enum IrohResponse {
    /// Pong response to Ping
    Pong { nonce: u64 },
    /// Fragment health check result
    FragmentHealthCheckResponse(files_rpc::FragmentHealthResponse),
    /// View consensus data for a specific view
    ViewDataFetchResponse(consensus_rpc::ViewDataResponse),
    /// Current view number
    ViewPollResponse(consensus_rpc::ViewPollResponse),
    /// Ack for timeout vote broadcast
    TimeoutVoteBroadcastResponse(consensus_rpc::TimeoutVoteBroadcastResponse),
    /// Ack for TC broadcast
    TcBroadcastResponse(consensus_rpc::TcBroadcastResponse),
    /// Ack for QC broadcast
    QcBroadcastResponse(consensus_rpc::QcBroadcastResponse),
    /// Ballot vote response (signed vote)
    BallotSubmissionResponse(consensus_rpc::BallotResponse),
    /// Ack for transaction forward
    TransactionForwardResponse(consensus_rpc::TransactionForwardResponse),
    /// Fragment fetch result
    FragmentFetchResponse(files_rpc::FragmentFetchResponse),
    /// Fragment store result
    FragmentStoreResponse(files_rpc::FragmentStoreResponse),
    /// Error response
    Error { message: String },
}

impl IrohRequest {
    /// Extract the consensus view from a consensus message, if applicable.
    /// Used for message-driven catch-up: if the message's view is ahead of ours,
    /// we catch up before dispatching.
    pub fn consensus_view(&self) -> Option<i32> {
        match self {
            IrohRequest::BallotSubmission(req) => Some(req.ballot.data.view),
            IrohRequest::TimeoutVoteBroadcast(req) => Some(req.timeout_vote.data.view_number),
            IrohRequest::TcBroadcast(req) => Some(req.tc.view_number),
            IrohRequest::QcBroadcast(req) => Some(req.qc.view_number),
            IrohRequest::TransactionForward(req) => Some(req.view),
            _ => None, // Ping, FragmentHealthCheck, FragmentFetch, FragmentStore, ViewDataFetch, ViewPoll
        }
    }
}
