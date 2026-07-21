//! HopNet consensus: Malachite (Tendermint) engine host and adapters.
//!
//! This crate owns the consensus side of HopNet: the Malachite `Context`
//! implementation over HopNet's block/transaction types, canonical signing
//! payloads, quorum profiles (BFT and crash-fault majority), and host-side
//! certificate verification. The application plugs in behind trait seams
//! (`Application`, `ConsensusGossip` — arriving in later stages); the engine's
//! protocol state machine is upstream-verified (Quint + MBT), so tests here
//! cover OUR surface: determinism, codec fidelity, signing injectivity, and
//! certificate verification.
//!
//! During the migration (plan: consensus-malachite branch) the main crate's
//! bespoke engine stays live; the types defined here are the future canonical
//! versions and intentionally duplicate main-crate shapes until the Stage-5
//! swap re-points the main crate at this crate.

pub mod codec;
pub mod config;
pub mod context;
pub mod host;
pub mod membership;
#[cfg(feature = "shell")]
pub mod shell;
pub mod signing;
pub mod sim;
pub mod store;
pub mod traits;
pub mod types;
pub mod validators;
pub mod verify;

pub use config::{MalachiteThresholds, QuorumProfile};
pub use context::{
    Address, ConsensusProposal, ConsensusVote, Height, HopNetContext, HopNetValidatorSet, Validator,
};
pub use signing::Ed25519Scheme;
pub use types::{Block, BlockData, BlockError, Transaction, Transactions};

/// Convenience alias: the engine instantiated over HopNet's context.
pub type SignedConsensusMsg = malachitebft_core_consensus::SignedConsensusMsg<HopNetContext>;
pub type CommitCertificate = malachitebft_core_types::CommitCertificate<HopNetContext>;

// Engine types the embedding application needs at the seams, re-exported so
// it never depends on the malachite crates directly (version pin isolation).
pub use malachitebft_core_consensus::{Params, PeerId};
pub use malachitebft_core_types::{LinearTimeouts, Round, Timeout, Validity, ValuePayload};
