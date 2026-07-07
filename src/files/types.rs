/// Storage-owned (RFC-015, Stage D3): the fragment self-attestation
/// vocabulary lives in hopnet-storage; re-exported at the old path so call
/// sites (src/, snapshotter/) don't churn.
pub use hopnet_storage::SelfCheckFragments;
