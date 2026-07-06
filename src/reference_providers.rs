//! GC reference-provider seam — the trait and its inventory registry live
//! in hopnet-projection (RFC-015) so projection crates can register
//! providers across the crate boundary.

pub use hopnet_projection::DataBlockReferenceProvider;
