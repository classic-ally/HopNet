//! HopNet publisher for the Apple Photos ingress daemon (the concrete impl
//! behind ingress-core's `Publisher` seam).
//!
//! Three layers:
//! - [`map`] — sidecar/record → RFC-011 `PhotoAsset` mapping (pure).
//! - [`dispatch`] — `HttpDispatch`, a `PhotoDispatch` over the node's
//!   thin-client routes (`/api/photos/client/*`) with an RFC-012 device
//!   token; streams resource bytes, never buffers.
//! - [`flow`] — `NodePublisher`, the confirm-then-retry publish flow that
//!   satisfies the publisher idempotency contract against consensus's
//!   duplicate-photo_id rejection.

pub mod dispatch;
pub mod flow;
pub mod map;

pub use dispatch::HttpDispatch;
pub use flow::NodePublisher;
