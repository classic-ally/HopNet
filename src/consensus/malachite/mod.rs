//! Malachite-engine adapters (migration Stage 4).
//!
//! Everything here is ADDITIVE: the bespoke engine stays live and nothing in
//! `main.rs` spawns these components until the Stage-5 cutover. The pieces:
//!
//! - [`app`]: `HopNetApplication` — the `hopnet_consensus::Application` impl
//!   over the `DISPATCH_TABLE`, plus the proposer's value builder.
//! - [`gossip`]: fire-and-forget publish of consensus messages over the
//!   existing `IrohTransport`, and the standalone accept loop that feeds the
//!   consensus shell (merged into `net::handler` at Stage 5).
//! - [`sync`]: the decided-value sync client (replaces `catch_up_state` for
//!   the new engine).

pub mod app;
pub mod gossip;
pub mod sync;
