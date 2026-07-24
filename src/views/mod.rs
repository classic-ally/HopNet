//! View models: read-only assemblies shaped for one frontend consumer.
//!
//! Deliberately compartmentalised for eventual extraction into an admin module.
//! The rule that makes that cheap: **this module owns no domain logic, only
//! composition.** Every number it reports is produced by a primitive that stays
//! in its own home — `db::resilience`, `storage_host::substrate_host`,
//! `consensus::evidence`, `hopnet_common::quorum`. Moving this module later
//! means moving a thin assembler, with nothing to untangle from a domain.

pub mod resilience;
pub mod routes;
