//! Library surface for the `ingress-server` binary and its integration tests.
//!
//! The binary (`main.rs`) is a thin shell over these modules; `tests/` drives
//! them directly.

pub mod config;
pub mod dto;
pub mod index;
