// Drive-owned (RFC-015, Stage D3): the share consensus handlers live in
// hopnet_drive::handlers and register cross-crate via inventory.
// Drive-owned (RFC-015, Stage D4): the /shares routes live in
// hopnet_drive::http::shares; the host mounts the router in main.rs.
pub mod types;
