//! The host's projection registry (RFC-016 Stage 3).
//!
//! ONE static list drives every host integration point: the schema
//! install chain, the boot tripwire, takeout's exporter collection, and
//! (Stage 4+) router mounts and background-work dispatch. Adding a
//! projection to HopNet = its crate implements
//! `hopnet_projection::Projection` + one entry here — that is the whole
//! host diff.

use hopnet_projection::Projection;

/// Registration order = schema install order = FK direction (drive FKs
/// users/data_blocks; takeout's work tables reference nothing forward).
pub fn manifests() -> &'static [&'static dyn Projection] {
    &[
        &hopnet_drive::DriveProjection,
        &hopnet_takeout::TakeoutProjection,
        // photos: add its manifest here.
    ]
}
