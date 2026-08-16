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
/// users/data_blocks; photos FKs users/data_blocks/shared_libraries;
/// takeout's work tables reference nothing forward).
pub fn manifests() -> &'static [&'static dyn Projection] {
    &[
        &hopnet_drive::DriveProjection,
        &hopnet_photos::PhotosProjection,
        &hopnet_takeout::TakeoutProjection,
    ]
}

/// DeviceToken-authed surfaces the host mounts OUTSIDE the manifest
/// system (RFC-023): (full prefix, min_client). The named sibling of
/// the tripwire's `storage_host::handlers::TX_FUNCTIONS` check —
/// host-owned surfaces must not escape the coverage rule just because
/// no manifest declares them. S3's enforcement layer reads minimums
/// from here; the coverage assertion validates the codes.
pub const HOST_DEVICE_TOKEN_MIN_CLIENT: &[(&str, u32)] = &[
    // RFC-018 S8 statfs: host-owned because capacity math lives in
    // views::resilience, which the drive crate cannot see.
    ("/api/integrations/mount/statfs", 20260802),
    // Photo-ingress daemon dispatch surface: host-owned because the
    // handlers close over AppState (photos declares no mounts).
    ("/api/photos/client", 20260802),
];

/// RFC-023 coverage assertion — the post-capabilities sibling of
/// `assert_projection_registrations` (which runs before AppState exists
/// and so cannot walk `mounts()`): every `DeviceToken` surface, manifest
/// or host-owned, must resolve a valid minimum client version. Versioning
/// is a precondition of the auth class, not an option.
pub fn assert_client_compat_coverage(caps: &hopnet_projection::host::HostCapabilities) {
    use hopnet_common::version::code_is_valid;
    use hopnet_projection::AuthClass;

    for m in manifests() {
        for mount in m.mounts(caps) {
            if mount.auth != AuthClass::DeviceToken {
                continue;
            }
            let resolved = mount.min_client.or(m.min_client());
            let code = resolved.unwrap_or_else(|| {
                panic!(
                    "{} mount '{}' is DeviceToken-authed but resolves no min_client \
                     (neither the mount nor the projection declares one) — RFC-023 \
                     makes versioning a precondition of the auth class",
                    m.name(),
                    mount.prefix
                )
            });
            assert!(
                code_is_valid(code),
                "{} mount '{}' declares non-CalVer min_client {code}",
                m.name(),
                mount.prefix
            );
        }
    }
    for (prefix, code) in HOST_DEVICE_TOKEN_MIN_CLIENT {
        assert!(
            code_is_valid(*code),
            "host surface '{prefix}' declares non-CalVer min_client {code}"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Impact: coverage is what turns RFC-023's "versioning is a
    // precondition of the DeviceToken auth class" from aspiration into
    // mechanics — a new DeviceToken surface with no declaration must
    // fail here (and at boot), not ship unversioned and recreate the
    // invisible-skew gap.
    // Should: resolve a valid minimum client version for every
    // DeviceToken surface, manifest-declared and host-owned alike.
    #[test]
    fn device_token_surfaces_all_declare_minimums() {
        let app_state = crate::consensus::tests::create_test_app_state();
        let caps = crate::capabilities::build_capabilities(&app_state);

        assert_client_compat_coverage(&caps);

        for m in manifests() {
            for mount in m.mounts(&caps) {
                if mount.auth != hopnet_projection::AuthClass::DeviceToken {
                    continue;
                }
                let code = mount.min_client.or(m.min_client()).unwrap();
                assert!(hopnet_common::version::code_is_valid(code));
            }
        }
        for (_, code) in HOST_DEVICE_TOKEN_MIN_CLIENT {
            assert!(hopnet_common::version::code_is_valid(*code));
        }
    }
}
