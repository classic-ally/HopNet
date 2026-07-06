use hopnet_common::OnboardingFlags;
use serde::{Deserialize, Serialize};

/// Consensus payload for updating a user's profile fields.
/// For each field: None = no change, Some(None) = clear, Some(Some(v)) = set.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UpdateUserProfilePayload {
    pub user_id: i32,
    pub first_name: Option<Option<String>>,
    pub last_name: Option<Option<String>>,
    pub avatar: Option<Option<Vec<u8>>>,
}

/// Consensus payload for bitfield update of `users.onboarding_flags`.
/// Applied as `flags = (flags | set_flags) & ~clear_flags`. Idempotent.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UpdateUserOnboardingPayload {
    pub user_id: i32,
    pub set_flags: OnboardingFlags,
    pub clear_flags: OnboardingFlags,
}

/// insert_user wire payload: the new user plus their mesh-key grant
/// (RFC-014 — the grant rides the same tx so membership and all-users
/// access install atomically; induction from the genesis grant).
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct InsertUserPayload {
    pub user: crate::types::User,
    pub mesh_grant: hopnet_storage::MeshKeyGrant,
}
