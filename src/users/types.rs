use serde::{Serialize, Deserialize};

/// Consensus payload for updating a user's profile fields.
/// For each field: None = no change, Some(None) = clear, Some(Some(v)) = set.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UpdateUserProfilePayload {
    pub user_id: i32,
    pub first_name: Option<Option<String>>,
    pub last_name: Option<Option<String>>,
    pub avatar: Option<Option<Vec<u8>>>,
}
