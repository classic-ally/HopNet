use serde::{Serialize, Deserialize};
use typeshare::typeshare;

/// Request to share a file with another user
#[derive(Serialize, Deserialize)]
#[typeshare]
pub struct ShareRequest {
    pub inode_id: String,
    pub recipient_username: String,
}

/// Request to accept a pending incoming share
#[derive(Serialize, Deserialize)]
#[typeshare]
pub struct AcceptShareRequest {
    pub placement_path: String,
}

/// Incoming share pending acceptance
#[derive(Serialize, Deserialize)]
#[typeshare]
pub struct IncomingShareResponse {
    pub id: String,
    pub sender_username: String,
    pub display_name: String,
    pub created_at: String,
}

/// Badge count for pending incoming shares
#[derive(Serialize, Deserialize)]
#[typeshare]
pub struct ShareCountResponse {
    #[typeshare(serialized_as = "number")]
    pub count: i64,
}

/// Sharing details for a file
#[derive(Serialize, Deserialize)]
#[typeshare]
pub struct ShareDetailResponse {
    pub users: Vec<ShareParticipant>,
}

/// Participant in a shared file
#[derive(Serialize, Deserialize)]
#[typeshare]
pub struct ShareParticipant {
    pub username: String,
    #[typeshare(serialized_as = "number")]
    pub user_id: i32,
    pub status: String,
}
