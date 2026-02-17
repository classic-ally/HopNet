use serde::{Serialize, Deserialize};
use typeshare::typeshare;

/// Public user info returned by API — no key material exposed
#[derive(Serialize, Deserialize, Debug, Clone)]
#[typeshare]
pub struct PublicUserInfo {
    #[typeshare(serialized_as = "number")]
    pub user_id: i32,
    pub username: String,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub avatar: Option<String>, // base64-encoded JPEG
}
