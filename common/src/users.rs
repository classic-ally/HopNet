use serde::{Serialize, Deserialize};
use typeshare::typeshare;

/// Public user info returned by `GET /users/` (list endpoint, all peers on
/// mesh). Excludes personal/UI state so peers don't leak that info.
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

/// Self-only user info returned by `GET /users/me`. Superset of
/// `PublicUserInfo` plus personal UI state (onboarding flags). Wire shape
/// keeps `onboarding_flags` as raw `u32` for typeshare cleanliness;
/// backend uses `OnboardingFlags` newtype for type safety.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[typeshare]
pub struct SelfUserInfo {
    #[typeshare(serialized_as = "number")]
    pub user_id: i32,
    pub username: String,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub avatar: Option<String>,
    #[typeshare(serialized_as = "number")]
    pub onboarding_flags: u32,
}

/// One named onboarding step. Source of truth for the closed set of valid
/// flags — exposed to frontend via typeshare so callers can't fabricate
/// unknown values. Each variant maps to a single bit position in
/// `OnboardingFlags` via `bit()`.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[typeshare]
pub enum OnboardingFlag {
    /// User has been offered the import flow at least once. Suppresses
    /// auto-offer cross-device. Set on accept-or-decline.
    ImportOffered,
    /// Import reached terminal `Completed` status.
    ImportCompleted,
    /// User has either filled out their profile (name/avatar) or explicitly
    /// dismissed the prompt. Status is also derived from observed profile
    /// fields, so this bit is the "I've seen this" ack rather than a strict
    /// has-profile signal.
    ProfileCompleted,
}

impl OnboardingFlag {
    pub fn bit(self) -> OnboardingFlags {
        match self {
            OnboardingFlag::ImportOffered    => OnboardingFlags(1 << 0),
            OnboardingFlag::ImportCompleted  => OnboardingFlags(1 << 1),
            OnboardingFlag::ProfileCompleted => OnboardingFlags(1 << 2),
        }
    }
}

/// Bitfield of onboarding-progress flags stored in `users.onboarding_flags`.
/// Serde-transparent over `u32` so wire/DB representation stays compact.
/// ToSql/FromSql in `common/src/db_impl.rs`. Construct via `OnboardingFlag::bit`
/// or `OnboardingFlags::from_iter` to keep the closed-set guarantee.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[serde(transparent)]
pub struct OnboardingFlags(pub u32);

impl OnboardingFlags {
    pub const NONE: Self = Self(0);

    pub const IMPORT_OFFERED:    Self = Self(1 << 0);
    pub const IMPORT_COMPLETED:  Self = Self(1 << 1);
    pub const PROFILE_COMPLETED: Self = Self(1 << 2);

    pub fn contains(self, other: Self) -> bool {
        (self.0 & other.0) == other.0 && other.0 != 0
    }

    pub fn insert(&mut self, other: Self) { self.0 |= other.0; }
    pub fn remove(&mut self, other: Self) { self.0 &= !other.0; }

    pub fn raw(self) -> u32 { self.0 }
}

impl std::ops::BitOr for OnboardingFlags {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self { Self(self.0 | rhs.0) }
}

impl std::ops::BitOrAssign for OnboardingFlags {
    fn bitor_assign(&mut self, rhs: Self) { self.0 |= rhs.0; }
}

impl FromIterator<OnboardingFlag> for OnboardingFlags {
    fn from_iter<I: IntoIterator<Item = OnboardingFlag>>(iter: I) -> Self {
        iter.into_iter().fold(Self::NONE, |acc, f| acc | f.bit())
    }
}
