use super::types::UpdateUserOnboardingPayload;
use crate::AppState;
use hopnet_common::OnboardingFlags;

/// Submit a `update_user_onboarding` consensus transaction. Used by the
/// `PUT /users/me/onboarding` route and by import-completion side effects.
/// Stringly-typed error keeps both call sites generic — they map as needed.
pub async fn submit_onboarding_update(
    state: &AppState,
    user_id: i32,
    set_flags: OnboardingFlags,
    clear_flags: OnboardingFlags,
) -> Result<(), String> {
    let payload = UpdateUserOnboardingPayload {
        user_id,
        set_flags,
        clear_flags,
    };
    let encoded = bincode::serde::encode_to_vec(&payload, bincode::config::standard())
        .map_err(|e| format!("encode: {:?}", e))?;
    let txn = crate::consensus::dispatch::create_signed_user_transaction(
        state,
        "update_user_onboarding".to_string(),
        encoded,
        user_id,
    )
    .await
    .map_err(|e| format!("sign: {:?}", e))?;
    state
        .consensus_queue
        .submit(txn)
        .await
        .map_err(|e| format!("submit: {:?}", e))?;
    Ok(())
}
