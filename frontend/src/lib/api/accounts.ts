import { API_BASE_URL, authenticatedFetch } from '../stores';
import type { UserInfo } from './shares';
import type { SelfUserInfo, OnboardingFlag } from '../types';

export type { UserInfo };

export async function fetchAccounts(): Promise<UserInfo[]> {
    const response = await authenticatedFetch(`${API_BASE_URL}/users`);
    if (!response.ok) throw new Error(`Failed to fetch users: ${response.status}`);
    return response.json();
}

export async function fetchCurrentUser(): Promise<SelfUserInfo> {
    const response = await authenticatedFetch(`${API_BASE_URL}/users/me`);
    if (!response.ok) throw new Error(`Failed to fetch current user: ${response.status}`);
    return response.json();
}

/// PUT /users/me/onboarding — flip the onboarding bitfield. `set` bits are
/// OR'd in, `clear` bits are AND-NOT'd. Idempotent. Replicated via consensus.
export async function setOnboardingFlags(set: OnboardingFlag[], clear: OnboardingFlag[]): Promise<void> {
    const response = await authenticatedFetch(`${API_BASE_URL}/users/me/onboarding`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ set, clear }),
    });
    if (!response.ok) throw new Error(`Failed to set onboarding flags: ${response.status}`);
}

export async function updateProfile(fields: { first_name?: string | null; last_name?: string | null }): Promise<Response> {
    return authenticatedFetch(`${API_BASE_URL}/users/me/profile`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(fields),
    });
}

export async function uploadAvatar(blob: Blob): Promise<Response> {
    const formData = new FormData();
    formData.append('avatar', blob);
    return authenticatedFetch(`${API_BASE_URL}/users/me/avatar`, {
        method: 'PUT',
        body: formData,
    });
}

export async function createAccount(username: string): Promise<{ passphrase: string }> {
    const response = await authenticatedFetch(`${API_BASE_URL}/users`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username }),
    });
    if (!response.ok) {
        const errorText = await response.text();
        throw new Error(errorText || `Failed to create account: ${response.status}`);
    }
    return response.json();
}
