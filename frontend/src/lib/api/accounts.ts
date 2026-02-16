import { API_BASE_URL, authenticatedFetch } from '../stores';
import type { UserInfo } from './shares';

export type { UserInfo };

export async function fetchAccounts(): Promise<UserInfo[]> {
    const response = await authenticatedFetch(`${API_BASE_URL}/users`);
    if (!response.ok) throw new Error(`Failed to fetch users: ${response.status}`);
    const users = await response.json();
    return users.map((u: any) => ({ user_id: u.user_id, username: u.username }));
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
