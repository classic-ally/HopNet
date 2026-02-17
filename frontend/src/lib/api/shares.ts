import { API_BASE_URL, authenticatedFetch } from '../stores';
import type { IncomingShareResponse, ShareParticipant, ShareDetailResponse } from '../types';

export interface UserInfo {
    user_id: number;
    username: string;
    first_name?: string;
    last_name?: string;
    avatar?: string; // base64-encoded WebP
}

export async function fetchUsers(): Promise<UserInfo[]> {
    const response = await authenticatedFetch(`${API_BASE_URL}/users`);
    if (!response.ok) throw new Error(`Failed to fetch users: ${response.status}`);
    const users = await response.json();
    return users;
}

export async function shareFile(inodeId: string, recipientUsername: string): Promise<Response> {
    return authenticatedFetch(`${API_BASE_URL}/shares`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ inode_id: inodeId, recipient_username: recipientUsername }),
    });
}

export async function fetchIncomingShares(): Promise<IncomingShareResponse[]> {
    const response = await authenticatedFetch(`${API_BASE_URL}/shares/incoming`);
    if (!response.ok) throw new Error(`Failed to fetch incoming shares: ${response.status}`);
    return response.json();
}

export async function fetchIncomingShareCount(): Promise<number> {
    const response = await authenticatedFetch(`${API_BASE_URL}/shares/incoming/count`);
    if (!response.ok) throw new Error(`Failed to fetch share count: ${response.status}`);
    const data = await response.json();
    return data.count;
}

export async function acceptShare(shareId: string, placementPath: string): Promise<Response> {
    return authenticatedFetch(`${API_BASE_URL}/shares/${shareId}/accept`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ placement_path: placementPath }),
    });
}

export async function declineShare(shareId: string): Promise<Response> {
    return authenticatedFetch(`${API_BASE_URL}/shares/incoming/${shareId}`, {
        method: 'DELETE',
    });
}

export async function fetchShareDetails(inodeId: string): Promise<ShareParticipant[]> {
    const response = await authenticatedFetch(`${API_BASE_URL}/shares/file/${inodeId}`);
    if (!response.ok) throw new Error(`Failed to fetch share details: ${response.status}`);
    const data: ShareDetailResponse = await response.json();
    return data.users;
}

export async function unshareFile(inodeId: string): Promise<Response> {
    return authenticatedFetch(`${API_BASE_URL}/shares/file/${inodeId}`, {
        method: 'DELETE',
    });
}
