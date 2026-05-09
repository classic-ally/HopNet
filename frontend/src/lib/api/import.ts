import { API_BASE_URL, authenticatedFetch } from '../stores';
import type { ImportRecord, ImportPathCounts, ImportPathRow } from '../types';

/// GET /takeout/import — singleton import row for current user. 404 → null.
export async function fetchCurrentImport(): Promise<ImportRecord | null> {
    const response = await authenticatedFetch(`${API_BASE_URL}/takeout/import`);
    if (response.status === 404) return null;
    if (!response.ok) throw new Error(`Failed to fetch import: ${response.status}`);
    return response.json();
}

/// GET /takeout/import/status — owner-local aggregate counts. 404 on non-owner → null.
export async function fetchImportCounts(): Promise<ImportPathCounts | null> {
    const response = await authenticatedFetch(`${API_BASE_URL}/takeout/import/status`);
    if (response.status === 404) return null;
    if (!response.ok) throw new Error(`Failed to fetch import counts: ${response.status}`);
    return response.json();
}

/// GET /takeout/import/paths — per-path debug rows; used for failed-row breakdown on summary card.
export async function fetchImportPaths(): Promise<ImportPathRow[]> {
    const response = await authenticatedFetch(`${API_BASE_URL}/takeout/import/paths`);
    if (response.status === 404) return [];
    if (!response.ok) throw new Error(`Failed to fetch import paths: ${response.status}`);
    return response.json();
}

export type UploadImportResult =
    | { ok: true }
    | { ok: false; status: number; message: string };

/// POST /takeout/import — multipart upload. Surfaces 507 (quota) and 409 (active) explicitly.
export async function uploadImport(file: File): Promise<UploadImportResult> {
    const form = new FormData();
    form.append('archive', file, file.name);
    const response = await authenticatedFetch(`${API_BASE_URL}/takeout/import`, {
        method: 'POST',
        body: form,
    });
    if (response.ok) return { ok: true };
    let message = '';
    try { message = await response.text(); } catch (_) { /* ignore */ }
    return { ok: false, status: response.status, message };
}
