import { API_BASE_URL, authenticatedFetch } from '../stores';

// ── Ingress viewer types (adapted for sidecar) ──

export interface SidecarStatus {
    enabled: boolean;
    cursor: number | null;
    file_on_disk: boolean;
}

export interface PhotoRow {
    photo_id: string;
    library_id: string | null;
    date_taken: string | null;
    upload_date: string | null;
    media_type: number;     // 0=photo, 1=video, 2=live, 3=raw
    width: number | null;
    height: number | null;
    orientation: number | null;
    duration_ms: number | null;
    camera_make: string | null;
    camera_model: string | null;
    latitude: number | null;
    longitude: number | null;
    group_id: string | null;
    group_type: number | null;
    group_index: number | null;
    is_group_pick: number;
    deleted_at: string | null;
    expires_at: string | null;
    undecryptable: boolean;
    /** (resource_type, data_block_id) pairs; key content caches by the blob id. */
    resources: [number, string][];
}

/** One keyset browse-page row: PhotoRow plus the server-computed sort key. */
export type PhotoPageItem = PhotoRow & { sort_ms: number };

export interface PhotoPage {
    items: PhotoPageItem[];
    next_cursor?: string;
}

export interface MonthBucket {
    month: string;
    count: number;
}

export interface SyncBatch {
    changes: PhotoChange[];
    high_water_mark: number;
}

export interface PhotoChange {
    photo_id: string;
    changed_at_height: number;
    state: EncryptedPhotoState | null;
}

export interface EncryptedPhotoState {
    library_id: string | null;
    uploaded_by: number;
    encrypted_metadata: number[];
    metadata_nonce: number[];
    deleted_at: string | null;
    deleted_by: number | null;
    ephemeral_pubkey: number[] | null;
    encrypted_metadata_key: number[] | null;
    resources: [number, string][];
}

// ── API ──

export async function fetchSidecarStatus(): Promise<SidecarStatus> {
    const r = await authenticatedFetch(`${API_BASE_URL}/photos/sidecar/status`);
    if (!r.ok) throw new Error(`status: ${r.status}`);
    return r.json();
}

export async function enableSidecar(): Promise<void> {
    const r = await authenticatedFetch(`${API_BASE_URL}/photos/sidecar/enable`, { method: 'POST' });
    if (!r.ok) {
        const msg = await r.text().catch(() => String(r.status));
        throw new Error(`enable: ${msg}`);
    }
}

export async function disableSidecar(): Promise<void> {
    const r = await authenticatedFetch(`${API_BASE_URL}/photos/sidecar/disable`, { method: 'POST' });
    if (!r.ok) {
        const msg = await r.text().catch(() => String(r.status));
        throw new Error(`disable: ${msg}`);
    }
}

export async function reinitSidecar(): Promise<void> {
    const r = await authenticatedFetch(`${API_BASE_URL}/photos/sidecar/reinit`, { method: 'POST' });
    if (!r.ok) {
        const msg = await r.text().catch(() => String(r.status));
        throw new Error(`reinit: ${msg}`);
    }
}

export async function fetchGallery(limit = 100, offset = 0): Promise<PhotoRow[]> {
    const q = new URLSearchParams({ limit: String(limit), offset: String(offset) });
    const r = await authenticatedFetch(`${API_BASE_URL}/photos/gallery?${q}`);
    if (!r.ok) throw new Error(`gallery: ${r.status}`);
    return r.json();
}

export async function fetchRecentlyDeleted(limit = 100, offset = 0): Promise<PhotoRow[]> {
    const q = new URLSearchParams({ limit: String(limit), offset: String(offset) });
    const r = await authenticatedFetch(`${API_BASE_URL}/photos/recently-deleted?${q}`);
    if (!r.ok) throw new Error(`deleted: ${r.status}`);
    return r.json();
}

import { filterQuery, type Filter } from '../panes/photos/filters';

export async function fetchPhotoPage(params: {
    cursor?: string;
    dir?: 'older' | 'newer';
    limit?: number;
    filter: Filter;
}): Promise<PhotoPage> {
    const q = new URLSearchParams();
    filterQuery(params.filter, q);
    if (params.cursor) q.set('cursor', params.cursor);
    if (params.dir) q.set('dir', params.dir);
    q.set('limit', String(params.limit ?? 100));
    const r = await authenticatedFetch(`${API_BASE_URL}/photos/page?${q}`);
    if (!r.ok) throw new Error(`page: ${r.status}`);
    return r.json();
}

export async function fetchHistogram(filter: Filter): Promise<MonthBucket[]> {
    const q = new URLSearchParams();
    filterQuery(filter, q);
    const r = await authenticatedFetch(`${API_BASE_URL}/photos/histogram?${q}`);
    if (!r.ok) throw new Error(`histogram: ${r.status}`);
    return r.json();
}

export async function fetchPhoto(id: string): Promise<PhotoRow> {
    const r = await authenticatedFetch(`${API_BASE_URL}/photos/${encodeURIComponent(id)}`);
    if (!r.ok) throw new Error(`photo ${id}: ${r.status}`);
    return r.json();
}

export async function submitTransaction(txType: string, payload: number[]): Promise<void> {
    const r = await authenticatedFetch(`${API_BASE_URL}/photos/transaction`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ tx_type: txType, payload }),
    });
    if (!r.ok) {
        const msg = await r.text().catch(() => String(r.status));
        throw new Error(`tx ${txType}: ${msg}`);
    }
}

// ── Helpers ──

const MEDIA_LABELS: Record<number, string> = {
    0: 'photo',
    1: 'video',
    2: 'live',
    3: 'raw',
};

export function mediaLabel(t: number): string { return MEDIA_LABELS[t] ?? 'unknown'; }

export function mediaIcon(t: number): string {
    switch (t) {
        case 1: return 'i-carbon-video';
        case 2: return 'i-carbon-live-photo'; // approximate — no exact match in Carbon
        case 3: return 'i-carbon-raw';
        default: return 'i-carbon-camera';
    }
}
