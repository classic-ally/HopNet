// View-model layer between HopNet's sidecar PhotoRow and the ported ingress
// viewer components. The components were written against the ingress server's
// typeshare DTOs; this module supplies the same shapes from PhotoRow so the
// components stay near-verbatim.

import type { PhotoPageItem, PhotoRow } from '../../api/photos';

/** What the grid/lightbox consume — same shape as the ingress PhotoSummary. */
export interface PhotoSummary {
    photo_id: string;
    library_id: string;
    /** Browse sort key (server-computed for page items; client-derived only in
     *  the deleted view, which never synthesizes cursors from it). */
    sort_ms: number;
    captured_at?: string;
    media_type: string; // 'image' | 'video'
    is_live_photo: boolean;
    pixel_width: number | null;
    pixel_height: number | null;
    orientation: number | null;
    duration_ms: number | null;
    favorite: boolean;
    media_subtypes: string[];
    /** (resource_type, data_block_id) pairs — content cache identity. */
    resources: [number, string][];
}

/** One month of the browse timeline for the histogram rail. */
export interface MonthBucket {
    month: string;
    count: number;
}

/** Trimmed PhotoDetail for the info panel. */
export interface PhotoDetailVM {
    photo_id: string;
    captured_at?: string;
    pixel_width: number | null;
    pixel_height: number | null;
    camera_make?: string;
    camera_model?: string;
    lat?: number;
    lon?: number;
    group_type?: number;
    resources: [number, string][];
}

export function toSummary(row: PhotoRow | PhotoPageItem, sortMs?: number): PhotoSummary {
    return {
        photo_id: row.photo_id,
        library_id: row.library_id ?? '',
        sort_ms:
            sortMs ??
            ('sort_ms' in row
                ? (row as PhotoPageItem).sort_ms
                : Date.parse(row.date_taken ?? row.upload_date ?? '') || 0),
        captured_at: row.date_taken ?? undefined,
        media_type: row.media_type === 1 ? 'video' : 'image',
        is_live_photo: row.media_type === 2,
        pixel_width: row.width,
        pixel_height: row.height,
        orientation: row.orientation,
        duration_ms: row.duration_ms,
        favorite: false, // favorites are Phase 4 — no sidecar column yet
        media_subtypes: row.media_type === 3 ? ['raw'] : [],
        resources: row.resources,
    };
}

export function toDetailVM(row: PhotoRow): PhotoDetailVM {
    return {
        photo_id: row.photo_id,
        captured_at: row.date_taken ?? undefined,
        pixel_width: row.width,
        pixel_height: row.height,
        camera_make: row.camera_make ?? undefined,
        camera_model: row.camera_model ?? undefined,
        lat: row.latitude ?? undefined,
        lon: row.longitude ?? undefined,
        group_type: row.group_type ?? undefined,
        resources: row.resources,
    };
}
