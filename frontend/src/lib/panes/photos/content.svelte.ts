// Module-scope reactive content cache: resolves photo ids to object URLs of
// decrypted resource bytes fetched with the Bearer JWT (bare <img src> cannot
// authenticate against HopNet's Authorization-header auth).
//
// Entries are keyed by data_block_id identity: a content edit swaps the blob
// under the same (photo_id, resource_type) URL, so a cached entry whose blobId
// no longer matches the registered resources is refetched — exactly the
// invalidation the content route's `resources` payload field was added for.
//
// Module scope is deliberate: Interface's pane switcher destroys panes on
// navigation, and this cache (like importStatusStore) must survive. Object
// URLs are revoked on LRU eviction and flushed on the tokenStore null
// transition (covers every logout path, including authenticatedFetch's 401).

import { SvelteMap } from 'svelte/reactivity';
import { API_BASE_URL, authenticatedFetch, tokenStore } from '../../stores';

const RESOURCE_NAMES: Record<number, string> = {
    0: 'original',
    1: 'edited',
    2: 'paired_video',
    3: 'adjustment_data',
    4: 'raw_alternate',
    5: 'thumbnail_small',
    6: 'thumbnail_medium',
    7: 'edited_paired_video',
};

export type ContentClass = 'thumb' | 'display' | 'original';

/** Best-available resource type per content class, in preference order. */
const PREFS: Record<ContentClass, number[]> = {
    thumb: [5, 6, 1, 0],
    display: [6, 1, 0],
    original: [0],
};

const MIME_HINTS: Record<ContentClass, string> = {
    thumb: 'image/jpeg',
    display: 'image/jpeg',
    original: 'video/mp4', // only requested for video playback
};

/** Thumbs/displays are small; originals (videos) are full-buffer blobs. */
const MAX_IMAGE_ENTRIES = 300;
const MAX_ORIGINAL_ENTRIES = 4;

interface Entry {
    blobId: string;
    url: string;
}

// SvelteMap so cells re-render when their entry lands. Insertion order doubles
// as LRU order (delete+set on touch).
const entries = new SvelteMap<string, Entry>();
const registered = new Map<string, [number, string][]>();
const inFlight = new Set<string>();

/** The data container registers each row's resources so class → blob resolves. */
export function registerResources(photoId: string, resources: [number, string][]): void {
    registered.set(photoId, resources);
}

function resolve(photoId: string, cls: ContentClass): [number, string] | null {
    const resources = registered.get(photoId);
    if (!resources) return null;
    for (const wanted of PREFS[cls]) {
        const hit = resources.find(([type]) => type === wanted);
        if (hit) return hit;
    }
    return null;
}

function evict(cls: ContentClass) {
    const isOriginal = cls === 'original';
    const cap = isOriginal ? MAX_ORIGINAL_ENTRIES : MAX_IMAGE_ENTRIES;
    const keys = [...entries.keys()].filter(
        (k) => k.endsWith('/original') === isOriginal,
    );
    for (const key of keys.slice(0, Math.max(0, keys.length - cap))) {
        const entry = entries.get(key);
        if (entry) URL.revokeObjectURL(entry.url);
        entries.delete(key);
    }
}

async function fetchInto(key: string, photoId: string, type: number, blobId: string, cls: ContentClass) {
    inFlight.add(key);
    try {
        const r = await authenticatedFetch(
            `${API_BASE_URL}/photos/${encodeURIComponent(photoId)}/resource/${RESOURCE_NAMES[type]}`,
        );
        if (!r.ok) return;
        const raw = await r.blob();
        // The route serves octet-stream; re-wrap with a type hint so <img>/<video>
        // and object URLs behave (FilePreview precedent).
        const url = URL.createObjectURL(new Blob([raw], { type: MIME_HINTS[cls] }));
        const stale = entries.get(key);
        if (stale) URL.revokeObjectURL(stale.url);
        entries.set(key, { blobId, url });
        evict(cls);
    } catch {
        // Token gone or network error — cell keeps its placeholder; a later
        // urlFor call retries.
    } finally {
        inFlight.delete(key);
    }
}

/**
 * Reactive URL getter passed as thumbUrl/displayUrl/videoUrl props. Returns
 * `''` until the blob lands (callers guard empty src); reading it inside a
 * template subscribes the cell to exactly its own entry.
 */
export function urlFor(photoId: string, cls: ContentClass): string {
    const key = `${photoId}/${cls}`;
    const resolved = resolve(photoId, cls);
    if (!resolved) return '';
    const [type, blobId] = resolved;

    const cached = entries.get(key);
    if (cached && cached.blobId === blobId) {
        // LRU touch. delete+set inside a getter is safe with SvelteMap (no
        // infinite loop: value identity is unchanged for subscribers).
        entries.delete(key);
        entries.set(key, cached);
        return cached.url;
    }
    if (!inFlight.has(key)) {
        void fetchInto(key, photoId, type, blobId, cls);
    }
    return '';
}

/** Authenticated download via a synthetic anchor (bare <a href> cannot auth). */
export async function downloadResource(
    photoId: string,
    type: number,
    filename: string,
): Promise<void> {
    const r = await authenticatedFetch(
        `${API_BASE_URL}/photos/${encodeURIComponent(photoId)}/resource/${RESOURCE_NAMES[type]}`,
    );
    if (!r.ok) throw new Error(`download: ${r.status}`);
    const url = URL.createObjectURL(await r.blob());
    try {
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        a.remove();
    } finally {
        URL.revokeObjectURL(url);
    }
}

export function resourceName(type: number): string {
    return RESOURCE_NAMES[type] ?? `type_${type}`;
}

function flushAll() {
    for (const entry of entries.values()) URL.revokeObjectURL(entry.url);
    entries.clear();
    registered.clear();
    inFlight.clear();
}

// Logout (or 401-driven clearAuth) → revoke everything.
tokenStore.subscribe((token) => {
    if (!token) flushAll();
});
