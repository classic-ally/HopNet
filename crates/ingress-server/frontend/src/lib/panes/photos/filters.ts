// Browse filter model shared by the toolbar dropdown, the grid fetch, and the
// histogram fetch — one shape, one query encoding, so they can't disagree.

/** Tri-state: 'any' = no constraint, 'only' = must match, 'exclude' = must not. */
export type TriState = 'any' | 'only' | 'exclude';

/** What the FilterDropdown edits. */
export interface FilterState {
  photos: boolean; // images + live photos
  videos: boolean;
  live: TriState;
  raw: TriState;
  favorite: TriState;
}

export const defaultFilterState: FilterState = {
  photos: true,
  videos: true,
  live: 'any',
  raw: 'any',
  favorite: 'any',
};

/** Wire filter: tri-state flags as the API takes them (absent = any). */
export interface Filter {
  video?: boolean;
  live?: boolean;
  raw?: boolean;
  favorite?: boolean;
}

function tri(t: TriState): boolean | undefined {
  return t === 'any' ? undefined : t === 'only';
}

/**
 * Collapse the dropdown state to wire flags. Photos/videos checkmarks reduce
 * to the `video` axis: both checked = unconstrained, one checked = only/exclude
 * video. Neither checked is a client-side empty result (see `isEmpty`).
 */
export function toFilter(s: FilterState): Filter {
  return {
    video: s.photos === s.videos ? undefined : s.videos,
    live: tri(s.live),
    raw: tri(s.raw),
    favorite: tri(s.favorite),
  };
}

/** Both media checkboxes off — nothing can match; skip fetching entirely. */
export function isEmpty(s: FilterState): boolean {
  return !s.photos && !s.videos;
}

export function filterQuery(f: Filter, q: URLSearchParams): void {
  for (const key of ['video', 'live', 'raw', 'favorite'] as const) {
    if (f[key] !== undefined) q.set(key, String(f[key]));
  }
}

/** Stable key for {#key} remount on filter change. */
export function filterKey(f: Filter): string {
  return `${f.video}:${f.live}:${f.raw}:${f.favorite}`;
}

/**
 * Encode a keyset cursor (mirrors the server's base64url `sort_ms:photo_id`
 * token). The windowed grid synthesizes these from its edge items — and from
 * a month boundary with an empty photo_id, which sits between the months in
 * the total order: dir=older admits exactly the boundary month and older,
 * dir=newer exactly everything above it.
 */
export function edgeCursor(sortMs: number, photoId: string): string {
  return btoa(`${sortMs}:${photoId}`)
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/, '');
}

/**
 * First ms of the month AFTER `month` ("YYYY-MM") — the jump anchor for
 * starting a window at the top of `month`. Histogram buckets come from
 * strftime over sort_ms in UTC, so the boundary must be UTC too.
 */
export function monthBoundaryMs(month: string): number {
  const [y, m] = month.split('-').map(Number);
  // JS months are 0-based, `m` is 1-based: Date.UTC(y, m, 1) = next month's start.
  return Date.UTC(y, m, 1);
}
