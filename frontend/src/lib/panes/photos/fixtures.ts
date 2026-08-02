// Mock data for Storybook — lets the presentational views render with no
// backend. Shapes mirror the viewmodel layer exactly.
import type { MonthBucket, PhotoDetailVM, PhotoSummary } from './viewmodel';

function photo(over: Partial<PhotoSummary> & { photo_id: string }): PhotoSummary {
  const merged = {
    library_id: 'quiet_maple',
    captured_at: undefined as string | undefined,
    media_type: 'image',
    is_live_photo: false,
    pixel_width: 4032,
    pixel_height: 3024,
    orientation: 1,
    duration_ms: 0,
    favorite: false,
    media_subtypes: [] as string[],
    resources: [] as [number, string][],
    sort_ms: undefined as number | undefined,
    ...over,
  };
  // Mirror the server: sort key falls back to an "ingest" epoch when undated.
  merged.sort_ms ??= merged.captured_at
    ? Date.parse(merged.captured_at)
    : Date.parse('2026-01-01T00:00:00Z');
  return merged as PhotoSummary;
}

// A spread across three capture days plus one undated, exercising every badge.
export const mockPhotos: PhotoSummary[] = [
  photo({ photo_id: 'a1', captured_at: '2026-06-24T09:29:57Z', favorite: true }),
  photo({ photo_id: 'a2', captured_at: '2026-06-24T08:10:00Z', is_live_photo: true }),
  photo({ photo_id: 'a3', captured_at: '2026-06-24T07:55:00Z', media_subtypes: ['raw_alternate'] }),
  photo({
    photo_id: 'b1',
    captured_at: '2026-06-21T18:00:00Z',
    media_type: 'video',
    duration_ms: 14000,
  }),
  photo({ photo_id: 'b2', captured_at: '2026-06-21T17:30:00Z' }),
  photo({ photo_id: 'b3', captured_at: '2026-06-21T17:00:00Z', favorite: true, is_live_photo: true }),
  photo({
    photo_id: 'b4',
    captured_at: '2026-06-21T16:00:00Z',
    media_type: 'video',
    duration_ms: 65000,
  }),
  photo({ photo_id: 'c1', captured_at: '2026-06-13T12:00:00Z' }),
  photo({ photo_id: 'c2', captured_at: '2026-06-13T11:00:00Z' }),
  photo({ photo_id: 'c3', captured_at: '2026-06-13T10:00:00Z' }),
  photo({ photo_id: 'd1', captured_at: undefined }),
];

export const mockDetail: PhotoDetailVM = {
  photo_id: 'a1',
  captured_at: '2026-06-24T09:29:57Z',
  pixel_width: 4032,
  pixel_height: 3024,
  camera_make: 'Apple',
  camera_model: 'iPhone 16 Pro',
  lat: 51.7548,
  lon: -1.2544,
  group_type: undefined,
  resources: [
    [0, '01980000-0000-7000-8000-00000000000a'],
    [6, '01980000-0000-7000-8000-00000000000b'],
  ],
};

// Placeholder thumbnails for Storybook. Inline SVG data URIs — stories run as
// headless-browser vitest tests (Nix sandbox, no network), so these must not
// touch the internet. Deterministic hue per id; explicit width/height so the
// hover preview's naturalWidth probe sees non-zero dimensions.
function svgDataUri(id: string, size: number): string {
  let hash = 0;
  for (const ch of id) hash = (hash * 31 + ch.charCodeAt(0)) % 360;
  const svg =
    `<svg xmlns="http://www.w3.org/2000/svg" width="${size}" height="${size}">` +
    `<rect width="100%" height="100%" fill="hsl(${hash},40%,35%)"/>` +
    `<text x="50%" y="50%" fill="#fff" font-size="${size / 8}" text-anchor="middle" dominant-baseline="middle">${id}</text>` +
    `</svg>`;
  return `data:image/svg+xml;utf8,${encodeURIComponent(svg)}`;
}
export const placeholderThumb = (id: string) => svgDataUri(id, 300);
// Same seed, higher res — what the hover preview upgrades to.
export const placeholderDisplay = (id: string) => svgDataUri(id, 900);

// ~4 years of months with seasonal-ish variation, deterministic (no RNG so
// stories are stable). Newest first, matching the API.
export const mockBuckets: MonthBucket[] = Array.from({ length: 48 }, (_, i) => {
  const d = new Date(Date.UTC(2026, 5 - i, 1)); // 2026-06 walking backwards
  const month = `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}`;
  const count = 40 + Math.round(260 * Math.abs(Math.sin(i * 1.7)) + (i % 5) * 30);
  return { month, count };
});

// A large flat page for scroll/scrub stories: one photo per mock bucket day.
export const mockManyPhotos: PhotoSummary[] = Array.from({ length: 120 }, (_, i) => {
  const day = String((i % 27) + 1).padStart(2, '0');
  const d = new Date(Date.UTC(2026, 5 - Math.floor(i / 10), 1));
  const month = String(d.getUTCMonth() + 1).padStart(2, '0');
  return photo({
    photo_id: `m${i}`,
    // Alternate libraries so fused-view stories can badge the shared ones.
    library_id: i % 3 === 0 ? 'vivid_birch' : 'quiet_maple',
    captured_at: `${d.getUTCFullYear()}-${month}-${day}T12:00:00Z`,
    favorite: i % 11 === 0,
    media_type: i % 7 === 0 ? 'video' : 'image',
    duration_ms: i % 7 === 0 ? 9000 + i * 500 : 0,
  });
});
