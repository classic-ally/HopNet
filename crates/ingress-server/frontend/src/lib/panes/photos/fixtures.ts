// Mock data for Storybook — lets the presentational views render with no
// backend. Shapes mirror the typeshare DTOs exactly.
import type { LibrarySummary, MonthBucket, PhotoSummary, PhotoDetail } from '../../types';

export const mockLibraries: LibrarySummary[] = [
  {
    library_id: 'vivid_birch',
    display_name: 'Shared',
    shared: true,
    photo_count: 6912,
    video_count: 228,
  },
  {
    library_id: 'quiet_maple',
    display_name: 'Personal',
    shared: false,
    photo_count: 24102,
    video_count: 1893,
  },
];

function photo(over: Partial<PhotoSummary> & { photo_id: string }): PhotoSummary {
  const merged = {
    library_id: 'vivid_birch',
    captured_at: undefined as string | undefined,
    media_type: 'image',
    is_live_photo: false,
    pixel_width: 4032,
    pixel_height: 3024,
    orientation: 1,
    duration_ms: 0,
    favorite: false,
    media_subtypes: [],
    group_id: undefined,
    group_type: undefined,
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

export const mockDetail: PhotoDetail = {
  photo_id: 'a1',
  library_id: 'vivid_birch',
  cloud_id: 'ABC123==',
  captured_at: '2026-06-24T09:29:57Z',
  ingested_at: '2026-07-02T22:00:00Z',
  media_type: 'image',
  media_subtypes: ['portrait'],
  pixel_width: 4032,
  pixel_height: 3024,
  orientation: 1,
  duration_ms: 0,
  camera_make: 'Apple',
  camera_model: 'iPhone 16 Pro',
  lat: 51.7548,
  lon: -1.2544,
  favorite: true,
  group_id: undefined,
  group_type: undefined,
  group_index: 0,
  group_is_pick: undefined,
  resources: [
    { resource_type: 'original', content_hash: 'deadbeef', ext: 'heic', size_bytes: 2_411_724 },
    { resource_type: 'display', content_hash: 'cafef00d', ext: 'jpg', size_bytes: 812_003 },
  ],
};

// Placeholder thumbnails for Storybook (no /api backend). Deterministic per id.
export const placeholderThumb = (id: string) => `https://picsum.photos/seed/${id}/300/300`;
// Same seed, higher res — what the hover preview upgrades to.
export const placeholderDisplay = (id: string) => `https://picsum.photos/seed/${id}/900/900`;

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
