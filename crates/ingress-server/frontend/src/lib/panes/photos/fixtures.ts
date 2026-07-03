// Mock data for Storybook — lets the presentational views render with no
// backend. Shapes mirror the typeshare DTOs exactly.
import type { PhotoSummary, PhotoDetail } from '../../types';

function photo(over: Partial<PhotoSummary> & { photo_id: string }): PhotoSummary {
  return {
    library_id: 'vivid_birch',
    captured_at: undefined,
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
