// Curated data for the website screenshot stories: a believable personal
// cloud rather than test-shaped fixtures. Photos are CC0 images served from
// public/website/photos (sources in CREDITS.json there).

import type { FileItem, SelfUserInfo, TakeoutRecord } from '../lib/types';
import { InodeType, TakeoutStatus } from '../lib/types';
import type { UserInfo } from '../lib/api/shares';
import type { MonthBucket, PhotoPageItem } from '../lib/api/photos';
import { notFound, serveStatic, type MockRoute } from './mockBackend';

export const ME: SelfUserInfo = {
    user_id: 1,
    username: 'robin',
    first_name: 'Robin',
    last_name: 'Okafor',
    // All onboarding bits set: no welcome modal, no setup banner.
    onboarding_flags: 7,
};

const PEOPLE: UserInfo[] = [
    { user_id: 1, username: 'robin', first_name: 'Robin', last_name: 'Okafor' },
    { user_id: 2, username: 'priya', first_name: 'Priya', last_name: 'Raman' },
    { user_id: 3, username: 'jordan', first_name: 'Jordan', last_name: 'Lee' },
    { user_id: 4, username: 'mei', first_name: 'Mei', last_name: 'Chen' },
    { user_id: 5, username: 'grandma.ruth', first_name: 'Ruth', last_name: 'Okafor' },
];

// ── Files ────────────────────────────────────────────────────────────────

function folder(name: string, modified: string): FileItem {
    return {
        id: `folder-${name}`,
        path: `/${name}`,
        inode_type: InodeType.Folder,
        file_size: '',
        creation_date: '2025-11-02T10:00:00Z',
        modification_date: modified,
    };
}

function file(name: string, bytes: number, modified: string, sharedWith = 0): FileItem {
    return {
        id: `file-${name}`,
        path: `/${name}`,
        inode_type: InodeType.File,
        file_size: String(bytes),
        creation_date: modified,
        modification_date: modified,
        ...(sharedWith > 0 ? { shared_with_count: sharedWith } : {}),
    };
}

export const SHARE_TARGET = 'Québec trip itinerary.pdf';

export const ROOT_FILES: FileItem[] = [
    folder('Documents', '2026-06-12T09:14:00Z'),
    folder('Family', '2026-06-08T18:40:00Z'),
    folder('Music', '2026-03-21T20:05:00Z'),
    folder('Projects', '2026-06-14T16:22:00Z'),
    folder('Recipes', '2026-05-30T12:10:00Z'),
    folder('Taxes 2025', '2026-04-27T21:33:00Z'),
    file(SHARE_TARGET, 2_413_772, '2026-06-02T08:51:00Z', 3),
    file('Household budget 2026.ods', 184_320, '2026-06-11T19:02:00Z', 1),
    file('Garden plan.png', 3_871_204, '2026-05-18T15:45:00Z'),
    file('Wedding speech (draft).docx', 48_906, '2026-06-13T22:17:00Z'),
    file('Apartment lease.pdf', 1_208_448, '2026-01-09T11:30:00Z'),
    file('Camping checklist.md', 3_190, '2026-05-21T07:58:00Z', 2),
];

// ── Photos ───────────────────────────────────────────────────────────────

// [file id, capture time, pixel width, pixel height] — six photos per day so
// each date header in the grid carries a full row, like a real library.
const PHOTOS: [string, string, number, number][] = [
    // A beach day
    ['24', '2026-06-14T20:41:00Z', 384, 256],
    ['23', '2026-06-14T19:58:00Z', 384, 219],
    ['22', '2026-06-14T18:30:00Z', 256, 384],
    ['14', '2026-06-14T16:20:00Z', 288, 384],
    ['13', '2026-06-14T15:02:00Z', 384, 288],
    ['12', '2026-06-14T09:12:00Z', 384, 256],
    // A city trip
    ['09', '2026-06-06T21:30:00Z', 384, 351],
    ['10', '2026-06-06T20:47:00Z', 384, 259],
    ['08', '2026-06-06T19:40:00Z', 384, 256],
    ['07', '2026-06-06T18:55:00Z', 384, 215],
    ['01', '2026-06-06T13:20:00Z', 384, 216],
    ['11', '2026-06-06T08:05:00Z', 384, 256],
    // A hike
    ['21', '2026-05-24T18:44:00Z', 384, 256],
    ['20', '2026-05-24T15:10:00Z', 384, 216],
    ['18', '2026-05-24T12:36:00Z', 384, 288],
    ['19', '2026-05-24T10:15:00Z', 384, 289],
    ['17', '2026-05-24T09:25:00Z', 288, 384],
    ['16', '2026-05-24T08:40:00Z', 288, 384],
    // A weekend at home
    ['06', '2026-04-26T17:03:00Z', 384, 256],
    ['05', '2026-04-26T14:18:00Z', 288, 384],
    ['15', '2026-04-26T11:32:00Z', 384, 288],
    ['04', '2026-04-26T09:50:00Z', 384, 216],
    ['03', '2026-04-26T08:40:00Z', 384, 261],
    ['02', '2026-04-26T08:05:00Z', 288, 384],
];

export const PHOTO_PAGE: PhotoPageItem[] = PHOTOS.map(([id, taken, width, height]) => ({
    photo_id: `photo-${id}`,
    library_id: null,
    date_taken: taken,
    upload_date: taken,
    media_type: 0,
    width,
    height,
    orientation: null,
    duration_ms: null,
    camera_make: null,
    camera_model: null,
    latitude: null,
    longitude: null,
    group_id: null,
    group_type: null,
    group_index: null,
    is_group_pick: 0,
    deleted_at: null,
    expires_at: null,
    undecryptable: false,
    resources: [[5, `blob-${id}`]],
    sort_ms: Date.parse(taken),
}));

const MONTHS: MonthBucket[] = ['2026-06', '2026-05', '2026-04'].map((month) => ({
    month,
    count: PHOTO_PAGE.filter((p) => p.date_taken?.startsWith(month)).length,
}));

// ── Takeout ──────────────────────────────────────────────────────────────

const TAKEOUTS: TakeoutRecord[] = [
    {
        id: '0197f3a2-5c1e-7d40-9b8e-3f2a6c1d9e01',
        user_id: 1,
        owner_node_id: 1,
        status: TakeoutStatus.Ready,
        created_at: '2026-06-14T09:30:00Z',
        expires_at: '2026-06-21T09:30:00Z',
        consensus_height: 48211,
    },
    {
        id: '0196a811-2b7f-7a13-8c55-90d4e7f2ab02',
        user_id: 1,
        owner_node_id: 2,
        status: TakeoutStatus.Expired,
        created_at: '2026-03-02T17:12:00Z',
        expires_at: '2026-03-09T17:12:00Z',
        consensus_height: 31877,
    },
];

// ── Routes ───────────────────────────────────────────────────────────────

/** What the shell itself polls, independent of the visible pane. */
export const shellRoutes: MockRoute[] = [
    { path: '/api/users/me', respond: () => ME },
    { path: '/api/shares/incoming/count', respond: () => ({ count: 2 }) },
    { path: '/api/takeout/import', respond: () => notFound() },
    { path: '/api/takeout/import/status', respond: () => notFound() },
];

export const filesRoutes: MockRoute[] = [
    { path: '/api/files', respond: () => ROOT_FILES },
    { path: '/api/users', respond: () => PEOPLE },
];

export const photosRoutes: MockRoute[] = [
    { path: '/api/photos/sidecar/status', respond: () => ({ enabled: true, cursor: 48211, file_on_disk: true }) },
    {
        path: '/api/photos/page',
        // One page holds the whole library; anything asking for more gets nothing.
        respond: (url) => (url.searchParams.has('cursor') ? { items: [] } : { items: PHOTO_PAGE }),
    },
    { path: '/api/photos/histogram', respond: () => MONTHS },
    {
        path: /^\/api\/photos\/photo-(\d+)\/resource\//,
        respond: (_url, _init, match) => serveStatic(`/website/photos/${match![1]}.jpg`),
    },
];

export const takeoutRoutes: MockRoute[] = [
    { path: '/api/takeout', respond: () => TAKEOUTS },
    { path: '/api/takeout/can-create', respond: () => ({ can_create: true }) },
];
