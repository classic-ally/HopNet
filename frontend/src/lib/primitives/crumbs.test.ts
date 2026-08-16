import { describe, expect, it } from 'vitest';
import { crumbsForFolder } from './crumbs';

describe('crumbsForFolder', () => {
    // Should: represent the root as a single icon-only crumb
    it('describes the root', () => {
        const trail = crumbsForFolder('/');
        expect(trail).toHaveLength(1);
        expect(trail[0]).toMatchObject({ label: 'Home', value: '/', iconOnly: true });
    });

    // Should: emit one crumb per path segment, after the root
    // Should: give each crumb the absolute path it points at, not its own name
    it('accumulates a path per segment', () => {
        const trail = crumbsForFolder('/photos/2026/trip');
        expect(trail.map((c) => c.label)).toEqual(['Home', 'photos', '2026', 'trip']);
        expect(trail.map((c) => c.value)).toEqual(['/', '/photos', '/photos/2026', '/photos/2026/trip']);
    });

    // Impact: the browse pane needs real links so middle-click opens a tab,
    // while the upload and new-folder modals target a directory without
    // navigating — the same trail has to serve both.
    // Should not: set any href when no href builder is supplied
    // Should: set an href on every crumb when one is supplied
    it('makes links only when asked', () => {
        expect(crumbsForFolder('/a/b').every((c) => c.href === undefined)).toBe(true);
        const linked = crumbsForFolder('/a/b', (p) => `/browse${p}`);
        expect(linked.map((c) => c.href)).toEqual(['/browse/', '/browse/a', '/browse/a/b']);
    });

    // Impact: a trailing or doubled slash reaches this from a hand-edited URL.
    // Should not: emit an empty crumb for a repeated or trailing separator
    it('ignores empty segments', () => {
        expect(crumbsForFolder('//photos//2026/').map((c) => c.label))
            .toEqual(['Home', 'photos', '2026']);
    });

    // Should: treat an empty path as the root
    it('survives an empty path', () => {
        expect(crumbsForFolder('')).toHaveLength(1);
    });
});
