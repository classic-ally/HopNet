import { describe, expect, it } from 'vitest';
import { browseUrlFor, folderFromBrowseUrl, paneForPath } from './router.svelte';

describe('paneForPath', () => {
    // Should: resolve an exact route to its pane
    it('resolves exact routes', () => {
        expect(paneForPath('/browse')).toBeTruthy();
        expect(paneForPath('/recent')).toBeTruthy();
    });

    // Impact: this prefix match is the whole reason a folder URL survives a
    // reload — App rewrites any path this returns null for, so without it a
    // deep link bounced the user back to /recent.
    // Should: resolve a browse subpath to the browse pane
    it('resolves browse subpaths to the browse pane', () => {
        expect(paneForPath('/browse/photos/2026')).toBe(paneForPath('/browse'));
    });

    // Should not: treat a path that merely starts with the route name as a subpath
    // Should not: resolve an unknown path
    it('rejects near-misses', () => {
        expect(paneForPath('/browsefoo')).toBeNull();
        expect(paneForPath('/nope')).toBeNull();
    });
});

describe('browse URL round trip', () => {
    // Should: map the root folder to the bare route, with no trailing slash
    it('maps the root to the bare route', () => {
        expect(browseUrlFor('/')).toBe('/browse');
        expect(folderFromBrowseUrl('/browse')).toBe('/');
    });

    // Impact: a folder name may contain any character except the separator, so
    // encoding per segment is what keeps `/` meaning "next folder" while
    // everything else survives a reload or a pasted bookmark.
    // Should: round-trip spaces, reserved URL characters, a literal percent, and non-ASCII
    it.each([
        '/photos',
        '/photos/2026',
        '/2026 trip',
        '/a#b',
        '/a?b',
        '/100%',
        '/été',
        '/a/b/c/d/e',
        '/mixed Été #1/100%',
    ])('round-trips %s', (folder) => {
        expect(folderFromBrowseUrl(browseUrlFor(folder))).toBe(folder);
    });

    // Should: percent-encode a reserved character rather than emit it raw
    // Should not: encode the separators between segments
    it('encodes per segment', () => {
        expect(browseUrlFor('/a#b/c d')).toBe('/browse/a%23b/c%20d');
    });

    // Impact: a bookmark truncated mid-escape would otherwise throw a URIError
    // out of decodeURIComponent and blank the pane.
    // Should: fall back to the raw segment when it is not valid encoding
    it('survives malformed escapes', () => {
        expect(folderFromBrowseUrl('/browse/100%')).toBe('/100%');
        expect(folderFromBrowseUrl('/browse/a%zz')).toBe('/a%zz');
    });

    // Should: read any non-browse path as the root folder
    it('reads foreign paths as the root', () => {
        expect(folderFromBrowseUrl('/recent')).toBe('/');
        expect(folderFromBrowseUrl('/')).toBe('/');
    });

    // Impact: a hand-edited or hand-shared URL carries these, and the pane
    // canonicalises rather than fetching a path with an empty segment.
    // Should: collapse doubled and trailing separators
    it('normalises stray separators', () => {
        expect(folderFromBrowseUrl('/browse/photos//2026/')).toBe('/photos/2026');
    });
});
