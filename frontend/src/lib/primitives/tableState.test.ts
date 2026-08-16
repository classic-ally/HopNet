import { describe, expect, it } from 'vitest';
import { TableState } from './tableState.svelte';

interface Row {
    id: number;
    name: string;
    size: string;
    status: string;
}

const ROWS: Row[] = [
    { id: 1, name: 'zebra.txt', size: '10', status: 'ready' },
    { id: 2, name: 'apple.txt', size: '9000', status: 'ready' },
    { id: 3, name: 'mango.txt', size: '200', status: 'expired' },
];

function state(rows: Row[] = ROWS, options = {}) {
    return new TableState<Row>(rows, {
        key: (r) => r.id,
        searchFields: (r) => [r.name, r.status],
        ...options,
    });
}

describe('search', () => {
    // Should: match rows on any of the configured search fields
    // Should: match case-insensitively
    // Should not: treat surrounding whitespace as part of the query
    it('filters on the configured fields', () => {
        const t = state();
        t.search = 'APPLE';
        expect(t.filtered.map((r) => r.id)).toEqual([2]);
        t.search = '  expired  ';
        expect(t.filtered.map((r) => r.id)).toEqual([3]);
    });

    // Impact: results shrink as you type, so a page number carried over from
    // the wider result set would show an empty slice of a non-empty result.
    // Should: return to the first page whenever the query changes
    it('resets to page one', () => {
        const t = state(ROWS, { rowsPerPage: 1 });
        t.setPage(3);
        expect(t.page).toBe(3);
        t.search = 'txt';
        expect(t.page).toBe(1);
    });

    // Should: report search as disabled when no search fields are configured
    // Should not: filter anything in that case
    it('is inert without configured fields', () => {
        const t = new TableState<Row>(ROWS, { key: (r) => r.id });
        expect(t.searchEnabled).toBe(false);
        t.search = 'apple';
        expect(t.filtered).toHaveLength(3);
    });
});

describe('sorting', () => {
    // Impact: file sizes arrive as numeric strings, and comparing them as text
    // put "9000" before "10" — the defect this accessor exists to fix.
    // Should: order by the accessor's value rather than the raw field
    // Should: compare numerically when the accessor yields numbers
    it('sorts through sortValue', () => {
        const t = state();
        t.toggleSort('size', (r) => parseInt(r.size));
        expect(t.sorted.map((r) => r.size)).toEqual(['10', '200', '9000']);
    });

    // Should: compare as text when the values are not numbers
    it('falls back to locale comparison', () => {
        const t = state();
        t.toggleSort('name');
        expect(t.sorted.map((r) => r.name)).toEqual(['apple.txt', 'mango.txt', 'zebra.txt']);
    });

    // Should: sort ascending on the first click of a field
    // Should: flip direction when the already-sorted field is clicked again
    // Should: restart ascending when a different field is clicked
    it('cycles direction per field', () => {
        const t = state();
        t.toggleSort('name');
        expect(t.sortDir).toBe(1);
        t.toggleSort('name');
        expect(t.sortDir).toBe(-1);
        expect(t.sorted.map((r) => r.name)).toEqual(['zebra.txt', 'mango.txt', 'apple.txt']);
        t.toggleSort('status');
        expect(t.sortDir).toBe(1);
    });

    // Should not: reuse the previous column's sort accessor
    it('clears sortValue when a field supplies none', () => {
        const t = state();
        t.toggleSort('size', (r) => parseInt(r.size));
        t.toggleSort('name');
        expect(t.sorted.map((r) => r.name)).toEqual(['apple.txt', 'mango.txt', 'zebra.txt']);
    });

    // Should: leave row order untouched while no sort field is set
    it('is identity before any sort', () => {
        const t = state();
        expect(t.sorted.map((r) => r.id)).toEqual([1, 2, 3]);
    });
});

describe('pagination', () => {
    // Impact: the previous table library rendered no page controls at all, so
    // rows past the first page were unreachable. These are the bounds the
    // footer's prev/next depend on.
    // Should: expose only the current page's slice
    // Should: count pages from the filtered total, not the unfiltered rows
    it('slices to the current page', () => {
        const t = state(ROWS, { rowsPerPage: 2 });
        expect(t.pageCount).toBe(2);
        expect(t.rows.map((r) => r.id)).toEqual([1, 2]);
        t.setPage(2);
        expect(t.rows.map((r) => r.id)).toEqual([3]);
    });

    // Should: clamp a page below the first onto the first
    // Should: clamp a page beyond the last onto the last
    it('clamps out-of-range pages', () => {
        const t = state(ROWS, { rowsPerPage: 2 });
        t.setPage(99);
        expect(t.page).toBe(2);
        t.setPage(-1);
        expect(t.page).toBe(1);
    });

    // Should: report a single page when pagination is disabled
    // Should: return every row in that case
    it('treats zero rows-per-page as unpaginated', () => {
        const t = state();
        expect(t.rowsPerPage).toBe(0);
        expect(t.pageCount).toBe(1);
        expect(t.rows).toHaveLength(3);
    });

    // Should: return to the first page when the page size changes
    it('resets the page on a size change', () => {
        const t = state(ROWS, { rowsPerPage: 1 });
        t.setPage(3);
        t.setRowsPerPage(2);
        expect(t.page).toBe(1);
        expect(t.rowsPerPage).toBe(2);
    });

    // Should: never report fewer than one page, even with no rows
    it('reports one page when empty', () => {
        const t = state([], { rowsPerPage: 10 });
        expect(t.pageCount).toBe(1);
    });
});

describe('selection', () => {
    // Should: identify rows by the configured key rather than object identity
    it('keys selection by the configured accessor', () => {
        const t = state();
        t.toggleSelect(ROWS[0]);
        expect(t.isSelected({ ...ROWS[0] })).toBe(true);
    });

    // Should: deselect a row that is already selected
    it('toggles', () => {
        const t = state();
        t.toggleSelect(ROWS[0]);
        t.toggleSelect(ROWS[0]);
        expect(t.isSelected(ROWS[0])).toBe(false);
    });

    // Impact: takeout gates selection on status; an ungated row could be
    // swept into a bulk Download or Delete it cannot serve.
    // Should not: select a row the selectable gate rejects
    // Should not: count ungated rows when deciding select-all
    it('honours the selectable gate', () => {
        const t = state(ROWS, { selectable: (r: Row) => r.status !== 'expired' });
        t.toggleSelect(ROWS[2]);
        expect(t.isSelected(ROWS[2])).toBe(false);
        t.toggleSelectAll();
        expect([...t.selected]).toEqual([1, 2]);
        expect(t.allSelected).toBe(true);
    });

    // Should: select every selectable row on the current page
    // Should: clear them when every one is already selected
    // Should not: reach rows on other pages
    it('selects all within the page', () => {
        const t = state(ROWS, { rowsPerPage: 2 });
        t.toggleSelectAll();
        expect([...t.selected]).toEqual([1, 2]);
        t.toggleSelectAll();
        expect([...t.selected]).toEqual([]);
    });

    // Should: report a partial selection as neither all nor none
    // Should not: report someSelected once everything is selected
    it('distinguishes partial from complete selection', () => {
        const t = state();
        t.toggleSelect(ROWS[0]);
        expect(t.someSelected).toBe(true);
        expect(t.allSelected).toBe(false);
        t.toggleSelect(ROWS[1]);
        t.toggleSelect(ROWS[2]);
        expect(t.allSelected).toBe(true);
        expect(t.someSelected).toBe(false);
    });

    // Should not: report select-all state for an empty page
    it('is not all-selected when there are no rows', () => {
        const t = state([]);
        expect(t.allSelected).toBe(false);
        expect(t.someSelected).toBe(false);
    });
});

describe('setRows', () => {
    // Impact: takeout polls every 5 seconds; a refresh that reset the view
    // would fight the user mid-interaction.
    // Should: keep the search, sort and page across a row replacement
    it('preserves the view', () => {
        const t = state(ROWS, { rowsPerPage: 2 });
        t.search = 'txt';
        t.toggleSort('name');
        t.setPage(2);
        t.setRows([...ROWS]);
        expect(t.search).toBe('txt');
        expect(t.sortField).toBe('name');
        expect(t.page).toBe(2);
    });

    // Impact: a selected key whose row has vanished is invisible in the UI but
    // still reaches a bulk action, which would act on a row nobody can see.
    // Should: drop selected keys whose rows are gone
    // Should: keep selected keys whose rows remain
    it('prunes selections for vanished rows', () => {
        const t = state();
        t.toggleSelectAll();
        expect([...t.selected]).toEqual([1, 2, 3]);
        t.setRows([ROWS[0]]);
        expect([...t.selected]).toEqual([1]);
    });

    // Should: drop a selected key whose row is still present but no longer selectable
    it('prunes selections that became ungated', () => {
        const t = state(ROWS, { selectable: (r: Row) => r.status !== 'expired' });
        t.toggleSelect(ROWS[0]);
        t.setRows([{ ...ROWS[0], status: 'expired' }]);
        expect([...t.selected]).toEqual([]);
    });

    // Should: pull the page back into range when the row count shrinks
    it('clamps the page when rows shrink', () => {
        const t = state(ROWS, { rowsPerPage: 1 });
        t.setPage(3);
        t.setRows([ROWS[0]]);
        expect(t.page).toBe(1);
    });
});
