// Rendering-agnostic table state: rows in, sorted/filtered/paged rows out,
// with keyed selection. Replaces @vincjo/datatables' TableHandler; the Table
// primitive is one consumer, a grid view is another — that split is the point.
//
// Rows are $state.raw and replaced wholesale via setRows (every pane refetches
// and replaces; nothing mutates a row in place). In-place row mutation is
// therefore NOT reactive, by design — it keeps hundreds of file objects out of
// the deep proxy.

import { SvelteSet } from 'svelte/reactivity';
import type { Snippet } from 'svelte';
import type { ColumnPreset } from './tablePresets';

export interface TableStateOptions<Row> {
    /** Selection identity. Default is object identity, which does not survive
     *  a refetch — always provide a key when selection is used. */
    key?: (row: Row) => string | number;
    /** Values the text search matches against. Omitted = search disabled. */
    searchFields?: (row: Row) => unknown[];
    /** Rows per page; 0 (the default) disables pagination. */
    rowsPerPage?: number;
    /** Per-row selection gate (e.g. takeout status). Ungated by default. */
    selectable?: (row: Row) => boolean;
}

/** Column config consumed by Table.svelte. Lives here so panes can type
 *  their arrays without importing from a .svelte file.
 *
 *  IMPORTANT: a column's `cell` snippet is only in scope in the template, so
 *  build the columns array in a template expression ({@const} or an inline
 *  prop), never in <script> — a script-built array cannot see the snippets. */
export interface TableColumn<Row> {
    id: string;
    /** Header label; omit for a visually blank header (icon columns). */
    header?: string;
    /** Presence makes the header sortable on this field. */
    sortField?: string;
    /** Sort accessor when the field's raw value sorts wrong (e.g. numeric
     *  strings like file_size, which sorted lexicographically before). */
    sortValue?: (row: Row) => unknown;
    /** Sizing preset; defaults to a flexible text column. */
    preset?: ColumnPreset;
    align?: 'left' | 'right';
    /** Custom renderer; falls back to plain text of `field`. */
    cell?: Snippet<[Row]>;
    field?: keyof Row;
}

export class TableState<Row> {
    #key: (row: Row) => unknown;
    #searchFields: ((row: Row) => unknown[]) | null;
    #sortValue: ((row: Row) => unknown) | null = null;
    readonly selectable: (row: Row) => boolean;
    readonly searchEnabled: boolean;

    #all = $state.raw<Row[]>([]);
    #search = $state('');
    sortField = $state<string | null>(null);
    sortDir = $state<1 | -1>(1);
    page = $state(1);
    rowsPerPage = $state(0);
    readonly selected = new SvelteSet<unknown>();

    constructor(rows: Row[] = [], options: TableStateOptions<Row> = {}) {
        this.#key = options.key ?? ((row) => row);
        this.#searchFields = options.searchFields ?? null;
        this.selectable = options.selectable ?? (() => true);
        this.searchEnabled = options.searchFields !== undefined;
        this.rowsPerPage = options.rowsPerPage ?? 0;
        this.#all = rows;
    }

    /** Typing a search always lands you on page 1 — results shrink, and a
     *  stale page number would show an empty slice of a non-empty result. */
    get search(): string {
        return this.#search;
    }

    set search(value: string) {
        this.#search = value;
        this.page = 1;
    }

    readonly filtered: Row[] = $derived.by(() => {
        const q = this.#search.trim().toLowerCase();
        if (!q || !this.#searchFields) return this.#all;
        const fields = this.#searchFields;
        return this.#all.filter((row) =>
            fields(row).some((v) => String(v ?? '').toLowerCase().includes(q))
        );
    });

    readonly sorted: Row[] = $derived.by(() => {
        const field = this.sortField;
        if (!field) return this.filtered;
        const value = this.#sortValue ?? ((row: Row) => (row as Record<string, unknown>)[field]);
        const dir = this.sortDir;
        return [...this.filtered].sort((a, b) => {
            const va = value(a);
            const vb = value(b);
            if (typeof va === 'number' && typeof vb === 'number') return (va - vb) * dir;
            return String(va ?? '').localeCompare(String(vb ?? '')) * dir;
        });
    });

    /** The visible slice: the current page, or everything when unpaginated. */
    readonly rows: Row[] = $derived.by(() => {
        if (this.rowsPerPage <= 0) return this.sorted;
        const start = (this.page - 1) * this.rowsPerPage;
        return this.sorted.slice(start, start + this.rowsPerPage);
    });

    /** Rows matching the current search, across all pages. */
    readonly total: number = $derived(this.filtered.length);

    readonly pageCount: number = $derived(
        this.rowsPerPage > 0 ? Math.max(1, Math.ceil(this.total / this.rowsPerPage)) : 1
    );

    #selectableRows: Row[] = $derived(this.rows.filter((row) => this.selectable(row)));

    /** Select-all state, over the selectable rows of the current page. */
    readonly allSelected: boolean = $derived(
        this.#selectableRows.length > 0 && this.#selectableRows.every((row) => this.isSelected(row))
    );

    readonly someSelected: boolean = $derived(
        !this.allSelected && this.#selectableRows.some((row) => this.isSelected(row))
    );

    keyOf(row: Row): unknown {
        return this.#key(row);
    }

    isSelected(row: Row): boolean {
        return this.selected.has(this.#key(row));
    }

    toggleSelect(row: Row): void {
        if (!this.selectable(row)) return;
        const key = this.#key(row);
        if (this.selected.has(key)) this.selected.delete(key);
        else this.selected.add(key);
    }

    toggleSelectAll(): void {
        if (this.allSelected) {
            for (const row of this.#selectableRows) this.selected.delete(this.#key(row));
        } else {
            for (const row of this.#selectableRows) this.selected.add(this.#key(row));
        }
    }

    /** Replace the rows, preserving search/sort/page/selection — background
     *  refreshes (takeout polling) must not visibly reset the view. The page
     *  is clamped, and selected keys whose rows vanished or became
     *  unselectable are pruned so bulk actions cannot target them. */
    setRows(rows: Row[]): void {
        this.#all = rows;
        if (this.page > this.pageCount) this.page = this.pageCount;
        const valid = new Set(rows.filter((row) => this.selectable(row)).map((row) => this.#key(row)));
        for (const key of [...this.selected]) {
            if (!valid.has(key)) this.selected.delete(key);
        }
    }

    /** First click sorts ascending; clicking the sorted field flips it. */
    toggleSort(field: string, sortValue?: (row: Row) => unknown): void {
        if (this.sortField === field) {
            this.sortDir = this.sortDir === 1 ? -1 : 1;
        } else {
            this.sortField = field;
            this.sortDir = 1;
        }
        this.#sortValue = sortValue ?? null;
    }

    setPage(page: number): void {
        this.page = Math.min(Math.max(1, page), this.pageCount);
    }

    setRowsPerPage(n: number): void {
        this.rowsPerPage = n;
        this.page = 1;
    }
}
