<script lang="ts" generics="Row">
    import type { Snippet } from 'svelte'
    import Card from './Card.svelte'
    import Checkbox from './Checkbox.svelte'
    import Button from '../Button.svelte'
    import type { TableState, TableColumn } from './tableState.svelte'
    import { COLUMN_PRESETS, DEFAULT_SIZING, calculateColumnWidths } from './tablePresets'

    /**
     * The data-table shell: Card chrome around a toolbar row, a table OR grid
     * body over the same TableState, and a footer with a working pager (the
     * library this replaces never rendered page controls, so page 2 was
     * unreachable everywhere).
     *
     * Column `cell` snippets are only in scope in the template — build the
     * `columns` array in a template expression ({@const} or an inline prop),
     * never in <script>.
     *
     * Selection modes: 'checkbox' prepends a Checkbox column driven by the
     * state's keyed selection; 'pointer' just makes rows clickable and leaves
     * the policy to the pane via onRowClick/rowClass (Browse's ctrl/shift
     * ranges live there, not here).
     */
    let {
        state: table,
        columns,
        view = 'table',
        gridItem,
        selection = 'none',
        onRowClick,
        onRowDblClick,
        rowClass,
        toolbar = true,
        toolbarExtras,
        searchPlaceholder = 'Search',
        loading = false,
        loadingText = 'Loading…',
        error = '',
        onRetry,
        empty = 'No results',
        footer = true,
        rowsPerPageOptions = [10, 20, 50]
    }: {
        state: TableState<Row>,
        columns: TableColumn<Row>[],
        /** 'grid' renders gridItem tiles over the same state and footer. */
        view?: 'table' | 'grid',
        gridItem?: Snippet<[Row, { selected: boolean }]>,
        selection?: 'none' | 'checkbox' | 'pointer',
        onRowClick?: (row: Row, event: MouseEvent) => void,
        onRowDblClick?: (row: Row) => void,
        /** Extra classes per row/tile — selected highlight, folder emphasis. */
        rowClass?: (row: Row) => string,
        /**
         * true renders the built-in search + toolbarExtras row; false renders
         * no row at all; a Snippet puts pane-owned chrome (Browse's breadcrumb
         * and navigation) inside the row, so it sits within the tile rather
         * than floating above it.
         */
        toolbar?: boolean | Snippet,
        toolbarExtras?: Snippet,
        searchPlaceholder?: string,
        loading?: boolean,
        loadingText?: string,
        error?: string,
        /** Renders a Retry button in the error banner. */
        onRetry?: () => void,
        empty?: string | Snippet,
        footer?: boolean,
        rowsPerPageOptions?: number[]
    } = $props()

    let containerWidth: number | undefined = $state()

    // Sizing: every column resolves to a preset range; the checkbox column
    // occupies slot 0 when present. Widths become <col> percentages, so the
    // table always fills the container exactly and shrinks tier by tier.
    const sizings = $derived([
        ...(selection === 'checkbox' ? [COLUMN_PRESETS.checkbox] : []),
        ...columns.map((c) => (c.preset ? COLUMN_PRESETS[c.preset] : DEFAULT_SIZING))
    ])
    const widths = $derived(
        containerWidth ? calculateColumnWidths(containerWidth, sizings) : sizings.map((s) => s.max)
    )
    const totalWidth = $derived(widths.reduce((a, b) => a + b, 0))
    const minTableWidth = $derived(sizings.reduce((a, s) => a + s.min, 0))

    // Density: date columns trade detail for fit as they narrow; cell padding
    // steps down with the container. Thresholds ported from tableColumns.ts.
    const checkboxOffset = $derived(selection === 'checkbox' ? 1 : 0)
    const dateWidth = $derived.by(() => {
        const ws = columns.flatMap((c, i) => (c.preset === 'date' ? [widths[i + checkboxOffset]] : []))
        return ws.length > 0 ? Math.min(...ws) : Infinity
    })
    const densityClass = $derived(
        `${dateWidth < 170 ? 'date-mini' : dateWidth < 200 ? 'date-compact' : ''} ${
            containerWidth && containerWidth < 830
                ? 'padding-mini'
                : containerWidth && containerWidth < 900
                  ? 'padding-compact'
                  : 'padding-normal'
        }`
    )

    const interactive = $derived(selection === 'pointer' || onRowClick || onRowDblClick)

    // The current page size always appears in the select, even when the pane
    // chose a size outside its option list — a blank select reads as broken.
    const pageSizeOptions = $derived(
        [...new Set([...rowsPerPageOptions, table.rowsPerPage])].filter((n) => n > 0).sort((a, b) => a - b)
    )

    const rangeLabel = $derived.by(() => {
        if (table.rowsPerPage <= 0) return `${table.total} ${table.total === 1 ? 'item' : 'items'}`
        const start = (table.page - 1) * table.rowsPerPage
        return `${start + 1}–${Math.min(start + table.rowsPerPage, table.total)} of ${table.total}`
    })
</script>

<Card padding={false}>
    {#if typeof toolbar === 'function'}
        <div class="flex items-center gap-2 p-2 border-b border-surface1">
            {@render toolbar()}
        </div>
    {:else if toolbar && (table.searchEnabled || toolbarExtras)}
        <div class="flex items-center gap-2 p-2 border-b border-surface1">
            {#if table.searchEnabled}
                <input
                    class="flex-1 min-w-0 bg-transparent text-primary border-overlay0 border-2 rounded-md p-1"
                    type="text"
                    placeholder={searchPlaceholder}
                    bind:value={table.search}
                    disabled={loading}
                >
            {/if}
            {#if toolbarExtras}
                {@render toolbarExtras()}
            {/if}
        </div>
    {/if}

    {#if error}
        <div class="text-red p-2 m-2 border border-red rounded">
            {error}
            {#if onRetry}
                <button class="ml-2 text-blue underline bg-transparent border-none cursor-pointer" onclick={onRetry}>
                    Retry
                </button>
            {/if}
        </div>
    {:else if loading}
        <div class="text-muted p-4 text-center">{loadingText}</div>
    {:else if table.total === 0}
        <div class="text-muted p-4 text-center">
            {#if typeof empty === 'string'}{empty}{:else}{@render empty()}{/if}
        </div>
    {:else if view === 'grid'}
        <div class="grid gap-2 p-2" style="grid-template-columns: repeat(auto-fill, minmax(9rem, 1fr))">
            {#each table.rows as row (table.keyOf(row))}
                <button
                    type="button"
                    class="text-left text-primary rounded-lg p-2 bg-transparent border-none {interactive ? 'cursor-pointer' : ''} hover:bg-surface1 {rowClass?.(row) ?? ''}"
                    onclick={(e) => onRowClick?.(row, e)}
                    ondblclick={() => onRowDblClick?.(row)}
                >
                    {@render gridItem?.(row, { selected: table.isSelected(row) })}
                </button>
            {/each}
        </div>
    {:else}
        <div class="overflow-x-auto" bind:clientWidth={containerWidth}>
            <table
                class="hn-table {densityClass}"
                style="width: {containerWidth && totalWidth > containerWidth ? `${totalWidth}px` : '100%'}; min-width: {minTableWidth}px"
            >
                <colgroup>
                    {#each widths as width, i (i)}
                        <col style="width: {(width / totalWidth) * 100}%">
                    {/each}
                </colgroup>
                <thead>
                    <tr class="border-b border-surface1">
                        {#if selection === 'checkbox'}
                            <th class="text-left">
                                <Checkbox
                                    checked={table.allSelected}
                                    indeterminate={table.someSelected}
                                    onCheckedChange={() => table.toggleSelectAll()}
                                    ariaLabel="Select all rows"
                                />
                            </th>
                        {/if}
                        {#each columns as col (col.id)}
                            <th class="text-sm font-medium text-subtitle {col.align === 'right' ? 'text-right' : 'text-left'}">
                                {#if col.sortField}
                                    <button
                                        type="button"
                                        class="inline-flex items-center gap-1 bg-transparent border-none p-0 cursor-pointer text-subtitle hover:text-primary text-sm font-medium"
                                        onclick={() => table.toggleSort(col.sortField ?? '', col.sortValue)}
                                    >
                                        {#if col.header}<span>{col.header}</span>{/if}
                                        <span
                                            class="{table.sortField === col.sortField
                                                ? table.sortDir === 1
                                                    ? 'i-carbon-arrow-up'
                                                    : 'i-carbon-arrow-down'
                                                : 'i-carbon-arrows-vertical opacity-40'} w-3.5 h-3.5 flex-shrink-0"
                                            aria-hidden="true"
                                        ></span>
                                    </button>
                                {:else if col.header}
                                    {col.header}
                                {/if}
                            </th>
                        {/each}
                    </tr>
                </thead>
                <tbody>
                    {#each table.rows as row (table.keyOf(row))}
                        <tr
                            class="border-b border-surface1 hover:bg-surface1 {interactive ? 'cursor-pointer' : ''} {rowClass?.(row) ?? ''}"
                            onclick={onRowClick ? (e) => onRowClick(row, e) : undefined}
                            ondblclick={onRowDblClick ? () => onRowDblClick(row) : undefined}
                        >
                            {#if selection === 'checkbox'}
                                <td>
                                    <Checkbox
                                        checked={table.isSelected(row)}
                                        disabled={!table.selectable(row)}
                                        onCheckedChange={() => table.toggleSelect(row)}
                                        ariaLabel="Select row"
                                    />
                                </td>
                            {/if}
                            {#each columns as col (col.id)}
                                <td class={col.align === 'right' ? 'text-right' : ''}>
                                    {#if col.cell}
                                        {@render col.cell(row)}
                                    {:else if col.field !== undefined}
                                        {row[col.field]}
                                    {/if}
                                </td>
                            {/each}
                        </tr>
                    {/each}
                </tbody>
            </table>
        </div>
    {/if}

    {#if footer && !error && !loading && table.total > 0}
        <div class="flex items-center justify-between gap-2 p-2 border-t border-surface1 text-sm text-subtitle">
            <span>{rangeLabel}</span>
            <div class="flex items-center gap-2">
                {#if table.rowsPerPage > 0}
                    <select
                        class="p-1 border-overlay0 border-2 rounded-md bg-transparent text-primary text-sm"
                        bind:value={table.rowsPerPage}
                        onchange={() => table.setPage(1)}
                    >
                        {#each pageSizeOptions as option (option)}
                            <option value={option}>{option} rows</option>
                        {/each}
                    </select>
                {/if}
                {#if table.pageCount > 1}
                    <Button
                        variant="compact"
                        icon="i-carbon-chevron-left"
                        text="Previous page"
                        onClick={() => table.setPage(table.page - 1)}
                        disabled={table.page <= 1}
                    />
                    <span class="whitespace-nowrap">page {table.page} of {table.pageCount}</span>
                    <Button
                        variant="compact"
                        icon="i-carbon-chevron-right"
                        text="Next page"
                        onClick={() => table.setPage(table.page + 1)}
                        disabled={table.page >= table.pageCount}
                    />
                {/if}
            </div>
        </div>
    {/if}
</Card>

<style>
    .hn-table {
        table-layout: fixed;
        border-collapse: collapse;
    }

    .hn-table td {
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
    }

    /* Density: ported thresholds from utils/tableColumns.ts. */
    .hn-table.padding-normal th, .hn-table.padding-normal td { padding: 8px 12px; }
    .hn-table.padding-compact th, .hn-table.padding-compact td { padding: 6px 8px; }
    .hn-table.padding-mini th, .hn-table.padding-mini td { padding: 4px; }

    /* DateCell renders all three densities; the table's width class picks one.
       :global because the spans live in a child component. */
    .hn-table :global(.date-time),
    .hn-table :global(.date-only) { display: none; }

    .hn-table.date-compact :global(.date-full),
    .hn-table.date-compact :global(.date-only) { display: none; }
    .hn-table.date-compact :global(.date-time) { display: inline; }

    .hn-table.date-mini :global(.date-full),
    .hn-table.date-mini :global(.date-time) { display: none; }
    .hn-table.date-mini :global(.date-only) { display: inline; }
</style>
