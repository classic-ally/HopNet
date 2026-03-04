<script lang="ts">
    import { TableHandler, ThSort, Datatable } from '@vincjo/datatables'
    import { tokenStore, API_BASE_URL, refreshTriggerStore, authenticatedFetch } from '../../stores'
    import { onMount } from 'svelte'
    import type { FileItem } from '../../types'
    import { InodeType } from '../../types'
    import { formatFileSize, getFileIcon, formatDateForContainer, getFileName } from '../../utils/formatters'
    import { tableColumns, recentsBrowserColumns } from '../../utils/tableColumns'
    import FilePreview from '../../Interface/FilePreview.svelte'
    import Toolbar from '../../primitives/Toolbar.svelte'

    export let onToggleSidebar: () => void = () => {};

    let files: FileItem[] = []
    let loading = true
    let error = ''
    let showPreview = false
    let previewFile: FileItem | null = null
    let previewFileIndex = 0

    const table = new TableHandler(files, {
        rowsPerPage: 50,
    })

    function getParentPath(fullPath: string): string {
        const lastSlash = fullPath.lastIndexOf('/')
        if (lastSlash <= 0) return '/'
        return fullPath.substring(0, lastSlash)
    }

    async function fetchRecentFiles() {
        try {
            loading = true
            error = ''

            const url = new URL(`${API_BASE_URL}/files/recent`, window.location.origin)
            url.searchParams.append('limit', '50')

            const response = await authenticatedFetch(url.toString(), {
                method: 'GET',
                headers: { 'Content-Type': 'application/json' },
            })

            if (response.ok) {
                const data = await response.json()
                files = data
                table.setRows(files)
            } else {
                error = `Failed to fetch recent files: ${response.status} ${response.statusText}`
            }
        } catch (err) {
            error = `Network error: ${err instanceof Error ? err.message : 'Unknown error'}`
        } finally {
            loading = false
        }
    }

    function handleItemClick(item: FileItem) {
        const fileIndex = table.rows.findIndex(row => row.path === item.path)
        if (fileIndex !== -1) {
            previewFileIndex = fileIndex
            previewFile = item
            showPreview = true
        }
    }

    function closePreview() {
        showPreview = false
        previewFile = null
        previewFileIndex = 0
    }

    function handlePreviewNavigation(newIndex: number) {
        if (newIndex >= 0 && newIndex < table.rows.length && table.rows[newIndex] && !loading) {
            previewFileIndex = newIndex
            previewFile = table.rows[newIndex]
        }
    }

    onMount(() => {
        fetchRecentFiles()
    })

    $: if ($tokenStore) {
        fetchRecentFiles()
    }

    $: if ($refreshTriggerStore > 0) {
        fetchRecentFiles()
    }
</script>

<Toolbar
    leftElements={[]}
    centerElements={[]}
    rightElements={[]}
    {onToggleSidebar}
/>

<div>
    <h3>Recents</h3>
    <p class="text-sm text-muted">{files.length} recently modified {files.length === 1 ? 'file' : 'files'}</p>
</div>

<div class="border-solid border-1 rounded-lg p-1 border-overlay1">
    {#if error}
        <div class="text-red p-2 mb-2 border border-red rounded">
            {error}
            <button class="ml-2 text-blue underline" onclick={() => fetchRecentFiles()}>Retry</button>
        </div>
    {/if}

    {#if loading}
        <div class="text-muted p-4 text-center">Loading recent files...</div>
    {:else}
        <div class="table-wrapper">
        <Datatable {table}>
            <table use:tableColumns={recentsBrowserColumns} class="recents-table">
                <thead>
                    <tr class="text-subtitle">
                        <ThSort {table} field="inode_type">Type</ThSort>
                        <ThSort {table} field="path">Name</ThSort>
                        <ThSort {table} field="path">Location</ThSort>
                        <ThSort {table} field="file_size">Size</ThSort>
                        <ThSort {table} field="modification_date">Modified</ThSort>
                    </tr>
                </thead>
                <tbody>
                    {#each table.rows as row}
                        {@const modFormats = row.modification_date ? formatDateForContainer(row.modification_date) : null}
                        <tr
                            class="text-left cursor-pointer hover:bg-surface0"
                            ondblclick={() => handleItemClick(row)}
                        >
                            <td class="w-8">
                                <div class="{getFileIcon('File', getFileName(row.path), 'list')} w-4 h-4 text-muted"></div>
                            </td>
                            <td>{getFileName(row.path)}{#if row.shared_with_count && row.shared_with_count > 0}<span class="share-badge" title="Shared with {row.shared_with_count}"><span class="i-carbon-share w-3 h-3"></span><span class="share-count">{row.shared_with_count}</span></span>{/if}</td>
                            <td class="text-sm text-muted font-mono">{getParentPath(row.path)}</td>
                            <td class="text-sm text-muted text-right font-mono">{formatFileSize(row.file_size)}</td>
                            <td class="date-cell text-sm text-muted">
                                {#if modFormats}
                                    <span class="date-full">{modFormats.full}</span>
                                    <span class="date-time">{modFormats.dateTime}</span>
                                    <span class="date-only">{modFormats.dateOnly}</span>
                                {:else}
                                    -
                                {/if}
                            </td>
                        </tr>
                    {:else}
                        <tr>
                            <td colspan="5" class="text-center text-muted p-4">
                                No recent files to display
                            </td>
                        </tr>
                    {/each}
                </tbody>
            </table>
        </Datatable>
        </div>
    {/if}
</div>

{#if showPreview && previewFile}
    <FilePreview
        file={previewFile}
        fileList={files}
        currentIndex={previewFileIndex}
        onClose={closePreview}
        onNavigate={handlePreviewNavigation}
    />
{/if}

<style>
    .table-wrapper {
        overflow-x: auto;
        overflow-y: visible;
    }

    .recents-table {
        table-layout: fixed;
        width: 100%;
    }

    .recents-table td {
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
    }

    /* Hide text in type column header */
    .recents-table :global(th:nth-child(1)) {
        text-indent: -9999px;
        overflow: hidden;
        position: relative;
        text-align: center;
    }

    .recents-table :global(th:nth-child(1) > *) {
        position: relative;
        left: 50%;
        transform: translateX(calc(-50% - 4px));
        padding-left: 0 !important;
    }

    :global(.padding-normal) td,
    :global(.padding-normal) :global(th) {
        padding: 8px 12px !important;
    }

    :global(.padding-compact) td,
    :global(.padding-compact) :global(th) {
        padding: 6px 8px !important;
    }

    :global(.padding-mini) td,
    :global(.padding-mini) :global(th) {
        padding: 4px 4px !important;
    }

    tbody tr:hover {
        background-color: #313244 !important;
    }

    :global(footer) {
        border-top: none !important;
    }

    :global(aside) {
        color: #bac2de !important;
    }

    :global(.date-cell .date-only),
    :global(.date-cell .date-time) {
        display: none;
    }
    :global(.date-cell .date-full) {
        display: inline;
    }

    :global(.date-compact .date-cell .date-full) {
        display: none;
    }
    :global(.date-compact .date-cell .date-time) {
        display: inline;
    }

    :global(.date-mini .date-cell .date-full),
    :global(.date-mini .date-cell .date-time) {
        display: none;
    }
    :global(.date-mini .date-cell .date-only) {
        display: inline;
    }

    :global(td) {
        border: 1px solid #313244 !important;
    }

    :global(th) {
        border-bottom: 1px solid #313244 !important;
    }

    .share-badge {
        display: inline-flex;
        align-items: center;
        gap: 2px;
        margin-left: 8px;
        color: #cba6f7;
        background: transparent;
        border: none;
        vertical-align: middle;
        padding: 0;
        font-size: 0.7rem;
    }

    .share-count {
        font-size: 0.7rem;
        line-height: 1;
    }
</style>
