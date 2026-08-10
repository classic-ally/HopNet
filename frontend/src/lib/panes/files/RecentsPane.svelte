<script lang="ts">
    import { tokenStore, API_BASE_URL, refreshTriggerStore, authenticatedFetch } from '../../stores'
    import { onMount } from 'svelte'
    import type { FileItem } from '../../types'
    import { formatFileSize, getFileIcon, getFileName } from '../../utils/formatters'
    import FilePreview from '../../Interface/FilePreview.svelte'
    import Toolbar from '../../primitives/Toolbar.svelte'
    import Table from '../../primitives/Table.svelte'
    import { TableState } from '../../primitives/tableState.svelte'
    import DateCell from '../../primitives/DateCell.svelte'
    import PaneHeader from '../../primitives/PaneHeader.svelte'

    let { onToggleSidebar = () => {} }: { onToggleSidebar?: () => void } = $props()

    let fileCount = $state(0)
    let loading = $state(true)
    let error = $state('')
    let showPreview = $state(false)
    let previewFile = $state<FileItem | null>(null)
    let previewFileIndex = $state(0)

    // The fetch is capped at 50, so no pagination — the footer just counts.
    const table = new TableState<FileItem>([])

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
                table.setRows(data)
                fileCount = data.length
            } else {
                error = `Failed to fetch recent files: ${response.status} ${response.statusText}`
            }
        } catch (err) {
            error = `Network error: ${err instanceof Error ? err.message : 'Unknown error'}`
        } finally {
            loading = false
        }
    }

    function handleItemDblClick(item: FileItem) {
        const fileIndex = table.rows.findIndex((row) => row.path === item.path)
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

    // Refetch when the token changes (login/logout).
    $effect(() => {
        if ($tokenStore) fetchRecentFiles()
    })

    $effect(() => {
        if ($refreshTriggerStore > 0) fetchRecentFiles()
    })
</script>

<Toolbar leftElements={[]} centerElements={[]} rightElements={[]} {onToggleSidebar} />

<PaneHeader title="Recents" subtitle={`${fileCount} recently modified ${fileCount === 1 ? 'file' : 'files'}`} />

{#snippet typeCell(row: FileItem)}
    <div class="{getFileIcon('File', getFileName(row.path), 'list')} w-4 h-4 text-muted"></div>
{/snippet}

{#snippet nameCell(row: FileItem)}
    {getFileName(row.path)}{#if row.shared_with_count && row.shared_with_count > 0}<span
            class="inline-flex items-center gap-0.5 ml-2 align-middle text-xs text-mauve"
            title="Shared with {row.shared_with_count}"
        ><span class="i-carbon-share w-3 h-3"></span>{row.shared_with_count}</span>{/if}
{/snippet}

{#snippet locationCell(row: FileItem)}
    <span class="text-sm text-muted font-mono">{getParentPath(row.path)}</span>
{/snippet}

{#snippet sizeCell(row: FileItem)}
    <span class="text-sm text-muted font-mono">{formatFileSize(row.file_size)}</span>
{/snippet}

{#snippet modifiedCell(row: FileItem)}
    <span class="text-sm text-muted"><DateCell date={row.modification_date} /></span>
{/snippet}

<Table
    state={table}
    selection="pointer"
    onRowDblClick={handleItemDblClick}
    {loading}
    loadingText="Loading recent files..."
    {error}
    onRetry={fetchRecentFiles}
    empty="No recent files to display"
    columns={[
        { id: 'type', sortField: 'inode_type', preset: 'icon', cell: typeCell },
        { id: 'name', header: 'Name', sortField: 'path', preset: 'name', cell: nameCell },
        { id: 'location', header: 'Location', sortField: 'path', preset: 'path', cell: locationCell },
        {
            id: 'size',
            header: 'Size',
            sortField: 'file_size',
            sortValue: (r) => parseInt(r.file_size ?? '0'),
            preset: 'size',
            align: 'right',
            cell: sizeCell
        },
        { id: 'modified', header: 'Modified', sortField: 'modification_date', preset: 'date', cell: modifiedCell }
    ]}
/>

{#if showPreview && previewFile}
    <!-- The list handed to the preview is the sorted view the index was
         computed against — the unsorted fetch order used to desync them. -->
    <FilePreview
        file={previewFile}
        fileList={table.rows}
        currentIndex={previewFileIndex}
        onClose={closePreview}
        onNavigate={handlePreviewNavigation}
    />
{/if}
