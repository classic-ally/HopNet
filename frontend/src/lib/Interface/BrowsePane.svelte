<script lang="ts">
    import { TableHandler, ThSort, ThFilter, Th, Datatable } from '@vincjo/datatables'
    import { tokenStore, API_BASE_URL, currentPathStore, refreshTriggerStore } from '../stores'
    import { onMount } from 'svelte'
    import type { FileItem } from '../types'
    import { InodeType } from '../types'
    import { formatFileSize, getFileIcon, formatDateForContainer, getFileName } from '../utils/formatters'
    import { tableColumns, fileBrowserColumns } from '../utils/tableColumns'
    import FilePreview from './FilePreview.svelte'

    let files: FileItem[] = []
    let loading = true
    let error = ''
    let currentPath = '/'
    let pathHistory: string[] = ['/'] // Track navigation history
    let showPreview = false
    let previewFile: FileItem | null = null
    let previewFileIndex = 0
    let fileOnlyList: FileItem[] = []
    let showSearchBar = false

    // Subscribe to current path store
    $: currentPath = $currentPathStore

    // Auto-adjust pagination based on directory size
    $: shouldPaginate = files.length > 300
    $: rowsPerPage = shouldPaginate ? 50 : files.length || 1

    // Update table when pagination setting changes
    $: if (table && rowsPerPage) {
        table.rowsPerPage = rowsPerPage
        table.setPage(1)
    }

    const table = new TableHandler(files, {
        rowsPerPage: rowsPerPage,
        selectBy: 'path',
    })
    const search = table.createSearch()



    async function fetchFiles(path: string = '/') {
        try {
            loading = true
            error = ''
            
            const token = $tokenStore
            if (!token) {
                error = 'No authentication token found'
                return
            }

            const url = new URL(`${API_BASE_URL}/files`)
            url.searchParams.append('path', path)

            const response = await fetch(url.toString(), {
                method: 'GET',
                headers: {
                    'Authorization': `Bearer ${token}`,
                    'Content-Type': 'application/json',
                },
            })

            if (response.ok) {
                const data = await response.json()
                files = data
                table.setRows(files)
                currentPath = path
                currentPathStore.set(path)
            } else {
                error = `Failed to fetch files: ${response.status} ${response.statusText}`
                console.error('Failed to fetch files:', response.status)
            }
        } catch (err) {
            error = `Network error: ${err instanceof Error ? err.message : 'Unknown error'}`
            console.error('Error fetching files:', err)
        } finally {
            loading = false
        }
    }

    async function handleItemClick(item: FileItem) {
        if (item.inode_type === InodeType.Folder) {
            // Navigate into the folder
            pathHistory.push(currentPath)
            // Clear search when navigating to a new folder
            search.value = ''
            search.set()
            fetchFiles(item.path)
        } else if (item.inode_type === InodeType.File) {
            // Find the index of this file in the file-only list (respects sorting/filtering)
            const fileRows = table.rows.filter(row => row.inode_type === InodeType.File)
            const fileIndex = fileRows.findIndex(row => row.path === item.path)
            if (fileIndex !== -1) {
                previewFileIndex = fileIndex
                previewFile = item
                showPreview = true
            }
        }
    }

    async function downloadFile(item: FileItem) {
        try {
            const token = $tokenStore
            if (!token) {
                error = 'No authentication token found'
                return
            }

            // Extract the path part after the first slash for the API call
            // Convert "/folder/file.txt" to "folder/file.txt"
            let apiPath = item.path
            if (apiPath.startsWith('/')) {
                apiPath = apiPath.substring(1)
            }
            
            const response = await fetch(`${API_BASE_URL}/files/${apiPath}`, {
                method: 'GET',
                headers: {
                    'Authorization': `Bearer ${token}`,
                },
            })

            if (response.ok) {
                // Get the filename from the response headers or extract from path
                const contentDisposition = response.headers.get('Content-Disposition')
                let filename = getFileName(item.path)
                
                // Try to extract filename from Content-Disposition header if present
                if (contentDisposition) {
                    const match = contentDisposition.match(/filename[^;=\n]*=((['"]).*?\2|[^;\n]*)/)
                    if (match && match[1]) {
                        filename = match[1].replace(/['"]/g, '')
                    }
                }

                // Create blob and trigger download
                const blob = await response.blob()
                const url = window.URL.createObjectURL(blob)
                const a = document.createElement('a')
                a.href = url
                a.download = filename
                document.body.appendChild(a)
                a.click()
                window.URL.revokeObjectURL(url)
                document.body.removeChild(a)
            } else {
                error = `Failed to download file: ${response.status} ${response.statusText}`
                console.error('Failed to download file:', response.status)
            }
        } catch (err) {
            error = `Download error: ${err instanceof Error ? err.message : 'Unknown error'}`
            console.error('Error downloading file:', err)
        }
    }

    function navigateBack() {
        if (pathHistory.length > 1) {
            pathHistory.pop() // Remove current path
            const previousPath = pathHistory[pathHistory.length - 1]
            // Clear search when navigating
            search.value = ''
            search.set()
            fetchFiles(previousPath)
        }
    }

    function navigateUp() {
        // Calculate parent directory from current path
        if (currentPath !== '/') {
            const segments = currentPath.split('/').filter(segment => segment.length > 0)
            segments.pop() // Remove the last segment
            const parentPath = segments.length > 0 ? '/' + segments.join('/') : '/'
            pathHistory.push(currentPath) // Add current to history
            // Clear search when navigating
            search.value = ''
            search.set()
            fetchFiles(parentPath)
        }
    }

    function navigateToRoot() {
        pathHistory = ['/']
        // Clear search when navigating
        search.value = ''
        search.set()
        fetchFiles('/')
    }

    function toggleSearchBar() {
        showSearchBar = !showSearchBar
        if (!showSearchBar) {
            // Clear search when closing search bar
            search.value = ''
            search.set()
        }
    }

    function navigateToPath(targetPath: string) {
        if (targetPath !== currentPath) {
            pathHistory.push(currentPath)
            // Clear search when navigating
            search.value = ''
            search.set()
            fetchFiles(targetPath)
        }
    }

    // Parse current path into clickable breadcrumb segments
    $: pathSegments = (() => {
        if (currentPath === '/') {
            return []
        }

        const segments = currentPath.split('/').filter(segment => segment.length > 0)
        const breadcrumbs = []

        let buildPath = ''
        for (const segment of segments) {
            buildPath += '/' + segment
            breadcrumbs.push({
                name: segment,
                path: buildPath
            })
        }

        return breadcrumbs
    })()

    function closePreview() {
        showPreview = false
        previewFile = null
        previewFileIndex = 0
    }

    function handlePreviewNavigation(newIndex: number) {
        // Get only files from the table rows (filter out folders)
        const fileRows = table.rows.filter(row => row.inode_type === InodeType.File)

        if (newIndex >= 0 && newIndex < fileRows.length && fileRows[newIndex] && !loading) {
            previewFileIndex = newIndex
            previewFile = fileRows[newIndex]
        }
    }

    // Get file-only list for preview navigation (use table.rows to respect sorting/filtering)
    $: fileOnlyList = table.rows ? table.rows.filter(row => row.inode_type === InodeType.File) : files.filter(row => row.inode_type === InodeType.File)

    onMount(() => {
        fetchFiles()
    })

    // Reactive statement to refetch when token changes
    $: if ($tokenStore) {
        fetchFiles(currentPath)
    }

    // Reactive statement to refetch when refresh is triggered
    $: if ($refreshTriggerStore > 0) {
        fetchFiles(currentPath)
    }
</script>

<div>
    <h3>Browse</h3>
    <p class="text-sm text-muted">{files.length} {files.length === 1 ? 'item' : 'items'} in this folder</p>
</div>

<div class="border-solid border-1 rounded-lg p-1 border-overlay1">
    {#if error}
        <div class="text-red p-2 mb-2 border border-red rounded">
            {error}
            <button
                class="ml-2 text-blue underline"
                onclick={() => fetchFiles(currentPath)}
            >
                Retry
            </button>
        </div>
    {/if}
    
    <!-- Navigation breadcrumb -->
    <div class="flex items-center justify-between gap-2 p-2 border-b border-overlay0">
        <div class="flex items-center gap-2">
            <button
                class="border-1 border-overlay1 text-muted border-solid rounded-md p-1 cursor-pointer bg-transparent hover:text-primary hover:border-mauve hover:bg-surface0 disabled:opacity-50 disabled:cursor-not-allowed"
                onclick={toggleSearchBar}
                aria-label={showSearchBar ? "Close search" : "Open search"}
                disabled={loading}
            >
                <div class="{showSearchBar ? 'i-carbon-close' : 'i-carbon-search'} w-4 h-4"></div>
            </button>
            {#if !showSearchBar}
                <button
                    class="border-1 border-overlay1 text-muted border-solid rounded-md p-1 cursor-pointer bg-transparent hover:text-primary hover:border-mauve hover:bg-surface0 disabled:opacity-50 disabled:cursor-not-allowed"
                    onclick={navigateToRoot}
                    aria-label="Navigate to root"
                    disabled={loading || currentPath === '/'}
                >
                    <div class="i-carbon-home w-4 h-4"></div>
                </button>
            {/if}
            {#if !showSearchBar}
                {#if currentPath !== '/'}
                    <button
                        class="border-1 border-overlay1 text-muted border-solid rounded-md p-1 cursor-pointer bg-transparent hover:text-primary hover:border-mauve hover:bg-surface0 disabled:opacity-50 disabled:cursor-not-allowed"
                        onclick={navigateUp}
                        aria-label="Navigate up a folder"
                        disabled={loading}
                    >
                        <div class="i-carbon-chevron-up w-4 h-4"></div>
                    </button>
                {/if}
                <span class="text-subtitle text-sm font-mono">{#if currentPath === '/'}<span class="text-primary">/</span>{:else}{#each pathSegments as segment, i}<span class="text-muted">/</span>{#if i === pathSegments.length - 1}<span class="text-primary">{segment.name}</span>{:else}<span class="text-blue hover:text-primary hover:underline cursor-pointer transition-colors" onclick={() => navigateToPath(segment.path)}>{segment.name}</span>{/if}{/each}{/if}</span>
            {:else}
                <!-- Search input when search bar is open -->
                <input
                    class="flex-1 bg-transparent text-primary border-overlay0 border-2 border-solid rounded-md p-1"
                    type="text"
                    placeholder="Search in {currentPath}"
                    bind:value={search.value}
                    oninput={() => search.set()}
                    disabled={loading}
                    autofocus
                >
            {/if}
        </div>

        <!-- Pagination selector on the right -->
        {#if shouldPaginate}
            <select
                class="p-1 border-overlay0 border-2 border-solid rounded-md bg-transparent text-primary text-sm"
                bind:value={table.rowsPerPage}
                onchange={() => table.setPage(1)}
                disabled={loading}
            >
                {#each [10, 25, 50, 100] as option}
                    <option value={option}>{option} items</option>
                {/each}
            </select>
        {/if}
    </div>
    
    {#if loading}
        <div class="text-muted p-4 text-center">
            Loading files...
        </div>
    {:else}
        <div class="table-wrapper">
        <Datatable {table}>
            <table use:tableColumns={fileBrowserColumns} class="browse-table">
                <thead>
                    <tr class="text-subtitle">
                        <ThSort {table} field="inode_type">Type</ThSort>
                        <ThSort {table} field="path">Name</ThSort>
                        <ThSort {table} field="file_size">Size</ThSort>
                        <ThSort {table} field="creation_date">Created</ThSort>
                        <ThSort {table} field="modification_date">Modified</ThSort>
                    </tr>
                </thead>
                <tbody>
                    {#each table.rows as row}
                        {@const createdFormats = formatDateForContainer(row.creation_date)}
                        {@const modFormats = row.modification_date ? formatDateForContainer(row.modification_date) : null}
                        <tr
                            class="text-left cursor-pointer hover:bg-surface0"
                            onclick={() => handleItemClick(row)}
                        >
                            <td class="w-8">
                                <div class="{getFileIcon(row.inode_type === InodeType.Folder ? 'Folder' : 'File', getFileName(row.path), 'list')} w-4 h-4 text-muted"></div>
                            </td>
                            <td>{getFileName(row.path)}</td>
                            <td class="text-sm text-muted text-right font-mono">{formatFileSize(row.file_size)}</td>
                            <td class="date-cell text-sm text-muted">
                                <span class="date-full">{createdFormats.full}</span>
                                <span class="date-time">{createdFormats.dateTime}</span>
                                <span class="date-only">{createdFormats.dateOnly}</span>
                            </td>
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
                            <td colspan="6" class="text-center text-muted p-4">
                                {currentPath === '/' ? 'No files or folders found' : 'This folder is empty'}
                            </td>
                        </tr>
                    {/each}
                </tbody>
            </table>
        </Datatable>
        </div>
    {/if}
</div>

<!-- Preview Modal -->
{#if showPreview && previewFile}
    <FilePreview
        file={previewFile}
        fileList={fileOnlyList}
        currentIndex={previewFileIndex}
        onClose={closePreview}
        onNavigate={handlePreviewNavigation}
    />
{/if}

<style>
    /* Scrollable table wrapper */
    .table-wrapper {
        overflow-x: auto;
        overflow-y: visible;
    }

    /* Fixed table layout - required for precise column control */
    .browse-table {
        table-layout: fixed;
        width: 100%;
        /* min-width is set dynamically by tableColumns action */
    }

    /* Handle text overflow in all columns */
    .browse-table td {
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
    }

    /* Hide text in type column header but keep sorting functionality */
    .browse-table :global(th:nth-child(1)) {
        text-indent: -9999px;
        overflow: hidden;
        position: relative;
        text-align: center;
    }

    /* Center the sorting arrows in the type column, compensating for their left padding */
    .browse-table :global(th:nth-child(1) > *) {
        position: relative;
        left: 50%;
        transform: translateX(calc(-50% - 4px)); /* Shift back by half their padding */
        padding-left: 0 !important; /* Remove the left padding entirely */
    }

    /* Responsive padding for table cells - use !important to override Datatable defaults */
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
        background-color: #313244 !important; /* surface0 */
    }

    :global(footer) {
        border-top: none !important;
    }

    /* Footer text */
    :global(aside) {
        color: #bac2de !important; /* subtitle */
    }

    /* Responsive date formatting */
    :global(.date-cell .date-only),
    :global(.date-cell .date-time) {
        display: none;
    }
    :global(.date-cell .date-full) {
        display: inline;
    }

    /* Compact: show date+time, hide full timestamp */
    :global(.date-compact .date-cell .date-full) {
        display: none;
    }
    :global(.date-compact .date-cell .date-time) {
        display: inline;
    }

    /* Mini: show date only */
    :global(.date-mini .date-cell .date-full),
    :global(.date-mini .date-cell .date-time) {
        display: none;
    }
    :global(.date-mini .date-cell .date-only) {
        display: inline;
    }
    
    :global(td) {
        border: 1px solid #313244 !important; /* surface0 - very subtle borders */
    }

    :global(th) {
        border-bottom: 1px solid #313244 !important; /* surface0 - header separator */
    }

    /* Make folder rows more obviously clickable */
    tbody tr:has(.i-carbon-folder) {
        cursor: pointer;
    }
    
    tbody tr:has(.i-carbon-folder):hover {
        background-color: #45475a !important; /* surface1 - slightly more emphasis for folders */
    }
</style>

