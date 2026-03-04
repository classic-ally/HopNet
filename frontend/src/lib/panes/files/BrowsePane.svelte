<script lang="ts">
    import { TableHandler, ThSort, ThFilter, Th, Datatable } from '@vincjo/datatables'
    import { tokenStore, API_BASE_URL, currentPathStore, refreshTriggerStore, authenticatedFetch, getCurrentUserId } from '../../stores'
    import { onMount } from 'svelte'
    import type { FileItem } from '../../types'
    import { InodeType } from '../../types'
    import { formatFileSize, getFileIcon, formatDateForContainer, getFileName } from '../../utils/formatters'
    import { tableColumns, fileBrowserColumns } from '../../utils/tableColumns'
    import FilePreview from '../../Interface/FilePreview.svelte'
    import Toolbar from '../../primitives/Toolbar.svelte'
    import type { ToolbarItem } from '../../primitives/Toolbar.svelte'
    import Upload from './Upload.svelte'
    import CreateFolder from './CreateFolder.svelte'
    import ConfirmDelete from './ConfirmDelete.svelte'
    import ShareFileModal from './ShareFileModal.svelte'
    import ShareDetailsModal from './ShareDetailsModal.svelte'
    import { fetchUsers, shareFile, fetchShareDetails, unshareFile, type UserInfo } from '../../api/shares'
    import type { ShareParticipant } from '../../types'

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

    // Toolbar-related state
    export let onToggleSidebar: () => void = () => {};
    let showUploadPopover = false
    let showCreateFolderPopover = false
    let showConfirmDelete = false
    let selectedFiles: FileItem[] = []
    let lastClickedIndex: number = -1
    let isShiftPressed = false
    let isDeleting = false
    let deleteError = ''
    let deleteSuccess = ''
    let showShareModal = false
    let shareUsers: UserInfo[] = []
    let shareLoading = false
    let shareError = ''
    let shareSuccess = ''
    let shareFileName = ''
    let shareInodeId = ''
    let showShareDetails = false
    let shareDetailsParticipants: ShareParticipant[] = []
    let shareDetailsLoading = false
    let shareDetailsFileName = ''
    let shareDetailsInodeId = ''

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

            const url = new URL(`${API_BASE_URL}/files`, window.location.origin)
            url.searchParams.append('path', path)

            const response = await authenticatedFetch(url.toString(), {
                method: 'GET',
                headers: {
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

    function handleItemClick(item: FileItem, event: MouseEvent) {
        const itemIndex = table.rows.findIndex(row => row.path === item.path)

        if (event.shiftKey && lastClickedIndex !== -1) {
            // Shift+click: range select
            event.preventDefault() // Prevent text selection
            const start = Math.min(lastClickedIndex, itemIndex)
            const end = Math.max(lastClickedIndex, itemIndex)
            const rangeItems = table.rows.slice(start, end + 1)

            // Add all items in range to selection if they're not already selected
            selectedFiles = [...new Set([...selectedFiles, ...rangeItems])]
        } else if (event.metaKey || event.ctrlKey) {
            // Cmd/Ctrl+click: toggle individual selection
            const isSelected = selectedFiles.some(f => f.path === item.path)
            if (isSelected) {
                selectedFiles = selectedFiles.filter(f => f.path !== item.path)
            } else {
                selectedFiles = [...selectedFiles, item]
            }
            lastClickedIndex = itemIndex
        } else {
            // Regular click: single select (replace selection)
            const isSelected = selectedFiles.length === 1 && selectedFiles[0].path === item.path
            if (isSelected) {
                // Deselect if clicking the only selected item
                selectedFiles = []
                lastClickedIndex = -1
            } else {
                selectedFiles = [item]
                lastClickedIndex = itemIndex
            }
        }
    }

    async function handleItemDoubleClick(item: FileItem) {
        if (item.inode_type === InodeType.Folder) {
            // Navigate into the folder
            pathHistory.push(currentPath)
            // Clear search when navigating to a new folder
            search.value = ''
            search.set()
            // Clear selection when navigating
            selectedFiles = []
            lastClickedIndex = -1
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
            // Extract the path part after the first slash for the API call
            // Convert "/folder/file.txt" to "folder/file.txt"
            let apiPath = item.path
            if (apiPath.startsWith('/')) {
                apiPath = apiPath.substring(1)
            }

            const response = await authenticatedFetch(`${API_BASE_URL}/files/${apiPath}`, {
                method: 'GET',
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
            // Clear selection when navigating
            selectedFiles = []
            lastClickedIndex = -1
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
            // Clear selection when navigating
            selectedFiles = []
            lastClickedIndex = -1
            fetchFiles(parentPath)
        }
    }

    function navigateToRoot() {
        pathHistory = ['/']
        // Clear search when navigating
        search.value = ''
        search.set()
        // Clear selection when navigating
        selectedFiles = []
        lastClickedIndex = -1
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
            // Clear selection when navigating
            selectedFiles = []
            lastClickedIndex = -1
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

        // Track shift key state for preventing text selection
        const handleKeyDown = (e: KeyboardEvent) => {
            if (e.key === 'Shift') {
                isShiftPressed = true
            }
        }
        const handleKeyUp = (e: KeyboardEvent) => {
            if (e.key === 'Shift') {
                isShiftPressed = false
            }
        }

        window.addEventListener('keydown', handleKeyDown)
        window.addEventListener('keyup', handleKeyUp)

        return () => {
            window.removeEventListener('keydown', handleKeyDown)
            window.removeEventListener('keyup', handleKeyUp)
        }
    })

    // Reactive statement to refetch when token changes
    $: if ($tokenStore) {
        fetchFiles(currentPath)
    }

    // Reactive statement to refetch when refresh is triggered
    $: if ($refreshTriggerStore > 0) {
        fetchFiles(currentPath)
    }

    // Toolbar handlers
    function handleUploadClick() {
        showUploadPopover = true;
    }

    function handleUploadClose() {
        showUploadPopover = false;
    }

    function handleFilesUploaded() {
        refreshTriggerStore.update(n => n + 1);
    }

    function handleCreateFolderClick() {
        showCreateFolderPopover = true;
    }

    function handleCreateFolderClose() {
        showCreateFolderPopover = false;
    }

    async function handleFolderCreated() {
        // Poll for the new folder to appear (consensus delay)
        const maxAttempts = 10;
        const pollInterval = 500;

        for (let attempt = 0; attempt < maxAttempts; attempt++) {
            await new Promise(resolve => setTimeout(resolve, pollInterval));
            await fetchFiles(currentPath);

            // If we've fetched at least once and have some files, break early
            // (The folder should appear after consensus processes)
            if (attempt > 0) {
                break; // One retry is usually enough
            }
        }
    }

    function handleViewModeClick() {
        console.log('View mode clicked');
    }

    function handleDownloadClick() {
        console.log('Download clicked');
        // TODO: Implement bulk download for selected files
    }

    function handleDeleteClick() {
        if (selectedFiles.length > 0) {
            showConfirmDelete = true;
        }
    }

    function handleDeleteCancel() {
        showConfirmDelete = false;
    }

    async function handleDeleteConfirm() {
        showConfirmDelete = false;
        isDeleting = true;
        deleteError = '';
        deleteSuccess = '';

        const filesToDelete = [...selectedFiles];
        let successCount = 0;
        let failCount = 0;

        try {
            // Delete each file
            const errors: string[] = [];
            for (const file of filesToDelete) {
                try {
                    const url = new URL(`${API_BASE_URL}/files`, window.location.origin);
                    url.searchParams.append('path', file.path);

                    const response = await authenticatedFetch(url.toString(), {
                        method: 'DELETE',
                    });

                    if (response.ok) {
                        successCount++;
                    } else {
                        failCount++;
                        const errorText = await response.text().catch(() => 'No error details');
                        const errorMsg = `${file.path}: ${response.status} ${response.statusText} - ${errorText}`;
                        console.error(`Failed to delete ${file.path}:`, response.status, errorText);
                        errors.push(errorMsg);
                    }
                } catch (err) {
                    failCount++;
                    const errorMsg = `${file.path}: ${err instanceof Error ? err.message : 'Unknown error'}`;
                    console.error(`Error deleting ${file.path}:`, err);
                    errors.push(errorMsg);
                }
            }

            // Show result message
            if (successCount > 0 && failCount === 0) {
                deleteSuccess = `Successfully deleted ${successCount} ${successCount === 1 ? 'item' : 'items'}`;
            } else if (successCount > 0 && failCount > 0) {
                deleteError = `Deleted ${successCount} items, but ${failCount} failed:\n${errors.join('\n')}`;
            } else {
                deleteError = `Failed to delete ${failCount} ${failCount === 1 ? 'item' : 'items'}:\n${errors.join('\n')}`;
            }

            // Clear selection and refresh file list
            selectedFiles = [];
            lastClickedIndex = -1;

            // Wait a moment for the user to see the message
            await new Promise(resolve => setTimeout(resolve, 1500));

            // Refresh the file list
            await fetchFiles(currentPath);

            // Clear messages after refresh
            deleteSuccess = '';
            deleteError = '';
        } catch (err) {
            deleteError = `Delete error: ${err instanceof Error ? err.message : 'Unknown error'}`;
            console.error('Error during deletion:', err);
        } finally {
            isDeleting = false;
        }
    }

    async function handleShareClick() {
        if (selectedFiles.length === 0) return;
        const file = selectedFiles[0];
        if (file.inode_type !== InodeType.File) return;

        shareFileName = getFileName(file.path);
        shareInodeId = file.id;
        shareError = '';
        shareSuccess = '';
        shareLoading = true;
        showShareModal = true;

        try {
            const allUsers = await fetchUsers();
            const myId = getCurrentUserId();
            shareUsers = myId != null ? allUsers.filter(u => u.user_id !== myId) : allUsers;
        } catch (err) {
            shareError = err instanceof Error ? err.message : 'Failed to load users';
        } finally {
            shareLoading = false;
        }
    }

    async function handleShareFile(username: string) {
        shareLoading = true;
        shareError = '';
        shareSuccess = '';
        try {
            const response = await shareFile(shareInodeId, username);
            if (response.ok) {
                shareSuccess = `Shared with ${username}`;
                setTimeout(() => {
                    showShareModal = false;
                    shareSuccess = '';
                    refreshTriggerStore.update(n => n + 1);
                }, 1500);
            } else if (response.status === 409) {
                shareError = 'Already shared with this user';
            } else {
                shareError = `Failed to share: ${response.status} ${response.statusText}`;
            }
        } catch (err) {
            shareError = err instanceof Error ? err.message : 'Failed to share file';
        } finally {
            shareLoading = false;
        }
    }

    function handleShareClose() {
        showShareModal = false;
        shareError = '';
        shareSuccess = '';
    }

    async function handleShareIconClick(file: FileItem, event: MouseEvent) {
        event.stopPropagation();
        shareDetailsFileName = getFileName(file.path);
        shareDetailsInodeId = file.id;
        shareDetailsLoading = true;
        shareDetailsParticipants = [];
        showShareDetails = true;

        try {
            shareDetailsParticipants = await fetchShareDetails(file.id);
        } catch (err) {
            console.error('Failed to load share details:', err);
        } finally {
            shareDetailsLoading = false;
        }
    }

    async function handleUnshare() {
        shareDetailsLoading = true;
        try {
            const response = await unshareFile(shareDetailsInodeId);
            if (response.ok) {
                showShareDetails = false;
                refreshTriggerStore.update(n => n + 1);
            }
        } catch (err) {
            console.error('Failed to unshare:', err);
        } finally {
            shareDetailsLoading = false;
        }
    }

    function handleShareDetailsClose() {
        showShareDetails = false;
    }

    // Toolbar configuration - stable references to avoid resize recalculation
    const leftElements: ToolbarItem[] = [
        {
            type: 'action' as const,
            icon: 'i-carbon-list',
            text: 'View Mode',
            onClick: handleViewModeClick,
            compactStage: 3, // First to compact (highest number)
            tooltip: 'Change view mode'
        }
    ];

    const centerElements: ToolbarItem[] = [
        {
            type: 'action' as const,
            icon: 'i-carbon-cloud-upload',
            text: 'Upload',
            onClick: handleUploadClick,
            compactStage: 2, // Second wave (lower number = more resistant)
            tooltip: 'Upload files to server'
        },
        {
            type: 'action' as const,
            icon: 'i-carbon-folder-add',
            text: 'New Folder',
            onClick: handleCreateFolderClick,
            compactStage: 3, // First to compact (highest number)
            tooltip: 'Create a new folder in the current directory'
        }
    ];

    const rightElements: ToolbarItem[] = [
        {
            type: 'action' as const,
            icon: 'i-carbon-cloud-download',
            text: 'Download',
            onClick: handleDownloadClick,
            compactStage: 2, // Second wave (lower number = more resistant)
            tooltip: 'Download selected files',
            disabled: false
        },
        {
            type: 'action' as const,
            icon: 'i-carbon-trash-can',
            text: 'Delete',
            onClick: handleDeleteClick,
            compactStage: 2, // Second wave (lower number = more resistant)
            tooltip: 'Delete selected files',
            disabled: false
        },
        {
            type: 'action' as const,
            icon: 'i-carbon-share',
            text: 'Share',
            onClick: handleShareClick,
            compactStage: 3, // First to compact (highest number)
            tooltip: 'Share selected files',
            disabled: false
        }
    ];

    // Update disabled state without creating new references
    $: {
        const hasSelection = selectedFiles.length > 0;
        rightElements[0].disabled = !hasSelection; // Download
        rightElements[1].disabled = !hasSelection; // Delete
        rightElements[2].disabled = !hasSelection; // Share
    }
</script>

<!-- Integrated Toolbar -->
<Toolbar
    {leftElements}
    {centerElements}
    {rightElements}
    {onToggleSidebar}
/>

<!-- Page Title -->
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

    {#if deleteSuccess}
        <div class="text-green p-2 mb-2 border border-green rounded bg-green/10">
            {deleteSuccess}
        </div>
    {/if}

    {#if deleteError}
        <div class="text-red p-2 mb-2 border border-red rounded bg-red/10 whitespace-pre-wrap font-mono text-xs max-h-40 overflow-y-auto">
            {deleteError}
        </div>
    {/if}

    {#if isDeleting}
        <div class="text-mauve p-2 mb-2 border border-mauve rounded bg-mauve/10">
            Deleting files...
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
                <tbody class="{isShiftPressed ? 'no-select' : ''}">
                    {#each table.rows as row}
                        {@const createdFormats = formatDateForContainer(row.creation_date)}
                        {@const modFormats = row.modification_date ? formatDateForContainer(row.modification_date) : null}
                        {@const isSelected = selectedFiles.some(f => f.path === row.path)}
                        <tr
                            class="text-left cursor-pointer hover:bg-surface0 {isSelected ? 'bg-mauve/20 selected-row' : ''}"
                            onclick={(e) => handleItemClick(row, e)}
                            ondblclick={() => handleItemDoubleClick(row)}
                        >
                            <td class="w-8">
                                <div class="{getFileIcon(row.inode_type === InodeType.Folder ? 'Folder' : 'File', getFileName(row.path), 'list')} w-4 h-4 text-muted"></div>
                            </td>
                            <td>{getFileName(row.path)}{#if row.shared_with_count && row.shared_with_count > 0}<button
                                        class="share-badge"
                                        title="Shared with {row.shared_with_count} — click for details"
                                        aria-label="View sharing details"
                                        onclick={(e) => handleShareIconClick(row, e)}
                                    ><span class="i-carbon-share w-3 h-3"></span><span class="share-count">{row.shared_with_count}</span></button>{/if}</td>
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

<!-- Upload Modal -->
<Upload
    isOpen={showUploadPopover}
    onClose={handleUploadClose}
    onUploadComplete={handleFilesUploaded}
/>

<!-- Create Folder Modal -->
<CreateFolder
    isOpen={showCreateFolderPopover}
    onClose={handleCreateFolderClose}
    onFolderCreated={handleFolderCreated}
/>

<!-- Confirm Delete Modal -->
<ConfirmDelete
    isOpen={showConfirmDelete}
    items={selectedFiles}
    onClose={handleDeleteCancel}
    onConfirm={handleDeleteConfirm}
/>

<!-- Share File Modal -->
<ShareFileModal
    isOpen={showShareModal}
    users={shareUsers}
    fileName={shareFileName}
    loading={shareLoading}
    error={shareError}
    success={shareSuccess}
    onShare={handleShareFile}
    onClose={handleShareClose}
/>

<!-- Share Details Modal -->
<ShareDetailsModal
    isOpen={showShareDetails}
    fileName={shareDetailsFileName}
    participants={shareDetailsParticipants}
    currentUserId={getCurrentUserId() ?? 0}
    loading={shareDetailsLoading}
    onUnshare={handleUnshare}
    onClose={handleShareDetailsClose}
/>

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

    /* Prevent text selection only when shift is pressed */
    tbody.no-select tr {
        user-select: none;
        -webkit-user-select: none;
        -moz-user-select: none;
        -ms-user-select: none;
    }

    tbody tr:hover {
        background-color: #313244 !important; /* surface0 */
    }

    /* Selected row styling */
    .selected-row {
        background-color: rgba(203, 166, 247, 0.2) !important; /* mauve/20 */
    }

    .selected-row:hover {
        background-color: rgba(203, 166, 247, 0.3) !important; /* mauve/30 - slightly stronger on hover */
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

    /* Inline share badge in name cell */
    .share-badge {
        display: inline-flex;
        align-items: center;
        gap: 2px;
        margin-left: 8px;
        color: #cba6f7; /* mauve */
        background: transparent;
        border: none;
        cursor: pointer;
        vertical-align: middle;
        padding: 0;
        font-size: 0.7rem;
    }

    .share-badge:hover {
        color: #cdd6f4; /* primary */
    }

    .share-count {
        font-size: 0.7rem;
        line-height: 1;
    }

    /* Make folder rows more obviously clickable */
    tbody tr:has(.i-carbon-folder) {
        cursor: pointer;
    }
    
    tbody tr:has(.i-carbon-folder):hover {
        background-color: #45475a !important; /* surface1 - slightly more emphasis for folders */
    }
</style>

