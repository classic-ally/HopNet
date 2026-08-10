<script lang="ts">
    import { tokenStore, API_BASE_URL, currentPathStore, refreshTriggerStore, authenticatedFetch, getCurrentUserId, writesGatedStore, WRITES_GATED_TOOLTIP } from '../../stores'
    import { onMount, untrack } from 'svelte'
    import type { FileItem } from '../../types'
    import { InodeType } from '../../types'
    import { formatFileSize, getFileIcon, getFileName } from '../../utils/formatters'
    import FilePreview from '../../Interface/FilePreview.svelte'
    import Toolbar from '../../primitives/Toolbar.svelte'
    import type { ToolbarItem } from '../../primitives/Toolbar.svelte'
    import Table from '../../primitives/Table.svelte'
    import { TableState } from '../../primitives/tableState.svelte'
    import DateCell from '../../primitives/DateCell.svelte'
    import Upload from './Upload.svelte'
    import CreateFolder from './CreateFolder.svelte'
    import ConfirmDelete from './ConfirmDelete.svelte'
    import ShareFileModal from './ShareFileModal.svelte'
    import ShareDetailsModal from './ShareDetailsModal.svelte'
    import { fetchUsers, shareFile, fetchShareDetails, unshareFile, type UserInfo } from '../../api/shares'
    import type { ShareParticipant } from '../../types'
    import PaneHeader from '../../primitives/PaneHeader.svelte'

    let { onToggleSidebar = () => {} }: { onToggleSidebar?: () => void } = $props()

    let fileCount = $state(0)
    let loading = $state(true)
    let error = $state('')
    let pathHistory: string[] = ['/'] // Track navigation history
    let showPreview = $state(false)
    let previewFile = $state<FileItem | null>(null)
    let previewFileIndex = $state(0)
    let showSearchBar = $state(false)
    let viewMode = $state<'table' | 'grid'>('table')

    let showUploadPopover = $state(false)
    let showCreateFolderPopover = $state(false)
    let showConfirmDelete = $state(false)
    let selectedFiles = $state<FileItem[]>([])
    let lastClickedIndex = -1
    let isShiftPressed = $state(false)
    let isDeleting = $state(false)
    let deleteError = $state('')
    let deleteSuccess = $state('')
    let showShareModal = $state(false)
    let shareUsers = $state<UserInfo[]>([])
    let shareLoading = $state(false)
    let shareError = $state('')
    let shareSuccess = $state('')
    let shareFileName = $state('')
    let shareInodeId = $state('')
    let showShareDetails = $state(false)
    let shareDetailsParticipants = $state<ShareParticipant[]>([])
    let shareDetailsLoading = $state(false)
    let shareDetailsFileName = $state('')
    let shareDetailsInodeId = $state('')

    const currentPath = $derived($currentPathStore)

    // Pointer selection: policy lives here (single / ctrl-toggle / shift-range),
    // the Table only reports clicks and paints rowClass.
    const table = new TableState<FileItem>([], { key: (r) => r.path })

    // Big directories paginate; small ones show everything. The Table's footer
    // carries the page controls when they exist.
    $effect(() => {
        const paginated = fileCount > 300
        if (paginated && table.rowsPerPage === 0) table.setRowsPerPage(50)
        if (!paginated && table.rowsPerPage !== 0) table.setRowsPerPage(0)
    })

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
                table.setRows(data)
                fileCount = data.length
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

    function handleItemDoubleClick(item: FileItem) {
        if (item.inode_type === InodeType.Folder) {
            // Navigate into the folder
            pathHistory.push(currentPath)
            navigateReset()
            fetchFiles(item.path)
        } else if (item.inode_type === InodeType.File) {
            // Find the index of this file in the file-only list (respects sorting/filtering)
            const fileIndex = fileOnlyList.findIndex(row => row.path === item.path)
            if (fileIndex !== -1) {
                previewFileIndex = fileIndex
                previewFile = item
                showPreview = true
            }
        }
    }

    /** Clear search and selection — every navigation does both. */
    function navigateReset() {
        table.search = ''
        selectedFiles = []
        lastClickedIndex = -1
    }

    function navigateUp() {
        // Calculate parent directory from current path
        if (currentPath !== '/') {
            const segments = currentPath.split('/').filter(segment => segment.length > 0)
            segments.pop() // Remove the last segment
            const parentPath = segments.length > 0 ? '/' + segments.join('/') : '/'
            pathHistory.push(currentPath) // Add current to history
            navigateReset()
            fetchFiles(parentPath)
        }
    }

    function navigateToRoot() {
        pathHistory = ['/']
        navigateReset()
        fetchFiles('/')
    }

    function toggleSearchBar() {
        showSearchBar = !showSearchBar
        if (!showSearchBar) {
            // Clear search when closing search bar
            table.search = ''
        }
    }

    function navigateToPath(targetPath: string) {
        if (targetPath !== currentPath) {
            pathHistory.push(currentPath)
            navigateReset()
            fetchFiles(targetPath)
        }
    }

    // Parse current path into clickable breadcrumb segments
    const pathSegments = $derived.by(() => {
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
    })

    function closePreview() {
        showPreview = false
        previewFile = null
        previewFileIndex = 0
    }

    function handlePreviewNavigation(newIndex: number) {
        if (newIndex >= 0 && newIndex < fileOnlyList.length && fileOnlyList[newIndex] && !loading) {
            previewFileIndex = newIndex
            previewFile = fileOnlyList[newIndex]
        }
    }

    // File-only view of the sorted rows, for preview navigation.
    const fileOnlyList = $derived(table.rows.filter(row => row.inode_type === InodeType.File))

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

    // Refetch when the token changes (login/logout). The path is untracked:
    // navigation already fetches, so tracking it here would double-fetch.
    $effect(() => {
        if ($tokenStore) untrack(() => fetchFiles(currentPath))
    })

    // Refetch when a refresh is triggered (uploads, folder creation, shares).
    $effect(() => {
        if ($refreshTriggerStore > 0) untrack(() => fetchFiles(currentPath))
    })

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

    function handleDeleteClick() {
        if (selectedFiles.length > 0) {
            showConfirmDelete = true;
        }
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

    // Toolbar: rebuilt whenever selection or the writes gate changes. Write
    // affordances (Upload, New Folder, Delete, Share) are gated during an
    // import — the backend 409s these same routes, so the affordance is
    // disabled with a tooltip that says why.
    const hasSelection = $derived(selectedFiles.length > 0)
    const gated = $derived($writesGatedStore)

    const leftElements = $derived([
        {
            type: 'action' as const,
            icon: viewMode === 'table' ? 'i-carbon-grid' : 'i-carbon-list',
            text: viewMode === 'table' ? 'Grid View' : 'List View',
            onClick: () => (viewMode = viewMode === 'table' ? 'grid' : 'table'),
            compactStage: 3,
            tooltip: viewMode === 'table' ? 'Switch to grid view' : 'Switch to list view'
        }
    ] satisfies ToolbarItem[])

    const centerElements = $derived([
        {
            type: 'action' as const,
            icon: 'i-carbon-cloud-upload',
            text: 'Upload',
            onClick: () => (showUploadPopover = true),
            compactStage: 2,
            tooltip: gated ? WRITES_GATED_TOOLTIP : 'Upload files to server',
            disabled: gated
        },
        {
            type: 'action' as const,
            icon: 'i-carbon-folder-add',
            text: 'New Folder',
            onClick: () => (showCreateFolderPopover = true),
            compactStage: 3,
            tooltip: gated ? WRITES_GATED_TOOLTIP : 'Create a new folder in the current directory',
            disabled: gated
        }
    ] satisfies ToolbarItem[])

    const rightElements = $derived([
        {
            type: 'action' as const,
            icon: 'i-carbon-cloud-download',
            text: 'Download',
            onClick: () => console.log('Download clicked'), // TODO: bulk download
            compactStage: 2,
            tooltip: 'Download selected files',
            disabled: !hasSelection
        },
        {
            type: 'action' as const,
            icon: 'i-carbon-trash-can',
            text: 'Delete',
            onClick: handleDeleteClick,
            compactStage: 2,
            tooltip: gated ? WRITES_GATED_TOOLTIP : 'Delete selected files',
            disabled: !hasSelection || gated
        },
        {
            type: 'action' as const,
            icon: 'i-carbon-share',
            text: 'Share',
            onClick: handleShareClick,
            compactStage: 3,
            tooltip: gated ? WRITES_GATED_TOOLTIP : 'Share selected files',
            disabled: !hasSelection || gated
        }
    ] satisfies ToolbarItem[])

    function isRowSelected(row: FileItem): boolean {
        return selectedFiles.some(f => f.path === row.path)
    }

    function rowClasses(row: FileItem): string {
        return `${isRowSelected(row) ? 'bg-mauve/20 hover:bg-mauve/30' : ''} ${isShiftPressed ? 'select-none' : ''}`
    }
</script>

<Toolbar {leftElements} {centerElements} {rightElements} {onToggleSidebar} />

<PaneHeader title="Browse" subtitle={`${fileCount} ${fileCount === 1 ? 'item' : 'items'} in this folder`} />

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
<div class="flex items-center gap-2 p-2">
    <button
        class="border-1 border-overlay1 text-muted rounded-md p-1 cursor-pointer bg-transparent hover:text-primary hover:border-mauve hover:bg-surface0 disabled:opacity-50 disabled:cursor-not-allowed"
        onclick={toggleSearchBar}
        aria-label={showSearchBar ? "Close search" : "Open search"}
        disabled={loading}
    >
        <div class="{showSearchBar ? 'i-carbon-close' : 'i-carbon-search'} w-4 h-4"></div>
    </button>
    {#if !showSearchBar}
        <button
            class="border-1 border-overlay1 text-muted rounded-md p-1 cursor-pointer bg-transparent hover:text-primary hover:border-mauve hover:bg-surface0 disabled:opacity-50 disabled:cursor-not-allowed"
            onclick={navigateToRoot}
            aria-label="Navigate to root"
            disabled={loading || currentPath === '/'}
        >
            <div class="i-carbon-home w-4 h-4"></div>
        </button>
        {#if currentPath !== '/'}
            <button
                class="border-1 border-overlay1 text-muted rounded-md p-1 cursor-pointer bg-transparent hover:text-primary hover:border-mauve hover:bg-surface0 disabled:opacity-50 disabled:cursor-not-allowed"
                onclick={navigateUp}
                aria-label="Navigate up a folder"
                disabled={loading}
            >
                <div class="i-carbon-chevron-up w-4 h-4"></div>
            </button>
        {/if}
        <span class="text-subtitle text-sm font-mono">{#if currentPath === '/'}<span class="text-primary">/</span>{:else}{#each pathSegments as segment, i}<span class="text-muted">/</span>{#if i === pathSegments.length - 1}<span class="text-primary">{segment.name}</span>{:else}<span class="text-blue hover:text-primary hover:underline cursor-pointer transition-colors" onclick={() => navigateToPath(segment.path)}>{segment.name}</span>{/if}{/each}{/if}</span>
    {:else}
        <!-- Search input when search bar is open; drives the table state. -->
        <input
            class="flex-1 bg-transparent text-primary border-overlay0 border-2 rounded-md p-1"
            type="text"
            placeholder="Search in {currentPath}"
            bind:value={table.search}
            disabled={loading}
            autofocus
        >
    {/if}
</div>

{#snippet typeCell(row: FileItem)}
    <div class="{getFileIcon(row.inode_type === InodeType.Folder ? 'Folder' : 'File', getFileName(row.path), 'list')} w-4 h-4 text-muted"></div>
{/snippet}

{#snippet nameCell(row: FileItem)}
    {getFileName(row.path)}{#if row.shared_with_count && row.shared_with_count > 0}<button
            class="inline-flex items-center gap-0.5 ml-2 align-middle text-xs text-mauve hover:text-primary bg-transparent border-none cursor-pointer p-0"
            title="Shared with {row.shared_with_count} — click for details"
            aria-label="View sharing details"
            onclick={(e) => handleShareIconClick(row, e)}
        ><span class="i-carbon-share w-3 h-3"></span>{row.shared_with_count}</button>{/if}
{/snippet}

{#snippet sizeCell(row: FileItem)}
    <span class="text-sm text-muted font-mono">{formatFileSize(row.file_size)}</span>
{/snippet}

{#snippet createdCell(row: FileItem)}
    <span class="text-sm text-muted"><DateCell date={row.creation_date} /></span>
{/snippet}

{#snippet modifiedCell(row: FileItem)}
    <span class="text-sm text-muted"><DateCell date={row.modification_date} /></span>
{/snippet}

{#snippet fileTile(row: FileItem)}
    {@const selected = isRowSelected(row)}
    <div class="flex flex-col items-center gap-1 rounded-lg p-1 {selected ? 'ring-2 ring-mauve bg-mauve/10' : ''}">
        <span class="{getFileIcon(row.inode_type === InodeType.Folder ? 'Folder' : 'File', getFileName(row.path), 'detail')} text-4xl text-muted" aria-hidden="true"></span>
        <span class="text-sm text-center break-all line-clamp-2">{getFileName(row.path)}</span>
        {#if row.inode_type === InodeType.File}
            <span class="text-xs text-muted">{formatFileSize(row.file_size)}</span>
        {/if}
    </div>
{/snippet}

<Table
    state={table}
    view={viewMode}
    gridItem={fileTile}
    selection="pointer"
    toolbar={false}
    onRowClick={handleItemClick}
    onRowDblClick={handleItemDoubleClick}
    rowClass={rowClasses}
    {loading}
    loadingText="Loading files..."
    {error}
    onRetry={() => fetchFiles(currentPath)}
    empty={currentPath === '/' ? 'No files or folders found' : 'This folder is empty'}
    rowsPerPageOptions={[10, 25, 50, 100]}
    columns={[
        { id: 'type', sortField: 'inode_type', preset: 'icon', cell: typeCell },
        { id: 'name', header: 'Name', sortField: 'path', preset: 'name', cell: nameCell },
        {
            id: 'size',
            header: 'Size',
            sortField: 'file_size',
            sortValue: (r) => parseInt(r.file_size ?? '0'),
            preset: 'size',
            align: 'right',
            cell: sizeCell
        },
        { id: 'created', header: 'Created', sortField: 'creation_date', preset: 'date', cell: createdCell },
        { id: 'modified', header: 'Modified', sortField: 'modification_date', preset: 'date', cell: modifiedCell }
    ]}
/>

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
    onClose={() => (showUploadPopover = false)}
    onUploadComplete={() => refreshTriggerStore.update(n => n + 1)}
/>

<!-- Create Folder Modal -->
<CreateFolder
    isOpen={showCreateFolderPopover}
    onClose={() => (showCreateFolderPopover = false)}
    onFolderCreated={handleFolderCreated}
/>

<!-- Confirm Delete Modal -->
<ConfirmDelete
    isOpen={showConfirmDelete}
    items={selectedFiles}
    onClose={() => (showConfirmDelete = false)}
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
    onClose={() => (showShareDetails = false)}
/>
