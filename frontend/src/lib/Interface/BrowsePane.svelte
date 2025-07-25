<script lang="ts">
    import { TableHandler, ThSort, ThFilter, Th, Datatable } from '@vincjo/datatables'
    import { tokenStore, API_BASE_URL, currentPathStore, refreshTriggerStore } from '../stores'
    import { onMount } from 'svelte'

    interface FileItem {
        owner: {
            Left: number;
        };
        path: string;
        inode_type: "File" | "Folder";
        data_id: any;
    }

    let files: FileItem[] = []
    let loading = true
    let error = ''
    let currentPath = '/'
    let pathHistory: string[] = ['/'] // Track navigation history

    // Subscribe to current path store
    $: currentPath = $currentPathStore

    const table = new TableHandler(files, {
        rowsPerPage: 50,
        selectBy: 'path',
    })
    const search = table.createSearch()

    // Extract filename from full path
    function getFileName(fullPath: string): string {
        if (fullPath === '/') return '/'
        const segments = fullPath.split('/').filter(segment => segment.length > 0)
        return segments[segments.length - 1] || '/'
    }

    // Get icon based on file type
    function getFileIcon(inodeType: "File" | "Folder"): string {
        return inodeType === "Folder" ? "i-carbon-folder" : "i-carbon-document"
    }

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
        if (item.inode_type === "Folder") {
            // Navigate into the folder
            pathHistory.push(currentPath)
            fetchFiles(item.path)
        } else if (item.inode_type === "File") {
            // Download the file
            await downloadFile(item)
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
            fetchFiles(parentPath)
        }
    }

    function navigateToRoot() {
        pathHistory = ['/']
        fetchFiles('/')
    }

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

<div class="border-solid border-1 rounded-lg p-1 border-overlay1 max-w-[800px]">
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
    <div class="flex items-center gap-2 p-2 border-b border-overlay0 mb-2">
        <button
            class="border-1 border-overlay1 text-muted border-solid rounded-md p-1 cursor-pointer bg-transparent hover:text-primary hover:border-mauve hover:bg-surface0 disabled:opacity-50 disabled:cursor-not-allowed"
            onclick={navigateToRoot}
            aria-label="Navigate to root"
            disabled={loading || currentPath === '/'}
        >
            <div class="i-carbon-home w-4 h-4"></div>
        </button>
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
        <span class="text-subtitle text-sm font-mono">{currentPath}</span>
    </div>
    
    <div class="flex gap-1">
        <!-- Search bar -->
        <input
            class="w-full bg-transparent text-primary border-overlay0 border-2 border-solid rounded-md p-1"
            type="text"
            placeholder="Search files and folders"
            bind:value={search.value}
            oninput={() => search.set()}
            disabled={loading}
        >
        <!-- Selector of qty -->
        <select
            class="p-1 border-overlay0 border-2 border-solid rounded-md bg-transparent text-primary"
            bind:value={table.rowsPerPage}
            onchange={() => table.setPage(1)}
            disabled={loading}
        >
            {#each [10, 25, 50, 100] as option}
                <option value={option}>{option} items</option>
            {/each}
        </select>
    </div>
    
    {#if loading}
        <div class="text-muted p-4 text-center">
            Loading files...
        </div>
    {:else}
        <Datatable {table}>
            <table>
                <thead>
                    <tr class="text-subtitle">
                        <ThSort {table} field="inode_type">Type</ThSort>
                        <ThSort {table} field="path">Name</ThSort>
                    </tr>
                </thead>
                <tbody>
                    {#each table.rows as row}
                        <tr
                            class="text-left cursor-pointer hover:bg-surface0"
                            onclick={() => handleItemClick(row)}
                        >
                            <td class="w-8">
                                <div class="{getFileIcon(row.inode_type)} w-4 h-4 text-muted"></div>
                            </td>
                            <td>{getFileName(row.path)}</td>
                        </tr>
                    {:else}
                        <tr>
                            <td colspan="3" class="text-center text-muted p-4">
                                {currentPath === '/' ? 'No files or folders found' : 'This folder is empty'}
                            </td>
                        </tr>
                    {/each}
                </tbody>
            </table>
        </Datatable>
    {/if}
</div>

<style>
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