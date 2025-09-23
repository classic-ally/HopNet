<script lang="ts">
    import { TableHandler, ThSort, Th, Datatable } from '@vincjo/datatables'
    import { tokenStore, API_BASE_URL } from '../stores'
    import { onMount, tick } from 'svelte'
    import type { TakeoutRecord, TakeoutStatus } from '../types'
    import { formatDateResponsive, formatIdResponsive } from '../utils/formatters'

    let takeouts: TakeoutRecord[] = []
    let loading = true
    let error = ''
    let actionLoading: string | null = null // Track which action is loading
    let canCreateTakeout = false // Track if user can create new takeout (disabled by default until we check)
    let containerWidth = 0 // Track container width for responsive rendering
    let containerRef: HTMLElement
    let autoRefreshInterval: number | null = null
    const AUTO_REFRESH_DELAY = 5000 // Refresh every 5 seconds

    const table = new TableHandler(takeouts, {
        rowsPerPage: 20,
        selectBy: 'id',
    })
    const search = table.createSearch()

    async function fetchTakeouts(isAutoRefresh = false) {
        try {
            // Don't show loading state for auto-refresh to avoid UI flicker
            if (!isAutoRefresh) {
                loading = true
            }
            error = ''

            const token = $tokenStore
            if (!token) {
                error = 'No authentication token found'
                return
            }

            // Fetch takeouts and can-create status in parallel
            const [takeutsResponse, canCreateResponse] = await Promise.all([
                fetch(`${API_BASE_URL}/takeout`, {
                    method: 'GET',
                    headers: {
                        'Authorization': `Bearer ${token}`,
                        'Content-Type': 'application/json',
                    },
                }),
                fetch(`${API_BASE_URL}/takeout/can-create`, {
                    method: 'GET',
                    headers: {
                        'Authorization': `Bearer ${token}`,
                        'Content-Type': 'application/json',
                    },
                })
            ])

            if (takeutsResponse.ok) {
                const data = await takeutsResponse.json()
                takeouts = data
                table.setRows(takeouts)
                // Update selections array for reactivity
                selectedIds = [...table.selected]
                // Update container width after DOM updates to ensure responsive formatting
                await tick()
                updateContainerWidth()
            } else {
                error = `Failed to fetch takeouts: ${takeutsResponse.status} ${takeutsResponse.statusText}`
                console.error('Failed to fetch takeouts:', takeutsResponse.status)
            }

            if (canCreateResponse.ok) {
                const canCreateData = await canCreateResponse.json()
                canCreateTakeout = canCreateData.can_create
            } else {
                console.warn('Failed to fetch can-create status:', canCreateResponse.status)
                // Default to false if we can't determine the status (safer)
                canCreateTakeout = false
            }
        } catch (err) {
            // Only show error for non-auto-refresh requests
            if (!isAutoRefresh) {
                error = `Network error: ${err instanceof Error ? err.message : 'Unknown error'}`
            }
            console.error('Error fetching takeouts:', err)
        } finally {
            if (!isAutoRefresh) {
                loading = false
            }
        }
    }

    async function initiateTakeout() {
        try {
            actionLoading = 'initiate'
            const token = $tokenStore
            if (!token) {
                error = 'No authentication token found'
                return
            }

            const response = await fetch(`${API_BASE_URL}/takeout/initiate`, {
                method: 'POST',
                headers: {
                    'Authorization': `Bearer ${token}`,
                    'Content-Type': 'application/json',
                },
            })

            if (response.ok) {
                // Refresh the table after successful initiation
                await fetchTakeouts()
            } else {
                error = `Failed to initiate takeout: ${response.status} ${response.statusText}`
                console.error('Failed to initiate takeout:', response.status)
            }
        } catch (err) {
            error = `Network error: ${err instanceof Error ? err.message : 'Unknown error'}`
            console.error('Error initiating takeout:', err)
        } finally {
            actionLoading = null
        }
    }

    async function downloadTakeout(id: string) {
        try {
            actionLoading = id
            const token = $tokenStore
            if (!token) {
                error = 'No authentication token found'
                return
            }

            const response = await fetch(`${API_BASE_URL}/takeout/${id}/download`, {
                method: 'GET',
                headers: {
                    'Authorization': `Bearer ${token}`,
                },
            })

            if (response.ok) {
                // Create a blob and download it
                const blob = await response.blob()
                const url = window.URL.createObjectURL(blob)
                const a = document.createElement('a')
                a.href = url
                a.download = `takeout-${id}.tar.gz`
                document.body.appendChild(a)
                a.click()
                window.URL.revokeObjectURL(url)
                document.body.removeChild(a)
            } else {
                error = `Failed to download takeout: ${response.status} ${response.statusText}`
                console.error('Failed to download takeout:', response.status)
            }
        } catch (err) {
            error = `Network error: ${err instanceof Error ? err.message : 'Unknown error'}`
            console.error('Error downloading takeout:', err)
        } finally {
            actionLoading = null
        }
    }

    async function cancelTakeout(id: string) {
        try {
            actionLoading = `cancel-${id}`
            const token = $tokenStore
            if (!token) {
                error = 'No authentication token found'
                return
            }

            const response = await fetch(`${API_BASE_URL}/takeout/${id}`, {
                method: 'DELETE',
                headers: {
                    'Authorization': `Bearer ${token}`,
                    'Content-Type': 'application/json',
                },
            })

            if (response.ok) {
                // Refresh the table after successful cancellation
                await fetchTakeouts()
            } else {
                error = `Failed to cancel takeout: ${response.status} ${response.statusText}`
                console.error('Failed to cancel takeout:', response.status)
            }
        } catch (err) {
            error = `Network error: ${err instanceof Error ? err.message : 'Unknown error'}`
            console.error('Error cancelling takeout:', err)
        } finally {
            actionLoading = null
        }
    }

    function getStatusColor(status: TakeoutStatus): string {
        switch (status) {
            case 'Pending': return 'text-yellow'
            case 'Materializing': return 'text-blue'
            case 'Ready': return 'text-green'
            case 'Expired': return 'text-red'
            case 'Cancelled': return 'text-red'
            default: return 'text-muted'
        }
    }


    function updateContainerWidth() {
        if (containerRef) {
            containerWidth = containerRef.clientWidth
        }
    }

    // Force table re-render when container width changes
    $: containerWidth && table.setRows(takeouts)

    // Track selections manually for reactivity
    let selectedIds = []

    function handleSelection(id: string) {
        if (!canSelect(takeouts.find(t => t.id === id)?.status || '')) return

        table.select(id)
        selectedIds = [...table.selected] // Force reactivity by creating new array
    }

    function canDownload(status: TakeoutStatus): boolean {
        return status === 'Ready'
    }

    function canCancel(status: TakeoutStatus): boolean {
        return status === 'Pending' || status === 'Materializing' || status === 'Ready'
    }

    function canSelect(status: TakeoutStatus): boolean {
        return status !== 'Expired' && status !== 'Cancelled'
    }

    function startAutoRefresh() {
        // Don't start if already running or if there's an action in progress
        if (autoRefreshInterval || actionLoading) return

        autoRefreshInterval = setInterval(() => {
            // Only refresh if no action is in progress
            if (!actionLoading) {
                fetchTakeouts(true)
            }
        }, AUTO_REFRESH_DELAY)
    }

    function stopAutoRefresh() {
        if (autoRefreshInterval) {
            clearInterval(autoRefreshInterval)
            autoRefreshInterval = null
        }
    }

    // Reactive statement to pause auto-refresh during actions
    $: if (actionLoading) {
        stopAutoRefresh()
    } else if (!autoRefreshInterval) {
        startAutoRefresh()
    }

    // Check if any takeouts are in transitional states that need monitoring
    $: hasActiveOperations = takeouts.some(t =>
        t.status === 'Pending' || t.status === 'Materializing'
    )

    onMount(() => {
        fetchTakeouts()
        updateContainerWidth()
        window.addEventListener('resize', updateContainerWidth)
        startAutoRefresh()

        return () => {
            window.removeEventListener('resize', updateContainerWidth)
            stopAutoRefresh()
        }
    })

    // Reactive state for button enabling
    let canDownloadSelected = false
    let canDeleteSelected = false

    $: canDownloadSelected = selectedIds.length > 0 && selectedIds.some(id => {
        const takeout = takeouts.find(t => t.id === id)
        return takeout && canDownload(takeout.status)
    })

    $: canDeleteSelected = selectedIds.length > 0 && selectedIds.some(id => {
        const takeout = takeouts.find(t => t.id === id)
        return takeout && canCancel(takeout.status)
    })

    // Functions to expose to parent component
    export { initiateTakeout, canCreateTakeout, canDownloadSelected, canDeleteSelected }

    export function getCanCreateTakeout() {
        return canCreateTakeout
    }

    export function downloadSelectedTakeouts() {
        const readyTakeouts = table.selected.filter(id => {
            const takeout = takeouts.find(t => t.id === id)
            return takeout && canDownload(takeout.status)
        })

        if (readyTakeouts.length === 0) {
            error = 'No ready takeouts selected for download'
            return
        }

        // Download each selected ready takeout
        readyTakeouts.forEach(id => downloadTakeout(id))
    }

    export function deleteSelectedTakeouts() {
        const cancellableTakeouts = table.selected.filter(id => {
            const takeout = takeouts.find(t => t.id === id)
            return takeout && canCancel(takeout.status)
        })

        if (cancellableTakeouts.length === 0) {
            error = 'No cancellable takeouts selected'
            return
        }

        const count = cancellableTakeouts.length
        const message = count === 1
            ? 'Are you sure you want to delete this takeout?'
            : `Are you sure you want to delete these ${count} takeouts?`

        if (confirm(message)) {
            // Cancel each selected takeout
            cancellableTakeouts.forEach(id => cancelTakeout(id))
        }
    }

    // Reactive statement to refetch when token changes
    $: if ($tokenStore) {
        fetchTakeouts()
    }
</script>

<div class="border-solid border-1 rounded-lg p-1 border-overlay1" bind:this={containerRef}>

    {#if error}
        <div class="text-red p-2 mb-2 border border-red rounded">
            {error}
            <button
                class="ml-2 text-blue underline"
                onclick={fetchTakeouts}
            >
                Retry
            </button>
        </div>
    {/if}

    <div class="flex gap-1 mb-2">
        <!-- Search bar -->
        <input
            class="flex-1 bg-transparent text-primary border-overlay0 border-2 border-solid rounded-md p-1"
            type="text"
            placeholder="Search takeouts..."
            bind:value={search.value}
            oninput={() => search.set()}
            disabled={loading}
        >
        <!-- Rows per page selector -->
        <select
            class="p-1 border-overlay0 border-2 border-solid rounded-md bg-transparent text-primary"
            bind:value={table.rowsPerPage}
            onchange={() => table.setPage(1)}
            disabled={loading}
        >
            {#each [10, 20, 50] as option}
                <option value={option}>{option} items</option>
            {/each}
        </select>
    </div>

    {#if loading}
        <div class="text-muted p-4 text-center">
            Loading takeouts...
        </div>
    {:else}
        <div class="overflow-x-auto">
        <Datatable {table}>
            <table class="w-full whitespace-nowrap">
                <thead>
                    <tr class="text-subtitle">
                        <Th></Th>
                        <Th>ID</Th>
                        <ThSort {table} field="status">Status</ThSort>
                        <ThSort {table} field="created_at">Created</ThSort>
                        <ThSort {table} field="expires_at">Expires</ThSort>
                    </tr>
                </thead>
                <tbody>
                    {#each table.rows as row}
                        <tr class="text-left">
                            <td>
                                <input type="checkbox"
                                    checked={selectedIds.includes(row.id)}
                                    disabled={!canSelect(row.status)}
                                    onclick={() => handleSelection(row.id)}
                                >
                            </td>
                            <td class="font-mono text-sm whitespace-nowrap">{formatIdResponsive(row.id, containerWidth)}</td>
                            <td class="{getStatusColor(row.status)} whitespace-nowrap">{row.status}</td>
                            <td class="text-sm whitespace-nowrap">{formatDateResponsive(row.created_at, containerWidth)}</td>
                            <td class="text-sm whitespace-nowrap">{formatDateResponsive(row.expires_at, containerWidth)}</td>
                        </tr>
                    {:else}
                        <tr>
                            <td colspan="5" class="text-center text-muted p-4">
                                No takeouts found. Create your first data export above.
                            </td>
                        </tr>
                    {/each}
                </tbody>
            </table>
        </Datatable>
        </div>
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
</style>