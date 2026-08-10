<script lang="ts">
    import { tokenStore, API_BASE_URL } from '../../stores'
    import { onMount } from 'svelte'
    import type { TakeoutRecord, TakeoutStatus } from '../../types'
    import Toolbar from '../../primitives/Toolbar.svelte'
    import type { ToolbarItem } from '../../primitives/Toolbar.svelte'
    import Table from '../../primitives/Table.svelte'
    import { TableState } from '../../primitives/tableState.svelte'
    import DateCell from '../../primitives/DateCell.svelte'
    import PaneHeader from '../../primitives/PaneHeader.svelte'

    let { onToggleSidebar = () => {} }: { onToggleSidebar?: () => void } = $props()

    let takeouts = $state.raw<TakeoutRecord[]>([])
    let loading = $state(true)
    let error = $state('')
    let actionLoading = $state<string | null>(null) // Track which action is loading
    let canCreateTakeout = $state(false)
    let autoRefreshInterval: number | null = null
    const AUTO_REFRESH_DELAY = 5000 // Refresh every 5 seconds

    function canDownload(status: TakeoutStatus): boolean {
        return status === 'Ready'
    }

    function canCancel(status: TakeoutStatus): boolean {
        return status === 'Pending' || status === 'Materializing' || status === 'Ready'
    }

    function canSelect(status: TakeoutStatus): boolean {
        return status !== 'Expired' && status !== 'Cancelled'
    }

    // Terminal takeouts are not selectable; setRows prunes rows that expire
    // out from under a selection during the background refresh, so the bulk
    // actions can never target them.
    const table = new TableState<TakeoutRecord>([], {
        key: (r) => r.id,
        searchFields: (r) => [r.id, r.status],
        rowsPerPage: 20,
        selectable: (r) => canSelect(r.status)
    })

    const selectedRecords = $derived(
        [...table.selected].flatMap((id) => takeouts.filter((t) => t.id === id))
    )

    // Data fetching
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
                takeouts = await takeutsResponse.json()
                table.setRows(takeouts)
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

    // Takeout actions
    async function handleCreateTakeout() {
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

    function handleDownloadSelected() {
        const ready = selectedRecords.filter((t) => canDownload(t.status))
        if (ready.length === 0) {
            error = 'No ready takeouts selected for download'
            return
        }
        ready.forEach((t) => downloadTakeout(t.id))
    }

    function handleDeleteSelected() {
        const cancellable = selectedRecords.filter((t) => canCancel(t.status))
        if (cancellable.length === 0) {
            error = 'No cancellable takeouts selected'
            return
        }

        const count = cancellable.length
        const message = count === 1
            ? 'Are you sure you want to delete this takeout?'
            : `Are you sure you want to delete these ${count} takeouts?`

        if (confirm(message)) {
            cancellable.forEach((t) => cancelTakeout(t.id))
        }
    }

    // Helper functions
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

    // Auto-refresh management
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

    // Pause auto-refresh during actions
    $effect(() => {
        if (actionLoading) {
            stopAutoRefresh()
        } else if (!autoRefreshInterval) {
            startAutoRefresh()
        }
    })

    // Reactive state for button enabling
    const canDownloadSelected = $derived(selectedRecords.some((t) => canDownload(t.status)))
    const canDeleteSelected = $derived(selectedRecords.some((t) => canCancel(t.status)))

    // Toolbar configuration
    const leftElements = $derived([
        {
            type: 'action' as const,
            icon: 'i-carbon-add',
            text: 'Create Takeout',
            onClick: handleCreateTakeout,
            compactStage: 2,
            tooltip: canCreateTakeout ? "Create new takeout" : "Cannot create - you already have an active takeout",
            disabled: !canCreateTakeout || actionLoading === 'initiate'
        }
    ] satisfies ToolbarItem[]);

    const rightElements = $derived([
        {
            type: 'action' as const,
            icon: 'i-carbon-cloud-download',
            text: 'Download',
            onClick: handleDownloadSelected,
            compactStage: 2,
            tooltip: canDownloadSelected ? "Download selected takeouts" : "No ready takeouts selected",
            disabled: !canDownloadSelected
        },
        {
            type: 'action' as const,
            icon: 'i-carbon-trash-can',
            text: 'Delete',
            onClick: handleDeleteSelected,
            compactStage: 2,
            tooltip: canDeleteSelected ? "Delete selected takeouts" : "No cancellable takeouts selected",
            disabled: !canDeleteSelected
        }
    ] satisfies ToolbarItem[]);

    // Lifecycle
    onMount(() => {
        fetchTakeouts()
        startAutoRefresh()

        return () => {
            stopAutoRefresh()
        }
    })

    // Refetch when the token changes (login/logout).
    $effect(() => {
        if ($tokenStore) fetchTakeouts()
    })
</script>

<Toolbar {leftElements} centerElements={[]} {rightElements} {onToggleSidebar} />

<PaneHeader title="Data Takeouts" subtitle="Export and download your data" />

{#snippet idCell(row: TakeoutRecord)}
    <span class="font-mono text-sm" title={row.id}>{row.id}</span>
{/snippet}

{#snippet statusCell(row: TakeoutRecord)}
    <span class={getStatusColor(row.status)}>{row.status}</span>
{/snippet}

{#snippet createdCell(row: TakeoutRecord)}
    <span class="text-sm"><DateCell date={row.created_at} /></span>
{/snippet}

{#snippet expiresCell(row: TakeoutRecord)}
    <span class="text-sm"><DateCell date={row.expires_at} /></span>
{/snippet}

<Table
    state={table}
    selection="checkbox"
    searchPlaceholder="Search takeouts..."
    {loading}
    loadingText="Loading takeouts..."
    {error}
    onRetry={() => fetchTakeouts()}
    empty="No takeouts found. Create your first data export above."
    columns={[
        { id: 'id', header: 'ID', preset: 'uuid', cell: idCell },
        { id: 'status', header: 'Status', sortField: 'status', preset: 'status', cell: statusCell },
        { id: 'created', header: 'Created', sortField: 'created_at', preset: 'date', cell: createdCell },
        { id: 'expires', header: 'Expires', sortField: 'expires_at', preset: 'date', cell: expiresCell }
    ]}
/>
