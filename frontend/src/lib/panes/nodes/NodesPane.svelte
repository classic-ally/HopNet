<script lang="ts">
    import { tokenStore, API_BASE_URL } from '../../stores'
    import { onMount } from 'svelte'
    import Toolbar from '../../primitives/Toolbar.svelte'
    import type { ToolbarItem } from '../../primitives/Toolbar.svelte'
    import Table from '../../primitives/Table.svelte'
    import { TableState } from '../../primitives/tableState.svelte'
    import NodeAddPane from './NodeAddPane.svelte'
    import { fetchUsers } from '../../api/shares'
    import type { UserInfo } from '../../api/shares'
    import PaneHeader from '../../primitives/PaneHeader.svelte'

    interface Node {
        node_id: number;
        name: string;
        owner: number;
    }

    let { onToggleSidebar = () => {} }: { onToggleSidebar?: () => void } = $props()

    let loading = $state(true)
    let error = $state('')
    let isNodeAddOpen = $state(false)
    let usersMap = $state<Map<number, UserInfo>>(new Map())
    let nodeCount = $state(0)

    // Owner search matches the displayed identity, not the numeric id the row
    // carries — usersMap is reactive state, so results refresh when it loads.
    const table = new TableState<Node>([], {
        key: (r) => r.node_id,
        searchFields: (r) => {
            const owner = usersMap.get(r.owner)
            return [r.name, owner?.username, owner?.first_name, owner?.last_name]
        },
        rowsPerPage: 50
    })

    const selectedCount = $derived(table.selected.size)

    function ownerLabel(owner: UserInfo | undefined, fallback: number): string {
        if (owner?.first_name) {
            return `${owner.first_name}${owner.last_name ? ` ${owner.last_name}` : ''}`
        }
        return owner?.username ?? String(fallback)
    }

    async function fetchNodes() {
        try {
            loading = true
            error = ''

            const token = $tokenStore
            if (!token) {
                error = 'No authentication token found'
                return
            }

            const response = await fetch(`${API_BASE_URL}/nodes`, {
                method: 'GET',
                headers: {
                    'Authorization': `Bearer ${token}`,
                    'Content-Type': 'application/json',
                },
            })

            if (response.ok) {
                const data = await response.json()
                table.setRows(data)
                nodeCount = data.length
            } else {
                error = `Failed to fetch nodes: ${response.status} ${response.statusText}`
                console.error('Failed to fetch nodes:', response.status)
            }
        } catch (err) {
            error = `Network error: ${err instanceof Error ? err.message : 'Unknown error'}`
            console.error('Error fetching nodes:', err)
        } finally {
            loading = false
        }
    }

    function handleDeleteNode() {
        // TODO: Implement delete functionality for selected nodes
        console.log('Delete node clicked');
    }

    const leftElements = [
        {
            type: 'action' as const,
            icon: 'i-carbon-add',
            text: 'Add Node',
            onClick: () => (isNodeAddOpen = true),
            compactStage: 2,
            tooltip: 'Add a new node to the network'
        }
    ] satisfies ToolbarItem[];

    const rightElements = $derived([
        {
            type: 'action' as const,
            icon: 'i-carbon-trash-can',
            text: 'Delete',
            onClick: handleDeleteNode,
            compactStage: 2,
            tooltip: selectedCount > 0 ? `Delete ${selectedCount} selected node${selectedCount === 1 ? '' : 's'}` : 'Select nodes to delete',
            disabled: selectedCount === 0
        }
    ] satisfies ToolbarItem[]);

    async function loadUsers() {
        try {
            const users = await fetchUsers()
            usersMap = new Map(users.map(u => [u.user_id, u]))
        } catch (_) { /* best-effort */ }
    }

    onMount(() => {
        fetchNodes()
        loadUsers()
    })

    // Refetch when the token changes (login/logout).
    $effect(() => {
        if ($tokenStore) fetchNodes()
    })
</script>

<Toolbar {leftElements} centerElements={[]} {rightElements} {onToggleSidebar} />

<PaneHeader title="Networked Nodes" subtitle={`Total nodes: ${nodeCount}`} />

{#snippet ownerCell(row: Node)}
    {@const owner = usersMap.get(row.owner)}
    <div class="flex items-center gap-2">
        {#if owner?.avatar}
            <img src="data:image/jpeg;base64,{owner.avatar}" alt="" class="w-5 h-5 rounded-full object-cover flex-shrink-0" />
        {:else}
            <div class="i-carbon-user w-5 h-5 text-muted flex-shrink-0"></div>
        {/if}
        <span>{ownerLabel(owner, row.owner)}</span>
    </div>
{/snippet}

<Table
    state={table}
    selection="checkbox"
    searchPlaceholder="Search"
    {loading}
    loadingText="Loading nodes..."
    {error}
    onRetry={fetchNodes}
    empty="No nodes found"
    rowsPerPageOptions={[5, 10, 20, 50]}
    columns={[
        { id: 'name', header: 'Name', sortField: 'name', preset: 'name', field: 'name' },
        {
            id: 'owner',
            header: 'Owner',
            sortField: 'owner',
            sortValue: (r) => ownerLabel(usersMap.get(r.owner), r.owner),
            preset: 'description',
            cell: ownerCell
        }
    ]}
/>

<NodeAddPane
    isOpen={isNodeAddOpen}
    onClose={() => {isNodeAddOpen = false}}
    onNodeAdded={fetchNodes}
/>
