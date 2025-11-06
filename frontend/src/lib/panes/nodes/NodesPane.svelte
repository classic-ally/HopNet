<script lang="ts">
    import { TableHandler, ThSort, Th, Datatable } from '@vincjo/datatables'
    import { tokenStore, API_BASE_URL } from '../../stores'
    import { onMount } from 'svelte'
    import Toolbar from '../../primitives/Toolbar.svelte'
    import type { ToolbarItem } from '../../primitives/Toolbar.svelte'
    import NodeAddPane from './NodeAddPane.svelte'

    interface Node {
        node_id: number;
        name: string;
        ip_address: string;
        port: number;
        owner: number;
    }

    // Props
    export let onToggleSidebar: () => void = () => {};

    // State
    let nodes: Node[] = []
    let loading = true
    let error = ''
    let isNodeAddOpen = false

    const table = new TableHandler(nodes, {
        rowsPerPage: 50,
        selectBy: 'node_id',
    })
    const search = table.createSearch()

    // Node management functions
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
                nodes = data
                table.setRows(nodes)
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

    function handleAddNode() {
        isNodeAddOpen = true;
    }

    function handleNodeAdded() {
        // Refresh the nodes list after a node is added
        fetchNodes();
    }

    function handleDeleteNode() {
        // TODO: Implement delete functionality for selected nodes
        console.log('Delete node clicked');
    }

    // Toolbar configuration
    $: selectedCount = table.selected?.length || 0;

    $: leftElements = [
        {
            type: 'action' as const,
            icon: 'i-carbon-add',
            text: 'Add Node',
            onClick: handleAddNode,
            compactStage: 2,
            tooltip: 'Add a new node to the network'
        }
    ] satisfies ToolbarItem[];

    $: rightElements = [
        {
            type: 'action' as const,
            icon: 'i-carbon-trash-can',
            text: 'Delete',
            onClick: handleDeleteNode,
            compactStage: 2,
            tooltip: selectedCount > 0 ? `Delete ${selectedCount} selected node${selectedCount === 1 ? '' : 's'}` : 'Select nodes to delete',
            disabled: selectedCount === 0
        }
    ] satisfies ToolbarItem[];

    onMount(() => {
        fetchNodes()
    })

    // Reactive statement to refetch when token changes
    $: if ($tokenStore) {
        fetchNodes()
    }
</script>

<!-- Integrated Toolbar -->
<Toolbar
    {leftElements}
    centerElements={[]}
    {rightElements}
    {onToggleSidebar}
/>

<!-- Page Title -->
<div>
    <h3>Networked Nodes</h3>
    <p class="text-sm text-muted">Total nodes: {nodes.length}</p>
</div>

<!-- Nodes Table -->
<div class="border-solid border-1 rounded-lg p-1 border-overlay1 max-w-[500px]">
    {#if error}
        <div class="text-red p-2 mb-2 border border-red rounded">
            {error}
            <button
                class="ml-2 text-blue underline"
                onclick={fetchNodes}
            >
                Retry
            </button>
        </div>
    {/if}

    <div class="flex gap-1">
    <!-- Search bar -->
    <input
        class="w-full bg-transparent text-primary border-overlay0 border-2 border-solid rounded-md p-1"
        type="text"
        placeholder="Search"
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
        {#each [5, 10, 20, 50] as option}
            <option value={option}>{option} nodes</option>
        {/each}
    </select>
    </div>

    {#if loading}
        <div class="text-muted p-4 text-center">
            Loading nodes...
        </div>
    {:else}
        <Datatable {table}>
            <table>
                <thead>
                    <tr class="text-subtitle">
                        <Th></Th>
                        <ThSort {table} field="name">Name</ThSort>
                        <Th>IP</Th>
                        <ThSort {table} field="owner">Owner</ThSort>
                    </tr>
                </thead>
                <tbody>
                    {#each table.rows as row}
                        <tr class="text-left">
                            <td>
                                <input type="checkbox"
                                    checked={table.selected.includes(row.node_id)}
                                    onclick={()=>table.select(row.node_id)}
                                >
                            </td>
                            <td class="">{row.name}</td>
                            <td class="">{row.ip_address}</td>
                            <td class="">{row.owner}</td>
                        </tr>
                    {:else}
                        <tr>
                            <td colspan="4" class="text-center text-muted p-4">
                                No nodes found
                            </td>
                        </tr>
                    {/each}
                </tbody>
            </table>
        </Datatable>
    {/if}
</div>

<!-- Node Add Overlay -->
<NodeAddPane
    isOpen={isNodeAddOpen}
    onClose={() => {isNodeAddOpen = false}}
    onNodeAdded={handleNodeAdded}
/>

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

    :global(th)  {
        border-bottom: 1px solid #313244 !important; /* surface0 - header separator */
    }
</style>