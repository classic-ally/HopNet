<script lang="ts">
    import { TableHandler, ThSort, ThFilter, Th, Datatable } from '@vincjo/datatables'
    import { tokenStore } from '../stores'
    import { onMount } from 'svelte'

    interface Node {
        node_id: number;
        name: string;
        ip_address: string;
        port: number;
        owner: number;
    }

    let nodes: Node[] = []
    let loading = true
    let error = ''

    const table = new TableHandler(nodes, {
        rowsPerPage: 50,
        selectBy: 'node_id',
    })
    const search = table.createSearch()

    async function fetchNodes() {
        try {
            loading = true
            error = ''
            
            const token = $tokenStore
            if (!token) {
                error = 'No authentication token found'
                return
            }

            const response = await fetch('http://localhost:34632/nodes', {
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

    onMount(() => {
        fetchNodes()
    })

    // Reactive statement to refetch when token changes
    $: if ($tokenStore) {
        fetchNodes()
    }
</script>

<div class="border-solid border-1 rounded-lg p-1 border-indigo-500 max-w-[500px]">
    {#if error}
        <div class="text-red-400 p-2 mb-2 border border-red-600 rounded">
            {error}
            <button
                class="ml-2 text-blue-400 underline"
                onclick={fetchNodes}
            >
                Retry
            </button>
        </div>
    {/if}
    
    <div class="flex gap-1">
    <!-- Search bar -->
    <input
        class="w-full bg-transparent text-white border-indigo-900 border-2 border-solid rounded-md p-1"
        type="text"
        placeholder="Search"
        bind:value={search.value}
        oninput={() => search.set()}
        disabled={loading}
    >
    <!-- Selector of qty -->
    <select
        class="p-1 border-indigo-900 border-2 border-solid rounded-md bg-transparent text-white"
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
        <div class="text-indigo-300 p-4 text-center">
            Loading nodes...
        </div>
    {:else}
        <Datatable {table}>
            <table>
                <thead>
                    <tr class="text-indigo-300">
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
                            <td colspan="4" class="text-center text-indigo-300 p-4">
                                No nodes found
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
        background-color: #1d1b4b !important;
    }

    :global(footer) {
        border-top: none !important;
    }

    /* Footer text */
    :global(aside) {
        color: #d1d5db !important;
    }
    
    :global(td) {
        border: 1px solid #1d1b4b !important; /* This is the line width between rows and columns */
    }

    :global(th)  {
        border-bottom: 1px solid #1d1b4b !important; /* Optional: Add row separator */
    }

</style>