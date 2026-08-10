<script lang="ts">
    import { TableHandler, ThSort, Th, Datatable } from '@vincjo/datatables';
    import { tokenStore } from '../../stores';
    import { onMount, tick } from 'svelte';
    import Toolbar from '../../primitives/Toolbar.svelte';
    import type { ToolbarItem } from '../../primitives/Toolbar.svelte';
    import AddAccountModal from './AddAccountModal.svelte';
    import ProfileEditor from './ProfileEditor.svelte';
    import { fetchAccounts, type UserInfo } from '../../api/accounts';
    import PaneHeader from '../../primitives/PaneHeader.svelte';

    // Props
    export let onToggleSidebar: () => void = () => {};

    // State
    let accounts: UserInfo[] = [];
    let loading = true;
    let error = '';
    let containerWidth = 0;
    let containerRef: HTMLElement;

    // Modal state
    let isAddModalOpen = false;

    const table = new TableHandler(accounts, {
        rowsPerPage: 20,
        selectBy: 'user_id',
    });
    const search = table.createSearch();

    // Data fetching
    async function loadAccounts() {
        try {
            loading = true;
            error = '';
            accounts = await fetchAccounts();
            table.setRows(accounts);
            await tick();
            updateContainerWidth();
        } catch (err) {
            error = err instanceof Error ? err.message : 'Failed to fetch accounts';
            console.error('Error fetching accounts:', err);
        } finally {
            loading = false;
        }
    }

    function handleAccountCreated() {
        loadAccounts();
    }

    // Helper functions
    function updateContainerWidth() {
        if (containerRef) {
            containerWidth = containerRef.clientWidth;
        }
    }

    // Reactive statements
    $: containerWidth && table.setRows(accounts);

    // Toolbar configuration
    $: leftElements = [
        {
            type: 'action' as const,
            icon: 'i-carbon-user-follow',
            text: 'Add Account',
            onClick: () => isAddModalOpen = true,
            compactStage: 2,
            tooltip: 'Create a new user account'
        }
    ] satisfies ToolbarItem[];

    // Lifecycle
    onMount(() => {
        loadAccounts();
        updateContainerWidth();
        window.addEventListener('resize', updateContainerWidth);

        return () => {
            window.removeEventListener('resize', updateContainerWidth);
        };
    });

    // Reactive statement to refetch when token changes
    $: if ($tokenStore) {
        loadAccounts();
    }
</script>

<!-- Integrated Toolbar -->
<Toolbar
    {leftElements}
    centerElements={[]}
    rightElements={[]}
    {onToggleSidebar}
/>

<div class="mb-4">
    <ProfileEditor onSaved={loadAccounts} />
</div>

<!-- Page Title -->
<PaneHeader title="User Accounts" subtitle="Manage user accounts on this node" />

<!-- Accounts Table -->
<div class="border-solid border-1 rounded-lg p-1 border-overlay1" bind:this={containerRef}>

    {#if error}
        <div class="text-red p-2 mb-2 border border-red rounded">
            {error}
            <button
                class="ml-2 text-blue underline"
                onclick={() => loadAccounts()}
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
            placeholder="Search accounts..."
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
            Loading accounts...
        </div>
    {:else}
        <div class="overflow-x-auto">
        <Datatable {table}>
            <table class="w-full whitespace-nowrap">
                <thead>
                    <tr class="text-subtitle">
                        <Th {table}></Th>
                        <ThSort {table} field="username">Username</ThSort>
                        <ThSort {table} field="first_name">Name</ThSort>
                        <ThSort {table} field="user_id">User ID</ThSort>
                    </tr>
                </thead>
                <tbody>
                    {#each table.rows as row}
                        <tr class="text-left">
                            <td class="w-8">
                                {#if row.avatar}
                                    <img src="data:image/jpeg;base64,{row.avatar}" alt="" class="w-6 h-6 rounded-full object-cover" />
                                {:else}
                                    <div class="i-carbon-user w-6 h-6 text-muted"></div>
                                {/if}
                            </td>
                            <td class="text-primary">{row.username}</td>
                            <td class="text-sm text-muted">
                                {[row.first_name, row.last_name].filter(Boolean).join(' ') || ''}
                            </td>
                            <td class="text-sm text-muted">{row.user_id}</td>
                        </tr>
                    {:else}
                        <tr>
                            <td colspan="4" class="text-center text-muted p-4">
                                No accounts found. Add an account to get started.
                            </td>
                        </tr>
                    {/each}
                </tbody>
            </table>
        </Datatable>
        </div>
    {/if}
</div>

<!-- Add Account Modal -->
<AddAccountModal
    isOpen={isAddModalOpen}
    onClose={() => isAddModalOpen = false}
    onAccountCreated={handleAccountCreated}
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

    :global(th) {
        border-bottom: 1px solid #313244 !important; /* surface0 - header separator */
    }
</style>
