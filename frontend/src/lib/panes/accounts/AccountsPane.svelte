<script lang="ts">
    import { tokenStore } from '../../stores';
    import { onMount } from 'svelte';
    import Toolbar from '../../primitives/Toolbar.svelte';
    import type { ToolbarItem } from '../../primitives/Toolbar.svelte';
    import Table from '../../primitives/Table.svelte';
    import { TableState } from '../../primitives/tableState.svelte';
    import AddAccountModal from './AddAccountModal.svelte';
    import ProfileEditor from './ProfileEditor.svelte';
    import { fetchAccounts, type UserInfo } from '../../api/accounts';
    import PaneHeader from '../../primitives/PaneHeader.svelte';

    let { onToggleSidebar = () => {} }: { onToggleSidebar?: () => void } = $props();

    let loading = $state(true);
    let error = $state('');
    let isAddModalOpen = $state(false);

    const table = new TableState<UserInfo>([], {
        searchFields: (r) => [r.username, r.first_name, r.last_name, r.user_id],
        rowsPerPage: 20
    });

    async function loadAccounts() {
        try {
            loading = true;
            error = '';
            table.setRows(await fetchAccounts());
        } catch (err) {
            error = err instanceof Error ? err.message : 'Failed to fetch accounts';
            console.error('Error fetching accounts:', err);
        } finally {
            loading = false;
        }
    }

    const leftElements = [
        {
            type: 'action' as const,
            icon: 'i-carbon-user-follow',
            text: 'Add Account',
            onClick: () => (isAddModalOpen = true),
            compactStage: 2,
            tooltip: 'Create a new user account'
        }
    ] satisfies ToolbarItem[];

    onMount(() => {
        loadAccounts();
    });

    // Refetch when the token changes (login/logout).
    $effect(() => {
        if ($tokenStore) loadAccounts();
    });
</script>

<Toolbar {leftElements} centerElements={[]} rightElements={[]} {onToggleSidebar} />

<div class="mb-4">
    <ProfileEditor onSaved={loadAccounts} />
</div>

<PaneHeader title="User Accounts" subtitle="Manage user accounts on this node" />

{#snippet avatarCell(row: UserInfo)}
    {#if row.avatar}
        <img src="data:image/jpeg;base64,{row.avatar}" alt="" class="w-6 h-6 rounded-full object-cover" />
    {:else}
        <div class="i-carbon-user w-6 h-6 text-muted"></div>
    {/if}
{/snippet}

{#snippet nameCell(row: UserInfo)}
    <span class="text-sm text-muted">
        {[row.first_name, row.last_name].filter(Boolean).join(' ') || ''}
    </span>
{/snippet}

{#snippet idCell(row: UserInfo)}
    <span class="text-sm text-muted">{row.user_id}</span>
{/snippet}

<Table
    state={table}
    searchPlaceholder="Search accounts..."
    {loading}
    loadingText="Loading accounts..."
    {error}
    onRetry={loadAccounts}
    empty="No accounts found. Add an account to get started."
    columns={[
        { id: 'avatar', preset: 'icon', cell: avatarCell },
        { id: 'username', header: 'Username', sortField: 'username', preset: 'name', field: 'username' },
        { id: 'name', header: 'Name', sortField: 'first_name', preset: 'description', cell: nameCell },
        { id: 'user_id', header: 'User ID', sortField: 'user_id', preset: 'uuid', cell: idCell }
    ]}
/>

<AddAccountModal
    isOpen={isAddModalOpen}
    onClose={() => (isAddModalOpen = false)}
    onAccountCreated={loadAccounts}
/>
