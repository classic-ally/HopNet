<script lang="ts">
    import { TableHandler, ThSort, Th, Datatable } from '@vincjo/datatables';
    import { tokenStore, currentUserStore, refreshCurrentUser } from '../../stores';
    import { onMount, tick } from 'svelte';
    import Toolbar from '../../primitives/Toolbar.svelte';
    import type { ToolbarItem } from '../../primitives/Toolbar.svelte';
    import AddAccountModal from './AddAccountModal.svelte';
    import AvatarCropModal from './AvatarCropModal.svelte';
    import { fetchAccounts, updateProfile, type UserInfo } from '../../api/accounts';

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
    let isAvatarModalOpen = false;

    // Profile editing state
    let editFirstName = '';
    let editLastName = '';
    let profileSaving = false;
    let profileSuccess = '';

    const table = new TableHandler(accounts, {
        rowsPerPage: 20,
        selectBy: 'user_id',
    });
    const search = table.createSearch();

    // Sync profile form with current user
    $: if ($currentUserStore) {
        editFirstName = $currentUserStore.first_name || '';
        editLastName = $currentUserStore.last_name || '';
    }

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

    async function handleProfileSave() {
        profileSaving = true;
        profileSuccess = '';
        try {
            const fields: { first_name?: string | null; last_name?: string | null } = {};
            const current = $currentUserStore;
            const newFirst = editFirstName.trim() || null;
            const newLast = editLastName.trim() || null;
            if (newFirst !== (current?.first_name || null)) fields.first_name = newFirst;
            if (newLast !== (current?.last_name || null)) fields.last_name = newLast;
            if (Object.keys(fields).length === 0) {
                profileSuccess = 'No changes';
                return;
            }
            const response = await updateProfile(fields);
            if (!response.ok) throw new Error(`Failed: ${response.status}`);
            await refreshCurrentUser();
            await loadAccounts();
            profileSuccess = 'Profile updated';
        } catch (err) {
            error = err instanceof Error ? err.message : 'Failed to update profile';
        } finally {
            profileSaving = false;
        }
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

    $: avatarSrc = $currentUserStore?.avatar ? `data:image/jpeg;base64,${$currentUserStore.avatar}` : null;
</script>

<!-- Integrated Toolbar -->
<Toolbar
    {leftElements}
    centerElements={[]}
    rightElements={[]}
    {onToggleSidebar}
/>

<!-- My Profile Section -->
{#if $currentUserStore}
<div class="border-solid border-1 rounded-lg p-3 border-overlay1 mb-4">
    <h3 class="mb-2">My Profile</h3>
    <div class="flex gap-4 items-start">
        <!-- Avatar -->
        <div class="flex flex-col items-center gap-1 flex-shrink-0">
            <button
                class="w-16 h-16 rounded-full overflow-hidden border-2 border-overlay1 hover:border-mauve transition-colors cursor-pointer bg-surface0 flex items-center justify-center"
                onclick={() => isAvatarModalOpen = true}
            >
                {#if avatarSrc}
                    <img src={avatarSrc} alt="Avatar" class="w-full h-full object-cover" />
                {:else}
                    <div class="i-carbon-user w-8 h-8 text-muted"></div>
                {/if}
            </button>
            <button
                class="text-xs text-muted hover:text-primary cursor-pointer bg-transparent border-none"
                onclick={() => isAvatarModalOpen = true}
            >Change</button>
        </div>
        <!-- Name fields -->
        <div class="flex-1 space-y-2">
            <div class="text-sm text-muted">{$currentUserStore.username}</div>
            <div class="flex gap-2">
                <input
                    class="flex-1 bg-transparent text-primary border-overlay0 border-2 border-solid rounded-md p-1 text-sm"
                    type="text"
                    placeholder="First name"
                    bind:value={editFirstName}
                    maxlength={32}
                    disabled={profileSaving}
                />
                <input
                    class="flex-1 bg-transparent text-primary border-overlay0 border-2 border-solid rounded-md p-1 text-sm"
                    type="text"
                    placeholder="Last name"
                    bind:value={editLastName}
                    maxlength={32}
                    disabled={profileSaving}
                />
            </div>
            <div class="flex gap-2 items-center">
                <button
                    class="text-sm px-2 py-1 rounded bg-surface0 border border-overlay1 text-primary hover:bg-overlay0 transition-colors disabled:opacity-50"
                    onclick={handleProfileSave}
                    disabled={profileSaving}
                >
                    {profileSaving ? 'Saving...' : 'Update'}
                </button>
                {#if profileSuccess}
                    <span class="text-sm text-green">{profileSuccess}</span>
                {/if}
            </div>
        </div>
    </div>
</div>
{/if}

<!-- Page Title -->
<div>
    <h3>User Accounts</h3>
    <p class="text-sm text-muted">Manage user accounts on this node</p>
</div>

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

<!-- Avatar Crop Modal -->
<AvatarCropModal
    isOpen={isAvatarModalOpen}
    onClose={() => isAvatarModalOpen = false}
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
