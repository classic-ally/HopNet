<script lang="ts">
    import { TableHandler, ThSort, Th, Datatable } from '@vincjo/datatables';
    import { tokenStore, API_BASE_URL, authenticatedFetch } from '../../stores';
    import { onMount, tick } from 'svelte';
    import type { DeviceInfo, RegisterDeviceResponse } from '../../types';
    import Toolbar from '../../primitives/Toolbar.svelte';
    import type { ToolbarItem } from '../../primitives/Toolbar.svelte';
    import AddDeviceModal from './AddDeviceModal.svelte';
    import DeviceKeyModal from './DeviceKeyModal.svelte';

    // Props
    export let onToggleSidebar: () => void = () => {};

    // State
    let devices: DeviceInfo[] = [];
    let loading = true;
    let error = '';
    let containerWidth = 0;
    let containerRef: HTMLElement;

    // Modal state
    let isAddModalOpen = false;
    let isKeyModalOpen = false;
    let newDeviceResponse: RegisterDeviceResponse | null = null;
    let newDeviceName = '';

    const table = new TableHandler(devices, {
        rowsPerPage: 20,
        selectBy: 'id',
    });
    const search = table.createSearch();

    // Track selections manually for reactivity
    let selectedIds: string[] = [];

    // Data fetching
    async function fetchDevices() {
        try {
            loading = true;
            error = '';

            const response = await authenticatedFetch(`${API_BASE_URL}/devices`);

            if (response.ok) {
                const data = await response.json();
                devices = data;
                table.setRows(devices);
                selectedIds = [...table.selected];
                await tick();
                updateContainerWidth();
            } else {
                error = `Failed to fetch devices: ${response.status} ${response.statusText}`;
                console.error('Failed to fetch devices:', response.status);
            }
        } catch (err) {
            error = `Network error: ${err instanceof Error ? err.message : 'Unknown error'}`;
            console.error('Error fetching devices:', err);
        } finally {
            loading = false;
        }
    }

    // Device actions
    async function revokeDevice(id: string) {
        try {
            const response = await authenticatedFetch(`${API_BASE_URL}/devices/${id}`, {
                method: 'DELETE',
            });

            if (response.ok) {
                await fetchDevices();
            } else {
                error = `Failed to revoke device: ${response.status} ${response.statusText}`;
                console.error('Failed to revoke device:', response.status);
            }
        } catch (err) {
            error = `Network error: ${err instanceof Error ? err.message : 'Unknown error'}`;
            console.error('Error revoking device:', err);
        }
    }

    function handleRevokeSelected() {
        if (selectedIds.length === 0) {
            error = 'No devices selected for revocation';
            return;
        }

        const count = selectedIds.length;
        const message = count === 1
            ? 'Are you sure you want to revoke access for this device?'
            : `Are you sure you want to revoke access for these ${count} devices?`;

        if (confirm(message)) {
            selectedIds.forEach(id => revokeDevice(id));
        }
    }

    function handleDeviceAdded(response: RegisterDeviceResponse, deviceName: string) {
        newDeviceResponse = response;
        newDeviceName = deviceName;
        isKeyModalOpen = true;
        fetchDevices();
    }

    function handleKeyModalClose() {
        isKeyModalOpen = false;
        newDeviceResponse = null;
        newDeviceName = '';
    }

    // Helper functions
    function updateContainerWidth() {
        if (containerRef) {
            containerWidth = containerRef.clientWidth;
        }
    }

    function handleSelection(id: string) {
        table.select(id);
        selectedIds = [...table.selected];
    }

    function formatDate(timestamp: number): string {
        // Backend sends Unix timestamp in seconds, JS Date expects milliseconds
        const date = new Date(timestamp * 1000);
        return date.toLocaleDateString(undefined, {
            year: 'numeric',
            month: 'short',
            day: 'numeric'
        });
    }

    // Reactive statements
    $: containerWidth && table.setRows(devices);

    // Toolbar configuration
    $: leftElements = [
        {
            type: 'action' as const,
            icon: 'i-carbon-add',
            text: 'Add Device',
            onClick: () => isAddModalOpen = true,
            compactStage: 2,
            tooltip: 'Register a new device'
        }
    ] satisfies ToolbarItem[];

    $: rightElements = [
        {
            type: 'action' as const,
            icon: 'i-carbon-trash-can',
            text: 'Revoke',
            onClick: handleRevokeSelected,
            compactStage: 2,
            tooltip: selectedIds.length > 0
                ? `Revoke ${selectedIds.length} device(s)`
                : 'Select devices to revoke',
            disabled: selectedIds.length === 0
        }
    ] satisfies ToolbarItem[];

    // Lifecycle
    onMount(() => {
        fetchDevices();
        updateContainerWidth();
        window.addEventListener('resize', updateContainerWidth);

        return () => {
            window.removeEventListener('resize', updateContainerWidth);
        };
    });

    // Reactive statement to refetch when token changes
    $: if ($tokenStore) {
        fetchDevices();
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
    <h3>Connected Devices</h3>
    <p class="text-sm text-muted">Manage devices that can access your files</p>
</div>

<!-- Devices Table -->
<div class="border-solid border-1 rounded-lg p-1 border-overlay1" bind:this={containerRef}>

    {#if error}
        <div class="text-red p-2 mb-2 border border-red rounded">
            {error}
            <button
                class="ml-2 text-blue underline"
                onclick={() => fetchDevices()}
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
            placeholder="Search devices..."
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
            Loading devices...
        </div>
    {:else}
        <div class="overflow-x-auto">
        <Datatable {table}>
            <table class="w-full whitespace-nowrap">
                <thead>
                    <tr class="text-subtitle">
                        <Th></Th>
                        <ThSort {table} field="device_name">Device Name</ThSort>
                        <ThSort {table} field="created_at">Created</ThSort>
                    </tr>
                </thead>
                <tbody>
                    {#each table.rows as row}
                        <tr class="text-left">
                            <td>
                                <input type="checkbox"
                                    checked={selectedIds.includes(row.id)}
                                    onclick={() => handleSelection(row.id)}
                                >
                            </td>
                            <td class="text-primary">{row.device_name}</td>
                            <td class="text-sm text-muted whitespace-nowrap">{formatDate(row.created_at)}</td>
                        </tr>
                    {:else}
                        <tr>
                            <td colspan="3" class="text-center text-muted p-4">
                                No devices registered. Add a device to get started.
                            </td>
                        </tr>
                    {/each}
                </tbody>
            </table>
        </Datatable>
        </div>
    {/if}
</div>

<!-- Add Device Modal -->
<AddDeviceModal
    isOpen={isAddModalOpen}
    onClose={() => isAddModalOpen = false}
    onDeviceAdded={handleDeviceAdded}
/>

<!-- Device Key Modal -->
{#if newDeviceResponse}
    <DeviceKeyModal
        isOpen={isKeyModalOpen}
        deviceName={newDeviceName}
        apiKey={newDeviceResponse.api_key}
        onClose={handleKeyModalClose}
    />
{/if}

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
