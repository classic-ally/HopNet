<script lang="ts">
    import { tokenStore, API_BASE_URL, authenticatedFetch } from '../../stores';
    import { onMount } from 'svelte';
    import type { DeviceInfo, RegisterDeviceResponse } from '../../types';
    import Toolbar from '../../primitives/Toolbar.svelte';
    import type { ToolbarItem } from '../../primitives/Toolbar.svelte';
    import Table from '../../primitives/Table.svelte';
    import { TableState } from '../../primitives/tableState.svelte';
    import AddDeviceModal from './AddDeviceModal.svelte';
    import DeviceKeyModal from './DeviceKeyModal.svelte';
    import PaneHeader from '../../primitives/PaneHeader.svelte';

    let { onToggleSidebar = () => {} }: { onToggleSidebar?: () => void } = $props();

    let loading = $state(true);
    let error = $state('');
    let isAddModalOpen = $state(false);
    let isKeyModalOpen = $state(false);
    let newDeviceResponse = $state<RegisterDeviceResponse | null>(null);
    let newDeviceName = $state('');

    const table = new TableState<DeviceInfo>([], {
        key: (r) => r.id,
        searchFields: (r) => [r.device_name],
        rowsPerPage: 20
    });

    const selectedCount = $derived(table.selected.size);

    async function fetchDevices() {
        try {
            loading = true;
            error = '';

            const response = await authenticatedFetch(`${API_BASE_URL}/devices`);

            if (response.ok) {
                table.setRows(await response.json());
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
        if (selectedCount === 0) {
            error = 'No devices selected for revocation';
            return;
        }

        const message = selectedCount === 1
            ? 'Are you sure you want to revoke access for this device?'
            : `Are you sure you want to revoke access for these ${selectedCount} devices?`;

        if (confirm(message)) {
            [...table.selected].forEach((id) => revokeDevice(id as string));
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

    function formatDate(timestamp: number): string {
        // Backend sends Unix timestamp in seconds, JS Date expects milliseconds
        const date = new Date(timestamp * 1000);
        return date.toLocaleDateString(undefined, {
            year: 'numeric',
            month: 'short',
            day: 'numeric'
        });
    }

    const leftElements = [
        {
            type: 'action' as const,
            icon: 'i-carbon-add',
            text: 'Add Device',
            onClick: () => (isAddModalOpen = true),
            compactStage: 2,
            tooltip: 'Register a new device'
        }
    ] satisfies ToolbarItem[];

    const rightElements = $derived([
        {
            type: 'action' as const,
            icon: 'i-carbon-trash-can',
            text: 'Revoke',
            onClick: handleRevokeSelected,
            compactStage: 2,
            tooltip: selectedCount > 0
                ? `Revoke ${selectedCount} device(s)`
                : 'Select devices to revoke',
            disabled: selectedCount === 0
        }
    ] satisfies ToolbarItem[]);

    onMount(() => {
        fetchDevices();
    });

    // Refetch when the token changes (login/logout).
    $effect(() => {
        if ($tokenStore) fetchDevices();
    });
</script>

<Toolbar {leftElements} centerElements={[]} {rightElements} {onToggleSidebar} />

<PaneHeader title="Connected Devices" subtitle="Manage devices that can access your files" />

{#snippet nameCell(row: DeviceInfo)}
    <span class="text-primary">{row.device_name}</span>
{/snippet}

{#snippet createdCell(row: DeviceInfo)}
    <!-- Unix seconds, not ISO — formatted locally rather than through DateCell. -->
    <span class="text-sm text-muted">{formatDate(row.created_at)}</span>
{/snippet}

<Table
    state={table}
    selection="checkbox"
    searchPlaceholder="Search devices..."
    {loading}
    loadingText="Loading devices..."
    {error}
    onRetry={fetchDevices}
    empty="No devices registered. Add a device to get started."
    columns={[
        { id: 'device_name', header: 'Device Name', sortField: 'device_name', preset: 'name', cell: nameCell },
        { id: 'created_at', header: 'Created', sortField: 'created_at', preset: 'date', cell: createdCell }
    ]}
/>

<AddDeviceModal
    isOpen={isAddModalOpen}
    onClose={() => (isAddModalOpen = false)}
    onDeviceAdded={handleDeviceAdded}
/>

{#if newDeviceResponse}
    <DeviceKeyModal
        isOpen={isKeyModalOpen}
        deviceName={newDeviceName}
        apiKey={newDeviceResponse.api_key}
        onClose={handleKeyModalClose}
    />
{/if}
