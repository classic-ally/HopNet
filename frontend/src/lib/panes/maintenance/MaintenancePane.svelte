<script lang="ts">
    import { onMount } from 'svelte';
    import { tokenStore, API_BASE_URL } from '../../stores';
    import Toolbar from '../../primitives/Toolbar.svelte';
    import type { ToolbarItem } from '../../primitives/Toolbar.svelte';
    import Button from '../../Button.svelte';
    import PaneHeader from '../../primitives/PaneHeader.svelte';

    // Props
    export let onToggleSidebar: () => void = () => {};

    // State
    let loading = false;
    let scanLoading = false;
    let deleteLoading = false;
    let error = '';
    let scanResult: {
        scanned_at: number;
        total_fragments: number;
        orphaned_fragments: string[];
        total_bytes: number;
    } | null = null;
    let deleteResult: {
        deleted_count: number;
        failed_count: number;
        bytes_freed: number;
    } | null = null;

    // Scan for orphaned fragments
    async function handleScanFragments() {
        try {
            scanLoading = true;
            error = '';
            deleteResult = null; // Clear previous delete results

            const token = $tokenStore;
            if (!token) {
                error = 'No authentication token found';
                return;
            }

            const response = await fetch(`${API_BASE_URL}/maintenance/orphaned-fragments?grace_period_hours=1`, {
                method: 'GET',
                headers: {
                    'Authorization': `Bearer ${token}`,
                    'Content-Type': 'application/json',
                },
            });

            if (response.ok) {
                scanResult = await response.json();
            } else {
                const errorData = await response.json().catch(() => null);
                error = errorData?.error || `Failed to scan: ${response.status} ${response.statusText}`;
            }
        } catch (e) {
            error = `Network error: ${e instanceof Error ? e.message : 'Unknown error'}`;
        } finally {
            scanLoading = false;
        }
    }

    // Delete orphaned fragments based on scan
    async function handleDeleteFragments() {
        if (!scanResult) return;

        try {
            deleteLoading = true;
            error = '';

            const token = $tokenStore;
            if (!token) {
                error = 'No authentication token found';
                return;
            }

            const response = await fetch(`${API_BASE_URL}/maintenance/orphaned-fragments`, {
                method: 'DELETE',
                headers: {
                    'Authorization': `Bearer ${token}`,
                    'Content-Type': 'application/json',
                },
            });

            if (response.ok) {
                deleteResult = await response.json();
                // Clear scan result after successful deletion
                scanResult = null;
            } else {
                const errorData = await response.json().catch(() => null);
                error = errorData?.error || `Failed to delete: ${response.status} ${response.statusText}`;
            }
        } catch (e) {
            error = `Network error: ${e instanceof Error ? e.message : 'Unknown error'}`;
        } finally {
            deleteLoading = false;
        }
    }

    // Format bytes for display
    function formatBytes(bytes: number): string {
        if (bytes === 0) return '0 Bytes';
        const k = 1024;
        const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return `${(bytes / Math.pow(k, i)).toFixed(2)} ${sizes[i]}`;
    }

    // Format timestamp
    function formatTimestamp(timestamp: number): string {
        return new Date(timestamp * 1000).toLocaleString();
    }

    // Toolbar configuration
    $: leftElements = [] satisfies ToolbarItem[];
    $: rightElements = [] satisfies ToolbarItem[];
</script>

<!-- Integrated Toolbar -->
<Toolbar
    {leftElements}
    centerElements={[]}
    {rightElements}
    {onToggleSidebar}
/>

<!-- Page Title -->
<PaneHeader title="System Maintenance" subtitle="Clean up orphaned fragments and optimize storage" />

<!-- Error Display -->
{#if error}
    <div class="text-red bg-surface0 border border-red rounded-lg p-4">
        <div class="flex items-center gap-2">
            <div class="i-carbon-warning text-lg"></div>
            <span>{error}</span>
        </div>
    </div>
{/if}

<!-- Main Content -->
<div class="space-y-4">
    <!-- Orphaned Fragments Section -->
    <div class="border-solid border-1 rounded-lg p-6 border-overlay1">
        <div class="mb-4">
            <h4 class="text-lg font-semibold text-primary mb-2">Orphaned Fragment Cleanup</h4>
            <p class="text-sm text-subtitle">
                Scan the filesystem for fragments that aren't tracked in the database and remove them to free up space.
                Only fragments older than 1 hour are considered to avoid race conditions.
            </p>
        </div>

        <!-- Scan Button -->
        <div class="flex gap-3 mb-4">
            <Button
                icon={scanLoading ? "i-carbon-update animate-spin" : "i-carbon-search"}
                text={scanLoading ? "Scanning..." : "Scan for Orphaned Fragments"}
                onClick={handleScanFragments}
                disabled={scanLoading || deleteLoading}
                className="bg-blue hover:bg-blue/80 px-3 py-2"
            />

            {#if scanResult && scanResult.orphaned_fragments.length > 0}
                <Button
                    icon={deleteLoading ? "i-carbon-update animate-spin" : "i-carbon-trash-can"}
                    text={deleteLoading ? "Deleting..." : `Delete ${scanResult.orphaned_fragments.length} Orphaned Fragment${scanResult.orphaned_fragments.length !== 1 ? 's' : ''}`}
                    onClick={handleDeleteFragments}
                    disabled={scanLoading || deleteLoading}
                    className="bg-red hover:bg-red/80 px-3 py-2"
                />
            {/if}
        </div>

        <!-- Scan Results -->
        {#if scanResult}
            <div class="bg-surface0 border border-overlay0 rounded-lg p-4 space-y-3">
                <div class="flex items-center justify-between pb-3 border-b border-overlay0">
                    <span class="text-sm font-medium text-subtitle">Scan Results</span>
                    <span class="text-xs text-muted">{formatTimestamp(scanResult.scanned_at)}</span>
                </div>

                <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
                    <div class="bg-base rounded-md p-3">
                        <div class="text-xs text-subtitle mb-1">Total Fragments Scanned</div>
                        <div class="text-2xl font-semibold text-primary">{scanResult.total_fragments.toLocaleString()}</div>
                    </div>

                    <div class="bg-base rounded-md p-3">
                        <div class="text-xs text-subtitle mb-1">Orphaned Fragments</div>
                        <div class="text-2xl font-semibold {scanResult.orphaned_fragments.length > 0 ? 'text-yellow' : 'text-green'}">
                            {scanResult.orphaned_fragments.length.toLocaleString()}
                        </div>
                    </div>

                    <div class="bg-base rounded-md p-3">
                        <div class="text-xs text-subtitle mb-1">Space to Reclaim</div>
                        <div class="text-2xl font-semibold {scanResult.total_bytes > 0 ? 'text-yellow' : 'text-green'}">
                            {formatBytes(scanResult.total_bytes)}
                        </div>
                    </div>
                </div>

                {#if scanResult.orphaned_fragments.length === 0}
                    <div class="flex items-center gap-2 text-green bg-green/10 rounded-md p-3 mt-3">
                        <div class="i-carbon-checkmark-filled text-lg"></div>
                        <span class="text-sm">No orphaned fragments found. Your storage is clean!</span>
                    </div>
                {/if}
            </div>
        {/if}

        <!-- Delete Results -->
        {#if deleteResult}
            <div class="bg-surface0 border border-green rounded-lg p-4 mt-4">
                <div class="flex items-center gap-2 mb-3">
                    <div class="i-carbon-checkmark-filled text-green text-lg"></div>
                    <span class="text-sm font-medium text-primary">Cleanup Complete</span>
                </div>

                <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
                    <div class="bg-base rounded-md p-3">
                        <div class="text-xs text-subtitle mb-1">Fragments Deleted</div>
                        <div class="text-2xl font-semibold text-green">{deleteResult.deleted_count.toLocaleString()}</div>
                    </div>

                    <div class="bg-base rounded-md p-3">
                        <div class="text-xs text-subtitle mb-1">Failed Deletions</div>
                        <div class="text-2xl font-semibold {deleteResult.failed_count > 0 ? 'text-red' : 'text-muted'}">
                            {deleteResult.failed_count.toLocaleString()}
                        </div>
                    </div>

                    <div class="bg-base rounded-md p-3">
                        <div class="text-xs text-subtitle mb-1">Space Freed</div>
                        <div class="text-2xl font-semibold text-green">{formatBytes(deleteResult.bytes_freed)}</div>
                    </div>
                </div>
            </div>
        {/if}
    </div>

    <!-- Additional Maintenance Tasks (Placeholder for future) -->
    <div class="border-solid border-1 rounded-lg p-6 border-overlay1 opacity-50">
        <div class="mb-4">
            <h4 class="text-lg font-semibold text-primary mb-2">Data Block Cleanup</h4>
            <p class="text-sm text-subtitle">
                Remove orphaned data blocks that are no longer referenced by any files.
            </p>
        </div>
        <Button
            icon="i-carbon-clean"
            text="Coming Soon"
            onClick={() => {}}
            disabled={true}
            className="px-3 py-2"
        />
    </div>
</div>
