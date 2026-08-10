<script lang="ts">
    import { onMount } from 'svelte';
    import { tokenStore, incomingShareCountStore, refreshTriggerStore } from '../../stores';
    import type { IncomingShareResponse } from '../../types';
    import { fetchIncomingShares, acceptShare, declineShare } from '../../api/shares';
    import Toolbar from '../../primitives/Toolbar.svelte';
    import IncomingSharesList from './IncomingSharesList.svelte';
    import AcceptShareModal from './AcceptShareModal.svelte';
    import PaneHeader from '../../primitives/PaneHeader.svelte';

    export let onToggleSidebar: () => void = () => {};

    let shares: IncomingShareResponse[] = [];
    let loading = true;
    let error = '';

    // Accept modal state
    let showAcceptModal = false;
    let acceptingShare: IncomingShareResponse | null = null;
    let acceptLoading = false;
    let acceptError = '';

    async function loadShares() {
        try {
            loading = true;
            error = '';
            shares = await fetchIncomingShares();
        } catch (err) {
            error = err instanceof Error ? err.message : 'Failed to load shares';
        } finally {
            loading = false;
        }
    }

    function handleAcceptClick(share: IncomingShareResponse) {
        acceptingShare = share;
        acceptError = '';
        showAcceptModal = true;
    }

    async function handleAcceptConfirm(path: string) {
        if (!acceptingShare) return;
        acceptLoading = true;
        acceptError = '';

        try {
            const response = await acceptShare(acceptingShare.id, path);
            if (response.ok) {
                showAcceptModal = false;
                acceptingShare = null;
                await loadShares();
                incomingShareCountStore.update(n => Math.max(0, n - 1));
                refreshTriggerStore.update(n => n + 1);
            } else {
                const text = await response.text().catch(() => '');
                acceptError = `Failed to accept: ${response.status}${text ? ' — ' + text : ''}`;
            }
        } catch (err) {
            acceptError = err instanceof Error ? err.message : 'Failed to accept share';
        } finally {
            acceptLoading = false;
        }
    }

    async function handleDecline(share: IncomingShareResponse) {
        try {
            const response = await declineShare(share.id);
            if (response.ok) {
                await loadShares();
                incomingShareCountStore.update(n => Math.max(0, n - 1));
            }
        } catch (err) {
            error = err instanceof Error ? err.message : 'Failed to decline share';
        }
    }

    function handleAcceptClose() {
        showAcceptModal = false;
        acceptingShare = null;
        acceptError = '';
    }

    onMount(() => {
        loadShares();
    });

    $: if ($tokenStore) {
        loadShares();
    }
</script>

<Toolbar
    leftElements={[]}
    centerElements={[]}
    rightElements={[]}
    {onToggleSidebar}
/>

<PaneHeader title="Shared With Me" subtitle="Files others have shared with you" />

<div class="border-solid border-1 rounded-lg p-3 border-overlay1">
    {#if error}
        <div class="text-red p-2 mb-2 border border-red rounded">
            {error}
            <button
                class="ml-2 text-blue underline"
                onclick={() => loadShares()}
            >
                Retry
            </button>
        </div>
    {/if}

    <IncomingSharesList
        {shares}
        {loading}
        onAccept={handleAcceptClick}
        onDecline={handleDecline}
    />
</div>

<AcceptShareModal
    isOpen={showAcceptModal}
    share={acceptingShare}
    loading={acceptLoading}
    error={acceptError}
    onAccept={handleAcceptConfirm}
    onClose={handleAcceptClose}
/>
