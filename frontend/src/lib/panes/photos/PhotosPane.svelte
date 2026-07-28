<script lang="ts">
    // ── Props ──
    import { onMount } from 'svelte';
    import Modal from '../../primitives/Modal.svelte';
    import type { PhotoRow } from '../../api/photos';
    import {
        fetchSidecarStatus, enableSidecar, disableSidecar, reinitSidecar,
        fetchGallery, fetchRecentlyDeleted,
        mediaLabel, mediaIcon,
    } from '../../api/photos';

    interface Props {
        onToggleSidebar?: () => void;
    }

    let { onToggleSidebar = () => {} }: Props = $props();

    // ── State ──
    let enabled    = $state(false);
    let cursor     = $state<number | null>(null);
    let fileOnDisk = $state(false);
    let loading    = $state(true);
    let error      = $state<string | null>(null);
    let gallery    = $state<PhotoRow[]>([]);
    let detail     = $state<PhotoRow | null>(null);
    let showDeleted= $state(false);
    let hasMore    = $state(false);
    let loadingMore = $state(false);

    const PAGE_SIZE = 100;

    // ── Load ──
    async function loadStatus() {
        try {
            const s = await fetchSidecarStatus();
            enabled = s.enabled;
            cursor  = s.cursor;
            fileOnDisk = s.file_on_disk;
            if (s.enabled) await loadGallery();
        } catch (e: any) {
            error = String(e.message ?? e);
        } finally {
            loading = false;
        }
    }

    async function loadGallery(append = false) {
        try {
            const offset = append ? gallery.length : 0;
            const rows = showDeleted
                ? await fetchRecentlyDeleted(PAGE_SIZE, offset)
                : await fetchGallery(PAGE_SIZE, offset);
            gallery = append ? [...gallery, ...rows] : rows;
            hasMore = rows.length === PAGE_SIZE;
            error = null;
        } catch (e: any) {
            error = String(e.message ?? e);
        }
    }

    async function handleEnable() {
        error = null;
        try {
            await enableSidecar();
            enabled = true;
            await loadStatus();
        } catch (e: any) {
            error = String(e.message ?? e);
        }
    }

    async function handleDisable() {
        error = null;
        try {
            await disableSidecar();
            enabled = false;
            cursor = null;
            fileOnDisk = false;
            gallery = [];
        } catch (e: any) {
            error = String(e.message ?? e);
        }
    }

    async function handleReinit() {
        error = null;
        try {
            await reinitSidecar();
            enabled = true;
            await loadStatus();
        } catch (e: any) {
            error = String(e.message ?? e);
        }
    }

    function toggleDeleted() {
        showDeleted = !showDeleted;
        if (enabled) loadGallery(false);
    }

    async function loadMore() {
        loadingMore = true;
        await loadGallery(true);
        loadingMore = false;
    }

    onMount(() => {
        loadStatus();
        const timer = window.setInterval(() => {
            if (enabled && gallery.length === 0) loadGallery(false);
        }, 2000);
        return () => window.clearInterval(timer);
    });
</script>

<!-- ── Toolbar ── -->
<div class="flex items-center justify-between">
    <div class="flex items-center gap-3">
        <button
            class="p-2 rounded-md bg-surface0 border-solid border-1 border-overlay0 md:hidden"
            aria-label="Toggle sidebar"
            onclick={() => onToggleSidebar()}
        >
            <span class="i-carbon-menu text-lg"></span>
        </button>
        <h3 class="font-semibold text-primary">Photos</h3>
        {#if enabled}
            <span class="text-xs text-muted">
                {gallery.length} photo{gallery.length !== 1 ? 's' : ''}
                {#if cursor != null} &middot; synced to height {cursor}{/if}
            </span>
        {/if}
    </div>
    <div class="flex items-center gap-2">
        {#if enabled}
            <button
                class="text-xs px-3 py-1 rounded-md border-solid border-1 border-overlay0 bg-surface0 text-muted hover:text-primary transition-colors"
                onclick={toggleDeleted}
            >
                {showDeleted ? 'Active' : 'Deleted'}
            </button>
            <button
                class="text-xs px-3 py-1 rounded-md border-solid border-1 border-overlay0 bg-surface0 text-muted hover:text-red transition-colors"
                onclick={handleDisable}
            >
                Disable
            </button>
            <button
                class="text-xs px-3 py-1 rounded-md border-solid border-1 border-overlay0 bg-surface0 text-muted hover:text-primary transition-colors"
                onclick={handleReinit}
            >
                Re-sync
            </button>
        {/if}
    </div>
</div>

<!-- ── Error ── -->
{#if error}
    <div class="border-solid border-1 border-red/50 rounded-lg bg-red/10 p-3 text-sm text-red">
        {error}
        <button class="ml-3 underline" onclick={() => { error = null; loadStatus(); }}>Retry</button>
    </div>
{/if}

<!-- ── Loading ── -->
{#if loading}
    <div class="flex items-center justify-center p-10">
        <span class="i-carbon-circle-dash animate-spin text-2xl text-muted"></span>
    </div>

<!-- ── Not enabled ── -->
{:else if !enabled}
    <div class="flex flex-col items-center justify-center gap-4 p-10 border-solid border-1 border-dashed border-overlay1 rounded-lg">
        <span class="i-carbon-image text-4xl text-muted"></span>
        {#if fileOnDisk}
            <p class="text-muted text-sm text-center max-w-md">
                A photo sidecar exists on disk from a previous session. Enable to resume syncing from where you left off, or re-sync to start fresh.
            </p>
            <div class="flex gap-3">
                <button
                    class="px-4 py-2 rounded-md bg-mauve text-crust font-medium hover:opacity-90 transition-opacity"
                    onclick={handleEnable}
                >
                    Resume
                </button>
                <button
                    class="px-4 py-2 rounded-md border-solid border-1 border-overlay0 bg-surface0 text-muted hover:text-red text-sm transition-colors"
                    onclick={handleDisable}
                >
                    Remove
                </button>
            </div>
        {:else}
            <p class="text-muted text-sm text-center max-w-md">
                Enable the photo gallery to decrypt and index your photo library on this device.
                Metadata is synced from consensus and decrypted locally — only you hold the keys.
            </p>
            <button
                class="px-4 py-2 rounded-md bg-mauve text-crust font-medium hover:opacity-90 transition-opacity"
                onclick={handleEnable}
            >
                Enable Photo Gallery
            </button>
        {/if}
    </div>

<!-- ── Gallery ── -->
{:else if gallery.length === 0}
    <div class="flex flex-col items-center justify-center gap-2 p-10 border-solid border-1 border-dashed border-overlay1 rounded-lg">
        <span class="i-carbon-image-search text-4xl text-muted"></span>
        <p class="text-muted text-sm">{showDeleted ? 'No recently deleted photos' : 'No photos yet'}</p>
        <p class="text-xs text-subtitle">
            {showDeleted ? 'Deleted photos appear here for 30 days before permanent removal.' : 'Photos will appear here once synced from consensus.'}
        </p>
    </div>

{:else}
    {#if detail}
        <Modal title="Photo Detail" onClose={() => detail = null} size="md">
            {#snippet content()}
                <div class="flex flex-col gap-2 text-sm">
                    {#if detail.date_taken}
                        <div class="flex justify-between"><span class="text-muted">Date</span><span class="text-primary">{detail.date_taken}</span></div>
                    {/if}
                    <div class="flex justify-between"><span class="text-muted">Media</span><span class="text-primary capitalize">{mediaLabel(detail.media_type)}</span></div>
                    {#if detail.width && detail.height}
                        <div class="flex justify-between"><span class="text-muted">Dimensions</span><span class="text-primary">{detail.width} &times; {detail.height}</span></div>
                    {/if}
                    {#if detail.camera_make}
                        <div class="flex justify-between"><span class="text-muted">Camera</span><span class="text-primary">{detail.camera_make} {detail.camera_model ?? ''}</span></div>
                    {/if}
                    {#if detail.orientation != null}
                        <div class="flex justify-between"><span class="text-muted">Orientation</span><span class="text-primary">{detail.orientation}&deg;</span></div>
                    {/if}
                    {#if detail.latitude != null && detail.longitude != null}
                        <div class="flex justify-between">
                            <span class="text-muted">Location</span>
                            <a href="https://www.openstreetmap.org/?mlat={detail.latitude}&mlon={detail.longitude}&zoom=14"
                               target="_blank" class="text-mauve hover:underline">
                                {detail.latitude.toFixed(4)}, {detail.longitude.toFixed(4)}
                            </a>
                        </div>
                    {/if}
                    {#if detail.duration_ms}
                        <div class="flex justify-between"><span class="text-muted">Duration</span><span class="text-primary">{(detail.duration_ms / 1000).toFixed(1)}s</span></div>
                    {/if}
                    <div class="flex justify-between"><span class="text-muted">ID</span><span class="text-subtitle text-xs font-mono">{detail.photo_id}</span></div>
                </div>
            {/snippet}
        </Modal>
    {/if}

    <!-- ── Grid ── -->
    <div class="grid gap-3"
         style="grid-template-columns: repeat(auto-fill, minmax(180px, 1fr));">
        {#each gallery as photo (photo.photo_id)}
            <button
                class="flex flex-col gap-1 p-3 rounded-lg border-solid border-1 border-overlay0 bg-surface0 hover:bg-surface1 transition-colors text-left cursor-pointer"
                onclick={() => detail = photo}
            >
                <!-- Media type badge + date -->
                <div class="flex items-center justify-between">
                    <span class="{mediaIcon(photo.media_type)} text-muted text-sm"></span>
                    <span class="text-subtitle text-[11px]">
                        {photo.date_taken ?? '—'}
                    </span>
                </div>

                <!-- Placeholder / dimensions -->
                <div class="flex items-center justify-center h-24 bg-mantle rounded">
                    {#if photo.width && photo.height}
                        <span class="text-xs text-subtitle">{photo.width} &times; {photo.height}</span>
                    {:else}
                        <span class="i-carbon-image text-2xl text-muted"></span>
                    {/if}
                </div>

                <!-- Footer -->
                <div class="flex items-center justify-between">
                    <span class="text-subtitle text-[10px] capitalize">{mediaLabel(photo.media_type)}</span>
                    {#if photo.camera_make}
                        <span class="text-subtitle text-[10px] truncate max-w-[80px]">{photo.camera_make}</span>
                    {/if}
                </div>
            </button>
        {/each}
    </div>
    {#if hasMore}
        <div class="flex justify-center pt-3">
            <button
                class="px-4 py-2 rounded-md border-solid border-1 border-overlay0 bg-surface0 text-muted hover:text-primary transition-colors"
                disabled={loadingMore}
                onclick={loadMore}
            >
                {loadingMore ? 'Loading…' : 'Load more'}
            </button>
        </div>
    {/if}
{/if}
