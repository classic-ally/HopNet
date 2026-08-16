<script lang="ts">
    import { onMount } from 'svelte';
    import type { PhotoRow } from '../../api/photos';
    import {
        fetchSidecarStatus, enableSidecar, disableSidecar, reinitSidecar,
        fetchRecentlyDeleted, fetchHistogram, fetchPhoto,
    } from '../../api/photos';
    import {
        defaultFilterState, filterKey, isEmpty, monthBoundaryMs, toFilter,
        type FilterState,
    } from './filters';
    import { downloadResource, registerResources, resourceName, urlFor } from './content.svelte';
    import { toDetailVM, toSummary, type MonthBucket, type PhotoSummary } from './viewmodel';
    import FilterDropdown from './FilterDropdown.svelte';
    import MonthHistogram from './MonthHistogram.svelte';
    import PhotoGrid from './PhotoGrid.svelte';
    import PhotoGridView from './PhotoGridView.svelte';
    import PhotoLightbox from './PhotoLightbox.svelte';

    interface Props {
        onToggleSidebar?: () => void;
    }

    let { onToggleSidebar = () => {} }: Props = $props();

    // ── Sidecar lifecycle ──
    let enabled    = $state(false);
    let cursor     = $state<number | null>(null);
    let fileOnDisk = $state(false);
    let loading    = $state(true);
    let error      = $state<string | null>(null);
    let showDeleted = $state(false);

    async function loadStatus() {
        try {
            const s = await fetchSidecarStatus();
            enabled = s.enabled;
            cursor  = s.cursor;
            fileOnDisk = s.file_on_disk;
        } catch (e: any) {
            error = String(e.message ?? e);
        } finally {
            loading = false;
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
            resetBrowse();
        } catch (e: any) {
            error = String(e.message ?? e);
        }
    }

    async function handleReinit() {
        error = null;
        try {
            await reinitSidecar();
            enabled = true;
            resetBrowse();
            await loadStatus();
        } catch (e: any) {
            error = String(e.message ?? e);
        }
    }

    // ── Browse orchestration (absorbed from the ingress viewer's App) ──

    // One filter source of truth; grid + histogram both derive from it.
    let filterState = $state<FilterState>({ ...defaultFilterState });
    const filter = $derived(toFilter(filterState));
    const filterEmpty = $derived(isEmpty(filterState));

    // The grid publishes its loaded window here so the lightbox can navigate it.
    let items = $state<PhotoSummary[]>([]);
    let lightboxIndex = $state<number | null>(null);

    let scrollEl = $state<HTMLDivElement>();
    // Month the grid is currently scrolled to (reported by the grid view).
    let currentMonth = $state<string | null>(null);
    // Non-null after a histogram jump to a month outside the loaded window:
    // the grid remounts anchored at this sort_ms boundary and pages BOTH
    // directions from there (windowed browse).
    let anchorMs = $state<number | null>(null);
    // Bumped by the first-sync poll so an empty grid refetches via remount.
    let refreshTick = $state(0);

    function jumpToMonth(month: string) {
        // Already in the window → just scroll to its first day header.
        const el = scrollEl?.querySelector(`h2[data-month="${month}"]`);
        if (el) {
            el.scrollIntoView({ behavior: 'smooth', block: 'start' });
            return;
        }
        // Outside the window → re-anchor the window at that month's boundary.
        resetBrowse();
        anchorMs = monthBoundaryMs(month);
        currentMonth = month;
    }

    // The grid remounts via {#key}, but `bind:items` hands it the same array —
    // without this, the new mount appends after the old window's photos.
    function resetBrowse() {
        items = [];
        lightboxIndex = null;
        anchorMs = null;
        currentMonth = null;
        if (scrollEl) scrollEl.scrollTop = 0;
    }

    // ── Rail hover zoom: scale the grid instead of reflowing it ──
    const RAIL_STRIP = 28;
    const RAIL_EXPANDED = 176;
    let railExpanded = $state(false);
    let gridAreaW = $state(0);
    const zoomK = $derived(
        railExpanded && gridAreaW > 0
            ? (gridAreaW - (RAIL_EXPANDED - RAIL_STRIP)) / gridAreaW
            : 1,
    );

    // Month histogram for the rail; any error → the rail simply doesn't render.
    let buckets = $state<MonthBucket[]>([]);
    $effect(() => {
        const f = filter;
        buckets = [];
        if (!enabled || showDeleted || filterEmpty) return;
        let cancelled = false;
        fetchHistogram(f)
            .then((b) => !cancelled && (buckets = b))
            .catch(() => {});
        return () => {
            cancelled = true;
        };
    });

    // ── Deleted view: simple offset paging rendered through the same grid ──
    const DELETED_PAGE = 100;
    let deletedRows = $state<PhotoSummary[]>([]);
    let deletedHasMore = $state(false);
    let deletedLoading = $state(false);

    async function loadDeleted(append = false) {
        deletedLoading = true;
        try {
            const offset = append ? deletedRows.length : 0;
            const rows = await fetchRecentlyDeleted(DELETED_PAGE, offset);
            const summaries = rows.map((row: PhotoRow) => {
                registerResources(row.photo_id, row.resources);
                return toSummary(row);
            });
            deletedRows = append ? [...deletedRows, ...summaries] : summaries;
            deletedHasMore = rows.length === DELETED_PAGE;
            error = null;
        } catch (e: any) {
            error = String(e.message ?? e);
        } finally {
            deletedLoading = false;
        }
    }

    function toggleDeleted() {
        showDeleted = !showDeleted;
        lightboxIndex = null;
        if (showDeleted) {
            loadDeleted(false);
        }
    }

    // ── Lightbox wiring: cache-backed URLs, sidecar-backed detail ──
    const lightboxItems = $derived(showDeleted ? deletedRows : items);
    const loadDetail = (id: string) => fetchPhoto(id).then(toDetailVM);
    const onDownload = (photoId: string, type: number) => {
        void downloadResource(photoId, type, `${resourceName(type)}-${photoId}`).catch((e) => {
            error = String(e instanceof Error ? e.message : e);
        });
    };

    onMount(() => {
        loadStatus();
        // First-sync poll: while the window is empty, remount the grid every
        // 2s (via refreshTick in the {#key}) so photos appear as the sidecar
        // hydrates. Stops mattering once anything is loaded.
        const timer = window.setInterval(() => {
            if (enabled && !showDeleted && items.length === 0) refreshTick += 1;
        }, 2000);
        return () => window.clearInterval(timer);
    });
</script>

<div class="flex flex-col flex-1 min-h-0 gap-3">
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
            <h3 class="text-lg font-semibold">Photos</h3>
            {#if enabled && cursor != null}
                <span class="text-xs text-muted">synced to height {cursor}</span>
            {/if}
        </div>
        <div class="flex items-center gap-2">
            {#if enabled}
                {#if !showDeleted}
                    <FilterDropdown
                        value={filterState}
                        onChange={(next) => {
                            filterState = next;
                            resetBrowse();
                        }}
                    />
                {/if}
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
            <span class="i-carbon-circle-dash spin text-2xl text-muted"></span>
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

    <!-- ── Browse ── -->
    {:else}
        <main class="flex-1 min-h-0 flex">
            <div class="flex-1 min-w-0" bind:clientWidth={gridAreaW}>
                <div
                    class="grid-zoom h-full overflow-y-auto"
                    bind:this={scrollEl}
                    style={zoomK < 1
                        ? `height: calc(100% / ${zoomK}); transform: scale(${zoomK});`
                        : ''}
                >
                    {#if showDeleted}
                        {#if deletedRows.length === 0 && !deletedLoading}
                            <div class="h-full grid place-items-center text-muted text-sm">
                                No recently deleted photos.
                            </div>
                        {:else}
                            <PhotoGridView
                                items={deletedRows}
                                onOpen={(i) => (lightboxIndex = i)}
                                thumbUrl={(id) => urlFor(id, 'thumb')}
                                displayUrl={(id) => urlFor(id, 'display')}
                            />
                            {#if deletedHasMore}
                                <div class="flex justify-center py-4">
                                    <button
                                        class="text-xs px-3 py-1 rounded-md border-solid border-1 border-overlay0 bg-surface0 text-muted hover:text-primary transition-colors"
                                        disabled={deletedLoading}
                                        onclick={() => loadDeleted(true)}
                                    >
                                        {deletedLoading ? 'Loading…' : 'Load more'}
                                    </button>
                                </div>
                            {/if}
                        {/if}
                    {:else if filterEmpty}
                        <div class="h-full grid place-items-center text-muted">
                            No media types selected.
                        </div>
                    {:else}
                        {#key `${filterKey(filter)}:${anchorMs ?? ''}:${refreshTick}`}
                            <PhotoGrid
                                {filter}
                                {anchorMs}
                                paused={lightboxIndex !== null}
                                {scrollEl}
                                onTopMonth={(m) => (currentMonth = m)}
                                bind:items
                                onOpen={(i) => (lightboxIndex = i)}
                            />
                        {/key}
                    {/if}
                </div>
            </div>

            {#if !showDeleted && !filterEmpty && buckets.length > 0}
                <MonthHistogram
                    {buckets}
                    current={currentMonth}
                    onJump={jumpToMonth}
                    onExpand={(e) => (railExpanded = e)}
                />
            {/if}
        </main>
    {/if}
</div>

{#if lightboxIndex !== null && lightboxItems[lightboxIndex]}
    <PhotoLightbox
        items={lightboxItems}
        index={lightboxIndex}
        onIndex={(i) => (lightboxIndex = i)}
        onClose={() => (lightboxIndex = null)}
        displayUrl={(id) => urlFor(id, 'display')}
        videoUrl={(id) => urlFor(id, 'original')}
        {loadDetail}
        {onDownload}
    />
{/if}

<style>
    /* Rail-hover zoom: transform only while expanded — at rest there's no
       transform so position:fixed children (hover preview) keep viewport
       coordinates. none <-> scale interpolates as identity.
       overflow-anchor off: the windowed grid pins the scroll position itself
       around prepends/evictions — browser scroll anchoring would double-adjust. */
    .grid-zoom {
        transform-origin: top left;
        overflow-anchor: none;
        transition:
            transform 0.18s ease,
            height 0.18s ease;
    }

    /* presetMini ships no animate-spin utility; scoped equivalent. */
    .spin {
        animation: spin 1s linear infinite;
    }
    @keyframes spin {
        to {
            transform: rotate(360deg);
        }
    }
</style>
