<script lang="ts">
    import ControlBarIcon from "./ControlBarIcon.svelte";
    import { onMount } from 'svelte';

    export let onToggleSidebar: () => void;
    export let onCreateTakeout: () => void;
    export let onDownloadSelected: () => void;
    export let onDeleteSelected: () => void;
    export let canCreate: boolean = true;
    export let canDownloadSelected: boolean = false;
    export let canDeleteSelected: boolean = false;

    let isMobile = false;

    function checkMobile() {
        isMobile = window.innerWidth < 768;
    }

    onMount(() => {
        checkMobile();
        window.addEventListener('resize', checkMobile);
        return () => window.removeEventListener('resize', checkMobile);
    });

</script>

<div class="flex gap-1">
    <div class="flex flex-1 min-w-0 justify-start gap-1">
        {#if isMobile}
            <ControlBarIcon
                icon="i-carbon-menu"
                title="Toggle sidebar"
                onClick={onToggleSidebar}
            />
        {/if}
        <ControlBarIcon
            icon="i-carbon-add"
            title={canCreate ? "Create new takeout" : "Cannot create - you already have an active takeout"}
            onClick={onCreateTakeout}
            disabled={!canCreate}
        />
    </div>
    <div class="flex flex-1 min-w-0 justify-center gap-1">
        <!-- Empty center section -->
    </div>
    <div class="flex flex-1 min-w-0 justify-end gap-1">
        <ControlBarIcon
            icon="i-carbon-cloud-download"
            title={canDownloadSelected ? "Download selected takeouts" : "No ready takeouts selected"}
            onClick={onDownloadSelected}
            disabled={!canDownloadSelected}
        />
        <ControlBarIcon
            icon="i-carbon-trash-can"
            title={canDeleteSelected ? "Delete selected takeouts" : "No cancellable takeouts selected"}
            onClick={onDeleteSelected}
            disabled={!canDeleteSelected}
        />
    </div>
</div>
<div>
    <h3>Data Takeouts</h3>
    <p class="text-sm text-muted">Export and download your data</p>
</div>