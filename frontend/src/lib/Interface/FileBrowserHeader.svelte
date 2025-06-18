<script>
    import ControlBarIcon from "./ControlBarIcon.svelte";
</script>

<div class="flex gap-1">
    <div class="flex flex-1 min-w-0 justify-start gap-1">
        <ControlBarIcon
            icon="i-carbon-list"
            title="Change view mode"
        />
    </div>
    <div class="flex flex-1 min-w-0 justify-center gap-1">
        <ControlBarIcon
            icon="i-carbon-cloud-upload"
            title="Upload..."
        />
    </div>
    <div class="flex flex-1 min-w-0 justify-end gap-1">
        <ControlBarIcon
            icon="i-carbon-cloud-download"
            title="Download..."
        />
        <ControlBarIcon
            icon="i-carbon-trash-can"
            title="Delete"
        />
        <ControlBarIcon
            icon="i-carbon-share"
            title="Share"
        />
    </div>
</div>
<div>
    <h3>Recents</h3>
    <p class="text-sm text-gray-300">50 items</p>
</div>