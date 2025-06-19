<script lang="ts">
    import ControlBarIcon from "./ControlBarIcon.svelte";
    export let onAddNode: () => void;
</script>

<div class="flex gap-1">
    <div class="flex flex-1 min-w-0 justify-start gap-1">
        <ControlBarIcon
            icon="i-carbon-add"
            title="Add a node"
            onClick={onAddNode}
        />
    </div>
    <!-- <div class="flex flex-1 min-w-0 justify-center gap-1">
        <ControlBarIcon
            icon="i-carbon-cloud-upload"
            title="Upload..."
        />
    </div> -->
    <div class="flex flex-1 min-w-0 justify-end gap-1">
        <ControlBarIcon
            icon="i-carbon-trash-can"
            title="Delete node"
            onClick={() => {}}
        />
    </div>
</div>
<div>
    <h3>Networked Nodes</h3>
    <p class="text-sm text-gray-300">Total nodes: 1</p>
</div>