<script lang="ts">
    import PaneHeader from "../../shared/PaneHeader.svelte";
    import type { HeaderAction } from "../../shared/PaneHeader.svelte";

    export let onToggleSidebar: () => void;
    export let onAddNode: () => void = () => {};
    export let onClearNodes: () => void = () => {};
    export let onAnalyze: () => void = () => {};

    $: leftActions = [
        {
            icon: "i-carbon-add",
            title: "Add test node",
            onClick: onAddNode
        },
        {
            icon: "i-carbon-reset",
            title: "Clear all nodes",
            onClick: onClearNodes
        }
    ] as HeaderAction[];

    $: rightActions = [
        {
            icon: "i-carbon-analytics",
            title: "Analyze network",
            onClick: onAnalyze
        }
    ] as HeaderAction[];
</script>

<PaneHeader
    title="Network Resilience"
    subtitle="Fault tolerance and capacity planning"
    {onToggleSidebar}
    {leftActions}
    centerActions={[]}
    {rightActions}
/>