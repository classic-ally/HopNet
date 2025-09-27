<script lang="ts">
    import ControlBarIcon from "../Interface/ControlBarIcon.svelte";
    import { onMount } from 'svelte';

    export let title: string;
    export let subtitle: string;
    export let onToggleSidebar: () => void;

    // Action interface for header buttons
    export interface HeaderAction {
        icon: string;
        title: string;
        onClick: () => void;
        disabled?: boolean;
    }

    export let leftActions: HeaderAction[] = [];
    export let centerActions: HeaderAction[] = [];
    export let rightActions: HeaderAction[] = [];

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
    <!-- Left section with mobile menu and left actions -->
    <div class="flex flex-1 min-w-0 justify-start gap-1">
        {#if isMobile}
            <ControlBarIcon
                icon="i-carbon-menu"
                title="Toggle sidebar"
                onClick={onToggleSidebar}
            />
        {/if}
        {#each leftActions as action}
            <ControlBarIcon
                icon={action.icon}
                title={action.title}
                onClick={action.onClick}
                disabled={action.disabled}
            />
        {/each}
    </div>

    <!-- Center section -->
    <div class="flex flex-1 min-w-0 justify-center gap-1">
        {#each centerActions as action}
            <ControlBarIcon
                icon={action.icon}
                title={action.title}
                onClick={action.onClick}
                disabled={action.disabled}
            />
        {/each}
    </div>

    <!-- Right section -->
    <div class="flex flex-1 min-w-0 justify-end gap-1">
        {#each rightActions as action}
            <ControlBarIcon
                icon={action.icon}
                title={action.title}
                onClick={action.onClick}
                disabled={action.disabled}
            />
        {/each}
    </div>
</div>

<!-- Title and subtitle section -->
<div>
    <h3>{title}</h3>
    <p class="text-sm text-muted">{subtitle}</p>
</div>