<script lang="ts">
    import { tokenStore } from '../stores';
    import Button from '../Button.svelte';
    
    export let selected: Boolean;
    export let onClick: () => void;
    export let onBack: () => void;
    
    function handleLogout() {
        tokenStore.set(null);
    }

    function handleContainerClick() {
        // Only handle click if not selected
        if (!selected && onClick) {
            onClick();
        }
    }

    function handleKeydown(event: KeyboardEvent) {
        if (event.key === ' ' || event.key === 'Enter') {
            if (!selected) {
                onClick();
            }
        }
    }

    function handleBack() {
        if (onBack) {
            onBack();
        }
    }
</script>

<div
    class={`flex flex-col p-1 gap-2 border-solid border-1 rounded-md ${selected ? 'border-mauve bg-surface0' : 'border-transparent hover:bg-surface0'} ${!selected ? 'cursor-pointer' : ''}`}
    on:click={!selected ? handleContainerClick : undefined}
    on:keydown={!selected ? handleKeydown : undefined}
    role="button"
    tabindex="0"
>
    <div class="flex gap-2 items-center">
        <img src="/vite.svg" class="w-6 h-6" alt="User profile"/>
        <h3>allison</h3>
    </div>
    {#if selected}
        <div class="flex justify-between gap-1">
            <Button
                icon="i-carbon-chevron-left"
                text="Back"
                onClick={handleBack}
                className="flex-1"
            />
            <Button
                icon="i-carbon-ibm-engineering-requirements-doors-next"
                text="Log out"
                onClick={handleLogout}
                className="flex-1"
            />
        </div>
    {/if}
</div>