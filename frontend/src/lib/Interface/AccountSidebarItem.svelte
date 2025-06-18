<script lang="ts">
    import { tokenStore } from '../stores';
    
    export let selected: Boolean;
    export let onClick: () => void;
    export let onBack: () => void;
    
    function handleLogout(event: MouseEvent) {
        // Prevent event bubbling to avoid triggering handleContainerClick
        event.stopPropagation();
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

    function handleBack(event: MouseEvent) {
        // Prevent event bubbling to avoid triggering handleContainerClick
        event.stopPropagation();
        
        // Handle back action using the provided onBack function
        if (onBack) {
            onBack();
        }
    }
</script>

<div
    class={`flex flex-col p-1 gap-2 border-solid border-1 rounded-md ${selected ? 'border-indigo-500' : 'border-transparent'} ${!selected ? 'cursor-pointer' : ''}`}
    on:click={!selected ? handleContainerClick : undefined}
    on:keydown={!selected ? handleKeydown : undefined}
    role="button"
    tabindex="0"
>
    <div class="flex gap-2 items-center">
        <img src="/src/assets/svelte.svg" class="w-6 h-6" alt="User profile"/>
        <h3>allison</h3>
    </div>
    {#if selected}
        <div class="flex justify-between gap-1">
            <button
                class="text-white justify-center flex bg-indigo-950 p-1 border-indigo-500 border-solid border-1 rounded-md text-sm items-center gap-1 flex-1 cursor-pointer"
                on:click={handleBack}
            >
                <div class="i-carbon-chevron-left"></div>
                <p>Back</p>
            </button>
            <button
                class="text-white justify-center flex bg-indigo-950 p-1 border-indigo-500 border-solid border-1 rounded-md text-sm items-center gap-1 flex-1 cursor-pointer"
                on:click={handleLogout}
            >
                <div class="i-carbon-ibm-engineering-requirements-doors-next"></div>
                <p>Log out</p>
            </button>
        </div>
    {/if}
</div>