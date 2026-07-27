<script lang="ts">
    import { tokenStore, currentUserStore } from '../stores';
    import { router } from '../router.svelte';
    import Button from '../Button.svelte';

    export let selected: Boolean;
    export let onClick: () => void;
    export let onBack: () => void;

    function handleLogout() {
        router.replace('/login');
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

    $: displayName = $currentUserStore?.first_name || $currentUserStore?.username || '';
    $: avatarSrc = $currentUserStore?.avatar ? `data:image/jpeg;base64,${$currentUserStore.avatar}` : null;
</script>

<div
    class={`flex flex-col p-1 gap-2 border-solid border-1 rounded-md ${selected ? 'border-mauve bg-surface0' : 'border-transparent hover:bg-surface0'} ${!selected ? 'cursor-pointer' : ''}`}
    on:click={!selected ? handleContainerClick : undefined}
    on:keydown={!selected ? handleKeydown : undefined}
    role="button"
    tabindex="0"
>
    <div class="flex gap-2 items-center">
        {#if avatarSrc}
            <img src={avatarSrc} alt="Avatar" class="w-6 h-6 rounded-full object-cover" />
        {:else}
            <div class="i-carbon-user w-6 h-6"></div>
        {/if}
        <h3>{displayName}</h3>
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
