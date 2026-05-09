<script lang="ts">
    const {
        title,
        body,
        features,
        buttons,
        buttonsClass = 'grid grid-cols-2 gap-2',
        logoSrc = undefined,
    } = $props();
    import Header from './Header.svelte'
</script>

<div class="flex-col flex gap-4 bg-surface0 rounded-lg p-4 text-primary max-w-[400px] m-2">
    {#if logoSrc}
        <img src={logoSrc} alt="HopNet" class="w-40 h-auto self-center mt-2" />
    {/if}
    {#if title || body}
        <Header
            title={title}
            body={body}
        />
    {/if}

    <div class="flex flex-col gap-2">
        {@render features()}
    </div>
    <div class={buttonsClass}>
        {@render buttons()}
    </div>
</div>