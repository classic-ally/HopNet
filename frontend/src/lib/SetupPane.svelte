<script lang="ts">
    const { title, body, features, buttons } = $props();
    import Header from './Header.svelte'
</script>

<div class="flex-col flex gap-4 bg-surface0 rounded-lg p-4 text-primary max-w-[400px] m-2">
    {#if title || body}
        <Header
            title={title}
            body={body}
        />
    {/if}

    <div class="flex flex-col gap-2">
        {@render features()}
    </div>
    <div class="grid grid-cols-2 gap-2">
        {@render buttons()}
    </div>
</div>