<script module lang="ts">
    import { defineMeta } from '@storybook/addon-svelte-csf';
    import ImportProgressCard from './ImportProgressCard.svelte';

    const { Story } = defineMeta({
        title: 'Panes/Imports/ImportProgressCard',
        component: ImportProgressCard,
    });
</script>

<Story name="Owner — early progress" args={{ counts: { total: 15, pending: 12, imported: 3, skipped: 0, failed: 0 } }}>
    {#snippet template(args)}
        <div class="bg-base p-6 max-w-2xl">
            <ImportProgressCard {...args} />
        </div>
    {/snippet}
</Story>

<Story name="Owner — mid-progress with failures" args={{ counts: { total: 100, pending: 30, imported: 60, skipped: 5, failed: 5 } }}>
    {#snippet template(args)}
        <div class="bg-base p-6 max-w-2xl">
            <ImportProgressCard {...args} />
        </div>
    {/snippet}
</Story>

<Story name="Owner — almost done" args={{ counts: { total: 100, pending: 1, imported: 95, skipped: 2, failed: 2 } }}>
    {#snippet template(args)}
        <div class="bg-base p-6 max-w-2xl">
            <ImportProgressCard {...args} />
        </div>
    {/snippet}
</Story>

<Story name="Non-owner (counts unavailable)" args={{ counts: null }}>
    {#snippet template(args)}
        <div class="bg-base p-6 max-w-2xl">
            <ImportProgressCard {...args} />
        </div>
    {/snippet}
</Story>
