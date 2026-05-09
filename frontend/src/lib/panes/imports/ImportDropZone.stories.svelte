<script module lang="ts">
    import { defineMeta } from '@storybook/addon-svelte-csf';
    import ImportDropZone from './ImportDropZone.svelte';

    const { Story } = defineMeta({
        title: 'Panes/Imports/ImportDropZone',
        component: ImportDropZone,
        argTypes: {
            uploading: { control: 'boolean' },
            errorMessage: { control: 'text' },
        },
    });
</script>

<Story name="Idle" args={{ onSelect: (f: File) => console.log('selected', f.name) }}>
    {#snippet template(args)}
        <div class="bg-base p-6 max-w-2xl">
            <ImportDropZone {...args} />
        </div>
    {/snippet}
</Story>

<Story name="Uploading" args={{ onSelect: (f: File) => console.log('selected', f.name), uploading: true }}>
    {#snippet template(args)}
        <div class="bg-base p-6 max-w-2xl">
            <ImportDropZone {...args} />
        </div>
    {/snippet}
</Story>

<Story name="Error — quota exceeded" args={{
    onSelect: (f: File) => console.log('selected', f.name),
    errorMessage: 'Archive too large for available quota. Need 12.4 GB, have 8.0 GB free.',
}}>
    {#snippet template(args)}
        <div class="bg-base p-6 max-w-2xl">
            <ImportDropZone {...args} />
        </div>
    {/snippet}
</Story>

<Story name="Error — active import conflict" args={{
    onSelect: (f: File) => console.log('selected', f.name),
    errorMessage: 'An import is already in progress for this account.',
}}>
    {#snippet template(args)}
        <div class="bg-base p-6 max-w-2xl">
            <ImportDropZone {...args} />
        </div>
    {/snippet}
</Story>
